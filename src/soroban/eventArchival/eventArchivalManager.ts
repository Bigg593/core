/**
 * Contract event archival manager.
 *
 * Coordinates event persistence from live subscriptions to storage adapters.
 * Handles batching, deduplication, and error recovery.
 */

import {
  subscribeContractEvents,
  type ContractEvent,
  type ContractEventFilter,
  type ContractEventSubscriptionOptions,
} from "../subscribeContractEvents";
import type {
  EventArchiveStorage,
  ArchivedContractEvent,
  EventArchivalOptions,
  EventArchivalSubscription,
  ArchivalStats,
  EventArchiveQuery,
  ArchiveQueryResult,
  EventAggregation,
} from "./types";
import { ok, err, SorokitErrorCode } from "../../shared/response";
import type { SorokitResult } from "../../shared/response";

/**
 * Convert a live contract event to an archived event record.
 */
function toArchivedEvent(event: ContractEvent): ArchivedContractEvent | null {
  // Validate required fields
  if (!event.id || !event.contractId || !event.ledger) {
    return null;
  }

  const topics = Array.isArray(event.topics)
    ? event.topics.filter((t): t is string => typeof t === "string")
    : Array.isArray(event.topic)
      ? event.topic.filter((t): t is string => typeof t === "string")
      : [];

  const eventType = event.eventType ?? event.name ?? "";
  if (!eventType) {
    return null;
  }

  const timestamp = event.timestamp ?? Date.now();

  return {
    id: String(event.id),
    contractId: String(event.contractId),
    eventType,
    topics,
    value: event.value,
    ledger: event.ledger,
    timestamp,
    transactionHash:
      typeof event.transaction_hash === "string"
        ? event.transaction_hash
        : undefined,
    emitter: typeof event.emitter === "string" ? event.emitter : undefined,
    metadata: {
      ...event,
      id: undefined,
      contractId: undefined,
      eventType: undefined,
      topics: undefined,
      value: undefined,
      ledger: undefined,
      timestamp: undefined,
    },
  };
}

/**
 * Event archival manager.
 *
 * Subscribes to live contract events and persists them to a storage adapter.
 * Handles batching, deduplication, and error recovery without corrupting
 * the live event stream.
 *
 * Retention policy (maxEvents, maxAgeMs) is enforced synchronously on insert
 * so writes are never blocked; eviction is best-effort and storage errors are
 * surfaced via `onStorageError` without interrupting the subscription.
 *
 * @example
 * const storage = new InMemoryEventArchiveStorage();
 * const manager = new EventArchivalManager(storage, {
 *   maxEvents: 10_000,
 *   maxAgeMs: 7 * 24 * 60 * 60 * 1000, // 7 days
 * });
 *
 * const subscription = await manager.archiveContractEvents(
 *   "CONTRACT123",
 *   undefined,
 *   { horizonUrl: "https://horizon-testnet.stellar.org" }
 * );
 *
 * // Later: stop archiving
 * subscription.unsubscribe();
 */
export class EventArchivalManager {
  private storage: EventArchiveStorage;
  private batchSize: number;
  private deduplicate: boolean;
  private maxEvents: number;
  private maxAgeMs: number;
  private onStorageError?:
    | ((error: Error, events: ArchivedContractEvent[]) => void)
    | undefined;

  constructor(
    storage: EventArchiveStorage,
    options?: Partial<EventArchivalOptions>,
  ) {
    this.storage = storage;
    this.batchSize = options?.batchSize ?? 50;
    this.deduplicate = options?.deduplicate ?? true;
    this.maxEvents = options?.maxEvents ?? 0;
    this.maxAgeMs = options?.maxAgeMs ?? 0;
    this.onStorageError = options?.onStorageError;
  }

  /**
   * Enforce the retention policy before storing a batch.
   *
   * - Age-based eviction: deletes events older than `now - maxAgeMs`.
   * - Count-based eviction: when total + incoming > maxEvents, removes oldest
   *   events (by timestamp ascending) to make room.
   *
   * Both steps run synchronously within the flush pipeline and do not block
   * the caller — if the eviction delete fails, the error is swallowed and
   * the insert proceeds anyway so we don't lose live events.
   */
  private async applyRetentionPolicy(
    incomingCount: number,
    now: number,
  ): Promise<void> {
    // 1. Age-based eviction
    if (this.maxAgeMs > 0) {
      const cutoff = now - this.maxAgeMs;
      await this.storage.delete({ toTimestamp: cutoff }).catch(() => undefined);
    }

    // 2. Count-based eviction
    if (this.maxEvents > 0) {
      const current = await this.storage.count().catch(() => 0);
      const overflow = current + incomingCount - this.maxEvents;
      if (overflow > 0) {
        // Query the oldest `overflow` events and delete them
        const oldestResult = await this.storage.query({
          orderBy: "timestamp",
          order: "asc",
          limit: overflow,
          offset: 0,
        }).catch(() => null);

        if (oldestResult?.status === "ok" && oldestResult.data.events.length > 0) {
          // Build a ledger-based delete that covers only those exact events.
          // We use the oldest ledger as the cutoff; imprecise but safe for
          // in-memory and straightforward for custom adapters to implement.
          const oldestTs = oldestResult.data.events
            .map((e) => (typeof e.timestamp === "number" ? e.timestamp : Date.parse(e.timestamp as string)))
            .filter((ts) => Number.isFinite(ts));
          if (oldestTs.length > 0) {
            const maxOldTs = Math.max(...oldestTs);
            await this.storage.delete({ toTimestamp: maxOldTs }).catch(() => undefined);
          }
        }
      }
    }
  }

  /**
   * Archive contract events from a live subscription.
   *
   * Subscribes to contract events and persists them to the storage adapter.
   * Storage failures do not interrupt the live event stream.
   *
   * @param contractId - Contract address to monitor
   * @param filter - Optional event filter
   * @param options - Subscription options
   * @returns Subscription handle with unsubscribe and stats
   */
  async archiveContractEvents(
    contractId: string,
    filter: ContractEventFilter | undefined,
    options: ContractEventSubscriptionOptions,
  ): Promise<SorokitResult<EventArchivalSubscription>> {
    try {
      const stats: ArchivalStats = {
        archivedCount: 0,
        duplicateCount: 0,
        errorCount: 0,
      };

      let pendingBatch: ArchivedContractEvent[] = [];
      let batchTimer: ReturnType<typeof setTimeout> | undefined;

      const flushBatch = async (): Promise<void> => {
        if (pendingBatch.length === 0) return;

        const batchToStore = pendingBatch;
        pendingBatch = [];

        // Clear batch timer
        if (batchTimer) {
          clearTimeout(batchTimer);
          batchTimer = undefined;
        }

        // Deduplicate if enabled
        let eventsToStore = batchToStore;
        if (this.deduplicate) {
          const deduped: ArchivedContractEvent[] = [];
          const seenIds = new Set<string>();

          for (const event of batchToStore) {
            // Check if already in current batch
            if (seenIds.has(event.id)) {
              stats.duplicateCount++;
              continue;
            }

            // Check if already in storage
            const exists = await this.storage.exists(event.id);
            if (exists) {
              stats.duplicateCount++;
              continue;
            }

            seenIds.add(event.id);
            deduped.push(event);
          }

          eventsToStore = deduped;
        }

        if (eventsToStore.length === 0) {
          return;
        }

        // Apply retention policy before writing
        await this.applyRetentionPolicy(eventsToStore.length, Date.now());

        // Store events
        try {
          const storeResult = await this.storage.store(eventsToStore);
          if (storeResult.status === "ok") {
            stats.archivedCount += eventsToStore.length;
            stats.lastArchivedAt = Date.now();
          } else {
            stats.errorCount++;
            if (this.onStorageError) {
              this.onStorageError(
                new Error(storeResult.error.message),
                eventsToStore,
              );
            }
          }
        } catch (error) {
          stats.errorCount++;
          if (this.onStorageError) {
            this.onStorageError(
              error instanceof Error ? error : new Error(String(error)),
              eventsToStore,
            );
          }
        }
      };

      const scheduleBatchFlush = (): void => {
        if (batchTimer) {
          clearTimeout(batchTimer);
        }
        // Flush batch after 5 seconds of inactivity
        batchTimer = setTimeout(() => {
          void flushBatch();
        }, 5000);
      };

      const handleEvents = (events: ContractEvent[]): void => {
        // Convert to archived events
        const archivedEvents = events
          .map(toArchivedEvent)
          .filter((e): e is ArchivedContractEvent => e !== null);

        // Add to pending batch
        pendingBatch.push(...archivedEvents);

        // Flush if batch is full
        if (pendingBatch.length >= this.batchSize) {
          void flushBatch();
        } else {
          scheduleBatchFlush();
        }
      };

      // Subscribe to contract events
      const unsubscribe = subscribeContractEvents(
        contractId,
        filter,
        handleEvents,
        options,
      );

      return ok({
        unsubscribe: () => {
          unsubscribe();
          // Flush any pending events
          void flushBatch();
        },
        getStats: () => ({ ...stats }),
      });
    } catch (error) {
      return err(
        SorokitErrorCode.UNKNOWN,
        `Failed to start archiving: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  /**
   * Query archived events.
   *
   * @param query - Query filters and pagination
   * @returns Query results with pagination
   */
  async queryArchivedEvents(
    query: EventArchiveQuery,
  ): Promise<SorokitResult<ArchiveQueryResult>> {
    return this.storage.query(query);
  }

  /**
   * Get aggregated event statistics.
   *
   * @param query - Query filters
   * @param intervalMs - Time series interval (optional)
   * @returns Aggregated statistics
   */
  async getEventAggregation(
    query: EventArchiveQuery,
    intervalMs?: number,
  ): Promise<SorokitResult<EventAggregation>> {
    return this.storage.aggregate(query, intervalMs);
  }

  /**
   * Delete archived events matching the query.
   *
   * @param query - Query filters
   * @returns Number of deleted events
   */
  async deleteArchivedEvents(
    query: EventArchiveQuery,
  ): Promise<SorokitResult<number>> {
    return this.storage.delete(query);
  }

  /**
   * Get storage statistics.
   *
   * Returns the stats from the underlying storage adapter.
   * The `totalEvents` field reflects the current event count after any
   * retention-policy evictions.
   */
  async getStorageStats() {
    return this.storage.getStats();
  }

  /**
   * Return the current archived event count.
   *
   * Useful for wiring into telemetry: call `recordConnectionPoolMetrics` or
   * a plain counter to surface the count in your observability stack.
   *
   * @example
   * setInterval(async () => {
   *   const count = await manager.getArchivedEventCount();
   *   activeManager?.recordCounter("event_archival.total_events", count);
   * }, 10_000);
   */
  async getArchivedEventCount(): Promise<number> {
    return this.storage.count();
  }
}

/**
 * Query archived contract events.
 *
 * Standalone function for querying without creating a manager instance.
 *
 * @param storage - Storage adapter
 * @param query - Query filters
 * @returns Query results
 *
 * @example
 * const results = await queryContractEventArchive(storage, {
 *   contractIds: ["CONTRACT123"],
 *   fromTimestamp: Date.now() - 86400000, // Last 24 hours
 *   limit: 100
 * });
 */
export async function queryContractEventArchive(
  storage: EventArchiveStorage,
  query: EventArchiveQuery,
): Promise<SorokitResult<ArchiveQueryResult>> {
  return storage.query(query);
}

/**
 * Calculate event rate from archived data.
 *
 * @param storage - Storage adapter
 * @param query - Query filters
 * @param windowMs - Time window for rate calculation
 * @returns Events per second
 *
 * @example
 * const rate = await calculateArchivedEventRate(storage, {
 *   contractIds: ["CONTRACT123"],
 *   fromTimestamp: Date.now() - 3600000, // Last hour
 * });
 */
export async function calculateArchivedEventRate(
  storage: EventArchiveStorage,
  query: EventArchiveQuery,
  windowMs?: number,
): Promise<SorokitResult<number>> {
  const aggResult = await storage.aggregate(query, windowMs);
  if (aggResult.status === "error") {
    return aggResult as SorokitResult<number>;
  }

  return ok(aggResult.data.rate ?? 0);
}

/**
 * Get time series data for archived events.
 *
 * @param storage - Storage adapter
 * @param query - Query filters
 * @param intervalMs - Time bucket interval
 * @returns Time series buckets
 *
 * @example
 * const timeSeries = await getArchivedEventTimeSeries(storage, {
 *   contractIds: ["CONTRACT123"],
 *   fromTimestamp: Date.now() - 86400000,
 * }, 3600000); // 1 hour buckets
 */
export async function getArchivedEventTimeSeries(
  storage: EventArchiveStorage,
  query: EventArchiveQuery,
  intervalMs: number,
): Promise<SorokitResult<EventAggregation>> {
  return storage.aggregate(query, intervalMs);
}
