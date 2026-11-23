export interface ConnectionPoolConfig {
  /** Maximum concurrent requests per origin. Defaults to 10. */
  maxPoolSize?: number;
  /**
   * Time after which an idle origin entry is released. Defaults to 30 seconds.
   * @deprecated Use `idleTimeoutMs` instead. Both names are accepted; `idleTimeoutMs` takes precedence.
   */
  keepAliveTimeoutMs?: number;
  /** Abort a request after this many milliseconds. Defaults to 30 seconds. */
  socketTimeoutMs?: number;

  // ── Server-oriented keepalive / eviction knobs ────────────────────────────

  /**
   * Maximum number of idle (no active requests) origin slots to retain across
   * the pool. When an origin becomes idle and exceeds this limit, the oldest
   * idle origin is evicted immediately. Set to 0 to disable the limit.
   * Defaults to 0 (unlimited, existing behaviour).
   */
  maxIdleSockets?: number;
  /**
   * How long an origin slot may remain idle before it is evicted. Effectively
   * replaces `keepAliveTimeoutMs` with a clearer name. Defaults to 30 seconds.
   */
  idleTimeoutMs?: number;
  /**
   * Enable persistent keepalive for sockets (passed through to the underlying
   * agent/dispatcher when using node-fetch-style agents). Defaults to `true`.
   * Set to `false` to allow connections to close naturally after each request.
   */
  keepAlive?: boolean;
  /**
   * How often the eviction sweep timer fires to clean up idle origins beyond
   * `maxIdleSockets`. Defaults to 10 seconds (10_000 ms). Set to 0 to disable
   * the periodic sweep; eviction still runs synchronously on each release.
   */
  evictionIntervalMs?: number;
}

export interface ConnectionPoolStats {
  origin: string;
  active: number;
  queued: number;
  lastUsedAt: number;
}

/** Aggregate counts across all tracked origins. */
export interface ConnectionPoolMetrics {
  /** Total requests currently in flight across all origins. */
  activeSockets: number;
  /** Number of origin slots that are currently idle (active === 0). */
  idleSockets: number;
  /** Total number of origin slots tracked. */
  totalOrigins: number;
}

type FetchLike = typeof globalThis.fetch;
type FetchInit = RequestInit & { agent?: unknown; dispatcher?: unknown };
type QueueItem = { resolve: () => void };

interface OriginState {
  active: number;
  queue: QueueItem[];
  lastUsedAt: number;
  idleTimer?: ReturnType<typeof setTimeout>;
}

interface ResolvedPoolConfig {
  maxPoolSize: number;
  idleTimeoutMs: number;
  socketTimeoutMs: number;
  maxIdleSockets: number;
  keepAlive: boolean;
  evictionIntervalMs: number;
}

const DEFAULTS: ResolvedPoolConfig = {
  maxPoolSize: 10,
  idleTimeoutMs: 30_000,
  socketTimeoutMs: 30_000,
  maxIdleSockets: 0,
  keepAlive: true,
  evictionIntervalMs: 10_000,
};

function resolveConfig(config: ConnectionPoolConfig): ResolvedPoolConfig {
  // `idleTimeoutMs` takes precedence over the deprecated `keepAliveTimeoutMs`
  const idleTimeoutMs =
    config.idleTimeoutMs !== undefined
      ? config.idleTimeoutMs
      : (config.keepAliveTimeoutMs ?? DEFAULTS.idleTimeoutMs);

  return {
    maxPoolSize: Math.max(1, Math.floor(config.maxPoolSize ?? DEFAULTS.maxPoolSize)),
    idleTimeoutMs: Math.max(0, Math.floor(idleTimeoutMs)),
    socketTimeoutMs: Math.max(0, Math.floor(config.socketTimeoutMs ?? DEFAULTS.socketTimeoutMs)),
    maxIdleSockets: Math.max(0, Math.floor(config.maxIdleSockets ?? DEFAULTS.maxIdleSockets)),
    keepAlive: config.keepAlive ?? DEFAULTS.keepAlive,
    evictionIntervalMs: Math.max(0, Math.floor(config.evictionIntervalMs ?? DEFAULTS.evictionIntervalMs)),
  };
}

/**
 * Lightweight per-origin request pool. Native fetch implementations (including
 * Node's undici) reuse their own sockets; this layer adds bounded concurrency,
 * request timeouts, idle-socket eviction, and one stable fetch entry point for
 * SDK server factories.
 *
 * Server-oriented options:
 * - `maxIdleSockets` — cap idle origin slots; oldest idle origin evicted on release.
 * - `idleTimeoutMs` — evict idle origins after this many ms (replaces `keepAliveTimeoutMs`).
 * - `keepAlive` — advertise persistent connections to the underlying agent.
 * - `evictionIntervalMs` — periodic sweep timer frequency.
 */
export class ConnectionPool {
  private readonly config: ResolvedPoolConfig;
  private readonly origins = new Map<string, OriginState>();
  private readonly baseFetch: FetchLike;
  private evictionTimer: ReturnType<typeof setInterval> | undefined;

  constructor(config: ConnectionPoolConfig = {}, fetchImpl: FetchLike = globalThis.fetch.bind(globalThis)) {
    this.config = resolveConfig(config);
    this.baseFetch = fetchImpl;
    this.startEvictionSweep();
  }

  // ── Eviction sweep ──────────────────────────────────────────────────────────

  private startEvictionSweep(): void {
    if (this.config.evictionIntervalMs <= 0) return;
    this.evictionTimer = setInterval(() => {
      this.evictIdle();
    }, this.config.evictionIntervalMs);
    if (typeof this.evictionTimer === "object" && "unref" in this.evictionTimer) {
      (this.evictionTimer as { unref(): void }).unref();
    }
  }

  /**
   * Evict idle origins that have exceeded `idleTimeoutMs`, then enforce
   * `maxIdleSockets` by removing the oldest idle origins over the limit.
   * This runs both on a periodic timer and synchronously on each release.
   */
  evictIdle(now: number = Date.now()): number {
    let evicted = 0;

    // 1. Evict origins whose idle timeout has expired
    for (const [origin, state] of this.origins) {
      if (state.active === 0 && state.queue.length === 0) {
        const idleFor = now - state.lastUsedAt;
        if (this.config.idleTimeoutMs > 0 && idleFor >= this.config.idleTimeoutMs) {
          if (state.idleTimer) clearTimeout(state.idleTimer);
          this.origins.delete(origin);
          evicted++;
        }
      }
    }

    // 2. Enforce maxIdleSockets cap
    if (this.config.maxIdleSockets > 0) {
      const idleOrigins: string[] = [];
      for (const [origin, state] of this.origins) {
        if (state.active === 0 && state.queue.length === 0) {
          idleOrigins.push(origin);
        }
      }
      // Origins are in insertion order; remove oldest first
      while (idleOrigins.length > this.config.maxIdleSockets) {
        const oldest = idleOrigins.shift()!;
        const state = this.origins.get(oldest);
        if (state) {
          if (state.idleTimer) clearTimeout(state.idleTimer);
          this.origins.delete(oldest);
          evicted++;
        }
      }
    }

    return evicted;
  }

  // ── Internal helpers ────────────────────────────────────────────────────────

  private stateFor(origin: string): OriginState {
    const existing = this.origins.get(origin);
    if (existing) return existing;
    const state: OriginState = { active: 0, queue: [], lastUsedAt: Date.now() };
    this.origins.set(origin, state);
    return state;
  }

  private async acquire(origin: string): Promise<OriginState> {
    const state = this.stateFor(origin);
    state.lastUsedAt = Date.now();
    if (state.active < this.config.maxPoolSize) {
      state.active += 1;
      return state;
    }
    await new Promise<void>((resolve) => state.queue.push({ resolve }));
    state.active += 1;
    return state;
  }

  private release(origin: string, state: OriginState): void {
    state.active = Math.max(0, state.active - 1);
    state.lastUsedAt = Date.now();
    const next = state.queue.shift();
    if (next) next.resolve();
    if (state.active === 0 && state.queue.length === 0) {
      // Schedule per-origin idle timeout eviction
      if (state.idleTimer) clearTimeout(state.idleTimer);
      if (this.config.idleTimeoutMs > 0) {
        state.idleTimer = setTimeout(() => {
          const current = this.origins.get(origin);
          if (current === state && current.active === 0) this.origins.delete(origin);
        }, this.config.idleTimeoutMs);
      }
      // Synchronously enforce maxIdleSockets on every release
      if (this.config.maxIdleSockets > 0) {
        this.evictIdle();
      }
    }
  }

  // ── Public API ──────────────────────────────────────────────────────────────

  /** Fetch through the pool. Connections are keyed by URL origin. */
  fetch: FetchLike = async (input, init) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const origin = new URL(url).origin;
    const state = await this.acquire(origin);
    const controller = new AbortController();
    const callerSignal = init?.signal;
    const onAbort = () => controller.abort(callerSignal && "reason" in callerSignal ? callerSignal.reason : undefined);
    callerSignal?.addEventListener("abort", onAbort, { once: true });
    const timeout = this.config.socketTimeoutMs > 0
      ? setTimeout(() => controller.abort(new Error(`Connection socket timeout after ${this.config.socketTimeoutMs}ms`)), this.config.socketTimeoutMs)
      : undefined;
    try {
      const requestInit: FetchInit = {
        ...init,
        signal: controller.signal,
      };
      // Propagate keepAlive hint to underlying fetch (Node.js / undici honour it)
      if (this.config.keepAlive) {
        (requestInit as Record<string, unknown>)["keepalive"] = true;
      }
      return await this.baseFetch(input, requestInit);
    } finally {
      if (timeout) clearTimeout(timeout);
      callerSignal?.removeEventListener("abort", onAbort);
      this.release(origin, state);
    }
  };

  /** Per-origin statistics. */
  stats(): ConnectionPoolStats[] {
    return Array.from(this.origins, ([origin, state]) => ({
      origin,
      active: state.active,
      queued: state.queue.length,
      lastUsedAt: state.lastUsedAt,
    }));
  }

  /**
   * Aggregate active/idle socket counts for telemetry reporting.
   * Wire these into `recordOperationDuration` / counters in `telemetry.ts`
   * to surface pool health in your observability stack.
   */
  metrics(): ConnectionPoolMetrics {
    let activeSockets = 0;
    let idleSockets = 0;
    for (const state of this.origins.values()) {
      if (state.active > 0) {
        activeSockets += state.active;
      } else {
        idleSockets++;
      }
    }
    return { activeSockets, idleSockets, totalOrigins: this.origins.size };
  }

  /** Stop the eviction sweep and clear all tracked origins. */
  close(): void {
    if (this.evictionTimer !== undefined) {
      clearInterval(this.evictionTimer);
      this.evictionTimer = undefined;
    }
    for (const state of this.origins.values()) {
      if (state.idleTimer) clearTimeout(state.idleTimer);
    }
    this.origins.clear();
  }
}

export function createConnectionPool(config: ConnectionPoolConfig = {}): ConnectionPool {
  return new ConnectionPool(config);
}
