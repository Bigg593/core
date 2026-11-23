/**
 * Next.js serverless example — server-side helpers (#next-serverless).
 *
 * This module runs only on the server. It holds the client factory, offline
 * signing, and batch operations. Nothing here should ever be imported from
 * browser-side code — keep it inside `pages/api/` or a server component.
 *
 * In your own app the SDK imports below become:
 *   import { createSorokitClient, signTransactionOffline, ... } from "sorokit-core";
 */

import { createSorokitClient } from "../../../src/client/createSorokitClient";
import { signTransactionOffline } from "../../../src/wallet/signTransaction";
import { createConnectionPool } from "../../../src/network/connectionPool";
import { SERVER_POOL_DEFAULTS } from "../../../src/network/config";
import { recordConnectionPoolMetrics } from "../../../src/performance/telemetry";
import type { SorokitClient } from "../../../src/client/createSorokitClient";
import type { NetworkType } from "../../../src/network/config";
import type { SorokitResult } from "../../../src/shared/response";
import type { TransactionResult } from "../../../src/transaction/types";
import type { AccountInfo } from "../../../src/account/types";

// ── Connection pool ───────────────────────────────────────────────────────────
//
// A single pool is shared across the process lifetime.  SERVER_POOL_DEFAULTS
// sets sensible keepalive and eviction values for a long-running Node server:
//   - persistent keepalive (keepAlive: true)
//   - up to 10 idle slots before oldest is evicted (maxIdleSockets: 10)
//   - idle origins released after 60 s (idleTimeoutMs: 60_000)
//   - eviction sweep every 15 s (evictionIntervalMs: 15_000)
//
// Tune these values for your host's per-origin connection limits and traffic
// profile; Horizon allows ~100 concurrent connections per IP by default.

export const serverPool = createConnectionPool(SERVER_POOL_DEFAULTS);

// Optional: report pool metrics to your telemetry pipeline every 15 s.
// Remove this if you haven't called initTelemetry() — it's a no-op then.
const _poolMetricsTimer = setInterval(() => {
  recordConnectionPoolMetrics(serverPool.metrics());
}, 15_000);
// Prevent the timer from keeping the process alive in serverless runtimes
if (typeof _poolMetricsTimer === "object" && "unref" in _poolMetricsTimer) {
  (_poolMetricsTimer as { unref(): void }).unref();
}

// ── client factory ────────────────────────────────────────────────────────────
//
// In a serverless environment each invocation is stateless, so we create a
// fresh client per request. The client itself is lightweight — it holds no
// connections, just config — so this is fine.

export function getServerClient(network: NetworkType = "testnet"): SorokitClient {
  const result = createSorokitClient({ network });
  if (result.status === "error") {
    // Misconfigured network is a deployment-time problem, not a request-time
    // one. Throwing here surfaces it in logs immediately.
    throw new Error(`sorokit-core client init failed: ${result.error.message}`);
  }
  return result.data;
}

// ── sign and submit ───────────────────────────────────────────────────────────

export interface SendPaymentParams {
  sourcePublicKey: string;
  destination: string;
  amount: string;
  /** Signing secret key — must come from an environment variable, never hardcoded */
  secretKey: string;
}

/**
 * Build a payment transaction on the server, sign it with a secret key, and
 * submit it — all in one call.
 *
 * Used by API routes and Lambda handlers where there is no browser wallet.
 */
export async function serverSendPayment(
  client: SorokitClient,
  params: SendPaymentParams,
): Promise<SorokitResult<TransactionResult>> {
  // 1. Build the unsigned XDR
  const built = await client.transaction.buildPayment(params.sourcePublicKey, {
    destination: params.destination,
    amount: params.amount,
  });
  if (built.status === "error") return built;

  // 2. Sign with the secret key — this runs in-process, no wallet extension
  const signed = signTransactionOffline(
    built.data,
    params.secretKey,
    client.networkConfig.networkPassphrase,
  );
  if (signed.status === "error") return signed;

  // 3. Submit
  return client.transaction.submit(signed.data);
}

// ── batch account fetch ───────────────────────────────────────────────────────

/**
 * Fetch balances for multiple accounts in parallel. Useful for dashboards or
 * batch reporting where you need the state of many accounts at once.
 */
export async function batchGetAccounts(
  client: SorokitClient,
  publicKeys: string[],
): Promise<SorokitResult<AccountInfo[]>> {
  return client.account.getAccountsBatch(publicKeys);
}
