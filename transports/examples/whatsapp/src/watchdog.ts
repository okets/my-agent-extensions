/**
 * WhatsApp connector zombie-watchdog policy (B2 — the silent half-open backstop, REPAIRED).
 *
 * PURE & DEPENDENCY-FREE on purpose: this module imports NOTHING (no baileys, no MCP). It owns the
 * watchdog TUNING CONSTANTS + the stale-decision so they can be unit-tested without the folder-local
 * node_modules (only the conformance harness installs those). `server.ts` wires the inbound-frame
 * listener + the interval and performs the side-effect (force a close → the B1 reconnect path).
 *
 * THE GAP IT CLOSES (live incident 2026-06-28): a half-open upstream socket fires NO
 * `connection.update` — the connector process stays alive answering MCP pings while its provider WSS
 * is dead, so the framework keeps reporting `connected`. Baileys' OWN keepalive watchdog can wedge
 * (it did, for ~8h). This is an INDEPENDENT backstop (invariant #6, breaker-independent): it tracks
 * the lowest-level activity signal — every inbound ws `'frame'` (invariant #7, frame-level activity)
 * — and forces a close when the socket goes silent past WATCHDOG_WINDOW_MS.
 *
 * WINDOW BUDGET (invariant: the connector path must LEAD the framework net): the framework derives
 * whatsapp's net staleThreshold as `max(90s, keepaliveMs × 3)` with `keepaliveMs = 30s` for an
 * `upstream: socket` connector → 90s. WATCHDOG_WINDOW_MS is deliberately BELOW that (60s) so the
 * connector self-heals FIRST and the framework `probeLiveness`/recycle is only the safety layer. It
 * is comfortably ABOVE KEEPALIVE_INTERVAL_MS (15s) so a couple of missed keepalive cycles (and the
 * frames they elicit) never false-fire the watchdog.
 */

/** The framework net's staleThreshold for an `upstream: socket` connector (`max(90s, 30s × 3)`).
 *  Mirrored here ONLY so the test can assert the window stays strictly below it (the lead invariant);
 *  the framework is the source of truth for the real value. */
export const NET_STALE_THRESHOLD_MS = 90_000

/** Tighter than Baileys' 30s default so the keepalive ping cadence — and thus the inbound frame
 *  cadence on a healthy socket — is brisk; the watchdog window spans several of these. */
export const KEEPALIVE_INTERVAL_MS = 15_000

/** No inbound frame for this long → the socket is presumed a zombie; force-close → B1 reconnect.
 *  MUST stay < NET_STALE_THRESHOLD_MS so the connector leads, and > KEEPALIVE_INTERVAL_MS so a
 *  single missed keepalive cycle never trips it. */
export const WATCHDOG_WINDOW_MS = 60_000

/** How often the watchdog interval re-evaluates staleness (several checks within one window). */
export const WATCHDOG_CHECK_MS = 15_000

/**
 * Pure stale-decision. Force a close iff there has been at least one recorded activity timestamp
 * (`lastActivityAt > 0` — the socket reached a live/connected state) AND the gap since the last
 * frame strictly exceeds the window. `lastActivityAt <= 0` (no frame ever) is NOT a zombie — it is a
 * not-yet-open socket, owned by the connect path / Baileys' connectTimeout, not this watchdog.
 */
export function shouldForceClose(
  lastActivityAt: number,
  now: number,
  windowMs: number = WATCHDOG_WINDOW_MS,
): boolean {
  if (lastActivityAt <= 0) return false
  return now - lastActivityAt > windowMs
}

/**
 * KILL-SWITCH decision (A6 — `SELFHEAL_WATCHDOG_OFF`, _CONTRACT.md §Kill-switch). Returns whether the
 * connector's OWN upstream watchdog should be ARMED. Pass `process.env.SELFHEAL_WATCHDOG_OFF`.
 *
 *   SELFHEAL_WATCHDOG_OFF=1  → false (do NOT arm the watchdog — no connector self-reconnect, so the
 *                                      framework probe→recycle net can be proven independently)
 *   unset / any other value  → true  (arm — the default; the connector self-heals as normal)
 *
 * STRICT literal match on '1' (no trim/coerce), mirroring the contract's
 * `process.env.SELFHEAL_WATCHDOG_OFF === '1'`. The literal env-var NAME is inlined at the read site in
 * server.ts (connectors do NOT import framework modules; watchdog.ts is the connector's own module).
 *
 * INVARIANT: this gates ARMING ONLY. It MUST NOT change what `transport/health` reports — a dead /
 * half-open upstream still reports `{ upstreamConnected: false }` with the watchdog off, so the
 * framework net catches the zombie on its own.
 */
export function watchdogEnabled(flag: string | undefined): boolean {
  return flag !== '1'
}
