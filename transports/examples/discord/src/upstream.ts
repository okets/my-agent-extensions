/**
 * Discord connector upstream-liveness policy (push-gateway zombie/half-open backstop).
 *
 * PURE & DEPENDENCY-FREE on purpose: this module imports NOTHING (no MCP, no ws). It owns the
 * watchdog TUNING CONSTANTS + the stale-decision + the never-lie `transport/health` verdict so they
 * can be unit-tested without the folder-local node_modules (only `scripts/setup.sh` installs those).
 * `server.ts` wires the real gateway socket, the frame-activity clock, the heartbeat-ACK check, and
 * the reconnect side-effect; this module holds only PURE decisions.
 *
 * THE GAP IT CLOSES (the WhatsApp live incident 2026-06-28, generalized to a push gateway): a
 * half-open gateway WSS fires NO close event — the connector process stays alive answering MCP pings
 * while its Discord gateway is dead, so the framework keeps reporting `connected`. This is the
 * INDEPENDENT frame-level backstop: it tracks every inbound gateway frame (DISPATCH, HEARTBEAT_ACK,
 * HELLO, …) and forces a close when the socket goes silent past WATCHDOG_WINDOW_MS. The Discord-idiomatic
 * heartbeat-ACK check in server.ts is the FASTER primary (a missed ACK → reconnect within ~1 interval);
 * this frame watchdog is the coarser safety layer beneath it.
 *
 * WINDOW BUDGET (invariant: the connector path must LEAD the framework net). The framework derives
 * discord's net staleThreshold as `max(90s floor, upstream_self_window_ms)`. A `push` upstream has no
 * framework-derived keepalive cadence, so the 90s floor applies. WATCHDOG_WINDOW_MS is set BELOW that
 * (75s) so the connector self-heals FIRST and the framework `probeLiveness`/recycle is only the safety
 * net. It is comfortably ABOVE the Discord gateway heartbeat interval (~41.25s from HELLO) so a single
 * healthy heartbeat-ACK cycle never false-fires the watchdog (75s ≈ 1.8 heartbeat cycles of tolerance).
 */

/** The framework net's staleThreshold for a `push` connector (`max(90s floor, self_window)`; a push
 *  upstream has no derived keepalive cadence, so the 90s floor governs). Mirrored here ONLY so the
 *  test can assert the connector window stays strictly below it (the lead invariant); the framework is
 *  the source of truth for the real value. */
export const NET_STALE_THRESHOLD_MS = 90_000

/** No inbound gateway frame for this long → the socket is presumed a zombie; force-close → reconnect.
 *  MUST stay < NET_STALE_THRESHOLD_MS so the connector leads, and comfortably > the Discord gateway
 *  heartbeat interval (~41.25s) so a single healthy heartbeat-ACK cycle never trips it. This is ALSO
 *  the value exported to the manifest as `upstream_self_window_ms` (the framework couples its threshold
 *  to it via max(floor, this); 75s < 90s floor → the floor wins → the connector leads by design). */
export const WATCHDOG_WINDOW_MS = 75_000

/** How often the watchdog interval re-evaluates staleness (several checks within one window). */
export const WATCHDOG_CHECK_MS = 15_000

/** Standard WebSocket readyState codes (mirrored locally — this module imports no ws). */
export const WS_CONNECTING = 0
export const WS_OPEN = 1
export const WS_CLOSING = 2
export const WS_CLOSED = 3

/**
 * Pure stale-decision. Force a close iff there has been at least one recorded activity timestamp
 * (`lastActivityAt > 0` — the socket reached a live/connected state) AND the gap since the last frame
 * strictly exceeds the window. `lastActivityAt <= 0` (no frame ever) is NOT a zombie — it is a
 * not-yet-open socket, owned by the connect path, not this watchdog.
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
 * connector's OWN upstream watchdog (the frame-level zombie detector AND the heartbeat-ACK reconnect)
 * should be ARMED. Pass `process.env.SELFHEAL_WATCHDOG_OFF`.
 *
 *   SELFHEAL_WATCHDOG_OFF=1  → false (do NOT arm — no connector self-reconnect, so the framework
 *                                      probe→recycle net can be proven independently)
 *   unset / any other value  → true  (arm — the default; the connector self-heals as normal)
 *
 * STRICT literal match on '1' (no trim/coerce). INVARIANT: this gates ARMING ONLY. It MUST NOT change
 * what `transport/health` reports — a dead/half-open upstream still reports `{upstreamConnected:false}`
 * with the watchdog off, so the framework net catches the zombie on its own.
 */
export function watchdogEnabled(flag: string | undefined): boolean {
  return flag !== '1'
}

/** Down-classification, mirroring `HealthToolOutput.reason` in the framework wire-protocol:
 *  'transient' (recoverable in-band drop / reconnect in flight), 'logged_out' (terminal — a revoked
 *  token / disallowed intents, needs a human, do NOT spin reconnect), 'network' (environmental —
 *  recoverable). */
export type HealthReason = 'transient' | 'logged_out' | 'network'

/** Matches the framework `HealthToolOutput` shape the connector returns on the wire. */
export interface HealthReport {
  upstreamConnected: boolean
  lastUpstreamActivityMs?: number
  reason?: HealthReason
}

export interface HealthInput {
  /** ECHO_MODE || conformanceSeen || TRANSPORT_CONFORMANCE — a cert run with no real gateway socket. */
  certRun: boolean
  /** A gateway socket object currently exists (non-null). */
  sockPresent: boolean
  /** The raw ws `readyState` (0..3), or `undefined` when unavailable. */
  wsReadyState: number | undefined
  /** ms-epoch of the last inbound gateway frame (the frame-activity clock); 0 = never. */
  lastActivityAt: number
  /** Current time (injected for testability). */
  now: number
  /** Staleness window; defaults to the watchdog window so health and the watchdog agree. */
  windowMs?: number
  /** The reason recorded by the most recent gateway close (server.ts), if any. */
  lastDisconnectReason?: HealthReason
  /** A (re)connect / RESUME is actively IN FLIGHT — sock briefly null while reconnecting, NOT dead.
   *  Set true on connect start, cleared on READY/RESUMED. Reports a null-sock window as TRANSIENT so a
   *  post-recycle reconnect can't drive a recycle loop. Does NOT mask a terminal logged_out. */
  connecting?: boolean
}

/**
 * Pure upstream-liveness verdict. No side effects, no socket reference — the caller passes the derived
 * primitives.
 *
 * CERT-RUN AFFORDANCE (CRIES-WOLF): a cert run never opens a real gateway socket, yet the A4 probe's
 * CRIES-WOLF check requires `upstreamConnected:true` with advancing activity (a healthy connector must
 * not false-alarm). The connector reports a synthetic "up" with a fresh timestamp — exactly as SEND
 * echoes in cert. This is NOT a lie: Discord exposes no mock-upstream control, so the A4 LIES-WHEN-DEAD
 * sub-check is skipped for it; the cert run's "upstream" IS the harness driving the connector.
 *
 * THREE down-conditions (any one ⇒ `upstreamConnected:false`):
 *   1. `!sockPresent`            — no socket at all (post-close / never-connected).
 *   2. ws readyState CLOSING/CLOSED — the socket ref lingers but the wire is tearing down.
 *   3. `lastActivityAt` STALE   — sock present + ws OPEN, but no inbound frame past the window: the
 *                                 silent half-open zombie.
 */
export function computeGatewayHealth(input: HealthInput): HealthReport {
  const { certRun, sockPresent, wsReadyState, lastActivityAt, now } = input
  const windowMs = input.windowMs ?? WATCHDOG_WINDOW_MS

  if (certRun) {
    return { upstreamConnected: true, lastUpstreamActivityMs: now }
  }

  const wsTearingDown = wsReadyState === WS_CLOSING || wsReadyState === WS_CLOSED
  const stale = shouldForceClose(lastActivityAt, now, windowMs)
  const down = !sockPresent || wsTearingDown || stale

  if (down) {
    const report: HealthReport = { upstreamConnected: false }
    if (lastActivityAt > 0) report.lastUpstreamActivityMs = lastActivityAt
    // Reason precedence:
    //   1. a terminal logged_out ALWAYS wins (a revoked token / disallowed intents must page).
    //   2. a (re)connect in flight while sock===null → 'transient' (defer the heavy recycle).
    //   3. otherwise the last recorded disconnect cause; a present-but-stale socket with no recorded
    //      close is a silent half-open = environmental ('network').
    const reason: HealthReason | undefined =
      input.lastDisconnectReason === 'logged_out'
        ? 'logged_out'
        : !sockPresent && input.connecting
          ? 'transient'
          : input.lastDisconnectReason ?? (sockPresent && stale ? 'network' : undefined)
    if (reason) report.reason = reason
    return report
  }

  const report: HealthReport = { upstreamConnected: true }
  if (lastActivityAt > 0) report.lastUpstreamActivityMs = lastActivityAt
  return report
}
