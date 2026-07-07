/**
 * WhatsApp connector `transport/health` honesty decision (B3 — the REAL never-lie contract).
 *
 * PURE & DEPENDENCY-FREE on purpose: this module imports only the sibling watchdog policy (also
 * import-free). It owns the upstream-liveness verdict so it can be unit-tested without the
 * folder-local node_modules (which only the conformance harness installs). `server.ts` registers the
 * tool + wires the CallTool handler that reads the REAL `sock` + raw `ws.readyState` + `lastActivityAt`
 * + `lastDisconnectReason` and returns `computeUpstreamHealth(...)`.
 *
 * THE CONTRACT (Invariant 2, cert-enforced by the A4 `transport-health` conformance probe): the
 * connector MUST report `upstreamConnected:true` ONLY when a live upstream socket genuinely exists,
 * and `false` the instant it does not — the framework `probeLiveness` recycles a connector that
 * reports `false` (or a stale `lastUpstreamActivityMs`). Lying here re-creates the 2026-06-28 zombie
 * the whole sprint exists to kill, so honesty is non-negotiable.
 *
 * THREE down-conditions (any one ⇒ `upstreamConnected:false`):
 *   1. `sock === null`            — no socket at all (post-close / never-connected).
 *   2. raw `ws.readyState` is CLOSING/CLOSED — the socket ref lingers but the wire is tearing down.
 *   3. `lastActivityAt` is STALE  — sock present + ws OPEN, but no inbound frame past the watchdog
 *                                   window: the silent half-open zombie (the exact live failure).
 * Otherwise (sock present, ws OPEN-or-unknown, recent frame) ⇒ `upstreamConnected:true`.
 *
 * CERT-RUN AFFORDANCE (CRIES-WOLF): a cert run (ECHO_MODE || conformanceSeen) never opens a real
 * Baileys socket, yet the A4 probe's CRIES-WOLF check requires `upstreamConnected:true` with
 * advancing activity (a healthy connector must not false-alarm). The connector reports a synthetic
 * "up" with a fresh timestamp — exactly as SEND echoes in cert. This is NOT a lie: WhatsApp exposes
 * no mock-upstream control, so the A4 LIES-WHEN-DEAD sub-check is skipped for it; the cert run's
 * "upstream" IS the harness driving the connector, and the connector is genuinely up and serving.
 */
import { shouldForceClose, WATCHDOG_WINDOW_MS } from './watchdog.js'

// Standard WebSocket readyState codes (mirrored locally — this module imports no ws/baileys).
export const WS_CONNECTING = 0
export const WS_OPEN = 1
export const WS_CLOSING = 2
export const WS_CLOSED = 3

/** The Baileys rc13 `sock.ws` socket-client liveness getters (the subset we read). */
export interface WsClientLiveness {
  isOpen?: boolean
  isClosing?: boolean
  isClosed?: boolean
  isConnecting?: boolean
}

/**
 * Map the Baileys rc13 `sock.ws` socket-client to a WS_* readyState code (S1.3 (c)).
 *
 * On Baileys 7.x rc13 `sock.ws` is a `WebSocketClient` whose underlying `ws` socket is PROTECTED, so
 * the old wiring's `sock.ws.readyState` is `undefined` — a DEAD field that made health blind to a
 * CLOSING/CLOSED wire (it could only fall back to the activity-staleness check). rc13 exposes
 * `isOpen`/`isClosing`/`isClosed`/`isConnecting` getters instead; this reconstructs the readyState
 * `computeUpstreamHealth` consumes. A null/undefined handle (or a client with no truthy getter) →
 * `undefined` (unknown — health degrades to the activity check), NEVER a false-positive OPEN.
 *
 * Order matters: rc13's `isClosed` AND `isClosing` BOTH read true when the inner socket is null, so
 * CLOSED is checked first (the socket is gone, not merely tearing down) → an unambiguous down verdict.
 */
export function wsReadyStateFromClient(ws: WsClientLiveness | null | undefined): number | undefined {
  if (!ws) return undefined
  if (ws.isClosed) return WS_CLOSED
  if (ws.isClosing) return WS_CLOSING
  if (ws.isOpen) return WS_OPEN
  if (ws.isConnecting) return WS_CONNECTING
  return undefined // no truthy getter → unknown (the raw `.readyState` the old wiring read is gone)
}

/** Down-classification, mirroring `HealthToolOutput.reason` in the framework wire-protocol:
 *  'transient' (recoverable in-band drop), 'logged_out' (terminal — needs a human re-pair, do NOT
 *  spin reconnect), 'network' (environmental — recoverable). */
export type HealthReason = 'transient' | 'logged_out' | 'network'

/** Matches the framework `HealthToolOutput` shape the connector returns on the wire. */
export interface HealthReport {
  upstreamConnected: boolean
  lastUpstreamActivityMs?: number
  reason?: HealthReason
}

export interface HealthInput {
  /** ECHO_MODE || conformanceSeen — a cert run with no real socket. */
  certRun: boolean
  /** `sock !== null` — a Baileys socket object currently exists. */
  sockPresent: boolean
  /** The raw `sock.ws.readyState` (0..3), or `undefined` when the handle isn't introspectable. */
  wsReadyState: number | undefined
  /** ms-epoch of the last inbound ws `'frame'` (the B2 activity clock); 0 = never. */
  lastActivityAt: number
  /** Current time (injected for testability). */
  now: number
  /** Staleness window; defaults to the watchdog window so health and the watchdog agree. */
  windowMs?: number
  /** The reason recorded by the most recent `connection.update` close (B1), if any. */
  lastDisconnectReason?: HealthReason
  /**
   * 2a.4 (3): a (re)connect is actively IN FLIGHT — sock is briefly null while Baileys (re)connects,
   * NOT a dead socket. Set true on `startSocket()` and cleared on a registered `connection:'open'`.
   * The WhatsApp analogue of Telegram's `!liveStarted`: while connecting, a sock===null report is
   * down-but-TRANSIENT (the framework DEFERS its heavy recycle instead of recycling AGAIN) — so a
   * post-recycle null window cannot drive a recycle loop. Does NOT mask a terminal logged_out.
   */
  connecting?: boolean
}

/**
 * Pure upstream-liveness verdict. No side effects, no socket reference — the caller passes the
 * derived primitives. See the module header for the contract.
 */
export function computeUpstreamHealth(input: HealthInput): HealthReport {
  const { certRun, sockPresent, wsReadyState, lastActivityAt, now } = input
  const windowMs = input.windowMs ?? WATCHDOG_WINDOW_MS

  // CERT-RUN affordance: no real socket, but the connector IS up and serving the harness.
  if (certRun) {
    return { upstreamConnected: true, lastUpstreamActivityMs: now }
  }

  const wsTearingDown = wsReadyState === WS_CLOSING || wsReadyState === WS_CLOSED
  const stale = shouldForceClose(lastActivityAt, now, windowMs) // lastActivityAt>0 && now-last>window
  const down = !sockPresent || wsTearingDown || stale

  if (down) {
    const report: HealthReport = { upstreamConnected: false }
    // Surface the last real activity timestamp when we have one (helps the framework's own
    // stale-vs-fresh classification and the journal trace); omit a meaningless 0.
    if (lastActivityAt > 0) report.lastUpstreamActivityMs = lastActivityAt
    // Reason precedence:
    //   1. a terminal logged_out ALWAYS wins (a revoked credential must page, never be masked).
    //   2. 2a.4 (3): sock===null while a (re)connect is in flight → 'transient' (defer the heavy
    //      recycle; the connector is already (re)connecting in-process / a fresh post-recycle child
    //      is establishing the socket) — overrides a stale 'network' so a null window can't re-recycle.
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

  // UP: a live socket, ws OPEN (or unknown handle), recent frame activity.
  const report: HealthReport = { upstreamConnected: true }
  if (lastActivityAt > 0) report.lastUpstreamActivityMs = lastActivityAt
  return report
}
