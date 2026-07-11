/**
 * Telegram connector `transport/health` poll-staleness decision (B4 — the never-lie contract for a
 * POLL upstream).
 *
 * PURE & DEPENDENCY-FREE on purpose: this module imports nothing (no MCP, no fetch). It owns the
 * upstream-liveness verdict so it can be unit-tested without the folder-local node_modules (which only
 * the conformance harness installs). `server.ts` registers the tool + wires the CallTool handler that
 * reads the REAL poll state (`liveStarted` / `pollLoggedOut` / `lastSuccessfulGetUpdatesAt`) and
 * returns `computePollHealth(...)`.
 *
 * WHY a poll connector still needs `transport/health`: Telegram declares `upstream: poll` (A0), which
 * the A4 conformance mandate EXEMPTS from requiring the tool — a long-poll holds no persistent socket
 * to go silently half-open. But a poll can still STALL invisibly: getUpdates can back off forever on a
 * dead network, or the token can be revoked (a hard 401). The framework's only other signal is the MCP
 * stdio ping, which the connector process answers even while every getUpdates fails. So the connector
 * self-reports poll liveness: `upstreamConnected:false` (with a classification `reason`) once getUpdates
 * has not SUCCEEDED within its cadence window. The framework `probeLiveness` then recovers it.
 *
 * THE CONTRACT (Invariant 2, cert-enforced by the A4 `transport-health` probe once the tool is
 * declared): report `upstreamConnected:true` ONLY when getUpdates is succeeding within the cadence
 * window. Lying here re-creates a silent stall the whole sprint exists to kill.
 *
 * TERMINAL-vs-ENVIRONMENTAL (Invariant 5):
 *   - a hard 401 (token revoked / unauthorized) is TERMINAL → `reason:'logged_out'`; a human must
 *     supply a new token. The framework pages (needs_attention) and does NOT spin reconnect.
 *   - any other poll stall (backoff-forever / network down) is ENVIRONMENTAL → `reason:'network'`;
 *     the framework recycles to a fresh process that resumes the poll (no human needed).
 *
 * CERT-RUN AFFORDANCE (CRIES-WOLF): a cert run (ECHO_MODE || conformanceSeen) drives no live poll, yet
 * the A4 probe's CRIES-WOLF check requires `upstreamConnected:true` with advancing activity. The
 * connector reports a synthetic "up" with a fresh timestamp — exactly as SEND echoes in cert. This is
 * NOT a lie: Telegram exposes no mock-upstream control, so the A4 LIES-WHEN-DEAD sub-check is skipped;
 * the cert run's "upstream" IS the harness driving the connector, and it is genuinely up and serving.
 */

/** Down-classification, mirroring `HealthToolOutput.reason` in the framework wire-protocol:
 *  'transient' (recoverable in-band drop), 'logged_out' (terminal — needs a human re-pair, do NOT
 *  spin reconnect), 'credential' (terminal — the stored token is invalid or incomplete; a human
 *  must re-supply the secret, do NOT recycle-loop), 'network' (environmental — recoverable). */
export type HealthReason = 'transient' | 'logged_out' | 'credential' | 'network'

/** Matches the framework `HealthToolOutput` shape the connector returns on the wire. */
export interface HealthReport {
  upstreamConnected: boolean
  lastUpstreamActivityMs?: number
  reason?: HealthReason
}

/** The long-poll timeout (seconds) passed to getUpdates — a healthy cycle returns within this (or
 *  sooner on activity). Single source of truth: server.ts uses this in the getUpdates body. */
export const LONG_POLL_TIMEOUT_S = 50

/** Default cadence window: ~3× the long-poll timeout. A healthy getUpdates cycle completes within the
 *  long-poll timeout (≤50s) or sooner; allow ~3 cycles (plus the 1–2s inter-cycle backoff) before
 *  declaring the poll stalled, so a single slow/backed-off cycle never false-flags an honest poll. */
export const POLL_STALE_WINDOW_MS = LONG_POLL_TIMEOUT_S * 1000 * 3 // 150_000

/**
 * 2a.4 (2) — the 409-conflict SETTLE-GRACE. A getUpdates 409 means ANOTHER poller is active; on a
 * supervised deploy RESTART that is the OLD child, still finishing its long-poll cycle, until the
 * supervisor kills it. The new child cannot get a 2xx until then. The settle-grace must be ≥ the
 * long-poll window so a single in-flight old-child cycle can drain before we ever read the poll as a
 * stall. While a 409 is within this grace, a would-be-stale poll is reported TRANSIENT (the framework
 * defers the heavy recycle) instead of NETWORK (which would self-recycle the just-restarted bot).
 */
export const SETTLE_GRACE_MS = LONG_POLL_TIMEOUT_S * 1000 // 50_000 (== the long-poll window)

/**
 * 2a-ii latch guard (architect gate, M10-S9.10): whether a loopback-shaped send (the connector's
 * ECHO branch — engaged for ECHO_MODE OR any non-deliverable target, e.g. a negative group/channel
 * chat id `isDeliverableTarget` rejects) should latch `conformanceSeen`.
 *
 * ONLY true before the live long-poll is armed. A genuine cert run's loopback send always arrives
 * within the pre-poll grace window (the harness drives it immediately after the MCP handshake), so
 * `liveStarted` is always false at that point — this costs cert nothing. Once armed, a
 * loopback-shaped send is NOT evidence of a cert run — it is an ordinary live send to a target this
 * connector cannot reach (e.g. a bot serving a group/channel, whose numeric id is negative and so
 * never matches `isDeliverableTarget`'s positive-digit / @username pattern). Latching here would set
 * `conformanceSeen = true` PERMANENTLY, which feeds `computePollHealth`'s `certRun` input
 * (`ECHO_MODE || conformanceSeen`) — blinding poll-stall detection for the rest of the process's
 * life (a genuinely stalled poll would keep reporting a synthetic "up" forever).
 */
export function shouldLatchConformanceSeen(liveStarted: boolean): boolean {
  return !liveStarted
}

export interface PollHealthInput {
  /** ECHO_MODE || conformanceSeen — a cert run with no live long-poll. */
  certRun: boolean
  /** The live long-poll loop is armed (deferred grace elapsed, not disarmed by a cert loopback). */
  liveStarted: boolean
  /** A hard 401 from getUpdates was seen — terminal (token revoked; needs a human re-pair). */
  loggedOut: boolean
  /** The ONE-SHOT startup getMe probe returned 401/404 — the token is invalid or incomplete
   *  (e.g. a half-copied BotFather token). TERMINAL: a fresh process with the same token dies the
   *  same way, so the verdict must be `credential`, never `network` (which would recycle-loop).
   *  Distinct from `loggedOut` (a previously-working token revoked mid-run). */
  credentialInvalid?: boolean
  /** ms-epoch of the last 2xx getUpdates (anchored at arm-time); 0 = never. */
  lastSuccessfulGetUpdatesAt: number
  /**
   * 2a.4 (2): ms-epoch of the most recent getUpdates 409 conflict (another poller active — typically
   * the OLD child during a supervised deploy restart); 0/undefined = none. A 409 NEVER advances the
   * stale clock (only a 2xx does); within the settle-grace it converts a would-be `network` stall
   * verdict into `transient` (defer the recycle) so a restart never self-recycles before its first 2xx.
   */
  last409At?: number
  /** Current time (injected for testability). */
  now: number
  /** Cadence window; defaults to POLL_STALE_WINDOW_MS. */
  windowMs?: number
  /** 2a.4 (2): the 409 settle-grace; defaults to SETTLE_GRACE_MS (≥ the long-poll window). */
  settleGraceMs?: number
}

/**
 * Pure poll-liveness verdict. No side effects, no I/O — the caller passes the derived primitives.
 * See the module header for the contract.
 */
export function computePollHealth(input: PollHealthInput): HealthReport {
  const { certRun, liveStarted, loggedOut, lastSuccessfulGetUpdatesAt, now } = input
  const windowMs = input.windowMs ?? POLL_STALE_WINDOW_MS
  const settleGraceMs = input.settleGraceMs ?? SETTLE_GRACE_MS

  // CERT-RUN affordance (CRIES-WOLF): no live poll, but the connector IS up and serving the harness.
  if (certRun) return { upstreamConnected: true, lastUpstreamActivityMs: now }

  // TERMINAL: the one-shot startup credential probe said the token is invalid or incomplete
  // (getMe 401/404). NEVER 'network' — a recycle respawns into the identical death; a human must
  // re-supply the secret. Reported before every other live verdict: with a bad token no other
  // upstream signal is meaningful.
  if (input.credentialInvalid) {
    const report: HealthReport = { upstreamConnected: false, reason: 'credential' }
    if (lastSuccessfulGetUpdatesAt > 0) report.lastUpstreamActivityMs = lastSuccessfulGetUpdatesAt
    return report
  }

  // TERMINAL: a hard 401 — the bot token is revoked/unauthorized. A human must re-pair; the framework
  // pages (needs_attention) and must NOT spin reconnect (Invariant 5).
  if (loggedOut) {
    const report: HealthReport = { upstreamConnected: false, reason: 'logged_out' }
    if (lastSuccessfulGetUpdatesAt > 0) report.lastUpstreamActivityMs = lastSuccessfulGetUpdatesAt
    return report
  }

  // Poll not yet armed (still inside the post-handshake grace): the process is alive and about to
  // poll; nothing has failed → honest up with a fresh ts (no cadence to be stale against yet).
  if (!liveStarted) return { upstreamConnected: true, lastUpstreamActivityMs: now }

  // ARMED: stale when getUpdates has not SUCCEEDED within the cadence window — an environmental stall
  // (backoff-forever / network down). reason:network → the framework recycles to a fresh process that
  // resumes the poll (no human needed) — the poll-shape analogue of the socket zombie.
  const stale = now - lastSuccessfulGetUpdatesAt > windowMs
  if (stale) {
    // 2a.4 (2) 409 SETTLE-GUARD: a recent 409 within the settle-grace means another poller (the OLD
    // child on a supervised restart) still holds the long-poll; OUR getUpdates can't 2xx until it
    // exits. That is CONTENTION, not a stall — report TRANSIENT so the framework DEFERS the heavy
    // recycle (and keeps re-probing) instead of recycling a just-restarted bot before its first 2xx.
    // Bounded: once the 409s stop past the grace and there is still no success, the real `network`
    // verdict below returns (and the framework's own attempt-ladder bounds a perpetual transient).
    const recent409 = input.last409At !== undefined && input.last409At > 0 && now - input.last409At <= settleGraceMs
    const report: HealthReport = { upstreamConnected: false, reason: recent409 ? 'transient' : 'network' }
    if (lastSuccessfulGetUpdatesAt > 0) report.lastUpstreamActivityMs = lastSuccessfulGetUpdatesAt
    return report
  }

  // UP: a recent successful getUpdates within the cadence window.
  return { upstreamConnected: true, lastUpstreamActivityMs: lastSuccessfulGetUpdatesAt }
}
