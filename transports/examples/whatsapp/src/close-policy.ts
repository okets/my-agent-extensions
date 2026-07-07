/**
 * WhatsApp connector close-code policy (B1 — the live reconnect fix).
 *
 * PURE & DEPENDENCY-FREE on purpose: this module imports NOTHING (no baileys, no MCP). It is the
 * single source of truth for "what should the connection.update close handler DO?", so it can be
 * unit-tested without the folder-local node_modules (which only the conformance harness installs).
 * `server.ts` imports `decideClose` and performs the side-effects (reconnect / page / idle).
 *
 * THE BUG IT FIXES (live incident 2026-06-28): the old close handler reconnected ONLY on 515
 * (restartRequired) and routed every real drop (408/428/500/connectionLost/undefined) into a silent
 * `idle` else-branch — no reconnect, no `needs_attention`, no log (pino is `level:'silent'`). A
 * registered session that dropped for any environmental reason just went dark.
 *
 * THE POLICY:
 *   - loggedOut (401) is TERMINAL — never reconnect; page the user (needs_attention) iff there was a
 *     real prior link (an abandoned pairing logout just idles).
 *   - restartRequired (515) — Baileys' post-scan restart that COMPLETES a link → quick reconnect.
 *   - 400 bad-request on an UNREGISTERED socket = companion_hello rejected → preflight_rejected, no
 *     reconnect (a pairing-identity problem, human-actionable).
 *   - everything else (408/428/500/connectionLost/undefined/...) on a REGISTERED-or-previously-linked
 *     session is ENVIRONMENTAL → reconnect with CAPPED EXPONENTIAL BACKOFF, retrying FOREVER. There
 *     is no "give-up after K" verdict here: a network outage must heal whenever the network returns
 *     (the supervisor crash-breaker is a SEPARATE, independent layer for a genuine crash loop). The
 *     cap bounds the spin (never a tight loop); the attempt counter resets on a registered open.
 *   - the same environmental drop on an UNPAIRED socket (never linked) → idle (the pairing window
 *     expired; regeneration is on-demand via onboarding_poll — no perpetual pairing-socket churn).
 */

// ── Baileys DisconnectReason numeric codes (mirrored locally so this module stays import-free) ──
// Source of truth is @whiskeysockets/baileys' DisconnectReason enum; these are its stable HTTP-ish
// status codes. Kept here as plain consts (NOT a TS enum) so Node's type-stripping can run the test.
export const DR_LOGGED_OUT = 401
export const DR_BAD_SESSION = 500
export const DR_RESTART_REQUIRED = 515
export const DR_CONNECTION_LOST = 408
export const DR_CONNECTION_CLOSED = 428

// ── Backoff tuning ──────────────────────────────────────────────────────────────────────────
export const RECONNECT_BASE_MS = 1_000 // first environmental reconnect delay
export const RECONNECT_CAP_MS = 30_000 // capped exponential ceiling (~30-60s band)
export const RESTART_REQUIRED_MS = 250 // 515 link-completion restart: fast, not backed-off

export type CloseDecision =
  | { action: 'preflight_rejected' }
  | { action: 'terminal_logged_out' }
  | { action: 'reconnect'; delayMs: number; cause: 'restart_required' | 'environmental' }
  | { action: 'idle' }

/** The framework `HealthToolOutput.reason` vocabulary (mirrored locally so this module stays
 *  import-free): 'transient' (recoverable in-band drop), 'logged_out' (terminal — needs a human
 *  re-pair, do NOT spin reconnect), 'network' (environmental — recoverable). */
export type CloseHealthReason = 'transient' | 'logged_out' | 'network'

export interface CloseContext {
  /** Baileys lastDisconnect status code; `undefined` when the close carried no code. */
  statusCode: number | undefined
  /** isRealRegistration(state.creds) — creds.registered === true on THIS socket. */
  registered: boolean
  /** A real link has happened at least once this process lifetime (gates pages + reconnect). */
  wasEverLinked: boolean
  /** Consecutive prior ENVIRONMENTAL reconnect attempts (0-based) — drives the backoff delay. */
  attempt: number
  /**
   * A DELIBERATE session release is in flight (the QUIESCE_TOOL set this true right before `sock.end()`).
   * While set, the close handler must NEVER reconnect — the old connector re-grabbing the WhatsApp login
   * would defeat the session-release fence and re-grab the credential inside the RecoveryOrchestrator's
   * re-derive/re-pair window (the live-WhatsApp regression the gate caught). It suppresses RECONNECT only;
   * a genuine terminal logout (401) still pages. Absent ⇒ false (normal reconnect behaviour). */
  releasing?: boolean
}

/** Capped exponential backoff: base * 2^attempt, ceilinged at the cap. Deterministic (no jitter)
 *  so the verdict is testable; `attempt` is clamped non-negative. */
export function backoffDelayMs(attempt: number): number {
  const a = Math.max(0, Math.floor(attempt))
  const exp = RECONNECT_BASE_MS * 2 ** a
  return Math.min(exp, RECONNECT_CAP_MS)
}

/** Decide what the `connection === 'close'` handler should do for a given status code + link state.
 *  Pure: no side-effects, no socket reference — the caller performs the action. */
export function decideClose(ctx: CloseContext): CloseDecision {
  const { statusCode, registered, wasEverLinked } = ctx

  // 400 bad-request while UNREGISTERED = companion_hello rejected (bad client identity). Pairing
  // problem, not an environmental drop → surface preflight_rejected, do NOT reconnect.
  if (statusCode === 400 && !registered) {
    return { action: 'preflight_rejected' }
  }

  // loggedOut (401) is TERMINAL — never reconnect. Page only after a real prior link; an abandoned
  // pairing logout just idles (must not nag the user about a pairing they walked away from). Checked
  // BEFORE the releasing latch so a genuine credential revoke still pages even mid-release.
  if (statusCode === DR_LOGGED_OUT) {
    return wasEverLinked ? { action: 'terminal_logged_out' } : { action: 'idle' }
  }

  // RELEASING LATCH (quiesce session-release fence): a deliberate `sock.end()` is closing this socket
  // so the framework can re-derive/re-pair the login. Go IDLE — never reconnect — so the old connector
  // can't re-grab the credential inside the release window (the live-WhatsApp regression the gate
  // caught). This overrides the 515 fast-restart and the environmental backoff branches below.
  if (ctx.releasing) {
    return { action: 'idle' }
  }

  // restartRequired (515) — Baileys' in-band post-scan restart that COMPLETES the link. Quick
  // reconnect (this finishes pairing; it is not a failure and not backed-off).
  if (statusCode === DR_RESTART_REQUIRED) {
    return { action: 'reconnect', delayMs: RESTART_REQUIRED_MS, cause: 'restart_required' }
  }

  // Everything else (408/428/500/connectionLost/undefined/...) is ENVIRONMENTAL. A real session
  // reconnects FOREVER with capped exponential backoff; an unpaired window just idles.
  if (registered || wasEverLinked) {
    return { action: 'reconnect', delayMs: backoffDelayMs(ctx.attempt), cause: 'environmental' }
  }
  return { action: 'idle' }
}

/**
 * 2a.4 (4): map a close DECISION to the `transport/health` down-reason the connector records as
 * `lastDisconnectReason`. SINGLE SOURCE OF TRUTH (server.ts consumes this instead of an inline
 * switch) so the live blackhole proof can assert it at the pure-function level.
 *
 * The invariant: a PURE connectivity drop (no auth signal — environmental reconnect) → 'network',
 * NEVER 'logged_out'. Reporting 'logged_out' for a mere network drop would page + QR the LIVE bot
 * for a re-pair the user never needs. 'logged_out' is reserved for a genuine auth signal (a real
 * 401 → terminal_logged_out, or a companion_hello identity rejection → preflight_rejected).
 */
export function closeHealthReason(decision: CloseDecision): CloseHealthReason {
  switch (decision.action) {
    case 'terminal_logged_out':
    case 'preflight_rejected':
      return 'logged_out'
    case 'reconnect':
      return decision.cause === 'environmental' ? 'network' : 'transient'
    case 'idle':
      return 'transient'
  }
}
