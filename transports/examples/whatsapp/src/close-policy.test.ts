/**
 * Focused logic test for the WhatsApp connector close-code switch (B1).
 *
 * This connector runs via tsx with FOLDER-LOCAL node_modules that are only installed by the
 * conformance harness, so a vitest-in-folder unit test isn't available. The close-decision logic is
 * therefore extracted into the dependency-free `close-policy.ts` (no baileys / no MCP import) and
 * exercised here as a pure assertion script. Run with a tsx binary, e.g.:
 *   ../../../packages/core/node_modules/.bin/tsx src/close-policy.test.ts
 * Exits non-zero on the first failed assertion.
 */
import {
  decideClose,
  closeHealthReason,
  backoffDelayMs,
  RECONNECT_BASE_MS,
  RECONNECT_CAP_MS,
  RESTART_REQUIRED_MS,
  DR_LOGGED_OUT,
  DR_RESTART_REQUIRED,
  DR_CONNECTION_LOST,
  DR_CONNECTION_CLOSED,
  DR_BAD_SESSION,
} from './close-policy.js'

let failures = 0
function check(name: string, cond: boolean): void {
  if (cond) {
    console.log(`  ok  ${name}`)
  } else {
    failures++
    console.error(`FAIL  ${name}`)
  }
}
function eq(name: string, actual: unknown, expected: unknown): void {
  check(`${name} (got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)})`, JSON.stringify(actual) === JSON.stringify(expected))
}

// ── TERMINAL: loggedOut (401) on a real prior link → page, NEVER reconnect ──────────────────────
eq('loggedOut + wasEverLinked → terminal_logged_out',
  decideClose({ statusCode: DR_LOGGED_OUT, registered: true, wasEverLinked: true, attempt: 0 }).action,
  'terminal_logged_out')
eq('loggedOut DR value is 401', DR_LOGGED_OUT, 401)

// loggedOut while never linked = abandoned pairing → idle, not a page, not a reconnect.
eq('loggedOut + !wasEverLinked → idle',
  decideClose({ statusCode: DR_LOGGED_OUT, registered: false, wasEverLinked: false, attempt: 0 }).action,
  'idle')

// ── ENVIRONMENTAL: 408 / 428 / 500 / connectionLost / undefined on a registered session → reconnect ──
for (const code of [DR_CONNECTION_LOST, DR_CONNECTION_CLOSED, DR_BAD_SESSION, 408, 428, 500, undefined]) {
  const d = decideClose({ statusCode: code, registered: true, wasEverLinked: true, attempt: 0 })
  eq(`statusCode=${String(code)} (registered) → reconnect`, d.action, 'reconnect')
  if (d.action === 'reconnect') eq(`statusCode=${String(code)} cause=environmental`, d.cause, 'environmental')
}

// wasEverLinked alone (creds re-derive race could read registered=false momentarily) still reconnects.
eq('undefined + wasEverLinked(registered:false) → reconnect',
  decideClose({ statusCode: undefined, registered: false, wasEverLinked: true, attempt: 0 }).action,
  'reconnect')

// An UNPAIRED window dropping (not registered, never linked) → idle, NOT a reconnect spin.
eq('408 + unpaired (never linked) → idle',
  decideClose({ statusCode: 408, registered: false, wasEverLinked: false, attempt: 0 }).action,
  'idle')

// ── restartRequired (515): Baileys' post-scan link-completion restart → quick reconnect ─────────
{
  const d = decideClose({ statusCode: DR_RESTART_REQUIRED, registered: false, wasEverLinked: false, attempt: 0 })
  eq('515 → reconnect', d.action, 'reconnect')
  if (d.action === 'reconnect') {
    eq('515 cause=restart_required', d.cause, 'restart_required')
    eq('515 delay is the fast restart delay', d.delayMs, RESTART_REQUIRED_MS)
  }
}

// ── 400 bad-request during an UNREGISTERED pairing attempt → preflight_rejected, no reconnect ────
eq('400 + !registered → preflight_rejected',
  decideClose({ statusCode: 400, registered: false, wasEverLinked: false, attempt: 0 }).action,
  'preflight_rejected')
// A 400 on a registered session is treated as environmental (reconnect), not pairing-rejection.
eq('400 + registered → reconnect',
  decideClose({ statusCode: 400, registered: true, wasEverLinked: true, attempt: 0 }).action,
  'reconnect')

// ── Capped exponential backoff: grows by attempt, NEVER exceeds the cap, retries FOREVER ────────
eq('backoff attempt 0 = base', backoffDelayMs(0), RECONNECT_BASE_MS)
eq('backoff attempt 1 = 2x base', backoffDelayMs(1), RECONNECT_BASE_MS * 2)
eq('backoff attempt 2 = 4x base', backoffDelayMs(2), RECONNECT_BASE_MS * 4)
check('backoff is monotonic non-decreasing', backoffDelayMs(3) >= backoffDelayMs(2))
check('backoff caps at RECONNECT_CAP_MS', backoffDelayMs(50) === RECONNECT_CAP_MS)
check('cap is in the ~30-60s band', RECONNECT_CAP_MS >= 30_000 && RECONNECT_CAP_MS <= 60_000)
// "forever": even at a huge attempt count there is still a (capped) reconnect, never a give-up verdict.
{
  const d = decideClose({ statusCode: 408, registered: true, wasEverLinked: true, attempt: 9999 })
  eq('huge attempt still reconnects (forever)', d.action, 'reconnect')
  if (d.action === 'reconnect') eq('huge-attempt delay is capped', d.delayMs, RECONNECT_CAP_MS)
}

// The environmental reconnect delay tracks the attempt counter.
{
  const d = decideClose({ statusCode: 408, registered: true, wasEverLinked: true, attempt: 2 })
  if (d.action === 'reconnect') eq('environmental delay = backoff(attempt)', d.delayMs, backoffDelayMs(2))
}

// ── RELEASING LATCH (quiesce session-release fence): while a deliberate session release is in flight
// the close handler must NEVER reconnect — the old connector re-grabbing the login would defeat the
// RecoveryOrchestrator re-derive/re-pair window. `releasing` forces idle, overriding every other code. ─
eq('releasing + environmental (registered) → idle, NOT reconnect',
  decideClose({ statusCode: 408, registered: true, wasEverLinked: true, attempt: 0, releasing: true }).action,
  'idle')
eq('releasing + undefined code (the sock.end() close) → idle, NOT reconnect',
  decideClose({ statusCode: undefined, registered: true, wasEverLinked: true, attempt: 0, releasing: true }).action,
  'idle')
// releasing overrides EVEN the fast 515 restart path — a release must not be undone by a link-restart.
eq('releasing + 515 → idle (override restart_required)',
  decideClose({ statusCode: DR_RESTART_REQUIRED, registered: true, wasEverLinked: true, attempt: 0, releasing: true }).action,
  'idle')
// CONTROL: the SAME environmental close while NOT releasing still reconnects normally (no regression).
eq('NOT releasing + environmental (registered) → reconnect',
  decideClose({ statusCode: 408, registered: true, wasEverLinked: true, attempt: 0, releasing: false }).action,
  'reconnect')
// loggedOut is still terminal even mid-release (a revoked credential must page, never silently idle as
// a routine release) — releasing only suppresses RECONNECT, it does not mask a terminal logout page.
eq('releasing + loggedOut(linked) → still terminal_logged_out',
  decideClose({ statusCode: DR_LOGGED_OUT, registered: true, wasEverLinked: true, attempt: 0, releasing: true }).action,
  'terminal_logged_out')

// ── 2a.4 (4): a PURE connectivity DROP (no auth signal) maps to the 'network' down-reason, NEVER
// 'logged_out'. The blackhole live-proof asserts reason==='network'; a 'logged_out' here would page
// + QR the LIVE bot for what is only an environmental drop (a re-pair the user never needs). ─────────
for (const code of [DR_CONNECTION_LOST, DR_CONNECTION_CLOSED, DR_BAD_SESSION, 408, 428, 500, undefined]) {
  const d = decideClose({ statusCode: code, registered: true, wasEverLinked: true, attempt: 0 })
  eq(`pure drop statusCode=${String(code)} → health reason 'network'`, closeHealthReason(d), 'network')
  check(`pure drop statusCode=${String(code)} is NEVER 'logged_out'`, closeHealthReason(d) !== 'logged_out')
}
// A genuine terminal 401 still maps to 'logged_out' (the auth signal IS present) — the guard is
// specifically "no auth signal → never logged_out", not "never logged_out at all".
eq('terminal 401 (auth signal present) → health reason logged_out',
  closeHealthReason(decideClose({ statusCode: DR_LOGGED_OUT, registered: true, wasEverLinked: true, attempt: 0 })),
  'logged_out')
// The 515 link-completion restart is transient, not an environmental network drop.
eq('515 restart → health reason transient',
  closeHealthReason(decideClose({ statusCode: DR_RESTART_REQUIRED, registered: true, wasEverLinked: true, attempt: 0 })),
  'transient')

if (failures > 0) {
  console.error(`\n${failures} assertion(s) FAILED`)
  process.exit(1)
}
console.log('\nall close-policy assertions passed')
