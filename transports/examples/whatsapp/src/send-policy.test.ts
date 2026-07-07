/**
 * Focused logic test for the WhatsApp connector outbound send-routing decision (S1.3 (a)+(b)).
 *
 * Like watchdog.test.ts / close-policy.test.ts / health.test.ts, the connector runs via tsx with
 * FOLDER-LOCAL node_modules, so the routing DECISION is extracted into the dependency-free
 * `send-policy.ts` (no baileys / no MCP import) and exercised here as a pure assertion script:
 *   ../../../packages/core/node_modules/.bin/tsx src/send-policy.test.ts
 * Exits non-zero on the first failed assertion.
 *
 * THE CONTRACT this proves (the live-poisoning fix):
 *   - The ECHO/loopback route fires ONLY on an EXPLICIT cert signal (certRun), NEVER inferred from
 *     the target shape. A LIVE send (certRun:false) to a NON-deliverable target is DROPPED loudly —
 *     it must NOT echo a fake inbound, and (server-side) must NOT latch conformanceSeen, which would
 *     poison a live session into cert mode. (S1.3 (a).)
 *   - A non-deliverable LIVE target → 'drop_non_deliverable' (the server logs loudly + never reports
 *     'sent'). (S1.3 (b).)
 */
import { decideSendRoute } from './send-policy.js'

let failures = 0
function eq(name: string, actual: unknown, expected: unknown): void {
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) console.log(`  ok  ${name}`)
  else { failures++; console.error(`FAIL  ${name} (got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)})`) }
}

// ── CERT RUN: always loopback-echo, REGARDLESS of target shape ───────────────────────────────────
eq(
  'cert run + deliverable target → echo (loopback cert path)',
  decideSendRoute({ certRun: true, deliverable: true }),
  'echo',
)
eq(
  'cert run + non-deliverable synthetic target (e.g. "probe") → echo (the harness drives synthetic targets)',
  decideSendRoute({ certRun: true, deliverable: false }),
  'echo',
)

// ── LIVE RUN: NEVER echo. The fix — a non-deliverable target must DROP, not echo as fake inbound ──
eq(
  'LIVE + non-deliverable target → drop_non_deliverable (NOT echo — the live-poisoning fix)',
  decideSendRoute({ certRun: false, deliverable: false }),
  'drop_non_deliverable',
)
eq(
  'LIVE + deliverable JID → live (real send)',
  decideSendRoute({ certRun: false, deliverable: true }),
  'live',
)

if (failures > 0) {
  console.error(`\n${failures} assertion(s) FAILED`)
  process.exit(1)
}
console.log('\nall send-policy assertions passed')
