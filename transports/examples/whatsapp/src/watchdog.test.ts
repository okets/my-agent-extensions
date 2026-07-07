/**
 * Focused logic test for the WhatsApp connector zombie-watchdog policy (B2, REPAIRED).
 *
 * This connector runs via tsx with FOLDER-LOCAL node_modules that are only installed by the
 * conformance harness, so a vitest-in-folder unit test isn't available. The watchdog stale-decision
 * + tuning constants are therefore extracted into the dependency-free `watchdog.ts` (no baileys / no
 * MCP import) and exercised here as a pure assertion script. Run with a tsx binary, e.g.:
 *   ../../../packages/core/node_modules/.bin/tsx src/watchdog.test.ts
 * Exits non-zero on the first failed assertion.
 *
 * The server-side wiring (the ws `'frame'` listener that bumps lastActivityAt, the interval that
 * calls shouldForceClose and force-closes via `sock.ws.close()`, the ECHO/cert guard, the
 * QUIESCE/SIGTERM clears) is verified by reading the source — it cannot be unit-tested without the
 * real Baileys socket. This script proves the pure decision + the WINDOW BUDGET invariant.
 */
import {
  shouldForceClose,
  watchdogEnabled,
  KEEPALIVE_INTERVAL_MS,
  WATCHDOG_WINDOW_MS,
  WATCHDOG_CHECK_MS,
  NET_STALE_THRESHOLD_MS,
} from './watchdog.js'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

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

const NOW = 1_000_000_000

// ── NEVER fire before the socket has had any activity (lastActivityAt <= 0 = not-yet-open) ────────
eq('lastActivityAt=0 (never any frame) → no force-close', shouldForceClose(0, NOW, WATCHDOG_WINDOW_MS), false)
eq('lastActivityAt<0 → no force-close', shouldForceClose(-1, NOW, WATCHDOG_WINDOW_MS), false)

// ── A fresh frame keeps the socket alive ─────────────────────────────────────────────────────────
eq('frame 1s ago → alive (no force-close)', shouldForceClose(NOW - 1_000, NOW, WATCHDOG_WINDOW_MS), false)
eq('frame just under the window → alive', shouldForceClose(NOW - (WATCHDOG_WINDOW_MS - 1), NOW, WATCHDOG_WINDOW_MS), false)
eq('frame exactly at the window boundary → not yet (strictly greater required)', shouldForceClose(NOW - WATCHDOG_WINDOW_MS, NOW, WATCHDOG_WINDOW_MS), false)

// ── A silent socket past the window IS a zombie → force-close → B1 reconnect ──────────────────────
eq('frame just past the window → force-close', shouldForceClose(NOW - (WATCHDOG_WINDOW_MS + 1), NOW, WATCHDOG_WINDOW_MS), true)
eq('frame long past the window (8h, the live incident) → force-close', shouldForceClose(NOW - 8 * 3600_000, NOW, WATCHDOG_WINDOW_MS), true)

// Default-window overload behaves the same as the explicit window.
eq('default window is used when omitted', shouldForceClose(NOW - (WATCHDOG_WINDOW_MS + 1), NOW), true)

// ── KILL-SWITCH: SELFHEAL_WATCHDOG_OFF (A6 — gates ARMING only, never transport/health) ───────────
// SELFHEAL_WATCHDOG_OFF=1 → connector self-heal watchdog NOT armed (so the framework net can be
// proven in isolation). Any other value (incl. unset) → armed (the default).
eq('SELFHEAL_WATCHDOG_OFF=1 → watchdog NOT armed', watchdogEnabled('1'), false)
eq('unset (undefined) → watchdog armed (default)', watchdogEnabled(undefined), true)
eq("'0' → watchdog armed", watchdogEnabled('0'), true)
eq("empty string → watchdog armed", watchdogEnabled(''), true)
eq("any other value ('true') → watchdog armed (only the literal '1' disables)", watchdogEnabled('true'), true)
eq("whitespace ' 1 ' → watchdog armed (strict literal match, no trim)", watchdogEnabled(' 1 '), true)

// ── WINDOW BUDGET INVARIANTS (the lead ordering + no-false-fire) ──────────────────────────────────
check('window < net staleThreshold (connector self-heal LEADS the framework net)', WATCHDOG_WINDOW_MS < NET_STALE_THRESHOLD_MS)
check('window > keepalive interval (a missed keepalive cycle never false-fires)', WATCHDOG_WINDOW_MS > KEEPALIVE_INTERVAL_MS)
check('window spans multiple keepalive cycles (≥3)', WATCHDOG_WINDOW_MS >= KEEPALIVE_INTERVAL_MS * 3)
check('keepalive tighter than the Baileys 30s default', KEEPALIVE_INTERVAL_MS < 30_000)
check('check cadence ≤ keepalive (watchdog evaluates at least once per keepalive cycle)', WATCHDOG_CHECK_MS <= KEEPALIVE_INTERVAL_MS)
check('net staleThreshold mirror is the 90s upstream:socket value', NET_STALE_THRESHOLD_MS === 90_000)

// ── 2a.2 CADENCE COUPLING: the manifest's exported self-window MUST equal WATCHDOG_WINDOW_MS ─────────
// The framework derives its staleThreshold from CAPABILITY.md's `upstream_self_window_ms`. Pinning it
// to WATCHDOG_WINDOW_MS keeps the framework window matched to the connector's real self-heal window
// (and, via max(90s floor, 60s)=90s, preserves the connector-LEADS-the-net ordering above).
{
  const capPath = fileURLToPath(new URL('../CAPABILITY.md', import.meta.url))
  const md = readFileSync(capPath, 'utf8')
  const m = md.match(/^upstream_self_window_ms:\s*(\d+)\s*$/m)
  const declared = m ? Number(m[1]) : NaN
  eq('CAPABILITY.md upstream_self_window_ms === WATCHDOG_WINDOW_MS', declared, WATCHDOG_WINDOW_MS)
}

if (failures > 0) {
  console.error(`\n${failures} assertion(s) FAILED`)
  process.exit(1)
}
console.log('\nall watchdog assertions passed')
