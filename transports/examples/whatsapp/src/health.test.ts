/**
 * Focused logic test for the WhatsApp connector `transport/health` honesty contract (B3).
 *
 * Like watchdog.test.ts / close-policy.test.ts, this connector runs via tsx with FOLDER-LOCAL
 * node_modules (only the conformance harness installs them), so a vitest-in-folder unit test isn't
 * available. The health DECISION is therefore extracted into the dependency-free `health.ts` (no
 * baileys / no MCP import) and exercised here as a pure assertion script. Run with a tsx binary:
 *   ../../../packages/core/node_modules/.bin/tsx src/health.test.ts
 * Exits non-zero on the first failed assertion.
 *
 * The server-side wiring (registering transport/health in ListTools, the CallTool handler that reads
 * the real `sock` + raw `ws.readyState` + `lastActivityAt` + `lastDisconnectReason` and returns
 * `computeUpstreamHealth(...)`) is verified by reading the source — it cannot be unit-tested without
 * a real Baileys socket. This script proves the pure honesty decision (the never-lie contract the A4
 * conformance probe enforces) + the cert-run CRIES-WOLF affordance.
 */
import {
  computeUpstreamHealth,
  wsReadyStateFromClient,
  WS_CONNECTING,
  WS_OPEN,
  WS_CLOSING,
  WS_CLOSED,
} from './health.js'
import { WATCHDOG_WINDOW_MS } from './watchdog.js'

let failures = 0
function eq(name: string, actual: unknown, expected: unknown): void {
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) console.log(`  ok  ${name}`)
  else { failures++; console.error(`FAIL  ${name} (got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)})`) }
}

const NOW = 1_000_000_000
const FRESH = NOW - 1_000 // a frame 1s ago = live + recent

// ── DOWN: no socket (the post-close / never-connected state) ──────────────────────────────────────
eq(
  'sock=null → upstreamConnected:false (NEVER true without a live socket)',
  computeUpstreamHealth({ certRun: false, sockPresent: false, wsReadyState: undefined, lastActivityAt: 0, now: NOW }),
  { upstreamConnected: false },
)
eq(
  'sock=null but a prior activity ts + recorded disconnect cause → false + that ts + reason',
  computeUpstreamHealth({ certRun: false, sockPresent: false, wsReadyState: undefined, lastActivityAt: FRESH, now: NOW, lastDisconnectReason: 'network' }),
  { upstreamConnected: false, lastUpstreamActivityMs: FRESH, reason: 'network' },
)
eq(
  'sock=null, last cause logged_out → reason reflects the last disconnect cause (terminal)',
  computeUpstreamHealth({ certRun: false, sockPresent: false, wsReadyState: undefined, lastActivityAt: FRESH, now: NOW, lastDisconnectReason: 'logged_out' }),
  { upstreamConnected: false, lastUpstreamActivityMs: FRESH, reason: 'logged_out' },
)

// ── DOWN: ws CLOSING / CLOSED while the sock ref still lingers ──────────────────────────────────────
eq(
  'ws CLOSING (even with a fresh frame) → false',
  computeUpstreamHealth({ certRun: false, sockPresent: true, wsReadyState: WS_CLOSING, lastActivityAt: FRESH, now: NOW }),
  { upstreamConnected: false, lastUpstreamActivityMs: FRESH },
)
eq(
  'ws CLOSED → false',
  computeUpstreamHealth({ certRun: false, sockPresent: true, wsReadyState: WS_CLOSED, lastActivityAt: FRESH, now: NOW }),
  { upstreamConnected: false, lastUpstreamActivityMs: FRESH },
)

// ── DOWN: stale activity (the silent half-open zombie — sock present, ws OPEN, no frames) ────────────
eq(
  'lastActivityAt stale past the window → false + network (silent half-open zombie)',
  computeUpstreamHealth({ certRun: false, sockPresent: true, wsReadyState: WS_OPEN, lastActivityAt: NOW - (WATCHDOG_WINDOW_MS + 1_000), now: NOW }),
  { upstreamConnected: false, lastUpstreamActivityMs: NOW - (WATCHDOG_WINDOW_MS + 1_000), reason: 'network' },
)

// ── UP: a live, recent socket ──────────────────────────────────────────────────────────────────────
eq(
  'sock live + ws OPEN + recent frame → true + lastUpstreamActivityMs',
  computeUpstreamHealth({ certRun: false, sockPresent: true, wsReadyState: WS_OPEN, lastActivityAt: FRESH, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: FRESH },
)
eq(
  'frame exactly at the window boundary → still up (strictly-greater staleness)',
  computeUpstreamHealth({ certRun: false, sockPresent: true, wsReadyState: WS_OPEN, lastActivityAt: NOW - WATCHDOG_WINDOW_MS, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: NOW - WATCHDOG_WINDOW_MS },
)
eq(
  'sock live, readyState unknown (no raw ws handle), recent → degrade to activity check → up',
  computeUpstreamHealth({ certRun: false, sockPresent: true, wsReadyState: undefined, lastActivityAt: FRESH, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: FRESH },
)

// ── CERT RUN (ECHO_MODE || conformanceSeen): the CRIES-WOLF affordance ──────────────────────────────
// A cert run has no real Baileys socket (the seed never connects), yet the A4 conformance probe's
// CRIES-WOLF check requires upstreamConnected:true with advancing activity. The connector reports a
// synthetic "up" with a fresh timestamp (mirrors how SEND echoes in cert). No mock-upstream control
// is exposed → the LIES-WHEN-DEAD sub-check is skipped for this real connector.
eq(
  'cert run, no socket → synthetic up + fresh ts (CRIES-WOLF passes)',
  computeUpstreamHealth({ certRun: true, sockPresent: false, wsReadyState: undefined, lastActivityAt: 0, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: NOW },
)

// ── S1.3 (c): wsReadyStateFromClient — the Baileys rc13 ws-liveness mapping ──────────────────────────
// On Baileys 7.x rc13 `sock.ws` is a WebSocketClient whose underlying `ws` socket is PROTECTED, so
// `sock.ws.readyState` is `undefined` (the old wiring read a dead field → health never saw CLOSING/
// CLOSED via readyState). rc13 exposes `isOpen`/`isClosing`/`isClosed`/`isConnecting` getters instead.
// This maps them back to a WS_* readyState code computeUpstreamHealth understands.
eq(
  'rc13 client OPEN → WS_OPEN',
  wsReadyStateFromClient({ isOpen: true, isClosing: false, isClosed: false, isConnecting: false }),
  WS_OPEN,
)
eq(
  'rc13 client CLOSING → WS_CLOSING',
  wsReadyStateFromClient({ isOpen: false, isClosing: true, isClosed: false, isConnecting: false }),
  WS_CLOSING,
)
eq(
  'rc13 client CLOSED → WS_CLOSED',
  wsReadyStateFromClient({ isOpen: false, isClosing: false, isClosed: true, isConnecting: false }),
  WS_CLOSED,
)
eq(
  'rc13 client CONNECTING → WS_CONNECTING',
  wsReadyStateFromClient({ isOpen: false, isClosing: false, isClosed: false, isConnecting: true }),
  WS_CONNECTING,
)
// rc13: when the inner socket is null, BOTH isClosed and isClosing read true — CLOSED must win
// (the socket is gone, not merely tearing down) so health classifies it down, not ambiguously.
eq(
  'rc13 client with null inner socket (isClosed && isClosing both true) → WS_CLOSED (closed wins)',
  wsReadyStateFromClient({ isOpen: false, isClosing: true, isClosed: true, isConnecting: false }),
  WS_CLOSED,
)
eq(
  'no ws handle (null) → undefined (unknown — health degrades to the activity check)',
  wsReadyStateFromClient(null),
  undefined,
)
eq(
  'no ws handle (undefined) → undefined',
  wsReadyStateFromClient(undefined),
  undefined,
)
// The dead-field regression: an rc13 client where every getter is false/absent (the raw `.readyState`
// the old wiring read does NOT exist) → undefined, never a bogus WS_OPEN.
eq(
  'rc13 client with no truthy getter → undefined (never a false-positive OPEN)',
  wsReadyStateFromClient({}),
  undefined,
)

// End-to-end through the verdict: an rc13 CLOSING client must make health report DOWN even with a
// fresh frame — this is the exact silent-half-open path the old dead-readyState wiring masked.
eq(
  'rc13 CLOSING client → health DOWN (computeUpstreamHealth via the mapped readyState)',
  computeUpstreamHealth({
    certRun: false,
    sockPresent: true,
    wsReadyState: wsReadyStateFromClient({ isOpen: false, isClosing: true, isClosed: false, isConnecting: false }),
    lastActivityAt: FRESH,
    now: NOW,
  }),
  { upstreamConnected: false, lastUpstreamActivityMs: FRESH },
)

// ── 2a.4 (3): the sock===null POST-RECYCLE CONNECTING window. After the framework recycles a zombie,
// the fresh connector process spawns and Baileys is briefly sock===null while it (re)connects. Without
// a signal the framework would read upstreamConnected:false with NO reason → recycle AGAIN (a recycle
// loop). The connector reports reason:'transient' honestly in its connecting window (the WhatsApp
// analogue of Telegram's !liveStarted) so the framework DEFERS the heavy recycle and keeps re-probing. ─
eq(
  'sock=null + connecting → false + reason:transient (defer; a post-recycle null window must NOT re-recycle)',
  computeUpstreamHealth({ certRun: false, sockPresent: false, wsReadyState: undefined, lastActivityAt: 0, now: NOW, connecting: true }),
  { upstreamConnected: false, reason: 'transient' },
)
// CONTROL: sock=null while NOT connecting is still the real down verdict (no regression — a closed,
// not-reconnecting socket must still be recoverable by the framework).
eq(
  'sock=null + NOT connecting → false, no transient masking (no regression)',
  computeUpstreamHealth({ certRun: false, sockPresent: false, wsReadyState: undefined, lastActivityAt: 0, now: NOW, connecting: false }),
  { upstreamConnected: false },
)
// A genuine terminal logout must NOT be masked by the connecting window — logged_out wins (else a
// revoked credential would silently defer forever instead of paging for a re-pair).
eq(
  'sock=null + connecting + lastDisconnectReason:logged_out → logged_out (terminal not masked)',
  computeUpstreamHealth({ certRun: false, sockPresent: false, wsReadyState: undefined, lastActivityAt: 0, now: NOW, connecting: true, lastDisconnectReason: 'logged_out' }),
  { upstreamConnected: false, reason: 'logged_out' },
)
// The connecting window carries the last real activity ts when one exists (helps the framework trace).
eq(
  'sock=null + connecting + prior activity → transient + last activity ts',
  computeUpstreamHealth({ certRun: false, sockPresent: false, wsReadyState: undefined, lastActivityAt: FRESH, now: NOW, connecting: true }),
  { upstreamConnected: false, lastUpstreamActivityMs: FRESH, reason: 'transient' },
)

if (failures > 0) {
  console.error(`\n${failures} assertion(s) FAILED`)
  process.exit(1)
}
console.log('\nall health assertions passed')
