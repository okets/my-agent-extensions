/**
 * Focused logic test for the Telegram connector `transport/health` poll-staleness duty (B4).
 *
 * Like the WhatsApp seed's health.test.ts, this connector runs via tsx with FOLDER-LOCAL
 * node_modules (only the conformance harness installs them), so a vitest-in-folder unit test isn't
 * available. The poll-health DECISION is therefore extracted into the dependency-free
 * `poll-health.ts` (no MCP import) and exercised here as a pure assertion script. Run with a tsx
 * binary:
 *   ../../../packages/core/node_modules/.bin/tsx src/poll-health.test.ts
 * Exits non-zero on the first failed assertion.
 *
 * The server-side wiring (registering transport/health in ListTools, the CallTool handler that reads
 * the real liveStarted/pollLoggedOut/lastSuccessfulGetUpdatesAt and returns computePollHealth(...),
 * plus the pollLoop 401-terminal + success-anchor) is verified by reading the source + the
 * conformance CLI — it cannot be unit-tested without a live long-poll. This script proves the pure
 * poll-staleness decision (the never-lie contract the A4 conformance probe enforces) + the cert-run
 * CRIES-WOLF affordance + the terminal-vs-environmental split (Invariant 5).
 */
import { computePollHealth, POLL_STALE_WINDOW_MS, SETTLE_GRACE_MS, LONG_POLL_TIMEOUT_S, shouldLatchConformanceSeen } from './poll-health.js'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

let failures = 0
function eq(name: string, actual: unknown, expected: unknown): void {
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) console.log(`  ok  ${name}`)
  else { failures++; console.error(`FAIL  ${name} (got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)})`) }
}

const NOW = 1_000_000_000
const FRESH = NOW - 10_000 // a successful getUpdates 10s ago = well inside the cadence window

// ── CERT RUN (ECHO_MODE || conformanceSeen): the CRIES-WOLF affordance ──────────────────────────────
// A cert run drives no live long-poll, yet the A4 conformance probe's CRIES-WOLF check requires
// upstreamConnected:true with advancing activity. The connector reports a synthetic "up" with a fresh
// timestamp (mirrors how SEND echoes in cert). No mock-upstream control is exposed → the A4
// LIES-WHEN-DEAD sub-check is skipped for this real connector.
eq(
  'cert run → synthetic up + fresh ts (CRIES-WOLF passes)',
  computePollHealth({ certRun: true, liveStarted: false, loggedOut: false, lastSuccessfulGetUpdatesAt: 0, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: NOW },
)

// ── TERMINAL: a hard 401 (token revoked) → logged_out, do NOT spin reconnect (Invariant 5) ──────────
eq(
  'hard 401 (logged out) → false + reason:logged_out (+ last success ts when one exists)',
  computePollHealth({ certRun: false, liveStarted: true, loggedOut: true, lastSuccessfulGetUpdatesAt: FRESH, now: NOW }),
  { upstreamConnected: false, reason: 'logged_out', lastUpstreamActivityMs: FRESH },
)
eq(
  'hard 401 before any success → false + reason:logged_out, no meaningless 0 ts',
  computePollHealth({ certRun: false, liveStarted: true, loggedOut: true, lastSuccessfulGetUpdatesAt: 0, now: NOW }),
  { upstreamConnected: false, reason: 'logged_out' },
)

// ── TERMINAL: the one-shot startup getMe probe said the token is invalid/incomplete (401/404) ────────
// reason:'credential', NEVER 'network' — a recycle respawns into the identical death; a human must
// re-supply the secret (the 2026-07-11 walkthrough's 40-minute silent spawn-die loop).
eq(
  'credential invalid → false + reason:credential (terminal, never network)',
  computePollHealth({ certRun: false, liveStarted: true, loggedOut: false, credentialInvalid: true, lastSuccessfulGetUpdatesAt: 0, now: NOW }),
  { upstreamConnected: false, reason: 'credential' },
)
eq(
  'cert run wins over a credential latch (the probe never runs in ECHO/cert — synthetic up holds)',
  computePollHealth({ certRun: true, liveStarted: false, loggedOut: false, credentialInvalid: true, lastSuccessfulGetUpdatesAt: 0, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: NOW },
)

// ── NOT YET ARMED: poll still in the post-handshake grace → honest up (nothing has failed) ───────────
eq(
  'poll not yet armed → up + fresh now ts (no cadence to be stale against yet)',
  computePollHealth({ certRun: false, liveStarted: false, loggedOut: false, lastSuccessfulGetUpdatesAt: 0, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: NOW },
)

// ── UP: armed + a recent successful getUpdates inside the cadence window ──────────────────────────────
eq(
  'armed + recent success → up + last success ts',
  computePollHealth({ certRun: false, liveStarted: true, loggedOut: false, lastSuccessfulGetUpdatesAt: FRESH, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: FRESH },
)
eq(
  'success exactly at the window boundary → still up (strictly-greater staleness)',
  computePollHealth({ certRun: false, liveStarted: true, loggedOut: false, lastSuccessfulGetUpdatesAt: NOW - POLL_STALE_WINDOW_MS, now: NOW }),
  { upstreamConnected: true, lastUpstreamActivityMs: NOW - POLL_STALE_WINDOW_MS },
)

// ── STALE: armed but getUpdates has not SUCCEEDED within the cadence window → environmental ───────────
// A poll that backed off forever / a dead network. reason:network → the framework recycles to a fresh
// process that resumes the poll (no human needed) — the poll-shape analogue of the socket zombie.
eq(
  'armed + last success past the window → false + reason:network + last success ts',
  computePollHealth({ certRun: false, liveStarted: true, loggedOut: false, lastSuccessfulGetUpdatesAt: NOW - (POLL_STALE_WINDOW_MS + 1_000), now: NOW }),
  { upstreamConnected: false, reason: 'network', lastUpstreamActivityMs: NOW - (POLL_STALE_WINDOW_MS + 1_000) },
)
eq(
  'armed + never succeeded, past window → false + reason:network, no meaningless 0 ts',
  computePollHealth({ certRun: false, liveStarted: true, loggedOut: false, lastSuccessfulGetUpdatesAt: 0, now: NOW }),
  { upstreamConnected: false, reason: 'network' },
)

// ── 2a.4 (2) 409 SETTLE-GUARD: a getUpdates 409 (another poller active — the OLD child during a
// supervised deploy restart) is TRANSIENT, not a stall. Within the settle-grace (≥ the long-poll
// window) a would-be-stale poll is reported reason:'transient' (the framework DEFERS the recycle),
// NOT reason:'network' (which would self-recycle the freshly-restarted bot before its first 2xx). ──
eq(
  'settle-grace is >= the long-poll window',
  SETTLE_GRACE_MS >= LONG_POLL_TIMEOUT_S * 1000,
  true,
)
// A supervised restart: armed, lastSuccess anchored at arm-time, the clock has crossed the stale
// window, BUT a recent 409 (old child still polling) is within the settle-grace → DEFER (transient).
eq(
  'past-window stale + recent 409 within settle-grace → transient (defer, NOT network recycle)',
  computePollHealth({
    certRun: false, liveStarted: true, loggedOut: false,
    lastSuccessfulGetUpdatesAt: NOW - (POLL_STALE_WINDOW_MS + 1_000),
    last409At: NOW - 1_000, now: NOW,
  }),
  { upstreamConnected: false, reason: 'transient', lastUpstreamActivityMs: NOW - (POLL_STALE_WINDOW_MS + 1_000) },
)
// Once the 409s stop (old child gone) past the settle-grace and STILL no success → the real
// environmental verdict returns (reason:network → recycle to a fresh poller). The guard is bounded.
eq(
  'past-window stale + 409 OLDER than the settle-grace → network (real stall, recycle)',
  computePollHealth({
    certRun: false, liveStarted: true, loggedOut: false,
    lastSuccessfulGetUpdatesAt: NOW - (POLL_STALE_WINDOW_MS + 1_000),
    last409At: NOW - (SETTLE_GRACE_MS + 1_000), now: NOW,
  }),
  { upstreamConnected: false, reason: 'network', lastUpstreamActivityMs: NOW - (POLL_STALE_WINDOW_MS + 1_000) },
)
// No 409 at all → unchanged behaviour (a plain stall is environmental network).
eq(
  'past-window stale + NO 409 → network (no regression)',
  computePollHealth({
    certRun: false, liveStarted: true, loggedOut: false,
    lastSuccessfulGetUpdatesAt: NOW - (POLL_STALE_WINDOW_MS + 1_000), now: NOW,
  }),
  { upstreamConnected: false, reason: 'network', lastUpstreamActivityMs: NOW - (POLL_STALE_WINDOW_MS + 1_000) },
)
// A recent 409 while still WITHIN the cadence window (not yet stale) is still up — the 409 doesn't
// advance the stale clock (only a 2xx does), so a healthy-but-contended poll is not reported down.
eq(
  'recent 409 but within cadence window → still up (409 never advances the stale clock)',
  computePollHealth({
    certRun: false, liveStarted: true, loggedOut: false,
    lastSuccessfulGetUpdatesAt: FRESH, last409At: NOW - 1_000, now: NOW,
  }),
  { upstreamConnected: true, lastUpstreamActivityMs: FRESH },
)
// A terminal 401 still wins over a concurrent 409 (auth signal is terminal regardless of contention).
eq(
  '401 + recent 409 → logged_out (terminal wins over the settle-guard)',
  computePollHealth({
    certRun: false, liveStarted: true, loggedOut: true,
    lastSuccessfulGetUpdatesAt: FRESH, last409At: NOW - 1_000, now: NOW,
  }),
  { upstreamConnected: false, reason: 'logged_out', lastUpstreamActivityMs: FRESH },
)

// ── 2a.2 CADENCE COUPLING: the manifest's exported self-window MUST equal the code constant ──────────
// The framework derives its staleThreshold from CAPABILITY.md's `upstream_self_window_ms`. If that
// drifts from POLL_STALE_WINDOW_MS, the framework's window no longer matches the connector's real
// window → the false-recycle bug returns. This keeps the two structurally pinned together.
{
  const capPath = fileURLToPath(new URL('../CAPABILITY.md', import.meta.url))
  const md = readFileSync(capPath, 'utf8')
  const m = md.match(/^upstream_self_window_ms:\s*(\d+)\s*$/m)
  const declared = m ? Number(m[1]) : NaN
  eq('CAPABILITY.md upstream_self_window_ms === POLL_STALE_WINDOW_MS', declared, POLL_STALE_WINDOW_MS)
}

// ── 2a-ii LATCH GUARD (architect gate, M10-S9.10): a loopback-shaped send must only latch
// conformanceSeen BEFORE the live poll is armed — never after, or a bot serving a group/channel
// (whose numeric id is negative and so never matches isDeliverableTarget) would blind poll-stall
// detection forever on its very first non-deliverable-target send. ──────────────────────────────────
eq(
  'before liveStarted (a genuine cert run) → latches conformanceSeen',
  shouldLatchConformanceSeen(false),
  true,
)
eq(
  'after liveStarted (an ordinary live send to a non-deliverable target, e.g. a group/channel id) → does NOT latch',
  shouldLatchConformanceSeen(true),
  false,
)

if (failures > 0) {
  console.error(`\n${failures} assertion(s) FAILED`)
  process.exit(1)
}
console.log('\nall poll-health assertions passed')
