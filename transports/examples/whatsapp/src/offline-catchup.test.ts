/**
 * Focused logic test for the WhatsApp connector offline catch-up policy (messages.upsert `append`).
 *
 * Like watchdog.test.ts / close-policy.test.ts, this exercises the dependency-free `offline-catchup.ts`
 * as a pure assertion script (no baileys / no MCP / no live account):
 *   ../../../packages/core/node_modules/.bin/tsx src/offline-catchup.test.ts   (or any tsx binary)
 * Exits non-zero on the first failed assertion.
 *
 * Proves: an `append` from another user newer than the watermark is delivered ONCE; fromMe / older than
 * the watermark / protocol stubs are ignored; the same id twice (same batch or across batches) is
 * delivered once; `notify` is unchanged; the floor is a connect-time snapshot (a live notify cannot
 * hide the older offline messages); the first run (no watermark) uses the bounded recent window; state
 * round-trips through snapshot/restore; the dedupe memory is bounded.
 */
import {
  InboundCatchup,
  FIRST_RUN_WINDOW_MS,
  MAX_HANDLED_IDS,
  isRealInboundContent,
  messageTimestampMs,
  type CatchupMessage,
} from './offline-catchup.js'

let failures = 0
function eq(name: string, actual: unknown, expected: unknown): void {
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) console.log(`  ok  ${name}`)
  else { failures++; console.error(`FAIL  ${name} (got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)})`) }
}

const NOW = Date.UTC(2026, 9, 30, 12, 0, 0)
const sec = (ms: number): number => Math.floor(ms / 1000)
const WATERMARK = NOW - 60 * 60 * 1000 // last handled inbound: 1 h before connect

function msg(id: string, tsMs: number, over: Partial<CatchupMessage> = {}): CatchupMessage {
  return {
    key: { id, remoteJid: '15550100@s.whatsapp.net', fromMe: false },
    message: { conversation: `text ${id}` },
    messageTimestamp: sec(tsMs),
    ...over,
  }
}
const ids = (ms: CatchupMessage[]): Array<string | null | undefined> => ms.map((m) => m.key?.id)

/** A catch-up instance restored from a persisted watermark and armed for a fresh connect. */
function armed(watermarkMs: number | null = WATERMARK): InboundCatchup {
  const c = new InboundCatchup()
  if (watermarkMs !== null) c.restore({ inboundWatermarkMs: watermarkMs })
  c.armForConnect(NOW)
  return c
}
/** Simulate server.ts: select, then mark each delivered message handled. */
function deliver(c: InboundCatchup, type: string, messages: CatchupMessage[]): CatchupMessage[] {
  const { deliver: out } = c.select({ type, messages })
  for (const m of out) c.markHandled(m)
  return out
}

// ── append: another user, newer than the watermark → delivered once ──────────────────────────────
{
  const c = armed()
  const out = deliver(c, 'append', [msg('A1', WATERMARK + 5 * 60_000)])
  eq('append from another user newer than the watermark → delivered', ids(out), ['A1'])
  eq('same append replayed in a later batch → not delivered again', ids(deliver(c, 'append', [msg('A1', WATERMARK + 5 * 60_000)])), [])
}

// ── append: fromMe → ignored ──────────────────────────────────────────────────────────────────────
{
  const c = armed()
  const own = msg('M1', WATERMARK + 60_000, { key: { id: 'M1', remoteJid: '15550100@s.whatsapp.net', fromMe: true } })
  eq('append fromMe → ignored', ids(deliver(c, 'append', [own])), [])
}

// ── append: older than the watermark → ignored ───────────────────────────────────────────────────
{
  const c = armed()
  eq('append older than the watermark → ignored', ids(deliver(c, 'append', [msg('O1', WATERMARK - 60_000)])), [])
  eq('append exactly AT the watermark second (unseen id) → delivered (same-second sibling must survive)', ids(deliver(armed(), 'append', [msg('S1', WATERMARK)])), ['S1'])
}

// ── the same id twice → once ─────────────────────────────────────────────────────────────────────
{
  const c = armed()
  const t = WATERMARK + 60_000
  eq('same id twice in ONE batch → delivered once', ids(deliver(c, 'append', [msg('D1', t), msg('D1', t)])), ['D1'])
  eq('same id again in the NEXT batch → not delivered', ids(deliver(c, 'append', [msg('D1', t)])), [])
}

// ── notify unchanged ─────────────────────────────────────────────────────────────────────────────
{
  const c = armed()
  const old = msg('N1', WATERMARK - 10 * 60_000) // would be filtered as an append
  const own = msg('N2', NOW, { key: { id: 'N2', remoteJid: '15550100@s.whatsapp.net', fromMe: true } })
  const stub = msg('N3', NOW, { message: { protocolMessage: {} } })
  eq('notify passes EVERY message through unchanged (old, fromMe, protocol — ingest decides)', ids(deliver(c, 'notify', [old, own, stub])), ['N1', 'N2', 'N3'])
  eq('notify delivers a repeated id again (no new dedupe on the live path)', ids(deliver(c, 'notify', [old])), ['N1'])
  eq('unknown upsert type → nothing', ids(deliver(c, 'prepend', [msg('P1', NOW)])), [])
  eq('notify batch reports no catch-up (no catch-up log line)', c.select({ type: 'notify', messages: [old] }).catchup, null)
}

// ── real-content filter (protocol / history-sync stubs) ──────────────────────────────────────────
{
  const t = WATERMARK + 60_000
  eq('protocolMessage-only → not real', isRealInboundContent(msg('X', t, { message: { protocolMessage: { type: 5 } } })), false)
  eq('senderKeyDistribution + contextInfo only → not real', isRealInboundContent(msg('X', t, { message: { senderKeyDistributionMessage: {}, messageContextInfo: {} } })), false)
  eq('no message body → not real', isRealInboundContent(msg('X', t, { message: null })), false)
  eq('stub type set → not real', isRealInboundContent(msg('X', t, { messageStubType: 2 })), false)
  eq('status@broadcast → not real', isRealInboundContent(msg('X', t, { key: { id: 'X', remoteJid: 'status@broadcast', fromMe: false } })), false)
  eq('text → real', isRealInboundContent(msg('X', t)), true)
  eq('image → real', isRealInboundContent(msg('X', t, { message: { imageMessage: {} } })), true)
  eq('unknown future kind → real (ingest degrades it honestly, never silently dropped here)', isRealInboundContent(msg('X', t, { message: { someNewMessage: {} } })), true)
  const c = armed()
  eq('append batch: stubs dropped, real kept', ids(deliver(c, 'append', [
    msg('R1', t, { message: { protocolMessage: {} } }), msg('R2', t), msg('R3', t, { messageStubType: 1 }),
  ])), ['R2'])
}

// ── floor is a connect-time snapshot: a live notify cannot hide older offline messages ───────────
{
  const c = armed()
  deliver(c, 'notify', [msg('L1', NOW - 1_000)]) // live message right after connect → watermark jumps to ~NOW
  const offline = msg('F1', WATERMARK + 10 * 60_000) // arrived while disconnected: older than L1, newer than the old watermark
  eq('append older than a LIVE notify but newer than the connect-time watermark → still delivered', ids(deliver(c, 'append', [offline])), ['F1'])
  eq('a notify-handled id replayed as append → not delivered twice', ids(deliver(c, 'append', [msg('L1', NOW - 1_000)])), [])
}

// ── first run (no watermark): bounded recent window since connect ────────────────────────────────
{
  const c = armed(null)
  eq('first run: append inside the window → delivered', ids(deliver(c, 'append', [msg('W1', NOW - FIRST_RUN_WINDOW_MS + 60_000)])), ['W1'])
  eq('first run: append older than the window → ignored (no stale history flood)', ids(deliver(c, 'append', [msg('W2', NOW - FIRST_RUN_WINDOW_MS - 60_000)])), [])
  eq('first-run window is wider than the 30 s reconnect backoff cap', FIRST_RUN_WINDOW_MS > 30_000, true)
}

// ── catch-up summary (one log line per batch: count + oldest timestamp) ──────────────────────────
{
  const c = armed()
  const a = WATERMARK + 20 * 60_000
  const b = WATERMARK + 10 * 60_000
  const batch = c.select({ type: 'append', messages: [msg('B1', a), msg('B2', b), msg('B3', WATERMARK - 1_000)] })
  eq('summary: count = delivered, oldest = oldest delivered timestamp', batch.catchup, { count: 2, oldestMs: sec(b) * 1000 })
  eq('summary: empty batch → null (no log line)', c.select({ type: 'append', messages: [msg('B3', WATERMARK - 1_000)] }).catchup, null)
}

// ── persistence round-trip + watermark only moves forward, only for inbound ─────────────────────
{
  const c = armed()
  deliver(c, 'append', [msg('P1', WATERMARK + 30 * 60_000)])
  deliver(c, 'append', [msg('P2', WATERMARK + 10 * 60_000)]) // older than P1 → must not lower the watermark
  deliver(c, 'notify', [msg('P3', NOW, { key: { id: 'P3', remoteJid: '1@s.whatsapp.net', fromMe: true } })]) // own → ignored
  const snap = c.snapshot()
  eq('watermark = newest handled inbound (monotonic, fromMe ignored)', snap.inboundWatermarkMs, sec(WATERMARK + 30 * 60_000) * 1000)
  const restored = new InboundCatchup()
  restored.restore(JSON.parse(JSON.stringify(snap)))
  restored.armForConnect(NOW + 3_600_000)
  eq('restart: handled ids survive (no re-delivery after a respawn)', ids(deliver(restored, 'append', [msg('P1', WATERMARK + 30 * 60_000)])), [])
  eq('restart: floor = restored watermark (older than it ignored)', ids(deliver(restored, 'append', [msg('P4', WATERMARK + 29 * 60_000)])), [])
  const bad = new InboundCatchup()
  bad.restore({ inboundWatermarkMs: 'x', handledIds: [1, null, 'ok'] })
  eq('restore tolerates malformed state', bad.snapshot(), { inboundWatermarkMs: 0, handledIds: ['ok'] })
}

// ── bounded dedupe memory ────────────────────────────────────────────────────────────────────────
{
  const c = new InboundCatchup({ maxIds: 5 })
  c.armForConnect(NOW)
  for (let i = 0; i < 8; i++) deliver(c, 'append', [msg(`E${i}`, NOW - 60_000)])
  eq('dedupe memory is capped (oldest evicted)', c.snapshot().handledIds, ['E3', 'E4', 'E5', 'E6', 'E7'])
  eq('default cap is 1000', MAX_HANDLED_IDS, 1000)
}

// ── timestamp parsing (number | numeric string | Long-like) ──────────────────────────────────────
eq('timestamp: number seconds → ms', messageTimestampMs({ messageTimestamp: 100 }), 100_000)
eq('timestamp: Long-like → ms', messageTimestampMs({ messageTimestamp: { toString: () => '100' } }), 100_000)
eq('timestamp: absent → 0', messageTimestampMs({}), 0)

if (failures > 0) {
  console.error(`\n${failures} assertion(s) FAILED`)
  process.exit(1)
}
console.log('\nall offline-catchup assertions passed')
