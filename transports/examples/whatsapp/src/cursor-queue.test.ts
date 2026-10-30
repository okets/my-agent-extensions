/**
 * Focused logic test for the WhatsApp connector inbound cursor queue (cursor-queue.ts).
 *
 * Like watchdog.test.ts / close-policy.test.ts / offline-catchup.test.ts, this exercises the
 * dependency-free module as a pure assertion script (no baileys / no MCP / no live account):
 *   node_modules/.bin/tsx src/cursor-queue.test.ts   (or any tsx binary)
 * Exits non-zero on the first failed assertion.
 *
 * Proves (contract "Cursor rules", transports/CONTRACT.md):
 *   Rule 1 — a cursor is minted at PUSH time, so cursor-order == push-order even when an earlier-arriving
 *            message awaits a slow media download; the slow message stays drainable after the framework
 *            has drained past the fast one (the 2026-07-02 loss). Offline catch-up (append) batches and a
 *            concurrent live batch get cursors in push order through the same enqueue().
 *   Retained tail — drain keeps the newest drained entries so a re-drain from an older cursor still works,
 *            without duplicates and without unbounded growth.
 *   Rule 4 — a stale cursor high-water re-anchors loudly; later pushes are drainable.
 *   Rule 3 — inboundReceived counts raw events, inboundEnqueued counts pushes; the SELFHEAL_DROP_INBOUND
 *            test switch is counted-but-not-stored and inert unless wired.
 *   Persistence round-trip + the hard-cap warning (Rule 2: loud, never silent).
 */
import { CursorQueue, MAX_BACKLOG, RETAIN_DRAINED_TAIL, type CursorEntry } from './cursor-queue.js'
import { InboundCatchup, type CatchupMessage } from './offline-catchup.js'

let failures = 0
function eq(name: string, actual: unknown, expected: unknown): void {
  const ok = JSON.stringify(actual) === JSON.stringify(expected)
  if (ok) console.log(`  ok  ${name}`)
  else { failures++; console.error(`FAIL  ${name} (got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)})`) }
}

interface Entry extends CursorEntry { from: string; content: string }
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
const contents = (es: Entry[]): string[] => es.map((e) => e.content)
const cursors = (es: Entry[]): number[] => es.map((e) => Number(e.cursor))

function makeQueue(over: { drop?: () => boolean; max?: number; tail?: number } = {}) {
  const log = { persist: 0, notify: 0, warn: [] as string[] }
  const q = new CursorQueue<Entry>(
    {
      persist: () => { log.persist++ },
      notify: () => { log.notify++ },
      warn: (m) => { log.warn.push(m) },
      ...(over.drop ? { shouldDropInbound: over.drop } : {}),
    },
    { ...(over.max ? { maxBacklog: over.max } : {}), ...(over.tail !== undefined ? { retainDrainedTail: over.tail } : {}) },
  )
  return { q, log }
}
const push = (q: CursorQueue<Entry>, content: string): void => { q.enqueue({ from: 'a@s.whatsapp.net', content }) }

// ── Rule 1: the cursor is minted at push time ────────────────────────────────────────────────────
{
  const { q } = makeQueue()
  push(q, 'one'); push(q, 'two'); push(q, 'three')
  eq('cursors start at 1 and increase by 1 per push', cursors(q.drain(null)), [1, 2, 3])
}
{
  // enqueue() takes no cursor from the caller; a stray one is overwritten, never trusted.
  const { q } = makeQueue()
  q.enqueue({ from: 'x', content: 'c', cursor: '999' } as unknown as Omit<Entry, 'cursor'>)
  eq('a caller-supplied cursor is ignored (minted at push)', q.drain(null)[0]?.cursor, '1')
}
{
  // A slow media message arrives FIRST, a fast text message SECOND. Both ingest concurrently (handleUpsert is
  // fire-and-forget per event batch). With mint-at-push, the text pushes first and gets the lower cursor; the
  // media pushes when its download finishes and gets the next one — cursor order == push order, monotonic
  // across the await.
  const { q } = makeQueue()
  const ingest = async (content: string, downloadMs: number): Promise<void> => {
    await sleep(downloadMs) // the awaited media download / API lookup
    q.enqueue({ from: 'a@s.whatsapp.net', content })
  }
  await Promise.all([ingest('slow-media', 30), ingest('fast-text', 0)])
  const all = q.drain(null)
  eq('push order is the cursor order across an await', contents(all), ['fast-text', 'slow-media'])
  eq('cursors are strictly increasing across an await', cursors(all), [1, 2])
}
{
  // The 2026-07-02 loss, replayed: the framework drains the fast message and advances sinceCursor, THEN the
  // slow media finishes. It must still be drainable (its cursor was minted after, so it is above sinceCursor).
  const { q } = makeQueue()
  const slow = (async (): Promise<void> => { await sleep(30); q.enqueue({ from: 'a', content: 'slow-media' }) })()
  q.enqueue({ from: 'a', content: 'fast-text' })
  const first = q.drain(null)
  const sinceCursor = first[first.length - 1]!.cursor // the framework durably advances past the fast one
  await slow
  eq('slow message stays drainable after the framework advanced past the fast one', contents(q.drain(sinceCursor)), ['slow-media'])
}
{
  // Negative control — the OLD behaviour (cursor allocated at ingest start) is exactly what loses the message.
  // Models the bug with a plain counter so the test documents what rule 1 prevents.
  let seq = 0
  const backlog: Array<{ cursor: number; content: string }> = []
  const slowCursor = ++seq // minted before the await
  const fastCursor = ++seq
  backlog.push({ cursor: fastCursor, content: 'fast-text' })
  const sinceCursor = fastCursor // framework drains past the fast one
  backlog.push({ cursor: slowCursor, content: 'slow-media' }) // slow finally pushes its LOWER cursor
  eq('control: mint-at-ingest-start makes the slow message undrainable (the bug)', backlog.filter((m) => m.cursor > sinceCursor).length, 0)
}

// ── Offline catch-up (#10) goes through the same enqueue ─────────────────────────────────────────
{
  const NOW = Date.UTC(2026, 9, 30, 12, 0, 0)
  const sec = (ms: number): number => Math.floor(ms / 1000)
  const mk = (id: string, tsMs: number): CatchupMessage => ({
    key: { id, remoteJid: '15550100@s.whatsapp.net', fromMe: false },
    message: { conversation: `text ${id}` },
    messageTimestamp: sec(tsMs),
  })
  const catchup = new InboundCatchup()
  catchup.restore({ inboundWatermarkMs: NOW - 3_600_000 })
  catchup.armForConnect(NOW)

  const { q } = makeQueue()
  // Mirrors server.ts handleUpsert: select() → for each delivered msg: noteReceived, ingest (awaits), enqueue.
  const handleUpsert = async (upsert: { type: string; messages: CatchupMessage[] }, perMsgDelay: (id: string) => number): Promise<void> => {
    const { deliver } = catchup.select(upsert)
    for (const m of deliver) {
      q.noteReceived()
      await sleep(perMsgDelay(m.key?.id ?? ''))
      q.enqueue({ from: m.key?.remoteJid ?? '', content: m.key?.id ?? '' })
      catchup.markHandled(m)
    }
  }
  const t = (minAgo: number): number => NOW - minAgo * 60_000
  await handleUpsert(
    { type: 'append', messages: [mk('off-1', t(50)), mk('off-2', t(40)), mk('off-3', t(30))] },
    (id) => (id === 'off-1' ? 25 : 0), // the first catch-up message is the slow one (media)
  )
  const afterCatchup = q.drain(null)
  eq('catch-up messages get cursors in delivery order', contents(afterCatchup), ['off-1', 'off-2', 'off-3'])
  eq('catch-up cursors are strictly increasing', cursors(afterCatchup), [1, 2, 3])

  // A live batch and a catch-up batch overlapping (both fire-and-forget): cursors still follow push order.
  const { q: q2 } = makeQueue()
  const catchup2 = new InboundCatchup()
  catchup2.restore({ inboundWatermarkMs: NOW - 3_600_000 })
  catchup2.armForConnect(NOW)
  const run = async (upsert: { type: string; messages: CatchupMessage[] }, delay: number): Promise<void> => {
    for (const m of catchup2.select(upsert).deliver) {
      q2.noteReceived(); await sleep(delay); q2.enqueue({ from: 'a', content: m.key?.id ?? '' }); catchup2.markHandled(m)
    }
  }
  await Promise.all([
    run({ type: 'append', messages: [mk('off-A', t(20))] }, 30),
    run({ type: 'notify', messages: [mk('live-B', t(0))] }, 0),
  ])
  const overlapped = q2.drain(null)
  eq('overlapping live + catch-up batches: cursor order == push order', contents(overlapped), ['live-B', 'off-A'])
  eq('overlapping batches: cursors strictly increasing', cursors(overlapped), [1, 2])
  eq('catch-up counts as received and enqueued', q.counters(), { inboundReceived: 3, inboundEnqueued: 3 })
}

// ── Retained drained tail ────────────────────────────────────────────────────────────────────────
{
  const { q } = makeQueue({ tail: 3 })
  for (let i = 1; i <= 10; i++) push(q, `m${i}`)
  eq('drain returns only entries above sinceCursor', contents(q.drain('7')), ['m8', 'm9', 'm10'])
  // After draining at 7 the newest 3 drained entries (m5..m7) are retained: a re-drain from an older cursor
  // still returns every entry above it that was retained, with no duplicates.
  eq('re-drain from an older cursor still sees the retained tail', contents(q.drain('4')), ['m5', 'm6', 'm7', 'm8', 'm9', 'm10'])
  eq('entries older than the tail are reclaimed', contents(q.drain('0')), ['m5', 'm6', 'm7', 'm8', 'm9', 'm10'])
  eq('drain is idempotent at the same cursor', contents(q.drain('7')), ['m8', 'm9', 'm10'])
  eq('the persisted backlog is bounded to tail + undrained', q.snapshot().backlog.length, 6)
}
{
  const { q, log } = makeQueue()
  for (let i = 1; i <= RETAIN_DRAINED_TAIL + 20; i++) push(q, `m${i}`)
  const before = log.persist
  q.drain(String(RETAIN_DRAINED_TAIL + 20))
  eq('default tail keeps RETAIN_DRAINED_TAIL drained entries', q.snapshot().backlog.length, RETAIN_DRAINED_TAIL)
  eq('reclaiming drained entries persists', log.persist > before, true)
  const after = log.persist
  q.drain(String(RETAIN_DRAINED_TAIL + 20))
  eq('a repeated drain at the same cursor does not persist again', log.persist, after)
}

// ── Rule 4: stale cursor high-water re-anchors ───────────────────────────────────────────────────
{
  const { q, log } = makeQueue()
  q.restore({ cursorSeq: 5, backlog: [] }) // a state write was lost; the framework is already at 12
  eq('a stale drain returns nothing', q.drain('12'), [])
  eq('the re-anchor is logged loudly', log.warn.length === 1 && /BEHIND the framework's sinceCursor \(12\).*re-anchoring/.test(log.warn[0]!), true)
  push(q, 'after-reanchor')
  const got = q.drain('12')
  eq('a push after the re-anchor mints above sinceCursor', cursors(got), [13])
  eq('...and is drainable', contents(got), ['after-reanchor'])
  eq('no re-anchor warning when the counter is not behind', (() => { q.drain('13'); return log.warn.length })(), 1)
}

// ── Rule 3: counters ─────────────────────────────────────────────────────────────────────────────
{
  const { q } = makeQueue()
  eq('counters start at zero', q.counters(), { inboundReceived: 0, inboundEnqueued: 0 })
  q.noteReceived(); q.noteReceived(); q.noteReceived() // 3 raw events...
  push(q, 'a') // ...only 1 reaches the backlog (the rest were own echoes / filtered)
  eq('received counts raw events, enqueued counts pushes', q.counters(), { inboundReceived: 3, inboundEnqueued: 1 })
  q.drain('1')
  eq('draining does not change the counters', q.counters(), { inboundReceived: 3, inboundEnqueued: 1 })
}

// ── SELFHEAL_DROP_INBOUND test switch ────────────────────────────────────────────────────────────
{
  const { q, log } = makeQueue() // not wired = production default
  push(q, 'normal')
  eq('without the switch wired, inbound is stored and counted', [contents(q.drain(null)), q.counters().inboundEnqueued], [['normal'], 1])
  eq('no drop warning in the normal path', log.warn, [])
}
{
  let on = false
  const { q, log } = makeQueue({ drop: () => on })
  push(q, 'kept')
  on = true
  push(q, 'swallowed-1'); push(q, 'swallowed-2')
  on = false
  push(q, 'kept-2')
  eq('switch on: entries are counted as enqueued', q.counters().inboundEnqueued, 4)
  eq('switch on: entries are NOT stored (drain cannot return them)', contents(q.drain(null)), ['kept', 'kept-2'])
  eq('switch on: no cursor is consumed by a swallowed entry', cursors(q.drain(null)), [1, 2])
  eq('switch on: announced loudly exactly once', log.warn.filter((w) => /SELFHEAL_DROP_INBOUND/.test(w)).length, 1)
}

// ── Wakeup + persistence + hard cap ──────────────────────────────────────────────────────────────
{
  const { q, log } = makeQueue()
  push(q, 'a')
  eq('each push persists and wakes the framework', [log.persist, log.notify], [1, 1])
  const state = JSON.parse(JSON.stringify(q.snapshot())) as { cursorSeq: number; backlog: Entry[] }
  const { q: q2 } = makeQueue()
  q2.restore({ ...state, backlog: [...state.backlog, null, { nope: 1 }] }) // junk is ignored
  eq('state round-trips: undrained entries survive a restart', contents(q2.drain(null)), ['a'])
  push(q2, 'b')
  eq('the high-water survives a restart (next cursor continues, never reset)', cursors(q2.drain(null)), [1, 2])
  const { q: q3 } = makeQueue()
  q3.restore({ cursorSeq: 7, backlog: [] }) // truncated backlog must not lower the next cursor
  push(q3, 'x')
  eq('a truncated backlog does not lower the next cursor', cursors(q3.drain(null)), [8])
}
{
  const { q, log } = makeQueue({ max: 5 })
  for (let i = 1; i <= 7; i++) push(q, `m${i}`)
  eq('hard cap evicts only the oldest, keeps the newest', contents(q.drain(null)), ['m3', 'm4', 'm5', 'm6', 'm7'])
  eq('the hard cap is warned loudly (never silent)', log.warn.filter((w) => /BACKLOG CAP HIT/.test(w)).length, 2)
  eq('MAX_BACKLOG default is unchanged', MAX_BACKLOG, 2000)
}

if (failures > 0) { console.error(`\n${failures} assertion(s) failed`); process.exit(1) }
console.log('\ncursor-queue.test.ts: all assertions passed')
