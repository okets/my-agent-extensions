/**
 * WhatsApp connector offline catch-up policy (messages that arrived while the connector was down).
 *
 * PURE & DEPENDENCY-FREE on purpose: this module imports NOTHING (no baileys, no MCP), so it can be
 * unit-tested without the folder-local node_modules. `server.ts` feeds every `messages.upsert` event
 * through `InboundCatchup.select()` and performs the side-effects (ingest / enqueue / persist).
 *
 * THE BUG IT FIXES: Baileys delivers live messages as `messages.upsert` with `type === 'notify'`, but
 * messages that arrived while the connector was disconnected are replayed on reconnect with
 * `type === 'append'` (offline catch-up). The connector used to keep only `notify`, so anything sent to
 * the agent's number during a disconnect was silently dropped — against the contract's "never silently
 * drop inbound" rule.
 *
 * THE POLICY:
 *   - `notify` is passed through UNCHANGED (every message, same as before).
 *   - `append` messages are delivered only when ALL hold:
 *       (a) not fromMe (our own echoes / history of our own sends),
 *       (b) real inbound content (not a protocol / key-distribution / stub / status-broadcast entry),
 *       (c) not older than the catch-up FLOOR — the persisted watermark (timestamp of the newest inbound
 *           the connector handled) captured when the socket was armed; with no watermark (first run),
 *           `now - FIRST_RUN_WINDOW_MS`. This stops a stale history replay from flooding the agent,
 *       (d) not already handled (dedupe by message id, bounded).
 *   - The floor is a SNAPSHOT taken at connect time (`armForConnect`), NOT the live watermark: a live
 *     `notify` that lands before the catch-up batch is processed advances the live watermark past the
 *     older offline messages, and filtering against that moving value would drop exactly the messages
 *     this module exists to deliver. Dedupe (d) is what stops double delivery within a session.
 *   - Comparison is `>=` on whole seconds (WhatsApp timestamps are seconds): two messages in the same
 *     second as the watermark must both survive; id-dedupe removes the one already handled.
 *
 * The caller must call `markHandled(msg)` AFTER a message has been ingested/enqueued, so the watermark
 * and dedupe ids never run ahead of what was actually committed to the backlog.
 */

/** First run (no persisted watermark): accept `append` messages from the last N minutes since connect.
 *  15 min is far wider than the reconnect backoff cap (30 s) and a supervisor / deploy restart, so a
 *  normal short gap is recovered, but small enough that a first-link history replay does not surface
 *  stale conversations to the agent. */
export const FIRST_RUN_WINDOW_MS = 15 * 60 * 1000
/** Bounded dedupe memory (message ids). */
export const MAX_HANDLED_IDS = 1000

/** The minimal structural shape of a Baileys WAMessage this policy reads. */
export interface CatchupMessage {
  key?: { id?: string | null; remoteJid?: string | null; fromMe?: boolean | null } | null
  message?: object | null
  messageStubType?: number | null
  /** Seconds since epoch; Baileys may hand a number, a numeric string or a protobuf Long. */
  messageTimestamp?: number | string | { toString(): string } | null
}

export interface CatchupUpsert<M extends CatchupMessage = CatchupMessage> {
  type: string
  messages: M[]
}

export interface CatchupBatch<M extends CatchupMessage = CatchupMessage> {
  /** Messages the caller should ingest, in arrival order. */
  deliver: M[]
  /** Set only for an `append` batch that delivered something — for the one-line log. */
  catchup: { count: number; oldestMs: number } | null
}

/** Message-content keys that carry no user-visible inbound (the rest — including unknown future kinds —
 *  count as real so they degrade honestly in ingest rather than being silently dropped here). */
const NON_CONTENT_KEYS = new Set(['messageContextInfo', 'senderKeyDistributionMessage', 'protocolMessage'])

/** Message timestamp in epoch ms; 0 when absent / unparsable. */
export function messageTimestampMs(msg: CatchupMessage): number {
  const t = msg.messageTimestamp
  const secs = typeof t === 'number' ? t : t != null ? Number(t.toString()) : 0
  return Number.isFinite(secs) && secs > 0 ? secs * 1000 : 0
}

/** True for a real inbound message with user content (not a stub / protocol / key-distribution /
 *  status-broadcast entry). */
export function isRealInboundContent(msg: CatchupMessage): boolean {
  if (msg.messageStubType) return false
  const jid = msg.key?.remoteJid
  if (!jid || jid === 'status@broadcast') return false
  const m = msg.message as Record<string, unknown> | null | undefined
  if (!m) return false
  return Object.keys(m).some((k) => !NON_CONTENT_KEYS.has(k) && m[k] != null)
}

export class InboundCatchup {
  private watermarkMs = 0
  private floorMs = 0
  private ids: string[] = []
  private idSet = new Set<string>()
  private readonly firstRunWindowMs: number
  private readonly maxIds: number

  constructor(opts: { firstRunWindowMs?: number; maxIds?: number } = {}) {
    this.firstRunWindowMs = opts.firstRunWindowMs ?? FIRST_RUN_WINDOW_MS
    this.maxIds = opts.maxIds ?? MAX_HANDLED_IDS
  }

  /** Load persisted state (tolerant of missing / malformed fields — first run passes nothing). */
  restore(saved: { inboundWatermarkMs?: unknown; handledIds?: unknown }): void {
    if (typeof saved.inboundWatermarkMs === 'number' && saved.inboundWatermarkMs > 0) this.watermarkMs = saved.inboundWatermarkMs
    if (Array.isArray(saved.handledIds)) {
      for (const id of saved.handledIds) if (typeof id === 'string') this.remember(id)
    }
  }

  /** State to persist (alongside the connector's cursor state). */
  snapshot(): { inboundWatermarkMs: number; handledIds: string[] } {
    return { inboundWatermarkMs: this.watermarkMs, handledIds: this.ids.slice(-this.maxIds) }
  }

  /** Snapshot the catch-up floor for a NEW socket. Call at every (re)connect. */
  armForConnect(nowMs: number): void {
    this.floorMs = this.watermarkMs > 0 ? this.watermarkMs : nowMs - this.firstRunWindowMs
  }

  /** Decide which messages of an upsert event to ingest. Pure w.r.t. state (it never records ids —
   *  `markHandled` does, after ingest), except for in-batch dedupe which is local to the call. */
  select<M extends CatchupMessage>(upsert: CatchupUpsert<M>): CatchupBatch<M> {
    if (upsert.type === 'notify') return { deliver: upsert.messages, catchup: null }
    if (upsert.type !== 'append') return { deliver: [], catchup: null }
    const inBatch = new Set<string>()
    const deliver: M[] = []
    let oldestMs = Infinity
    for (const msg of upsert.messages) {
      if (msg.key?.fromMe) continue
      if (!isRealInboundContent(msg)) continue
      const ts = messageTimestampMs(msg)
      if (ts < this.floorMs) continue
      const id = msg.key?.id
      if (id) {
        if (this.idSet.has(id) || inBatch.has(id)) continue
        inBatch.add(id)
      }
      deliver.push(msg)
      if (ts < oldestMs) oldestMs = ts
    }
    return {
      deliver,
      catchup: deliver.length > 0 ? { count: deliver.length, oldestMs: Number.isFinite(oldestMs) ? oldestMs : 0 } : null,
    }
  }

  /** Record a message as handled (after it was ingested): dedupe id + watermark. Own messages ignored. */
  markHandled(msg: CatchupMessage): void {
    if (msg.key?.fromMe) return
    if (msg.key?.id) this.remember(msg.key.id)
    const ts = messageTimestampMs(msg)
    if (ts > this.watermarkMs) this.watermarkMs = ts
  }

  private remember(id: string): void {
    if (this.idSet.has(id)) return
    this.idSet.add(id)
    this.ids.push(id)
    if (this.ids.length > this.maxIds) {
      const evicted = this.ids.splice(0, this.ids.length - this.maxIds)
      for (const e of evicted) this.idSet.delete(e)
    }
  }
}
