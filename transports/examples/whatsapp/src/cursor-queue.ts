/**
 * WhatsApp connector inbound cursor queue — the connector-owned, monotonic, non-consuming backlog.
 *
 * PURE & DEPENDENCY-FREE on purpose: this module imports NOTHING (no baileys, no MCP, no fs), so it can
 * be unit-tested without the folder-local node_modules. `server.ts` supplies the side-effects (persist
 * to `cursor-state.json`, the `message_available` wakeup, the log line) as callbacks.
 *
 * It implements the connector contract's "Cursor rules" (transports/CONTRACT.md):
 *
 *   Rule 1 — MINT THE CURSOR AT PUSH TIME. `enqueue()` takes an entry WITHOUT a cursor and mints it
 *     itself, in the same synchronous step that pushes the entry. Nothing can be awaited between the
 *     mint and the push, so cursor-order is always push-order. (Minting at ingest-start, before a media
 *     download is awaited, let a faster concurrent message overtake a slow one: the framework advanced
 *     `sinceCursor` past the slow message's lower cursor and the slow message, once pushed, was
 *     permanently undrainable — a real live loss on 2026-07-02.) There is exactly one `nextCursor()`
 *     caller and it lives inside `enqueue()`.
 *   Rule 2 — never evict an un-acked entry silently: hitting the hard cap is warned LOUDLY.
 *   Rule 3 — `inboundEnqueued` counts pushes to the drainable backlog; `inboundReceived` counts raw
 *     provider events (kept apart — see `noteReceived`). Both are reported on `transport/health`.
 *   Rule 4 — a drain whose `sinceCursor` is AHEAD of our persisted counter (a lost state write + a
 *     restart) re-anchors the counter to it, loudly; otherwise every later enqueue would mint below
 *     `sinceCursor` and be undrainable.
 *
 * `drain()` is non-consuming for the RETURNED set (strictly `cursor > sinceCursor`) but reclaims acked
 * entries to keep the persisted backlog bounded — keeping the newest `RETAIN_DRAINED_TAIL` already-
 * drained entries so a RE-drain from an older cursor (the cert harness's temporal-contract probe; a
 * second in-process reader) still sees everything above it.
 */

/** Hard cap on the un-drained backlog — a backstop for a framework that never drains. */
export const MAX_BACKLOG = 2000
/** Already-drained (acked) entries kept behind `sinceCursor` so a re-drain from an older cursor works. */
export const RETAIN_DRAINED_TAIL = 64

/** The only field the queue itself needs on an entry. */
export interface CursorEntry {
  cursor: string // monotonic, strictly increasing, connector-owned
}

export interface CursorQueueHooks {
  /** Persist state (atomic write in server.ts; best-effort). Called after any mutation worth keeping. */
  persist: () => void
  /** Wakeup for the framework: new inbound is drainable. */
  notify: () => void
  /** Loud log line (→ stderr → journal). */
  warn: (message: string) => void
  /**
   * TEST-ONLY drop switch (server.ts wires it to `process.env.SELFHEAL_DROP_INBOUND === '1'`).
   * When it returns true the entry is counted as enqueued but never stored — see `enqueue()`.
   */
  shouldDropInbound?: () => boolean
}

export interface CursorQueueOptions {
  maxBacklog?: number
  retainDrainedTail?: number
}

/** What `server.ts` persists (alongside `seen` and the catch-up snapshot) in `cursor-state.json`. */
export interface CursorQueueState<T extends CursorEntry> {
  cursorSeq: number
  backlog: T[]
}

export class CursorQueue<T extends CursorEntry> {
  private backlog: T[] = []
  private cursorSeq = 0
  private received = 0
  private enqueued = 0
  private dropSwitchWarned = false
  private readonly maxBacklog: number
  private readonly retainDrainedTail: number

  constructor(private readonly hooks: CursorQueueHooks, options: CursorQueueOptions = {}) {
    this.maxBacklog = options.maxBacklog ?? MAX_BACKLOG
    this.retainDrainedTail = options.retainDrainedTail ?? RETAIN_DRAINED_TAIL
  }

  /** Restore persisted state. The high-water `cursorSeq` is restored independently of the backlog, so a
   *  truncated/GC'd backlog can never lower the next cursor. Bad shapes are ignored (first run). */
  restore(raw: { cursorSeq?: unknown; backlog?: unknown }): void {
    if (typeof raw.cursorSeq === 'number' && raw.cursorSeq >= 0) this.cursorSeq = raw.cursorSeq
    if (Array.isArray(raw.backlog)) {
      this.backlog = (raw.backlog as unknown[])
        .filter((m): m is T => !!m && typeof (m as T).cursor === 'string')
        .slice(-this.maxBacklog)
    }
  }

  snapshot(): CursorQueueState<T> {
    return { cursorSeq: this.cursorSeq, backlog: this.backlog }
  }

  /** One raw provider event observed (before any filtering). Feeds `inboundReceived`. */
  noteReceived(): void {
    this.received++
  }

  /** Self-report counters for `transport/health` (monotonic since process start). */
  counters(): { inboundReceived: number; inboundEnqueued: number } {
    return { inboundReceived: this.received, inboundEnqueued: this.enqueued }
  }

  private nextCursor(): string {
    this.cursorSeq += 1
    return String(this.cursorSeq)
  }

  /**
   * Push one inbound. The cursor is minted HERE, synchronously with the push (contract Cursor rule 1);
   * callers never see or choose a cursor. Returns the stored entry (undefined when the test-only drop
   * switch swallowed it).
   *
   * SELFHEAL_DROP_INBOUND (test-only): claims commitment — `inboundEnqueued` still advances, which is
   * the counter the framework's runtime cross-check watches — but never stores the entry, so
   * `transport/drain` can never return it. That is the "enqueued but not delivered" drop class the
   * framework's silent-drop net exists to catch; a live-proof of that net needs a connector that lies
   * this way on demand. It is OFF unless the env var is exactly '1', and nothing in the framework or
   * this connector sets it; it exists only for deliberate self-heal drills. A pre-enqueue skip (a
   * filtered event) must NOT be simulated here — ordinary filtering (own echoes, dedup'd edits) looks
   * identical and must never trip the recycle ladder.
   */
  enqueue(entry: Omit<T, 'cursor'>): T | undefined {
    this.enqueued++
    if (this.hooks.shouldDropInbound?.()) {
      if (!this.dropSwitchWarned) {
        this.dropSwitchWarned = true
        this.hooks.warn('SELFHEAL_DROP_INBOUND=1 — TEST SWITCH ACTIVE: inbound is counted but NOT stored (never set in production)')
      }
      return undefined
    }
    const full = { ...entry, cursor: this.nextCursor() } as T
    this.backlog.push(full)
    if (this.backlog.length > this.maxBacklog) {
      // The hard cap means the framework has not drained for `maxBacklog` messages — the evicted
      // (oldest) entries are UN-ACKED inbound being dropped. The one residual silent-loss path here,
      // so make it LOUD rather than a silent splice.
      const dropped = this.backlog.length - this.maxBacklog
      this.hooks.warn(`BACKLOG CAP HIT — dropping ${dropped} un-drained inbound message(s); framework has not drained in >${this.maxBacklog} — possible inbound loss`)
      this.backlog.splice(0, dropped)
    }
    this.hooks.persist()
    this.hooks.notify()
    return full
  }

  /** All backlog entries with cursor > sinceCursor, oldest-first (contract: strictly increasing). */
  drain(sinceCursor: string | null): T[] {
    const after = sinceCursor != null ? Number(sinceCursor) : 0
    // Stale-reload self-heal (Cursor rule 4): the framework's durable cursor AHEAD of our persisted
    // high-water means a state write was silently lost and a restart reloaded a stale counter. Re-anchor
    // LOUDLY instead of minting below `sinceCursor` forever.
    if (after > this.cursorSeq) {
      this.hooks.warn(`cursorSeq (${this.cursorSeq}) is BEHIND the framework's sinceCursor (${after}) — stale cursor-state reload; re-anchoring high-water to ${after}`)
      this.cursorSeq = after
      this.hooks.persist()
    }
    const out = this.backlog.filter((m) => Number(m.cursor) > after).sort((a, b) => Number(a.cursor) - Number(b.cursor))
    // GC: reclaim what the framework already drained, but keep the newest `retainDrainedTail` of it.
    // The RETURNED set above is strictly > sinceCursor, so retention never causes duplicates; the
    // persisted backlog stays bounded. Idempotent; persists only when something was reclaimed.
    if (after > 0) {
      const floor = Math.max(0, after - this.retainDrainedTail)
      const kept = this.backlog.filter((m) => Number(m.cursor) > floor)
      if (kept.length !== this.backlog.length) {
        this.backlog = kept
        this.hooks.persist()
      }
    }
    return out
  }
}
