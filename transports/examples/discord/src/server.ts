#!/usr/bin/env tsx
/**
 * Discord (gateway WSS) transport connector — a self-contained MCP stdio server.
 *
 * SELF-CONTAINED: imports ONLY @modelcontextprotocol/sdk (folder-local node_modules) + its own pure
 * `upstream.ts` module, and inlines the wire strings. It never imports a framework module — the wire
 * contract is protocol, not code. The Discord gateway is spoken with Node 24's BUILT-IN global
 * `WebSocket` (no external ws dependency) and REST via `fetch`.
 *
 * PUSH-GATEWAY backend (the first non-Baileys instance of the generalized push model). Discord PUSHES
 * events down a persistent connector-held gateway WSS; the connector buffers them into a
 * CONNECTOR-OWNED monotonic INTEGER-cursor queue (start 0, ++ per event) — reset-safe & PERSISTED
 * across restarts — and serves `transport/drain` non-consumingly from a bounded retained backlog. The
 * cursor is this OWN integer, NEVER the gateway `seq` (resets on IDENTIFY → would collide below
 * sinceCursor) and NEVER a raw message snowflake (> 2^53 → the durable guard's Number() coercion loses
 * precision → the `cursor-numeric` cert probe FAILS it).
 *
 * CONTROL-PLANE ONLY. The framework speaks:
 *   - transport/send   {to, content}        → transmit one outbound DM; return {messageId} on success.
 *   - transport/drain  {sinceCursor}        → {messages:[{cursor,from,content,timestamp,...}]}.
 *   - transport/health {}                   → REAL gateway-liveness (never true without a live socket).
 *   - notifications/transport/message_available          → emitted on new inbound (a WAKEUP only).
 *   - notifications/transport/needs_attention {reason}   → a HUMAN must act (revoked token / intents).
 * Never bytes on the wire, never unprompted push.
 *
 * ADDRESSING (DM-first): `from` = the author snowflake (numeric, verbatim), reply `to` = that same
 * snowflake. The connector opens+caches the DM channel to send (from == to, like Telegram chat_id —
 * no core change for DM). Only DM MESSAGE_CREATE (no guild_id) from a non-bot author is ingested.
 *
 * INTENTS: GUILDS(1<<0) | DIRECT_MESSAGES(1<<12) | DIRECT_MESSAGE_REACTIONS(1<<13) = 12289.
 * DIRECT_MESSAGE_TYPING(1<<14) is intentionally NOT set — outbound typing is a REST POST, not a gateway
 * intent; we do not need to receive user typing events. MESSAGE_CONTENT is NOT required to read DM
 * content (a bot always receives full content in its own DMs).
 *
 * TWO MODES, one binary:
 *   - LIVE  (DISCORD_BOT_TOKEN present): connect the gateway WSS for real, REST-send DMs.
 *   - ECHO  (no token, e.g. the conformance harness, which injects no secret): a local loopback —
 *     transport/send echoes the content straight into the drain queue (from := to) so the wire
 *     contract + render carry certify WITHOUT a token or any network. The echo branch is ALSO taken
 *     per-send during an explicit cert run (TRANSPORT_CONFORMANCE), so certification is DETERMINISTIC
 *     even when an ambient DISCORD_BOT_TOKEN leaks into the certifying process's env.
 *
 * SELF-HEAL: heartbeat-ACK watchdog + first-heartbeat jitter + RESUME(op6); on INVALID_SESSION → fresh
 * IDENTIFY (honestly NOT zero-loss across the gap); 4004/4014 (bad token / disallowed intents) →
 * terminal → needs_attention{logged_out} + idle (no reconnect loop); everything else → capped
 * exponential backoff, retry environmental FOREVER. A frame-level zombie watchdog (upstream.ts) is the
 * coarser backstop beneath the heartbeat-ACK check. `session_held: false` — a bot-token connector holds
 * no exportable login; the supervisor simply re-spawns it. No transport/quiesce, no pairing.
 */
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { readFileSync, writeFileSync, renameSync, mkdirSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import {
  computeGatewayHealth,
  shouldForceClose,
  watchdogEnabled,
  WATCHDOG_WINDOW_MS,
  WATCHDOG_CHECK_MS,
  type HealthReason,
} from './upstream.js'

// ── Wire strings (inlined — never import a framework module) ──────────────────────────────
const SEND_TOOL = 'transport/send'
const DRAIN_TOOL = 'transport/drain'
// Value MUST match the framework HEALTH_TOOL ('transport/health' in wire-protocol.ts).
const HEALTH_TOOL = 'transport/health'
// Polish-tier control-plane VERBS (M10 Discord Phase 3). Values MUST match the framework
// TYPING_TOOL / REACTION_TOOL / EDIT_TOOL in wire-protocol.ts — advertising these in listTools() is
// what the generic plugin's `supportsTool(name)` gates the outbound announcements on. These are VERBS,
// not style render flags: no `style.*` boolean is invented for them (never-lie).
const TYPING_TOOL = 'transport/typing'
const REACTION_TOOL = 'transport/reaction'
const EDIT_TOOL = 'transport/edit'
const MESSAGE_AVAILABLE = 'notifications/transport/message_available'
// Control-plane notification the connector emits when a HUMAN must act (revoked token / disallowed
// intents) — value MUST match the framework NEEDS_ATTENTION.
const NEEDS_ATTENTION = 'notifications/transport/needs_attention'

// ── Config / env ────────────────────────────────────────────────────────────────────────
const token = process.env.DISCORD_BOT_TOKEN
// Env-overridable endpoints so a stub/harness can steer the connector (contract). Default the real ones.
const API_BASE = (process.env.DISCORD_API_BASE ?? 'https://discord.com/api/v10').replace(/\/+$/, '')
const GATEWAY_URL = process.env.DISCORD_GATEWAY_URL ?? 'wss://gateway.discord.gg/?v=10&encoding=json'
// ECHO loopback engages when there is no token (the conformance harness injects no secret) OR an
// explicit override. The gateway connect is gated on this — it only runs in genuine LIVE mode.
const ECHO_MODE = !token || process.env.DISCORD_ECHO === '1'
// The harness sets this explicitly so a recert over a token-carrying env is still a cert run, never live.
const CONFORMANCE_ENV = process.env.TRANSPORT_CONFORMANCE === '1'
// A6 kill-switch: disable ONLY the connector's own self-heal so the framework net can be proven alone.
const WATCHDOG_ARMED = watchdogEnabled(process.env.SELFHEAL_WATCHDOG_OFF)

// DM-first IDENTIFY intents: GUILDS(1<<0) | DIRECT_MESSAGES(1<<12) | DIRECT_MESSAGE_REACTIONS(1<<13).
// = 4097 | 8192 = 12289. DIRECT_MESSAGE_REACTIONS(1<<13) is REQUIRED to receive MESSAGE_REACTION_ADD on
// DMs (Phase 3 inbound reactions). DIRECT_MESSAGE_TYPING(1<<14) is intentionally NOT set — outbound
// typing is a REST POST (no gateway intent needed) and we do not need to receive the user's typing events.
// Message edits (MESSAGE_UPDATE) ride the SAME frame as MESSAGE_CREATE under DIRECT_MESSAGES — no new
// intent for edits. MESSAGE_CONTENT is still NOT required for DM content.
const INTENTS = (1 << 0) | (1 << 12) | (1 << 13)

// Discord gateway opcodes.
const OP_DISPATCH = 0
const OP_HEARTBEAT = 1
const OP_IDENTIFY = 2
const OP_RESUME = 6
const OP_RECONNECT = 7
const OP_INVALID_SESSION = 9
const OP_HELLO = 10
const OP_HEARTBEAT_ACK = 11

// A DELIVERABLE Discord target is a user snowflake (a 17-20 digit id). The conformance harness drives
// synthetic targets (e.g. 'probe') and never delivers real inbound, so a non-numeric target means
// "certify via loopback"; a live reply always targets a numeric author snowflake, so a real send never
// takes the echo branch.
const isDeliverableTarget = (to: string): boolean => /^\d{5,}$/.test(to)
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

// ── Media endpoint (S4 handle substrate, §1 NO BYTES ON THE WIRE) ───────────────────────────
// Binary moves out-of-band over the localhost media HTTP endpoint (PUT/GET /api/media); only the JSON
// MediaHandle ever crosses the MCP wire. Config is resolved lazily + best-effort: the canonical source
// is the framework-injected scoped env (`MEDIA_ENDPOINT`/`MEDIA_TOKEN`, declared in requires.env and
// copied into the connector's env by the generic plugin). A localhost fallback (token file at
// <agentDir>/media/token, endpoint http://127.0.0.1:${PORT}) covers a pure code-respawn that hasn't yet
// re-wired the new env. If neither resolves, media degrades HONESTLY and the TEXT path is unaffected.
const MEDIA_ENDPOINT: string = (() => {
  const env = process.env.MEDIA_ENDPOINT
  if (env && env.trim()) return env.trim().replace(/\/+$/, '')
  const port = process.env.PORT ?? '4321'
  return `http://127.0.0.1:${port}`
})()

const MEDIA_TOKEN: string | undefined = (() => {
  const env = process.env.MEDIA_TOKEN
  if (env && env.trim()) return env.trim()
  // cwd = the capability folder (launched via relative `src/server.ts`); the per-instance token is
  // persisted at <agentDir>/media/token = ../../media/token from here.
  try {
    return readFileSync(join(process.cwd(), '..', '..', 'media', 'token'), 'utf8').trim()
  } catch {
    return undefined
  }
})()

/** GET a handle's bytes from the media endpoint (out-of-band — never on the MCP wire). */
async function mediaGet(id: string): Promise<Buffer> {
  if (!MEDIA_TOKEN) throw new Error('media endpoint token unavailable')
  const res = await fetch(`${MEDIA_ENDPOINT}/api/media/${id}`, {
    headers: { 'x-media-token': MEDIA_TOKEN },
  })
  if (!res.ok) throw new Error(`media GET ${id} failed: ${res.status}`)
  return Buffer.from(await res.arrayBuffer())
}

/** PUT raw bytes to the media endpoint and receive a fresh JSON MediaHandle (out-of-band). Caption is
 *  intentionally NOT sent (arbitrary user unicode would break an HTTP header); inbound captions ride
 *  the WireInbound `content` field instead. */
async function mediaPut(bytes: Buffer | Uint8Array, opts: { mime: string; kind: MediaKind }): Promise<MediaHandle> {
  if (!MEDIA_TOKEN) throw new Error('media endpoint token unavailable')
  const res = await fetch(`${MEDIA_ENDPOINT}/api/media`, {
    method: 'PUT',
    headers: { 'content-type': opts.mime, 'x-media-token': MEDIA_TOKEN, 'x-media-kind': opts.kind },
    body: bytes,
  })
  if (!res.ok) throw new Error(`media PUT failed: ${res.status}`)
  const { handle } = (await res.json()) as { handle: MediaHandle }
  return handle
}

// ── Attachment classification (Phase 2 feature 1 = images; feature 2 = files; feature 3 = voice/audio) ──
interface DiscordAttachment {
  id?: string
  filename?: string
  size?: number
  url?: string
  proxy_url?: string
  content_type?: string
  width?: number
  height?: number
  // Voice-message fields (present on a Discord voice memo attachment, feature 3): the clip length in
  // seconds and the base64 amplitude waveform. Carried through so a re-emit could preserve them.
  duration_secs?: number
  waveform?: string
}

const IMAGE_EXT = /\.(png|jpe?g|gif|webp|bmp|tiff?|heic|heif)$/i
const AUDIO_EXT = /\.(ogg|oga|opus|mp3|m4a|aac|wav|flac|webm)$/i

/** True iff this Discord attachment is an IMAGE (by content_type, else by filename extension). */
function isImageAttachment(att: DiscordAttachment): boolean {
  if (att.content_type && att.content_type.startsWith('image/')) return true
  if (!att.content_type && att.filename && IMAGE_EXT.test(att.filename)) return true
  return false
}

/** True iff this Discord attachment is AUDIO — a true voice memo (`isVoiceMemo`, from the message
 *  IS_VOICE_MESSAGE flag) OR a plain audio file (by content_type `audio/*`, else by extension). Feature 3
 *  routes ALL audio through STT (kind:'audio' + ptt:true) so the transcript reaches the brain as text with
 *  the voice-mode hint — matching how WhatsApp marks a voice note (`isVoiceNote`). */
function isAudioAttachment(att: DiscordAttachment, isVoiceMemo: boolean): boolean {
  if (isVoiceMemo) return true
  if (att.content_type && att.content_type.startsWith('audio/')) return true
  if (!att.content_type && att.filename && AUDIO_EXT.test(att.filename)) return true
  return false
}

/** Normalize an attachment's mime type: the `content_type` verbatim (parameters stripped) when present,
 *  else a kind-appropriate default. A file with no reported content_type falls back to the generic
 *  `application/octet-stream` (mirrors telegram's `msg.document.mime_type ?? 'application/octet-stream'`). */
function mimeForAttachment(att: DiscordAttachment, isImage: boolean): string {
  if (att.content_type && att.content_type.trim()) return att.content_type.split(';')[0]!.trim()
  return isImage ? 'image/png' : 'application/octet-stream'
}

/** Inbound: download ANY Discord attachment (image OR non-image file) from its CDN url → PUT to the media
 *  endpoint → a JSON MediaHandle. Phase 2 feature 2 generalizes the former image-only path to all
 *  attachment kinds — an image → `kind:'image'`, everything else → `kind:'file'`. The ORIGINAL filename
 *  is preserved on the wire handle via `caption` (the framework MediaHandle has NO dedicated filename
 *  field, so the telegram/whatsapp document contract rides the filename on `caption` — set client-side
 *  after the PUT, exactly like telegram's `msg.document.file_name`; NOT sent as an HTTP header, where
 *  arbitrary unicode/newlines would break). Best-effort: any failure (no media token, download/PUT error,
 *  a 413 over the framework media body limit) returns null so the caller degrades HONESTLY (an `[image]`
 *  or `[attachment: filename]` placeholder rides `content`) and never silently drops. */
async function downloadAttachmentToHandle(att: DiscordAttachment, isVoiceMemo = false): Promise<MediaHandle | null> {
  // NOTE: every failure here is logged to stderr (console.warn) with the concrete reason so an inbound
  // media degrade — image OR file OR audio — is never a silent mystery. The prior bare `catch {}` swallowed
  // the cause (e.g. a `media PUT failed: 413` when the blob exceeds the framework /api/media body limit),
  // which is why the 2026-07-02 live image degrade had to be reconstructed from the dashboard side.
  // Diagnostics only — the honest-degrade contract (return null → placeholder) is unchanged.
  const isImage = isImageAttachment(att)
  const isAudio = !isImage && isAudioAttachment(att, isVoiceMemo)
  const kind: MediaKind = isImage ? 'image' : isAudio ? 'audio' : 'file'
  const label = att.filename ?? att.id ?? '?'
  try {
    if (!MEDIA_TOKEN) {
      console.warn(`[discord] media store unavailable (no MEDIA_TOKEN) — degrading ${kind} ${label} honestly`)
      return null
    }
    const src = att.url ?? att.proxy_url
    if (!src) {
      console.warn(`[discord] ${kind} ${label} has no url/proxy_url on the attachment frame — cannot download`)
      return null
    }
    const dl = await fetch(src)
    if (!dl.ok) {
      console.warn(`[discord] CDN download failed for ${kind} ${label}: HTTP ${dl.status} ${dl.statusText}`)
      return null
    }
    const bytes = Buffer.from(await dl.arrayBuffer())
    const mime = mimeForAttachment(att, isImage)
    const handle = await mediaPut(bytes, { mime, kind })
    if (isAudio) {
      // Feature 3 (voice): mark the audio handle so the framework runs STT and the brain gets the
      // voice-mode hint. `ptt:true` is exactly how WhatsApp marks a voice note (`isVoiceNote`) —
      // the framework's resolveInboundMedia sets `isVoiceNote` only when a `kind:'audio'` handle has
      // `ptt:true`, which is the gate for transcription + speech-friendly reply + a voice reply back.
      // Every audio attachment is routed as PTT so a plain audio file is also transcribed (never a
      // silent drop into an unconsumed audioAttachment). Discord voice memos also carry `duration_secs`.
      handle.kind = 'audio'
      handle.ptt = true
      if (typeof att.duration_secs === 'number' && att.duration_secs > 0) {
        handle.duration = att.duration_secs
      }
      return handle
    }
    // Preserve the ORIGINAL filename on the wire handle for a non-image file (filename + mime fidelity —
    // a PDF must arrive as that PDF with its real name). Mirrors telegram's document carry: the framework
    // MediaHandle has no `filename` field, so the filename rides `caption`. Images keep the prior contract
    // (no filename-caption — an image caption rides `content` instead), so the certified image path is
    // untouched.
    if (!isImage && att.filename) handle.caption = handle.caption ?? att.filename
    return handle
  } catch (e) {
    // e.g. "media PUT failed: 413" = the blob exceeded the framework /api/media body limit (see the
    // deliverable's framework-gate flag — files routinely exceed it until the raise is deployed).
    // `att.size` is the Discord-reported byte size, logged for triage. Shared by images AND files.
    console.warn(
      `[discord] could not store inbound ${kind} ${label} (${att.size ?? '?'} bytes): ${e instanceof Error ? e.message : String(e)} — degrading honestly`,
    )
    return null
  }
}

/** Honest-degrade placeholder for one attachment that couldn't be stored/forwarded — `[image]` for an
 *  image, `[voice message]` for a voice memo / audio, `[attachment: filename]` for a non-image file. Never
 *  a silent drop (a dropped voice note must never look like the bot ignored the user). */
function degradePlaceholder(att: DiscordAttachment, isVoiceMemo = false): string {
  if (isImageAttachment(att)) return '[image]'
  if (isAudioAttachment(att, isVoiceMemo)) return '[voice message]'
  return `[attachment: ${att.filename ?? 'file'}]`
}

// ── Media (S4 handle substrate, §1 NO BYTES ON THE WIRE — Phase 2 feature 1: images) ─────────
// A MediaHandle is a small JSON descriptor — bytes NEVER ride a handle. Binary moves out-of-band over
// the localhost media HTTP endpoint (PUT/GET /api/media). Only the handle crosses the MCP wire (on
// transport/send `media` and on a drained WireInbound `media`). Inlined + kept in sync with the
// framework wire-protocol, exactly like the telegram/whatsapp connectors. Phase 2 wires the `image`
// (feature 1), `file` (feature 2) and `audio` (feature 3, voice) kinds; reactions/edits are later features.
// For a `file` handle, the ORIGINAL filename rides `caption` (the framework MediaHandle has no dedicated
// filename field — the telegram/whatsapp document contract), preserving filename fidelity end-to-end.
type MediaKind = 'image' | 'audio' | 'file'
interface MediaHandle {
  id: string
  mimeType: string
  size: number
  kind: MediaKind
  caption?: string
  duration?: number
  ptt?: boolean
}

// ── WireInbound (inlined — kept in sync with the framework wire-protocol) ──────────────────
interface WireInbound {
  cursor: string // monotonic, strictly increasing, connector-owned INTEGER (String(++seq))
  from: string // UNTRUSTED raw author snowflake, reported VERBATIM
  content: string
  timestamp: number // ms epoch
  providerMessageId?: string // the Discord message snowflake (enables react-to-user's-message + edits)
  /** JSON-only media handles accompanying the message (Phase 2). NEVER bytes. */
  media?: MediaHandle[]
  // ── Phase 3 interaction kinds (M10-S9 inbound event kinds) — absent `kind` ⇒ 'message'. ──
  /** 'reaction' = the user reacted to one of the bot's messages; 'edit' = the user edited an earlier
   *  message. The framework surfaces both as in-band awareness (a note), never rewriting history. */
  kind?: 'message' | 'reaction' | 'edit'
  /** For kind:'reaction' — the reacted-to provider message snowflake (`d.message_id`). */
  targetMessageId?: string
  /** For kind:'reaction' — the unicode emoji name (`d.emoji.name`). */
  emoji?: string
}

// ── Cert-run detection ─────────────────────────────────────────────────────────────────────
// Explicit signals only — NEVER inferred from the target shape (that poisons a live session). ECHO_MODE
// (no token), the explicit TRANSPORT_CONFORMANCE cert env, or a latched conformanceSeen (a loopback send
// arrived, proving the harness is driving us).
let conformanceSeen = false
function isCertRun(): boolean {
  return ECHO_MODE || CONFORMANCE_ENV || conformanceSeen
}

// ── Connector-owned monotonic cursor queue (PERSISTED across restarts) ─────────────────────
// A push gateway has no provider offset to anchor on, so a reset-to-zero counter would let a
// post-restart message collide below the framework's sinceCursor and vanish. cursorSeq + the bounded
// `seen` dedup + the un-drained backlog are persisted to STATE_FILE across restarts.
let backlog: WireInbound[] = []
let cursorSeq = 0
// The highest `sinceCursor` the framework has EVER drained past (monotonic). An entry at/below this is
// ACKED — safe to reclaim under the cap. Persisted so a restart keeps the safe-reclaim boundary.
let ackedHighWater = 0
let seen: string[] = []
const seenSet = new Set<string>()
const MAX_BACKLOG = 2000
const STATE_DIR = join(process.cwd(), '.state')
const STATE_FILE = join(STATE_DIR, 'cursor-state.json')
let inboundReceivedCount = 0
let inboundEnqueuedCount = 0

function loadCursorState(): void {
  try {
    const raw = JSON.parse(readFileSync(STATE_FILE, 'utf8')) as {
      cursorSeq?: number
      ackedHighWater?: number
      seen?: string[]
      backlog?: WireInbound[]
    }
    if (typeof raw.cursorSeq === 'number' && raw.cursorSeq >= 0) cursorSeq = raw.cursorSeq
    if (typeof raw.ackedHighWater === 'number' && raw.ackedHighWater >= 0) ackedHighWater = raw.ackedHighWater
    if (Array.isArray(raw.seen)) {
      seen = raw.seen.slice(-1000)
      for (const id of seen) seenSet.add(id)
    }
    if (Array.isArray(raw.backlog)) {
      backlog = raw.backlog
        .filter((m): m is WireInbound => !!m && typeof (m as WireInbound).cursor === 'string')
        .slice(-MAX_BACKLOG)
    }
  } catch {
    /* first run / no state — start at 0 */
  }
}

function persistCursorState(): void {
  // Never persist during a cert run — certification must not mutate live state.
  if (isCertRun()) return
  try {
    if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true })
    // Atomic (temp + rename): a respawn must never read a half-written state file (a torn cursor-state
    // would itself manufacture inbound loss).
    const tmp = STATE_FILE + '.tmp'
    writeFileSync(tmp, JSON.stringify({ cursorSeq, ackedHighWater, seen: seen.slice(-1000), backlog }))
    renameSync(tmp, STATE_FILE)
  } catch {
    /* best-effort */
  }
}

/** Mint the NEXT cursor. One caller only (inside enqueue), so cursor-order ≡ push-order. */
function nextCursor(): string {
  cursorSeq += 1
  return String(cursorSeq)
}

/** Dedup by the provider message snowflake so a re-delivered MESSAGE_CREATE isn't double-enqueued. */
function markSeen(id: string): boolean {
  if (seenSet.has(id)) return false
  seenSet.add(id)
  seen.push(id)
  if (seen.length > 1000) {
    const drop = seen.length - 1000
    for (let i = 0; i < drop; i++) seenSet.delete(seen[i]!)
    seen.splice(0, drop)
  }
  return true
}

/**
 * Append one inbound. The cursor is minted HERE, at push time — NEVER earlier (the mint-before-await
 * collision class that lost a live WhatsApp message: a cursor allocated before an await can be overtaken
 * by a faster concurrent message, drained past, and filtered out forever). `inboundEnqueued` counts
 * drainable pushes EXACTLY (the framework's silent-drop net compares it against what it drains).
 */
function enqueue(entry: Omit<WireInbound, 'cursor'>): void {
  inboundEnqueuedCount++
  const full: WireInbound = { ...entry, cursor: nextCursor() }
  backlog.push(full)
  if (backlog.length > MAX_BACKLOG) {
    // Cap eviction is the ONLY reclaim path (drain is NON-consuming — see drain()). Evicting the
    // oldest entries is SAFE when they are at/below the framework-acked high-water (already delivered);
    // evicting an entry ABOVE it is real un-acked inbound loss → warn LOUDLY (contract cursor-rule 2).
    const dropped = backlog.length - MAX_BACKLOG
    const evicted = backlog.splice(0, dropped)
    const unacked = evicted.filter((m) => Number(m.cursor) > ackedHighWater).length
    if (unacked > 0) {
      console.error(
        `[discord] BACKLOG CAP HIT — evicting ${unacked} UN-ACKED inbound message(s) above the framework-drained cursor (${ackedHighWater}); the framework has not drained in >${MAX_BACKLOG} — possible inbound loss`,
      )
    }
  }
  persistCursorState()
  void mcp.notification({ method: MESSAGE_AVAILABLE, params: {} }).catch(() => {})
}

/** All backlog entries with cursor > sinceCursor, oldest-first. TRULY NON-CONSUMING: draining does NOT
 *  reclaim (the framework re-drains from an older cursor on reconnect / after a dropped wakeup, and the
 *  temporal-contract cert probe re-drains from a baseline BELOW the latest ack — reclaiming on drain
 *  would lose those entries). The ONLY reclaim path is the MAX_BACKLOG cap in enqueue(); this just
 *  advances the acked high-water so the cap knows which evictions are safe. Self-heals a stale cursor. */
function drain(sinceCursor: string | null): WireInbound[] {
  const after = sinceCursor != null ? Number(sinceCursor) : 0
  // Stale-reload self-heal: the framework's durable cursor being AHEAD of our persisted high-water means
  // a cursor-state write was silently lost and a restart reloaded a stale cursorSeq — every future
  // enqueue would then mint below sinceCursor and be undrainable forever. Re-anchor LOUDLY.
  if (after > cursorSeq) {
    console.error(
      `[discord] cursorSeq (${cursorSeq}) is BEHIND the framework's sinceCursor (${after}) — stale cursor-state reload; re-anchoring high-water to ${after}`,
    )
    cursorSeq = after
    persistCursorState()
  }
  // Advance the monotonic acked high-water so the cap-eviction in enqueue() can distinguish a SAFE
  // reclaim (entry at/below this — already delivered) from real un-acked loss (above this).
  if (after > ackedHighWater) {
    ackedHighWater = after
    persistCursorState()
  }
  const out = backlog
    .filter((m) => Number(m.cursor) > after)
    .sort((a, b) => Number(a.cursor) - Number(b.cursor))
  return out
}

// ── REST send (LIVE mode) — open+cache the DM channel, then POST the message ────────────────
const dmChannelCache = new Map<string, string>() // author snowflake → DM channel id

// Recent sent-message provider-ids → the DM channel they were delivered to. Reactions/edits target
// these: the framework passes the message id back on transport/reaction|edit, and this map resolves
// the channel so the REST call can address it. IN-MEMORY ONLY (bounded) — a connector restart loses
// pre-restart ids, so reactions/edits reliably target only RECENT (this-process) messages, exactly as
// the adapter documents. A target id NOT in the map is not a hard failure: the handler falls back to
// the DM channel derived from `to` and lets Discord be the authority — an unknown/foreign id then
// fails GRACEFULLY (logged, honest not-delivered) without ever wedging the turn.
const recentSentChannels = new Map<string, string>() // messageId → channelId
const MAX_RECENT_SENT = 500
function recordSent(messageId: string | undefined, channelId: string): void {
  if (!messageId) return
  // Refresh recency: delete+set moves it to the end of the Map's insertion order.
  recentSentChannels.delete(messageId)
  recentSentChannels.set(messageId, channelId)
  if (recentSentChannels.size > MAX_RECENT_SENT) {
    const oldest = recentSentChannels.keys().next().value
    if (oldest !== undefined) recentSentChannels.delete(oldest)
  }
}

// ── Recent message-TEXT map (for reaction target-content resolution) ──────────────────────────
// Mirrors recentSentChannels (same cap, same insertion-order LRU eviction) but retains the message
// TEXT keyed by provider message id — for BOTH the bot's OWN outbound replies and inbound user
// messages/edits. An inbound MESSAGE_REACTION_ADD carries the reacted-to `targetMessageId`; this lets
// the connector render `*reacted 🔥 to: "<original text>"*` (WhatsApp/Telegram parity) instead of the
// generic phrasing. IN-MEMORY ONLY (bounded) — a connector restart loses pre-restart ids and an
// evicted id is gone, so an UNRESOLVED target HONESTLY degrades to the framework's generic
// `*reacted <emoji> to your message*` (never a fabricated/guessed quote), exactly like WA/TG.
const recentSentText = new Map<string, string>() // messageId → message text
const REACTION_SNIPPET_MAX = 80
function rememberMessageText(messageId: string | undefined, text: string | undefined): void {
  if (!messageId || !text) return
  recentSentText.delete(messageId)
  recentSentText.set(messageId, text)
  if (recentSentText.size > MAX_RECENT_SENT) {
    const oldest = recentSentText.keys().next().value
    if (oldest !== undefined) recentSentText.delete(oldest)
  }
}
/** Resolve a reacted-to message id to a ≤80-char single-line snippet, or null when the text is NOT
 *  retained (evicted or sent before this process started) — the caller then leaves the reaction content
 *  empty so the framework surfaces its generic phrasing (honest degrade, never a guessed quote). */
function reactionSnippet(targetId: string | undefined): string | null {
  if (!targetId) return null
  const text = recentSentText.get(targetId)
  if (!text) return null
  const oneLine = text.replace(/\s+/g, ' ').trim()
  if (!oneLine) return null
  return oneLine.length > REACTION_SNIPPET_MAX ? `${oneLine.slice(0, REACTION_SNIPPET_MAX)}…` : oneLine
}

async function discordRest(method: string, path: string, body?: unknown): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    method,
    headers: {
      authorization: `Bot ${token}`,
      'content-type': 'application/json',
      'user-agent': 'DiscordBot (my-agent-discord-connector, 1.0.0)',
    },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  })
}

/** Open (or reuse a cached) DM channel with a user snowflake. Returns the channel id. */
async function openDmChannel(userId: string): Promise<string> {
  const cached = dmChannelCache.get(userId)
  if (cached) return cached
  const res = await discordRest('POST', '/users/@me/channels', { recipient_id: userId })
  if (!res.ok) {
    const b = await res.text().catch(() => '')
    throw new Error(`open DM channel for ${userId} failed (${res.status}): ${b}`)
  }
  const ch = (await res.json()) as { id: string }
  dmChannelCache.set(userId, ch.id)
  return ch.id
}

/** Send one DM to a user snowflake. Handles 429 (rate-limit) with an ordering-preserving retry.
 *  Returns the Discord message snowflake on success. */
async function sendDirectMessage(userId: string, content: string): Promise<string | undefined> {
  const channelId = await openDmChannel(userId)
  for (let attempt = 0; attempt < 5; attempt++) {
    const res = await discordRest('POST', `/channels/${channelId}/messages`, { content })
    if (res.ok) {
      const msg = (await res.json().catch(() => ({}))) as { id?: string }
      recordSent(msg.id, channelId) // so a later edit/reaction can target this sent message
      rememberMessageText(msg.id, content) // retain the sent TEXT so a reaction to THIS reply resolves to a snippet
      return msg.id
    }
    if (res.status === 429) {
      // Rate-limited: honor retry_after, then retry the SAME send before any later one (order preserved —
      // the framework's OutboundPacer is not provider-bucket-aware, so we block here).
      const info = (await res.json().catch(() => ({}))) as { retry_after?: number }
      const waitMs = Math.max(200, Math.ceil((info.retry_after ?? 1) * 1000))
      console.error(`[discord] 429 rate-limited on DM to ${userId}; retrying after ${waitMs}ms`)
      await sleep(waitMs)
      continue
    }
    const b = await res.text().catch(() => '')
    throw new Error(`send DM to ${userId} failed (${res.status}): ${b}`)
  }
  throw new Error(`send DM to ${userId} failed: exhausted 429 retries`)
}

/** A file-extension guess from a mime type (best-effort — only used to synthesize a fallback name). */
function extForMime(mimeType: string): string {
  switch (mimeType) {
    case 'image/png': return 'png'
    case 'image/jpeg': return 'jpg'
    case 'image/gif': return 'gif'
    case 'image/webp': return 'webp'
    case 'application/pdf': return 'pdf'
    case 'application/zip': return 'zip'
    case 'text/plain': return 'txt'
    default: {
      const sub = mimeType.split('/')[1]?.split('+')[0]
      return sub && /^[A-Za-z0-9.-]{1,12}$/.test(sub) ? sub : 'bin'
    }
  }
}

/** A safe download filename for an outbound media handle (Discord requires a filename on the file part).
 *  For a `file` handle the ORIGINAL filename rides `caption` (the telegram/whatsapp document contract —
 *  the framework MediaHandle has no dedicated filename field); when present and file-like it is used
 *  VERBATIM (basename only, control chars/separators scrubbed) so a PDF leaves as `report.pdf`, not a
 *  synthetic name. Otherwise a name is synthesized from the id + mime. Images keep the prior `image-<id>`
 *  naming so the certified image path is untouched. */
function filenameForHandle(handle: MediaHandle): string {
  if (handle.kind === 'file' && handle.caption && /\.[A-Za-z0-9]{1,12}$/.test(handle.caption)) {
    // Basename only + scrub path separators / CR / LF / control chars that would break the multipart part.
    const base = handle.caption.split(/[\\/]/).pop() ?? handle.caption
    const safe = base.replace(/[\r\n\x00-\x1f]+/g, '_').trim()
    if (safe.length > 0) return safe.slice(0, 255)
  }
  const prefix = handle.kind === 'image' ? 'image' : 'file'
  return `${prefix}-${handle.id}.${extForMime(handle.mimeType)}`
}

/** Best-effort Ogg/Opus clip duration in seconds, read from the LAST Ogg page's granule position (Opus
 *  granules are always 48 kHz samples). Never throws — a parse miss returns 0 so the caller substitutes a
 *  small default. Edge-TTS output is real Ogg/Opus (`ffmpeg -c:a libopus -f ogg`), so this yields the REAL
 *  duration for a native Discord voice message. */
function oggOpusDurationSecs(bytes: Buffer): number {
  try {
    // Scan backwards for the last "OggS" capture pattern (0x4F 0x67 0x67 0x53).
    for (let i = bytes.length - 4; i >= 0; i--) {
      if (bytes[i] === 0x4f && bytes[i + 1] === 0x67 && bytes[i + 2] === 0x67 && bytes[i + 3] === 0x53) {
        // granule_position = 64-bit LE at offset +6 from the capture pattern.
        const g = bytes.readBigUInt64LE(i + 6)
        const secs = Number(g) / 48000
        if (Number.isFinite(secs) && secs > 0) return Math.round(secs * 10) / 10
        return 0
      }
    }
  } catch {
    /* best-effort */
  }
  return 0
}

/** A Discord voice-message `waveform`: a base64 byte array (≤256 entries, 1 amplitude/datapoint). This is
 *  a SYNTHETIC preview (a gentle rise-and-fall envelope) — the audio itself is genuine; only the visual
 *  bars are approximated (declared honestly in DECISIONS.md / the deliverable). Discord does not validate
 *  the bars against the audio, so this only affects the preview drawing. */
function syntheticWaveform(durationSecs: number): string {
  const n = Math.max(8, Math.min(64, Math.round((durationSecs || 1) * 8)))
  const arr = new Uint8Array(n)
  for (let i = 0; i < n; i++) {
    // A smooth half-sine envelope (0..~180) so the preview reads as speech-like, never a flat/empty bar.
    arr[i] = Math.round(90 + 90 * Math.sin((Math.PI * i) / (n - 1)))
  }
  return Buffer.from(arr).toString('base64')
}

/** Deliver a genuine NATIVE Discord voice message (the voice bubble with a scrubber + waveform) via the
 *  3-step cloud-attachment flow: (1) register the upload → get a signed GCS `upload_url` + `upload_filename`,
 *  (2) PUT the Ogg/Opus bytes to that URL, (3) POST the message with `flags: IS_VOICE_MESSAGE(1<<13)` plus
 *  the attachment's `duration_secs` + `waveform`. Throws on ANY failure so the caller falls back to a plain
 *  playable audio attachment. Returns the message snowflake on success. */
const IS_VOICE_MESSAGE_FLAG = 1 << 13 // 8192
async function sendVoiceMessage(channelId: string, bytes: Buffer, filename: string): Promise<string | undefined> {
  // Step 1 — register the attachment upload.
  const reg = await discordRest('POST', `/channels/${channelId}/attachments`, {
    files: [{ id: '0', filename, file_size: bytes.length }],
  })
  if (!reg.ok) throw new Error(`voice: attachment register failed (${reg.status}): ${await reg.text().catch(() => '')}`)
  const regJson = (await reg.json()) as { attachments?: Array<{ upload_url?: string; upload_filename?: string }> }
  const slot = regJson.attachments?.[0]
  if (!slot?.upload_url || !slot.upload_filename) throw new Error('voice: register returned no upload slot')

  // Step 2 — PUT the bytes to the signed URL (no Discord auth on the GCS URL; content-type must be audio/*).
  const put = await fetch(slot.upload_url, {
    method: 'PUT',
    headers: { 'content-type': 'audio/ogg' },
    body: bytes,
  })
  if (!put.ok) throw new Error(`voice: bytes PUT failed (${put.status}): ${await put.text().catch(() => '')}`)

  // Step 3 — POST the voice message referencing the uploaded file.
  const durationSecs = oggOpusDurationSecs(bytes) || 1
  const waveform = syntheticWaveform(durationSecs)
  const post = await discordRest('POST', `/channels/${channelId}/messages`, {
    flags: IS_VOICE_MESSAGE_FLAG,
    attachments: [
      { id: '0', filename, uploaded_filename: slot.upload_filename, duration_secs: durationSecs, waveform },
    ],
  })
  if (!post.ok) throw new Error(`voice: message POST failed (${post.status}): ${await post.text().catch(() => '')}`)
  const msg = (await post.json().catch(() => ({}))) as { id?: string }
  recordSent(msg.id, channelId)
  return msg.id
}

/** Outbound: GET a handle's bytes from the media endpoint → deliver on a DM. `image` (feature 1) + `file`
 *  (feature 2) go via the multipart `payload_json` + `files[0]` form, preserving the original filename +
 *  content-type. `audio` (feature 3, voice) is delivered as a NATIVE Discord voice message (the 3-step
 *  cloud-attachment flow), falling back to the same multipart form as a plain playable audio attachment on
 *  any failure. Best-effort — a media failure never throws past the turn (the text has already been
 *  delivered). Honors 429 with an ordering-preserving retry (like the text send). */
async function sendMediaHandle(userId: string, handle: MediaHandle): Promise<string | undefined> {
  try {
    if (handle.kind !== 'image' && handle.kind !== 'file' && handle.kind !== 'audio') {
      // Phase 2 carries image + file + audio (voice) — any other kind is a later menu feature; log,
      // don't throw (the turn's text is already delivered).
      console.error(`[discord] outbound handle ${handle.id} kind='${handle.kind}' not carried (image + file + audio)`)
      return undefined
    }
    const channelId = await openDmChannel(userId)
    const bytes = await mediaGet(handle.id)
    const isAudio = handle.kind === 'audio'
    // Feature 3 (voice): an audio handle delivers as a NATIVE Discord voice message when possible (a voice
    // bubble with a scrubber). Try the 3-step voice-message flow first; on ANY failure fall through to the
    // plain multipart audio attachment below — Discord still renders that with an inline player (honest
    // degrade, never a dropped reply). A voice note's filename is normalized to `voice-message.ogg`.
    const filename = isAudio ? 'voice-message.ogg' : filenameForHandle(handle)
    const contentType = isAudio ? 'audio/ogg' : handle.mimeType
    if (isAudio) {
      try {
        const vid = await sendVoiceMessage(channelId, bytes, filename)
        return vid
      } catch (e) {
        console.error(
          `[discord] native voice message failed (${e instanceof Error ? e.message : String(e)}) — falling back to a plain playable audio attachment`,
        )
        // fall through to the multipart attachment path
      }
    }
    for (let attempt = 0; attempt < 5; attempt++) {
      const form = new FormData()
      const payload: Record<string, unknown> = { attachments: [{ id: 0, filename }] }
      // For a `file` handle the caption IS the filename (already threaded into the file part above), so it
      // is NOT duplicated as message text — the filename shows on Discord's file bubble. For an `image`
      // the caption is a real user caption and rides message content (unchanged from feature 1). An audio
      // fallback carries no message text (the audio bubble IS the reply).
      if (handle.kind === 'image' && handle.caption) payload.content = handle.caption
      form.append('payload_json', JSON.stringify(payload))
      form.append('files[0]', new Blob([bytes], { type: contentType }), filename)
      // Multipart POST: do NOT set content-type (FormData sets the boundary); only auth + user-agent.
      const res = await fetch(`${API_BASE}/channels/${channelId}/messages`, {
        method: 'POST',
        headers: {
          authorization: `Bot ${token}`,
          'user-agent': 'DiscordBot (my-agent-discord-connector, 1.0.0)',
        },
        body: form,
      })
      if (res.ok) {
        const msg = (await res.json().catch(() => ({}))) as { id?: string }
        recordSent(msg.id, channelId)
        return msg.id
      }
      if (res.status === 429) {
        const info = (await res.json().catch(() => ({}))) as { retry_after?: number }
        const waitMs = Math.max(200, Math.ceil((info.retry_after ?? 1) * 1000))
        console.error(`[discord] 429 rate-limited on ${handle.kind} DM to ${userId}; retrying after ${waitMs}ms`)
        await sleep(waitMs)
        continue
      }
      const b = await res.text().catch(() => '')
      throw new Error(`send ${handle.kind} DM to ${userId} failed (${res.status}): ${b}`)
    }
    throw new Error(`send ${handle.kind} DM to ${userId} failed: exhausted 429 retries`)
  } catch (e) {
    console.error(`[discord] outbound ${handle.kind} handle ${handle.id} failed: ${e instanceof Error ? e.message : String(e)}`)
    return undefined
  }
}

// ── Typing indicator (Phase 3, OUTBOUND) — POST /channels/{id}/typing + refresh loop ─────────
// Discord's typing indicator auto-expires ~10s after a POST (and is cleared client-side the instant a
// message is sent). While the framework holds 'start' we REFRESH every ~4s so the "…is typing" stays
// visible; we clear the refresh timer on 'stop', on any send to that DM, and on connector shutdown.
// Keyed by the user snowflake (the DM peer); the channel is resolved+cached via openDmChannel.
const TYPING_REFRESH_MS = 4000
const typingTimers = new Map<string, ReturnType<typeof setInterval>>() // userId → refresh interval

async function postTyping(channelId: string): Promise<void> {
  try {
    const res = await discordRest('POST', `/channels/${channelId}/typing`)
    // 204 = success. A non-2xx (except 429, which just means "slow down") is logged, never thrown — a
    // typing failure must never wedge a turn.
    if (!res.ok && res.status !== 429) {
      console.error(`[discord] typing POST failed (${res.status})`)
    }
  } catch (e) {
    console.error(`[discord] typing POST error: ${e instanceof Error ? e.message : String(e)}`)
  }
}

/** Stop refreshing the typing indicator for a DM (clears the per-DM timer if present). Idempotent. */
function stopTyping(userId: string): void {
  const t = typingTimers.get(userId)
  if (t) {
    clearInterval(t)
    typingTimers.delete(userId)
  }
}

/** Start (or keep) the typing indicator for a DM: fire an immediate POST, then refresh on the interval
 *  until stopped. Best-effort — resolving the DM channel or the POST failing never throws past here. */
async function startTyping(userId: string): Promise<void> {
  try {
    const channelId = await openDmChannel(userId)
    void postTyping(channelId)
    if (!typingTimers.has(userId)) {
      const timer = setInterval(() => {
        void postTyping(channelId)
      }, TYPING_REFRESH_MS)
      typingTimers.set(userId, timer)
    }
  } catch (e) {
    console.error(`[discord] startTyping failed for ${userId}: ${e instanceof Error ? e.message : String(e)}`)
  }
}

/** Clear ALL typing refresh timers (connector shutdown). */
function stopAllTyping(): void {
  for (const t of typingTimers.values()) clearInterval(t)
  typingTimers.clear()
}

// ── Reaction (Phase 3, OUTBOUND) — PUT .../reactions/{urlencoded-emoji}/@me ────────────────────
/** Resolve the DM channel for an interaction that targets a specific message: prefer the recorded
 *  channel for that sent message id (this-process memory), else fall back to the DM channel derived
 *  from `to`. */
async function resolveInteractionChannel(to: string, messageId: string): Promise<string> {
  const known = recentSentChannels.get(messageId)
  if (known) return known
  return openDmChannel(to)
}

/** Attach the bot's own emoji reaction to a message. A unicode emoji is URL-encoded so multi-byte
 *  codepoints (and skin-tone/variation sequences) survive the path. Throws on a non-2xx so the caller
 *  can report honest not-delivered; the caller swallows it (best-effort — never wedges the turn). */
async function reactToMessage(to: string, messageId: string, emoji: string): Promise<void> {
  const channelId = await resolveInteractionChannel(to, messageId)
  const encoded = encodeURIComponent(emoji)
  const res = await discordRest('PUT', `/channels/${channelId}/messages/${messageId}/reactions/${encoded}/@me`)
  if (!res.ok) {
    const b = await res.text().catch(() => '')
    throw new Error(`react ${emoji} to ${messageId} failed (${res.status}): ${b}`)
  }
}

// ── Edit (Phase 3, OUTBOUND) — PATCH /channels/{id}/messages/{id} {content} ────────────────────
/** Replace the text of a message the bot previously sent. `content` arrives ALREADY rendered to the
 *  discord-md dialect by the framework (the adapter renders it exactly like transport/send), so it is
 *  PATCHed verbatim — the same render carry as a fresh send. Throws on a non-2xx (caller swallows). */
async function editMessageContent(to: string, messageId: string, content: string): Promise<string | undefined> {
  const channelId = await resolveInteractionChannel(to, messageId)
  const res = await discordRest('PATCH', `/channels/${channelId}/messages/${messageId}`, { content })
  if (!res.ok) {
    const b = await res.text().catch(() => '')
    throw new Error(`edit ${messageId} failed (${res.status}): ${b}`)
  }
  const msg = (await res.json().catch(() => ({}))) as { id?: string }
  recordSent(msg.id, channelId) // keep the edited message addressable for a follow-up edit/reaction
  return msg.id
}

// ── Gateway client (LIVE mode) ──────────────────────────────────────────────────────────────
let ws: WebSocket | null = null
let sessionId: string | null = null
let resumeGatewayUrl: string | null = null
let lastSeq: number | null = null // the gateway `seq` (for RESUME/heartbeat) — NOT the cursor
let botUserId: string | null = null
let lastActivityAt = 0 // ms-epoch of the last inbound gateway frame (the frame-activity clock)
let heartbeatIntervalMs = 41_250
let heartbeatTimer: ReturnType<typeof setInterval> | null = null
let heartbeatAcked = true // set false on send, true on OP_HEARTBEAT_ACK; a missed ack → zombie reconnect
let watchdogTimer: ReturnType<typeof setInterval> | null = null
let connecting = false
let loggedOut = false // terminal auth failure latched (revoked token / disallowed intents)
let lastDisconnectReason: HealthReason | undefined
let reconnectAttempts = 0
let liveStarted = false
let deferredConnectTimer: ReturnType<typeof setTimeout> | null = null
let intentionalClose = false // a supervisor SIGTERM teardown — do not reconnect

function noteActivity(): void {
  lastActivityAt = Date.now()
}

function clearHeartbeat(): void {
  if (heartbeatTimer !== null) {
    clearInterval(heartbeatTimer)
    heartbeatTimer = null
  }
}

function clearWatchdog(): void {
  if (watchdogTimer !== null) {
    clearInterval(watchdogTimer)
    watchdogTimer = null
  }
}

function sendGateway(payload: Record<string, unknown>): void {
  try {
    ws?.send(JSON.stringify(payload))
  } catch (e) {
    console.error(`[discord] gateway send failed: ${e instanceof Error ? e.message : String(e)}`)
  }
}

function sendHeartbeat(): void {
  sendGateway({ op: OP_HEARTBEAT, d: lastSeq })
  heartbeatAcked = false
}

function startHeartbeat(intervalMs: number): void {
  clearHeartbeat()
  heartbeatIntervalMs = intervalMs
  // First-heartbeat JITTER (Discord mandate): the first beat fires after interval × random, to avoid a
  // thundering herd. Subsequent beats fire on the fixed interval.
  const firstDelay = Math.floor(intervalMs * Math.random())
  setTimeout(() => {
    if (!ws) return
    sendHeartbeat()
    heartbeatTimer = setInterval(() => {
      if (!ws) return
      if (!heartbeatAcked) {
        // The previous heartbeat was never ACKed — the connection is a zombie. Terminate → reconnect
        // (RESUME). This is the FASTER Discord-idiomatic primary self-heal (fires within ~1 interval),
        // leading the coarser frame-level watchdog and the framework net. Gated on the kill-switch.
        if (WATCHDOG_ARMED) {
          console.error('[discord] heartbeat not ACKed within an interval — forcing reconnect (zombie)')
          forceReconnect('network')
          return
        }
      }
      sendHeartbeat()
    }, intervalMs)
  }, firstDelay)
}

function startWatchdog(): void {
  if (!WATCHDOG_ARMED) return // A6: framework-net-only proof mode
  clearWatchdog()
  watchdogTimer = setInterval(() => {
    if (!ws) return
    if (shouldForceClose(lastActivityAt, Date.now(), WATCHDOG_WINDOW_MS)) {
      console.error(
        `[discord] no gateway frame for >${WATCHDOG_WINDOW_MS}ms — half-open zombie; forcing reconnect`,
      )
      forceReconnect('network')
    }
  }, WATCHDOG_CHECK_MS)
}

/** Force the current socket down so the close handler drives the reconnect path. */
function forceReconnect(reason: HealthReason): void {
  lastDisconnectReason = reason
  clearHeartbeat()
  try {
    ws?.close(4900, 'connector-forced reconnect')
  } catch {
    /* ignore */
  }
}

function identify(): void {
  sendGateway({
    op: OP_IDENTIFY,
    d: {
      token,
      intents: INTENTS,
      properties: { os: 'linux', browser: 'my-agent-discord', device: 'my-agent-discord' },
    },
  })
}

function resume(): void {
  sendGateway({ op: OP_RESUME, d: { token, session_id: sessionId, seq: lastSeq } })
}

/** Handle one gateway frame. */
function onGatewayMessage(raw: string): void {
  noteActivity()
  let payload: { op: number; d?: unknown; s?: number | null; t?: string | null }
  try {
    payload = JSON.parse(raw)
  } catch {
    return
  }
  if (typeof payload.s === 'number') lastSeq = payload.s

  switch (payload.op) {
    case OP_HELLO: {
      const hello = payload.d as { heartbeat_interval?: number }
      startHeartbeat(hello.heartbeat_interval ?? 41_250)
      // RESUME if we hold a session; otherwise a fresh IDENTIFY.
      if (sessionId && lastSeq !== null) resume()
      else identify()
      break
    }
    case OP_HEARTBEAT: {
      // The gateway asked for an immediate heartbeat.
      sendHeartbeat()
      break
    }
    case OP_HEARTBEAT_ACK: {
      heartbeatAcked = true
      break
    }
    case OP_RECONNECT: {
      // The gateway asked us to reconnect — RESUME on the next connection.
      console.error('[discord] gateway requested RECONNECT (op7) — reconnecting to RESUME')
      forceReconnect('transient')
      break
    }
    case OP_INVALID_SESSION: {
      // d === true → the session IS resumable; false → NOT resumable, must fresh-IDENTIFY.
      const resumable = payload.d === true
      if (resumable) {
        console.error('[discord] INVALID_SESSION (resumable) — reconnecting to RESUME')
      } else {
        // Not resumable — we cannot replay the gap. Be HONEST: this is NOT zero-loss. Drop the session
        // so the next connection does a fresh IDENTIFY, and reconnect after Discord's mandated 1-5s.
        console.error(
          '[discord] INVALID_SESSION (NOT resumable) — dropping session for a fresh IDENTIFY; the gap is NOT zero-loss',
        )
        sessionId = null
        lastSeq = null
      }
      lastDisconnectReason = 'transient'
      clearHeartbeat()
      setTimeout(
        () => {
          try {
            ws?.close(4900, 'invalid session')
          } catch {
            /* ignore */
          }
        },
        1000 + Math.floor(Math.random() * 4000),
      )
      break
    }
    case OP_DISPATCH: {
      onDispatch(payload.t ?? '', payload.d)
      break
    }
    default:
      break
  }
}

// Serialize inbound ingestion so cursor-order ≡ arrival-order EVEN when an image download awaits. A
// MESSAGE_CREATE that carries an image must download its bytes (async) before enqueue; a later text
// frame must not overtake it. Chaining every enqueue through this tail keeps the mint-at-push-time
// contract intact (cursor is still minted inside enqueue, never before an await) while preserving order.
let ingestChain: Promise<void> = Promise.resolve()

/** Handle a DISPATCH event. Phase 1 cares about READY / RESUMED / MESSAGE_CREATE (DM only). */
function onDispatch(type: string, d: unknown): void {
  if (type === 'READY') {
    const ready = d as { session_id?: string; resume_gateway_url?: string; user?: { id?: string } }
    sessionId = ready.session_id ?? null
    resumeGatewayUrl = ready.resume_gateway_url ?? null
    botUserId = ready.user?.id ?? null
    connecting = false
    reconnectAttempts = 0
    lastDisconnectReason = undefined
    console.error(`[discord] gateway READY (bot ${botUserId ?? '?'}) — DM connector live`)
    return
  }
  if (type === 'RESUMED') {
    connecting = false
    reconnectAttempts = 0
    lastDisconnectReason = undefined
    console.error('[discord] gateway RESUMED — replayed missed events (zero-loss across this reconnect)')
    return
  }
  if (type === 'MESSAGE_CREATE') {
    const msg = d as {
      id?: string
      channel_id?: string
      guild_id?: string
      content?: string
      author?: { id?: string; bot?: boolean }
      timestamp?: string
      attachments?: DiscordAttachment[]
      flags?: number
    }
    // DM-first: only messages with NO guild_id (a DM channel) from a non-bot author that is not us.
    if (msg.guild_id) return
    if (!msg.author?.id || msg.author.bot) return
    if (botUserId && msg.author.id === botUserId) return
    if (!msg.id) return
    inboundReceivedCount++
    if (!markSeen(msg.id)) return // de-dup a re-delivered event
    // Cache the DM channel so a reply reuses it (from == to; open-if-missing on send otherwise).
    if (msg.channel_id) dmChannelCache.set(msg.author.id, msg.channel_id)
    const authorId = msg.author.id
    const baseContent = msg.content ?? ''
    const timestamp = msg.timestamp ? Date.parse(msg.timestamp) : Date.now()
    const providerMessageId = msg.id
    // Feature 3 (voice): IS_VOICE_MESSAGE = 1<<13 (8192) on the message `flags` bitfield marks a true
    // voice memo (vs a plain audio-file attachment). The flag rides the MESSAGE_CREATE frame in the bot's
    // OWN DMs with NO extra gateway intent (like image/file attachment metadata — VERIFY live). When set,
    // the single audio attachment is treated as a voice note; a plain audio file is still transcribed too.
    const isVoiceMemo = typeof msg.flags === 'number' && (msg.flags & (1 << 13)) !== 0
    // MESSAGE_CONTENT intent is NOT required to receive attachment metadata on the bot's own DMs; the
    // attachments[] array is present on the frame. VERIFY live — if it arrives empty here, the fix is to
    // enable MESSAGE_CONTENT and it is surfaced, never silently dropped (see the degrade below). Phase 2
    // feature 2: ALL attachment kinds (image OR non-image file) are carried by-handle — no per-kind
    // filter, no parallel pipeline. Each attachment either becomes a MediaHandle or an honest placeholder.
    const attachments = Array.isArray(msg.attachments) ? msg.attachments : []

    // Route EVERY enqueue through the serial chain so arrival-order ≡ cursor-order even across a media
    // download await (a later text frame can't overtake an earlier file/image frame). The cursor is still
    // minted inside enqueue at push time (never before the await) — the mint-before-await collision
    // class stays closed.
    ingestChain = ingestChain
      .then(async () => {
        const handles: MediaHandle[] = []
        const degradeNotes: string[] = []
        // Preserve attachment order: a successful download yields a handle, a failure yields an honest
        // placeholder ([image] / [attachment: filename]) — never a silent drop, for images AND files.
        for (const att of attachments) {
          const h = await downloadAttachmentToHandle(att, isVoiceMemo)
          if (h) handles.push(h)
          else degradeNotes.push(degradePlaceholder(att, isVoiceMemo))
        }
        let content = baseContent
        if (degradeNotes.length > 0) {
          const note = degradeNotes.join('\n')
          content = content ? `${content}\n${note}` : note
        }
        rememberMessageText(providerMessageId, content) // so a reaction to THIS user message resolves to a snippet
        enqueue({
          from: authorId, // the raw author snowflake, VERBATIM (the framework derives trust)
          content,
          timestamp,
          providerMessageId,
          ...(handles.length > 0 ? { media: handles } : {}),
        })
      })
      .catch((e) => {
        console.error(`[discord] inbound ingest failed for message ${providerMessageId}: ${e instanceof Error ? e.message : String(e)}`)
      })
    return
  }
  if (type === 'MESSAGE_REACTION_ADD') {
    // Phase 3 inbound reaction. Requires the DIRECT_MESSAGE_REACTIONS intent (1<<14). DM-only: no
    // guild_id (mirrors MESSAGE_CREATE). IGNORE the bot's OWN reactions (d.user_id === botUserId) so a
    // reaction the bot places (transport/reaction) never loops back as an inbound event.
    const r = d as {
      user_id?: string
      channel_id?: string
      message_id?: string
      guild_id?: string
      emoji?: { id?: string | null; name?: string | null }
    }
    if (r.guild_id) return // guild reaction — DM-first connector ignores it
    if (!r.user_id || !r.message_id) return
    if (botUserId && r.user_id === botUserId) return // the bot's own reaction — no loop
    const reactingUser = r.user_id
    const emojiName = r.emoji?.name ?? undefined
    const targetMessageId = r.message_id
    if (r.channel_id) dmChannelCache.set(reactingUser, r.channel_id)
    // WA/TG parity: resolve the reacted-to message to a ≤80-char snippet so the brain knows WHICH
    // message. When resolved, carry the fully-formed note in `content` (the adapter prefers a non-empty
    // reaction content — mcp-wire-adapter.ts:1119-1124); otherwise leave it EMPTY so the framework uses
    // its generic `*reacted <emoji> to your message*` phrasing (honest degrade, never a guessed quote).
    const snippet = reactionSnippet(targetMessageId)
    const resolvedNote = snippet ? `*reacted ${emojiName ?? '?'} to: "${snippet}"*` : ''
    // Route through the SAME serialized ingest chain so cursor-order ≡ arrival-order across concurrent
    // frames (a reaction must not overtake an earlier still-downloading media message). The cursor is
    // still minted inside enqueue() at push time — the mint-at-push-time contract is preserved.
    ingestChain = ingestChain
      .then(() => {
        enqueue({
          from: reactingUser, // the UNTRUSTED raw reacting-user snowflake, VERBATIM
          content: resolvedNote, // resolved note when the target text is retained; else '' → framework generic phrasing
          timestamp: Date.now(),
          kind: 'reaction',
          ...(emojiName ? { emoji: emojiName } : {}),
          targetMessageId,
        })
      })
      .catch((e) => {
        console.error(`[discord] inbound reaction ingest failed for ${targetMessageId}: ${e instanceof Error ? e.message : String(e)}`)
      })
    return
  }
  if (type === 'MESSAGE_UPDATE') {
    // Phase 3 inbound edit. MESSAGE_UPDATE rides the SAME DIRECT_MESSAGES intent as MESSAGE_CREATE — no
    // new intent. DM-only (no guild_id). IGNORE the bot's OWN edits (author is the bot). A partial
    // update with no author or no content string (e.g. an embed/link-unfurl resolution, not a user text
    // edit) is skipped — that is a provider-internal update, not a user message being dropped.
    const msg = d as {
      id?: string
      channel_id?: string
      guild_id?: string
      content?: string
      author?: { id?: string; bot?: boolean }
      timestamp?: string
      edited_timestamp?: string
    }
    if (msg.guild_id) return
    if (!msg.id || !msg.author?.id || msg.author.bot) return
    if (botUserId && msg.author.id === botUserId) return // the bot's own edit — no loop
    if (typeof msg.content !== 'string') return // non-content update (embed/unfurl) — not a text edit
    const authorId = msg.author.id
    const newContent = msg.content
    const providerMessageId = msg.id
    const timestamp = msg.edited_timestamp
      ? Date.parse(msg.edited_timestamp)
      : msg.timestamp
        ? Date.parse(msg.timestamp)
        : Date.now()
    if (msg.channel_id) dmChannelCache.set(authorId, msg.channel_id)
    rememberMessageText(providerMessageId, newContent) // so a reaction to the EDITED message resolves to its new text
    ingestChain = ingestChain
      .then(() => {
        enqueue({
          from: authorId, // the UNTRUSTED raw author snowflake, VERBATIM
          content: newContent, // the NEW full text of the edited message
          timestamp,
          kind: 'edit',
          providerMessageId,
        })
      })
      .catch((e) => {
        console.error(`[discord] inbound edit ingest failed for ${providerMessageId}: ${e instanceof Error ? e.message : String(e)}`)
      })
    return
  }
}

/** Open the gateway WSS (LIVE mode). Bounded capped-backoff reconnect on a recoverable close;
 *  terminal (bad token / disallowed intents) → needs_attention + idle (no reconnect loop). */
function connectGateway(): void {
  if (ECHO_MODE || isCertRun() || loggedOut || intentionalClose) return
  connecting = true
  const url = resumeGatewayUrl && sessionId ? `${resumeGatewayUrl}?v=10&encoding=json` : GATEWAY_URL
  let socket: WebSocket
  try {
    socket = new WebSocket(url)
  } catch (e) {
    console.error(`[discord] gateway connect failed: ${e instanceof Error ? e.message : String(e)}`)
    scheduleReconnect('network')
    return
  }
  ws = socket
  socket.addEventListener('open', () => {
    noteActivity()
    console.error('[discord] gateway socket open — awaiting HELLO')
  })
  socket.addEventListener('message', (ev: MessageEvent) => {
    const data = typeof ev.data === 'string' ? ev.data : String(ev.data)
    onGatewayMessage(data)
  })
  socket.addEventListener('error', () => {
    // 'error' is always followed by 'close' — let the close handler drive reconnect.
    console.error('[discord] gateway socket error')
  })
  socket.addEventListener('close', (ev: CloseEvent) => {
    clearHeartbeat()
    ws = null
    if (intentionalClose) return
    handleGatewayClose(ev.code)
  })
  startWatchdog()
}

/** Discord gateway close-code policy: terminal (page a human) vs recoverable (reconnect). */
function handleGatewayClose(code: number): void {
  // Terminal, human-actionable close codes: 4004 auth failed (bad token), 4013 invalid intents,
  // 4014 disallowed intents. Do NOT reconnect-loop — page the human and idle.
  if (code === 4004 || code === 4013 || code === 4014) {
    loggedOut = true
    lastDisconnectReason = 'logged_out'
    const why =
      code === 4004
        ? 'authentication failed (invalid bot token)'
        : 'disallowed/invalid gateway intents (enable them in the Developer Portal)'
    console.error(`[discord] gateway closed ${code} — TERMINAL: ${why}; paging needs_attention and idling`)
    void mcp.notification({ method: NEEDS_ATTENTION, params: { reason: 'logged_out' } }).catch(() => {})
    return
  }
  // 4007 invalid seq, 4009 session timed out → the session is dead; fresh IDENTIFY next connect.
  if (code === 4007 || code === 4009) {
    sessionId = null
    lastSeq = null
  }
  // Everything else is environmental/transient — reconnect with capped backoff, FOREVER.
  scheduleReconnect(lastDisconnectReason ?? 'network')
}

function scheduleReconnect(reason: HealthReason): void {
  if (ECHO_MODE || isCertRun() || loggedOut || intentionalClose) return
  lastDisconnectReason = reason
  reconnectAttempts++
  // Capped exponential backoff: 1s, 2s, 4s, … up to 30s. The cap bounds the INTERVAL, not the attempt
  // count — an environmental outage is retried forever (Invariant 5).
  const backoff = Math.min(30_000, 1000 * 2 ** Math.min(reconnectAttempts - 1, 5))
  const jittered = backoff + Math.floor(Math.random() * 1000)
  console.error(`[discord] reconnecting in ${jittered}ms (attempt ${reconnectAttempts}, reason=${reason})`)
  setTimeout(connectGateway, jittered)
}

function startLive(): void {
  if (liveStarted || isCertRun()) return
  liveStarted = true
  connectGateway()
}

// ── MCP server ──────────────────────────────────────────────────────────────────────────────
const mcp = new Server({ name: 'discord', version: '1.0.0' }, { capabilities: { tools: {} } })

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: SEND_TOOL,
      description: 'send one outbound DM',
      inputSchema: {
        type: 'object',
        properties: {
          to: { type: 'string' },
          content: { type: 'string' },
          media: { type: 'array', items: { type: 'object' } },
        },
        required: ['to', 'content'],
      },
    },
    {
      name: DRAIN_TOOL,
      description: 'pull inbound after cursor',
      inputSchema: {
        type: 'object',
        properties: { sinceCursor: { type: ['string', 'null'] } },
        required: ['sinceCursor'],
      },
    },
    {
      name: HEALTH_TOOL,
      description: 'report REAL gateway-upstream liveness (never true without a live gateway socket)',
      inputSchema: { type: 'object', properties: {} },
    },
    {
      name: TYPING_TOOL,
      description: 'show/clear the typing indicator on a DM',
      inputSchema: {
        type: 'object',
        properties: {
          to: { type: 'string' },
          action: { type: 'string', enum: ['start', 'stop'] },
        },
        required: ['to', 'action'],
      },
    },
    {
      name: REACTION_TOOL,
      description: "attach the bot's emoji reaction to a message",
      inputSchema: {
        type: 'object',
        properties: {
          to: { type: 'string' },
          messageId: { type: 'string' },
          emoji: { type: 'string' },
        },
        required: ['to', 'messageId', 'emoji'],
      },
    },
    {
      name: EDIT_TOOL,
      description: "replace the text of one of the bot's sent messages",
      inputSchema: {
        type: 'object',
        properties: {
          to: { type: 'string' },
          messageId: { type: 'string' },
          content: { type: 'string' },
        },
        required: ['to', 'messageId', 'content'],
      },
    },
  ],
}))

/** A loopback send proves this is a cert run, not live → disarm the deferred live connect. */
function noteLoopback(): void {
  if (liveStarted) return // already live — never latch cert mid-session
  conformanceSeen = true
  if (deferredConnectTimer !== null) {
    clearTimeout(deferredConnectTimer)
    deferredConnectTimer = null
  }
}

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name === SEND_TOOL) {
    const { to, content, media } = req.params.arguments as {
      to: string
      content: string
      media?: MediaHandle[]
    }
    // Cert-vs-live is EXPLICIT (isCertRun), never inferred from the target shape.
    if (isCertRun()) {
      noteLoopback()
      // Conformance loopback: echo the content into the drain queue (from := to). Carry the rendered
      // wire bytes VERBATIM so the render prover sees them intact. Echo the media handles JSON-only with
      // their ids INTACT (the carry-fidelity `images` probe matches on id) — NO endpoint call, NO bytes:
      // §1 holds, and re-staging would change the id and fail the probe. NO network, NO token.
      enqueue({
        from: to,
        content,
        timestamp: Date.now(),
        ...(media && media.length > 0 ? { media } : {}),
      })
      return { content: [{ type: 'text', text: 'echoed' }] }
    }
    if (!isDeliverableTarget(to)) {
      // A LIVE send to a non-deliverable target: DROP it LOUDLY — never echo a fake inbound, never
      // latch cert, never claim 'sent'.
      console.error(`[discord] DROPPED send — target "${to}" is not a deliverable Discord snowflake`)
      return { content: [{ type: 'text', text: 'not-delivered: target is not a deliverable Discord user id' }] }
    }
    // ── LIVE send ──
    // Sending clears the typing indicator (Discord auto-clears it client-side on a message; we also
    // stop our refresh loop so a held 'start' can't re-show "…is typing" after the reply lands).
    stopTyping(to)
    // Text first (when present). A pure-media reply arrives with empty content — skip the blank text
    // message and deliver only the attachment(s).
    let messageId: string | undefined
    if (content && content.length > 0) messageId = await sendDirectMessage(to, content)
    // Then deliver each media handle (image / file / audio-voice) out-of-band: GET bytes from the endpoint
    // → POST to the DM channel (multipart for image/file, native voice-message flow for audio). Best-effort
    // — a media failure never throws past the turn (text already sent). A voice reply arrives with empty
    // content (the audio bubble IS the reply), so only the handle is delivered.
    if (media && media.length > 0) {
      for (const handle of media) {
        const mediaMsgId = await sendMediaHandle(to, handle)
        if (messageId === undefined) messageId = mediaMsgId
      }
    }
    const text = messageId !== undefined ? JSON.stringify({ messageId }) : 'sent'
    return { content: [{ type: 'text', text }] }
  }

  if (req.params.name === DRAIN_TOOL) {
    const { sinceCursor } = req.params.arguments as { sinceCursor: string | null }
    return { content: [{ type: 'text', text: JSON.stringify({ messages: drain(sinceCursor) }) }] }
  }

  if (req.params.name === HEALTH_TOOL) {
    // The never-lie gateway-liveness report. A cert run holds no real socket → synthetic "up" (the
    // CRIES-WOLF affordance the A4 probe requires). LIVE: derive STRICTLY from the real socket.
    const certRun = isCertRun()
    const report = computeGatewayHealth({
      certRun,
      sockPresent: ws !== null,
      wsReadyState: ws ? ws.readyState : undefined,
      lastActivityAt,
      now: Date.now(),
      lastDisconnectReason,
      connecting,
    })
    const reportWithInbound = {
      ...report,
      inboundReceived: inboundReceivedCount,
      inboundEnqueued: inboundEnqueuedCount,
    }
    return {
      structuredContent: reportWithInbound as unknown as Record<string, unknown>,
      content: [{ type: 'text', text: JSON.stringify(reportWithInbound) }],
    }
  }

  if (req.params.name === TYPING_TOOL) {
    const { to, action } = req.params.arguments as { to: string; action: 'start' | 'stop' }
    // Cert runs hold no gateway/token; typing is a live-only verb (no conformance probe drives it).
    if (isCertRun()) return { content: [{ type: 'text', text: 'ok' }] }
    if (!isDeliverableTarget(to)) {
      stopTyping(to)
      return { content: [{ type: 'text', text: 'not-delivered: target is not a deliverable Discord user id' }] }
    }
    if (action === 'stop') stopTyping(to)
    else await startTyping(to) // startTyping swallows its own errors — never wedges the turn
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === REACTION_TOOL) {
    const { to, messageId, emoji } = req.params.arguments as { to: string; messageId: string; emoji: string }
    if (isCertRun()) return { content: [{ type: 'text', text: 'ok' }] }
    if (!isDeliverableTarget(to)) {
      return { content: [{ type: 'text', text: 'not-delivered: target is not a deliverable Discord user id' }] }
    }
    // Best-effort: a reaction to an unknown/deleted message id fails GRACEFULLY (honest not-delivered)
    // without ever throwing past the turn.
    try {
      await reactToMessage(to, messageId, emoji)
      return { content: [{ type: 'text', text: 'reacted' }] }
    } catch (e) {
      console.error(`[discord] reaction failed: ${e instanceof Error ? e.message : String(e)}`)
      return { content: [{ type: 'text', text: 'not-delivered: reaction failed' }] }
    }
  }

  if (req.params.name === EDIT_TOOL) {
    const { to, messageId, content } = req.params.arguments as { to: string; messageId: string; content: string }
    if (isCertRun()) return { content: [{ type: 'text', text: 'ok' }] }
    if (!isDeliverableTarget(to)) {
      return { content: [{ type: 'text', text: 'not-delivered: target is not a deliverable Discord user id' }] }
    }
    // Best-effort: an edit of a message whose id is unknown (e.g. lost after a restart) or foreign fails
    // GRACEFULLY — honest not-delivered, never a wedged turn.
    try {
      const editedId = await editMessageContent(to, messageId, content)
      const text = editedId !== undefined ? JSON.stringify({ messageId: editedId }) : 'edited'
      return { content: [{ type: 'text', text }] }
    } catch (e) {
      console.error(`[discord] edit failed: ${e instanceof Error ? e.message : String(e)}`)
      return { content: [{ type: 'text', text: 'not-delivered: edit failed' }] }
    }
  }

  throw new Error(`unknown tool: ${req.params.name}`)
})

// ── Boot ──────────────────────────────────────────────────────────────────────────────────────
// On graceful shutdown (the supervisor sends SIGTERM on an intentional bounce), stop the timers and
// release the socket best-effort so nothing forces a reconnect mid-teardown.
process.on('SIGTERM', () => {
  intentionalClose = true
  clearHeartbeat()
  clearWatchdog()
  stopAllTyping()
  try {
    ws?.close(1000, 'shutdown')
  } catch {
    /* best-effort */
  }
  process.exit(0)
})

// Load the persisted cursor state (only meaningful for a live session — cert runs never persist).
if (!isCertRun()) loadCursorState()

await mcp.connect(new StdioServerTransport())

// Defer the live gateway connect until shortly AFTER the MCP handshake. The conformance harness drives
// a loopback send immediately after connecting, which disarms this timer (noteLoopback) — so a cert
// run, even in an env that carries the live token, never opens the gateway. In real operation no
// loopback send arrives, so the gateway connects after the brief grace (first inbound delayed ≤ the
// grace — zero-loss, the gateway RESUMEs / re-delivers).
const LIVE_CONNECT_GRACE_MS = 1500
if (!ECHO_MODE && !CONFORMANCE_ENV) {
  deferredConnectTimer = setTimeout(() => {
    deferredConnectTimer = null
    if (conformanceSeen) return
    startLive()
  }, LIVE_CONNECT_GRACE_MS)
}
