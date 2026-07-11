#!/usr/bin/env tsx
/**
 * Telegram (Bot API) transport connector — a self-contained MCP stdio server.
 *
 * SELF-CONTAINED: imports ONLY @modelcontextprotocol/sdk (folder-local node_modules) and inlines
 * the three wire strings. It never imports a framework module — the wire contract is protocol,
 * not code.
 *
 * CONTROL-PLANE ONLY. The framework speaks:
 *   - transport/send      {to, content}                 → transmit one outbound; return {messageId}.
 *   - transport/drain     {sinceCursor}                  → {messages:[{cursor,from,content,timestamp,...}]}.
 *   - transport/typing    {to, action:'start'|'stop'}    → drive the "typing…" indicator (best-effort).
 *   - transport/reaction  {to, messageId, emoji}         → react to a message (best-effort).
 *   - transport/edit      {to, messageId, content}       → edit a previously-sent message (best-effort).
 *   - notifications/transport/message_available          → emitted on new inbound (a WAKEUP only).
 * Never bytes on the wire, never unprompted push.
 *
 * ZERO-LOSS: the background long-poll uses offset = framework-drained cursor + 1 (NOT the highest
 * received update_id), so Telegram retains every update until the framework has drained it — a
 * connector crash loses nothing. The framework's durable cursor store is the source of truth; this
 * process's in-memory backlog is disposable.
 *
 * TWO MODES, one binary:
 *   - LIVE  (TELEGRAM_BOT_TOKEN present): talk to api.telegram.org for real.
 *   - ECHO  (no token, e.g. the conformance harness, which injects no secret): a local loopback —
 *     transport/send echoes the content straight into the drain queue (from := to) so the wire
 *     contract + render carry can be certified WITHOUT a token or any network.
 *   The echo branch is ALSO taken per-send for any non-deliverable target (the harness's synthetic
 *   `to`, e.g. 'probe'), so certification is DETERMINISTIC even when an ambient TELEGRAM_BOT_TOKEN
 *   leaks into the certifying process's env. A live reply always targets a numeric chat_id, so a
 *   real send never takes the echo branch — live behaviour is unchanged.
 *
 * M10-S9 extensions:
 *   - Sticker inbound: extracts emoji + set_name fields (→ '[sticker: 👍]' or '[sticker from set "..." : 🐻]').
 *   - Video / video_note inbound: downloads thumbnail as image handle; degrades to '[video message]' if unavailable.
 *   - Reaction inbound: message_reaction updates → WireInbound {kind:'reaction', emoji, targetMessageId}.
 *   - Edited_message inbound: edited_message → WireInbound {kind:'edit', content:newText, targetMessageId}.
 *   - transport/reaction tool: calls Telegram setMessageReaction (best-effort; connector swallows errors).
 *   - transport/edit tool: calls Telegram editMessageText (MarkdownV2 + plaintext fallback; best-effort).
 *   - transport/send returns JSON {messageId} so the brain can react/edit its own messages.
 *   - providerMessageId threaded on WireInbound plain messages to enable react-to-user's-message.
 */
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
// B4: the pure poll-staleness verdict for transport/health (dependency-free; unit-tested as a tsx
// script — see poll-health.test.ts). The long-poll timeout is sourced from here too (single source).
import { computePollHealth, isCursorRegression, LONG_POLL_TIMEOUT_S, shouldLatchConformanceSeen } from './poll-health.js'

// ── Wire strings (inlined — never import a framework module) ──────────────────────────────
const SEND_TOOL = 'transport/send'
const DRAIN_TOOL = 'transport/drain'
const TYPING_TOOL = 'transport/typing'
const REACTION_TOOL = 'transport/reaction'
const EDIT_TOOL = 'transport/edit'
// B4: the upstream-liveness report tool. Value MUST match the framework HEALTH_TOOL
// ('transport/health' in wire-protocol.ts) — inlined, never imported from a framework module.
const HEALTH_TOOL = 'transport/health'
const MESSAGE_AVAILABLE = 'notifications/transport/message_available'
// B4: control-plane notification the connector emits when a HUMAN must act (a terminal 401 →
// the token was revoked / re-pair needed) — value MUST match the framework NEEDS_ATTENTION.
const NEEDS_ATTENTION = 'notifications/transport/needs_attention'

// ── Media (S4 handle substrate, §7.2 / §1 NO BYTES ON THE WIRE) ────────────────────────────
// A MediaHandle is a small JSON descriptor — bytes NEVER ride a handle. Binary moves out-of-band
// over the localhost media HTTP endpoint (PUT/GET /api/media). Only the handle crosses the MCP wire
// (on transport/send `media` and on a drained WireInbound `media`). Inlined — self-contained.
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

/**
 * WireInbound — inlined, kept in sync with wire-protocol.ts (framework source of truth).
 * M10-S9 B3: kind / providerMessageId / targetMessageId / emoji are additive optional fields.
 */
interface WireInbound {
  cursor: string // monotonic, strictly increasing, connector-owned
  from: string // UNTRUSTED raw provider sender id, reported VERBATIM
  content: string
  timestamp: number // ms epoch
  /** JSON-only media handles accompanying the message (S4). NEVER bytes. */
  media?: MediaHandle[]
  /** 'message' (default/absent) | 'reaction' | 'edit'. Unknown → treated as 'message'. */
  kind?: 'message' | 'reaction' | 'edit'
  /** The provider's own id for THIS inbound item (message_id for messages; update_id for events). */
  providerMessageId?: string
  /** For reaction/edit: the provider id of the targeted message. */
  targetMessageId?: string
  /** For reaction events: the emoji the user reacted with. */
  emoji?: string
}

const token = process.env.TELEGRAM_BOT_TOKEN
// The provider base URL is env-overridable so a stub/harness can steer the connector (contract).
const API_BASE = process.env.TELEGRAM_API_BASE ?? 'https://api.telegram.org'
// ECHO loopback engages when there is no token (the conformance harness injects no secret) OR an
// explicit override is set. The poll loop / startup deleteWebhook are gated on this — they only run
// in genuine LIVE mode (a real token present).
const ECHO_MODE = !token || process.env.TELEGRAM_ECHO === '1'
// CONFORMANCE_MODE — the authoritative cert-run signal set by the conformance harness
// (TRANSPORT_CONFORMANCE=1). The live auto-wire path NEVER sets it. Cert-ness derives from THIS
// flag ONLY — never from message/target shape — so a live send to a non-deliverable target can no
// longer poison (latch off) the live poll loop (2026-07-11 live fix: a shape-inferred latch
// silently disarmed the long-poll while health reported a synthetic "up").
const CONFORMANCE_MODE = process.env.TRANSPORT_CONFORMANCE === '1'

// A DELIVERABLE Telegram target is a numeric chat_id (a DM's chat==sender id) or an `@username`.
// The conformance harness drives the connector entirely through synthetic targets (e.g. `to:'probe'`)
// and never delivers real inbound — so a non-deliverable target ALWAYS means "certify via loopback",
// even when an ambient TELEGRAM_BOT_TOKEN is present in the certifying process's env. This makes
// conformance deterministic regardless of ambient env: the framework only ever replies to a numeric
// `from`, so a live send never takes the echo branch.
const isDeliverableTarget = (to: string): boolean => /^\d+$/.test(to) || /^@[A-Za-z0-9_]{4,}$/.test(to)

const TG = (method: string): string => `${API_BASE}/bot${token}/${method}`
// Telegram serves file bytes from a DIFFERENT path: /file/bot<token>/<file_path>.
const TG_FILE = (filePath: string): string => `${API_BASE}/file/bot${token}/${filePath}`
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

// ── Media endpoint (S4 handle substrate, §1 NO BYTES ON THE WIRE) ───────────────────────────
// Binary moves out-of-band over the localhost media HTTP endpoint (PUT/GET /api/media); only the
// JSON MediaHandle ever crosses the MCP wire. Config is resolved lazily + best-effort: the canonical
// source is the scoped env (`MEDIA_ENDPOINT`/`MEDIA_TOKEN`, declared in requires.env). A localhost
// fallback (token file at <agentDir>/media/token, endpoint http://127.0.0.1:${PORT}) covers a pure
// code-respawn that hasn't yet re-wired the new env. If neither resolves, media degrades honestly and
// the TEXT path is entirely unaffected.
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

interface MediaPutOpts {
  mime: string
  kind: MediaKind
  caption?: string
  duration?: number
  ptt?: boolean
}

/** GET a handle's bytes from the media endpoint (out-of-band — never on the MCP wire). */
async function mediaGet(id: string): Promise<Buffer> {
  if (!MEDIA_TOKEN) throw new Error('media endpoint token unavailable')
  const res = await fetch(`${MEDIA_ENDPOINT}/api/media/${id}`, {
    headers: { 'x-media-token': MEDIA_TOKEN },
  })
  if (!res.ok) throw new Error(`media GET ${id} failed: ${res.status}`)
  return Buffer.from(await res.arrayBuffer())
}

/** PUT raw bytes to the media endpoint and receive a fresh JSON MediaHandle (out-of-band).
 *  Caption is intentionally NOT sent as a header (arbitrary user unicode/newlines would break the
 *  HTTP header); inbound captions ride the WireInbound `content` field instead. */
async function mediaPut(bytes: Buffer | Uint8Array, opts: MediaPutOpts): Promise<MediaHandle> {
  if (!MEDIA_TOKEN) throw new Error('media endpoint token unavailable')
  const headers: Record<string, string> = {
    'content-type': opts.mime,
    'x-media-token': MEDIA_TOKEN,
    'x-media-kind': opts.kind,
  }
  if (opts.duration !== undefined) headers['x-media-duration'] = String(opts.duration)
  if (opts.ptt !== undefined) headers['x-media-ptt'] = String(opts.ptt)
  const res = await fetch(`${MEDIA_ENDPOINT}/api/media`, { method: 'PUT', headers, body: bytes })
  if (!res.ok) throw new Error(`media PUT failed: ${res.status}`)
  const { handle } = (await res.json()) as { handle: MediaHandle }
  return handle
}

/** Telegram getFile → download bytes → PUT to the media endpoint → MediaHandle. Best-effort: any
 *  failure (no token, getFile/download/PUT error) returns null so the caller can degrade honestly. */
async function downloadToHandle(
  fileId: string,
  opts: { kind: MediaKind; mime: string; ptt?: boolean; duration?: number },
): Promise<MediaHandle | null> {
  try {
    if (!MEDIA_TOKEN) return null
    const fileRes = await fetch(TG('getFile'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ file_id: fileId }),
    })
    if (!fileRes.ok) return null
    const fileBody = (await fileRes.json()) as { ok?: boolean; result?: { file_path?: string } }
    const filePath = fileBody.result?.file_path
    if (!filePath) return null
    const dlRes = await fetch(TG_FILE(filePath))
    if (!dlRes.ok) return null
    const bytes = Buffer.from(await dlRes.arrayBuffer())
    return await mediaPut(bytes, {
      mime: opts.mime,
      kind: opts.kind,
      ...(opts.ptt !== undefined ? { ptt: opts.ptt } : {}),
      ...(opts.duration !== undefined ? { duration: opts.duration } : {}),
    })
  } catch {
    return null
  }
}

/** Outbound: GET a handle's bytes and deliver via the right Telegram method (multipart form).
 *  image→sendPhoto, audio→sendVoice (OGG/Opus PTT bubble), file→sendDocument. Best-effort — a media
 *  failure never throws past the turn (the text has already been delivered). */
async function sendMediaHandle(chatId: number | string, handle: MediaHandle): Promise<void> {
  try {
    const bytes = await mediaGet(handle.id)
    const form = new FormData()
    form.append('chat_id', String(chatId))
    const blob = new Blob([bytes], { type: handle.mimeType })
    if (handle.kind === 'image') {
      form.append('photo', blob, handle.caption ?? 'image')
      if (handle.caption) form.append('caption', handle.caption)
      await tgSendForm('sendPhoto', form)
    } else if (handle.kind === 'audio') {
      // sendVoice renders an OGG/Opus PTT bubble (a playable voice note), not a file attachment.
      form.append('voice', blob, 'voice.ogg')
      if (handle.duration !== undefined) form.append('duration', String(handle.duration))
      if (handle.caption) form.append('caption', handle.caption)
      await tgSendForm('sendVoice', form)
    } else {
      form.append('document', blob, handle.caption ?? `file-${handle.id}`)
      if (handle.caption) form.append('caption', handle.caption)
      await tgSendForm('sendDocument', form)
    }
  } catch (e) {
    console.warn(
      `[media] outbound handle ${handle.id} (${handle.kind}) failed: ${e instanceof Error ? e.message : String(e)}`,
    )
  }
}

async function tgSendForm(method: string, form: FormData): Promise<void> {
  const res = await fetch(TG(method), { method: 'POST', body: form })
  if (!res.ok) {
    const body = await res.text().catch(() => '')
    throw new Error(`${method} failed (${res.status}): ${body}`)
  }
}

/** Honest-degrade content for media we can't store/forward — caption verbatim + a `[kind]`
 *  placeholder, never a silent drop (S8 Finding-5 contract). */
function degradeContent(kind: string, caption: string): string {
  const placeholder = `[${kind}]`
  return caption ? `${caption}\n${placeholder}` : placeholder
}

// ── Zero-loss cursor↔offset tracker (LIVE mode) ───────────────────────────────────────────
// The getUpdates offset advances ONLY up to the framework-drained cursor, so undrained updates
// stay on Telegram's server and survive a restart. drain() is NON-consuming (the framework's
// durable cursor is the source of truth) and records the high watermark for the next poll offset.
const backlog: WireInbound[] = []
let localHighestSeen = 0
let lastDrainedCursor: number | null = null

interface TelegramFileRef {
  file_id: string
  file_unique_id?: string
  file_size?: number
}
interface TelegramPhotoSize extends TelegramFileRef {
  width: number
  height: number
}
interface TelegramUpdate {
  update_id: number
  message?: {
    message_id: number
    from?: { id: number; first_name?: string; username?: string }
    chat: { id: number; type: string }
    date: number // seconds epoch
    text?: string
    caption?: string
    photo?: TelegramPhotoSize[]
    voice?: TelegramFileRef & { duration?: number; mime_type?: string }
    audio?: TelegramFileRef & { duration?: number; mime_type?: string }
    document?: TelegramFileRef & { mime_type?: string; file_name?: string }
    sticker?: TelegramFileRef & { emoji?: string; set_name?: string }
    video?: TelegramFileRef & { thumbnail?: TelegramFileRef; duration?: number; mime_type?: string }
    animation?: TelegramFileRef
    video_note?: TelegramFileRef & { thumbnail?: TelegramFileRef; duration?: number }
  }
  /** Inbound reaction event (Bot API 7.0+). User reacted to a message. */
  message_reaction?: {
    chat: { id: number; type: string }
    message_id: number
    user?: { id: number; first_name?: string; username?: string }
    actor_chat?: { id: number; type: string; title?: string }
    date: number // seconds epoch
    old_reaction: Array<{ type: string; emoji?: string; custom_emoji_id?: string }>
    new_reaction: Array<{ type: string; emoji?: string; custom_emoji_id?: string }>
  }
  /** Inbound edited message. The `message_id` is the id of the original message. */
  edited_message?: {
    message_id: number
    from?: { id: number; first_name?: string; username?: string }
    chat: { id: number; type: string }
    date: number
    edit_date: number // seconds epoch of the edit
    text?: string
    caption?: string
  }
}

type IngestMessage = NonNullable<TelegramUpdate['message']>

/** Build the WireInbound entry for one message — text, photo, voice, audio, document, or an
 *  honest-degrade placeholder for an unsupported kind. Media downloads happen here (async); the
 *  caption rides the `content` field (and, for media, accompanies the JSON handle). NEVER bytes.
 *  M10-S9: providerMessageId is set on all message types; sticker emoji/set_name extracted;
 *  video/video_note thumbnail downloaded as image handle. */
async function buildInboundEntry(updateId: number, msg: IngestMessage): Promise<WireInbound> {
  // In a DM the sender id equals the chat_id — a stable owner identity AND a valid reply target.
  const base = {
    cursor: String(updateId),
    from: String(msg.from!.id),
    timestamp: msg.date * 1000, // Telegram is seconds; the wire is ms
    providerMessageId: String(msg.message_id), // M10-S9 B3: enables react_to_message
  }
  const caption = msg.caption ?? ''

  // Plain text.
  if (msg.text) return { ...base, content: msg.text }

  // Photo → largest PhotoSize (Telegram returns ascending sizes).
  if (msg.photo && msg.photo.length > 0) {
    const largest = msg.photo[msg.photo.length - 1]
    const handle = await downloadToHandle(largest.file_id, { kind: 'image', mime: 'image/jpeg' })
    return handle
      ? { ...base, content: caption, media: [handle] }
      : { ...base, content: degradeContent('photo', caption) }
  }

  // Voice note (OGG/Opus PTT) → ptt:true drives the framework's voice-in→voice-out reply.
  if (msg.voice) {
    const handle = await downloadToHandle(msg.voice.file_id, {
      kind: 'audio',
      mime: msg.voice.mime_type ?? 'audio/ogg',
      ptt: true,
      ...(msg.voice.duration !== undefined ? { duration: msg.voice.duration } : {}),
    })
    return handle
      ? { ...base, content: caption, media: [handle] }
      : { ...base, content: degradeContent('voice', caption) }
  }

  // Audio (a music file) — audio kind but NOT ptt (it's not a voice note → no voice reply trigger).
  if (msg.audio) {
    const handle = await downloadToHandle(msg.audio.file_id, {
      kind: 'audio',
      mime: msg.audio.mime_type ?? 'audio/mpeg',
      ...(msg.audio.duration !== undefined ? { duration: msg.audio.duration } : {}),
    })
    return handle
      ? { ...base, content: caption, media: [handle] }
      : { ...base, content: degradeContent('audio', caption) }
  }

  // Document (a generic file).
  if (msg.document) {
    const handle = await downloadToHandle(msg.document.file_id, {
      kind: 'file',
      mime: msg.document.mime_type ?? 'application/octet-stream',
    })
    if (handle) {
      if (msg.document.file_name) handle.caption = handle.caption ?? msg.document.file_name
      return { ...base, content: caption, media: [handle] }
    }
    return { ...base, content: degradeContent('document', caption) }
  }

  // Sticker (M10-S9): extract emoji + set_name for a readable description.
  // Falls back to '[sticker]' when no emoji is available (e.g. custom emoji without a shortcut).
  if (msg.sticker) {
    const emoji = msg.sticker.emoji
    const setName = msg.sticker.set_name
    let stickerContent: string
    if (emoji && setName) {
      stickerContent = `[sticker from set "${setName}": ${emoji}]`
    } else if (emoji) {
      stickerContent = `[sticker: ${emoji}]`
    } else {
      stickerContent = '[sticker]'
    }
    return { ...base, content: stickerContent }
  }

  // Video / video_note (M10-S9): download thumbnail as image handle; video file as handle if available.
  // The framework's vision system will describe the thumbnail image.
  if (msg.video || msg.video_note) {
    const videoData = msg.video ?? msg.video_note
    const isVideoNote = !!msg.video_note
    const label = isVideoNote ? 'video note' : 'video message'
    const handles: MediaHandle[] = []
    let downloadedThumbnail = false

    // Download thumbnail as image handle for vision processing.
    if (videoData?.thumbnail?.file_id) {
      const thumbHandle = await downloadToHandle(videoData.thumbnail.file_id, {
        kind: 'image',
        mime: 'image/jpeg',
      })
      if (thumbHandle) {
        handles.push(thumbHandle)
        downloadedThumbnail = true
      }
    }

    // Download the video file itself as a file handle.
    const videoFileHandle = await downloadToHandle(videoData.file_id, {
      kind: 'file',
      mime: videoData.mime_type ?? 'video/mp4',
      ...(videoData.duration !== undefined ? { duration: videoData.duration } : {}),
    })
    if (videoFileHandle) {
      handles.push(videoFileHandle)
    }

    // Return with whatever media we successfully downloaded (thumbnail for vision, video as backup).
    if (handles.length > 0) {
      return { ...base, content: caption || `[${label}]`, media: handles }
    }

    // No media could be downloaded: honest degrade with caption.
    return { ...base, content: degradeContent(label, caption) }
  }

  // Animation (GIF) — honest degrade.
  if (msg.animation) {
    return { ...base, content: degradeContent('animation', caption) }
  }

  // Unknown kind — honest degrade, never silent.
  return { ...base, content: degradeContent('unsupported message', caption) }
}

/** Enqueue updates with update_id > localHighestSeen (idempotent re-ingest), preserving update_id
 *  order. Media updates await download+PUT BEFORE the backlog push, so a photo and a following text
 *  from the same sender keep their order on the wire (the framework then enforces per-`from` FIFO
 *  dispatch). The seen-watermark advances per update so a media failure can never wedge the poll
 *  offset. Returns count added. */
async function ingest(updates: TelegramUpdate[]): Promise<number> {
  let added = 0
  for (const u of updates) {
    if (u.update_id <= localHighestSeen) continue // already seen — Telegram re-returned it
    inboundReceived++

    // ── Reaction (M10-S9) ──────────────────────────────────────────────────────────────────
    if (u.message_reaction) {
      const rxn = u.message_reaction
      // Prefer user.id (named user) over actor_chat.id (anonymous group admin).
      const userId = rxn.user?.id ?? rxn.actor_chat?.id
      if (userId) {
        // Collect only the EMOJI reactions from new_reaction (ignores custom emoji / removals).
        const emojis = rxn.new_reaction
          .filter((r) => r.type === 'emoji' && r.emoji)
          .map((r) => r.emoji as string)
        // Forward only when new emoji are present (removing a reaction → empty new_reaction → skip).
        if (emojis.length > 0) {
          const emojiStr = emojis.join(' ')
          backlog.push({
            cursor: String(u.update_id),
            from: String(userId),
            content: emojiStr, // the adapter uses emoji field primarily, content as fallback
            timestamp: rxn.date * 1000,
            kind: 'reaction',
            emoji: emojiStr,
            targetMessageId: String(rxn.message_id),
            providerMessageId: String(u.update_id),
          })
          added++
        }
      }
      if (u.update_id > localHighestSeen) localHighestSeen = u.update_id
      continue
    }

    // ── Edited message (M10-S9) ────────────────────────────────────────────────────────────
    if (u.edited_message) {
      const edit = u.edited_message
      if (edit.from) {
        const editedText = edit.text ?? edit.caption ?? ''
        backlog.push({
          cursor: String(u.update_id),
          from: String(edit.from.id),
          content: editedText,
          timestamp: edit.edit_date * 1000,
          kind: 'edit',
          targetMessageId: String(edit.message_id),
          providerMessageId: String(u.update_id),
        })
        added++
      }
      if (u.update_id > localHighestSeen) localHighestSeen = u.update_id
      continue
    }

    // ── Regular message ────────────────────────────────────────────────────────────────────
    const msg = u.message
    if (!msg || !msg.from) {
      // Advance the seen-watermark so a non-message update can't wedge the poll offset.
      if (u.update_id > localHighestSeen) localHighestSeen = u.update_id
      continue
    }
    try {
      backlog.push(await buildInboundEntry(u.update_id, msg))
      added++
    } catch {
      // Defensive: buildInboundEntry already degrades on download failure, but never let an
      // unexpected throw wedge the watermark — degrade to a placeholder and keep advancing.
      backlog.push({
        cursor: String(u.update_id),
        from: String(msg.from.id),
        content: degradeContent('media', msg.caption ?? ''),
        timestamp: msg.date * 1000,
      })
      added++
    }
    localHighestSeen = u.update_id
  }
  inboundEnqueued += added
  return added
}

/** All backlog entries with cursor > sinceCursor, oldest-first. NON-consuming; records the high
 *  watermark so the next poll offset advances past what the framework has acknowledged. */
function drain(sinceCursor: string | null): WireInbound[] {
  const after = sinceCursor != null ? Number(sinceCursor) : 0
  const out = backlog
    .filter((m) => Number(m.cursor) > after)
    .sort((a, b) => Number(a.cursor) - Number(b.cursor))
  if (out.length > 0) everReturnedEntries = true
  else if (
    isCursorRegression({ returnedNow: out.length, backlogSize: backlog.length, everReturned: everReturnedEntries }) &&
    after !== lastRegressionWarnAfter
  ) {
    lastRegressionWarnAfter = after
    console.warn(
      `[telegram] CURSOR REGRESSION SUSPECTED: drain(sinceCursor=${sinceCursor}) matched 0 of ` +
        `${backlog.length} buffered update(s) and nothing has ever drained this process — the durable ` +
        `cursor appears to come from a DIFFERENT bot/update-id space (e.g. inherited across a bot ` +
        `swap). Inbound is stalled until the framework's cursor store for this transport is reset.`,
    )
  }
  const maxReturned = out.length > 0 ? Number(out[out.length - 1].cursor) : Number.NEGATIVE_INFINITY
  const candidate = Math.max(lastDrainedCursor ?? Number.NEGATIVE_INFINITY, after, maxReturned)
  if (candidate !== Number.NEGATIVE_INFINITY) lastDrainedCursor = candidate
  return out
}

/** getUpdates offset = lastDrainedCursor + 1; undefined until the first drain (fetch full backlog). */
function pollOffset(): number | undefined {
  return lastDrainedCursor === null ? undefined : lastDrainedCursor + 1
}

// ── LIVE send: parse_mode MarkdownV2 with a MANDATORY plaintext fallback ────────────────────
/** Is this a Telegram markup/parse failure (→ safe + correct to resend the same text as plain)?
 *  Only an HTTP 400 whose description names an entity-parse problem; any other 400 or a 5xx is not. */
function isMarkupParseError(status: number, description: string | undefined): boolean {
  if (status !== 400) return false
  return /can't parse entities|parse entities|byte offset|reserved character|can't find/i.test(description ?? '')
}

interface PostResult { ok: boolean; status: number; description?: string; body?: string; messageId?: number }

async function postSendMessage(payload: Record<string, unknown>): Promise<PostResult> {
  const resp = await fetch(TG('sendMessage'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (resp.ok) {
    // Parse the response to capture the Telegram message_id (M10-S9 B1).
    try {
      const body = (await resp.json()) as { result?: { message_id?: number } }
      return { ok: true, status: resp.status, messageId: body.result?.message_id }
    } catch {
      return { ok: true, status: resp.status }
    }
  }
  const body = await resp.text().catch(() => '')
  let description: string | undefined
  try {
    description = (JSON.parse(body) as { description?: string }).description
  } catch {
    /* non-JSON body — leave description undefined */
  }
  return { ok: false, status: resp.status, description, body }
}

/** Send rich MarkdownV2; on a markup-parse 400, resend the IDENTICAL text WITHOUT parse_mode so a
 *  markup error never loses the message. Any other failure throws (surfaces to the supervisor).
 *  Returns the Telegram message_id on success (M10-S9 B1 — enables react_to_message). */
async function sendTelegramMessage(chatId: number | string, text: string): Promise<number | undefined> {
  const first = await postSendMessage({ chat_id: chatId, text, parse_mode: 'MarkdownV2' })
  if (first.ok) return first.messageId
  if (isMarkupParseError(first.status, first.description)) {
    const plain = await postSendMessage({ chat_id: chatId, text })
    if (plain.ok) {
      console.warn('[parse_mode fallback] telegram rejected MarkdownV2 markup; resent as plain text')
      return plain.messageId
    }
    throw new Error(`telegram sendMessage (plaintext fallback) failed (${plain.status}): ${plain.body ?? ''}`)
  }
  throw new Error(`telegram sendMessage failed (${first.status}): ${first.body ?? ''}`)
}

// ── Typing indicator (LIVE mode) — best-effort, never breaks a turn ─────────────────────────
// Telegram's "typing…" auto-expires after ~5 s, so a held indicator is refreshed below that.
// A transport/send to the active chat auto-clears it client-side → stop refreshing on send.
const TYPING_REFRESH_MS = 4000
let typingChat: number | string | null = null
let typingTimer: ReturnType<typeof setInterval> | null = null

async function postChatAction(chatId: number | string): Promise<void> {
  try {
    const resp = await fetch(TG('sendChatAction'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, action: 'typing' }),
    })
    if (!resp.ok) console.warn(`[typing] sendChatAction ${chatId} → HTTP ${resp.status}`)
  } catch (e) {
    console.warn(`[typing] sendChatAction ${chatId} failed: ${e instanceof Error ? e.message : String(e)}`)
  }
}

function clearTyping(): void {
  if (typingTimer !== null) {
    clearInterval(typingTimer)
    typingTimer = null
  }
  typingChat = null
}

function startTyping(chatId: number | string): void {
  clearTyping()
  typingChat = chatId
  void postChatAction(chatId) // immediate
  typingTimer = setInterval(() => {
    if (typingChat !== null) void postChatAction(typingChat)
  }, TYPING_REFRESH_MS)
}

function noteSend(chatId: number | string): void {
  if (typingChat !== null && typingChat === chatId) clearTyping()
}

// ── Reaction send (M10-S9) — best-effort, never breaks a turn ──────────────────────────────
/** Send a reaction to a Telegram message via setMessageReaction (Bot API 7.0+). Best-effort. */
async function sendReaction(chatId: number | string, messageId: string, emoji: string): Promise<void> {
  try {
    const resp = await fetch(TG('setMessageReaction'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        message_id: Number(messageId),
        reaction: [{ type: 'emoji', emoji }],
        is_big: false,
      }),
    })
    if (!resp.ok) {
      const body = await resp.text().catch(() => '')
      console.warn(`[reaction] setMessageReaction failed (${resp.status}): ${body}`)
    }
  } catch (e) {
    console.warn(`[reaction] setMessageReaction failed: ${e instanceof Error ? e.message : String(e)}`)
  }
}

// ── Edit send (M10-S9) — best-effort, MarkdownV2 + plaintext fallback ───────────────────────
/** Edit a previously-sent Telegram message. MarkdownV2 first; falls back to plain on parse error. */
async function editTelegramMessage(chatId: number | string, messageId: string, text: string): Promise<void> {
  try {
    const payload: Record<string, unknown> = {
      chat_id: chatId,
      message_id: Number(messageId),
      text,
      parse_mode: 'MarkdownV2',
    }
    const resp = await fetch(TG('editMessageText'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    })
    if (resp.ok) return
    const body = await resp.text().catch(() => '')
    let description: string | undefined
    try { description = (JSON.parse(body) as { description?: string }).description } catch { /* */ }
    if (isMarkupParseError(resp.status, description)) {
      // Resend without parse_mode
      const plain = await fetch(TG('editMessageText'), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, message_id: Number(messageId), text }),
      })
      if (!plain.ok) console.warn(`[edit] editMessageText (plain) failed (${plain.status})`)
    } else {
      console.warn(`[edit] editMessageText failed (${resp.status}): ${body}`)
    }
  } catch (e) {
    console.warn(`[edit] editMessageText failed: ${e instanceof Error ? e.message : String(e)}`)
  }
}

// ── ECHO loopback (conformance mode, no token) ──────────────────────────────────────────────
let echoSeq = 0

// ── Live-poll gating (conformance-safe) ─────────────────────────────────────────────────────
// A loopback send (the echo branch below) proves the connector is being driven by the conformance
// harness, NOT running live — so it DISARMS the deferred long-poll before any network I/O. This
// matters because the certifying process inherits the running dashboard's env (a live
// TELEGRAM_BOT_TOKEN IS present) AND a real bot may be polling concurrently: without this gate a
// cert spawn calls getUpdates and 409-contends with the live bot's poller (and, in any gap where
// the live bot is down, could ACK/steal its queued updates). The grace is anchored AFTER the MCP
// handshake (not process spawn) so a slow `npx tsx` cold start can't fire it early; in real
// operation no loopback send arrives, so the poll starts after the grace (first inbound delayed
// ≤ the grace — zero-loss, Telegram retains every update until the framework drains it).
const LIVE_POLL_GRACE_MS = 1500
let liveStarted = false
let conformanceSeen = false
let deferredPollTimer: ReturnType<typeof setTimeout> | null = null
// B4: poll-staleness state feeding transport/health. `lastSuccessfulGetUpdatesAt` is bumped on every
// 2xx getUpdates (and anchored at arm-time so a freshly-armed poll isn't instantly "stale"); the
// transport/health verdict reports the upstream down once it goes past the cadence window. `pollLoggedOut`
// latches a hard 401 (token revoked) — terminal, so the framework pages rather than spins (Invariant 5).
let lastSuccessfulGetUpdatesAt = 0
let pollLoggedOut = false
// Credential probe: latches when the ONE-SHOT startup getMe probe returns 401/404 — the token is
// invalid or incomplete (e.g. a half-copied BotFather token). TERMINAL: fed to transport/health as
// reason:'credential' (never 'network' — a recycle respawns into the identical death) and paged
// ONCE via needs_attention with a plain-language message. Probed exactly once per process start —
// never retry-looped.
let credentialInvalid = false
// Divergence-net counters (2a-ii Telegram parity, 2026-07-11): the framework's inbound
// divergence net ("connector reports traffic arriving but nothing drained") is BLIND unless the
// connector self-reports these. `inboundReceived` = genuinely new updates seen by the poll
// (post de-dup); `inboundEnqueued` = entries actually buffered for drain. Reported via
// transport/health so a silent inbound stall (e.g. a poisoned cursor) pages instead of hiding
// behind a green dot.
let inboundReceived = 0
let inboundEnqueued = 0
// Cursor-regression tripwire state (see poll-health.ts isCursorRegression).
let everReturnedEntries = false
let lastRegressionWarnAfter: number | null = null
// 2a.4 (2): ms-epoch of the most recent getUpdates 409 conflict (another poller active — the OLD
// child during a supervised deploy restart). Fed to transport/health so a would-be-stale poll is
// reported transient (defer) within the settle-grace, not network (which would self-recycle a
// just-restarted bot before its first 2xx). Cleared on a 2xx success.
let last409At = 0

function noteLoopback(): void {
  // Cert-ness derives ONLY from the authoritative TRANSPORT_CONFORMANCE flag — never from target
  // shape. In LIVE mode a send to a non-deliverable target must NOT latch conformanceSeen (that
  // was the poll-poisoning bug: a live non-deliverable send disarmed the long-poll forever).
  if (!CONFORMANCE_MODE) return
  // 2a-ii latch guard (architect gate, M10-S9.10) — see poll-health.ts's shouldLatchConformanceSeen
  // for the full contract. Pure decision, thin wiring here.
  if (!shouldLatchConformanceSeen(liveStarted)) return
  conformanceSeen = true
  if (deferredPollTimer !== null) {
    clearTimeout(deferredPollTimer)
    deferredPollTimer = null
  }
}

/** ONE-SHOT startup credential probe (LIVE mode only — never runs in ECHO/cert). getMe is the
 *  cheapest authenticated call: a 401 (unauthorized) or 404 (malformed/incomplete token — the
 *  BotFather half-copy) is TERMINAL. Latch `credentialInvalid` so transport/health reports
 *  reason:'credential' (a human must re-supply the secret) and page ONCE via needs_attention with
 *  a plain-language message. Any other outcome (2xx, 5xx, network error) is NOT a credential
 *  verdict — the poll loop's own backoff/staleness handles those. Runs exactly once per process
 *  start; never retry-looped. */
async function probeCredentialOnce(): Promise<void> {
  try {
    const resp = await fetch(TG('getMe'), { method: 'POST' })
    if (resp.status === 401 || resp.status === 404) {
      credentialInvalid = true
      console.warn(
        `[telegram] getMe ${resp.status} — the bot token is invalid or incomplete; paging needs_attention (credential) and not arming the poll (terminal)`,
      )
      void mcp
        .notification({
          method: NEEDS_ATTENTION,
          params: { reason: 'credential', detail: 'the bot token is invalid or incomplete' },
        })
        .catch(() => {})
    }
  } catch {
    // Network error ≠ credential verdict — never latch terminal on an environmental failure.
  }
}

function startLivePolling(): void {
  if (liveStarted || conformanceSeen || ECHO_MODE || CONFORMANCE_MODE) return
  liveStarted = true
  // B4: anchor the cadence clock at arm-time so transport/health doesn't read a freshly-armed poll as
  // instantly stale before the first getUpdates returns (a long-poll cycle can take up to the timeout).
  lastSuccessfulGetUpdatesAt = Date.now()
  void (async () => {
    // Credential probe FIRST (one getMe per process start): a 401/404 verdict is terminal — do NOT
    // arm the poll (getUpdates would only re-die on the same bad token, the silent recycle loop this
    // probe exists to kill) and do NOT clear the webhook. needs_attention has already paged;
    // transport/health now answers reason:'credential'.
    await probeCredentialOnce()
    if (credentialInvalid) return
    // Telegram rejects getUpdates while a webhook is set; clear it so long-polling works.
    void fetch(TG('deleteWebhook'), { method: 'POST' }).catch(() => {})
    void pollLoop()
  })()
}

// ── MCP server ──────────────────────────────────────────────────────────────────────────────
const mcp = new Server({ name: 'telegram', version: '1.0.0' }, { capabilities: { tools: {} } })

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: SEND_TOOL,
      description: 'send one outbound message',
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
      name: TYPING_TOOL,
      description: 'drive the typing indicator',
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
      description: 'react to a message with an emoji (best-effort)',
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
      description: 'edit a previously-sent message (best-effort)',
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
    {
      name: HEALTH_TOOL,
      description: 'report REAL poll-upstream liveness (false/stale when getUpdates is not succeeding)',
      inputSchema: { type: 'object', properties: {} },
    },
  ],
}))

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name === SEND_TOOL) {
    const { to, content, media } = req.params.arguments as {
      to: string
      content: string
      media?: MediaHandle[]
    }
    const chatId = /^\d+$/.test(to) ? Number(to) : to
    // Echo when in ECHO_MODE (no token) OR the target is not a deliverable Telegram address (the
    // conformance harness's synthetic targets). A live reply always targets a numeric `from`, so a
    // real send never falls here — this keeps certification deterministic without ever requiring a
    // real network call or a token at cert time.
    if (ECHO_MODE || !isDeliverableTarget(to)) {
      // Conformance loopback: echo the content into the drain queue (from := to) so the wire
      // contract + render carry certify without a token or network. Carry the content VERBATIM
      // so the render prover sees the framework's rendered MarkdownV2 bytes intact. Echo the media
      // handles JSON-only with their ids INTACT (the carry-fidelity `images` probe matches on id) —
      // NO endpoint call, NO bytes: §1 holds, and re-staging would change the id and fail the probe.
      noteLoopback() // a loopback send proves this is a cert run, not live → disarm the long-poll
      inboundEnqueued++
      backlog.push({
        cursor: String(++echoSeq),
        from: to,
        content,
        timestamp: Date.now(),
        ...(media && media.length > 0 ? { media } : {}),
      })
      await mcp.notification({ method: MESSAGE_AVAILABLE, params: {} })
      return { content: [{ type: 'text', text: 'sent' }] }
    }
    noteSend(chatId) // the outbound auto-clears the typing indicator client-side
    // Send the text first (when present). A pure media reply (e.g. a synthesized voice note) arrives
    // with empty content — skip the blank text message and deliver only the media bubble.
    let messageId: number | undefined
    if (content && content.length > 0) messageId = await sendTelegramMessage(chatId, content)
    // Then deliver each media handle out-of-band: GET bytes from the endpoint → Telegram sendPhoto /
    // sendVoice (PTT) / sendDocument. Best-effort — a media failure never throws past the turn.
    if (media && media.length > 0) {
      for (const handle of media) await sendMediaHandle(chatId, handle)
    }
    // Return messageId as JSON so the framework can thread it for reactions/edits (M10-S9 B1).
    const result = messageId !== undefined ? JSON.stringify({ messageId: String(messageId) }) : 'sent'
    return { content: [{ type: 'text', text: result }] }
  }

  if (req.params.name === DRAIN_TOOL) {
    const { sinceCursor } = req.params.arguments as { sinceCursor: string | null }
    return { content: [{ type: 'text', text: JSON.stringify({ messages: drain(sinceCursor) }) }] }
  }

  if (req.params.name === TYPING_TOOL) {
    const { to, action } = req.params.arguments as { to: string; action: 'start' | 'stop' }
    if (!ECHO_MODE && isDeliverableTarget(to)) {
      const chatId = /^\d+$/.test(to) ? Number(to) : to
      if (action === 'start') startTyping(chatId)
      else clearTyping()
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === REACTION_TOOL) {
    // Best-effort — swallow errors, never break a turn.
    const { to, messageId, emoji } = req.params.arguments as { to: string; messageId: string; emoji: string }
    if (!ECHO_MODE && isDeliverableTarget(to)) {
      const chatId = /^\d+$/.test(to) ? Number(to) : to
      await sendReaction(chatId, messageId, emoji)
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === EDIT_TOOL) {
    // Best-effort — swallow errors, never break a turn.
    const { to, messageId, content } = req.params.arguments as { to: string; messageId: string; content: string }
    if (!ECHO_MODE && isDeliverableTarget(to)) {
      const chatId = /^\d+$/.test(to) ? Number(to) : to
      await editTelegramMessage(chatId, messageId, content)
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === HEALTH_TOOL) {
    // B4: the never-lie poll-liveness report. Derive STRICTLY from the real poll state — never report
    // true while getUpdates is stalled past the cadence window or after a terminal 401. A cert run
    // (ECHO_MODE || conformanceSeen) holds no live poll → reports a synthetic "up" (the CRIES-WOLF
    // affordance the A4 probe requires). See poll-health.ts for the full contract.
    const report = computePollHealth({
      certRun: ECHO_MODE || conformanceSeen,
      liveStarted,
      loggedOut: pollLoggedOut,
      credentialInvalid, // one-shot startup getMe verdict — terminal 'credential', never 'network'
      lastSuccessfulGetUpdatesAt,
      last409At, // 2a.4 (2): the settle-guard input
      now: Date.now(),
    })
    // Divergence-net counters ride the same report (additive fields — the framework treats
    // their absence as "connector opted out", so shipping them ARMS the net).
    const reportWithCounters = { ...report, inboundReceived, inboundEnqueued }
    // Return BOTH structuredContent and a JSON text block (the framework reads structuredContent
    // first, then falls back to parsing the text block — SDK-shape resilient).
    return {
      structuredContent: reportWithCounters as unknown as Record<string, unknown>,
      content: [{ type: 'text', text: JSON.stringify(reportWithCounters) }],
    }
  }

  throw new Error(`unknown tool: ${req.params.name}`)
})

/** Background long-poll (LIVE mode only) — never crashes the process on a transient error. */
async function pollLoop(): Promise<void> {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      const offset = pollOffset()
      const resp = await fetch(TG('getUpdates'), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          ...(offset !== undefined ? { offset } : {}),
          timeout: LONG_POLL_TIMEOUT_S,
          // M10-S9: include message_reaction + edited_message so reactions and edits are surfaced.
          allowed_updates: ['message', 'message_reaction', 'edited_message'],
        }),
      })
      // B4: a hard 401 = the bot token is revoked / unauthorized — a TERMINAL condition. A human must
      // supply a new token (re-pair). Page once via needs_attention and STOP the loop — never spin a
      // terminal error (Invariant 5: terminal-vs-environmental). transport/health then reports
      // reason:logged_out, and the framework holds restart rather than recycling forever.
      if (resp.status === 401) {
        pollLoggedOut = true
        console.warn('[telegram] getUpdates 401 unauthorized — bot token revoked; paging needs_attention and halting the poll (terminal)')
        void mcp.notification({ method: NEEDS_ATTENTION, params: { reason: 'logged_out' } }).catch(() => {})
        return
      }
      // 409 = another getUpdates poller is active (the OLD child during a supervised deploy restart);
      // any other non-ok = transient/environmental. Back off and retry FOREVER — never strand on a
      // recoverable error (Invariant 5). 2a.4 (2): record the 409 timestamp so transport/health treats
      // a would-be-stale poll as transient (defer) within the settle-grace instead of recycling a
      // just-restarted bot. The cadence-staleness check surfaces a poll that never recovers.
      if (resp.status === 409) last409At = Date.now()
      if (resp.status === 409 || !resp.ok) {
        await sleep(2000)
        continue
      }
      // B4: a successful (2xx) getUpdates — record the cadence anchor that transport/health reads and
      // clear the 409 settle marker (contention resolved — the old child is gone).
      lastSuccessfulGetUpdatesAt = Date.now()
      last409At = 0
      const body = (await resp.json()) as { result?: TelegramUpdate[] }
      const newCount = await ingest(body.result ?? [])
      if (newCount > 0) await mcp.notification({ method: MESSAGE_AVAILABLE, params: {} })
      // No NEW updates: Telegram may re-return seen-but-undrained updates (zero-loss — we never ack
      // past the framework-drained cursor). Sleep to avoid a busy loop; the window closes once the
      // framework drains and lastDrainedCursor advances.
      if (newCount === 0) await sleep(1000)
    } catch {
      // Transient fetch/network error — back off and retry, never crash the connector.
      await sleep(2000)
    }
  }
}

await mcp.connect(new StdioServerTransport())
if (!ECHO_MODE && !CONFORMANCE_MODE) {
  // Defer the live long-poll until shortly AFTER the MCP handshake. The conformance harness drives
  // a loopback send immediately after connecting, which disarms this timer (noteLoopback) — so a
  // cert run, even in an env that carries the live token, never calls api.telegram.org and can't
  // contend with or steal updates from the running bot. In real operation no loopback send arrives,
  // so polling (and the startup deleteWebhook) begins after the brief grace.
  deferredPollTimer = setTimeout(startLivePolling, LIVE_POLL_GRACE_MS)
}
