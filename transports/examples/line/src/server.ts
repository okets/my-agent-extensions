#!/usr/bin/env tsx
/**
 * LINE (Messaging API) transport connector — a self-contained MCP stdio server.
 *
 * SELF-CONTAINED: imports ONLY @modelcontextprotocol/sdk (folder-local node_modules) + Node built-ins
 * (node:crypto, node:fs). It never imports a framework module — the wire contract is protocol, not code.
 * Outbound uses the LINE REST push API via the global `fetch`; inbound arrives THROUGH the framework's
 * public webhook DOOR (docs/design/webhook-ingress.md §3), never a listener this connector binds.
 *
 * THE LONE WEBHOOK-PUSH BACKEND (`upstream: webhook`, `ingress: webhook`). Unlike every poll/socket/
 * gateway connector, LINE has NO polling and NO provider-held socket: the LINE Platform PUSHES an HTTPS
 * POST to a public webhook URL when a user messages the Official Account. That URL is the framework's
 * DOOR: LINE → POST ${WEBHOOK_PUBLIC_BASE}/line → the framework's /hook/:connector route → the framework
 * calls THIS connector's `transport/webhook` MCP tool with {method, path, headers, bodyB64}. The
 * connector owns NO local HTTP listener and NO Tailscale funnel (§3.6: a connector NEVER touches host
 * networking or binds a public listener — the door is the `webhook-ingress` capability's job). There is
 * no getUpdates and no provider-side redelivery queue — so if this connector PROCESS is down when LINE
 * knocks, the door answers 503 and LINE retries; a knock that outlives LINE's retries is LOST
 * (documented, accepted limitation; see CAPABILITY.md / DECISIONS.md). The connector buffers received
 * events into a CONNECTOR-OWNED monotonic INTEGER-cursor queue (start 0, ++ per event) — reset-safe and
 * PERSISTED across restarts — and serves `transport/drain` non-consumingly from a bounded backlog.
 *
 * CONTROL-PLANE ONLY. The framework speaks:
 *   - transport/send    {to, content}                 → push one outbound text message to a stored userId.
 *   - transport/drain   {sinceCursor}                 → {messages:[{cursor,from,content,timestamp,...}]}.
 *   - transport/typing  {to, action:'start'|'stop'}   → drive the "loading…" animation (best-effort).
 *                                                        Only 1:1 chats (userId 'U…') — silently degrades
 *                                                        for groups/rooms. Auto-cleared by the next send.
 *   - transport/webhook {method,path,headers,bodyB64} → verify x-line-signature over the raw bytes,
 *                                                        enqueue events, return {status:200} (401 on a
 *                                                        signature mismatch). The door's forward tool.
 *   - transport/health  {}                            → process liveness + inbound counters (arms the
 *                                                        framework's silent-drop net + the cert temporal
 *                                                        cursor proof).
 *   - notifications/transport/message_available          → emitted on new inbound (a WAKEUP only).
 *   - notifications/transport/needs_attention {reason}   → a HUMAN must act (revoked token).
 * Never bytes on the wire (webhook payloads are the ONE §3.3 exception — small signed JSON events, the
 * exactness of the bytes IS the security property), never unprompted push.
 *
 * WEBHOOK SIGNATURE (the #1 LINE pitfall): every inbound POST carries an `x-line-signature` header =
 * base64(HMAC-SHA256(LINE_CHANNEL_SECRET, RAW_REQUEST_BODY)). The door forwards the EXACT raw bytes as
 * bodyB64; the connector base64-decodes them and runs the HMAC on those raw bytes, BEFORE any JSON parse
 * / re-serialize — a re-serialized body changes whitespace/key-order and breaks the signature. We verify
 * (constant-time), return {status:401} on mismatch, else parse + enqueue and return {status:200}. The
 * empty-events verification POST (the console "Verify" button) returns 200 cleanly.
 *
 * INBOUND MEDIA (increment 2): LINE delivers image/video/audio/file as content fetched from the LINE
 * content endpoint (GET https://api-data.line.me/v2/bot/message/{id}/content, Bearer auth). Bytes are
 * staged via the framework's by-handle media substrate (PUT/GET /api/media via MEDIA_ENDPOINT /
 * MEDIA_TOKEN) — the EXACT same mechanism as the Telegram, WhatsApp, and Discord sibling connectors.
 * Only the JSON MediaHandle ever crosses the MCP wire; bytes never ride the wire.
 * Audio handles are marked ptt:true so the framework's inbound-media router runs STT and the brain
 * gets a text transcript (exactly the sibling voice-note path). Location renders to text; sticker
 * degrades honestly (LINE sticker binaries are proprietary and have no portable media).
 * LINE webhook responses must be delivered within ~1 second, so media downloads are FIRE-AND-FORGET
 * (void+catch): the webhook tool returns {status:200} immediately; the async download/stage/enqueue
 * runs in the background and fires message_available when done. Cert/echo mode degrades synchronously
 * (no network) so conformance probes are unaffected.
 *
 * TWO MODES, one binary:
 *   - LIVE  (LINE_CHANNEL_ACCESS_TOKEN + LINE_CHANNEL_SECRET present): REST-push replies; verify + enqueue
 *     door knocks via transport/webhook; media fetched from LINE content API + staged to media endpoint.
 *   - ECHO  (secrets absent, e.g. the conformance harness, which injects none): a local loopback —
 *     `transport/send` echoes the content straight into the drain queue (from := to) so the wire contract
 *     + render + cursor + degrade carry certify WITHOUT secrets, a door, or any network. The echo branch
 *     is ALSO taken per-send during an explicit cert run (TRANSPORT_CONFORMANCE), so certification is
 *     DETERMINISTIC even if an ambient token leaks into the certifying process's env.
 *
 * NO PAIRING, `session_held: false`: a token-paste connector holds no exportable login; the supervisor
 * simply re-spawns it. A webhook connector via the door has NO local listener and NO provider socket to
 * heartbeat — `transport/health` reports process liveness + inbound counters only.
 */
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { createHmac, timingSafeEqual } from 'node:crypto'
import { readFileSync, writeFileSync, renameSync, mkdirSync, existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { tmpdir } from 'node:os'

const execFileAsync = promisify(execFile)

// ── Wire strings (inlined — never import a framework module) ──────────────────────────────
const SEND_TOOL = 'transport/send'
const DRAIN_TOOL = 'transport/drain'
// Value MUST match the framework TYPING_TOOL ('transport/typing' in wire-protocol.ts).
const TYPING_TOOL = 'transport/typing'
// Value MUST match the framework HEALTH_TOOL ('transport/health' in wire-protocol.ts).
const HEALTH_TOOL = 'transport/health'
// Value MUST match the framework WEBHOOK_TOOL ('transport/webhook' in wire-protocol.ts). The framework's
// public /hook/line door forwards each raw provider knock to this tool as {method,path,headers,bodyB64}.
const WEBHOOK_TOOL = 'transport/webhook'
const MESSAGE_AVAILABLE = 'notifications/transport/message_available'
// Control-plane notification the connector emits when a HUMAN must act (revoked token).
const NEEDS_ATTENTION = 'notifications/transport/needs_attention'

// ── Config / env ────────────────────────────────────────────────────────────────────────
const accessToken = process.env.LINE_CHANNEL_ACCESS_TOKEN
const channelSecret = process.env.LINE_CHANNEL_SECRET
// Env-overridable endpoint so a stub/harness can steer the connector at a mock (contract). Default real.
const API_BASE = (process.env.LINE_API_BASE ?? 'https://api.line.me').replace(/\/+$/, '')
// Content API base (media downloads). Separate host from the push/API base per LINE's architecture.
const LINE_CONTENT_BASE = (process.env.LINE_CONTENT_BASE ?? 'https://api-data.line.me').replace(/\/+$/, '')
// LINE sticker CDN base (public — no auth). Sticker images are fetched from here and staged as
// kind:'image' handles so the brain can see the actual sticker via native multimodal vision.
// URL pattern verified 2026-07-07: stickershop.line-scdn.net/stickershop/v1/sticker/{id}/android/sticker.png
// → HTTP 200, content-type:image/png (14197 bytes for sticker 13; 7547 bytes for sticker 51626494).
const LINE_STICKER_CDN = (process.env.LINE_STICKER_CDN ?? 'https://stickershop.line-scdn.net').replace(/\/+$/, '')
// ECHO loopback engages when EITHER secret is absent (the conformance harness injects none) OR an
// explicit override. Inbound arrives via the framework door's transport/webhook tool in LIVE mode.
const ECHO_MODE = !accessToken || !channelSecret || process.env.LINE_ECHO === '1'
// The harness sets this explicitly so a recert over a token-carrying env is still a cert run, never live.
const CONFORMANCE_ENV = process.env.TRANSPORT_CONFORMANCE === '1'

// LINE hard limits.
const TEXT_MAX = 5000 // chars per text message object
const MESSAGES_PER_PUSH = 5 // message objects per push request

// A DELIVERABLE LINE target is a user/group/room id: 'U'/'C'/'R' + 32 hex. The conformance harness drives
// synthetic targets (e.g. 'probe') and never delivers real inbound, so a non-matching target means
// "certify via loopback"; a live reply always targets a stored 33-char id, so a real send never echoes.
const isDeliverableTarget = (to: string): boolean => /^[UCR][0-9a-fA-F]{32}$/.test(to)
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

// ── Media (S4 by-handle substrate — SAME mechanism as Telegram / WhatsApp / Discord) ────────
// Binary moves out-of-band over the localhost media HTTP endpoint (PUT/GET /api/media); only the
// JSON MediaHandle ever crosses the MCP wire (on transport/send `media` and on a drained WireInbound
// `media`). Config resolved eagerly: canonical source is the scoped env (MEDIA_ENDPOINT/MEDIA_TOKEN,
// declared in requires.env). A localhost fallback (token file at <agentDir>/media/token, endpoint
// http://127.0.0.1:${PORT}) covers a pure code-respawn that hasn't yet re-wired the new env. If neither
// resolves, inbound media degrades honestly (placeholder text) — TEXT is entirely unaffected.
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
interface MediaPutOpts {
  mime: string
  kind: MediaKind
  caption?: string
  duration?: number
  ptt?: boolean
}

const MEDIA_ENDPOINT: string = (() => {
  const env = process.env.MEDIA_ENDPOINT
  if (env && env.trim()) return env.trim().replace(/\/+$/, '')
  const port = process.env.PORT ?? '4321'
  return `http://127.0.0.1:${port}`
})()

const MEDIA_TOKEN: string | undefined = (() => {
  const env = process.env.MEDIA_TOKEN
  if (env && env.trim()) return env.trim()
  // cwd = the capability folder; the per-instance token lives at <agentDir>/media/token
  // = ../../media/token from the capability folder.
  try {
    return readFileSync(join(process.cwd(), '..', '..', 'media', 'token'), 'utf8').trim()
  } catch {
    return undefined
  }
})()

// MEDIA_PUBLIC_BASE is RETIRED (increment 3 activation, 2026-07-07). The framework now provides
// a presigned public-fetch lane: POST /api/media/:id/public-link (localhost + x-media-token)
// returns a URL at GET /hook/_media/:assetId/:token, which is served through the Tailscale Funnel
// (/hook is the public door). The connector calls mintPublicLink() to get this URL dynamically,
// so no static MEDIA_PUBLIC_BASE env var is needed. See mintPublicLink() below.

/** PUT raw bytes to the media endpoint and receive a fresh JSON MediaHandle (out-of-band).
 *  Caption is NOT sent as an HTTP header (arbitrary unicode/newlines break headers); set on the
 *  handle client-side after the PUT (matching the telegram/discord document-carry pattern). */
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

/** Mint a presigned public-fetch URL for a staged media asset via the framework's fetch-back lane.
 *  Calls POST /api/media/:id/public-link (localhost + x-media-token). The returned URL resolves
 *  at GET /hook/_media/:assetId/:token, which is served publicly through the Tailscale Funnel
 *  (/hook is the public door, path-scoped on 443 — no /api/media/* exposure needed).
 *  LINE's servers fetch `originalContentUrl` anonymously (no auth header) — this URL is the
 *  answer to that requirement.
 *
 *  Returns null when:
 *    - MEDIA_TOKEN is unavailable (no media endpoint — graceful degrade)
 *    - The asset is unknown or expired (mint returns 404)
 *    - The MIME type is not grantable (only image/audio/video — the endpoint returns 404)
 *    - WEBHOOK_PUBLIC_BASE is not set in the framework (endpoint returns 503)
 *    - Network error contacting MEDIA_ENDPOINT
 *  Any null → honest degrade below (never a silent drop). */
async function mintPublicLink(id: string): Promise<string | null> {
  if (!MEDIA_TOKEN) return null
  try {
    const res = await fetch(`${MEDIA_ENDPOINT}/api/media/${id}/public-link`, {
      method: 'POST',
      headers: { 'x-media-token': MEDIA_TOKEN },
    })
    if (!res.ok) {
      console.error(`[line] public-link mint for ${id} → HTTP ${res.status}`)
      return null
    }
    const body = (await res.json()) as { url?: string }
    return typeof body.url === 'string' ? body.url : null
  } catch (e) {
    console.error(`[line] public-link mint for ${id} failed: ${e instanceof Error ? e.message : String(e)}`)
    return null
  }
}

/** Fetch raw bytes for a staged media asset from the localhost media endpoint (GET /api/media/:id).
 *  Returns {bytes, mimeType} on success, null on any failure (caller degrades honestly). */
async function fetchMediaBytes(id: string): Promise<{ bytes: Buffer; mimeType: string } | null> {
  if (!MEDIA_TOKEN) return null
  try {
    const res = await fetch(`${MEDIA_ENDPOINT}/api/media/${id}`, {
      headers: { 'x-media-token': MEDIA_TOKEN },
    })
    if (!res.ok) {
      console.error(`[line] media GET for ${id} → HTTP ${res.status}`)
      return null
    }
    const ct = res.headers.get('content-type') ?? 'application/octet-stream'
    const mimeType = ct.split(';')[0]!.trim()
    const bytes = Buffer.from(await res.arrayBuffer())
    return { bytes, mimeType }
  } catch (e) {
    console.error(`[line] media GET for ${id} failed: ${e instanceof Error ? e.message : String(e)}`)
    return null
  }
}

/**
 * Transcode audio bytes to m4a/AAC via ffmpeg and compute the real duration via ffprobe.
 *
 * LINE's audio message type accepts ONLY "mp3 or m4a" (verified against the official spec
 * at developers.line.biz/en/reference/messaging-api/#audio-message). OGG/Opus — the format
 * produced by Edge TTS — is NOT accepted: LINE renders it as a ~1-minute blank note.
 *
 * Uses Node child_process (no new npm deps). Returns {m4aBytes, durationMs} on success.
 * Returns null if ffmpeg/ffprobe are unavailable or the transcode fails — the caller then
 * degrades to text (never sends a broken audio note, never a silent drop).
 */
async function transcodeToM4a(inputBytes: Buffer): Promise<{ m4aBytes: Buffer; durationMs: number } | null> {
  const ts = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
  const inputPath = join(tmpdir(), `line-audio-in-${ts}.ogg`)
  const outputPath = join(tmpdir(), `line-audio-out-${ts}.m4a`)
  try {
    writeFileSync(inputPath, inputBytes)
    // Transcode to m4a/AAC — LINE spec requires mp3 or m4a; OGG/Opus causes blank audio.
    // -movflags +faststart: ensures the MOOV atom is at the start (web/streaming-friendly).
    await execFileAsync('ffmpeg', [
      '-y', '-i', inputPath,
      '-c:a', 'aac',
      '-b:a', '64k',
      '-movflags', '+faststart',
      '-f', 'mp4',
      outputPath,
    ])
    // Get real duration from the transcoded file via ffprobe.
    // If ffprobe fails, fall back to 60000ms (LINE's own doc example default) — the audio
    // PLAYS (the scrubbar shows wrong length) but is never blank.
    let durationMs = 60000
    try {
      const { stdout } = await execFileAsync('ffprobe', [
        '-v', 'quiet',
        '-show_entries', 'format=duration',
        '-of', 'csv=p=0',
        outputPath,
      ])
      const sec = parseFloat(stdout.trim())
      if (Number.isFinite(sec) && sec > 0) durationMs = Math.round(sec * 1000)
    } catch (probeErr) {
      console.error(`[line] ffprobe failed (duration fallback to 60s): ${probeErr instanceof Error ? probeErr.message : String(probeErr)}`)
    }
    const m4aBytes = readFileSync(outputPath)
    return { m4aBytes, durationMs }
  } catch (e) {
    // ENOENT → ffmpeg not on PATH; other errors → transcode failed.
    const msg = e instanceof Error ? e.message : String(e)
    if ((e as NodeJS.ErrnoException).code === 'ENOENT') {
      console.error(`[line] ffmpeg not found on PATH — cannot transcode OGG to m4a; audio degrades to text`)
    } else {
      console.error(`[line] m4a transcode failed: ${msg}`)
    }
    return null
  } finally {
    try { unlinkSync(inputPath) } catch {}
    try { unlinkSync(outputPath) } catch {}
  }
}

/**
 * Fetch the staged audio, transcode to m4a/AAC, re-stage the m4a, and mint a presigned
 * public URL — the full preparation pipeline for a LINE audio send.
 *
 * Returns {publicUrl, durationMs} on success, null on any failure at any step.
 * Any null → honest degrade below (audio-only: throw → text fallback; text+audio: skip audio).
 */
async function prepareAudioForLine(handle: MediaHandle): Promise<{ publicUrl: string; durationMs: number } | null> {
  // 1. Fetch the staged bytes (typically OGG/Opus from Edge TTS).
  const fetched = await fetchMediaBytes(handle.id)
  if (!fetched) return null

  // 2. Transcode to m4a/AAC + compute real duration (LINE requires mp3 or m4a).
  const transcoded = await transcodeToM4a(fetched.bytes)
  if (!transcoded) return null
  const { m4aBytes, durationMs } = transcoded

  // 3. Stage the m4a as a new handle (duration in seconds — internal handle convention).
  let m4aHandle: MediaHandle
  try {
    m4aHandle = await mediaPut(m4aBytes, {
      mime: 'audio/mp4',
      kind: 'audio',
      duration: Math.round(durationMs / 1000), // internal: seconds; LINE needs ms (see pushLineNativeMedia)
    })
  } catch (e) {
    console.error(`[line] m4a mediaPut failed: ${e instanceof Error ? e.message : String(e)}`)
    return null
  }

  // 4. Mint a presigned public URL for the m4a handle.
  const publicUrl = await mintPublicLink(m4aHandle.id)
  if (!publicUrl) {
    console.error(`[line] could not mint presigned public link for transcoded m4a ${m4aHandle.id}`)
    return null
  }

  return { publicUrl, durationMs }
}

/** Fetch binary content from the LINE content endpoint for a given messageId.
 *  Returns {bytes, mimeType} on success, null on any error (caller degrades honestly). */
async function fetchLineContent(messageId: string): Promise<{ bytes: Buffer; mimeType: string } | null> {
  if (!accessToken) return null
  try {
    const res = await fetch(`${LINE_CONTENT_BASE}/v2/bot/message/${messageId}/content`, {
      headers: { authorization: `Bearer ${accessToken}` },
    })
    if (!res.ok) {
      console.error(`[line] content fetch for message ${messageId} → HTTP ${res.status}`)
      return null
    }
    const contentType = res.headers.get('content-type') ?? 'application/octet-stream'
    // Strip parameters (e.g. "image/jpeg; charset=utf-8" → "image/jpeg")
    const mimeType = contentType.split(';')[0]!.trim()
    const bytes = Buffer.from(await res.arrayBuffer())
    return { bytes, mimeType }
  } catch (e) {
    console.error(
      `[line] content fetch for message ${messageId} failed: ${e instanceof Error ? e.message : String(e)}`,
    )
    return null
  }
}

/** Fetch from LINE content API → mediaPut → return a MediaHandle. Best-effort: any failure
 *  (missing MEDIA_TOKEN, download error, PUT error) returns null so the caller degrades HONESTLY.
 *  All failures logged to stderr — an inbound media degrade is never a silent mystery. */
async function downloadLineMediaToHandle(
  messageId: string,
  kind: MediaKind,
  opts: { ptt?: boolean; duration?: number; caption?: string },
): Promise<MediaHandle | null> {
  try {
    if (!MEDIA_TOKEN) {
      console.warn(
        `[line] media store unavailable (no MEDIA_TOKEN) — degrading ${kind} message ${messageId} honestly`,
      )
      return null
    }
    const fetched = await fetchLineContent(messageId)
    if (!fetched) return null // fetchLineContent already logged
    const handle = await mediaPut(fetched.bytes, {
      mime: fetched.mimeType,
      kind,
      ...(opts.ptt !== undefined ? { ptt: opts.ptt } : {}),
      ...(opts.duration !== undefined ? { duration: opts.duration } : {}),
    })
    if (opts.caption) handle.caption = opts.caption
    return handle
  } catch (e) {
    console.error(
      `[line] downloadLineMediaToHandle ${kind} message ${messageId} failed: ${e instanceof Error ? e.message : String(e)}`,
    )
    return null
  }
}

// ── Sticker helpers (inbound sticker CDN fetch, increment 4) ─────────────────────────────────
/**
 * Build a concise human-readable description for a sticker event.
 * Includes packageId, stickerId, and keywords (if any) so the brain has semantic context
 * even when the CDN image cannot be fetched (honest degrade baseline).
 *
 * Examples:
 *   "[sticker: package 446, sticker 1988]"
 *   "[sticker: package 11537, sticker 51626494 — 🎉 celebration, happy]"
 */
function buildStickerDescription(msg: NonNullable<LineEvent['message']>): string {
  const parts: string[] = []
  if (msg.packageId) parts.push(`package ${msg.packageId}`)
  if (msg.stickerId) parts.push(`sticker ${msg.stickerId}`)
  const base = parts.length > 0 ? `[sticker: ${parts.join(', ')}]` : '[sticker]'
  if (Array.isArray(msg.keywords) && msg.keywords.length > 0) {
    const kw = msg.keywords.slice(0, 5).join(', ')
    return `${base} — ${kw}`
  }
  return base
}

/**
 * Fetch a LINE sticker PNG from the public CDN and stage it as a MediaHandle.
 * The sticker CDN is PUBLIC — no auth headers. URL pattern (verified 2026-07-07):
 *   https://stickershop.line-scdn.net/stickershop/v1/sticker/{stickerId}/android/sticker.png
 *
 * Returns a kind:'image' MediaHandle on success, null on any failure (caller degrades honestly).
 * The handle is then enqueued on the WireInbound.media field so the framework's inbound-media
 * router routes it via native-vision (mediaCapabilityFor('image','inbound') → {via:'native-vision'})
 * — the brain can literally see the sticker image via multimodal vision.
 *
 * Degradation cases (all honest — never silent):
 *   - MEDIA_TOKEN unavailable (no media endpoint configured) → null
 *   - CDN returns 0-byte body or non-200 → null (log to stderr)
 *   - Network error → null (log to stderr)
 *   - mediaPut fails → null (log to stderr)
 */
async function downloadStickerToHandle(stickerId: string): Promise<MediaHandle | null> {
  if (!MEDIA_TOKEN) {
    // No media endpoint — cannot stage. Caller still enqueues descriptive text content.
    console.warn(`[line] sticker ${stickerId}: no MEDIA_TOKEN — cannot stage CDN image; descriptive text only`)
    return null
  }
  const url = `${LINE_STICKER_CDN}/stickershop/v1/sticker/${stickerId}/android/sticker.png`
  try {
    const res = await fetch(url)
    if (!res.ok) {
      console.error(`[line] sticker CDN fetch ${stickerId} → HTTP ${res.status}`)
      return null
    }
    const bytes = Buffer.from(await res.arrayBuffer())
    if (bytes.length === 0) {
      // CDN returned empty body — sticker may be private/deleted. Degrade to text.
      console.warn(`[line] sticker CDN for ${stickerId} returned 0 bytes — sticker may be private`)
      return null
    }
    const handle = await mediaPut(bytes, { mime: 'image/png', kind: 'image' })
    return handle
  } catch (e) {
    console.error(`[line] sticker CDN fetch/stage for ${stickerId} failed: ${e instanceof Error ? e.message : String(e)}`)
    return null
  }
}

// ── WireInbound (inlined — kept in sync with the framework wire-protocol) ──────────────────
interface WireInbound {
  cursor: string // monotonic, strictly increasing, connector-owned INTEGER (String(++seq))
  from: string // UNTRUSTED raw LINE source.userId, reported VERBATIM
  content: string
  timestamp: number // ms epoch
  providerMessageId?: string // the LINE message id (a decimal string)
  kind?: 'message' | 'reaction' | 'edit'
  /** JSON-only media handles (S4). Binary bytes NEVER cross the MCP wire. */
  media?: MediaHandle[]
}

// ── Cert-run detection ─────────────────────────────────────────────────────────────────────
// Explicit signals only — NEVER inferred from the target shape (that poisons a live session). ECHO_MODE
// (secrets absent), the explicit TRANSPORT_CONFORMANCE cert env, or a latched conformanceSeen (a loopback
// send arrived, proving the harness is driving us).
let conformanceSeen = false
function isCertRun(): boolean {
  return ECHO_MODE || CONFORMANCE_ENV || conformanceSeen
}

// ── Connector-owned monotonic cursor queue (PERSISTED across restarts) ─────────────────────
// A webhook push has no provider offset to anchor on, so a reset-to-zero counter would let a
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

/** Dedup by the provider message id (or a synthetic key) so a re-delivered webhook isn't double-enqueued. */
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
 * Append one inbound. The cursor is minted HERE, at push time — NEVER earlier. `inboundEnqueued` counts
 * drainable pushes EXACTLY (the framework's silent-drop net compares it against what it drains). The
 * enqueue path is fully SYNCHRONOUS per invocation (no await inside enqueue()), so the cursor mint is
 * atomic from Node's single-threaded perspective — concurrent async tasks that call enqueue() interleave
 * only at their own await boundaries, not within a single enqueue() call.
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
        `[line] BACKLOG CAP HIT — evicting ${unacked} UN-ACKED inbound message(s) above the framework-drained cursor (${ackedHighWater}); the framework has not drained in >${MAX_BACKLOG} — possible inbound loss`,
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
      `[line] cursorSeq (${cursorSeq}) is BEHIND the framework's sinceCursor (${after}) — stale cursor-state reload; re-anchoring high-water to ${after}`,
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
  return backlog
    .filter((m) => Number(m.cursor) > after)
    .sort((a, b) => Number(a.cursor) - Number(b.cursor))
}

// ── Outbound: LINE push message API ─────────────────────────────────────────────────────────
/** Split content into ≤TEXT_MAX-char chunks (LINE's text message limit). Preserves order. */
function splitText(content: string): string[] {
  if (content.length <= TEXT_MAX) return [content]
  const parts: string[] = []
  for (let i = 0; i < content.length; i += TEXT_MAX) parts.push(content.slice(i, i + TEXT_MAX))
  return parts
}

/** Push one batch (≤5 message objects) to a userId via the LINE push API. Honors 429 with an
 *  ordering-preserving retry. Throws on a non-2xx (surfacing the provider's error body verbatim). */
async function pushBatch(to: string, texts: string[]): Promise<void> {
  const body = JSON.stringify({ to, messages: texts.map((text) => ({ type: 'text', text })) })
  for (let attempt = 0; attempt < 5; attempt++) {
    const res = await fetch(`${API_BASE}/v2/bot/message/push`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': 'application/json',
      },
      body,
    })
    if (res.ok) return
    if (res.status === 429) {
      // Rate-limited: honor Retry-After, then retry the SAME batch before any later one (order preserved).
      const ra = Number(res.headers.get('retry-after') ?? '1')
      const waitMs = Math.max(200, (Number.isFinite(ra) ? ra : 1) * 1000)
      console.error(`[line] 429 rate-limited on push to ${to}; retrying after ${waitMs}ms`)
      await sleep(waitMs)
      continue
    }
    const b = await res.text().catch(() => '')
    // 401 = a revoked/invalid access token → terminal, page a human (do NOT retry-loop).
    if (res.status === 401) {
      console.error(`[line] push to ${to} got 401 (invalid/revoked access token): ${b}`)
      void mcp.notification({ method: NEEDS_ATTENTION, params: { reason: 'logged_out' } }).catch(() => {})
    }
    throw new Error(`push to ${to} failed (${res.status}): ${b}`)
  }
  throw new Error(`push to ${to} failed: exhausted 429 retries`)
}

/** Send one outbound text to a stored userId. Splits to LINE's 5000-char limit and batches ≤5 message
 *  objects per push request, preserving order (part N+1 only after part N is ACCEPTED). */
async function pushMessage(to: string, content: string): Promise<void> {
  const parts = splitText(content)
  for (let i = 0; i < parts.length; i += MESSAGES_PER_PUSH) {
    await pushBatch(to, parts.slice(i, i + MESSAGES_PER_PUSH))
  }
}

/** Attempt to push a LINE native media message (image or audio) to a userId via the push API.
 *  LINE requires a publicly accessible HTTPS `originalContentUrl` that LINE's servers can fetch.
 *  This is the LINE-specific adaptation of the sibling connectors' outbound media mechanism:
 *  instead of mediaGet(id)→bytes→multipart-upload (Telegram/WhatsApp/Discord), LINE needs
 *  handle.id → public URL → LINE push API (LINE fetches media server-side via the URL).
 *
 *  LINE message types by MediaKind:
 *    image → {type:'image', originalContentUrl, previewImageUrl} — both must be public HTTPS.
 *    audio → {type:'audio', originalContentUrl, duration (ms)} — duration in milliseconds.
 *    file  → LINE push API has NO generic file message type; returns false (caller degrades).
 *
 *  explicitDurationMs (audio only): the real duration in milliseconds from ffprobe on the
 *  transcoded m4a. Passed by prepareAudioForLine(); overrides the handle-derived fallback.
 *
 *  Returns true on a successful push, false on any provider error (caller degrades honestly).
 *  Honors 429 with an ordering-preserving retry (same pattern as pushBatch). */
async function pushLineNativeMedia(to: string, handle: MediaHandle, publicUrl: string, explicitDurationMs?: number): Promise<boolean> {
  if (!accessToken) return false
  let message: Record<string, unknown>
  if (handle.kind === 'image') {
    // Both URLs required by LINE. previewImageUrl is ideally a thumbnail; we use the same URL
    // (best-effort without a separate thumbnail in this build; LINE may downscale the preview).
    message = { type: 'image', originalContentUrl: publicUrl, previewImageUrl: publicUrl }
  } else if (handle.kind === 'audio') {
    // Use the explicit real duration from ffprobe (prepareAudioForLine) when provided.
    // Fallback: handle.duration is in seconds (internal convention) → convert to ms.
    // Last-resort default: 60000ms (LINE's own example; audio plays but scrubbar is wrong).
    const durationMs = explicitDurationMs !== undefined
      ? explicitDurationMs
      : (typeof handle.duration === 'number' ? handle.duration * 1000 : 60000)
    message = { type: 'audio', originalContentUrl: publicUrl, duration: durationMs }
  } else {
    // 'file' kind: LINE push API has no generic file type. Caller sends a text note instead.
    return false
  }
  const body = JSON.stringify({ to, messages: [message] })
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const res = await fetch(`${API_BASE}/v2/bot/message/push`, {
        method: 'POST',
        headers: { authorization: `Bearer ${accessToken}`, 'content-type': 'application/json' },
        body,
      })
      if (res.ok) return true
      if (res.status === 429) {
        const ra = Number(res.headers.get('retry-after') ?? '1')
        const waitMs = Math.max(200, (Number.isFinite(ra) ? ra : 1) * 1000)
        console.error(`[line] 429 on native ${handle.kind} push to ${to}; retrying after ${waitMs}ms`)
        await sleep(waitMs)
        continue
      }
      const b = await res.text().catch(() => '')
      if (res.status === 401) {
        console.error(`[line] native ${handle.kind} push to ${to} got 401 (revoked token): ${b}`)
        void mcp.notification({ method: NEEDS_ATTENTION, params: { reason: 'logged_out' } }).catch(() => {})
      } else {
        console.error(`[line] native ${handle.kind} push to ${to} failed (${res.status}): ${b}`)
      }
      return false
    } catch (e) {
      console.error(`[line] native ${handle.kind} push error: ${e instanceof Error ? e.message : String(e)}`)
      return false
    }
  }
  return false
}

// ── Typing indicator — LINE loading animation ────────────────────────────────────────────────
// LINE's "display a loading animation" endpoint: POST /v2/bot/chat/loading/start
//   body: { chatId: <userId>, loadingSeconds: 5 }
//   Authorization: Bearer <access token>
// ONLY works in 1:1 chats (userId starts with 'U'). Groups ('C') and rooms ('R') degrade silently.
// loadingSeconds must be in 5..60 and a multiple of 5. The animation auto-clears when a message
// is sent or the time elapses — the send that follows always wins. Best-effort: swallow all errors.
const LOADING_SECONDS = 5

/** POST loading/start for a 1:1 userId. Best-effort — errors go to stderr, never surfaced. */
async function startLoadingAnimation(userId: string): Promise<void> {
  if (!accessToken) return // no token — cert run, silently skip
  try {
    const res = await fetch(`${API_BASE}/v2/bot/chat/loading/start`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ chatId: userId, loadingSeconds: LOADING_SECONDS }),
    })
    if (!res.ok) {
      console.error(`[line] loading/start for ${userId} → HTTP ${res.status}`)
    }
  } catch (e) {
    console.error(`[line] loading/start for ${userId} failed: ${e instanceof Error ? e.message : String(e)}`)
  }
}

// ── Honest-degrade placeholder ────────────────────────────────────────────────────────────
interface MediaHandleLike { kind?: string; caption?: string }
/** A bracketed placeholder for a media kind that cannot be carried natively — never a silent drop.
 *  Used for (a) cert/echo mode (no network) and (b) live mode if the download/stage fails. */
function degradePlaceholder(handle: MediaHandleLike): string {
  switch (handle.kind) {
    case 'image': return '[image]'
    case 'audio': return '[voice message]'
    case 'video': return '[video]'
    case 'file': return `[attachment${handle.caption ? `: ${handle.caption}` : ''}]`
    default: return '[unsupported message]'
  }
}

// ── Webhook receiver (LIVE mode) — raw-body HMAC-SHA256 signature verify ─────────────────────
/** Constant-time compare of the computed HMAC against the x-line-signature header. Any shape mismatch
 *  (missing header, wrong length, bad base64) is a rejection, never a throw. */
function verifySignature(rawBody: Buffer, signature: string | undefined): boolean {
  if (!signature || !channelSecret) return false
  try {
    const expected = createHmac('sha256', channelSecret).update(rawBody).digest() // raw bytes
    const provided = Buffer.from(signature, 'base64')
    if (provided.length !== expected.length) return false
    return timingSafeEqual(provided, expected)
  } catch {
    return false
  }
}

interface LineEvent {
  type?: string
  timestamp?: number
  source?: { type?: string; userId?: string; groupId?: string; roomId?: string }
  message?: {
    id?: string
    type?: string
    // text
    text?: string
    // location
    title?: string
    address?: string
    latitude?: number
    longitude?: number
    // audio (duration in ms from LINE)
    duration?: number
    // file
    fileName?: string
    fileSize?: number
    // sticker
    stickerId?: string
    packageId?: string
    keywords?: string[]
    stickerResourceType?: string  // e.g. 'STATIC' | 'ANIMATION' | 'SOUND' | 'ANIMATION_SOUND' | 'POPUP' | 'CUSTOM' | 'MESSAGE'
    // content provider (image/video/audio may have an external provider)
    contentProvider?: { type?: string; originalContentUrl?: string }
  }
  webhookEventId?: string
}

/** Render an inbound location event to a concise text string (lat/long + title/address if present).
 *  Format: "[location: <title> — <address> (lat, lon)]" — always non-empty, brackets identify kind. */
function renderLocation(msg: NonNullable<LineEvent['message']>): string {
  const lat = msg.latitude
  const lon = msg.longitude
  const parts: string[] = []
  if (msg.title) parts.push(msg.title)
  if (msg.address) parts.push(msg.address)
  if (lat !== undefined && lon !== undefined) {
    parts.push(`(${lat.toFixed(6)}, ${lon.toFixed(6)})`)
  }
  return parts.length > 0 ? `[location: ${parts.join(' — ')}]` : '[location]'
}

/** Task descriptor for one queued media event awaiting an async LINE content download. */
interface MediaEventTask {
  msg: NonNullable<LineEvent['message']>
  from: string
  timestamp: number
  providerMessageId: string | undefined
}

/**
 * Process one media event asynchronously: fetch binary from the LINE content API, stage to the media
 * endpoint, enqueue with a MediaHandle. Falls back to an honest-degrade placeholder on any error.
 * Called sequentially within a batch (see processMediaEventsBatch) so intra-batch cursor order is
 * preserved: events in the same LINE webhook batch get consecutive cursors in arrival order.
 */
async function processOneMediaEvent(task: MediaEventTask): Promise<void> {
  const { msg, from, timestamp, providerMessageId } = task
  const base = { from, timestamp, ...(providerMessageId ? { providerMessageId } : {}) }
  const messageId = msg.id ?? ''

  // If the content provider is external (not LINE-hosted), we cannot fetch via the LINE content API.
  // Degrade honestly rather than attempt an unauthenticated fetch to an arbitrary URL.
  const cp = msg.contentProvider
  const isLineHosted = !cp || cp.type === 'line' || cp.type === undefined

  switch (msg.type) {
    case 'image': {
      if (!isLineHosted || !messageId) {
        enqueue({ ...base, content: '[image]' })
        break
      }
      const handle = await downloadLineMediaToHandle(messageId, 'image', {})
      if (handle) {
        enqueue({ ...base, content: '', media: [handle] })
      } else {
        enqueue({ ...base, content: '[image]' })
      }
      break
    }
    case 'video': {
      if (!isLineHosted || !messageId) {
        enqueue({ ...base, content: '[video]' })
        break
      }
      const handle = await downloadLineMediaToHandle(messageId, 'file', {})
      if (handle) {
        enqueue({ ...base, content: '[video]', media: [handle] })
      } else {
        enqueue({ ...base, content: '[video]' })
      }
      break
    }
    case 'audio': {
      if (!isLineHosted || !messageId) {
        enqueue({ ...base, content: '[voice message]' })
        break
      }
      // LINE audio duration is in milliseconds; the framework/Deepgram expect seconds on the handle.
      const durationMs = typeof msg.duration === 'number' ? msg.duration : undefined
      const durationSec = durationMs !== undefined ? Math.round(durationMs / 1000) : undefined
      const handle = await downloadLineMediaToHandle(messageId, 'audio', {
        ptt: true, // marks as voice note → framework routes to STT (audio-to-text capability)
        ...(durationSec !== undefined ? { duration: durationSec } : {}),
      })
      if (handle) {
        // ptt:true on the audio handle signals the framework's inbound-media router to run STT.
        // The brain then receives the transcript as text + voice-mode hint (same as WhatsApp/Telegram).
        enqueue({ ...base, content: '', media: [handle] })
      } else {
        enqueue({ ...base, content: '[voice message]' })
      }
      break
    }
    case 'file': {
      if (!messageId) {
        const fname = msg.fileName
        enqueue({ ...base, content: fname ? `[attachment: ${fname}]` : '[attachment]' })
        break
      }
      const fname = msg.fileName
      const handle = await downloadLineMediaToHandle(messageId, 'file', {
        ...(fname ? { caption: fname } : {}),
      })
      if (handle) {
        // Preserve original filename on the wire handle (mirrors telegram/discord document-carry pattern).
        if (fname && !handle.caption) handle.caption = fname
        enqueue({ ...base, content: '', media: [handle] })
      } else {
        enqueue({ ...base, content: fname ? `[attachment: ${fname}]` : '[attachment]' })
      }
      break
    }
    case 'sticker': {
      // ── Sticker: fetch PNG from LINE's public CDN + stage as kind:'image' handle ────────
      // The CDN (stickershop.line-scdn.net) is PUBLIC — no auth needed. The staged handle
      // goes on WireInbound.media so the framework routes it via native-vision: the brain
      // actually SEES the sticker. If the fetch/stage fails, degrade to descriptive text only.
      // buildStickerDescription is called in both branches so the brain always has text context.
      const desc = buildStickerDescription(msg)
      if (msg.stickerId) {
        const handle = await downloadStickerToHandle(msg.stickerId)
        if (handle) {
          // Send both the image handle (vision) and the descriptive text (context).
          enqueue({ ...base, content: desc, media: [handle] })
          break
        }
      }
      // CDN fetch failed or no stickerId — honest text-only degrade (still more informative
      // than a bare '[sticker]').
      enqueue({ ...base, content: desc })
      break
    }
    default: {
      // Unknown future media type — honest degrade, never silent.
      enqueue({ ...base, content: `[${msg.type ?? 'message'}]` })
      break
    }
  }
}

/**
 * Process a batch of media events SEQUENTIALLY (one await at a time). Sequential processing within
 * a batch ensures intra-batch cursor order matches LINE event order. This is the fire-and-forget
 * target for processWebhookBody (live mode only).
 */
async function processMediaEventsBatch(tasks: MediaEventTask[]): Promise<void> {
  for (const task of tasks) {
    await processOneMediaEvent(task)
  }
}

/**
 * Process one parsed webhook body (already signature-verified).
 *
 * Event handling per type:
 *   text     → enqueue content directly (synchronous)
 *   location → render to concise text "[location: title — address (lat, lon)]" (synchronous)
 *   sticker  → degrade to "[sticker]" (honest: LINE sticker binaries are proprietary, no portable media)
 *   image/video/audio/file → cert mode: degrade placeholder; live mode: fire-and-forget async download
 *
 * LINE webhook timeout constraint: the platform expects a response within ~1 second. For this reason,
 * media events in LIVE mode are queued for async processing (fire-and-forget) and the webhook tool
 * returns 200 IMMEDIATELY after the sync path completes. Text/sticker/location are always synchronous
 * (no network needed), so they are enqueued before the 200 response regardless of mode.
 *
 * Cert/echo mode (isCertRun): all events processed SYNCHRONOUSLY — no network, no async tasks.
 * This keeps the conformance probes deterministic (temporal-contract + honest-degrade probes drive
 * via transport/send, which is the cert-mode echo path, not transport/webhook — but keeping the
 * webhook cert path sync is the right defensive choice).
 */
function processWebhookBody(parsed: unknown): void {
  const events = (parsed as { events?: LineEvent[] } | undefined)?.events
  if (!Array.isArray(events) || events.length === 0) return // verification handshake / nothing to do

  const mediaQueue: MediaEventTask[] = []

  for (const ev of events) {
    if (ev.type !== 'message') continue // this connector cares only about message events
    const src = ev.source
    const from = src?.userId ?? src?.groupId ?? src?.roomId
    if (!from) continue // cannot address a reply without a source id
    const msg = ev.message
    if (!msg) continue
    const providerMessageId = msg.id
    // Dedup by the provider message id (fallback to webhookEventId) so a LINE redelivery isn't doubled.
    const dedupKey = providerMessageId ?? ev.webhookEventId
    if (dedupKey && !markSeen(dedupKey)) continue
    inboundReceivedCount++
    const timestamp = typeof ev.timestamp === 'number' ? ev.timestamp : Date.now()
    const base = { from, timestamp, ...(providerMessageId ? { providerMessageId } : {}) }

    if (msg.type === 'text') {
      // ── Plain text (synchronous, unchanged from text-only build) ──────────────────────
      enqueue({ ...base, content: msg.text ?? '' })
    } else if (msg.type === 'location') {
      // ── Location → concise text rendering (synchronous) ───────────────────────────────
      enqueue({ ...base, content: renderLocation(msg) })
    } else if (msg.type === 'sticker') {
      // ── Sticker: descriptive text + async CDN image fetch (increment 4) ──────────────────
      // LINE sticker events carry packageId, stickerId, and optional keywords. We render a
      // concise description (better than a bare '[sticker]') AND, in live mode, async-fetch
      // the sticker PNG from LINE's PUBLIC CDN (no auth needed), stage it as a kind:'image'
      // MediaHandle, and enqueue it alongside the description — so the brain's native
      // multimodal vision can literally SEE the sticker (mediaCapabilityFor → native-vision).
      // In cert/echo mode: sync descriptive degrade only (no network, conformance unaffected).
      if (isCertRun()) {
        enqueue({ ...base, content: buildStickerDescription(msg) })
      } else {
        // Live mode: async CDN fetch + stage (same fire-and-forget contract as other media).
        // The task is added to mediaQueue; processOneMediaEvent handles the 'sticker' type.
        mediaQueue.push({ msg, from, timestamp, providerMessageId })
      }
    } else if (isCertRun()) {
      // ── Cert/echo mode: media degrade (synchronous, no network) ───────────────────────
      // In cert mode the connector holds no secret and fetches no content; degrade immediately.
      const kind = msg.type ?? 'message'
      const content =
        kind === 'image' ? '[image]'
        : kind === 'audio' ? '[voice message]'
        : kind === 'video' ? '[video]'
        : kind === 'file' ? (msg.fileName ? `[attachment: ${msg.fileName}]` : '[attachment]')
        : `[${kind}]`
      enqueue({ ...base, content })
    } else {
      // ── Live mode: queue for async download (cursor minted at enqueue time in the async task) ──
      // We store the pre-resolved fields (from, timestamp, providerMessageId) alongside the msg so
      // the async task doesn't need to re-derive them from the event.
      mediaQueue.push({ msg, from, timestamp, providerMessageId })
    }
  }

  // Fire off async media batch (non-blocking — returns 200 immediately). Sequential within the batch
  // preserves intra-batch event order. Cross-batch interleaving (two concurrent batches) is acceptable
  // and expected for concurrent users / rapid re-deliveries.
  if (mediaQueue.length > 0) {
    void processMediaEventsBatch(mediaQueue).catch((e) =>
      console.error(
        `[line] media batch error: ${e instanceof Error ? e.message : String(e)}`,
      ),
    )
  }
}

/**
 * Handle one raw provider knock forwarded by the framework's public /hook/line door
 * (docs/design/webhook-ingress.md §3.3). Input is {method, path, headers, bodyB64}; bodyB64 is the
 * EXACT raw request bytes, base64. We decode to the raw Buffer, verify the x-line-signature HMAC over
 * those bytes (BEFORE any JSON parse — the #1 LINE pitfall), and on a valid signature parse + call
 * processWebhookBody SYNCHRONOUSLY (which in live mode fires async media tasks and returns immediately).
 * Returns the door's {status} contract.
 *
 * LINE platform timeout: ~1 second for webhook response. processWebhookBody returns synchronously
 * for text/sticker/location events and fires-and-forgets for media events, so the 200 is always
 * delivered within the platform's timeout window.
 */
function handleWebhookKnock(headers: Record<string, unknown> | undefined, bodyB64: string | undefined): { status: number } {
  const rawBody = typeof bodyB64 === 'string' && bodyB64.length > 0 ? Buffer.from(bodyB64, 'base64') : Buffer.alloc(0)
  // Header names arrive lowercased from the framework door (Node/Fastify normalize them); look up
  // case-insensitively for safety. Values are strings (the door joins arrays).
  const sigRaw = headers ? (headers['x-line-signature'] ?? headers['X-Line-Signature']) : undefined
  const signature = Array.isArray(sigRaw) ? sigRaw[0] : (typeof sigRaw === 'string' ? sigRaw : undefined)
  // RAW-BODY HMAC verification — on the exact decoded bytes, BEFORE any JSON parse. A re-serialized
  // body breaks the signature (the #1 LINE pitfall).
  if (!verifySignature(rawBody, signature)) {
    console.error('[line] webhook signature verification FAILED — rejecting 401')
    return { status: 401 }
  }
  try {
    const parsed = rawBody.length > 0 ? JSON.parse(rawBody.toString('utf8')) : {}
    // processWebhookBody is synchronous for text/sticker/location and fires async for media events.
    // Either way it returns synchronously — media downloads happen in the background.
    processWebhookBody(parsed)
  } catch (e) {
    // A malformed-but-signed body is not a signature failure — ack 200 so LINE does not retry-storm a
    // payload we simply could not parse (logged loudly for diagnosis).
    console.error(`[line] webhook processing error: ${e instanceof Error ? e.message : String(e)}`)
  }
  return { status: 200 }
}

// ── MCP server ──────────────────────────────────────────────────────────────────────────────
const mcp = new Server({ name: 'line', version: '1.0.0' }, { capabilities: { tools: {} } })

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: SEND_TOOL,
      description: 'push one outbound text message to a LINE user',
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
      description: 'pull inbound after cursor (non-consuming)',
      inputSchema: {
        type: 'object',
        properties: { sinceCursor: { type: ['string', 'null'] } },
        required: ['sinceCursor'],
      },
    },
    {
      name: TYPING_TOOL,
      description: 'drive the loading animation (1:1 chats only; silent degrade for groups/rooms)',
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
      name: WEBHOOK_TOOL,
      description: 'framework /hook door forward: verify x-line-signature over the raw body, enqueue events, return {status}',
      inputSchema: {
        type: 'object',
        properties: {
          method: { type: 'string' },
          path: { type: 'string' },
          headers: { type: 'object' },
          bodyB64: { type: 'string' },
        },
        required: ['method', 'path', 'headers', 'bodyB64'],
      },
    },
    {
      name: HEALTH_TOOL,
      description: 'report process liveness + inbound counters (arms the framework silent-drop net + cert cursor proof)',
      inputSchema: { type: 'object', properties: {} },
    },
  ],
}))

/** A loopback send proves this is a cert run, not live → latch it. */
function noteLoopback(): void {
  conformanceSeen = true
}

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name === SEND_TOOL) {
    const { to, content, media } = req.params.arguments as {
      to: string
      content: string
      media?: MediaHandle[]  // full handle shape; framework sends JSON MediaHandle objects on transport/send
    }
    // Cert-vs-live is EXPLICIT (isCertRun), never inferred from the target shape.
    if (isCertRun()) {
      noteLoopback()
      // Conformance loopback: echo the content into the drain queue (from := to). Carry media handles
      // JSON-only with their ids INTACT (the carry-fidelity `render:images` probe matches on id) —
      // NO endpoint call, NO bytes: the by-handle contract holds, and re-staging would change the id
      // and fail the probe. This is the same pattern as the Telegram/WhatsApp/Discord sibling connectors.
      enqueue({
        from: to,
        content: content ?? '',
        timestamp: Date.now(),
        ...(Array.isArray(media) && media.length > 0 ? { media } : {}),
      })
      return { content: [{ type: 'text', text: 'echoed' }] }
    }
    if (!isDeliverableTarget(to)) {
      // A LIVE send to a non-deliverable target: DROP it LOUDLY — never echo a fake inbound, never
      // latch cert, never claim 'sent'.
      console.error(`[line] DROPPED send — target "${to}" is not a deliverable LINE id`)
      return { content: [{ type: 'text', text: 'not-delivered: target is not a deliverable LINE user id' }] }
    }
    // ── LIVE send (INCREMENT 3 activated, 2026-07-07: presigned public-link lane) ─────────────
    // LINE image/audio messages require public HTTPS URLs that LINE's servers can fetch anonymously.
    // The framework's fetch-back lane (POST /api/media/:id/public-link) mints a presigned URL at
    // GET /hook/_media/:assetId/:token — served through the Tailscale Funnel (/hook is the public
    // door on 443). The connector calls mintPublicLink() to get this URL dynamically.
    //
    // Per-kind live policy:
    //   audio (kind:'audio') — the voice note IS the agent's reply. [voice message] silently drops
    //     the actual text answer — a never-lie violation. Fix (retained from initial increment 3):
    //     • content empty (TTS voice-only path): THROW so the framework's catch in
    //       deliverSynthesizedAudio() returns false → framework sends the text answer instead.
    //     • content present: send the text; skip the audio (text IS the degraded reply, no noise).
    //   image (kind:'image') — degrade to caption if available; text if present; descriptive note.
    //   file (kind:'file') — LINE push API has NO generic file type (verified against official docs:
    //     developers.line.biz/en/reference/messaging-api/#message-objects). Send a text note.
    let outbound = content ?? ''
    if (Array.isArray(media) && media.length > 0) {
      for (const m of media as MediaHandle[]) {
        // Attempt LINE native media push via the presigned public-fetch lane.
        // Only image and audio have LINE native message types (file has none — docs verified).
        if (m.kind === 'audio') {
          // ── Audio: transcode to m4a/AAC first (LINE requires mp3 or m4a, not OGG/Opus) ──
          // prepareAudioForLine: fetch → ffmpeg transcode → mediaPut m4a → mintPublicLink.
          // Also computes real duration via ffprobe (fixes the ~1-min blank-note symptom).
          const audioPrep = await prepareAudioForLine(m)
          if (audioPrep) {
            const { publicUrl, durationMs } = audioPrep
            const pushed = await pushLineNativeMedia(to, m, publicUrl, durationMs)
            if (pushed) {
              console.error(`[line] native audio push to ${to} succeeded (m4a/AAC, ${durationMs}ms)`)
              continue // delivered natively — no text degrade needed
            }
            // Push failed after retries — fall through to honest degrade below.
            console.error(`[line] native audio push failed after transcode; degrading honestly`)
          } else {
            // prepareAudioForLine returned null: ffmpeg unavailable, transcode failed, mediaPut
            // failed, or mintPublicLink failed. Degrade honestly — never a silent drop.
            console.error(`[line] audio preparation failed (transcode/stage/mint); degrading honestly`)
          }
        } else if (m.kind === 'image') {
          // ── Image: use mintPublicLink directly (no format conversion needed) ──
          const publicUrl = await mintPublicLink(m.id)
          if (publicUrl) {
            const pushed = await pushLineNativeMedia(to, m, publicUrl)
            if (pushed) {
              console.error(`[line] native image push to ${to} succeeded via presigned link`)
              continue // delivered natively — no text degrade needed
            }
            // Push failed after retries — fall through to honest degrade below.
            console.error(`[line] native image push failed; degrading honestly`)
          } else {
            // mintPublicLink returned null: no MEDIA_TOKEN, asset expired, MIME not grantable,
            // or no webhook-ingress door. Degrade honestly — never a silent drop.
            console.error(`[line] could not mint presigned public link for image ${m.id}; degrading honestly`)
          }
        }
        // ── Honest degrade (native push failed, no public link, or unsupported kind) ──────────
        if (m.kind === 'audio') {
          // VOICE-REPLY DEGRADE FIX (increment 3): never drop the text answer behind [voice message].
          if (!outbound) {
            // Audio-only send (TTS voice-only path: content is empty, the audio IS the reply).
            // The framework has the text in currentText. Throw → deliverSynthesizedAudio returns
            // false → message-handler.ts (line 817-830) sends the text answer as a plain message.
            throw new Error(
              '[line] audio delivery requires a publicly reachable URL (LINE fetches media server-side). ' +
              'mintPublicLink returned null (MEDIA_TOKEN unavailable, asset expired, MIME not grantable, ' +
              'or no webhook-ingress door). Throwing so the framework sends the text fallback instead.',
            )
          }
          // Text is present alongside audio → text IS the degraded reply. Skip audio silently.
          // Do NOT append [voice message] — the text answers the user and noise-free is better.
        } else if (m.kind === 'file') {
          // LINE push API has no generic file type. Send a text note with the filename.
          const fname = m.caption ?? m.id
          const note = `📎 ${fname}`
          outbound = outbound ? `${outbound}\n${note}` : note
        } else if (m.kind === 'image') {
          // Image without a deliverable URL. Degrade to caption if set; text if present; descriptive note.
          // Do NOT add [image] noise when text is already present (text is the degraded reply).
          if (!outbound && m.caption) {
            outbound = m.caption
          } else if (!outbound) {
            // Image-only send with no caption — something must be sent (never silent).
            outbound = '[image: not deliverable — could not mint public media link]'
          }
          // If outbound is already non-empty: skip the image silently (text is the degrade).
        } else {
          // Unknown future kind — fall back to the generic placeholder (honest, never silent).
          const placeholder = degradePlaceholder(m)
          outbound = outbound ? `${outbound}\n${placeholder}` : placeholder
        }
      }
    }
    if (!outbound) return { content: [{ type: 'text', text: 'sent' }] } // nothing to send
    await pushMessage(to, outbound)
    return { content: [{ type: 'text', text: 'sent' }] }
  }

  if (req.params.name === TYPING_TOOL) {
    // Best-effort — swallow errors, never break a turn.
    // LINE loading animation: only valid for 1:1 chats (userId 'U…'). Groups ('C…') and rooms ('R…')
    // get a silent degrade (the API would reject them; no need to surface an error).
    // action:'stop' is a no-op — the animation auto-clears when a message is sent or the timer elapses.
    const { to, action } = req.params.arguments as { to: string; action: 'start' | 'stop' }
    if (!isCertRun() && isDeliverableTarget(to) && action === 'start') {
      // Only 1:1 chats (userId starts with 'U') — groups 'C' and rooms 'R' degrade silently.
      if (to.startsWith('U')) {
        void startLoadingAnimation(to)
      }
      // Groups/rooms: silent degrade — fall through to return 'ok' below.
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === WEBHOOK_TOOL) {
    // The framework's public /hook/line door forwards each raw provider knock here as
    // {method, path, headers, bodyB64}. Verify + enqueue + answer {status}; both the door's dual-shape
    // read (structuredContent OR the JSON text block) are provided.
    const { headers, bodyB64 } = req.params.arguments as {
      method?: string
      path?: string
      headers?: Record<string, unknown>
      bodyB64?: string
    }
    const answer = handleWebhookKnock(headers, bodyB64)
    return {
      structuredContent: answer as unknown as Record<string, unknown>,
      content: [{ type: 'text', text: JSON.stringify(answer) }],
    }
  }

  if (req.params.name === DRAIN_TOOL) {
    const { sinceCursor } = req.params.arguments as { sinceCursor: string | null }
    return { content: [{ type: 'text', text: JSON.stringify({ messages: drain(sinceCursor) }) }] }
  }

  if (req.params.name === HEALTH_TOOL) {
    // A webhook connector via the framework DOOR has NO local listener and NO provider socket to be
    // "down" (§3.6: the connector never binds a listener; the door is the webhook-ingress capability's
    // job). So `upstreamConnected` is simply the process being alive to answer — always true here. Health
    // is advertised VOLUNTARILY (webhook is exempt from the mandate) for the inbound counters, which arm
    // the framework's silent-drop net + the cert temporal-contract cursor proof. `lastUpstreamActivityMs`
    // is DELIBERATELY OMITTED: a webhook is idle by design between user messages, and reporting a stale
    // last-activity would false-trip the framework's staleness recycle.
    const report = {
      upstreamConnected: true,
      inboundReceived: inboundReceivedCount,
      inboundEnqueued: inboundEnqueuedCount,
    }
    return {
      structuredContent: report as unknown as Record<string, unknown>,
      content: [{ type: 'text', text: JSON.stringify(report) }],
    }
  }

  throw new Error(`unknown tool: ${req.params.name}`)
})

// ── Boot ──────────────────────────────────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
  process.exit(0)
})

// Load the persisted cursor state (only meaningful for a live session — cert runs never persist).
if (!isCertRun()) loadCursorState()

// Inbound arrives THROUGH the framework's public /hook/line door → the transport/webhook tool. This
// connector binds NO local listener and stands up NO funnel (§3.6). Just serve MCP over stdio.
await mcp.connect(new StdioServerTransport())
