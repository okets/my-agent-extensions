#!/usr/bin/env tsx
/**
 * WhatsApp (Baileys multi-device) transport connector — a self-contained MCP stdio server.
 *
 * SELF-CONTAINED: imports ONLY folder-local node_modules (@modelcontextprotocol/sdk +
 * @whiskeysockets/baileys + pino) and inlines every wire string. It NEVER imports a framework
 * module — the wire contract is protocol, not code. This is the reference WhatsApp connector,
 * a refactor of the in-process `plugins/channel-whatsapp` BaileysPlugin into the generic
 * out-of-process drop-in shape (M10.5).
 *
 * CONTROL-PLANE ONLY. The framework speaks:
 *   - transport/send      {to, content, media?}          → transmit one outbound; return {messageId}.
 *   - transport/drain     {sinceCursor}                   → {messages:[{cursor,from,content,...}]}.
 *   - transport/typing    {to, action:'start'|'stop'}     → drive the "composing…" presence (best-effort).
 *   - transport/reaction  {to, messageId, emoji}          → react to a message (best-effort).
 *   - transport/edit      {to, messageId, content}        → edit a previously-sent message (best-effort).
 *   - transport/mark_read {messageId, from}               → send a WhatsApp read receipt (best-effort).
 *   - transport/quiesce   {action:'export'}               → flush creds + close the socket (session_held).
 *   - transport/onboarding_poll {}                        → {artifacts:[...]} current QR/code/status (M10.5).
 *   - notifications/transport/message_available           → emitted on new inbound (a WAKEUP only).
 *   - notifications/transport/needs_attention {reason}    → terminal auth failure (logged_out).
 *   - notifications/transport/onboarding_artifact         → interactive-onboarding wakeup (poll is truth).
 * Never bytes on the wire (the QR rides a MediaHandle, never bytes), never unprompted push.
 *
 * PUSH-MODEL ≠ Telegram's poll model. Baileys holds a PERSISTENT socket and PUSHES events. So
 * instead of a poll loop with a server-side offset, this connector subscribes to `messages.upsert`
 * (plus reaction/edit decoding within it), converts each event to a WireInbound, and appends it to
 * an in-memory monotonic cursor queue (cursor = a strictly-increasing connector-owned integer). It
 * emits message_available on append. drain({sinceCursor}) returns queued records with cursor >
 * sinceCursor. The records are RETAINED (non-consuming) — the framework's durable cursor is the
 * source of truth, so a bounded retained backlog is zero-loss across a re-drain.
 *
 * TWO MODES, one binary:
 *   - LIVE  (WHATSAPP_PHONE_NUMBER present): open a real Baileys socket; pair via pairing code;
 *     push real inbound events into the queue.
 *   - ECHO  (no phone number, e.g. the conformance harness, which injects no env): NO real socket —
 *     transport/send echoes the content straight into the drain queue (from := to) so the wire
 *     contract + render carry + media-by-handle certify WITHOUT WhatsApp creds or any network.
 *   The echo branch is ALSO taken per-send for any non-deliverable target (the harness's synthetic
 *   `to`, e.g. 'probe'), so certification is DETERMINISTIC even if WHATSAPP_PHONE_NUMBER leaks into
 *   the certifying env. A live reply always targets a real JID (`...@s.whatsapp.net` / `...@g.us`),
 *   so a real send never takes the echo branch — live behaviour is unchanged.
 */
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import baileysPkg, {
  useMultiFileAuthState,
  makeCacheableSignalKeyStore,
  downloadMediaMessage,
  fetchLatestBaileysVersion,
  Browsers,
  proto,
  type WASocket,
  type BaileysEventMap,
  type WAMessage,
  type AnyMessageContent,
} from '@whiskeysockets/baileys'
import { pino } from 'pino'
// `qrcode` ships no bundled types; it is used in LIVE mode only to render Baileys' scannable QR
// string to a PNG. The connector runs via tsx (not tsc), so an untyped CJS-interop import is fine.
import qrcodePkg from 'qrcode'
import { createWriteStream, existsSync, mkdirSync, readFileSync, writeFileSync, renameSync, promises as fsp } from 'node:fs'
import { join, isAbsolute } from 'node:path'
import { InboundCatchup } from './offline-catchup.js'
import { CursorQueue } from './cursor-queue.js'
// Pure, dependency-free close-code policy (B1). The reconnect verdict lives here so it is unit-tested
// without the folder-local node_modules; this file performs the side-effects of its decision.
import { decideClose, closeHealthReason } from './close-policy.js'
// Pure, dependency-free zombie-watchdog policy (B2). The tuning constants + the stale-decision live
// there (unit-tested without the folder-local node_modules); this file wires the frame listener +
// interval and performs the side-effect (force a close → the B1 reconnect path).
import {
  shouldForceClose,
  watchdogEnabled,
  KEEPALIVE_INTERVAL_MS,
  WATCHDOG_WINDOW_MS,
  WATCHDOG_CHECK_MS,
} from './watchdog.js'
// Pure, dependency-free upstream-liveness verdict (B3 — the never-lie transport/health contract).
// This file reads the REAL sock + the rc13 ws-client liveness getters + lastActivityAt +
// lastDisconnectReason and hands them to computeUpstreamHealth; the cert run reports a synthetic
// "up" (CRIES-WOLF affordance). `wsReadyStateFromClient` maps Baileys rc13's `sock.ws` getters
// (isOpen/isClosing/isClosed) to a WS_* readyState (the raw `sock.ws.readyState` is dead on rc13).
import { computeUpstreamHealth, wsReadyStateFromClient, type HealthReason } from './health.js'
// Pure, dependency-free outbound send-routing decision (S1.3 (a)+(b)). Distinguishes an explicit
// cert/echo run (loopback) from a LIVE send; a LIVE non-deliverable target is DROPPED loudly, never
// echoed as fake inbound and never latches conformanceSeen (the live-poisoning fix).
import { decideSendRoute } from './send-policy.js'
// Outbound link previews (built here, not by Baileys: timeouts, private-address refusal, no-picture step-down).
import { buildLinkPreview, extractFirstUrl, warmLinkPreview } from './link-preview.js'

// CJS interop: `qrcode`'s API lands on `.default` under ESM. Narrow to just the `toBuffer` we use.
const qrcode = ((qrcodePkg as unknown as { default?: unknown }).default ?? qrcodePkg) as {
  toBuffer: (text: string, opts?: { type?: string }) => Promise<Buffer>
}

// Baileys ships makeWASocket as the default export (CJS interop → it lands on `.default`).
const makeWASocket = (baileysPkg as unknown as { default?: typeof baileysPkg }).default ?? baileysPkg

// ── Wire strings (inlined — never import a framework module) ──────────────────────────────
const SEND_TOOL = 'transport/send'
const DRAIN_TOOL = 'transport/drain'
const TYPING_TOOL = 'transport/typing'
const REACTION_TOOL = 'transport/reaction'
const EDIT_TOOL = 'transport/edit'
const MARK_READ_TOOL = 'transport/mark_read'
const QUIESCE_TOOL = 'transport/quiesce'
const ONBOARDING_POLL_TOOL = 'transport/onboarding_poll'
// B3: the upstream-liveness report tool. Value MUST match the framework HEALTH_TOOL
// ('transport/health' in wire-protocol.ts) — inlined, never imported from a framework module.
const HEALTH_TOOL = 'transport/health'
const MESSAGE_AVAILABLE = 'notifications/transport/message_available'
const NEEDS_ATTENTION = 'notifications/transport/needs_attention'
const ONBOARDING_ARTIFACT_NOTIF = 'notifications/transport/onboarding_artifact'

// ── Media (S4 handle substrate, §7.2 / §1 NO BYTES ON THE WIRE) ────────────────────────────
// A MediaHandle is a small JSON descriptor — bytes NEVER ride a handle. Binary moves out-of-band
// over the localhost media HTTP endpoint (PUT/GET /api/media). Only the handle crosses the MCP wire.
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

/** WireInbound — inlined, kept in sync with the framework's wire-protocol.ts. kind /
 *  providerMessageId / targetMessageId / emoji are additive optional fields (M10-S9). */
interface WireInbound {
  cursor: string // monotonic, strictly increasing, connector-owned
  from: string // UNTRUSTED raw provider sender id (JID), reported VERBATIM
  content: string
  timestamp: number // ms epoch
  media?: MediaHandle[]
  kind?: 'message' | 'reaction' | 'edit'
  providerMessageId?: string
  targetMessageId?: string
  emoji?: string
}

// ── Onboarding artifacts (M10.5 interactive handshake; §1 NO BYTES ON THE WIRE) ──────────────
// During an interactive link the connector emits typed artifacts the framework presents to the
// user: an `image` (a QR — carried by MediaHandle, NEVER bytes), a `code` (the Linked-Devices
// pairing code), and a `status`. The framework polls `transport/onboarding_poll` for the current
// snapshot (the notification is a wakeup; the poll is truth — same principle as drain). Kept inlined;
// kept in sync with the framework's onboarding-artifact.ts (the wire is protocol, not a code import).
type OnboardingArtifact =
  | { kind: 'code'; code: string; label?: string; instructions?: string }
  | { kind: 'status'; state: 'waiting' | 'linked' | 'expired' | 'error' | 'preflight_ok' | 'preflight_rejected'; detail?: string; registered?: boolean }
  | { kind: 'image'; handle: MediaHandle; label?: string }
  | { kind: 'prompt'; text: string; secretKey?: string }

// ── Mode / env ────────────────────────────────────────────────────────────────────────────
const PHONE_NUMBER = process.env.WHATSAPP_PHONE_NUMBER
const PHONE_DIGITS = (PHONE_NUMBER ?? '').replace(/[^0-9]/g, '')
const HAS_PHONE = PHONE_DIGITS.length >= 6

// AUTH_DIR: default is ../../auth/whatsapp relative to the capability folder cwd (= the framework
// fence <agentDir>/auth/whatsapp/). Launched with cwd = .my_agent/capabilities/whatsapp/, so two
// levels up lands in .my_agent/, then auth/whatsapp. Overridable via WHATSAPP_AUTH_DIR (used by tests
// and conformance harness to point at a temp dir).
const AUTH_DIR = (() => {
  const env = process.env.WHATSAPP_AUTH_DIR?.trim()
  if (env) return isAbsolute(env) ? env : join(process.cwd(), env)
  return join(process.cwd(), '..', '..', 'auth', 'whatsapp')
})()
function ensureAuthDir(): void { try { if (!existsSync(AUTH_DIR)) mkdirSync(AUTH_DIR, { recursive: true }) } catch { /* best-effort */ } }

// ECHO loopback engages when there is no phone number (the conformance harness injects none) OR an
// explicit override is set. The real Baileys socket is opened ONLY in genuine LIVE mode.
// Mutable: re-finalized after auth dir is read in main() (same as live connector).
let ECHO_MODE = !HAS_PHONE || process.env.WHATSAPP_ECHO === '1'

// EXPLICIT cert signal (S1.3 (a)). The conformance harness spawns the connector with
// TRANSPORT_CONFORMANCE=1 so the no-phone-but-REGISTERED recert case (ECHO_MODE=false) is still
// recognised as a cert run WITHOUT inferring it from the target shape. This is the only correct
// cert-vs-live discriminator: the harness declares it, the connector never guesses.
const CONFORMANCE_ENV = process.env.TRANSPORT_CONFORMANCE === '1'

// Is this process being driven as a cert/echo run (no real upstream, loopback the wire)? ECHO_MODE
// (no creds / explicit WHATSAPP_ECHO), the explicit cert env, OR an already-latched conformanceSeen.
// A LIVE run is NONE of these — so a LIVE send to a non-deliverable target is NEVER echoed/latched.
function isCertRun(): boolean { return ECHO_MODE || CONFORMANCE_ENV || conformanceSeen }

// A DELIVERABLE WhatsApp target is a JID: <digits>@s.whatsapp.net (DM), <id>@g.us (group), or a
// LID (<digits>@lid). The conformance harness drives only synthetic targets (e.g. to:'probe'), which
// never match — so a non-deliverable target ALWAYS means "certify via loopback", even if a real
// WHATSAPP_PHONE_NUMBER leaked into the certifying env. The framework only ever replies to a real
// JID surfaced from drain, so a live send never takes the echo branch.
const isDeliverableTarget = (to: string): boolean => /@(s\.whatsapp\.net|g\.us|lid|c\.us|broadcast)$/.test(to)

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

// ── Media endpoint (out-of-band binary; §1 NO BYTES ON THE WIRE) ────────────────────────────
const MEDIA_ENDPOINT: string = (() => {
  const env = process.env.MEDIA_ENDPOINT
  if (env && env.trim()) return env.trim().replace(/\/+$/, '')
  const port = process.env.PORT ?? '4321'
  return `http://127.0.0.1:${port}`
})()

const MEDIA_TOKEN: string | undefined = (() => {
  const env = process.env.MEDIA_TOKEN
  return env && env.trim() ? env.trim() : undefined
})()

interface MediaPutOpts {
  mime: string
  kind: MediaKind
  duration?: number
  ptt?: boolean
}

/** GET a handle's bytes from the media endpoint (out-of-band — never on the MCP wire). */
async function mediaGet(id: string): Promise<Buffer> {
  if (!MEDIA_TOKEN) throw new Error('media endpoint token unavailable')
  const res = await fetch(`${MEDIA_ENDPOINT}/api/media/${id}`, { headers: { 'x-media-token': MEDIA_TOKEN } })
  if (!res.ok) throw new Error(`media GET ${id} failed: ${res.status}`)
  return Buffer.from(await res.arrayBuffer())
}

/** PUT raw bytes to the media endpoint and receive a fresh JSON MediaHandle (out-of-band).
 *  Caption is intentionally NOT a header (arbitrary user unicode/newlines would break the HTTP
 *  header); inbound captions ride the WireInbound `content` field instead. */
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

/** Download a Baileys media message's bytes → PUT to the media endpoint → MediaHandle. Best-effort:
 *  any failure (no token, download error, PUT error) returns null so the caller degrades honestly. */
async function downloadToHandle(
  msg: WAMessage,
  opts: { kind: MediaKind; mime: string; ptt?: boolean; duration?: number },
): Promise<MediaHandle | null> {
  try {
    if (!MEDIA_TOKEN) return null
    const bytes = (await downloadMediaMessage(msg, 'buffer', {})) as Buffer
    if (!bytes || bytes.length === 0) return null
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

/** Honest-degrade content for media we can't store/forward — caption verbatim + a `[kind]`
 *  placeholder, never a silent drop (S8 Finding-5 contract). */
function degradeContent(kind: string, caption: string): string {
  const placeholder = `[${kind}]`
  return caption ? `${caption}\n${placeholder}` : placeholder
}

// ── Monotonic cursor queue (connector-owned, PERSISTENT across restarts) ──────────────────────
// Push model: events arrive via messages.upsert and are appended with a strictly-increasing cursor.
// drain() is NON-consuming (the framework's durable cursor is the source of truth); the backlog is a
// bounded retained ring so a re-drain after a missed wakeup loses nothing.
//
// PERSISTENT CURSOR: a push socket has no provider offset to anchor on, so a reset-to-zero counter
// would let a post-restart message collide below the framework's sinceCursor and vanish.
// cursorSeq + the bounded `seen` dedup are persisted to STATE_FILE across restarts.
// The queue itself (mint-at-push cursor, retained drained tail, stale re-anchor, inbound counters) is the
// dependency-free CursorQueue in cursor-queue.ts; this file wires its persist / wakeup / log callbacks.
let seen: string[] = []
const seenSet = new Set<string>()
// Offline catch-up (messages.upsert type 'append'): watermark + bounded handled-id dedupe, persisted in
// STATE_FILE alongside the cursor. See offline-catchup.ts.
const inboundCatchup = new InboundCatchup()
const STATE_FILE = join(AUTH_DIR, 'cursor-state.json')

// S1.2 (D1) — DURABLE BACKLOG. Persisting only {cursorSeq, seen} loses an in-flight inbound message
// when the connector PROCESS respawns between `enqueue` and the framework's drain (the in-memory
// backlog dies with the process and WhatsApp will not re-deliver an already-acked message). So we
// persist the backlog CONTENT alongside the cursor — a respawn reloads the un-drained entries and
// the framework re-drains them from its durable cursor. BOUNDED two ways so it can never grow
// without limit: (1) GC entries at/below the framework-acked (drained) cursor — the happy-path
// reclaim; (2) the hard MAX_BACKLOG cap as the backstop if the framework never drains. The monotonic
// high-water is `cursorSeq` (a separate counter, persisted) — NEVER re-derived from the (possibly
// GC'd/truncated) backlog — so a post-respawn message can never collide below the framework cursor.

function loadCursorState(): void {
  try {
    const raw = JSON.parse(readFileSync(STATE_FILE, 'utf8')) as { cursorSeq?: number; seen?: string[]; backlog?: WireInbound[]; inboundWatermarkMs?: number; handledIds?: string[] }
    // cursorSeq (the high-water) is restored independently of the backlog, so a truncated backlog can't
    // lower the next cursor (see CursorQueue.restore).
    inboundQueue.restore(raw)
    inboundCatchup.restore(raw)
    if (Array.isArray(raw.seen)) { seen = raw.seen.slice(-1000); for (const id of seen) seenSet.add(id) }
  } catch { /* first run / no state — start at 0 */ }
}

function persistCursorState(): void {
  // Persist only for a real live session — never during a cert run so certification never mutates the
  // live auth dir. Gate on isCertRun() (ECHO || explicit cert env || latched) — broader than the old
  // ECHO||conformanceSeen so a future cert-time enqueue can never write the live state.
  if (isCertRun()) return
  try {
    ensureAuthDir()
    // Atomic (temp + rename): a respawn must never read a half-written / truncated state file — a torn
    // cursor-state would itself manufacture inbound loss.
    const tmp = STATE_FILE + '.tmp'
    writeFileSync(tmp, JSON.stringify({ ...inboundQueue.snapshot(), seen: seen.slice(-1000), ...inboundCatchup.snapshot() }))
    renameSync(tmp, STATE_FILE)
  } catch { /* best-effort */ }
}

/** The inbound queue. Cursors are minted INSIDE enqueue(), at push time (contract Cursor rule 1) — no
 *  caller allocates one, so an await between event arrival and the push can never reorder cursors. */
const inboundQueue = new CursorQueue<WireInbound>({
  persist: persistCursorState,
  notify: () => { void mcp.notification({ method: MESSAGE_AVAILABLE, params: {} }).catch(() => {}) },
  warn: (message) => console.warn(`[whatsapp] ${message}`),
  // Test-only drill switch (see CursorQueue.enqueue). Off unless the env var is exactly '1'; never set in
  // production; same convention as SELFHEAL_WATCHDOG_OFF.
  shouldDropInbound: () => process.env.SELFHEAL_DROP_INBOUND === '1',
})

function enqueue(entry: Omit<WireInbound, 'cursor'>): void {
  inboundQueue.enqueue(entry)
}

/** All backlog entries with cursor > sinceCursor, oldest-first (non-consuming for the returned set). */
function drain(sinceCursor: string | null): WireInbound[] {
  return inboundQueue.drain(sinceCursor)
}

// ── getMessage store (for Baileys getMessage / inbound edit processing) ───────────────────────
// Baileys only emits a `messages.update` for a received MESSAGE_EDIT if it can fetch the ORIGINAL
// message via the makeWASocket `getMessage` callback — without it the edit is silently dropped. The
// original was received earlier via `messages.upsert`, so we cache id → IMessage here and serve it
// via getMessage. Bounded LRU (oldest-evicted) so this can't grow unbounded. IN-MEMORY ONLY: an edit
// of a message received BEFORE the last connector restart won't be in the store (getMessage miss →
// the edit is dropped, as before).
const MESSAGE_STORE_LIMIT = 500
const messageStore = new Map<string, proto.IMessage>()
function rememberMessage(id: string | null | undefined, message: proto.IMessage | null | undefined): void {
  if (!id || !message) return
  if (messageStore.has(id)) messageStore.delete(id)
  messageStore.set(id, message)
  if (messageStore.size > MESSAGE_STORE_LIMIT) {
    const oldest = messageStore.keys().next().value
    if (oldest !== undefined) messageStore.delete(oldest)
  }
}

// ── Recent inbound key map (for read receipts / onMarkRead) ──────────────────────────────────
// Maps providerMessageId → Baileys message key so the framework's onMarkRead hook can look up the
// original key and call sock.readMessages([key]). Bounded to the last 200 messages to avoid
// unbounded growth — a live read receipt is always for a recent message.
const RECENT_KEYS_LIMIT = 200
const recentInboundKeys = new Map<string, proto.IMessageKey>()
function rememberInboundKey(id: string | null | undefined, key: proto.IMessageKey | null | undefined): void {
  if (!id || !key) return
  recentInboundKeys.set(id, key)
  if (recentInboundKeys.size > RECENT_KEYS_LIMIT) {
    const oldest = recentInboundKeys.keys().next().value
    if (oldest !== undefined) recentInboundKeys.delete(oldest)
  }
}

// ── Recent message-text map (for reaction target resolution) ──────────────────────────────────
// Maps a provider message id → its text/snippet, for BOTH inbound user messages and our own outbound
// replies. A reaction carries the targetMessageId of the message reacted to; this lets us render
// `*reacted ❤️ to: "<original text>"*` instead of the generic phrasing. Bounded LRU; best-effort — an
// unresolved target falls back to the generic phrasing.
const RECENT_TEXT_LIMIT = 300
const REACTION_SNIPPET_MAX = 80
const recentMessageText = new Map<string, string>()
function rememberMessageText(id: string | null | undefined, text: string | null | undefined): void {
  if (!id || !text) return
  if (recentMessageText.has(id)) recentMessageText.delete(id)
  recentMessageText.set(id, text)
  if (recentMessageText.size > RECENT_TEXT_LIMIT) {
    const oldest = recentMessageText.keys().next().value
    if (oldest !== undefined) recentMessageText.delete(oldest)
  }
}
function reactionSnippet(targetId: string | null | undefined): string | null {
  if (!targetId) return null
  const text = recentMessageText.get(targetId)
  if (!text) return null
  const oneLine = text.replace(/\s+/g, ' ').trim()
  if (!oneLine) return null
  return oneLine.length > REACTION_SNIPPET_MAX ? `${oneLine.slice(0, REACTION_SNIPPET_MAX)}…` : oneLine
}

// ── Onboarding artifact state (latest-wins per kind; poll returns a fresh snapshot) ───────────
// `image` (QR) + `code` are at-most-one (latest-wins); `status` is the current handshake state.
// A `null` slot means "not present" and is omitted from the polled snapshot.
let onboardImage: Extract<OnboardingArtifact, { kind: 'image' }> | null = null
let onboardCode: Extract<OnboardingArtifact, { kind: 'code' }> | null = null
let onboardStatus: Extract<OnboardingArtifact, { kind: 'status' }> = { kind: 'status', state: 'waiting' }

/** Emit the onboarding-artifact wakeup (best-effort). The notification carries NO payload — it is a
 *  wakeup only; the framework reads the truth via `transport/onboarding_poll` (same as drain). */
function emitOnboardingArtifact(): void {
  void mcp.notification({ method: ONBOARDING_ARTIFACT_NOTIF, params: {} }).catch(() => {})
}

/** The current onboarding snapshot, in present-order (image, code, status). Empty slots dropped. */
function onboardingSnapshot(): OnboardingArtifact[] {
  const arts: OnboardingArtifact[] = []
  if (onboardImage) arts.push(onboardImage)
  if (onboardCode) arts.push(onboardCode)
  arts.push(onboardStatus)
  return arts
}

/** Set the QR image artifact (latest-wins) and wake the framework. */
function setOnboardImage(handle: MediaHandle, label?: string): void {
  onboardImage = { kind: 'image', handle, ...(label ? { label } : {}) }
  emitOnboardingArtifact()
}

/** Set the pairing-code artifact (latest-wins) and wake the framework. */
function setOnboardCode(code: string, instructions?: string): void {
  onboardCode = { kind: 'code', code, ...(instructions ? { instructions } : {}) }
  emitOnboardingArtifact()
}

/** Set the handshake status and wake the framework. On `linked` the live linking artifacts (QR +
 *  code) are cleared — they are spent one-time credentials and must not linger after linking.
 *  `registered` is the ground-truth attestation field: set to `creds.registered === true` on the
 *  real socket's `linked` artifact; always `true` in echo mode (synthetic link). */
function setOnboardStatus(state: 'waiting' | 'linked' | 'expired' | 'error' | 'preflight_ok' | 'preflight_rejected', detail?: string, registered?: boolean): void {
  onboardStatus = {
    kind: 'status',
    state,
    ...(detail ? { detail } : {}),
    ...(typeof registered === 'boolean' ? { registered } : {}),
  }
  if (state === 'linked') {
    onboardImage = null
    onboardCode = null
  }
  emitOnboardingArtifact()
}

// ── ECHO-mode synthetic handshake (conformance only; no Baileys socket, no media endpoint) ─────
// The conformance harness injects no WHATSAPP_PHONE_NUMBER (→ ECHO) and exposes no media endpoint,
// so the real QR-render→media-PUT path can't run. To certify the interactive-handshake WIRE SHAPE
// deterministically WITHOUT WhatsApp or a network, the poll returns a synthetic sequence:
//   poll 1 → image + code + status:preflight_ok   (companion_hello accepted — probe saw preflight)
//   poll 2 → status:linked(registered:true)        (simulated device link with ground-truth attestation)
// This mirrors the real socket's preflight_ok → linked(registered:true) sequence (A-F1) so the
// strengthened conformance probe (requirePairingTruth:true) passes without a live WhatsApp socket.
//
// HARNESS_SIMULATE support: the adversarial probe (Task 5) re-spawns the connector with
// HARNESS_SIMULATE=<flag>. Compliant behaviour mirrors mock-pairing-compliant's adversarial branch:
//   open_not_registered    → emit preflight_ok(registered:false) and NEVER advance to linked.
//                            Models: socket opened, companion_hello accepted, but device registration
//                            did NOT complete (WhatsApp issued QR/code but the server did not confirm).
//   selftest_during_pairing → ignore the synthetic self-test signal; stay in preflight_ok(unregistered).
//                            Models: a self-test echo arrived mid-pairing; the connector must not
//                            misread its own echo as confirmation of registration.
// Both branches stay alive (setInterval) so the probe's poll window drains cleanly then disconnects.
let echoPolls = 0

function echoOnboardingSnapshot(): OnboardingArtifact[] {
  // ── HARNESS_SIMULATE: adversarial probe support ──────────────────────────────────────────────
  // If HARNESS_SIMULATE is set, the connector is being probed for incorrect promotion to
  // linked/registered:true. Compliant behaviour: emit preflight_ok but NEVER linked/registered:true.
  const simulate = process.env.HARNESS_SIMULATE
  if (simulate === 'open_not_registered' || simulate === 'selftest_during_pairing') {
    // Connection is open (preflight_ok) but registration has NOT been confirmed. Stay here forever —
    // the probe polls for a bounded window then disconnects. registered:false makes the ground-truth
    // explicit (open ≠ registered), mirroring the real socket rule.
    return [{ kind: 'status', state: 'preflight_ok', registered: false }]
  }

  echoPolls++
  if (echoPolls <= 1) {
    // Poll 1: recognized artifact set + preflight_ok (companion_hello accepted, not yet linked).
    // Synthetic image handle — JSON only, NO bytes (§1) and NO media-endpoint call (none exists in
    // conformance). The probe only needs the parser to ADMIT it (isMediaHandle), never the bytes.
    return [
      { kind: 'image', handle: { id: 'echo-qr', mimeType: 'image/png', size: 1, kind: 'image' }, label: 'Scan to link WhatsApp' },
      { kind: 'code', code: 'ECHO-CODE', instructions: 'enter in WhatsApp → Linked Devices' },
      { kind: 'status', state: 'preflight_ok' },
    ]
  }
  // Poll 2+: simulated link — terminal linked state with ground-truth attestation (registered:true).
  // The probe stops here (linked is terminal). registered:true is the signal the pairing-truth probe
  // requires: the connector attests that Baileys creds.registered would be true in a real link.
  return [{ kind: 'status', state: 'linked', registered: true }]
}

// ── Registration predicate (single source of truth for a real WhatsApp link) ─────────────────
// A real WhatsApp link is proven ONLY by creds.registered === true — NEVER by a bare
// connection:'open' (which can fire mid-pairing before the device is registered).
export function isRealRegistration(creds: { registered?: boolean } | null | undefined): boolean {
  return creds?.registered === true
}

// ── No-contention re-cert gate ────────────────────────────────────────────────────────────────
// ECHO_MODE is re-finalized in main() after the auth dir is read. The cert gate uses:
//   conformanceSeen — set by the first loopback send; disarms the deferred live connect so a
//                     re-cert over a registered folder never opens a 2nd socket / steals updates.
//   deferredConnectTimer — the timer set in boot after MCP handshake; disarmed by noteLoopback().
const LIVE_CONNECT_GRACE_MS = 1500
let conformanceSeen = false
let deferredConnectTimer: ReturnType<typeof setTimeout> | null = null

function noteLoopback(): void {
  // A loopback send proves this process is being driven by the conformance harness, NOT live
  // → disarm the deferred live connect so a re-cert never opens a 2nd socket / steals the session.
  conformanceSeen = true
  if (deferredConnectTimer !== null) { clearTimeout(deferredConnectTimer); deferredConnectTimer = null }
}

// ── Live Baileys socket state ───────────────────────────────────────────────────────────────
let sock: WASocket | null = null
// Single-flight sentinel (M10.5 forensic fix #1): set SYNCHRONOUSLY before startSocket()'s first
// await so two concurrent callers (boot `void startSocket()` + an `onboarding_poll`-kicked one)
// can never both pass the guard and open two Baileys sockets racing one auth dir. The non-atomic
// `if (sock) return` (checked before, but `sock=` only assigned AFTER the mkdir/auth/version awaits)
// was the seed of the multi-socket → QR-desynced-from-code defect. `sock || socketStarting` closes it.
let socketStarting = false
let saveCredsFn: (() => Promise<void>) | null = null
let pairingRequested = false
// When the current pairing code was last requested (ms epoch). A WhatsApp pairing code expires in
// ~60s, so on the user's ask we re-request a FRESH one if this has aged out — the single-device path
// needs a code that's valid at entry time, not one minted on boot minutes ago.
let codeRequestedAt = 0
const CODE_FRESH_MS = 45_000
let loggedOut = false
// RELEASING LATCH (quiesce session-release fence): set true in the QUIESCE_TOOL handler immediately
// BEFORE `sock.end()`, so the resulting `connection:'close'` is read by decideClose as a DELIBERATE
// release → idle, never a reconnect. Without this, the post-quiesce close (statusCode undefined on a
// registered session) classifies as environmental and the old connector reconnects ~1s later, re-
// grabbing the WhatsApp login inside the RecoveryOrchestrator re-derive/re-pair window (the live
// regression the gate caught). Cleared at the top of startSocket() so a later legitimate (re)start
// restores normal reconnect behaviour. The SIGTERM path exits the process cleanly and is unaffected.
let releasing = false
// wasEverLinked gates needs_attention{logged_out} on a real (post-link) logout — a pairing failure
// that was abandoned must NOT page the user; only a genuine de-register after a real link should.
let wasEverLinked = false
// Consecutive ENVIRONMENTAL reconnect attempts (B1) — drives the capped exponential backoff in the
// close handler. Reset to 0 on a registered `connection:'open'` (a healthy session re-established),
// so a later outage starts from the base delay again. Independent of the supervisor crash-breaker.
let reconnectAttempts = 0
// B2 zombie-watchdog state. `lastActivityAt` is bumped on every inbound ws `'frame'` (the
// lowest-level activity signal — invariant #7). `watchdogTimer` runs an INDEPENDENT interval that
// force-closes a silent socket (→ the B1 reconnect), defending against the silent half-open that
// fires no connection.update (the 2026-06-28 zombie). Both are reset per socket in startSocket().
let lastActivityAt = 0
// B3: the classification of the most recent close (set in the connection.update close handler from
// the B1 `decideClose` verdict), surfaced as `transport/health`'s `reason` when the upstream is down.
// Cleared on a healthy registered open. Lets the framework distinguish terminal (logged_out → needs a
// human re-pair) from environmental (network → recoverable) — Invariant 5.
let lastDisconnectReason: HealthReason | undefined
// 2a.4 (3): a (re)connect is in flight — sock is briefly null while Baileys connects, NOT a dead
// socket. Set true the moment we DECIDE to (re)connect (startSocket top + the close→reconnect branch,
// covering the whole sock===null gap including the backoff delay) and cleared on a registered open or
// a terminal/idle verdict. Fed to transport/health so a post-recycle null window reports transient
// (defer) instead of driving a re-recycle loop. The WhatsApp analogue of Telegram's !liveStarted.
let connecting = false
let watchdogTimer: ReturnType<typeof setInterval> | null = null
function clearWatchdog(): void {
  if (watchdogTimer !== null) { clearInterval(watchdogTimer); watchdogTimer = null }
}
// On-demand QR regeneration (M10.5): resolvers waiting for the NEXT freshly-surfaced QR, so an
// `onboarding_poll` that kicks a regen can return the fresh artifact in the same call.
let qrWaiters: Array<() => void> = []
// Credential-save serialization (ported from auth.ts CredentialSaveQueue): serialize all
// creds.update writes so concurrent saves can't corrupt the on-disk auth files.
let credSaveChain: Promise<void> = Promise.resolve()
function enqueueCredSave(): void {
  if (!saveCredsFn) return
  const fn = saveCredsFn
  credSaveChain = credSaveChain.then(() => fn()).catch((e) => {
    console.warn(`[whatsapp] credential save failed: ${e instanceof Error ? e.message : String(e)}`)
  })
}
async function flushCredSaves(): Promise<void> {
  await credSaveChain
}

// ── Inbound mapping (ported + extended from plugin.ts messages.upsert) ──────────────────────
async function handleUpsert(upsert: BaileysEventMap['messages.upsert']): Promise<void> {
  // `notify` = live; `append` = offline catch-up replayed on reconnect (filtered by watermark / dedupe /
  // real-content — see offline-catchup.ts). Anything else is not inbound.
  const { deliver, catchup } = inboundCatchup.select(upsert)
  if (catchup) {
    console.warn(`[whatsapp] offline catch-up: delivering ${catchup.count} message(s) that arrived while disconnected (oldest ${new Date(catchup.oldestMs).toISOString()})`)
  }
  for (const msg of deliver) {
    // inboundReceived = raw provider events handed to ingest (live notify + accepted catch-up append);
    // inboundEnqueued is counted inside the queue. Their divergence is what the framework's silent-drop
    // net watches (transport/health).
    inboundQueue.noteReceived()
    try {
      // Cache the raw message content (BOTH inbound and our own) so Baileys' getMessage can fetch the
      // ORIGINAL when it later processes a received MESSAGE_EDIT — otherwise the edit is dropped.
      if (msg.key?.id && msg.message) rememberMessage(msg.key.id, msg.message)
      await ingestMessage(msg)
    } catch (e) {
      // Never let one bad message wedge the stream — degrade and keep going.
      const remoteJid = msg.key?.remoteJid
      const from = msg.key?.fromMe ? undefined : remoteJid
      if (from) {
        enqueue({
          from,
          content: degradeContent('media', ''),
          timestamp: Date.now(),
          ...(msg.key?.id ? { providerMessageId: msg.key.id } : {}),
        })
      }
      console.warn(`[whatsapp] ingest error: ${e instanceof Error ? e.message : String(e)}`)
    }
    inboundCatchup.markHandled(msg) // after the commit, so the watermark never runs ahead of the backlog
  }
  if (deliver.length > 0) persistCursorState()
}

async function ingestMessage(msg: WAMessage): Promise<void> {
  if (msg.key.fromMe) return // skip our own echoes
  const remoteJid = msg.key.remoteJid
  if (!remoteJid) return

  const isGroup = remoteJid.endsWith('@g.us')
  // In a group the actual sender is `participant`; in a DM it's the remoteJid itself.
  const from = isGroup ? (msg.key.participant ?? remoteJid) : remoteJid
  const providerMessageId = msg.key.id ?? undefined
  const timestamp = msg.messageTimestamp ? Number(msg.messageTimestamp) * 1000 : Date.now()
  const m = msg.message
  if (!m) return
  // Record the Baileys message key so the onMarkRead hook (transport/mark_read) can look it up later.
  rememberInboundKey(providerMessageId, msg.key)
  // NO cursor here — enqueue() mints it at push time (contract Cursor rule 1; see CursorQueue). Anything
  // awaited below (media downloads) therefore cannot let a later message overtake this one.
  const base = {
    from,
    timestamp,
    ...(providerMessageId ? { providerMessageId } : {}),
  }

  // ── Reaction ──────────────────────────────────────────────────────────────────────────────
  const reactionMsg = m.reactionMessage
  if (reactionMsg) {
    const emoji = reactionMsg.text ?? ''
    if (!emoji) return // empty emoji = reaction removed → skip
    // Best-effort: resolve the reacted-to message to a ≤80-char snippet so the brain knows WHICH
    // message. When resolved, carry the fully-formed note in `content` (the adapter prefers a
    // non-empty reaction content); otherwise leave it empty so the framework uses its generic
    // `*reacted <emoji> to your message*` phrasing.
    const snippet = reactionSnippet(reactionMsg.key?.id)
    enqueue({
      ...base,
      content: snippet ? `*reacted ${emoji} to: "${snippet}"*` : '',
      kind: 'reaction',
      emoji,
      ...(reactionMsg.key?.id ? { targetMessageId: reactionMsg.key.id } : {}),
    })
    void markRead(msg)
    return
  }

  // ── Edit (protocolMessage MESSAGE_EDIT) ─────────────────────────────────────────────────────
  // In Baileys 7.x an inbound edit is normally unwrapped upstream and delivered via `messages.update`
  // (see handleMessagesUpdate) — this upsert path is the secondary route, kept for resilience. Both
  // routes share the `edit:<targetId>` dedup key so a single edit is enqueued at most once.
  const protocolMsg = m.protocolMessage
  if (protocolMsg && protocolMsg.type === proto.Message.ProtocolMessage.Type.MESSAGE_EDIT) {
    const targetId = protocolMsg.key?.id ?? providerMessageId
    const dedupId = `edit:${targetId ?? ''}`
    if (targetId && seenSet.has(dedupId)) return
    const newText = extractEditedText(protocolMsg.editedMessage)
    if (targetId) { seenSet.add(dedupId); seen.push(dedupId); if (seen.length > 1000) seen = seen.slice(-1000) }
    rememberMessageText(targetId, newText)
    enqueue({
      ...base,
      content: newText,
      kind: 'edit',
      ...(targetId ? { providerMessageId: targetId, targetMessageId: targetId } : {}),
    })
    return
  }

  // ── Voice note (audioMessage, ptt) ──────────────────────────────────────────────────────────
  const audioMsg = m.audioMessage
  if (audioMsg) {
    const isPtt = audioMsg.ptt === true
    const handle = await downloadToHandle(msg, {
      kind: 'audio',
      mime: audioMsg.mimetype ?? 'audio/ogg; codecs=opus',
      ...(isPtt ? { ptt: true } : {}),
      ...(audioMsg.seconds != null ? { duration: Number(audioMsg.seconds) } : {}),
    })
    if (handle) {
      enqueue({ ...base, content: isPtt ? '[voice note]' : '', media: [handle] })
    } else {
      enqueue({ ...base, content: degradeContent(isPtt ? 'voice note' : 'audio', '') })
    }
    void markRead(msg)
    return
  }

  // ── Image (+caption) ────────────────────────────────────────────────────────────────────────
  const imageMsg = m.imageMessage
  if (imageMsg) {
    const caption = imageMsg.caption ?? ''
    const handle = await downloadToHandle(msg, { kind: 'image', mime: imageMsg.mimetype ?? 'image/jpeg' })
    if (handle) {
      enqueue({ ...base, content: caption, media: [handle] })
    } else {
      enqueue({ ...base, content: degradeContent('photo', caption) })
    }
    void markRead(msg)
    return
  }

  // ── Document (documentMessage / documentWithCaptionMessage) ───────────────────────────────────
  const docMsg = m.documentMessage ?? m.documentWithCaptionMessage?.message?.documentMessage
  if (docMsg) {
    const caption = docMsg.caption ?? ''
    const fileName = docMsg.fileName ?? undefined
    const handle = await downloadToHandle(msg, { kind: 'file', mime: docMsg.mimetype ?? 'application/octet-stream' })
    if (handle) {
      if (fileName) handle.caption = handle.caption ?? fileName
      enqueue({ ...base, content: caption, media: [handle] })
    } else {
      enqueue({ ...base, content: degradeContent(fileName ? `document: ${fileName}` : 'document', caption) })
    }
    void markRead(msg)
    return
  }

  // ── Plain text (conversation / extendedTextMessage) ──────────────────────────────────────────
  const text = m.conversation ?? m.extendedTextMessage?.text ?? ''
  if (text) {
    rememberMessageText(providerMessageId, text) // so a reaction to THIS message resolves to a snippet
    enqueue({ ...base, content: text })
    void markRead(msg)
    return
  }

  // ── Honest degrade: sticker / video / unsupported — caption verbatim + bracketed placeholder ──
  if (m.stickerMessage) {
    enqueue({ ...base, content: '[sticker]' })
    void markRead(msg)
    return
  }
  if (m.videoMessage) {
    const caption = m.videoMessage.caption ?? ''
    enqueue({ ...base, content: degradeContent('video — not viewable yet', caption) })
    void markRead(msg)
    return
  }
  // Unknown kind — never silent. (No content + no handle means nothing surfaced otherwise.)
  enqueue({ ...base, content: degradeContent('unsupported message', '') })
  void markRead(msg)
}

/** Mark an inbound message read (best-effort) — the connector equivalent of the plugin's
 *  per-message readMessages call. */
async function markRead(msg: WAMessage): Promise<void> {
  try {
    if (sock && msg.key) await sock.readMessages([msg.key])
  } catch {
    /* best-effort — never break the stream */
  }
}

/** Extract the new text from a Baileys editedMessage envelope. The edit's replacement content is a
 *  normal IMessage (conversation / extendedTextMessage); image/document edits replace a caption. */
function extractEditedText(inner: proto.IMessage | null | undefined): string {
  if (!inner) return ''
  return (
    inner.conversation ??
    inner.extendedTextMessage?.text ??
    inner.imageMessage?.caption ??
    inner.videoMessage?.caption ??
    inner.documentMessage?.caption ??
    ''
  )
}

/** Handle `messages.update` — the delivery route for an INBOUND message edit (the user edits a
 *  message they already sent). Baileys 7.x unwraps a received MESSAGE_EDIT protocolMessage and emits
 *  it here as { key:{...,id:<edited msg id>}, update:{ message:{ editedMessage:{ message:<new
 *  IMessage> } } } }. We decode it into the SAME WireInbound{kind:'edit'} the upsert protocolMessage
 *  path produces, skip our OWN outbound edits (key.fromMe), and dedup (`edit:<id>`) so the same edit
 *  can't be enqueued twice if both delivery routes ever fire for one edit.
 *
 *  KNOWN LIMITATION (coexistence mode): when this number runs in WhatsApp Business / Cloud-API
 *  "coexistence" mode, an inbound EDIT is delivered to the companion device as an
 *  `<enc decrypt-fail='hide'>` node the companion CANNOT decrypt — so NO `messages.update` (and no
 *  `messages.upsert`) event ever fires for the edit, and it is silently invisible to us. This is a
 *  platform/coexistence decryption constraint, NOT a bug in this handler: both this handler and the
 *  getMessage store are correct and DO surface inbound edits on a DEDICATED, non-coexistence number.
 *  Ref: Baileys #1686 (coexistence decrypt-fail on inbound edits). */
async function handleMessagesUpdate(updates: BaileysEventMap['messages.update']): Promise<void> {
  for (const u of updates) {
    try {
      const key = u.key
      const upd = u.update
      if (!key || !upd) continue
      if (key.fromMe) continue // skip our own outbound edits — only echo edits made by the other party
      const editedEnvelope = (upd.message as proto.IMessage | undefined)?.editedMessage
      if (!editedEnvelope) continue // not an edit update (read receipt, status change, etc.)
      const targetId = key.id ?? undefined
      const from = key.remoteJid ?? undefined
      if (!targetId || !from) continue
      const dedupId = `edit:${targetId}`
      if (seenSet.has(dedupId)) continue
      const newText = extractEditedText(editedEnvelope.message)
      const t = (upd as { messageTimestamp?: number | { toString(): string } }).messageTimestamp
      const secs = typeof t === 'number' ? t : t ? Number(t.toString()) : 0
      const timestamp = (secs > 0 ? secs : Math.floor(Date.now() / 1000)) * 1000
      seenSet.add(dedupId); seen.push(dedupId); if (seen.length > 1000) seen = seen.slice(-1000)
      rememberMessageText(targetId, newText)
      enqueue({
        from,
        content: newText,
        timestamp,
        kind: 'edit',
        providerMessageId: targetId,
        targetMessageId: targetId,
      })
    } catch (e) {
      console.warn(`[whatsapp] messages.update edit decode error: ${e instanceof Error ? e.message : String(e)}`)
    }
  }
}

// ── Live socket lifecycle ───────────────────────────────────────────────────────────────────
async function getWaVersion(): Promise<[number, number, number]> {
  // Fallback to a known-working version if the dynamic fetch fails (ported from plugin.ts).
  const FALLBACK_VERSION: [number, number, number] = [2, 3000, 1033846690]
  try {
    const { version } = await fetchLatestBaileysVersion()
    return version as [number, number, number]
  } catch {
    console.warn(`[whatsapp] version fetch failed; using fallback ${FALLBACK_VERSION.join('.')}`)
    return FALLBACK_VERSION
  }
}

async function startSocket(): Promise<void> {
  // SINGLE-FLIGHT (M10.5 forensic fix #1): bail if a socket is live OR one is mid-creation. The
  // `socketStarting` sentinel is flipped SYNCHRONOUSLY (below) before the first await, so two
  // concurrent callers can't both reach makeWASocket — exactly ONE Baileys socket per pairing
  // window. This is what makes the QR and the pairing code come from the SAME socket.
  if (sock || socketStarting) return
  socketStarting = true
  connecting = true // 2a.4 (3): a (re)connect is now in flight (sock null until 'open')
  // A legitimate (re)start ends any release window: restore normal reconnect classification for the
  // new socket's closes. (The release fence is only meant to suppress the AUTO-reconnect that races a
  // quiesce; once the framework deliberately starts a socket again, normal close policy applies.)
  releasing = false
  try {
    await fsp.mkdir(AUTH_DIR, { recursive: true })
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR)
    saveCredsFn = saveCreds
    const version = await getWaVersion()
    inboundCatchup.armForConnect(Date.now()) // snapshot the offline catch-up floor for THIS socket
    // A NEW socket is opening: reset the per-socket pairing-code flag so THIS socket re-requests a
    // pairing code alongside its first QR (QR + code bound to one socket generation). Without this,
    // a fresh socket after a prior window would surface a rotating QR but inherit the OLD code.
    pairingRequested = false
    // pino MUST write to STDERR (fd 2), NOT stdout: the connector's stdout IS the MCP JSON-RPC
    // channel, and pino 9.x defaults to fd 1 (stdout) — any non-JSON-RPC byte there corrupts the
    // protocol (breaks JSON.parse in the framework's stream transport → aborts the drain loop, a
    // SILENT inbound failure). 'warn' un-mutes Baileys warnings/errors (heaviest during the exact
    // disconnect/decrypt-retry windows we care about) to the journal for diagnosability; the explicit
    // pino.destination(2) keeps them off the wire.
    const logger = pino({ level: 'warn' }, pino.destination(2))

    const s = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      logger,
      printQRInTerminal: false,
      // CRITICAL (M10.5): the browser tuple's middle slot is the WhatsApp companion CLIENT TYPE, and
      // 'Desktop' → Electron, which WhatsApp REJECTS for pairing-code registration with
      // `400 bad-request` on companion_hello (proven empirically + per Baileys docs: pairing code
      // needs a "valid/logical browser config"). Use a real web-client identity so the companion
      // registration is accepted. This is version-independent — the previous ['My-Agent','Desktop',…]
      // failed identically on baileys rc.9 AND rc13.
      browser: Browsers.ubuntu('Chrome'),
      version,
      // B2: a tighter keepalive than Baileys' 30s default → brisk ping cadence (and thus inbound
      // frame cadence on a healthy socket), so Baileys' own keepalive watchdog reacts faster AND the
      // frame-based watchdog below has several cycles inside its window. See watchdog.ts.
      keepAliveIntervalMs: KEEPALIVE_INTERVAL_MS,
      markOnlineOnConnect: false,
      syncFullHistory: false,
      // Baileys fetches the ORIGINAL message here when processing a received MESSAGE_EDIT. Without it
      // an inbound edit is silently dropped (no `fromMe:false` messages.update fires). We serve the
      // message cached on upsert. Signature: (key: WAMessageKey) => Promise<IMessage | undefined>.
      getMessage: async (key) => messageStore.get(key.id ?? '') ?? undefined,
    })
    sock = s

    // Guard raw WebSocket errors so they can't crash the process.
    const ws = (s as unknown as {
      ws?: { on?: (ev: string, cb: (...a: unknown[]) => void) => void; close?: () => Promise<void> | void }
    }).ws
    if (ws && typeof ws.on === 'function') {
      ws.on('error', (err) => console.warn(`[whatsapp] websocket error: ${err instanceof Error ? err.message : String(err)}`))
      // B2 (invariant #7): every decoded inbound frame is the lowest-level proof the upstream socket
      // is alive (Baileys emits `'frame'` on each one — keepalive pongs included). Bump the activity
      // clock here; the watchdog below force-closes when this goes stale past the window.
      ws.on('frame', () => { lastActivityAt = Date.now() })
    }
    // B2 ZOMBIE WATCHDOG: a true silent half-open fires NO connection.update, so we cannot wait for a
    // close — we derive liveness from frame activity and FORCE a close when it stalls. Reset the clock
    // to "now" (grace) and (re)arm a fresh interval, clearing any prior socket's so they never stack.
    // Always bump the activity clock (transport/health derives `lastUpstreamActivityMs` from it and
    // MUST stay truthful regardless of the kill-switch) and clear any prior socket's interval.
    lastActivityAt = Date.now()
    clearWatchdog()
    // KILL-SWITCH (A6 — _CONTRACT.md §Kill-switch `SELFHEAL_WATCHDOG_OFF`): arm the connector's OWN
    // zombie watchdog ONLY when the flag is not set to '1'. With it off, the connector does NOT
    // self-reconnect — a zombie upstream stays a blackhole until the FRAMEWORK net
    // (probeLiveness → transport/health → supervisor.recycle) catches it, which is exactly the
    // live-blackhole proof. The literal env-var name is inlined here (connectors do not import
    // framework modules); the strict `!== '1'` verdict lives in the pure, unit-tested watchdog.ts.
    // transport/health stays honest either way — only ARMING is gated, never the health report.
    if (watchdogEnabled(process.env.SELFHEAL_WATCHDOG_OFF)) {
      watchdogTimer = setInterval(() => {
        // Guard (per task): only police a LIVE, non-cert socket. isCertRun() (ECHO_MODE / the explicit
        // cert env / conformanceSeen) means no real upstream; `!sock` means a teardown already nulled
        // it (the guard keeps the interval inert even if one ever lingers before its clear).
        // `sock === s` keeps this watchdog bound to ITS OWN socket — a stale sibling interval can
        // never close the current live socket.
        if (!sock || sock !== s || isCertRun()) return
        if (!shouldForceClose(lastActivityAt, Date.now(), WATCHDOG_WINDOW_MS)) return
        console.warn(
          `[whatsapp] zombie watchdog — no upstream frame in >${WATCHDOG_WINDOW_MS}ms; forcing close → reconnect`,
        )
        // Force the close a half-open never emits. ws.close() drives Baileys' ws 'close' handler
        // (statusCode connectionClosed → the B1 environmental reconnect). NEVER terminate() — it does
        // NOT exist on the Baileys rc13 WebSocketClient; ws.close() is the supported force-close.
        // sock.end(error) is the fallback if the raw ws handle is somehow absent.
        try {
          if (ws && typeof ws.close === 'function') {
            void Promise.resolve(ws.close()).catch(() => {})
          } else {
            s.end(new Error('zombie watchdog force-close'))
          }
        } catch (e) {
          console.warn(`[whatsapp] zombie watchdog force-close failed: ${e instanceof Error ? e.message : String(e)}`)
        }
      }, WATCHDOG_CHECK_MS)
    } else {
      console.warn('[whatsapp] SELFHEAL_WATCHDOG_OFF=1 — connector zombie watchdog NOT armed; relying on the framework probe→recycle net')
    }

    s.ev.on('creds.update', () => {
      enqueueCredSave()
      // Promotion to linked may happen via creds.update (before connection:'open'). Mirror the live
      // connector: if creds.registered flips true here, mark wasEverLinked so a subsequent logout pages.
      if (isRealRegistration(state.creds)) wasEverLinked = true
    })

    // A-F1 (P3): track whether companion_hello was accepted on THIS socket (i.e. whether preflight_ok
    // has already fired). The first `qr` event proves companion_hello succeeded — we emit preflight_ok
    // exactly ONCE per socket, on the same socket, with no separate probe (A-F1: no throwaway socket).
    let preflightOkEmitted = false

    s.ev.on('connection.update', (update: BaileysEventMap['connection.update']) => {
      const { connection, lastDisconnect, qr } = update
      // A `qr` string is a scannable WhatsApp login QR, emitted (and rotated, ~20-60s) until the
      // device links. M10.5 interactive onboarding surfaces BOTH paths: render the QR to a PNG handle
      // (image artifact, latest-wins on rotation) AND request a Linked-Devices pairing code (code
      // artifact). The framework presents the QR as a plain image in the conversation and relays the
      // code in chat; the user links by whichever they prefer. Single-flight (fix #1) guarantees this
      // QR and the code below come from THIS one socket. Bytes never ride the wire — only the handle.
      if (qr && PHONE_NUMBER && !state.creds.registered) {
        // A-F1 (P3) PREFLIGHT OK: the first QR proves companion_hello was accepted by the server —
        // the pairing socket is live and accepted. Emit preflight_ok ONCE on this socket (same socket,
        // no separate probe) before the user is asked for the code. Subsequent QR rotations are not
        // re-emitted (the socket is still alive; only one preflight_ok per socket lifecycle).
        if (!preflightOkEmitted) {
          preflightOkEmitted = true
          setOnboardStatus('preflight_ok')
        }
        void surfaceQrArtifact(qr)
        if (!pairingRequested) {
          pairingRequested = true
          void requestAndSurfacePairingCode()
        } else {
          // A subsequent `qr` after the code was already requested is a rotation of THIS socket's QR
          // (latest-wins on `onboardImage`). The socket is still alive and still pairing, so the
          // status stays `waiting` — the rotated QR pairs with the SAME socket's existing code. (We
          // no longer flip to `expired` on a mere rotation — that comment assumed >1 socket and was
          // actively misleading once single-flight guarantees exactly one.)
        }
      }

      if (connection === 'open') {
        // Linked. Clear the spent QR/code linking artifacts and report the terminal linked state.
        // STICKY: the socket stays alive (we do NOT null it) so it can drain the owner-bind inbound
        // and serve as the live WhatsApp channel — no poll/regen may tear down a linked socket.
        //
        // GROUND-TRUTH: gate STRICTLY on isRealRegistration (creds.registered === true). A bare
        // `connection:'open'` without creds.registered means the socket opened but the companion
        // registration was NOT confirmed (e.g. QR arrived mid-pairing but the user hasn't linked yet).
        // The framework's live-pair gate and the conformance pairing-truth probe both require
        // `registered:true` on this artifact; a missing/false value is "not really paired".
        pairingRequested = false
        const isRegistered = isRealRegistration(state.creds)
        if (isRegistered) {
          wasEverLinked = true
          reconnectAttempts = 0 // B1: a healthy registered session re-established → backoff resets
          lastDisconnectReason = undefined // B3: a live upstream again → clear the stale down-reason
          connecting = false // 2a.4 (3): the (re)connect landed — no longer in the connecting window
        }
        setOnboardStatus('linked', undefined, isRegistered)
      }

      if (connection === 'close') {
        const statusCode = (lastDisconnect?.error as { output?: { statusCode?: number } })?.output?.statusCode
        // Identity-scoped null: only clear the global if the CLOSING socket is the current one. With
        // single-flight there is only ever one, but this keeps the pointer honest if a stale sibling
        // ever closes — it can never null a live socket out from under a scan/owner-bind drain.
        if (sock === s) sock = null

        const registered = isRealRegistration(state.creds)
        // B1: a single source of truth for the close verdict (pure, unit-tested in close-policy.ts).
        const decision = decideClose({ statusCode, registered, wasEverLinked, attempt: reconnectAttempts, releasing })

        // NEVER-SILENT (B1): log EVERY close with its verdict. pino is `level:'silent'`, so the old
        // handler dropped real drops with no journal trace — this `console.warn` (→ stderr → journal)
        // is the visibility that was missing on 2026-06-28. One line per close, always.
        const verdict =
          decision.action === 'reconnect'
            ? `reconnect (cause=${decision.cause}, in ${decision.delayMs}ms${decision.cause === 'environmental' ? `, attempt=${reconnectAttempts + 1}` : ''})`
            : decision.action
        console.warn(
          `[whatsapp] connection close — statusCode=${statusCode ?? 'undefined'} registered=${registered} wasEverLinked=${wasEverLinked} → ${verdict}`,
        )

        // B3 / 2a.4 (4): record the close classification so `transport/health` can surface it as
        // `reason` while the upstream is down (terminal logged_out vs. recoverable network/transient
        // — Invariant 5). The mapping lives in close-policy.ts (`closeHealthReason`, unit-tested) so a
        // pure connectivity DROP maps to 'network', NEVER 'logged_out' (which would QR the live bot).
        lastDisconnectReason = closeHealthReason(decision)

        switch (decision.action) {
          case 'preflight_rejected':
            // A-F1 (P3): a 400 bad-request on close = companion_hello rejected (bad client identity).
            // Surface preflight_rejected on THIS socket (no second probe socket) and stay idle; the
            // pairing code is never requested in this path — registration was not accepted.
            pairingRequested = false
            connecting = false // 2a.4 (3): identity rejected — not reconnecting; no transient masking
            setOnboardStatus(
              'preflight_rejected',
              'registration rejected — likely invalid client identity (400 bad-request on companion_hello)',
            )
            return
          case 'terminal_logged_out':
            // TERMINAL: a logout AFTER a real prior link. Page the user (needs_attention) and idle —
            // NEVER reconnect a logged-out session (that would spin against a revoked credential).
            loggedOut = true
            connecting = false // 2a.4 (3): terminal — not reconnecting; health must page, not defer
            void mcp.notification({ method: NEEDS_ATTENTION, params: { reason: 'logged_out' } }).catch(() => {})
            return
          case 'reconnect':
            // 515 = Baileys' post-scan link-completion restart (fast, fixed delay). Environmental =
            // a real session dropped (408/428/500/connectionLost/undefined) → capped exponential
            // backoff, retried FOREVER (the network heals when it heals; the supervisor crash-breaker
            // is a separate layer). The counter only advances for environmental drops.
            pairingRequested = false
            connecting = true // 2a.4 (3): a reconnect is scheduled → cover the whole sock===null gap
            if (decision.cause === 'environmental') reconnectAttempts += 1
            setTimeout(() => {
              void startSocket().catch((e) =>
                console.warn(`[whatsapp] reconnect (${decision.cause}) failed: ${e instanceof Error ? e.message : String(e)}`),
              )
            }, decision.delayMs)
            return
          case 'idle':
            // An UNPAIRED QR/code window expired/dropped (never linked): GO IDLE — do NOT auto-cycle a
            // fresh QR. Regeneration is ON-DEMAND (next onboarding_poll mints a fresh QR), so there is
            // no perpetual pairing-socket churn. See _CONTRACT.md.
            pairingRequested = false
            connecting = false // 2a.4 (3): idle — not reconnecting; no transient masking
            setOnboardStatus('expired')
            return
        }
      }
    })

    s.ev.on('messages.upsert', (u: BaileysEventMap['messages.upsert']) => {
      void handleUpsert(u)
    })
    // Inbound edits arrive HERE, not via messages.upsert. In Baileys 7.x a received MESSAGE_EDIT
    // protocolMessage is unwrapped inside processMessage → emitted as a `messages.update` whose
    // update.message.editedMessage.message is the new content. The upsert protocolMessage path
    // (ingestMessage) is the secondary route kept for resilience; both share the `edit:<id>` dedup.
    s.ev.on('messages.update', (u: BaileysEventMap['messages.update']) => {
      void handleMessagesUpdate(u)
    })
  } finally {
    // Release the single-flight sentinel once creation has either fully wired up `sock` or thrown.
    // `sock` itself is now the "live" guard; `socketStarting` only covers the await window above.
    socketStarting = false
  }
}

/** Render Baileys' scannable QR string to a PNG, PUT it to the media endpoint, and set the resulting
 *  handle as the `image` onboarding artifact (latest-wins on rotation). Best-effort: if QR render or
 *  the media PUT fails (e.g. no media endpoint), the pairing-code path still onboards — never throws
 *  past here. Bytes go out-of-band over the media endpoint; only the JSON handle rides the wire. */
async function surfaceQrArtifact(qr: string): Promise<void> {
  try {
    if (!MEDIA_TOKEN) return // no endpoint to stage the PNG → fall back to the code path only
    const png = await qrcode.toBuffer(qr, { type: 'png' })
    const handle = await mediaPut(png, { mime: 'image/png', kind: 'image' })
    setOnboardImage(handle, 'Scan to link WhatsApp')
    setOnboardStatus('waiting')
    qrWaiters.splice(0).forEach((r) => r()) // wake any on-demand poll awaiting a fresh QR
    console.warn('[whatsapp] QR artifact surfaced (scannable)')
  } catch (e) {
    console.warn(`[whatsapp] QR artifact failed: ${e instanceof Error ? e.message : String(e)}`)
  }
}

/** Request a Linked-Devices pairing code for WHATSAPP_PHONE_NUMBER and SET it as the `code`
 *  onboarding artifact (M10.5 — the framework relays it to the user in chat). The user types it on
 *  their phone (Settings → Linked Devices → Link a Device → Link with phone number → enter code). */
async function requestAndSurfacePairingCode(): Promise<void> {
  try {
    if (!sock || !PHONE_NUMBER) return
    const normalized = PHONE_NUMBER.replace(/[^\d]/g, '')
    if (normalized.length < 7) {
      console.warn('[whatsapp] WHATSAPP_PHONE_NUMBER too short — include country code')
      return
    }
    // RAW 8-char code, no hyphen: WhatsApp's "Link with phone number" field expects the 8 characters
    // verbatim; a cosmetic hyphen risks a literal-mismatch on entry. The old plugin returned raw too.
    const code = await sock.requestPairingCode(normalized)
    setOnboardCode(code, 'open WhatsApp → Settings → Linked Devices → Link a Device → Link with phone number, and enter this 8-character code (no spaces/dashes; expires ~60s)')
    setOnboardStatus('waiting')
    codeRequestedAt = Date.now()
    console.warn('[whatsapp] pairing code surfaced as onboarding artifact')
  } catch (e) {
    console.warn(`[whatsapp] requestPairingCode failed: ${e instanceof Error ? e.message : String(e)}`)
    // A pairing-code failure is human-actionable: surface needs_attention so the user is re-prompted,
    // and reflect it in the handshake status so the framework can re-present a fresh artifact.
    setOnboardStatus('error', 'pairing code request failed')
    void mcp.notification({ method: NEEDS_ATTENTION, params: { reason: 'pairing_expired' } }).catch(() => {})
  }
}

// ── Outbound send (ported from plugin.ts send / sendAudio) ──────────────────────────────────
/** Deliver one media handle to a JID: GET bytes from the endpoint, then send via the right Baileys
 *  message kind. Best-effort — a media failure never throws past the turn (text already delivered). */
async function sendMediaHandle(to: string, handle: MediaHandle): Promise<void> {
  try {
    if (!sock) return
    const bytes = await mediaGet(handle.id)
    if (handle.kind === 'image') {
      await sock.sendMessage(to, { image: bytes, ...(handle.caption ? { caption: handle.caption } : {}) })
    } else if (handle.kind === 'audio') {
      // A voice note (PTT bubble) when ptt, else a generic audio attachment.
      await sock.sendMessage(to, {
        audio: bytes,
        mimetype: handle.mimeType || 'audio/ogg; codecs=opus',
        ptt: handle.ptt !== false,
      })
    } else {
      await sock.sendMessage(to, {
        document: bytes,
        mimetype: handle.mimeType || 'application/octet-stream',
        fileName: handle.caption ?? `file-${handle.id}`,
      })
    }
  } catch (e) {
    console.warn(`[whatsapp] outbound media handle ${handle.id} (${handle.kind}) failed: ${e instanceof Error ? e.message : String(e)}`)
  }
}

// ── Typing indicator (ported from plugin.ts sendPresenceUpdate) ──────────────────────────────
// WhatsApp's "composing…" presence auto-expires (~5s), so a held indicator is refreshed below that.
const TYPING_REFRESH_MS = 4000
let typingChat: string | null = null
let typingTimer: ReturnType<typeof setInterval> | null = null

function clearTyping(): void {
  if (typingTimer !== null) {
    clearInterval(typingTimer)
    typingTimer = null
  }
  if (typingChat && sock) void sock.sendPresenceUpdate('paused', typingChat).catch(() => {})
  typingChat = null
}

function startTyping(to: string): void {
  if (!sock) return
  if (typingTimer !== null) clearInterval(typingTimer)
  typingChat = to
  void sock.sendPresenceUpdate('composing', to).catch(() => {})
  typingTimer = setInterval(() => {
    if (typingChat && sock) void sock.sendPresenceUpdate('composing', typingChat).catch(() => {})
  }, TYPING_REFRESH_MS)
}

function noteSend(to: string): void {
  if (typingChat === to) clearTyping()
}

// ── MCP server ──────────────────────────────────────────────────────────────────────────────
const mcp = new Server({ name: 'whatsapp', version: '1.0.0' }, { capabilities: { tools: {} } })

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
        properties: { to: { type: 'string' }, action: { type: 'string', enum: ['start', 'stop'] } },
        required: ['to', 'action'],
      },
    },
    {
      name: REACTION_TOOL,
      description: 'react to a message with an emoji (best-effort)',
      inputSchema: {
        type: 'object',
        properties: { to: { type: 'string' }, messageId: { type: 'string' }, emoji: { type: 'string' } },
        required: ['to', 'messageId', 'emoji'],
      },
    },
    {
      name: EDIT_TOOL,
      description: 'edit a previously-sent message (best-effort)',
      inputSchema: {
        type: 'object',
        properties: { to: { type: 'string' }, messageId: { type: 'string' }, content: { type: 'string' } },
        required: ['to', 'messageId', 'content'],
      },
    },
    {
      name: MARK_READ_TOOL,
      description: 'mark an inbound message read (send a WhatsApp read receipt — best-effort)',
      inputSchema: {
        type: 'object',
        properties: { messageId: { type: 'string' }, from: { type: 'string' } },
        required: ['messageId', 'from'],
      },
    },
    {
      name: QUIESCE_TOOL,
      description: 'export the live session (flush creds + close socket) before a code re-derive',
      inputSchema: {
        type: 'object',
        properties: { action: { type: 'string', enum: ['export'] } },
        required: ['action'],
      },
    },
    {
      name: ONBOARDING_POLL_TOOL,
      description: 'poll the current interactive-onboarding artifacts (QR image / pairing code / status)',
      inputSchema: { type: 'object', properties: {} },
    },
    {
      name: HEALTH_TOOL,
      description: 'report REAL upstream-socket liveness (never true without a live socket)',
      inputSchema: { type: 'object', properties: {} },
    },
  ],
}))

/**
 * Outbound text content with its link preview attached (see link-preview.ts). Text without a URL is
 * returned untouched. With a URL, `linkPreview` is the built preview or `null` (plain text) — never
 * throws, never waits longer than the preview timeout.
 */
async function withLinkPreview(content: string): Promise<AnyMessageContent> {
  if (!extractFirstUrl(content)) return { text: content }
  const preview = await buildLinkPreview(content)
  return { text: content, linkPreview: preview } as AnyMessageContent
}

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name === SEND_TOOL) {
    const { to, content, media } = req.params.arguments as { to: string; content: string; media?: MediaHandle[] }
    // S1.3 (a): the cert-vs-live decision is EXPLICIT (isCertRun), NEVER inferred from the target
    // shape. The old `ECHO_MODE || !isDeliverableTarget(to)` poisoned a LIVE session: a reply to a
    // target that merely didn't match the JID regex latched conformanceSeen (→ cert mode) AND echoed
    // the outbound back as a FAKE inbound. The pure send-policy returns the route.
    const route = decideSendRoute({ certRun: isCertRun(), deliverable: isDeliverableTarget(to) })
    if (route === 'echo') {
      noteLoopback() // explicit cert run → disarm the deferred live connect (a re-cert opens no 2nd socket)
      // Carry content VERBATIM (the render prover compares the rendered wire bytes intact) and echo
      // media handles JSON-only with ids INTACT (the images carry-fidelity probe matches on id) —
      // NO endpoint call, NO bytes: §1 holds and re-staging would change the id and fail the probe.
      enqueue({
        from: to,
        content,
        timestamp: Date.now(),
        ...(media && media.length > 0 ? { media } : {}),
      })
      // S1.3 (b): NEVER report 'sent' for a loopback — it was echoed locally, not transmitted to a
      // real provider. 'echoed' is honest; the adapter only parses a JSON {messageId}, so this is inert.
      return { content: [{ type: 'text', text: 'echoed' }] }
    }
    if (route === 'drop_non_deliverable') {
      // S1.3 (a)+(b): a LIVE send to a non-deliverable target. DROP it LOUDLY — never echo a fake
      // inbound, never latch conformanceSeen, never claim 'sent'. (pino is un-muted but this is a
      // connector-level decision, so console.warn → stderr → journal is the right surface.)
      console.warn(`[whatsapp] DROPPED send — target "${to}" is not a deliverable WhatsApp JID; nothing sent (NOT reporting 'sent')`)
      return { content: [{ type: 'text', text: 'not-delivered: target is not a deliverable WhatsApp JID' }] }
    }
    // ── LIVE send (route === 'live') ──
    if (!sock) throw new Error('whatsapp socket not connected')
    noteSend(to) // the outbound auto-clears the typing presence
    let messageId: string | undefined
    // content arrives ALREADY RENDERED for the whatsapp dialect (*bold* _italic_ ~strike~ `code`) —
    // send VERBATIM. A pure-media reply arrives with empty content → skip the blank text bubble.
    if (content && content.length > 0) {
      // Link preview: when the text carries a URL, build the preview OURSELVES (short timeout, private-
      // address guard, sharp thumbnail) and pass it explicitly. `null` = no preview → plain text, never
      // blocks longer than the preview timeout. Passing it (even null) stops Baileys' own uncapped fetch.
      const result = await sock.sendMessage(to, await withLinkPreview(content))
      messageId = result?.key?.id ?? undefined
      // Remember our own outbound text so a reaction to THIS reply resolves to a snippet, and cache
      // the sent message so getMessage can resolve an edit of it.
      rememberMessageText(messageId, content)
      if (result?.key?.id && result.message) rememberMessage(result.key.id, result.message)
    }
    if (media && media.length > 0) {
      for (const handle of media) await sendMediaHandle(to, handle)
    }
    const text = messageId !== undefined ? JSON.stringify({ messageId }) : 'sent'
    return { content: [{ type: 'text', text }] }
  }

  if (req.params.name === DRAIN_TOOL) {
    const { sinceCursor } = req.params.arguments as { sinceCursor: string | null }
    return { content: [{ type: 'text', text: JSON.stringify({ messages: drain(sinceCursor) }) }] }
  }

  if (req.params.name === TYPING_TOOL) {
    const { to, action } = req.params.arguments as { to: string; action: 'start' | 'stop' }
    if (!ECHO_MODE && isDeliverableTarget(to) && sock) {
      if (action === 'start') startTyping(to)
      else clearTyping()
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === REACTION_TOOL) {
    const { to, messageId, emoji } = req.params.arguments as { to: string; messageId: string; emoji: string }
    if (!ECHO_MODE && isDeliverableTarget(to) && sock) {
      try {
        await sock.sendMessage(to, { react: { text: emoji, key: { remoteJid: to, id: messageId, fromMe: false } } })
      } catch (e) {
        console.warn(`[whatsapp] reaction failed: ${e instanceof Error ? e.message : String(e)}`)
      }
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === EDIT_TOOL) {
    const { to, messageId, content } = req.params.arguments as { to: string; messageId: string; content: string }
    if (!ECHO_MODE && isDeliverableTarget(to) && sock) {
      try {
        await sock.sendMessage(to, { ...(await withLinkPreview(content)), edit: { remoteJid: to, id: messageId, fromMe: true } } as AnyMessageContent)
      } catch (e) {
        console.warn(`[whatsapp] edit failed: ${e instanceof Error ? e.message : String(e)}`)
      }
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === MARK_READ_TOOL) {
    const { messageId, from } = req.params.arguments as { messageId: string; from: string }
    // Best-effort: no-op in ECHO mode, when sock is null, or when the key isn't in the recent map
    // (the message may predate this connector instance). The onMarkRead lifecycle hook drives this.
    if (!ECHO_MODE && sock && messageId) {
      const key = recentInboundKeys.get(messageId)
      if (key) {
        try {
          await sock.readMessages([key])
        } catch (e) {
          console.warn(`[whatsapp] mark_read failed (${from}): ${e instanceof Error ? e.message : String(e)}`)
        }
      }
    }
    return { content: [{ type: 'text', text: 'ok' }] }
  }

  if (req.params.name === QUIESCE_TOOL) {
    const { action } = req.params.arguments as { action: string }
    if (action !== 'export') return { content: [{ type: 'text', text: JSON.stringify({ status: 'failed' }) }] }
    // No live session held (echo mode or never connected) → not_held, leave login intact.
    if (ECHO_MODE || !sock) {
      return { content: [{ type: 'text', text: JSON.stringify({ status: 'not_held' }) }] }
    }
    try {
      await flushCredSaves() // serialize: ensure all pending creds.update writes have landed in ./auth
      persistCursorState()  // flush cursor so a restarted connector resumes from the right position
      clearWatchdog()       // B2: this is an INTENTIONAL session release — stop the watchdog so it
                            // can't force-close (and trigger a reconnect) against the quiesced socket
      releasing = true      // SESSION-RELEASE FENCE: latch BEFORE sock.end() so the resulting close is
                            // classified idle (no reconnect) — the old connector must not re-grab the
                            // login while the framework re-derives/re-pairs. Cleared in startSocket().
      sock.end(undefined)   // release the live login
      sock = null
      return { content: [{ type: 'text', text: JSON.stringify({ status: 'exported' }) }] }
    } catch (e) {
      console.warn(`[whatsapp] quiesce export failed: ${e instanceof Error ? e.message : String(e)}`)
      return { content: [{ type: 'text', text: JSON.stringify({ status: 'failed' }) }] }
    }
  }

  if (req.params.name === ONBOARDING_POLL_TOOL) {
    // OLD-PLUGIN PARITY (M10.5 root-cause fix): mint/re-request the pairing code ONLY on the user's
    // EXPLICIT ask (`refresh:true` — from get_onboarding_artifact), NEVER on a background status
    // poll. Each `requestPairingCode` regenerates the code AND its companion-registration keys, so an
    // automatic re-request racing the user's entry invalidates the very code they are typing →
    // WhatsApp rejects it instantly (the bug we hunted). The deleted in-process plugin requested the
    // code exactly ONCE, on the user's action — this restores that contract. Without `refresh`, the
    // poll is strictly READ-ONLY: it opens nothing and re-requests nothing, just returns the current
    // snapshot. So the frequent background status polls can never churn a code out from under entry.
    const refresh =
      (req.params.arguments as { refresh?: boolean } | undefined)?.refresh === true
    // A cert run is recognised by the EXPLICIT signal (isCertRun): ECHO_MODE (no phone), the explicit
    // TRANSPORT_CONFORMANCE cert env (covers a cert env that carries WHATSAPP_PHONE_NUMBER yet has no
    // real link), or an already-latched conformanceSeen. In a cert run, NEVER open a live Baileys
    // socket (defensive: the probe polls without refresh, but a refresh during cert must not connect).
    const certRun = isCertRun()
    if (refresh && !certRun && !loggedOut && onboardStatus.state !== 'linked') {
      if (!sock && !socketStarting) {
        // Idle → open a fresh single-flight socket; its first qr triggers the ONE-TIME code request,
        // so THIS explicit ask returns a code freshly minted at ask-time.
        void startSocket().catch((e) =>
          console.warn(`[whatsapp] poll-triggered start failed: ${e instanceof Error ? e.message : String(e)}`),
        )
        await Promise.race([new Promise<void>((r) => qrWaiters.push(r)), sleep(8000)])
      } else if (sock && codeRequestedAt > 0 && Date.now() - codeRequestedAt > CODE_FRESH_MS) {
        // Socket alive but the code aged out (>~45s): re-request ONE fresh code on THIS explicit ask
        // (asks are user-paced, and this path is unreachable from background polls). Never while
        // a code is still fresh — so a quick double-ask returns the SAME live code, not a new one.
        await requestAndSurfacePairingCode()
      }
    }
    // ECHO/CERT (conformance): a deterministic synthetic handshake (no socket / no media endpoint).
    // certRun also covers a cert env that carries WHATSAPP_PHONE_NUMBER (ECHO_MODE off) but is
    // proven a cert run by the harness's loopback send (conformanceSeen).
    // LIVE: the current real artifact snapshot (QR handle / pairing code / status), latest-wins.
    const artifacts = certRun ? echoOnboardingSnapshot() : onboardingSnapshot()
    // Return BOTH a structuredContent map and a JSON text block: the framework adapter reads
    // structuredContent first, then falls back to parsing the text block (SDK-shape resilient).
    return {
      structuredContent: { artifacts },
      content: [{ type: 'text', text: JSON.stringify({ artifacts }) }],
    }
  }

  if (req.params.name === HEALTH_TOOL) {
    // B3: the never-lie upstream-liveness report. A cert run (isCertRun) holds no real Baileys
    // socket, so it reports a synthetic "up" (the CRIES-WOLF affordance the A4 probe requires).
    // LIVE: derive STRICTLY from the real socket — `sock` presence, the ws-client liveness (CLOSING/
    // CLOSED ⇒ down), and the B2 frame-activity clock (stale ⇒ silent half-open ⇒ down). NEVER true
    // without a live socket.
    const certRun = isCertRun()
    // S1.3 (c): Baileys rc13's `sock.ws` is a WebSocketClient whose inner `ws` socket is PROTECTED,
    // so `sock.ws.readyState` is undefined (a DEAD field). Read the rc13 getters (isOpen/isClosing/
    // isClosed/isConnecting) and map them to a WS_* readyState via the pure helper.
    const wsClient = (sock as unknown as {
      ws?: { isOpen?: boolean; isClosing?: boolean; isClosed?: boolean; isConnecting?: boolean }
    } | null)?.ws
    const wsReadyState = wsReadyStateFromClient(wsClient)
    const report = computeUpstreamHealth({
      certRun,
      sockPresent: sock !== null,
      wsReadyState,
      lastActivityAt,
      now: Date.now(),
      lastDisconnectReason,
      connecting, // 2a.4 (3): the post-recycle / in-flight (re)connect window → transient, not recycle
    })
    // OPTIONAL inbound self-report counters, additive to the never-lie upstream report (contract Cursor
    // rule 3): the framework compares inboundEnqueued against what it drains.
    const reportWithInbound = { ...report, ...inboundQueue.counters() }
    // Return BOTH structuredContent and a JSON text block (the framework reads structuredContent
    // first, then falls back to parsing the text block — SDK-shape resilient).
    return {
      structuredContent: reportWithInbound as unknown as Record<string, unknown>,
      content: [{ type: 'text', text: JSON.stringify(reportWithInbound) }],
    }
  }

  throw new Error(`unknown tool: ${req.params.name}`)
})

// ── Boot ──────────────────────────────────────────────────────────────────────────────────────
// Read auth dir BEFORE connecting so ECHO_MODE / wasEverLinked are known. Skipped when there is
// clearly no live identity (no phone) to keep a fresh-build cert fully offline.
if (HAS_PHONE || existsSync(AUTH_DIR)) {
  try {
    await fsp.mkdir(AUTH_DIR, { recursive: true })
    const { state } = await useMultiFileAuthState(AUTH_DIR)
    if (isRealRegistration(state.creds)) wasEverLinked = true
  } catch { /* unreadable creds → treat as unregistered */ }
  loadCursorState()
}
// Re-finalize ECHO_MODE now that we know wasEverLinked (= a real registered creds set was found).
ECHO_MODE = !HAS_PHONE && !wasEverLinked

// B2: on graceful shutdown (the supervisor sends SIGTERM on an intentional bounce), stop the watchdog
// interval so it can't force-close mid-teardown, release the live socket best-effort, and exit clean.
process.on('SIGTERM', () => {
  clearWatchdog()
  try { sock?.end(undefined) } catch { /* best-effort */ }
  process.exit(0)
})

await mcp.connect(new StdioServerTransport())

if (!ECHO_MODE && !CONFORMANCE_ENV) {
  void warmLinkPreview() // pre-load link-preview-js + sharp so the first preview skips the cold start (never throws)
  // S1.3 (a): skip the live connect entirely under the explicit cert env (TRANSPORT_CONFORMANCE) —
  // a recert over a registered folder (ECHO_MODE=false) is a cert run, never a live session, so it
  // must open NO real socket. (conformanceSeen still disarms the timer below as a second belt.)
  // Defer the live connect until shortly AFTER the MCP handshake. The conformance harness drives a
  // loopback send immediately after connecting, which disarms this timer (noteLoopback) — so a
  // re-cert, even over a registered folder, never opens a second socket and can't steal updates.
  // LIVE boot is LAZY for an unpaired connector (old-plugin parity). If creds already exist
  // (registered), auto-open the socket to RESUME the linked session. If NOT yet registered, open
  // NOTHING here: a pairing code minted at boot would be stale, and an idle QR loop exhausts to
  // a 408. Instead the socket opens on the user's first EXPLICIT ask (onboarding_poll{refresh:true}).
  deferredConnectTimer = setTimeout(() => {
    deferredConnectTimer = null
    if (conformanceSeen) return // cert run confirmed by loopback send — skip live connect
    void (async () => {
      try {
        await fsp.mkdir(AUTH_DIR, { recursive: true })
        const { state } = await useMultiFileAuthState(AUTH_DIR)
        if (isRealRegistration(state.creds)) {
          await startSocket()
        } else {
          console.warn('[whatsapp] not yet paired — deferring socket until the first onboarding ask (lazy)')
        }
      } catch (e) {
        console.warn(`[whatsapp] boot check failed: ${e instanceof Error ? e.message : String(e)}`)
      }
    })()
  }, LIVE_CONNECT_GRACE_MS)
}
