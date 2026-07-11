---
name: discord
provides: transport
interface: transport
backend: discord-gateway
template_version: 1
provenance: agent
roles:
  - dedicated
session_held: false
upstream: push
# The connector's OWN frame-silence window — WATCHDOG_WINDOW_MS in src/upstream.ts (75_000). A `push`
# upstream has no framework-derived keepalive cadence, so the framework derives its staleThreshold as
# max(90s floor, this) = 90s — which keeps the connector watchdog (75s) + heartbeat-ACK check (~1
# heartbeat interval) LEADING the framework recycle (90s): the connector self-heals first; the
# framework net is the safety layer. MUST stay equal to WATCHDOG_WINDOW_MS.
upstream_self_window_ms: 75000
# M10-S9.9: simple-icons slug for the dashboard health-icon (monochrome brand glyph).
icon: discord
# Post-hatch concierge menu metadata — the honest pool-connector line the
# break-in menu renders. REQUIRED for pool membership (missing → cert failure).
menu:
  label: "Discord"
  description: "Chat with me on Discord — DMs, images, files."
  requirements:
    - "A Discord account and a free bot token from the Developer Portal (I'll walk you through it)"
  effort: "a few minutes, guided — we do it together right now"
  impact: 0
requires:
  env:
    - DISCORD_BOT_TOKEN
    # Framework-INJECTED (not user secrets) — the media handle endpoint the connector PUTs inbound
    # images to / GETs outbound image bytes from. Excluded from the onboarding secret-gate by the
    # framework (FINDING F15); set in process.env at boot and copied into the connector's scoped env.
    - MEDIA_ENDPOINT
    - MEDIA_TOKEN
style:
  dialect: discord-md
  images: true
  bold: true
  max_length: 2000
onboarding:
  - "Open the Discord Developer Portal (discord.com/developers/applications) → New Application → Bot → Reset Token, and copy the bot token."
  - "Invite the bot to any server you share (OAuth2 URL Generator → scope bot) so it can open DMs with you — MESSAGE_CONTENT is NOT required for the DM-first path."
  - "Paste the bot token into the secure token field — it is captured as DISCORD_BOT_TOKEN and never appears in chat."
---

# Discord (gateway WSS) transport connector

A self-contained `interface: transport` drop-in for the generic connector path. The framework
discovers, installs (`scripts/setup.sh` → folder-local `node_modules`), supervises and certifies this
folder; no named factory and no `config.yaml` are needed. Once certified and `.enabled`, the file
watcher + registry rescan auto-wire it live. This is M10 Discord — the DM-first path, now carrying
**text + images + files/documents + voice** (Phase 2 features 1, 2 & 3) PLUS the **polish tier**:
**message reactions (bidirectional), message edits (inbound: LIVE; outbound: substrate wired but
deliberately not exposed as an agent action — see Phase 3), and the typing indicator (outbound)**
(Phase 3). Nothing remains unintentionally deferred.

## Push model, not poll

Discord PUSHES events down a persistent connector-held gateway WSS (the first non-Baileys instance of
the framework's generalized push-gateway model). The connector IDENTIFYs with intents
`GUILDS(1<<0) | DIRECT_MESSAGES(1<<12) | DIRECT_MESSAGE_REACTIONS(1<<13)` (= 12289 — the DM-first set
plus DM reactions; **not** MESSAGE_CONTENT alone, which receives zero events). `DIRECT_MESSAGE_TYPING`
(1<<14) is intentionally NOT set — outbound typing is a REST POST, not a gateway intent. `DIRECT_MESSAGE_REACTIONS`
(a NON-privileged intent — no Developer Portal toggle) is what delivers `MESSAGE_REACTION_ADD` on DMs.
**Earlier build bug (on record):** `DIRECT_MESSAGE_REACTIONS` is `1<<13` (8192), NOT `1<<14` (16384 =
`DIRECT_MESSAGE_TYPING`); the off-by-one caused inbound DM reactions to silently never fire until
corrected (2026-07-03 fix — see DECISIONS.md). Message edits (`MESSAGE_UPDATE`) ride the SAME
`DIRECT_MESSAGES` intent as `MESSAGE_CREATE`, so no new intent is needed for edits. The connector subscribes to DM `MESSAGE_CREATE` / `MESSAGE_REACTION_ADD` /
`MESSAGE_UPDATE`, converts each event to a `WireInbound` (`kind: 'message' | 'reaction' | 'edit'`), and
appends it to a **connector-owned monotonic INTEGER-cursor queue** (`String(++seq)`, minted at push
time — reactions/edits enqueue through the SAME serialized ingest chain, so cursor-order ≡ arrival-order
holds for them too). It emits `notifications/transport/message_available` on append.
`transport/drain {sinceCursor}` returns queued records with cursor > sinceCursor and is NON-consuming
— the framework's durable cursor is the source of truth, so the retained bounded backlog is zero-loss
across a re-drain. The cursor is this OWN integer, **never** the gateway `seq` (it resets on IDENTIFY)
and **never** a raw message snowflake (> 2^53 → the durable guard's `Number()` coercion loses precision
→ the `cursor-numeric` cert probe FAILS it). cursorSeq + the dedup + the backlog persist across
restarts so a post-restart message can't collide below the framework cursor and vanish.

## What this connector carries (declared = proven)

- **Text** + **Discord Markdown** (`discord-md`) rendering. The framework renders each turn to the
  `discord-md` dialect (`**bold**` `*italic*` `~~strike~~` `` `code` `` ` ```fences``` ` `> quotes`,
  lists, `# ` headings; NO anchor-text links → degrade to `text (url)`; NO tables; 2000-char cap) and
  hands the connector the rendered wire text. The connector sends it VERBATIM via the Discord REST
  `POST /channels/{dm}/messages`.
- **Liveness** — the framework's heartbeat sweep pings the MCP server; the connector answers, and
  `transport/health` reports REAL gateway-socket liveness (see below).

## Addressing (DM-first)

`from` = the author snowflake (numeric, reported VERBATIM — the framework derives trust). A reply's
`to` is that same snowflake: the connector opens+caches the DM channel (`POST /users/@me/channels`)
and sends there (`from == to`, exactly like Telegram's `chat_id` — no core change for DM). Only DM
`MESSAGE_CREATE` (no `guild_id`) from a non-bot author (and not the bot itself) is ingested.

## Contract invariants encoded

- **Control-plane only:** speaks `transport/send`, `transport/drain`, `transport/health`,
  `transport/typing`, `transport/reaction`, `transport/edit`, and emits the
  `notifications/transport/message_available` wakeup + `notifications/transport/needs_attention`.
  Never bytes on the wire, never unprompted push. The three polish-tier tools are control-plane VERBS,
  not `style.*` render flags — advertising them in `listTools()` (so the generic plugin's
  `supportsTool()` gate opens) + implementing each handler is what makes them real; no style boolean is
  invented for them (never-lie).
- **Cursor durability (zero-loss):** the connector-owned integer cursor is monotonic and PERSISTED;
  `drain` is non-consuming and GC-reclaims only what the framework has acked; a stale cursor high-water
  self-re-anchors loudly; `inboundEnqueued` counts drainable pushes exactly (the framework's
  silent-drop net compares it against what it drains).
- **Upstream self-heal (`upstream: push`):** a heartbeat-ACK watchdog (a missed ACK → reconnect within
  ~1 interval, the FASTER primary), first-heartbeat jitter, RESUME(op6) on a recoverable close, and a
  frame-level zombie watchdog (`src/upstream.ts`, 75s < the framework's 90s net → the connector leads).
  On INVALID_SESSION (not resumable) → fresh IDENTIFY, logged honestly as **NOT** zero-loss across the
  gap. On a terminal close (4004 bad token / 4013/4014 disallowed intents) → emit
  `needs_attention{logged_out}` and idle — never a reconnect loop. Environmental drops back off
  (capped exponential) and retry FOREVER (Invariant 5). Gated on `SELFHEAL_WATCHDOG_OFF` so the
  framework net can be proven independently.
- **`transport/health` never-lies:** reports `upstreamConnected:true` ONLY when a live gateway socket
  genuinely exists (ws OPEN) with a recent frame; `false` the instant it does not, with a `reason`
  (`transient` / `network` / `logged_out`). A cert run reports a synthetic "up" (the CRIES-WOLF
  affordance — the connector IS up serving the harness; Discord exposes no mock-upstream control).
- **Roles are binding-derived:** `roles:` is the CEILING (`dedicated`). The effective role/trust is
  derived framework-side from the owner-binding; the connector never reads a role from frontmatter and
  never declares a singular `role:`.
- **Secrets never in transcript:** the token is captured via the declared `onboarding` flow as
  `DISCORD_BOT_TOKEN` (the secure widget — it is already in the secret store); the brain only ever
  sees `key DISCORD_BOT_TOKEN submitted`. The connector reads it from env and never echoes it.
- **session_held: false** — a bot-token connector holds no exportable login; the supervisor simply
  re-spawns it. No pairing (no QR / no pairing-code), no `transport/quiesce`, no
  `transport/onboarding_poll` (token-paste onboarding, like Telegram).

## Conformance / echo mode

When `DISCORD_BOT_TOKEN` is absent (the conformance harness injects no env) OR the explicit
`TRANSPORT_CONFORMANCE` cert env is set, the connector runs in ECHO/cert mode: it opens NO gateway
socket and `transport/send` echoes the message straight into the drain queue (`from := to`) with a
connector-owned numeric cursor. This certifies the wire shape, the render carry, the cursor contract,
and the never-lie health report WITHOUT a Discord token or any network — the offline cert stays
deterministic.

## Media, bidirectional (Phase 2 feature 1 — images; feature 2 — files/documents)

`style.images: true` is declared AND end-to-end wired + conformance-proven. The SAME by-handle path now
carries **non-image files/documents** too (feature 2) — one pipeline, not a parallel one. **Inbound:** any
DM attachment (image OR file) arrives as an `attachments[]` entry on `MESSAGE_CREATE` (`url`/`proxy_url`,
`content_type`, `filename`); the connector classifies it (`kind:'image'` for an image, else `kind:'file'`),
downloads the bytes from the Discord CDN and `PUT`s them to the localhost media endpoint (`/api/media`) with
the correct mime, attaching only the JSON `MediaHandle` to the drained `WireInbound.media` (§1 NO BYTES on
the MCP wire). **Filename fidelity:** a file's ORIGINAL filename is preserved on the wire handle via
`caption` (the framework `MediaHandle` has no dedicated filename field — the same contract telegram/whatsapp
carry a document's name by). An image caption rides the message `content`; a download/store failure degrades
HONESTLY to an `[image]` or `[attachment: filename]` placeholder in `content`, never a silent drop — for
images AND files. Inbound ingestion is SERIALIZED (a per-arrival chain) so the connector-owned cursor still
equals arrival order even when a media download awaits — the mint-at-push-time contract is preserved.
**Outbound:** a handle (image OR file) on `transport/send.media` has its bytes `GET`-fetched from the endpoint
and delivered as a multipart `POST /channels/{dm}/messages` (`payload_json` + `files[0]`), preserving the
original filename + content-type (a file handle's `caption` becomes the multipart file-part name verbatim
when it looks like a filename, else `file-<id>.<ext-from-mime>`); text is sent first, then each attachment,
and a media failure never throws past the turn (the text is already delivered). 429s honor `retry_after`.

MESSAGE_CONTENT intent is NOT required to receive attachment metadata on the bot's OWN DMs (the DM-first
intent set `4097` already carries it) — VERIFIED by the live round-trip. If an image ever arrives with an
empty `attachments[]`, that surfaces as the `[image]`-degrade (never a silent success).

## Voice, bidirectional (Phase 2 feature 3 — speech in + speech out)

Voice reuses the framework's existing STT/TTS pipeline (the SAME `audio-to-text` Deepgram + `text-to-audio`
Edge-TTS capabilities WhatsApp + the dashboard use) — the connector builds NO new STT/TTS. **Inbound:** a
Discord voice memo arrives as a DM `MESSAGE_CREATE` with a single audio attachment (`content_type` like
`audio/ogg`) and the message `flags` bit `IS_VOICE_MESSAGE = 1<<13` set; a plain audio-file attachment is
also handled. The connector classifies audio (voice-flag → true memo, else `audio/*` content-type / audio
extension), downloads the bytes from the Discord CDN and `PUT`s them to `/api/media` as a
`kind:'audio'` `MediaHandle` **with `ptt:true`** (+ `duration` from the memo's `duration_secs` when present).
`ptt:true` is exactly how WhatsApp marks a voice note (`isVoiceNote`): the framework then transcribes the
audio (Deepgram, `detect_language=true`), threads the DETECTED LANGUAGE through, and injects the
VOICE_MODE_HINT so the brain replies speech-friendly. The transcript reaches the brain as text — the user's
spoken words, understood. A download/store failure honest-degrades to a `[voice message]` placeholder in
`content`, never a silent drop. **No extra gateway intent** is needed: `flags` + the audio attachment ride
the DM frame under the existing `DIRECT_MESSAGES(1<<12)` intent, exactly like image/file metadata (no new
Portal step for the owner).

**Outbound:** when the input was voice, the framework synthesizes the reply (Edge-TTS → Ogg/Opus) and hands
the connector a `kind:'audio'` `ptt:true` handle on `transport/send.media`. The connector delivers it as a
**native Discord voice message** (the voice bubble with a scrubber) via Discord's 3-step cloud-attachment
flow: register the upload → `PUT` the Opus bytes to the signed URL → `POST` the message with
`flags: IS_VOICE_MESSAGE` + `duration_secs` (read REAL from the Ogg granule) + a `waveform` preview. **Honest
fidelity caveat:** the `waveform` bars are a SYNTHETIC envelope (Discord does not validate them against the
audio — only the visual preview is approximated; the audio + duration are genuine). If the native
voice-message envelope is rejected for any reason, the connector falls back to delivering the SAME Opus bytes
as a plain playable audio attachment (Discord renders it with an inline player) — declared honestly, never a
dropped reply. The native-voice-message send is wire-/cert-proven for the `audio` handle carry but is **not
yet live-proven end-to-end** (the offline cert cannot reach Discord); the live voice round-trip is the
owner-QA step.

**Known framework constraint — inbound file/image size (2026-07-02):** the shared `/api/media` PUT endpoint
inherits Fastify's DEFAULT body limit of **1 MiB**; it was never raised when the endpoint was authored
(M10-S4). Discord DMs deliver full-resolution, uncompressed originals, so a normal phone photo (often
2–8 MB) is rejected by the framework with **HTTP 413 `FST_ERR_CTP_BODY_TOO_LARGE`**. The connector's
download+PUT path is correct (identical to whatsapp/telegram) — the cap is on the framework side, and it
affects EVERY connector for any inbound blob > 1 MiB (WhatsApp/Telegram just haven't hit it because those
platforms pre-compress inbound media well under 1 MiB). This bites FILES harder than images: documents
routinely exceed 1 MiB, so a large inbound file will 413-reject until the raise deploys. The connector LOGS
the concrete failure — the kind (`image`/`file`), the reported byte size, and the 413 — instead of swallowing
it, then honest-degrades to `[image]` / `[attachment: filename]`. Full-fidelity inbound media requires the
framework to raise the `/api/media` body limit; the raise to 25 MiB is committed in the endpoint SOURCE but is
Architect-gated + pending a dashboard DEPLOY (out of scope for a connector job — see the diagnose job's
deliverable). Until that deploy ships live, sub-limit files/images already store and deliver correctly.

## Reactions, edits, typing (Phase 3 — the polish tier, bidirectional/outbound)

All three are control-plane VERBS (advertised tools + handlers), not `style.*` render flags.

- **Typing — outbound: LIVE, verified.** `transport/typing {to, action:'start'|'stop'}`. On `start`
  the connector `POST /channels/{dm}/typing` immediately, then REFRESHES every ~4s (Discord auto-expires
  the indicator ~10s after a POST) until `stop`. The per-DM refresh timer is cleared on `stop`, on ANY
  send to that DM (so a held `start` can't re-show "…is typing" after the reply lands), and on connector
  shutdown (SIGTERM). Best-effort — a typing failure is logged, never thrown, never wedges a turn.
  **Confirmed 2026-07-03:** user saw the "…is typing" bubble in the Discord client. No gateway intent is
  needed — outbound typing is a REST POST (`DIRECT_MESSAGE_TYPING` / 1<<14 is intentionally NOT set).
- **Reactions — bidirectional: LIVE, verified.** OUTBOUND: `transport/reaction {to, messageId, emoji}` →
  `PUT /channels/{dm}/messages/{messageId}/reactions/{urlencoded-emoji}/@me` (the unicode emoji is
  `encodeURIComponent`-encoded so multi-byte codepoints + variation/skin-tone sequences survive the
  path). INBOUND: a DM `MESSAGE_REACTION_ADD` (DM-only — no `guild_id`; the bot's OWN reactions,
  `d.user_id === botUserId`, are IGNORED so a reaction the bot places never loops back) drains as
  `kind:'reaction'` with `emoji` (`d.emoji.name`) + `targetMessageId` (`d.message_id`) + `from` = the
  reacting user. `content` carries a ≤80-char snippet of the target message text drawn from a bounded
  in-process LRU of recent sent text (WhatsApp parity — e.g. `reacted 👍 to "Hello there"`); when the
  target id predates the current process or was evicted from the LRU, `content` honest-degrades to the
  generic `reacted <emoji> to your message`. **LIVE, verified 2026-07-03:** a reaction to an in-memory
  message resolved to the quoted snippet; a reaction to a pre-bounce message correctly degraded.
- **Edits — inbound (user→bot): LIVE, verified.** A DM `MESSAGE_UPDATE` (DM-only; the bot's OWN edits
  are IGNORED; a partial update with no author or no `content` string — e.g. an embed/link-unfurl
  resolution — is skipped, that is a provider-internal update, not a dropped user message) drains as
  `kind:'edit'` with the NEW full `content` + `providerMessageId` = the edited message's id. **Confirmed
  via live edit round-trip 2026-07-03.**
- **Edits — outbound (bot→user): substrate WIRED + wire-certified, DELIBERATELY NOT EXPOSED as an agent
  action.** `transport/edit`, the adapter's `editMessage`, and all hooks remain intact and inert. This is
  an INTENTIONAL product decision (CTO/user, 2026-06-12, reaffirmed 2026-07-03): editing the bot's own
  past message would diverge the Discord display from the conversation transcript. This is NOT a bug or
  an incomplete feature — outbound edits are a deliberate known-limitation by user choice. If the decision
  is ever reversed, the transport/edit wire carry is already there; no code change is required, only
  exposure at the agent-action layer.

**Honest-degrade paths (never-lie).** (1) Reactions/edits target a message via an IN-MEMORY, bounded
recent-sent map (`messageId → DM channel`) populated on each successful send — a connector RESTART loses
pre-restart ids, so reactions/edits reliably target only RECENT (this-process) messages, matching the
adapter's documented behavior. (2) A reaction/edit to an id NOT in the map still attempts the REST call
keyed off `to` (Discord is the authority); an unknown/deleted/foreign id then fails GRACEFULLY — logged,
returns an honest `not-delivered`, never throws past the turn. (3) A send to a non-deliverable target
(a synthetic cert target) drops loudly and, for typing, also clears any refresh timer. (4) In a cert run
(no token / echo mode) the three verbs are benign no-ops (`ok`) — they are live-only and no conformance
probe drives them.

## Declared vs deferred (never-lie honesty)

Phase 2 feature 1 adds **images**; feature 2 adds **files/documents**; feature 3 adds **voice** (all above) —
carried by-handle over the framework media substrate, conformance-proven on the wire. The style manifest
declares `images: true` as the media render flag; **files** and **voice/audio** are media KINDS (not
render-style flags), proven by the honest-degrade + wire-round-trip cert probes, so nothing false is
declared. **Voice honesty:** inbound STT + outbound TTS reuse the framework's proven pipeline and the
`audio` handle carry certifies; the OUTBOUND native-voice-message envelope is not yet live-proven (the
offline cert cannot reach Discord) and falls back to a plain playable audio attachment if Discord rejects
it — so the outbound fidelity is declared as "native voice message preferred, audio-attachment fallback",
not asserted as always-native. The polish-tier verbs — **reactions and typing** (Phase 3) — are **LIVE**: they are control-plane VERBS
advertised as MCP tools (`transport/reaction`, `transport/typing`) with implemented bidirectional +
outbound paths (see the Phase 3 section above), NOT `style.*` render flags — advertising the tools + the
implemented handlers is what makes them real (a live human round-trip is the final QA of on-screen
rendering). **Edits:** inbound (`MESSAGE_UPDATE` → `kind:'edit'`) is LIVE and verified (2026-07-03).
Outbound edits (`transport/edit`) are substrate-wired and wire-certified but DELIBERATELY NOT EXPOSED as
an agent action — this is an INTENTIONAL user/CTO product decision (2026-06-12, reaffirmed 2026-07-03),
not a bug or deferred feature (see the Phase 3 section above). A voice note or file that can't be
downloaded/stored still degrades HONESTLY to a `[voice message]` / `[attachment: filename]` note in
`content`, never a silent drop; a reaction to an unknown message id fails gracefully (honest
`not-delivered`) without wedging the turn. Nothing remains unintentionally deferred. See `DECISIONS.md`.

## Onboarding

Create a bot in the Discord Developer Portal, invite it to a shared server (so DMs work; MESSAGE_CONTENT
is NOT required for the DM-first path), and paste the bot token into the secure token field when
prompted; it is stored as `DISCORD_BOT_TOKEN`. Then **DM the bot** to bind — the first DM you send
establishes the owner binding and proves the live text round-trip.
