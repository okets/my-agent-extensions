---
name: whatsapp
provides: transport
interface: transport
backend: baileys
template_version: 1
provenance: framework
roles:
  - dedicated
  - personal
session_held: true
upstream: socket
# 2a.2 (cadence coupling): the connector's OWN socket-silence window — WATCHDOG_WINDOW_MS in
# src/watchdog.ts (60_000). The framework derives its staleThreshold as max(90s floor, this) = 90s,
# which keeps the connector watchdog (60s) LEADING the framework recycle (90s) — the connector
# self-heals first; the framework net is the safety layer.
upstream_self_window_ms: 60000
# M10-S9.9: simple-icons slug for the dashboard health-icon (monochrome brand glyph).
icon: whatsapp
# Post-hatch concierge menu metadata — the honest pool-connector line the
# break-in menu renders. REQUIRED for pool membership (missing → cert failure).
menu:
  label: "WhatsApp"
  description: "Chat with me on WhatsApp — messages, photos, voice notes."
  requirements:
    - "Your WhatsApp on your phone to scan a pairing code (like WhatsApp Web; no extra account)"
  effort: "a few minutes, guided — we do it together right now"
  impact: 0
requires:
  env:
    - WHATSAPP_PHONE_NUMBER
    - MEDIA_ENDPOINT
    - MEDIA_TOKEN
style:
  dialect: whatsapp
  images: true
  bold: true
  max_length: 4096
lifecycle:
  - typing
  - onMarkRead
onboarding:
  - "Provide the WhatsApp phone number (with country code) of the device you will link, e.g. +1 555 010 1234. It is captured as WHATSAPP_PHONE_NUMBER and never appears in chat."
  - "On launch the connector starts an interactive handshake and emits onboarding artifacts (transport/onboarding_poll): a scannable QR image shown in the dashboard AND an 8-character Linked-Devices pairing code (shown as ABCD-1234) relayed to you in chat."
  - "Link by EITHER path — scan the dashboard QR with WhatsApp → Linked Devices → Link a Device, OR on the phone holding that number open WhatsApp → Settings → Linked Devices → Link a Device → Link with phone number instead and enter the code before it expires (~60s)."
  - "Once you link, WhatsApp completes the handshake (status: linked), the QR/code artifacts are cleared, credentials persist locally, and the connector goes live."
---

# WhatsApp (Baileys) transport connector

A self-contained `interface: transport` drop-in for the generic connector path, refactored from the
in-process `channel-whatsapp` plugin into the out-of-process MCP connector shape. The framework
discovers, installs (`scripts/setup.sh` → folder-local `node_modules`), supervises and certifies
this folder; no named factory and no `config.yaml` are needed. Once certified and `.enabled`, the
file watcher + registry rescan discover it; a runtime-new folder goes LIVE via chat onboarding (`create_transport`) or at the next boot's auto-wire — discovery alone does not connect it. This is the reference WhatsApp connector.

## Push model, not poll

Unlike a poll-based connector (Telegram getUpdates), Baileys holds a PERSISTENT socket and PUSHES
events. The connector subscribes to `messages.upsert` (decoding text / image+caption / voice note /
document / reaction / edit inside it), converts each event to a `WireInbound`, and appends it to an
in-memory monotonic cursor queue (cursor = a strictly-increasing connector-owned integer). It emits
`notifications/transport/message_available` on append. `transport/drain {sinceCursor}` returns
queued records with cursor > sinceCursor and is NON-consuming — the framework's durable cursor is the
source of truth, so the retained bounded backlog is zero-loss across a re-drain.

## What this connector carries (declared = proven)

- **Text** + **WhatsApp dialect** rendering. The framework renders each turn to the `whatsapp`
  dialect (single-char markers `*bold*` `_italic_` `~strike~` `` `code` ``; raw URLs, no anchor text,
  no tables) and hands the connector the rendered wire text. The connector sends it VERBATIM via
  `sock.sendMessage(to, {text})`.
- **Link previews (outbound).** When a sent (or edited) text contains a URL, `src/link-preview.ts`
  builds a rich preview — title, description and a small JPEG thumbnail (192 px, via `sharp`) — with
  `link-preview-js` 3.2.0 and passes it to Baileys as `linkPreview`. 8 s page timeout, 4 s thumbnail
  budget, 10 s hard ceiling on the whole build. Degrades in steps: slow/failed thumbnail → card with
  title+description and no picture; page not done by the ceiling → plain text. Every fallback log line
  carries per-stage timings (`load`/`page`/`imageDns`/`imageDownload`/`sharp`) and names the stage still
  running. Private/loopback hosts are refused (SSRF guard, also on every redirect hop and the thumbnail
  URL); a send is never failed by a preview. Echo/cert mode never fetches.
- **Typing indicator** (`transport/typing` → `sendPresenceUpdate('composing'|'paused')`, refreshed
  every 4 s under WhatsApp's ~5 s auto-expire; a send to the chat clears it).
- **Liveness** — the framework's heartbeat sweep pings the MCP server; the connector answers.
- **Media, bidirectional** (`style.images: true`). **Inbound:** an image (with caption), voice note
  (OGG/Opus → `ptt:true`, which drives the framework's voice-in→voice-out reply), or document is
  downloaded via Baileys' `downloadMediaMessage` and `PUT` to the localhost media endpoint; only the
  JSON `MediaHandle` rides the MCP wire (§1 NO BYTES). Captions ride the message `content` alongside
  the handle. **Outbound:** a handle on `transport/send.media` has its bytes `GET`-fetched from the
  endpoint and delivered as an image, a voice note (`audio` + `ptt`), or a document. **Honest
  degrade:** an unsupported kind (sticker / video / animation) or a media-endpoint outage surfaces
  the caption verbatim + a bracketed placeholder (e.g. `[sticker]`, `[video — not viewable yet]`) —
  never a silent drop, and the text path is never affected.

## Onboarding — interactive handshake (QR + pairing code)

Onboarding is an **interactive handshake** (M10.5): the connector emits typed onboarding artifacts the
framework presents to the user, and serves the current snapshot via the `transport/onboarding_poll`
tool (the `notifications/transport/onboarding_artifact` notification is a wakeup; the poll is truth —
the same wakeup/poll split as `drain`). The connector reads `WHATSAPP_PHONE_NUMBER` (declared in
`requires.env`), opens the socket, and on first launch (no existing credentials):

- renders Baileys' scannable login **QR** string to a PNG, `PUT`s it to the localhost media endpoint,
  and emits an `image` artifact carrying only the JSON `MediaHandle` (§1 NO BYTES on the wire). The
  framework resolves the handle → a data-URL and shows it as a QR widget in the dashboard. The QR
  rotates (~20-60 s); each rotation replaces the artifact (latest-wins) and the framework re-presents.
- also calls `requestPairingCode`, emitting a `code` artifact (the 8-character `ABCD-1234` code). The
  framework relays it to the user in chat.
- emits a `status` artifact (`waiting` → `linked` on `connection: open`; `expired`/`error` on
  rotation/failure). On `linked` the QR/code artifacts are cleared (spent one-time linking credentials).

The user links by **either** path — scan the dashboard QR, or enter the relayed code on the phone
(Settings → Linked Devices → Link a Device → Link with phone number instead). Baileys completes the
link and credentials persist to the folder-local `./auth` dir (gitignored).

> **The QR is an EPHEMERAL linking credential.** It rides the controls broadcast (a transient WS
> frame, never persisted to the conversation transcript), is never logged, is superseded on rotation,
> and is cleared on `linked`. The pairing code is a one-time user-facing link code (not a secret).

## Contract invariants encoded

- **Control-plane only:** speaks `transport/send`, `transport/drain`, `transport/typing`,
  `transport/reaction`, `transport/edit`, `transport/mark_read`, `transport/quiesce`,
  `transport/onboarding_poll`, `transport/health` (B3 — reports REAL upstream-socket liveness from
  the live `sock` + raw `ws.readyState` + the B2 frame-activity clock; never `upstreamConnected:true`
  without a live socket), and emits
  the `notifications/transport/message_available` wakeup, `notifications/transport/needs_attention`, and
  `notifications/transport/onboarding_artifact` (the interactive-onboarding wakeup). Never bytes on the
  wire (the QR image rides a `MediaHandle`, never bytes), never unprompted push.
- **Cursor durability (zero-loss):** the in-memory cursor queue is connector-owned and monotonic;
  `drain` is non-consuming; the framework's durable cursor is the source of truth. The cursor is minted
  at push time inside `enqueue()` (contract Cursor rule 1), `drain` keeps a short tail of already-drained
  entries, and a stale cursor high-water re-anchors loudly (`src/cursor-queue.ts`). `transport/health`
  also carries the `inboundReceived` / `inboundEnqueued` counters the framework's silent-drop net reads.
- **Backoff suppression:** transient drops re-open the socket (515 restart is Baileys' in-band
  re-pair signal); a true crash loop is the supervisor breaker's job.
- **Roles are binding-derived:** `roles:` is the CEILING (`dedicated`, `personal`). The effective
  role/trust is derived framework-side from the owner-binding; the connector never reads a role from
  frontmatter and never declares a singular `role:`.
- **Secrets never in transcript:** the phone number is captured via the declared `onboarding` flow as
  `WHATSAPP_PHONE_NUMBER`; the brain only ever sees `key WHATSAPP_PHONE_NUMBER submitted`. The
  connector reads it from env and never echoes it. The pairing code + QR are surfaced to the user via
  onboarding artifacts (a one-time, user-facing link code / ephemeral QR, not a secret); the QR's bytes
  never reach the brain — only "a QR is displayed in the dashboard".
- **session_held: true** — Baileys holds a live multi-device login. `transport/quiesce {action:
  'export'}` flushes credentials to `./auth` and closes the socket so a code re-derive reconnects the
  SAME login (`exported`); with no live session it returns `not_held` and leaves the login intact.
- **Terminal logout → needs_attention:** on `DisconnectReason.loggedOut` the connector emits
  `notifications/transport/needs_attention {reason:"logged_out"}` and idles — it never reconnect-loops
  a revoked session.

## Conformance / echo mode

When `WHATSAPP_PHONE_NUMBER` is absent (the conformance harness injects no env), the connector runs
in ECHO mode: it opens NO Baileys socket and `transport/send` echoes the message (and any media
handles, by id) straight into the drain queue (`from := to`). This certifies the wire shape, the
render carry, and media-by-handle WITHOUT WhatsApp credentials or any network. The echo branch is
also taken per-send for any non-deliverable target (a non-JID like the harness's `probe`), so cert is
deterministic even if a real phone number leaks into the certifying env.

In ECHO mode `transport/onboarding_poll` returns a deterministic **synthetic handshake** so the
conformance `onboarding-handshake` probe certifies the interactive wire shape WITHOUT WhatsApp or a
media endpoint: the first poll yields a recognized artifact set (a JSON-only image handle — NO bytes,
NO endpoint call, exactly as the `images` media probe certifies; a code; `status: waiting`), and a
subsequent poll advances to `status: linked` (a simulated link). This proves the shape, not that
WhatsApp itself onboards — that is the live test's job.
