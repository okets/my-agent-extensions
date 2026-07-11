---
name: telegram
provides: transport
interface: transport
backend: telegram-bot-api
template_version: 1
provenance: framework
roles:
  - dedicated
session_held: false
upstream: poll
# 2a.2 (cadence coupling): the connector's OWN long-poll stale window — POLL_STALE_WINDOW_MS in
# src/poll-health.ts (LONG_POLL_TIMEOUT_S 50s × 3 = 150_000). The framework derives its
# staleThreshold as max(90s floor, this) so it never recycles a healthy idle bot in the 90–150s band.
# MUST stay equal to POLL_STALE_WINDOW_MS (asserted by the connector poll-health test).
upstream_self_window_ms: 150000
# M10-S9.9: simple-icons slug for the dashboard health-icon (monochrome brand glyph).
icon: telegram
# Post-hatch concierge menu metadata — the honest pool-connector line the
# break-in menu renders. REQUIRED for pool membership (missing → cert failure).
menu:
  description: "Chat with me on Telegram — messages, photos, voice notes."
  requirements:
    - "A Telegram account and a free bot token from @BotFather (I'll walk you through it)"
  effort: "a few minutes, guided — we do it together right now"
  impact: 0
requires:
  env:
    - TELEGRAM_BOT_TOKEN
    - MEDIA_ENDPOINT
    - MEDIA_TOKEN
style:
  dialect: telegram-markdownv2
  images: true
  bold: true
  links: true
  max_length: 4096
lifecycle:
  - typing
onboarding:
  - "Open a chat with @BotFather in Telegram and send /newbot (or reuse an existing bot)."
  - "BotFather replies with an HTTP API token of the form 123456789:AA...; copy it."
  - "Paste the bot token into the secure token field — it is captured as TELEGRAM_BOT_TOKEN and never appears in chat."
---

# Telegram (Bot API) transport connector

A self-contained `interface: transport` drop-in for the generic connector path, and the worked-example
SEED template that the `transport-builder` agent copies + adapts. The framework discovers, installs
(`scripts/setup.sh` → folder-local `node_modules`), supervises and certifies this folder; no named
factory and no `config.yaml` are needed. Once certified and `.enabled`, the file watcher + registry
rescan discover it; a runtime-new folder goes LIVE via chat onboarding (`create_transport`) or at the next boot's auto-wire — discovery alone does not connect it.

## What this connector carries (declared = proven)

- **Text** + **Telegram MarkdownV2** rendering. The framework renders each turn to the
  `telegram-markdownv2` dialect (bold / italic / inline-code / anchor links — Telegram has no
  tables) with a hardened escaper, then hands the connector the rendered wire text. The connector
  transmits it with `parse_mode: MarkdownV2` and, on a markup-parse rejection, **resends the
  identical text as plain** so a markup error can never lose a message.
- **Typing indicator** (`transport/typing` → `sendChatAction: typing`, refreshed every 4 s, below
  Telegram's ~5 s auto-expire; a send to the chat auto-clears it client-side).
- **Liveness** — the framework's heartbeat sweep pings the MCP server; the connector answers.
- **Media, bidirectional** (`style.images: true`, M10-S6 PB7). **Inbound:** a photo (largest size),
  voice note (OGG/Opus → `ptt:true`, which drives the framework's voice-in→voice-out reply), audio
  file, or document is fetched via `getFile`, downloaded, and `PUT` to the localhost media endpoint;
  only the JSON `MediaHandle` rides the MCP wire (§1 NO BYTES). Captions ride the message `content`
  alongside the handle. **Outbound:** a handle on `transport/send.media` has its bytes `GET`-fetched
  from the endpoint and delivered as `sendPhoto` (image), `sendVoice` (audio — a playable PTT bubble),
  or `sendDocument` (file). **Honest degrade:** an unsupported kind (sticker/video/animation/…) or a
  media-endpoint outage surfaces the caption verbatim + a `[kind]` placeholder — never a silent drop,
  and the text path is never affected. The media endpoint config comes from the scoped env
  (`MEDIA_ENDPOINT`/`MEDIA_TOKEN`) with a localhost fallback (`<agentDir>/media/token`,
  `http://127.0.0.1:${PORT}`) so media survives a code-only respawn before a re-wire.

## Contract invariants encoded

- **Control-plane only:** speaks only `transport/send`, `transport/drain`, `transport/typing`, and
  emits the `notifications/transport/message_available` wakeup. Never bytes on the wire, never
  unprompted push.
- **Cursor durability (zero-loss):** the background `getUpdates` long-poll uses
  `offset = framework-drained cursor + 1` (never the highest received `update_id`), so Telegram
  retains every update until the framework has drained it — a connector crash loses nothing.
- **Backoff suppression:** transient network / 409 / rate-limit drops back off and retry; they are
  the supervisor's job, never a code fix. The connector never crashes the process on a transient.
- **Roles are binding-derived:** `roles:` is the CEILING (`dedicated`). The effective role/trust is
  derived framework-side from the owner-binding; the connector never reads a role from frontmatter
  and never declares a singular `role:`.
- **Secrets never in transcript:** the token is captured via the declared `onboarding` flow as
  `TELEGRAM_BOT_TOKEN`; the brain only ever sees `key TELEGRAM_BOT_TOKEN submitted`. The connector
  reads it from env and never echoes it.
- **session_held: false** — a bot-token connector holds no exportable login; the supervisor simply
  re-spawns it. No `transport/quiesce`.

## Onboarding

Message **@BotFather** in Telegram, send `/newbot` (or reuse an existing bot), and copy the HTTP API
token it gives you (form `123456789:AA...`). Paste it into the secure token field when prompted; it is
stored as `TELEGRAM_BOT_TOKEN` and never appears in chat. Nothing to edit in `.env` by hand.
