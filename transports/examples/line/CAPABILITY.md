---
name: line
provides: transport
interface: transport
backend: line-messaging-api
template_version: 1
provenance: agent
roles:
  - dedicated
session_held: false
# The framework's LONE webhook-PUSH backend. LINE has no polling and no provider-held socket: the LINE
# Platform PUSHES an HTTPS POST to the framework's public /hook/line DOOR. `webhook` is EXEMPT from the
# transport/health MANDATE (requiresHealthTool('webhook')===false) — there is no persistent socket to go
# silently half-open. This connector still advertises transport/health VOLUNTARILY to report inbound
# counters so the framework's silent-drop net + the cert temporal-contract cursor proof are both armed.
upstream: webhook
# ingress: webhook — this connector RECEIVES inbound through the framework's public webhook door
# (docs/design/webhook-ingress.md §3). The declaration is what makes the `/hook/line` route forward
# raw provider knocks to this connector's `transport/webhook` control tool; WITHOUT it the door 404s.
# The connector owns NO local listener and NO funnel — the public door is the `webhook-ingress`
# capability's job (§3.6: connectors NEVER touch host networking or bind public listeners).
ingress: webhook
# NO upstream_self_window_ms: a webhook connector has no keepalive cadence to heartbeat and, via the
# door abstraction, no local listener to be "down". transport/health is advertised VOLUNTARILY for the
# inbound counters only; lastUpstreamActivityMs is DELIBERATELY OMITTED so an idle webhook (normal
# between user messages) never false-trips a staleness recycle.
# M10-S9.9: simple-icons slug for the dashboard health-icon (monochrome brand glyph).
icon: line
# Post-hatch concierge menu metadata — the honest pool-connector line the
# break-in menu renders. REQUIRED for pool membership (missing → cert failure).
menu:
  label: "LINE"
  description: "Chat with me on LINE — messages, photos, voice."
  requirements:
    - "A LINE account and a free Official Account + Messaging API channel (I'll walk you through it)"
    - "A public webhook door on my machine (I set this up as part of the build if it's not there yet)"
  effort: "a few minutes, guided — plus the webhook door if it's the first webhook channel"
  impact: 0
requires:
  env:
    # The two pasted secrets the framework's secure onboarding captures. NEVER hardcoded — read from env.
    - LINE_CHANNEL_ACCESS_TOKEN  # long-lived; outbound push API Bearer auth
    - LINE_CHANNEL_SECRET        # webhook x-line-signature HMAC-SHA256 verification
    # The framework injects this from the active `webhook-ingress` capability (the live door). The
    # connector composes its provider-facing URL as ${WEBHOOK_PUBLIC_BASE}/line. No door → env unset →
    # the ordinary fail-closed gate keeps LINE honestly unavailable ("needs a public door").
    - WEBHOOK_PUBLIC_BASE        # public base of the framework's /hook door (e.g. https://<host>/hook)
    # Media endpoint (S4 by-handle substrate). Binary bytes never cross the MCP wire; only the
    # JSON MediaHandle does. The framework injects MEDIA_ENDPOINT + MEDIA_TOKEN; a localhost fallback
    # (token file at <agentDir>/media/token, endpoint http://127.0.0.1:PORT) covers a pure code-respawn.
    # If neither resolves, inbound media degrades honestly to placeholder text — text is unaffected.
    - MEDIA_ENDPOINT             # e.g. http://127.0.0.1:4321
    - MEDIA_TOKEN                # bearer token for /api/media PUT/GET
style:
  dialect: plaintext
  images: true         # Outbound images + audio deliverable end-to-end via the framework's presigned
                       # public-link lane: POST /api/media/:id/public-link → URL at /hook/_media/:id/:token
                       # (publicly reachable via Tailscale Funnel /hook → 443). See DECISIONS.md §8 (presigned public media-grant lane).
  max_length: 5000     # LINE text message hard limit (chars per message object)
onboarding:
  - "Create a LINE Official Account and a Messaging API channel in the LINE Developers Console (developers.line.biz), signing in with your existing personal LINE login as admin."
  - "Copy the long-lived Channel access token → paste into the secure field (captured as LINE_CHANNEL_ACCESS_TOKEN; it never appears in chat)."
  - "Copy the Channel secret → paste into the secure field (captured as LINE_CHANNEL_SECRET; it never appears in chat)."
  - "Set the channel's Webhook URL to the framework door: ${WEBHOOK_PUBLIC_BASE}/line (concretely https://<your-public-funnel-host>/hook/line) and turn ON 'Use webhook'."
  - "In the OA settings, DISABLE 'Auto-reply messages' and the greeting message so the bot's own responses are the only replies."
  - "Add the Official Account as a friend and send it a message to bind ownership (the first inbound establishes the owner binding)."
---

# LINE (Messaging API) transport connector

A self-contained `interface: transport` drop-in for the generic connector path. The framework
discovers, installs (`scripts/setup.sh` → folder-local `node_modules`), supervises and certifies this
folder; no named factory and no `config.yaml` are needed. Once certified and `.enabled`, the file
watcher + registry rescan auto-wire it live. This is the M10 LINE build — **text + typing indicator +
inbound media (increment 2) + outbound media LIVE (increment 3 activated 2026-07-07) + inbound sticker CDN image (increment 4 activated 2026-07-07)**:
inbound images, video, audio (→ STT transcript), files, locations handled; stickers now fetch the CDN
PNG for native brain vision + descriptive text fallback; outbound images and audio delivered natively
via the framework's presigned public-link lane; voice-reply text fallback fixed; reactions are not
exposed to bots by LINE; quick-reply/Flex/outbound-sticker are genuine framework gaps — see `DECISIONS.md` §13.

## Webhook-PUSH via the framework door — the framework's lone webhook shape

LINE has **no polling and no provider-held socket**. The LINE Platform PUSHES an HTTPS `POST` to a
public webhook URL whenever a user messages the Official Account. This connector declares
`ingress: webhook` and receives those knocks **through the framework's public door**
(`docs/design/webhook-ingress.md` §3): LINE Platform → `POST ${WEBHOOK_PUBLIC_BASE}/line` → the
framework's `/hook/:connector` route → the framework calls this connector's **`transport/webhook`**
MCP tool with `{method, path, headers, bodyB64}` (bodyB64 = the exact raw request bytes, base64). The
connector **owns no local listener and no funnel** — the public door is the `webhook-ingress`
capability's job (§3.6: a connector NEVER touches host networking or binds a public listener). There is
**no getUpdates and no provider-side redelivery queue**: if the connector process is down when LINE
knocks, the door answers 503 and LINE's own retry carries the gap; a knock that outlives all retries is
**LOST** — a known, accepted limitation (see *Known limitations* and `DECISIONS.md`), not a bug.

### Raw-body HMAC signature verification (the #1 LINE pitfall)

Every inbound `POST` carries an `x-line-signature` header = `base64(HMAC-SHA256(LINE_CHANNEL_SECRET,
RAW_REQUEST_BODY))`. The door forwards the **exact raw bytes** as `bodyB64`; the connector
base64-decodes them and computes the HMAC **on those raw bytes, BEFORE any JSON parse or
re-serialization** — a re-serialized body changes whitespace/key-order and breaks the signature. The
comparison is **constant-time** (`crypto.timingSafeEqual`); a mismatch returns **`{status:401}`**. On a
valid signature the connector parses, enqueues each message event synchronously (cursor-order ≡
push-order), and returns **`{status:200}`** fast. The empty-`events[]` **verification handshake** (the
console "Verify" button) returns **200** cleanly.

## What this connector carries (declared = proven)

- **Text** in + out. Inbound: each `message` event of `type:'text'` → a `WireInbound` with `from` =
  `source.userId` (VERBATIM), `content` = the text, `providerMessageId` = the LINE message id,
  `timestamp` = the event timestamp. Outbound: `transport/send` pushes via the LINE **push message** API
  (`POST /v2/bot/message/push`, `Bearer` = the access token, keyed on the stored `userId`).
- **Outbound images + audio (LIVE, increment 3 activated 2026-07-07):** `transport/send` accepts
  `media?: MediaHandle[]`. For image/audio handles, the connector calls `mintPublicLink(id)` which
  hits `POST /api/media/:id/public-link` (localhost + MEDIA_TOKEN). The framework returns a presigned
  URL at `GET /hook/_media/:assetId/:token` — served through the Tailscale Funnel (`/hook` is public).
  LINE's servers fetch `originalContentUrl` anonymously. Verified live 2026-07-07.
  - **Audio (voice reply, LIVE):** `mintPublicLink` → `pushLineNativeMedia` → LINE `{type:'audio'}` push.
    Degrade: if mint fails (expired/no token/no door), audio-only sends THROW so the framework's
    `deliverSynthesizedAudio` returns false → text fallback sent. Audio with text: text is sent, audio
    skipped silently. Never a bare `[voice message]` placeholder.
  - **Image (LIVE):** `mintPublicLink` → `pushLineNativeMedia` → LINE `{type:'image'}` push.
    Degrade: caption text if set, descriptive note if image-only.
  - **File:** LINE push API has NO generic file message type (verified against official docs). Sends a
    text note with the filename (`📎 filename`). Permanent limitation.
  - `style.images: true` — outbound images and audio are deliverable end-to-end (see `DECISIONS.md` §12).
- **Inbound images / video / files (WIRED, increment 2):** binary fetched from the LINE content
  endpoint (`GET https://api-data.line.me/v2/bot/message/{id}/content`, Bearer auth), staged to the
  framework's media endpoint (`PUT /api/media`, `MEDIA_ENDPOINT`/`MEDIA_TOKEN`), enqueued as a
  `WireInbound` with a `media: [MediaHandle]` array — the **exact same by-handle mechanism** as the
  Telegram, WhatsApp, and Discord sibling connectors. Bytes never cross the MCP wire.
- **Inbound audio / voice (WIRED, increment 2 → STT):** fetched and staged as above; handle carries
  `kind:'audio'`, `ptt:true` — the framework's inbound-media router routes it to the `audio-to-text`
  capability (same sibling voice-note path). The brain receives the transcript as text.
- **Inbound location (WIRED, increment 2):** rendered to a concise text string
  `[location: title — address (lat, lon)]` — no binary, pure text.
- **Stickers (INBOUND — increment 4):** LINE sticker events carry `packageId`, `stickerId`, and
  optional `keywords`. In live mode: fetches the sticker PNG from LINE's **public CDN**
  (`stickershop.line-scdn.net/stickershop/v1/sticker/{stickerId}/android/sticker.png` — no auth),
  stages it as a `kind:'image'` MediaHandle, and enqueues it alongside a descriptive content string
  (e.g. `[sticker: package 446, sticker 1988 — happy, thumbsup]`) — the framework routes the image
  via **native multimodal vision** so the brain can literally SEE the sticker. If the CDN fetch or
  staging fails (no `MEDIA_TOKEN`, 0-byte body, network error): honest degrade to the descriptive text
  only (still more informative than a bare `[sticker]`, never silent drop).
  Cert/echo mode: sync descriptive text degrade only (no network; conformance unaffected).
  **Outbound stickers: framework gap** — `transport/send` has no sticker concept and `MediaKind` has
  no `'sticker'` — cannot send stickers without packages/ changes (see DECISIONS.md §13).
- **Media download is fire-and-forget:** LINE webhook responses must be delivered in ~1 second. The
  webhook tool returns `{status:200}` immediately; media downloads happen asynchronously and enqueue
  via `message_available` when done. Cert/echo mode degrades synchronously (no network).
- `style.images: true` — outbound images and audio are **deliverable end-to-end** (increment 3
  activated 2026-07-07). The presigned public-link lane (`/hook/_media`) removes the tailnet-only
  restriction. Verified by live anonymous fetch test (see `DECISIONS.md` §12).

## Addressing (push, not reply-token)

Outbound is keyed on the stored `source.userId` via the **push** API — deliberately **NOT** the reply
token. Reply tokens expire in ~30s and the agent may take longer to respond; depending on them would
drop late replies. `from == to` (the same `userId` the connector received), exactly like Telegram's
`chat_id` — no core change.

## Contract invariants encoded

- **Control-plane only:** speaks `transport/send`, `transport/drain`, `transport/typing`,
  `transport/health`, and emits the `notifications/transport/message_available` wakeup +
  `notifications/transport/needs_attention` (`logged_out` on a 401 revoked token). Never bytes on
  the wire, never unprompted push.
- **Cursor durability (zero-loss within the process):** a connector-owned **monotonic INTEGER** cursor
  (`String(++seq)`), minted **at push time inside `enqueue()`** (the enqueue path is fully synchronous —
  no await between event arrival and the backlog push, so the mint-before-await collision class cannot
  arise). Persisted to `.state/cursor-state.json` (atomic temp+rename) so a restart cannot reset it and
  let a post-restart message collide below the framework's `sinceCursor`. `drain` is **non-consuming**;
  the only reclaim path is the bounded-backlog cap, which never evicts an un-acked entry (warns loudly
  if forced). A stale cursor high-water self-re-anchors loudly. `inboundEnqueued` counts drainable
  pushes exactly (the framework's silent-drop net + the cert temporal probe compare against it).
- **`transport/health` (voluntary — webhook is exempt from the mandate):** a webhook connector via the
  door has **no local listener and no provider socket** to be "down", so health simply reports the
  process is up (`upstreamConnected:true`) alongside the `inboundReceived`/`inboundEnqueued` counters
  that arm the framework's silent-drop net + the cert temporal-contract probe. `lastUpstreamActivityMs`
  is omitted so an idle webhook never reads as stale; no `upstream_self_window_ms` is declared.
- **Roles are binding-derived:** `roles:` is the CEILING (`dedicated`). The effective role/trust is
  derived framework-side from the owner-binding; the connector never reads a role from frontmatter and
  never declares a singular `role:`.
- **Secrets never in transcript:** `LINE_CHANNEL_ACCESS_TOKEN` + `LINE_CHANNEL_SECRET` are captured via
  the declared `onboarding` flow (the secure widget) and read from env; the brain only ever sees
  `key <KEY> submitted`. The connector never echoes a secret.
- **session_held: false** — a token-paste connector holds no exportable login; the supervisor simply
  re-spawns it. No pairing (no QR / no pairing-code), no `transport/quiesce`.

## Public ingress (the framework door — NOT owned by this connector)

This connector owns **no listener and no funnel**. The public address is the framework's `/hook/line`
door, exposed by the separate `webhook-ingress` capability (`ingress-tailscale-funnel`, already live).
The framework injects that capability's public base as `WEBHOOK_PUBLIC_BASE`; the provider-facing URL
the connector's inbound arrives on is:

```
${WEBHOOK_PUBLIC_BASE}/line   →   https://<your-public-funnel-host>/hook/line
```

If no `webhook-ingress` capability is installed, `WEBHOOK_PUBLIC_BASE` is unset and this connector is
honestly **unavailable — needs a public door** through the ordinary `requires.env` fail-closed gate
(the same UX as a missing API key). Provisioning the door is the ingress capability's job, never this
connector's (§3.6).

## Conformance / echo mode

When `LINE_CHANNEL_ACCESS_TOKEN`/`LINE_CHANNEL_SECRET` are absent (the conformance harness injects none)
OR the explicit `TRANSPORT_CONFORMANCE` cert env is set, the connector runs in ECHO/cert mode: it holds
**no** secret and `transport/send` echoes the message straight into the drain queue (`from := to`) with a
connector-owned numeric cursor. A `media` handle is DEGRADED to a bracketed placeholder (text-only
build). This certifies the wire shape, the render carry, the cursor temporal contract, and the
honest-degrade path WITHOUT secrets, a door, or any network — the offline cert stays deterministic
(`docs/design/webhook-ingress.md` §3.5: a webhook connector certifies fully offline; the live door is
needed only for the real provider handshake).

## Declared vs deferred (never-lie honesty)

Text + typing indicator + inbound media (image/video/audio→STT/file/location) are wired and
conformance-proven (increments 1–2). Outbound images and audio are now **live** (increment 3
activated 2026-07-07). Inbound stickers upgraded (increment 4 activated 2026-07-07): CDN image
staged as `kind:'image'` handle for native brain vision + descriptive text fallback.
`style.images:true` — outbound images/audio deliverable end-to-end. Voice-reply degrade bug fixed.

**Genuine framework gaps (not deferrable within this connector):** quick-reply buttons, Flex messages,
and outbound stickers all require `transport/send` extensions (new fields / new `MediaKind`) that live
under `packages/` — not in the capability folder. These are honest gaps, not deferrals — see
`DECISIONS.md` §13. **Reactions** are **NOT exposed to bots** by the LINE Messaging API — a genuine
provider limitation (see `DECISIONS.md`). **Outbound video** deferred: no `kind:'video'` in framework.
See `DECISIONS.md` §13 for the full updated table.

## Known limitations

- **Inbound-loss-while-down:** LINE has no provider-side redelivery queue. If the connector process is
  down when LINE POSTs, the framework door answers 503 and LINE retries; a knock that outlives all of
  LINE's retries is lost (LINE does not retry indefinitely). This is inherent to the webhook model, not
  silently swallowed.
- **Public ingress requires a live `webhook-ingress` capability** (the door). No door → the connector is
  honestly gated unavailable via the `WEBHOOK_PUBLIC_BASE` `requires.env` check (above).
- **Media downloads are async (fire-and-forget):** inbound media enqueues AFTER the download completes.
  In the rare case of a LINE webhook batch containing mixed text+media events, the text events (sync)
  may receive lower cursor numbers than media events (async). Within a single media batch, events are
  processed sequentially so intra-batch order is preserved.
- **External content providers:** if a LINE message has `contentProvider.type !== 'line'`, the binary
  is not LINE-hosted and cannot be fetched via the content API. These degrade to `[image]` etc.
- **LINE stickers (inbound, increment 4):** CDN PNG fetched and staged as `kind:'image'` handle for
  native vision. If CDN returns empty body (private/deleted sticker) or MEDIA_TOKEN is unavailable,
  degrades to descriptive text (e.g. `[sticker: package 446, sticker 1988 — happy]`). The CDN fetch
  is async (same fire-and-forget contract as other media). Animated stickers (`stickerResourceType:
  ANIMATION`) are served as static PNG from the CDN (the animated .gif path is not used).
- **Stickers (outbound), Quick reply, Flex messages:** genuine framework gaps — `transport/send` has
  no sticker/quick-reply/card concept; cannot be added without `packages/` changes. See §13.
- **Reactions unavailable** (§9) — provider limitation.

## Onboarding

Create a LINE Official Account + Messaging API channel, paste the **Channel access token** and **Channel
secret** into the secure fields, set the **Webhook URL** to the framework door **`${WEBHOOK_PUBLIC_BASE}/line`**
(concretely `https://<your-public-funnel-host>/hook/line`) and enable "Use webhook", disable the OA
auto-reply/greeting, then **add the OA as a friend and message it** to bind ownership.
