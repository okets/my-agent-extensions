# LINE connector — design decisions & provenance

> The design-rationale record for the LINE connector exemplar. LINE is the **webhook exception**
> among the connector family: every sibling (Telegram, WhatsApp, Discord) either polls or holds a
> provider socket, while LINE has no polling and no provider-held socket — the LINE Platform PUSHES
> an HTTPS POST to a public webhook URL. This record captures the design consequences of that shape:
> the framework-door ingress model, the presigned public media-grant lane, reply-token semantics,
> audio format decisions, and dependency decisions.

## What this is

The **canonical, Working-Agent-authored LINE connector exemplar** — a self-contained, certified
generic transport drop-in built under the transport-loop methodology (authored via the
`transport-brainstorming` / `transport-builder` toolchain and certified through the conformance
harness).

- `provenance: agent` (declared in `CAPABILITY.md` frontmatter) — authored by the Working Agent,
  not byte-copied from any prior plugin.
- **Backend:** LINE Messaging API (dedicated LINE Official Account + Messaging API channel — the
  bot model, analogous to the Discord/Telegram bots). No personal-account API path exists —
  dedicated OA only.
- This is the framework's **lone webhook-PUSH connector** (`upstream: webhook`).
- Certification gate: changes are re-certified through the conformance CLI
  (`npx my-agent-transport-kit certify`) — including the temporal-contract, honest-degrade, and
  render-carry probes — before the connector is considered green.

## 1. Dependency decisions — plain `fetch` + `node:crypto`, NOT `@line/bot-sdk`

**Decision: implement with Node built-ins, no `@line/bot-sdk`.** Justification:

- The entire LINE surface this connector needs is two things: (a) HMAC-SHA256 signature verify —
  one `crypto.createHmac('sha256', secret).update(rawBody).digest()` + `timingSafeEqual`; (b) the
  push API — one `fetch` POST to `/v2/bot/message/push`. Both are ~10 lines each. `@line/bot-sdk`
  would pull a dependency tree (with its own middleware/express assumptions) to wrap two trivial
  calls.
- **Keeps deps minimal** (`@modelcontextprotocol/sdk` + `tsx` only) — mirrors the Discord
  connector's "Node built-in `WebSocket` + `fetch`, no external gateway lib" decision, and
  sidesteps the MCP-stdout-purity hazard of a library that logs to stdout (a library writing to
  stdout corrupts the MCP wire).
- **Raw-body control is mandatory and easier without the SDK.** `@line/bot-sdk`'s middleware
  parses the body; getting the RAW bytes for the signature (the #1 LINE pitfall) is cleanest when
  the connector owns the handler and reads the raw Buffer itself before any parse.
- `@modelcontextprotocol/sdk` is pinned **exact** to the same version the sibling connectors
  certify against, with every used shape (`Server`, `setRequestHandler`, `notification`,
  `StdioServerTransport`, request schemas) verified against the *installed* type declarations —
  not docs, not older code.

## 2. Webhook ingress — the framework DOOR, not a connector-owned listener

The framework's webhook-ingress design forbids a connector from touching host networking or
binding its own public/loopback listener. (An early iteration of this connector ran its own local
HTTP listener and expected a public funnel pointed straight at it; it was reworked to the door
abstraction — the verify + parse logic moved, unchanged, into the `transport/webhook` MCP tool,
and the listener plus its funnel script were deleted.)

- **The connector declares `ingress: webhook`** in `CAPABILITY.md` frontmatter — the declaration
  that makes the framework's public `/hook/line` route forward to this connector (without it the
  door 404s).
- **Inbound arrives via the framework door**, not a connector-owned socket: LINE Platform →
  `POST ${WEBHOOK_PUBLIC_BASE}/line` → the framework's `/hook/:connector` route → the framework
  calls this connector's **`transport/webhook`** MCP tool with `{method, path, headers, bodyB64}`.
  `bodyB64` is the EXACT raw request bytes, base64 — the door never parses or re-serializes them
  (the exactness is the security property).
- **Raw-body HMAC-SHA256 signature verification runs on the decoded RAW bytes BEFORE any JSON
  parse.** The handler base64-decodes `bodyB64` into a `Buffer`, computes
  `base64(HMAC-SHA256(LINE_CHANNEL_SECRET, rawBody))`, and `crypto.timingSafeEqual`-compares
  (constant-time, length-checked) against the `x-line-signature` header. Mismatch →
  **`{status:401}`**. This is the #1 LINE pitfall — a re-serialized body breaks the signature —
  and the reason the connector never parses-then-reserializes before verifying.
- **Verify → parse → enqueue is SYNCHRONOUS** (no `setImmediate`): the door already answered the
  provider off the route, so the tool owns only verify+enqueue, and keeping it synchronous
  preserves cursor-order ≡ push-order. Returns **`{status:200}`** on success.
- **Empty `events[]` verification handshake** (the LINE console "Verify" button) → clean
  `{status:200}` no-op.
- **Dedup** by `message.id` (fallback `webhookEventId`) so a LINE redelivery isn't double-enqueued.
- **The connector owns NO local listener and NO funnel.** The public door is provided by a
  separate `webhook-ingress` capability (e.g. a Tailscale Funnel ingress capability), which is the
  ONLY thing that touches host networking. The framework injects that door's public base as
  `WEBHOOK_PUBLIC_BASE`; if no door capability is installed the env is unset and this connector is
  honestly **unavailable — needs a public door** through the ordinary `requires.env` fail-closed
  gate.
- **Webhook URL** (paste into LINE Developers Console → Messaging API → Webhook URL):
  `${WEBHOOK_PUBLIC_BASE}/line` — e.g. `https://<your-public-funnel-host>/hook/line`.

## 3. Cursor / queue design (the mint-at-push-time contract)

Follows `_CONTRACT.md`'s cursor rules precisely — guarding against the cursor-at-ingest reorder
class of bug that once caused a live incident on a sibling connector:

- **Connector-owned monotonic INTEGER cursor** (`String(++seq)`), start 0. NOT a provider id
  (LINE message ids would be fine numerically, but an own counter is reset-safe and gap-free).
- **Minted at push time inside `enqueue()`, one caller only** → cursor-order ≡ push-order. The
  enqueue path for LINE text is fully synchronous (no media download, no API lookup between
  webhook arrival and the backlog push), so the mint-before-await collision cannot even arise
  here — but the cursor is still minted inside `enqueue()` as the contract mandates, so the
  invariant holds if a future await is ever added.
- **Persisted** state (atomic temp+rename) — `cursorSeq`, `ackedHighWater`, `seen` dedup, and the
  un-drained backlog — so a restart cannot reset the counter and let a post-restart message
  collide below the framework's `sinceCursor`.
- **`drain` is non-consuming**; the only reclaim path is the bounded `MAX_BACKLOG` (2000) cap,
  which never evicts an entry ABOVE the framework-acked high-water (warns loudly if forced — real
  un-acked loss).
- **Stale cursor self-heal:** a drain with `sinceCursor` above the persisted `cursorSeq`
  re-anchors loudly.
- **`inboundEnqueued` counts drainable pushes exactly** (reported on `transport/health`). The
  conformance **temporal-contract probe** proves wakeup-per-message, counter identity, strictly
  ascending cursor order, and drain-respects-sinceCursor at cert time — the cursor-order ≡
  push-order check for webhook connectors.

## 4. Liveness / self-heal for a webhook (no listener, no socket to heartbeat)

`webhook` is EXEMPT from the `transport/health` mandate — there is no persistent provider socket
to go silently half-open, and (via the door abstraction) no local listener either. This connector
still **advertises `transport/health` VOLUNTARILY** for:

1. **Process liveness.** A webhook connector via the door has no listener/socket to be "down", so
   `upstreamConnected` is simply the process being alive to answer (always `true` here). A dead
   process can't answer at all → MCP ping/spawn failure is the framework's recycle trigger, not a
   health flag.
2. **Inbound counters** (`inboundReceived`/`inboundEnqueued`) → arms the framework's silent-drop
   net AND the cert temporal-contract probe (§3).

- **`lastUpstreamActivityMs` is DELIBERATELY OMITTED.** A webhook is idle by design between user
  messages; reporting a stale last-activity would false-trip the framework's staleness recycle.
  Omitting it means only a genuinely down door triggers recovery. No `upstream_self_window_ms` is
  declared (a webhook has no keepalive cadence).
- **401 on push = terminal:** a revoked/invalid access token emits
  `notifications/transport/needs_attention {reason:'logged_out'}` and does not retry-loop.

## 5. Outbound — push API, NOT reply tokens

`transport/send` → `POST ${LINE_API_BASE}/v2/bot/message/push` (`LINE_API_BASE` default
`https://api.line.me`, env-overridable for the harness/stub), `Authorization: Bearer <access
token>`, body `{to: <userId>, messages: [{type:'text', text}]}`.

**Push, keyed on the stored `userId` — NOT reply tokens**: reply tokens expire in ~30s and the
agent may take longer to respond; depending on them would drop late replies. Text is split to
LINE's **5000-char** limit and batched **≤5 message objects per push**, order preserved (part N+1
only after part N is accepted). 429 honors `Retry-After` with an ordering-preserving retry.

## 6. Inbound media — by-handle substrate + the webhook deadline

Inbound media reuses the **exact same by-handle media substrate** as the sibling connectors
(`MEDIA_ENDPOINT`/`MEDIA_TOKEN` env vars, `PUT /api/media`, JSON `MediaHandle` on the wire — bytes
never cross the MCP wire):

| Kind | Handling |
|------|----------|
| image | LINE content API → media store → `kind:'image'` handle (brain's native vision) |
| video | LINE content API → media store → `kind:'file'` handle + `[video]` content |
| audio | LINE content API → media store → `kind:'audio'`, `ptt:true` → framework routes to the `audio-to-text` capability (STT) — same flag the WhatsApp voice-note / Discord voice-memo paths use |
| file | LINE content API → media store → `kind:'file'` handle, filename preserved |
| location | rendered to text: `[location: title — address (lat, lon)]` |
| sticker | see §7 |

**Async design (webhook-specific constraint):** LINE webhook responses must be delivered within
~1 second. The `transport/webhook` tool returns `{status:200}` **immediately** (after signature
verify). Media events are queued in a **fire-and-forget async batch**, processed **sequentially**
within a batch to preserve intra-batch cursor order. Text/sticker/location events are enqueued
**synchronously** (before the 200 response), so they are always delivered even if the async batch
fails. Cert/echo mode runs the full sync path — no fire-and-forget, no network — so conformance
probes are unaffected.

## 7. Inbound stickers — public CDN image → vision + descriptive text

Rather than degrading every sticker to a bare `[sticker]` placeholder, the connector delivers a
two-part payload:

1. **CDN image fetch** — LINE sticker PNGs are publicly available (no auth) at
   `https://stickershop.line-scdn.net/stickershop/v1/sticker/{stickerId}/android/sticker.png`.
   The connector fetches the PNG, stages it as a `kind:'image'` handle, and enqueues the handle
   alongside descriptive text — the brain's multimodal vision receives the actual sticker image.
2. **Descriptive content text** (always present, even on CDN failure):
   `[sticker: package 446, sticker 1988 — happy, thumbsup]` — semantic context without vision,
   and the honest-degrade baseline when the CDN returns 0 bytes (private/deleted sticker) or the
   media substrate is unavailable.

Animated stickers (`stickerResourceType: ANIMATION`) are served as static PNG from the CDN — the
static frame is sufficient for brain vision. All degrade cases (no media token, 0-byte body,
network error, missing `stickerId`) fall back to descriptive text only — never a silent drop.

## 8. Outbound media — the presigned public media-grant lane

LINE's outbound media mechanism differs fundamentally from the siblings: Telegram/WhatsApp/Discord
accept **byte uploads** (multipart form or binary body), while LINE fetches media **server-side**
via a public HTTPS `originalContentUrl`. The handle-ID-on-the-wire contract is IDENTICAL to the
siblings (the framework passes `MediaHandle[]` on `transport/send`; bytes never cross the MCP
wire) — only the delivery step differs, because LINE's architecture demands it.

The framework provides a **presigned public media-grant** mechanism for exactly this shape:

**Mint a presigned public URL (private/localhost side):**

```
POST /api/media/:id/public-link
  Headers: x-media-token: <MEDIA_TOKEN>
  Response: { url: "https://<host>/hook/_media/<asset-id>/<token>", expiresAt: "..." }
```

**Serve the asset (public side, no auth):**

```
GET /hook/_media/:assetId/:token
  Response: 200, content-type: <image/*|audio/*|video/*>, body: bytes
```

**Key design properties:**

- **No new public surface** — the `_media` path is a reserved sub-namespace of `/hook`, which is
  already the public door (e.g. a Tailscale Funnel mapping `/hook` to the local dashboard port).
  The static `_media` segment outranks `:connector` in the router, so it never forwards to a
  connector.
- **Token is a 128-bit random hex string** — not guessable — with a **1-hour TTL**.
- **Grants survive a process restart** (persisted to a grants file under the agent's data dir).
- **MIME allowlist:** only `image/*` (except SVG), `audio/*`, `video/*` are grantable —
  active-content types (HTML, scripts) can never be served publicly.

The connector's send path: mint a presigned link for the handle → push
`{type:'image'|'audio', originalContentUrl}` via the LINE push API → on any failure (no media
token, mint failure, push failure) fall through to honest degrade. This replaced an earlier
static-public-base env-var approach; the presigned lane needs only `MEDIA_ENDPOINT` +
`MEDIA_TOKEN`, already in `requires.env`.

## 9. Outbound audio — m4a/AAC transcode + real duration (the blank-audio lesson)

Two format decisions, both learned from a live failure where voice replies arrived as a ~1-minute
blank audio note:

**Format: LINE requires mp3 or m4a.** Per the official LINE Messaging API spec,
`originalContentUrl` for audio messages must be **mp3 or m4a**. TTS capabilities commonly emit
OGG/Opus — LINE downloads the file, cannot decode it, and renders a blank note. The connector
therefore transcodes before minting the public link: fetch the staged bytes → `ffmpeg -c:a aac -f
mp4` (OGG/Opus → m4a/AAC) → re-stage as `audio/mp4` → mint the presigned link on the m4a asset.

**Duration: never default it.** LINE's audio message takes a `duration` field; a hardcoded
fallback (e.g. 60000ms — LINE's own documentation example value) produces a visibly wrong
~1-minute clip. The transcode step runs `ffprobe` to measure the REAL clip duration and passes it
explicitly.

**Honest degradation:** if any step fails (ffmpeg not on PATH, transcode error, staging failure,
no public link), the audio-only send **throws** → the framework's synthesized-audio delivery
returns `false` → the framework sends the text answer as plain text. Never a broken/blank audio
note, never silence.

## 10. Voice-reply honesty — throw, don't placeholder

When the brain produces a TTS voice reply, the framework sends `{to, content: "", media:
[audioHandle]}` (empty content — the voice note IS the reply). An early version degraded an
unsendable audio handle to a `[voice message]` placeholder and reported `'sent'` — so the
framework's text fallback never ran and the actual answer was lost: a never-lie violation.

The corrected design: for audio handles in live mode with empty `content`, **THROW** instead of
degrading — the framework catches it, sees the send failed, and delivers the actual answer as a
text message. When `content` is non-empty (text + audio together): send the text, skip the audio
silently — no `[voice message]` noise appended.

## 11. Typing / loading indicator

LINE's loading animation is wired as the `transport/typing` indicator, mirroring the exact sibling
contract (tool `transport/typing`, schema `{to, action:'start'|'stop'}`):

- `action:'start'` → best-effort `POST /v2/bot/chat/loading/start` with `{chatId, loadingSeconds:
  5}` — **1:1 chats only** (userId `U…`); groups (`C…`) and rooms (`R…`) degrade silently (the
  LINE API rejects them).
- `action:'stop'` is a no-op — the animation auto-clears when the next message is sent or the
  timer elapses. No timer/interval needed; LINE's own 5-second window + send-clears-it behavior is
  sufficient.
- In cert/echo runs → silent no-op. Errors go to stderr only — stdout stays pure for the MCP wire.

## 12. Cert / echo mode (no-contention gate)

Cert-vs-live is EXPLICIT (`isCertRun()` = secrets absent OR the conformance env flag OR a latched
loopback), NEVER inferred from the target shape. In cert mode the connector binds **no** webhook
door and `transport/send` echoes into the drain queue (`from := to`); media handles are carried
JSON-only with ids INTACT (matching the sibling pattern — this is what the `render:images`
conformance probe tests). This certifies the wire shape, render carry, cursor temporal contract,
and honest-degrade WITHOUT secrets or network. A live send to a non-deliverable target
(`/^[UCR][0-9a-fA-F]{32}$/`) drops loudly (never a fake echo).

## 13. Parity ledger — shipped, degraded, framework gaps

Checked against the official LINE Messaging API reference (`developers.line.biz`):

| Capability | LINE supports? | This connector | Classification |
|---|---|---|---|
| Text in/out | Yes | Wired + certified | shipped |
| Push message (userId-keyed) | Yes | Wired | shipped |
| Loading (typing) indicator | Yes | Wired — 1:1 only, 5s | shipped |
| Inbound images | Yes | Wired → `kind:'image'` handle (vision) | shipped |
| Inbound video | Yes | Wired → staged as `kind:'file'` | shipped |
| Inbound audio / voice | Yes | Wired → `kind:'audio'`, `ptt:true` → STT | shipped |
| Inbound files | Yes | Wired → `kind:'file'`, filename preserved | shipped |
| Inbound location | Yes | Wired → rendered text | shipped |
| Inbound stickers | Yes | Wired — CDN PNG → vision + descriptive text | shipped |
| Outbound images | Yes | Wired — presigned public-link lane | shipped |
| Outbound audio (voice reply) | Yes | Wired — transcode to m4a + presigned link | shipped |
| Outbound files | **NO** generic file type in push API | `📎 filename` text note | honest degrade (permanent) |
| Outbound video | Yes (needs preview image URL) | Not wired — no `kind:'video'` in framework `MediaKind` | deferred |
| Outbound stickers | Yes | Not expressible — see framework gaps | framework gap |
| Quick reply | Yes | Not expressible — see framework gaps | framework gap |
| Flex messages | Yes | Not expressible — see framework gaps | framework gap |
| Reactions (send or inbound) | **NO** — no reaction API for bots | n/a | genuine provider limitation |

**Framework gaps (documented honestly, never faked):** the framework's outbound wire contract is
`{to: string, content: string, media?: MediaHandle[]}` with `MediaKind = 'image'|'audio'|'file'`,
and the render pipeline produces channel text from markdown — no button, card, or carousel output.

- **Outbound stickers** need `{type:'sticker', packageId, stickerId}` — no framework concept of
  sticker-send intent exists.
- **Quick reply** needs a `quickReply`/`suggestedActions` field on the send contract plus a render
  hook. UI-sugar gap, not data-loss — the text answer always arrives.
- **Flex messages** (bubbles, carousels, buttons) need a structured card abstraction. This is the
  same gap class as WhatsApp's list messages and Discord's embeds — none of these rich-layout
  features can be expressed through the current `{to, content, media?}` wire shape.

Where LINE has a feature the framework abstraction doesn't reach, the connector degrades honestly
and documents it as a genuine framework gap — it never fakes a capability.

## 14. Known limitations (proven)

- **Inbound-loss-while-down (inherent to webhook):** LINE has NO provider-side redelivery queue.
  If the connector process (or the door) is down when LINE POSTs, the framework door answers 503
  and LINE retries; a knock that outlives all of LINE's retries is lost — LINE does not retry
  indefinitely. This is inherent to the webhook model (the `_backends.md` LINE row: "LOSES inbound
  when door down (no provider queue)"), NOT a bug. The connector owns no listener; door liveness
  is the `webhook-ingress` capability's health, and 503-on-down is the framework route's job.
- **Public ingress needs a live `webhook-ingress` capability** (the door). No door →
  `WEBHOOK_PUBLIC_BASE` unset → the connector is honestly gated unavailable via the ordinary
  `requires.env` check (§2).
- **Reactions unavailable** (§13) — provider limitation.

## 15. Deferred (the extensions offer)

- **Outbound video** — requires `kind:'video'` in the framework's `MediaKind` (not present
  today); LINE video needs both `originalContentUrl` and a separate `previewImageUrl` (thumbnail).
  Deferred until video-out framework support exists.
- **Outbound stickers / quick reply / Flex messages** — blocked on the framework contract
  extensions described in §13; the connector side (building LINE JSON) is straightforward once
  the framework exposes the intent.
