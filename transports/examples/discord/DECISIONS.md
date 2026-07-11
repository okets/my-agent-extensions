# Discord connector — design decisions (DM-first)

> The design record for the Discord DM connector: why it is shaped the way it is — its
> gateway backend choice, its cursor/event model, its media handling, its self-heal
> design, and its certification discipline. This record exists so future builders (of
> this connector or of any analogous push-gateway connector) can see the reasoning,
> not just the resulting code.

## Provenance & build discipline

Authored by the Working Agent during its connector-authoring loop, under
**build-not-copy** discipline: the connector was authored from the backend-agnostic
contract (`skills/transport-templates/_CONTRACT.md` + `_backends.md`) and the
framework's generic transport substrate — not ported from another connector. The
Telegram connector was *mined* for the control-plane shape and DM `chat_id`-style
addressing (where `from == to`); the WhatsApp connector was *mined* for the
push→cursor-queue pattern and folder layout. **No WhatsApp QR/pairing/`session_held`/
`quiesce` machinery was copied** — Discord is token-auth, `session_held: false`, no
pairing. Each decision below was a deliberate, tested change against the contract,
re-certified through the conformance CLI (`npx my-agent-transport-kit certify`)
before being considered green.

## D1 — Backend = push-gateway over Node's built-in global `WebSocket` (no external ws dep)

Discord's gateway is a persistent connector-held WSS that PUSHES events (`upstream: push`). Node 24
(the runtime) ships a stable global `WebSocket` (undici), so the connector speaks the gateway with it
and REST with `fetch` — the ONLY runtime dependency is `@modelcontextprotocol/sdk` (+ `tsx`). This
sidesteps two contract hazards at once: (a) discord.js and many gateway libs write debug output to
**stdout**, which IS the MCP JSON-RPC wire — a single stray byte corrupts the protocol (a failure
class proven live during earlier connector work); (b) a `legacy`-dist-tag protocol lib is the top
cause of a live 400/401 the offline cert can't see. Zero gateway deps ⇒ neither can bite. All
connector logging is `console.error` → stderr.

## D2 — Cursor = connector-owned monotonic INTEGER, minted at push time, persisted

The cursor is `String(++cursorSeq)`, minted **inside `enqueue()` at push time** (never at
ingest-start — the mint-before-await collision class that once lost a live message on the WhatsApp
connector). It is explicitly NOT the gateway `seq` (resets to null on a fresh IDENTIFY → would
collide below the framework's `sinceCursor`) and NOT a raw message snowflake (18-20 digits > 2^53 →
the framework durable guard's `Number()` coercion loses precision → distinct ids compare EQUAL → the
`cursor-numeric` cert probe FAILS). `cursorSeq` + the `seen` dedup + the un-drained backlog are
persisted (atomic temp+rename) to `.state/cursor-state.json` across restarts so a post-restart message
cannot be born below the framework cursor. `drain` is non-consuming (framework durable cursor is truth)
and re-anchors loudly if `sinceCursor` ever runs ahead of the persisted high-water.

## D3 — Addressing: `from` = author snowflake, reply `to` = same snowflake (DM, from == to)

Only DM `MESSAGE_CREATE` (no `guild_id`) from a non-bot author (and not the bot itself) is ingested.
`from` is the raw author snowflake, reported VERBATIM — the framework derives trust from the
owner-binding, the connector only reports. A reply opens+caches the DM channel
(`POST /users/@me/channels`) and sends there; `from == to` exactly like Telegram's `chat_id`, so no
core change was needed for the DM path (the substrate's `deriveReplyTo` DM guard covers it).

## D4 — IDENTIFY intents = GUILDS(1<<0) | DIRECT_MESSAGES(1<<12) = 4097

The DM-first minimal set. NOT MESSAGE_CONTENT alone (that receives zero events); MESSAGE_CONTENT is
NOT required to read the content of DMs sent to the bot. GUILDS is included so the shared-guild
membership that makes DMs possible is visible.

## D5 — session_held:false, token auth, no onboarding_poll tool

A bot token is a pasted secret already in the store (`DISCORD_BOT_TOKEN`). There is no live login to
export, so `session_held:false`, no `transport/quiesce`, and no `transport/onboarding_poll` — onboarding
is declared as token-paste steps (like Telegram) and captured by the framework's secure widget. The
cert harness only FAILs a missing `onboarding_poll` for a `session_held !== false` connector, so this
is clean; the connector reads plain **CERTIFIED** (no live-pairing tag).

## D6 — Self-heal: heartbeat-ACK primary + frame watchdog backstop + honest INVALID_SESSION

`upstream: push` mandates `transport/health` (structural cert FAIL without it) + a connector-side
watchdog. Two layers: (1) a **heartbeat-ACK check** — on each heartbeat, if the prior beat was never
ACKed, the connection is a zombie → force-reconnect within ~1 interval (the FASTER Discord-idiomatic
primary); (2) a **frame-level zombie watchdog** (`src/upstream.ts`, `WATCHDOG_WINDOW_MS = 75_000`) as
the coarser backstop. Both are gated on `SELFHEAL_WATCHDOG_OFF` so the framework's own liveness net
can be proven alone; the toggle NEVER changes what `transport/health` reports. First-heartbeat
**jitter** per the Discord mandate. RESUME(op6) on a recoverable close; on INVALID_SESSION
(not-resumable) → drop the session for a fresh IDENTIFY and log the gap **honestly as NOT zero-loss**
(never claim zero-loss across it). Terminal closes (4004 bad token, 4013/4014 disallowed intents) →
`needs_attention{logged_out}` + idle, never a reconnect loop. Everything else → capped-exponential
backoff, retrying environmental failures forever per the contract invariant. 429 REST sends honor
`retry_after` and retry the SAME send before any later one (order preserved — the framework
`OutboundPacer` is not provider-bucket-aware).

## D7 — upstream_self_window_ms = 75_000 (the connector leads the framework net)

`WATCHDOG_WINDOW_MS` (75s) is exported to the manifest as `upstream_self_window_ms`. A `push` upstream
has no framework-derived keepalive cadence, so the framework's `staleThreshold = max(90s floor, 75s) =
90s`. 75s < 90s ⇒ the connector's frame watchdog LEADS the framework recycle; the heartbeat-ACK check
leads it further. 75s is comfortably above the Discord gateway heartbeat interval (~41.25s) so a single
healthy heartbeat-ACK cycle never false-fires (≈1.8 cycles of tolerance) — the WhatsApp precedent
(60s window under a 90s floor) applied to Discord's longer heartbeat.

## D8 — Never-lie style profile: TEXT declared first, media/voice/reactions DEFERRED (`false` by omission)

The initial scope was DM TEXT. `style` declared only `dialect: discord-md`, `bold: true`,
`max_length: 2000` — all conformance-PROVEN by the render prover against the mock. `style.images`
(and any voice/reaction/typing kind) was **NOT declared**: the send/drain paths did not carry media yet,
and the `discord-md` dialect has no anchor-text links or tables (so `links`/`tables` are correctly
absent too). Declaring an unwired kind is a lie the never-lie system may eventually credit, so it stays
off until a later pass wires it end-to-end (inbound handle upload + outbound handle fetch, reactions,
typing) — at which point the flag flips and the decision is superseded (as D10 and D12–D15 later did
for images, files, and voice). This is the DOCUMENTED `false`-by-omission the contract requires (no
same-backend connector existed to match down to — this was the first Discord connector).

## D9 — Cert determinism: explicit cert signal, echo loopback, no target-shape inference

Cert-vs-live is decided by an EXPLICIT signal only (`isCertRun()` = ECHO_MODE (no token) ||
`TRANSPORT_CONFORMANCE` || a latched `conformanceSeen` from a loopback send), NEVER inferred from the
target shape (a live-poisoning inference the WhatsApp connector once carried and had removed). In a
cert run the connector opens NO gateway socket and `transport/send` echoes into the drain queue with a
numeric cursor; the live gateway connect is deferred ~1.5s after the MCP handshake and disarmed by the
harness's immediate loopback send, so a re-cert over a token-carrying env never opens a real gateway.

## D10 — Images by-handle, matching the telegram/whatsapp media contract (NO bytes on the MCP wire)

Photos ride the framework's existing out-of-band media substrate, NOT a new wire carry. **Inbound:** a DM
image arrives as an `attachments[]` entry on `MESSAGE_CREATE` (`url`/`proxy_url`, `content_type`,
`filename`). The connector downloads the CDN bytes and `PUT`s them to the localhost media endpoint
(`MEDIA_ENDPOINT`/`MEDIA_TOKEN`, framework-injected and excluded from the onboarding secret-gate),
receiving a JSON `MediaHandle`; only that handle rides `WireInbound.media` (the contract's
no-bytes-on-the-wire rule). **Outbound:** a handle on `transport/send.media` has its bytes
`GET`-fetched and delivered as a multipart `POST /channels/{dm}/messages` (`payload_json` + `files[0]`),
text first then each image. `style.images` flipped `true` ONLY after this certified (never-lie): the
offline cert's carry-fidelity `images` probe passes (handle id carried intact in the echo loopback — no
re-staging, no bytes). This supersedes the images half of D8. Files and voice were wired by later
decisions (D12, D14).

## D11 — Serialized inbound ingestion preserves cursor-order ≡ arrival-order across an image-download await

An image `MESSAGE_CREATE` must download bytes (async) before `enqueue`; a later text frame must not
overtake it and mint a lower cursor (a reorder the framework would drain out of order). Rather than mint
the cursor before the await (the mint-before-await collision class D2 closed — it lost a live message
on the WhatsApp connector), EVERY inbound enqueue is chained through a single serial promise tail
(`ingestChain`). The cursor is STILL minted inside `enqueue()` at push time; the chain only guarantees
the enqueues run in arrival order. Telegram gets this for free from its sequential poll loop; Discord's
gateway delivers frames concurrently, so the chain is the explicit equivalent. Honest-degrade: an image
that can't be downloaded/stored becomes a `[image]` placeholder in `content` (never a silent empty); a
non-image attachment (file/video, not yet wired at this stage) becomes an `[attachment: filename]` note
(never a silent drop). MESSAGE_CONTENT intent is NOT required for attachment metadata on the bot's own
DMs — verified by the live gateway staying READY on the `4097` intent set.

## D12 — Non-image files ride the SAME image-by-handle path (one pipeline, not a parallel one)

File/document support GENERALIZES the certified image path to all attachment kinds instead of building a
second pipeline. **Inbound:** `downloadAttachmentToHandle` no longer filters to images — it classifies each
`attachments[]` entry (`isImageAttachment` → `kind:'image'`, else `kind:'file'`), downloads the Discord CDN
bytes, `PUT`s them to `/api/media` with the correct mime (`content_type` verbatim, params stripped; a file
with no `content_type` → `application/octet-stream`, mirroring telegram's document default), and returns a
`MediaHandle`. The `MESSAGE_CREATE` handler loops over EVERY attachment: a success yields a handle, a failure
yields an honest placeholder (`[image]` / `[attachment: filename]`) — never a silent drop, for images AND
files. **Outbound:** `sendMediaHandle` carries `kind:'file'` alongside `kind:'image'` over the same
multipart `POST /channels/{dm}/messages` (`payload_json` + `files[0]`), preserving the original filename +
content-type. **Filename fidelity:** the framework `MediaHandle` has NO dedicated `filename` field, so — exactly
as telegram/whatsapp carry a document's name — the ORIGINAL filename rides `caption` (set client-side after the
inbound PUT, NOT as an HTTP header where unicode/newlines would break; and NOT via the media endpoint's
`x-media-caption`). Outbound, a `file` handle's `caption` (when it looks like a filename) is used VERBATIM as
the multipart file-part name (basename only, control chars/separators scrubbed, ≤255) so a PDF leaves as
`report.pdf`; a caption-less file synthesizes `file-<id>.<ext-from-mime>`. For a `file` the caption is the
filename and is NOT duplicated as message text (it shows on Discord's file bubble); for an `image` the caption
is a real user caption and still rides message content — so the certified image path is byte-for-byte
unchanged. `style` is UNCHANGED: `images: true` stays the only media render flag — "files" is a media KIND
carried by-handle (proven by the honest-degrade + wire-round-trip probes), NOT a render-style flag, so nothing
new is declared and never-lie holds. Supersedes the "non-image attachment → `[attachment: filename]` note only"
half of D11 (that degrade is now the FAILURE path; the success path carries the file by-handle).

## D13 — Files above the framework media endpoint's body limit 413-degrade HONESTLY, with concrete logging

The shared framework media endpoint enforces a request-body limit (Fastify's default is 1 MiB unless the
framework raises it). Discord DMs carry uncompressed full-resolution originals (2–8 MB typical), while
WhatsApp/Telegram pre-compress inbound media well under that size — so Discord is the connector that
surfaces the limit. Raising it is a change to SHARED framework substrate that benefits all connectors,
so it belongs to the framework, never merged unilaterally from a connector job. Connector-side, the
design commitment is diagnosability: `downloadAttachmentToHandle` shares one concrete failure-logging
path for all kinds — on a store failure it logs the kind (`image`/`file`), the label, the
Discord-reported `att.size` in bytes, and the concrete error (e.g. `media PUT failed: 413`) — and then
honest-degrades to the `[image]` / `[attachment: filename]` placeholder. A media degrade is instantly
diagnosable from the connector's own stderr, never a silent mystery; sub-limit files store + deliver
correctly. (The connector's download+PUT path is identical in shape to whatsapp's `mediaPut` — a 413
here indicts the endpoint limit, not the connector.)

## D14 — Voice reuses the framework STT/TTS pipeline; the connector only classifies + carries the `audio` handle

Voice is a purely CONNECTOR-SIDE additive change — NO framework change. The framework
already routes audio both directions for a generic transport: (inbound) `TransportManager.resolveInboundMedia`
sets `msg.audioAttachment` for a `kind:'audio'` handle and `msg.isVoiceNote` when it carries `ptt:true`, which
gates the chat-service STT (Deepgram `detect_language=true`) + VOICE_MODE_HINT + detected-language threading;
(outbound) a voice-input turn calls `sendAudioUrlViaTransport` → `deliverSynthesizedAudio` →
`manager.send({attachments:[{mimeType:'audio/ogg; codecs=opus', ptt:true}]})` → staged into a `kind:'audio'`
`ptt:true` handle on `transport/send.media`. So the connector's whole job is: **inbound** — classify an audio
attachment (message `flags` IS_VOICE_MESSAGE `1<<13` → true voice memo; else `audio/*` content-type / audio
extension), download from the Discord CDN, `PUT` to `/api/media` as `kind:'audio'` **with `ptt:true`** (and
`duration` from `duration_secs` when present) so the framework transcribes it — mirroring WhatsApp's
`isVoiceNote`. ALL audio is routed as PTT so a plain audio file is also transcribed (never a silent drop into
an unconsumed `audioAttachment`). **No extra gateway intent** — `flags` + the audio attachment ride the DM
`MESSAGE_CREATE` under the existing `DIRECT_MESSAGES(1<<12)` set, exactly like image/file metadata; no new
onboarding step. A download/store failure honest-degrades to `[voice message]`.

## D15 — Outbound = native Discord voice message PREFERRED, plain audio attachment FALLBACK (declared honestly)

Discord native voice messages require the 3-step cloud-attachment flow (register upload → `PUT` Opus bytes to
the signed GCS URL → `POST` message with `flags: IS_VOICE_MESSAGE` + `duration_secs` + `waveform`); a plain
multipart upload will NOT produce a voice bubble. The connector attempts the native flow first: `duration_secs`
is read REAL from the Ogg/Opus granule position (Edge-TTS emits genuine `ffmpeg -c:a libopus -f ogg`), and the
`waveform` is a SYNTHETIC half-sine envelope — Discord does not validate the waveform against the audio, so
only the visual preview bars are approximated; the audio + duration are genuine (never-lie: declared as
synthetic). On ANY failure of the native flow, the connector falls back to delivering the SAME Opus bytes as a
plain playable audio attachment (multipart `files[0]`), which Discord renders with an inline player — an
honest degrade, never a dropped reply. **The native-voice-message send is wire-/cert-proven for the `audio`
handle carry but NOT live-proven end-to-end by the offline cert alone** (the offline cert runs in echo mode
and cannot reach Discord's API), so outbound fidelity is declared conservatively as "native voice message
preferred, audio-attachment fallback" — the live voice round-trip is the operator's QA step. If a future gate
wants the native envelope proven before the flag flips, that is a live-QA follow-up, not a code change.
