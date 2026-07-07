# Transport Connector Contract (backend-agnostic)

A transport connector is a SELF-CONTAINED folder the framework discovers, installs, supervises,
and certifies. It speaks ONLY the MCP control plane — never bytes, never unprompted push.

## Folder layout (the dependency-resolution decision — frozen)
```
<connector>/
  CAPABILITY.md        # frontmatter: interface: transport, backend, roles (CEILING only),
                       #   session_held, requires.env, style{}, onboarding[], template_version, provenance
  package.json         # "type":"module"  +  deps: @modelcontextprotocol/sdk, tsx  (MANDATORY type:module)
  package-lock.json    # committed (deterministic npm ci); node_modules is gitignored
  scripts/setup.sh     # npm ci || npm install  (folder-local node_modules — the browser-chrome pattern)
  src/server.ts        # the connector; imports @modelcontextprotocol/sdk from folder-local node_modules
  .mcp.json            # { "command": "npx", "args": ["tsx", "src/server.ts"] }
```
NODE_PATH is a dead end for ESM. No node_modules may sit in any ancestor of the folder.

## Canonical-source rule (the seed is authoritative; the live folder is DERIVED)
A connector ships as a **SEED** at `skills/transport-templates/<id>/` (e.g. the WhatsApp seed) — that
seed is **CANONICAL**. Its live runtime copy at `.my_agent/capabilities/<id>/` is a **DERIVED artifact**,
**provisioned from the seed at onboard time**, not an independently-maintained folder.

- **Edits go to the SEED, then re-provision (re-onboard) — never hand-edit the live folder.** Hand-editing
  the live folder makes it drift from the canonical exemplar (a real failure this rule closes: the live
  WhatsApp folder had silently lost the seed's real-socket preflight). The live folder is gitignored and
  carries no history — it is not a source of truth.
- **Provision-from-seed bridge.** The generic onboarding path (`create_transport`, dashboard
  `transport-setup-server.ts` → `provisionFromSeed()` in `mcp/provision-from-seed.ts`) copies
  `skills/transport-templates/<id>/` over the live folder on a **fresh** onboard, BEFORE bringing the
  transport up. It runs only when the transport is **not already live** (the idempotent guard protects a
  running session); a connector with **no seed** (agent-authored drop-in) is a clean **no-op**.
- **PRESERVED — never overwritten:** the live `auth/` dir (the paired session/secret — copying it would force
  a **re-pair**) and `node_modules` (the folder-local installed deps). Everything else (`src/`, `package.json`,
  `CAPABILITY.md`, `scripts/`, `references/`, `.mcp.json`, …) is provisioned from the seed.
- **Provenance.** Each seed SHOULD carry a `DECISIONS.md` build-integrity record (who authored it, the
  build-not-copy discipline, its pairing-truth design, key build history) so it can serve as the trusted
  rebuild reference — see `../examples/whatsapp/DECISIONS.md`.

## The core wire strings (inline them — do NOT import a framework module)
- `transport/send`   — `{ to, content, media? }` → transmit one outbound. Return `{content:[{type:'text',text:'sent'}]}`,
                       OR — if the provider gives you the sent message's id — return the text block as JSON
                       `{"messageId":"<provider id>"}` (OPTIONAL but enables reactions targeting your
                       messages; absent/plain-text return = fine, nothing breaks).
- `transport/drain`  — `{ sinceCursor }` → `{ messages: [{ cursor, from, content, timestamp, media?, kind?,
                       providerMessageId?, targetMessageId?, emoji? }] }`,
                       cursor strictly increasing, monotonic, connector-owned. `from` = the UNTRUSTED raw
                       provider sender id, VERBATIM (the framework derives trust; the connector only reports).
                       `providerMessageId` = the provider's own id for the message (set it when available —
                       it lets the agent react to the user's messages).
- `notifications/transport/message_available` — emit on new inbound (a WAKEUP ONLY; the framework's durable
                       cursor is the source of truth — a dropped notification just means re-drain, never loss).

## Optional interaction tools (implement what the provider supports; undeclared = clean no-op)
All best-effort: the framework connection-gates and swallows failures — an interaction failure must never
break a turn. Implement these as additional tools next to send/drain when the provider can do them:
- `transport/typing`   — `{ to, action: 'start' | 'stop' }` → show/clear a typing indicator. Providers
                         auto-expire typing (~5s on Telegram): refresh on a short interval while 'start' is
                         held, stop refreshing on 'stop' or when you transmit the reply.
- `transport/reaction` — `{ to, messageId, emoji }` → attach an emoji reaction to the given provider message.
- `transport/edit`     — `{ to, messageId, content }` → replace a sent message's text (content arrives
                         already rendered for your dialect; single message, never multi-part). NOTE: the
                         framework currently exposes no agent-facing edit trigger (UX decision) — implement
                         it for completeness/future use or omit it; both are conformance-clean.

**Plaintext fallback warnings SHOULD include the provider's error description verbatim** (e.g.
Telegram's `"can't parse entities: …"`). A swallowed or summarised reason makes render bugs
undiagnosable — log the full provider error string at WARN level and, where possible, surface
it in the fallback send result so the framework can observe it.

## Inbound event kinds (report user interactions, not just messages)
A drained message may carry `kind: 'message' | 'reaction' | 'edit'` (absent = 'message'):
- `'reaction'` — the user reacted to one of YOUR messages: set `emoji` + `targetMessageId` (the reacted-to
                 provider message id); `content` may be empty (the framework synthesizes the in-band note).
- `'edit'`     — the user edited an earlier message: set `content` = the NEW full text +
                 `providerMessageId` = the edited message's id. The framework surfaces it as awareness
                 (a note) — it never rewrites history.
Subscribe to the provider's corresponding update types (e.g. Telegram `allowed_updates` must include
`message_reaction` and `edited_message` or the provider will never deliver them).

## Inbound is cursor-driven (zero-loss)
Poll/long-poll the provider with an offset = the FRAMEWORK-DRAINED cursor + 1 (NOT the highest received id),
so the provider keeps every update until the framework has drained it — a connector crash loses nothing.

### Cursor rules (2026-07-02 — a real live message loss)
1. **Mint the cursor AT PUSH TIME, inside `enqueue()` — never at ingest-start.** If ingestion awaits
   anything (a media download, an API lookup) between event arrival and the backlog push, a cursor
   allocated before that await can be OVERTAKEN by a faster concurrent message: the later message gets a
   higher cursor, drains first, the framework advances `sinceCursor` past your still-pending lower
   cursor, and when you finally push, the entry is **permanently undrainable** (`drain` filters
   `cursor > sinceCursor`; the next drain GCs it). This silently lost a live WhatsApp inbound on
   2026-07-02. Cursor-order MUST ≡ push-order — one `nextCursor()` caller, inside the push.
2. **Never evict an un-acked entry.** A bounded backlog may GC entries at/below the framework-drained
   cursor (acked) freely; evicting entries ABOVE it (e.g. a hard cap splice of the oldest) is silent
   inbound loss. If a cap forces eviction, warn LOUDLY and surface an eviction count on
   `transport/health` so the framework can alarm.
3. **`inboundEnqueued` counts drainable pushes, exactly.** The framework's silent-drop net compares
   your `inboundEnqueued` against what it drains. Increment it when (and only when) an entry is
   committed to the drainable backlog — never on raw receipt (that's `inboundReceived`), never for
   entries you filtered out. Miscounting either direction breaks the net: over-count → your healthy
   connector gets recycled forever; under-count → real drops go undetected.
4. **Self-heal a stale cursor high-water.** If a drain arrives with `sinceCursor` ABOVE your persisted
   cursor counter (a lost state write + restart), re-anchor your counter to it loudly — otherwise every
   future enqueue mints below `sinceCursor` and is undrainable.

## Honest degrade — NEVER a silent drop (HARD RULE, cert-enforced)
An inbound the connector cannot fully carry — a photo/voice note/sticker/file on a connector that
doesn't support that kind (yet), an unknown message shape, a failed media fetch — MUST still surface
something to the user: the **caption verbatim** (when present) plus a **bracketed placeholder**
(`[photo]`, `[voice note]`, `[unsupported message]`). Never silence, never an empty entry (an empty
entry is silence in disguise downstream). Scoping a capability out (e.g. a text-only Phase 1) is
legitimate; swallowing what users send is not — the user must never have to ask "did you get that?".
**Enforced at cert** by the `transport-inbound-honest-degrade` probe (2026-07-02): an attachment
send through the cert echo must drain with the caption/placeholder (or carry the handles, for a
media-declaring connector) — a silent or empty result is a cert FAIL.

## MCP stdout purity — stdout is the wire (HARD RULE)
The connector's **stdout (fd 1) IS the MCP JSON-RPC channel**. Any stray byte corrupts the protocol.
- **Never `console.log`** in connector code — use `console.error` / write to **stderr (fd 2)** (the
  supervisor forwards stderr to the journal, id-prefixed).
- **Loggers default to stdout** — e.g. pino 9.x needs `pino(opts, pino.destination(2))`. Check every
  library you pull in (discord.js and many WS/gateway libs have stdout debug output; disable or
  redirect it). This corrupted a live wire once, in practice.

## Media env (`MEDIA_ENDPOINT` / `MEDIA_TOKEN`) — declare them or lose media
The connector child receives ONLY the env keys listed in `requires.env` (scoped env — never the host's
full `process.env`). The media endpoint config is NOT auto-injected: **list `MEDIA_ENDPOINT` and
`MEDIA_TOKEN` in `requires.env`** or every media send/receive hits an undefined endpoint. (They are
automatically EXCLUDED from the onboarding secret gate — the user is never asked for them; the
framework provisions them. Two different lists: `requires.env` = what reaches the child; the
onboarding gate = what the user must supply.)

## `ingress: webhook` (provider-knocks transports) — the door is PROVIDED, never provisioned
Some providers (LINE, Messenger, Viber, Slack-default…) have no poll/socket mode: they deliver by
knocking on a public HTTPS URL. The framework owns that door;
your connector NEVER touches host networking — no port binding beyond your own MCP stdio, no
tailscale/proxy/DNS/firewall changes, ever (**HARD RULE**, born from a real double remote-access
lockout). Instead, declare the coupled triple:
1. `ingress: webhook` in frontmatter (+ `upstream: webhook` when you hold no persistent socket).
2. **`WEBHOOK_PUBLIC_BASE` in `requires.env`** (framework-provisioned like `MEDIA_ENDPOINT` —
   excluded from the onboarding secret gate; the user is never asked for it). No public door
   capability installed → the env is absent → your folder honestly scans `unavailable — missing
   WEBHOOK_PUBLIC_BASE`. That is CORRECT behavior, not a bug: tell the user a public door needs
   setting up; do not work around it.
3. **The `transport/webhook` tool.** The framework forwards every knock on
   `<WEBHOOK_PUBLIC_BASE>/<your-name>[/sub/path]` as
   `{ method, path, headers, bodyB64 }` — `bodyB64` is the EXACT raw request bytes (base64).
   You: verify the provider signature over those exact bytes (decode, HMAC, compare — reject
   unsigned/bad-signature knocks with `{ status: 401 }`); enqueue accepted events into your
   durable backlog (mint cursors at push time, normal Cursor rules); answer FAST with
   `{ status }` (default 200) — never do slow work (media downloads etc.) before answering, the
   provider's timeout is short. GET-challenge providers: echo via `{ status, body, contentType }`.
Register `<WEBHOOK_PUBLIC_BASE>/<your-name>` with the provider (env-overridable base, as always).
A knock while you're down is answered 503 by the framework so the provider retries — your backlog
dedupes on arrival (provider event ids), so retries are safe. Cert note: the harness knocks on
`transport/webhook` directly with synthetic signed payloads — you certify fully offline, no live
door needed.

**Outbound media on a FETCH-shaped provider (LINE class — the provider downloads from a public
URL instead of accepting uploaded bytes):** ask the framework to mint a presigned public link —
`POST ${MEDIA_ENDPOINT}/api/media/<handle-id>/public-link` (with `x-media-token`, like every media
call) → `{ url, expiresAt }` → hand that url to the provider (e.g. LINE `originalContentUrl`).
The link serves exactly ONE asset, expires within the hour, and rides the already-public door
path. **NEVER build a public URL yourself, never expose your own listener, never point the
provider at MEDIA_ENDPOINT (it is tailnet-private — the provider cannot reach it).** A 503 from
the mint means no public door capability is live: degrade honestly (caption/text fallback) and say
so. Underscore-prefixed paths under `/hook/` (`/hook/_media/…`) are a RESERVED framework
namespace — never register a connector name starting with `_`.

## `upstream: push` (gateway/websocket) — the coupled triple
A push-upstream connector (Discord gateway class) MUST declare all three together:
1. `upstream: push` in frontmatter, **with `transport/health`** (structural cert FAIL without it) —
   report frame-level evidence (last heartbeat-ack), never "process is up".
2. `upstream_self_window_ms: <your gateway heartbeat window>` — push connectors get NO derived
   keepalive cadence; without this the framework probes you against a bare 90s floor.
3. **RESUME is yours.** After a supervisor RECYCLE you get a fresh process, never a re-attach — restore
   your gateway session (RESUME/re-IDENTIFY) yourself, and treat INVALID_SESSION honestly: if the lost
   window can't be replayed, say so via `transport/health` (`needs_attention` over a zero-loss lie).
Also: provider snowflakes/seq are NOT cursors (a snowflake overflows `Number.isSafeInteger` → cert
FAIL; `seq` resets on IDENTIFY) — keep your own persisted integer counter (see Cursor rules).

## Multi-part sends preserve order manually
The framework's `OutboundPacer` is not provider-bucket-aware: when you split a long message, send part
N+1 only after part N is ACCEPTED by the provider, or parts can arrive reordered under rate limiting.

## `.enabled` is written LAST
Discovery treats a folder as onboardable only when it is BOTH certified AND `.enabled`. Write
`.enabled` after the cert passes — an `.enabled`-before-cert folder shows up as "no known connector"
in onboarding (fail-closed), which reads like a framework bug but is your ordering.

## The provider endpoint MUST be env-overridable
Read the base URL from an env var (e.g. `TELEGRAM_API_BASE`, default the real provider) so the conformance
harness (and the S3 mock provider) can steer the connector at a stub. A hardcoded endpoint is un-probeable.

## Roles & trust are BINDING-DERIVED (never declared)
`roles:` in frontmatter is the CEILING (which roles the connector CAN carry), strict-enum. The EFFECTIVE role
and trust are derived framework-side from the owner-binding — NEVER read from frontmatter. A frontmatter
singular `role:` is impersonation and is rejected pre-cert.

## Secrets never enter the transcript
The connector DECLARES its onboarding steps (`onboarding:`); the brain CONDUCTS them; the framework captures
secrets to the secret store. The connector reads its secret from `requires.env`; the brain only ever sees
`key <KEY> submitted`. Never echo a secret.

## session_held & self-heal
`session_held: true` (fail-safe default if a live login is held) means a code re-derive must re-pair, not
clobber — the session-quiesce fence governs. Transient network/rate-limit drops are handled
connector-side FIRST (the bounded upstream watchdog + reconnect mandated in **Upstream liveness** below),
with the supervisor's recycle as the framework safety net underneath — never a one-off manual code fix.

### Upstream liveness — watchdog + `transport/health` (persistent-upstream MANDATE)

A connector that holds a **persistent upstream relationship** with its provider — declared via the
manifest `upstream:` axis as `socket` (a held provider WebSocket, e.g. WhatsApp/Baileys),
`poll` (a long-poll / getUpdates pull loop, e.g. Telegram), or `push` (a provider-initiated push
gateway, e.g. Discord) — can **ZOMBIE**: the connector PROCESS stays alive and answers the MCP
`ping`, while its provider socket has died half-open (no error, no close event). The stdio ping
never touches the upstream, so a zombie is invisible to process-liveness. This is the live failure
of 2026-06-28 — WhatsApp's WSS died silently for ~8h while the framework kept reporting `connected`,
and the connector had to be bounced by hand.

Every connector with `upstream: socket | poll | push` MUST:

1. **Declare its upstream shape (so the framework knows the cadence).** Set `upstream:` in
   `CAPABILITY.md` frontmatter (`socket`/`poll`/`push`; absent ⇒ fail-safe `none` = no real
   upstream, mandate inert). The framework derives this transport's stale-threshold from the
   shape's keepalive/poll cadence (`threshold = max(90s floor, cadence × safety-multiple)`), so a
   single missed keepalive never reads as dead. Keep your real keepalive/poll interval at or under
   that cadence (an explicit-but-unknown `upstream:` value is REJECTED pre-cert — a typo must not
   silently drop you out of this mandate).

2. **Run a connector-side upstream watchdog driven by FRAME-LEVEL evidence.** Track a
   `lastActivityAt` updated on every real upstream frame (inbound message, keepalive / heartbeat
   ACK, connection event). A watchdog (a single `setInterval` — **no new external clock**) that
   sees `now - lastActivityAt > window` MUST force the upstream down (`ws.terminate()` /
   `sock.end()`) to drive the reconnect path. **Absence of a close event is NOT liveness** — a true
   half-open fires no close, so `connected` MUST be derived from the independent activity check,
   never from "no close seen". Gate this watchdog on `SELFHEAL_WATCHDOG_OFF` (next section).

3. **Reconnect a dead upstream — bounded, and terminal-aware (Invariant 5).**
   - **Environmental / transient** drops (network blip, rate-limit, a transient provider close —
     e.g. WhatsApp 408/428/500, a Discord non-1000/1001 close, a watchdog-forced terminate):
     reconnect with **capped exponential backoff**, and **retry environmental failures FOREVER** —
     the cap bounds the backoff INTERVAL, not the attempt count. A network outage that lasts hours
     must still recover on its own when the network returns; never permanently give up on a
     recoverable cause.
   - **Terminal / human-actionable** failures (auth revoked, logged out, pairing expired AFTER a
     successful link): do NOT spin a reconnect loop. **PAGE the human** — emit
     `notifications/transport/needs_attention { reason }` (see the terminal-auth-failures section) and idle.
   A capped-backoff retry-forever loop is correct ONLY for an environmental cause; a terminal cause
   pages, never retries.

4. **Report `transport/health` from REAL upstream evidence, with a reason.** Implement the
   `transport/health` wire tool (inline the literal `transport/health` — connectors do NOT import
   framework modules). Return `{ upstreamConnected, lastUpstreamActivityMs?, reason? }`:
   - `upstreamConnected` — `true` ONLY when a live upstream actually exists (a non-null socket / a
     healthy poll loop) AND `lastActivityAt` is recent. Derive it from the SAME frame-level evidence
     the watchdog uses — NEVER from "the process is up".
   - `lastUpstreamActivityMs` — ms-epoch of the last real upstream frame, so the framework catches a
     silent half-open (`now - lastUpstreamActivityMs > staleThreshold`) even when the socket object
     still looks "open".
   - `reason` (when down) — `'transient'` (recoverable — reconnecting), `'network'` (environmental —
     recoverable), or `'logged_out'` (terminal — needs a human re-pair, do NOT reconnect). This is
     what lets the framework read the terminal-vs-environmental verdict (Invariant 5) off the report.
   **Never report `upstreamConnected: true` without a live upstream** — that is the never-lie
   violation the conformance honesty fixture drives down and FAILS.

**The framework enforces both, two ways:**
- **(a) Runtime net.** The 30s heartbeat sweep's `probeLiveness` calls `transport/health` after the
  stdio ping; `upstreamConnected: false` OR a stale `lastUpstreamActivityMs` routes into the SAME
  recycle path as a ping timeout (fresh process → fresh upstream → resume). So even a connector whose
  OWN watchdog is disabled or buggy is caught by the framework net within one sweep + threshold.
- **(b) Cert.** Conformance REQUIRES the `transport/health` tool for any persistent-upstream
  connector — omitting it is a structural FAIL (the mandate is not opt-out) — and the honesty
  sub-check drives the connector's mock upstream down and asserts it reports
  `upstreamConnected: false` (a lying connector FAILS cert).

> The connector-side watchdog (2 + 3) is the **PRIMARY** self-heal — fastest, on the connector's own
> terms. The framework probe → recycle (a) is the **SAFETY NET** — it catches a connector that can't
> or doesn't self-heal. Both must hold; the `SELFHEAL_WATCHDOG_OFF` toggle (next) disables only the
> connector's own watchdog so the framework net can be proven independently.

### Kill-switch: `SELFHEAL_WATCHDOG_OFF` (A6 — the framework-net self-heal proof)
A connector that runs its OWN upstream watchdog (the connector-side zombie/half-open detector that
forces a reconnect — Phase B2) MUST gate that watchdog on the `SELFHEAL_WATCHDOG_OFF` env var:

```
SELFHEAL_WATCHDOG_OFF=1   → do NOT start the connector's own upstream watchdog (no self-reconnect)
unset / any other value   → watchdog ON (the default — the connector self-heals as normal)
```

Read it once at startup (`process.env.SELFHEAL_WATCHDOG_OFF === '1'`); inline the literal — connectors
do NOT import framework modules. The framework forwards this flag to the connector through the supervisor's
safe env baseline (it is a plain non-secret toggle, never a credential), so setting it on the agent process
reaches every connector.

CRITICAL — **keep `transport/health` truthful even when the watchdog is OFF.** The toggle disables only
the connector's self-recovery; it MUST NOT change what `transport/health` reports. With the watchdog off,
a dead/half-open upstream still reports `{ upstreamConnected: false }`, so the FRAMEWORK net
(`probeLiveness` → `transport/health` → `supervisor.recycle()`) catches the zombie on its own. This is the
reversible way to prove the framework safety net independently of the connector's own self-heal: flip the
env, force a zombie, and watch the framework recycle. Unset the env to restore connector-side self-heal.

### Quiesce / session export
A `session_held: true` connector that CAN export its session MUST implement the `transport/quiesce` tool.
Inline the literal method string `transport/quiesce` — connectors do NOT import framework modules.

On `{ action: 'export' }`:
1. Serialize the live session/auth state to the on-disk auth dir (`.my_agent/auth/<transportId>/`, the same
   dir it reads creds from on spawn).
2. Release the live login (close sockets / drop handles).
3. Only then return `{ status: 'exported' }`.

On ANY failure, or if the connector holds no active session, return `{ status: 'failed' }` (or
`{ status: 'not_held' }`) with the live login **left intact**.

If a `session_held: true` connector CANNOT export its session, it SHOULD omit the `transport/quiesce` tool
entirely — the framework will then ask the human to re-pair rather than re-derive over a live identity.

The framework calls quiesce BEFORE any code re-derive, and only when the on-disk session dir is present.
A successful export lets the re-derived connector reconnect the SAME login — no re-import step is needed;
the connector reads the auth dir on startup as normal.

### Terminal auth failures — needs_attention
When a connector detects a terminal, human-actionable failure (auth revoked, session logged out, pairing
expired), it MUST emit:

```
notifications/transport/needs_attention  { reason: "logged_out" | "pairing_expired" }
```

Then it SHOULD idle (awaiting re-pair) or exit — the framework holds restart either way and surfaces a
"needs you" signal to the user. The connector MUST NOT spin a reconnect loop on a terminal auth failure.

Inline the literal method string `notifications/transport/needs_attention` — connectors do NOT import
framework modules. The `{reason}` field is the only recognized key; use `"logged_out"` for revoked
sessions and `"pairing_expired"` for QR/link-code timeouts. Unknown reasons are accepted (forward-compat).

### Interactive-onboarding artifacts MUST regenerate ON DEMAND
A link credential (QR / pairing code) is short-lived BY DESIGN (e.g. WhatsApp QR rotates ~20-60s). The
user may be slow, distracted, or asleep — so a stale credential must **never be a dead end**. The model
is **on-demand, not keep-alive**: don't churn the provider in the background re-minting credentials
nobody is looking at; mint a fresh one **when the user asks**, regenerating if the prior went stale.

- **Pairing-phase expiry is not `needs_attention`, and is not auto-cycled.** Reserve
  `needs_attention{logged_out}` for a logout AFTER a successful link (a real revoked session — gate it
  on "were we ever registered/linked?"). On a QR/code window timeout while still UNPAIRED, **go idle**
  (close the socket; do NOT reconnect-loop a perpetual fresh-QR factory).
- **The "ask" regenerates.** `transport/onboarding_poll` while unpaired-and-idle MUST mint a fresh
  artifact on that call (open a socket / request a new code) and return a currently-valid one — the user
  can ask as many times as they need, with NO manual cleanup. A live socket already holds a recent
  artifact; return it. Never strand the user on a stale credential, and never keep a socket churning
  just to hold one warm.
- **One exception — completing a link is not keep-alive.** A provider's in-band post-scan restart
  (e.g. Baileys' 515 `restartRequired`) MUST reconnect immediately to finish the link; that is finishing
  pairing, not background cycling.

## Pairing correctness (QR / code transports)
Hard-won rules — see [`references/pairing-pitfalls.md`](references/pairing-pitfalls.md) for the full
worked example and evidence. Every QR/code connector MUST:
- **Mint the rotating credential ONLY on the user's explicit ask** (`refresh`), never on a background
  or status poll — a background re-mint races the user's entry and invalidates the code they're typing.
- **Regenerate on demand, never keep-alive.** Don't run a background factory minting codes nobody is
  looking at.
- **Never tear down / de-register a transport mid-pairing** — it kills the in-flight QR/code.
- **Use a real, recognized client identity.** If the provider advertises a client/browser type, use a
  legitimate one (WhatsApp: `Browsers.ubuntu('Chrome')`; a `'Desktop'`/custom tuple → Electron →
  `400 bad-request` on `companion_hello`, before any code is entered).
- **Finalize a post-pair restart.** If the provider sends a restart after the link (WhatsApp's `515`
  `restartRequired`), reconnect immediately to finish, or the device de-registers minutes later.
- **Debug by reading the wire.** A `400` on the registration step is a malformed request, NOT a
  rate-limit — capture the wire (e.g. pino `trace` → file) and read the server's response.

## Media (handles only — bytes move out-of-band)
Binary is **by-reference**: only a small JSON `MediaHandle` (`{id, mimeType, size, kind, caption?,
duration?, ptt?}`) ever crosses `transport/send` or `transport/drain`. **NEVER** put base64 / bytes / a
`data:` URL in the wire JSON. Bytes move out-of-band over the localhost media HTTP endpoint, whose base URL
+ token the framework injects into your connector env as `MEDIA_ENDPOINT` + `MEDIA_TOKEN`.

- **Inbound** (provider → user): download the provider's media bytes, then
  `PUT ${MEDIA_ENDPOINT}/api/media` with the raw blob as the body, `content-type: <mimeType>`,
  `x-media-token: ${MEDIA_TOKEN}`, and `x-media-kind` / `x-media-caption` / `x-media-duration` /
  `x-media-ptt` headers as applicable. The response is `{ handle }` — emit that handle in the inbound's
  `media` field (`WireInbound.media: MediaHandle[]`). Never inline the bytes.
- **Outbound** (user → provider): a `transport/send` may carry `media: MediaHandle[]`. For each handle,
  `GET ${MEDIA_ENDPOINT}/api/media/:id` (with `x-media-token: ${MEDIA_TOKEN}`) to fetch the bytes, then
  upload them to the provider. The send args give you the handle, not the bytes — fetch out-of-band.

Declare the kinds you support via `style.images` etc. Until a kind is conformance-proven it stays
"declared-unproven" and the brain never promises it.

`style.register` — an optional one-line tone instruction for the agent when speaking on this channel
(e.g. `"warm, brief, emoji-light"`). Unset → the framework's concise messaging default applies:
"concise messaging voice — lead with the answer; no meta-narration, no thinking-out-loud, no spoken
plans; match the brevity of a chat app". Connector manifests override this per-channel by setting a
non-empty `style.register` string.

**Declare ONLY what the connector actually carries.** A `style` flag is a claim about THIS
connector's implemented behavior, not about what the provider could do: `style.images: true`
with a drain that skips non-text updates (or a send that ignores `media`) is a LIE the
never-lie system may eventually credit. If you don't wire a kind end-to-end (inbound handle
upload + outbound handle fetch, per the media section above), declare it `false`. But `false` is a
DOCUMENTED LAST RESORT after a genuine attempt — not a default: a same-backend connector in the repo
or its git history that already carries the kind is your reference, and you must not ship fewer
capabilities than it proved. Record a deliberate `false` + its reason in `DECISIONS.md`, then flip it
on when you implement it.

**Undeclared media must DEGRADE HONESTLY — never silently drop.** When an inbound update
carries media you don't handle, you must still: (1) deliver any caption/accompanying text
VERBATIM — a caption is the user's words, dropping it loses a text message; and (2) emit a
bracketed placeholder for the media itself (e.g. `[photo — not viewable on this channel yet]`)
so the brain knows something arrived and can answer honestly. Skipping the whole update makes
the user's message vanish without a trace — the worst possible failure mode (a real live
finding: a captioned photo disappeared entirely; the user saw silence).

## Certification is persisted — and invalidated by edits
A conformance run persists its report to `<connector-folder>/.conformance.json` (generated —
gitignored, never hand-written). The framework reads certification from that file; it is what
makes the folder onboardable and auto-wired after restart. **After ANY edit to the connector,
re-run the conformance CLI** — the persisted cert describes the code that was certified, not
the code on disk now.

The framework watches cert freshness: editing connector source after certification marks the
capability degraded and the self-heal machinery re-certifies it automatically — but do not rely
on that net; re-certify as part of your build.

### No-contention cert gate (REQUIRED)
Cert runs must never contend with a live instance: gate live provider polling so a
conformance-driven send (synthetic/non-deliverable target) disarms it. A concrete pattern:

- **Echo-on-non-deliverable-target**: when the connector detects a send to a target that
  matches the conformance harness's synthetic recipient (e.g. a recognizable test chat-id or
  `to` string), skip the provider call and echo the message back as an inbound drain entry
  instead.
- **Post-handshake grace timer**: after the MCP handshake completes, hold provider polling for
  a short grace window (e.g. 500 ms); a loopback send arriving within that window cancels the
  timer and keeps polling permanently suppressed for the cert run.

Without this, an automatic re-cert while your connector is live can steal provider updates
(409/poll contention), causing the cert run to drain real user messages and the live connector
to miss them.

## Done = certified
`scripts/setup.sh` then the conformance CLI must PASS:
`npx my-agent-transport-kit certify <connector-folder>`.

Connector and capability authoring ships when the conformance CLI passes — no extra review gate.

## Deep-framework review gate

A **deep-framework change** — anything touching routing/delivery, concurrency, recovery/CFR, the
wire/cert substrate, or the self-heal bridge — does **not** reach "done"/merge-to-live until an
independent adversarial review is attached to the sprint and its findings resolved. "Independent"
means a fresh zero-context reviewer or a verification workflow, not the same session that wrote
the code.

**Trigger question:** "Can a single live round-trip exercise this change end-to-end?"
- If YES (a connector, a capability, a UI string, a diagnostic message) → live-oracle is enough; no extra gate.
- If NO — the change rests on **concurrency, multiple conversations, scheduled timing, retries, recovery
  state, adversarial connectors, or edge cases that only manifest under load** → it gets the independent gate.

Examples that require the gate: anything in `transport-selfheal-bridge.ts`, the `RecoveryOrchestrator`,
the `McpWireAdapter`, the `OutboundPacer`, cert admission (`synthesize-transport-config.ts`), the
provision-from-seed bridge.

Examples that do NOT require the gate: genericizing a UI string, renaming a console header (this file),
adding a connector template, adjusting a log message.

## Generalized-path testing discipline

A test or review for a **generalization** must exercise the **generalized path** — it must not rely
solely on the one live instance as the only exercised case:

- Use a **runtime-onboarded connector** (a connector added at runtime, not just the seed).
- Use a **second or non-default transport** (e.g. not just the live WhatsApp/Telegram instance).
- Keep `live === seed` (the live folder in `.my_agent/capabilities/` must reflect the current seed
  in `skills/transport-templates/`, or the "certified" claim is for a different codebase).

The generalized-path discipline is what catches regressions a single live round-trip cannot:
a code path that works for the one live connector but breaks for any other connector added later.
When a test is the only guard on a generalization, it must prove the general case — not just
document the specific instance.
