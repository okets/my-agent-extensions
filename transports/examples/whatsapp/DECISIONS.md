# WhatsApp connector seed — provenance & build record

> The build-integrity audit trail for the canonical WhatsApp connector exemplar. This
> record exists so the exemplar can serve as the trusted rebuild reference: who authored it, the loop-discipline it was built under, the
> design of its pairing-truth mechanism, and its parity and dependency decisions.

## What this is

`skills/transport-templates/whatsapp/` is the **canonical, framework-authored WhatsApp
connector exemplar** — a self-contained, certified generic transport drop-in. It is the
**source of truth** for the WhatsApp connector. Per `_CONTRACT.md` (Canonical-source rule),
the framework provisions a live runtime folder as a **DERIVED artifact**
from this seed at onboard time (`auth/` + `node_modules` preserved) — connector edits go to
**this seed** + re-provision, never to the live folder.

- `provenance: framework` (declared in `CAPABILITY.md` frontmatter).
- Build discipline: **build-not-copy.** The connector was *re-piped from Baileys into the
  drop-in contract* under the transport-loop methodology (authored via the
  `transport-brainstorming` / `transport-builder` toolchain and certified through the
  conformance harness), not byte-copied from the retired in-core Baileys plugin. Each
  parity fix below was a deliberate, tested change against the contract — not a port.
- Certification gate: changes are re-certified through the conformance CLI
  (`npx my-agent-transport-kit certify`) against the structural + pairing-truth
  probes before the seed is considered green. The seed is **never auto-enabled** — it ships
  inert and is provisioned only on an explicit onboard.

## The pairing-truth mechanism — real-socket preflight

The defining design property of this exemplar is its **real-socket preflight**: the
connector proves the pairing socket is genuinely accepted by the WhatsApp server *on the
same live pairing socket*, with **no separate throwaway probe socket**. Implemented
inside the real Baileys `connection.update` handler in `src/server.ts`:

- **`preflight_ok`** fires exactly **once per socket**, on the **first `qr` event** — the
  first QR proves `companion_hello` was accepted by the server (guarded by
  `preflightOkEmitted`, emitted via the single `setOnboardStatus` source-of-truth, *before*
  the user is asked for the pairing code).
- **`preflight_rejected`** fires on a `connection:'close'` with `statusCode === 400` and no
  real registration — a `400 bad-request` on `companion_hello` means the client identity was
  rejected (the class of failure behind the historical `Browsers` mis-config). It carries the
  status detail so the framework can surface the root cause to the brain **without opening a
  second socket**, and does not reconnect.
- **`registered: true`** ground-truth: the `linked` terminal state gates strictly on
  `isRealRegistration(state.creds)` (`creds.registered === true`) — a bare `connection:'open'`
  without confirmed companion registration is *not* "really paired". The framework's
  live-pair gate and the conformance pairing-truth probe both require this attestation.

This is the **pairing-truth** signal the framework's owner-bind gate and the live-pair
verifier read. It is enforced statically by a framework lint, which asserts the seed emits
both preflight artifacts from the real socket.

## Pairing-truth & parity

The **pairing-truth & parity** work is the reference standard this seed embodies: the connector's pairing path
proves a *real* link (real-socket preflight + `registered:true` ground-truth) rather than a
synthetic ECHO handshake, and the seed honors the adversarial conformance harness flags so a
lying connector cannot certify. This seed is the **reference exemplar** that the live folder
(and any future analogous connector) must derive from — the canonical-source rule + the
provision-from-seed bridge make it authoritative.

## Dependency canonicalization — baileys pinned to 7.0.0-rc13

The seed's `package.json` deps were canonicalized to the **current known-good pins**, aligning
the seed manifest with the live working connector and the
dep-freshness "current known-good pin" policy:

- `@whiskeysockets/baileys`: `^7.0.0-rc.9` → **`7.0.0-rc13`** (exact pin)
- `pino`: `^9.0.0` → **`^9.5.0`** (match the live)
- `qrcode` (`^1.5.4`), `@modelcontextprotocol/sdk` (`1.29.0`), `tsx` (`^4.22.3`), `"type": "module"` — unchanged.

**Why.** The live working connector pins `@whiskeysockets/baileys: 7.0.0-rc13` + `pino: ^9.5.0`;
the **rc13 upgrade is the known-good that fixed a `401` registration failure** (the live had moved
ahead of the seed). The seed's own `package-lock.json` already *resolved* baileys to `7.0.0-rc13`
and pino to `9.14.0`, so the `package.json` ranges were merely **stale relative to the lock + the
live**. Canonicalizing here makes the seed truly authoritative on deps: because the live folder is a
**DERIVED artifact provisioned from this seed** (canonical-source rule), a stale seed manifest would
**degrade the live's pin on reconcile** — re-deriving from a loose `^7.0.0-rc.9` could float the live
off the known-good rc13 and reintroduce the `401`. With the seed pinned to the known-good, the live
can be cleanly reconciled FROM the seed without regressing. No connector code changed — only the deps
manifest; the lockfile was regenerated (`npm install`) so `package.json` + lock agree, and the seed
was re-certified through the conformance CLI (STRUCTURALLY CERTIFIED, onboarding-handshake +
pairing-adversarial probes PASS).

## Outbound link previews — built by the connector, `link-preview-js` pinned 3.2.0

Baileys builds a link preview for any outgoing text containing a URL through its optional peer
dependency `link-preview-js`. Left to itself that fetch has no tight timeout, no private-address guard,
and (when the package is missing) logs "url generation failed" on every send with a link. The connector
now builds the preview itself and passes it to `sendMessage` explicitly — an object, or `null` for plain
text. Passing the field at all, even `null`, stops Baileys' own fetch, so the rules below are the ones
that apply. Applies to `transport/send` and `transport/edit`; echo/cert mode never fetches.

- **Code.** `src/link-preview.ts` (the builder; unit-tested without network by `src/link-preview.test.ts`)
  and `withLinkPreview()` in `src/server.ts`, which returns `{ text }` untouched when there is no URL.
  `link-preview-js` and `sharp` are imported lazily; `warmLinkPreview()` pre-loads both at start so the
  first preview skips the cold start.
- **Dependency.** `link-preview-js` is pinned **exactly `3.2.0`** in `package.json` (lockfile regenerated;
  it brings `cheerio` 1.0.0-rc.11 and `url` as transitive deps). Exact, because the builder passes
  options (`handleRedirects`, `resolveDNSHost`, `followRedirects: 'manual'`) whose behaviour is the
  safety net, and a floating minor could change them silently; Baileys' own peer range is `^3.0.0`, which
  3.2.0 satisfies. `sharp` is not a new dependency: it is Baileys' peer and already resolved in the
  lockfile.
- **Never blocks or fails a send.** The build never throws and has a hard 10 s ceiling (page 8 s,
  thumbnail 4 s and never past the ceiling). Any failure resolves to plain text.
- **Step-down, in order.** Slow or failed thumbnail → card with title and description, no picture. Ceiling
  reached after the page stage finished → the same no-picture card. Page stage unfinished → plain text.
- **Private-address refusal.** Only public http(s) hosts are fetched: loopback, private, link-local,
  unique-local, unspecified and `.local`/`.internal`/`localhost` names are refused — on the first URL,
  on every redirect hop (same-host redirects only: http→https, `www.`) and on the thumbnail URL. The
  thumbnail download is capped at 3 MB and re-encoded to a 192 px JPEG.
- **Per-stage timing log.** Every fallback line carries `load` / `page` / `imageDns` / `imageDownload` /
  `sharp` timings and names the stage still running when a deadline hit, so a slow preview is
  diagnosable from the log alone.
- **Known limit.** A message with a URL waits up to the ceiling for its preview before it is sent. Other
  tool calls (typing, reactions, drain) are not blocked; two back-to-back replies could in theory be
  reordered if the first waits on a slow preview — not guarded.
- No wire/contract change (no `CONTRACT_VERSION` bump).
## Offline catch-up — `messages.upsert` type `append` is delivered, not dropped

The connector used to keep only `messages.upsert` events of `type === 'notify'` (live). Baileys replays
messages that arrived **while the connector was disconnected** with `type === 'append'`, so anything sent
to the agent's number during a disconnect was silently dropped — against the contract's "never silently
drop inbound" rule.

`append` messages are now delivered when they are (a) not `fromMe`, (b) real inbound content (not a
protocol / key-distribution / stub / `status@broadcast` entry), (c) not older than a **floor**, and
(d) not already handled (bounded id dedupe). `notify` is unchanged. Policy lives in the dependency-free
`src/offline-catchup.ts` (tested by `src/offline-catchup.test.ts`); `server.ts` only wires it.

- **Watermark** = timestamp of the newest inbound the connector handled, persisted in `cursor-state.json`
  (the same atomically-written file as the cursor and backlog) together with the bounded handled-id list.
- **Floor is a connect-time snapshot** of the watermark, not the live value: a live `notify` that lands
  before the catch-up batch would otherwise advance the watermark past the older offline messages and
  hide them. Id dedupe is what prevents double delivery within a session. Comparison is `>=` (whole
  seconds) so a same-second sibling of the watermark message survives.
- **First run (no watermark)**: accept only `append` messages from the last **15 minutes** since connect —
  far wider than the 30 s reconnect backoff cap and a supervisor/deploy restart, narrow enough that a
  first-link history replay does not surface stale conversations.
- One log line per catch-up batch: count + oldest timestamp.
- No wire/contract change (no `CONTRACT_VERSION` bump): same `WireInbound` entries, same `enqueue()`.

## Cursor minted at push time, drained tail retained, inbound counted

**The bug.** `ingestMessage` allocated a cursor before it awaited a media download, and the offline
catch-up path (and the edit / reaction paths) did the same with `nextCursor()` at the call site. A faster
message arriving later could take a higher cursor and be pushed first; the framework then advanced
`sinceCursor` past the slower message's lower cursor, and when the slow message was finally pushed it was
permanently undrainable (`drain` returns `cursor > sinceCursor`). That is a silent inbound loss — the
contract's Cursor rule 1 (`transports/CONTRACT.md`) exists because it happened on a live WhatsApp account.

**The fix, in one place.** The queue is now the dependency-free `src/cursor-queue.ts`
(tested by `src/cursor-queue.test.ts`); `server.ts` wires its persist / wakeup / log callbacks.

- `enqueue()` takes an entry **without** a cursor and mints it itself, synchronously with the push. There
  is exactly one `nextCursor()` caller. Every inbound route (live `notify`, offline catch-up `append`,
  edits, reactions, the cert echo, the ingest-error fallback) goes through it, so cursor order is push
  order by construction; no call site can reintroduce the race.
- `drain()` keeps the newest `RETAIN_DRAINED_TAIL` (64) already-drained entries instead of reclaiming them
  the instant the framework acks them, so a re-drain from an older cursor (the cert harness's
  temporal-contract probe, a second in-process reader) still returns everything above it. The returned set
  is still strictly `cursor > sinceCursor` (no duplicates) and the persisted backlog stays bounded.
- A drain whose `sinceCursor` is **ahead** of the persisted counter (a lost state write, then a restart)
  re-anchors the counter to it with a loud warning (Cursor rule 4); otherwise every later enqueue would
  mint below `sinceCursor` and vanish.
- `transport/health` carries `inboundReceived` (raw events handed to ingest — live `notify` plus accepted
  catch-up `append`) and `inboundEnqueued` (pushes to the drainable backlog), the pair the framework's
  silent-drop net compares (Cursor rule 3).

**`SELFHEAL_DROP_INBOUND=1` is a test-only drill switch.** When (and only when) the env var is exactly `1`,
`enqueue()` counts the entry but does not store it, so the framework's "enqueued but never drainable" net
can be proven live on demand. Nothing in the connector or the framework sets it; unset (the production
default) it has no effect. Because it would swallow real inbound if someone did set it, the first swallowed
entry logs one loud `SELFHEAL_DROP_INBOUND=1 — TEST SWITCH ACTIVE` line. A swallowed entry consumes no
cursor. A pre-enqueue skip is deliberately **not** simulated: ordinary filtering looks identical and must
never trip the recycle ladder.

**Composition.** The offline catch-up policy (`offline-catchup.ts`) is unchanged: it still decides *which*
`append` messages are delivered and `markHandled` still runs after the commit, but the delivered messages
now get their cursors at push time like any other. The link-preview work is outbound-only and untouched.

**Not part of this change** (live-only and deliberately left out): the pairing-prompt `label` / `inputType`
fields — the contract does not define them. No wire/contract change (no `CONTRACT_VERSION` bump): the
persisted `cursor-state.json` shape is unchanged.

---

## Link previews follow cross-host redirects (≤ 5 hops), every hop guarded

Some sites send a bare host to another public host (`www.cnn.com` → `edition.cnn.com`). The preview
fetcher used to follow only same-host redirects (http→https, `www`), so those links went out as plain
text with no card.

**Rule.** A page redirect is followed to a different public host, at most `MAX_REDIRECTS = 5` times
(the first request does not count; a 6th redirect is refused and the message goes out as plain text).
**Every hop is re-checked before it is requested**: http(s) only, no `localhost` / `.local` / `.internal`
names, and the resolved address must not be private, loopback or link-local (metadata addresses
included). A redirect loop simply runs into the cap. The 8 s page budget is shared by all hops.

**How.** `link-preview-js` 3.2.0 follows at most one hop itself, so `fetchPageWithRedirects` runs the
chain: the library's `handleRedirects` records the (already absolute) `Location` target and returns
`false`, which stops the library; the loop re-checks the target and starts a fresh `getLinkPreview`
on it. The thumbnail URL of the final page keeps its own separate private-address guard.

Not changed: the send path, the thumbnail rules and the never-throws / never-delays guarantees.
