# Transport-shape resolution (generic — NOT a per-provider menu)

"Connect me to X" / "build a connector for X" is ambiguous. Before authoring or onboarding, resolve the
shape along three axes by RESEARCHING the service's real options, then ask the user 1-2 focused questions:

- **role** — `dedicated` (the agent's OWN identity on the service) vs `personal` (the USER's own account,
  monitored / acted-as — HIGH trust). Effective trust is binding-derived; `roles:` in the manifest is the ceiling.
- **backend / identity-mechanism** — e.g. Telegram = a BotFather bot (dedicated, official, zero ban-risk) vs a
  dedicated-number MTProto user account vs link-your-own; elsewhere = native API vs Matrix-bridge vs Unipile.
  `backend:` is a free string (see `../backends.md`).
- **auth path** — what each shape implies (a pasted bot token, a QR pairing, an OAuth flow, a signed webhook).

Then build/onboard. This is shared verbatim with the Pass-D `transport-onboarding` skill (which runs a
lightweight version at onboarding); the research-driven version matures here.

A transport IS a channel — so resolving the channel shape is MANDATORY here (this is the explicit correction
of `capability-brainstorming` SKILL.md "channels are transport-agnostic" and `watcher-brainstorming` "don't
ask about channels": true for capabilities/watchers, FALSE for a transport).

## A fourth axis — the INGRESS shape (poll-drain vs push-gateway vs webhook-push)

Besides role × backend × auth, pin HOW inbound events arrive, because it changes the connector's internals
and the onboarding/auth path:

- **poll-drain** (the ~8/9 default): inbound is an outbound-drain poll of the provider (e.g. a `getUpdates`
  loop). The provider holds the queue; the connector just maps a poll → the framework cursor.
- **webhook-push** (e.g. LINE): the provider knocks on a public HTTPS URL. **The framework owns the door**
  (the `ingress: webhook` section of `../CONTRACT.md`): declare
  `ingress: webhook` + `WEBHOOK_PUBLIC_BASE` in `requires.env` + implement `transport/webhook`
  (verify the provider signature over the raw bytes, enqueue, answer fast). **NEVER touch host
  networking yourself — no ports, tunnels, proxies, DNS (HARD RULE).** If `WEBHOOK_PUBLIC_BASE` is
  absent, your folder honestly scans unavailable: tell the user in plain words that a public door
  needs setting up first (an ingress capability provides it), and stop — that's correct, not a wall
  to work around. Check the provider's retry/redelivery story from its official docs (some re-knock
  for hours, some are knock-and-forget) and surface what a door outage really loses.
- **push-gateway** — the connector holds a **persistent WebSocket internally** and the provider PUSHES
  events down it (e.g. a Discord-style gateway socket; Baileys is the same shape off WhatsApp). Auth is a
  pasted **token** (no QR, no pairing, no quiesce) → `session_held: false`. The connector buffers pushed
  events into a **connector-owned monotonic INTEGER-cursor queue** and serves `transport/drain` from that
  buffer NON-consumingly (the framework's `sinceCursor` chooses the slice; the buffer is a bounded retained
  backlog, not a consume-on-read queue). Addressing: a **DM** → `from`/`to` are the **user id** (a
  snowflake/string); a **guild/room** message → still keyed by the user id, but carry the **channel id**
  separately so a reply targets the right room. This shape is backend-agnostic — Discord is merely the
  first instance.

## Baseline SEND/LIVENESS contract for a push-gateway (the authoritative reference the builder follows)

These are the load-bearing rules a push-gateway connector MUST implement. Backend-agnostic; the parenthetical
op numbers are Discord examples only.

- **Heartbeat-ACK watchdog + first-heartbeat jitter.** Send heartbeats on the gateway's interval (jitter the
  FIRST one to avoid a thundering herd). Track the matching ACK (e.g. Discord op 11). On a **missing ack**:
  do NOT keep the socket — terminate it with a **non-1000/1001 close code** (a 1000/1001 normal-close tells
  the provider you went away cleanly and forfeits resume), reconnect to the provider's `resume_gateway_url`,
  and **RESUME** the session (e.g. op 6 `{session_id, seq}`) so the post-gap events replay.
- **INVALID_SESSION → fresh IDENTIFY + HONEST gap.** If the provider rejects the resume (e.g. op 9
  `INVALID_SESSION`), start a **fresh IDENTIFY** and surface the gap honestly: log it AND raise
  `needs_attention`. A fresh identify means the window between the last delivered event and reconnect is
  LOST — **never claim zero-loss over a lost window.** (Resume = zero-loss; fresh identify = a real gap.)
- **The connector-owned cursor is a small INTEGER counter, stable across RESUME and across a fresh
  IDENTIFY.** Do NOT expose the provider gateway `seq` as the framework cursor — `seq` resets on a fresh
  IDENTIFY, which would make a post-identify event collide BELOW the framework's `sinceCursor` and vanish.
  Do NOT use the raw message **snowflake** id either — it is far above `2^53`, so the durable cursor guard's
  `Number()` coercion silently loses precision. Maintain your OWN ever-increasing integer (start at 0, ++
  per buffered event), persisted across restarts.
- **REST send: respect the provider's rate limiter AND preserve order.** Honor `429` `retry_after` (and the
  **global** rate-limit flag and the **per-route bucket** headers), back off and **requeue** rather than
  drop. The framework `OutboundPacer` is NOT provider-bucket-aware, so a multi-part send must **PRESERVE
  ORDER** — send part N+1 only after part N is accepted (don't fire concurrently and let a 429 reorder them).
- **A wedged/deaf connector is RECYCLED, not re-attached.** The framework liveness PING (`probeLiveness`,
  5s) hits the connector over the control plane; on timeout the supervisor **RECYCLES** the connector with a
  **fresh spawn** (not a re-attach). So **RESUME / session-resume is the CONNECTOR's own responsibility on
  its restart** — the framework gives you a clean process, you re-open the gateway and resume from your
  persisted session_id/seq + integer cursor.
