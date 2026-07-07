# Transport Backends (choose at build time — the framework owns none)

- **native** (default): the provider's own API/SDK. Inbound = an outbound-drain poll for ~8/9 providers.
- **LINE** is the lone webhook-PUSH exception: a public ingress (tunnel + raw-body signature verify);
  it loses inbound while its door is down (no provider-side queue). Surface "inbound paused".
- **Matrix / mautrix**: DOCUMENTED as one known option for personal / no-API networks — NEVER endorsed in a
  template (a template suggestion re-owns integration quality the maintainers deliberately left to the author). The agent may choose it.
- **Unipile**: the named cloud exception for LinkedIn / X DMs.
- **Baileys** (WhatsApp): the persistent-socket exception. Unlike the ~8/9 poll-drain providers, Baileys
  holds a live WebSocket and PUSHES events — the connector buffers pushed events into its own monotonic
  cursor queue and serves `transport/drain` from that buffer (the cursor model is unchanged; the source
  is a queue, not a getUpdates call). Two more Baileys-specific shapes the contract didn't originally
  anticipate (see the `examples/whatsapp/` seed):
  - **Auth is NOT a pasted secret.** Login is QR-scan or an 8-char **pairing code**; it yields a
    MULTI-FILE credential DIRECTORY (not a single token). The connector owns that dir folder-locally
    (`./auth/`, gitignored) — the framework injects no credential, so no credential-directory storage is
    needed framework-side. The MVP onboarding surfaces the **pairing code as an inbound chat message**
    (a string the user types into WhatsApp), sidestepping the absence of a QR-image onboarding widget.
    QR-image onboarding is a future framework enhancement (a new control type).
  - **`session_held: true`** — a live login is held, so a code re-derive must re-pair (the connector
    implements `transport/quiesce` to export/flush creds + release the socket).
  - The connector's "env-overridable endpoint" for conformance is an **ECHO mode**: with
    `WHATSAPP_PHONE_NUMBER` absent the connector never opens a real socket and loopback-echoes sends
    (mirrors Telegram's no-token echo) so the harness can certify the wire without WhatsApp creds.
  - **The `examples/whatsapp/` seed is the worked example for QR/code pairing.** It embodies every rule in
    `references/pairing-pitfalls.md` — `Browsers.ubuntu('Chrome')` (not a `'Desktop'` tuple →
    `400 bad-request`), the `515 restartRequired` finalize-reconnect, and mint-on-explicit-`refresh`
    only. If you author another QR/code backend, read the pitfalls and use this seed as the reference.

- **push-gateway** (backend-agnostic — generalizes the Baileys precedent OFF WhatsApp specifics; Discord is
  the first non-Baileys instance): a **persistent push gateway** where the connector holds the socket and the
  provider PUSHES events down it. The connector buffers pushed events into a **connector-owned monotonic
  INTEGER-cursor counter** (start at 0, ++ per event) — **reset-safe** and **PERSISTED across restarts** so a
  post-restart message can't collide BELOW the framework's `sinceCursor` and vanish — and serves
  `transport/drain` **non-consumingly** from a **bounded retained backlog** (`sinceCursor` selects the slice;
  reading does not pop). Auth is a pasted **token**; `session_held: false`; **no pairing** (no QR, no
  pairing-code, no `transport/quiesce`). Distinct from Baileys (same shape, but Baileys auth is a QR/code →
  credential DIRECTORY and `session_held: true`). The cursor must be this OWN integer, NOT the provider
  gateway `seq` (resets on a fresh IDENTIFY → would collide below `sinceCursor`) and NOT a raw message
  snowflake (above `2^53` → the durable guard's `Number()` coercion loses precision). The send/liveness
  baseline (heartbeat-ACK watchdog + first-heartbeat jitter, RESUME on missing-ack via a non-1000/1001 close,
  fresh IDENTIFY + honest `needs_attention` gap on INVALID_SESSION, `429`/`retry_after` + global-flag +
  per-route-bucket REST backoff with order preserved, and supervisor RECYCLE-not-reattach on a
  `probeLiveness` timeout) lives in `references/transport-shape-resolution.md`.

- **Upstream self-heal (every persistent-upstream backend — `socket` & `push`, plus `poll`):** any backend
  that holds a live provider relationship MUST declare it via the manifest `upstream:` axis (`socket` =
  Baileys, `push` = a push gateway like Discord, `poll` = Telegram) and carry the watchdog +
  `transport/health` duty defined in `_CONTRACT.md` (**Upstream liveness — watchdog + `transport/health`**).
  A held socket can **zombie** — the process stays alive while the socket dies half-open (no close event),
  invisible to the MCP `ping` — so liveness MUST come from FRAME-LEVEL activity tracking + a watchdog (force
  the socket down → bounded capped-backoff reconnect, retry-environmental-forever, page-only-terminal) +
  an honest `transport/health` report (`{ upstreamConnected, lastUpstreamActivityMs?, reason? }`). The
  framework enforces both — `probeLiveness` → `transport/health` → supervisor recycle (runtime net) +
  conformance never-lie (cert). `socket` and `push` are the persistent shapes that most need it; `poll`
  carries the lighter form (a stall surfaces as failing polls, plus the same health report). This
  generalizes the 2026-06-28 WhatsApp-zombie fix OFF WhatsApp specifics onto every persistent-upstream
  backend.

`backend:` in frontmatter is a FREE STRING, never a framework enum.

## Provenance
`provenance:` ∈ { framework | agent | human-contributed }. Tells audits + CFR what they re-derive. A human
may PR a vetted connector into `transports/examples/<backend>/` → it becomes a curated worked example
AND a heal target (no marketplace, no running strangers' code at runtime). Templates are versioned
(`template_version`) so improving an example propagates to every agent's next heal.
