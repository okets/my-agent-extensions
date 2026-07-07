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
