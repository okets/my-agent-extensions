# Pairing pitfalls (read before authoring a QR/code transport)

These are the hard-won rules for any transport that links via a QR code or a
pairing code. They were discovered the painful way during the WhatsApp build —
encode them once so they're never re-discovered.

## Universal pairing practices (every QR/code provider)

- **Mint a rotating credential ONLY on the user's explicit ask** (`refresh`),
  never on background/status polls — a background re-mint races the user's entry
  and invalidates the code they're typing.
- **Regenerate on demand**, don't keep a background keep-alive minting codes.
- **Never de-register / tear down a transport mid-pairing** — it kills the
  in-flight QR/code.
- **Debug method:** when a code is rejected, capture the wire (e.g. pino `trace`
  → file) and read the *server's response to the registration step*. A `400` on
  registration is a malformed/invalid request — NOT a rate-limit.
- **A `400`/`401` can also be a STALE/LEGACY protocol LIBRARY, not your code.** Run
  `npm view <lib> dist-tags` — if your installed version sits on the `legacy` tag (or far behind
  `latest`), UPGRADE before debugging the wire. Never carry a "known-good version" in your head; it
  rots — the WhatsApp rebuild hit exactly this when Baileys `6.7.23` had silently become `legacy`.
  The reusable check `check-dep-freshness.sh <folder>` (in this directory) flags a
  legacy-tagged dep (warn-only, best-effort, skips cleanly offline).

## Provider-specific: WhatsApp (Baileys) — worked example

- **`browser` is load-bearing.** `makeWASocket({ browser })` middle slot is the
  companion CLIENT TYPE. Use a real web client — `Browsers.ubuntu('Chrome')` /
  `Browsers.macOS('Safari')`. **`'Desktop'`/custom → Electron → `400 bad-request`
  on `companion_hello`**, before any code is entered.
- **Finalize the 515.** After "pairing configured successfully", WhatsApp sends
  `515 restartRequired`; you MUST reconnect to finalize, or the device
  de-registers ~2 min later.
- **Use a current protocol version.** Stale WA versions fail the handshake;
  `fetchLatestBaileysVersion()` then fall back to a *current* known-good pin.
- Reference connector: `../examples/whatsapp/` (embodies all of the above).
