# my-agent-extensions

The official extensions repo for [**my-agent**](https://github.com/okets) — a framework for
building AI assistants on the Claude Agent SDK. Extensions are **files, not code registrations**:
you drop a self-contained folder into the agent, it gets discovered, supervised, and certified.

**Transports** — the connectors that carry conversations over WhatsApp, Telegram, Discord, LINE,
and more — are the first extension type. **Capabilities** — self-built abilities such as web search,
speech, or vision — are shared here too, as worked examples to read for patterns and adapt.

```
my-agent-extensions/
├── transports/
│   ├── CONTRACT.md          # the backend-agnostic connector contract — read this first
│   ├── backends.md          # choosing a backend (native API, gateway, webhook, Baileys…)
│   ├── references/          # pairing pitfalls, push-gateway send/liveness baseline, dep-freshness
│   ├── examples/            # worked, certified connectors you can adapt (telegram, whatsapp)
│   └── scaffold/            # a start-here connector skeleton
├── packages/
│   └── transport-kit/       # npm: the conformance harness + wire types + `certify` CLI
└── capabilities/            # worked capability examples — inspiration, not canon
    ├── README.md            #   read first: patterns to adapt, not components to install
    └── examples/            #   e.g. image-search (keyless-fallback web image search)
```

## Build a transport connector

A transport connector is a self-contained folder that speaks the MCP control plane — it never
touches host networking, never pushes unprompted, and reports the raw provider sender id verbatim
(the framework derives trust). It certifies **offline**, against a mock provider — no live
credentials needed to prove conformance.

1. **Read** [`transports/CONTRACT.md`](transports/CONTRACT.md) — the load-bearing rules
   (wire protocol, cursor-driven zero-loss inbound, honest degrade, upstream self-heal, pairing).
2. **Start** from [`transports/scaffold/`](transports/scaffold) or adapt the closest
   [`transports/examples/`](transports/examples) connector. Re-derive against the contract — don't
   blind-copy.
3. **Certify** in a clean directory with the published kit:

   ```bash
   npx my-agent-transport-kit certify ./my-connector
   ```

   The harness spawns your connector, negotiates MCP, and drives the wire contract end-to-end. The
   verdict is written to `<folder>/.conformance.json`. Re-run after any edit.

See [`packages/transport-kit`](packages/transport-kit) for the harness, the wire-protocol types you
author against, and programmatic use.

## Contributing a connector back

Built a connector for a backend that isn't here yet? Once it certifies and you've run it live, open
a PR adding it under `transports/examples/<backend>/` — after sanitizing anything specific to your
machine (credentials, private paths, personal names). Every contributed example enriches the pool
every other builder — human or agent — starts from.

## Sharing a capability example

Capabilities are shared differently from connectors — as **inspiration, not canon**. See
[`capabilities/README.md`](capabilities/README.md): read the worked examples for their shape and
re-derive your own; there is no certification and nothing is installed verbatim.

## License

MIT — see [LICENSE](LICENSE).
