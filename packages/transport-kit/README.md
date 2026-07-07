# my-agent-transport-kit

The conformance harness, wire-protocol types, and manifest validator for authoring
[**my-agent**](https://github.com/okets/my-agent-extensions) transport connectors — packaged so
you can certify a connector folder in a clean directory, with no framework checkout.

```bash
npx my-agent-transport-kit certify ./my-connector
```

A my-agent transport connector is a self-contained folder that speaks the MCP control plane. This
kit runs the **same** structural conformance pass the framework runs internally — so a connector
that certifies here is a connector the framework will accept. That is what makes building a
connector possible without us.

## What certification checks

The harness spawns your connector, negotiates MCP, and drives the wire contract end-to-end:

- **manifest** — `CAPABILITY.md` frontmatter is a valid transport manifest.
- **handshake / protocol** — the connector speaks the current MCP protocol version.
- **wire round-trip** — `transport/send` and `transport/drain` behave, cursors are numeric,
  strictly increasing, and durable (zero-loss inbound).
- **untrusted `from`** — the raw provider sender id is surfaced verbatim (the framework derives
  trust; the connector only reports).
- **honest degrade** — media/attachments the connector can't carry still surface a caption plus a
  bracketed placeholder. Never a silent drop.
- **health** — persistent-upstream connectors (`socket`/`poll`/`push`) report `transport/health`
  truthfully from real upstream evidence.
- **render** — declared style flags (`bold`, `images`, `nestedList`, …) are proven against the
  dialect renderer; unproven flags are reported, never silently trusted.
- **pairing** (QR/code connectors) — onboarding handshake + adversarial pairing probes. Live
  pairing itself is marked `unproven` and is proven separately at live-pair time.

The verdict is written to `<folder>/.conformance.json`. **Re-run after any edit** — the persisted
cert describes the code that was certified, not the code on disk now.

Exit codes: `0` certified · `1` setup failed or not certified · `2` usage / malformed folder.

## Connector folder shape

```
my-connector/
  CAPABILITY.md      # frontmatter: interface: transport, backend, roles, requires.env, style{}, …
  package.json       # "type": "module"; deps: @modelcontextprotocol/sdk, tsx
  package-lock.json  # committed (deterministic npm ci); node_modules is gitignored
  scripts/setup.sh   # npm ci || npm install  (folder-local node_modules)
  src/server.ts      # the connector — imports @modelcontextprotocol/sdk
  .mcp.json          # { "command": "npx", "args": ["tsx", "src/server.ts"] }
```

`certify` installs the folder-local dependencies for you before the run. See the
[`transports/CONTRACT.md`](https://github.com/okets/my-agent-extensions/blob/main/transports/CONTRACT.md)
in the extensions repo for the full connector contract, and
[`transports/examples/`](https://github.com/okets/my-agent-extensions/tree/main/transports/examples)
for worked connectors you can adapt.

## Programmatic use

The harness, wire-protocol types, and manifest validator are also exported for use in code:

```ts
import {
  runTransportConformance,
  validateTransportManifest,
  deriveConnectorLaunch,
  runConnectorSetup,
  // wire-protocol contract: SEND_TOOL, DRAIN_TOOL, HEALTH_TOOL, WireInbound, MediaHandle, …
} from 'my-agent-transport-kit'
```

## How this is built

The published package is a **self-contained bundle**: the conformance harness, wire types, and
manifest validator are inlined from the my-agent framework core at build time, so the package has
no `@my-agent/core` runtime dependency and runs anywhere via `npx`. `dist/` is prebuilt and
published from the framework monorepo; the source of truth for what it certifies is the framework's
own conformance suite.

## License

MIT
