# Transport connector — scaffold

A minimal start-here skeleton. It shows the folder shape and the MCP wire tools a connector must
implement; the `TODO`s are yours to fill in. **It does not certify as-is** — it's a starting frame,
not a finished connector.

For a working, certified reference, copy the closest connector under
[`../examples/`](../examples) instead and re-derive it against the contract.

## Steps

1. Read [`../CONTRACT.md`](../CONTRACT.md) — the load-bearing rules.
2. Fill in `CAPABILITY.md` frontmatter (backend, roles, `requires.env`, `style`).
3. Implement `transport/send`, `transport/drain`, and (for a persistent upstream)
   `transport/health` in `src/server.ts`. Bytes never cross the wire — media moves by handle.
4. Certify in a clean directory:

   ```bash
   npx my-agent-transport-kit certify .
   ```

## Layout

```
scaffold/
  CAPABILITY.md      # manifest: interface: transport, backend, roles, requires.env, style{}
  package.json       # "type": "module"; deps: @modelcontextprotocol/sdk, tsx
  scripts/setup.sh   # npm ci || npm install (folder-local node_modules)
  src/server.ts      # the connector — implement the wire tools here
  .mcp.json          # { "command": "npx", "args": ["tsx", "src/server.ts"] }
```
