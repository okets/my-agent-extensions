/*
 * Transport connector — skeleton.
 *
 * SELF-CONTAINED: import ONLY @modelcontextprotocol/sdk (folder-local node_modules) and inline the
 * wire-tool strings — never import a framework module. Read ../CONTRACT.md for the full contract.
 *
 * stdout (fd 1) IS the MCP JSON-RPC channel: NEVER console.log — log to stderr (console.error).
 *
 * This skeleton spawns and answers the wire, but it is NOT a finished connector: fill in every TODO
 * (connect to your provider, real inbound with monotonic cursors, real outbound). It will not
 * certify until you do. Copy a connector from ../examples/ for a working reference.
 */
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'

// The provider base URL MUST be env-overridable so the conformance harness can steer it at a stub.
const API_BASE = process.env.TODO_PROVIDER_API_BASE ?? 'https://api.example.com'

// Connector-owned, strictly-increasing, monotonic cursor. Mint AT PUSH TIME inside enqueue().
let cursor = 0
const backlog: Array<{
  cursor: string
  from: string
  content: string
  timestamp: number
}> = []
function enqueue(from: string, content: string) {
  cursor += 1 // one nextCursor() caller, inside the push — cursor-order MUST equal push-order
  backlog.push({
    cursor: String(cursor),
    from,
    content,
    timestamp: Date.now(),
  })
  // TODO: emit notifications/transport/message_available as a wakeup (optional — a dropped
  // notification just means re-drain; the framework's durable cursor is the source of truth).
}

const mcp = new Server({ name: 'my-connector', version: '1.0.0' }, { capabilities: { tools: {} } })

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'transport/send',
      description: 'Transmit one outbound message',
      inputSchema: { type: 'object' },
    },
    {
      name: 'transport/drain',
      description: 'Return inbound messages since a cursor',
      inputSchema: { type: 'object' },
    },
    // Add transport/health for a persistent upstream (socket/poll/push) — required at cert for those.
  ],
}))

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args = {} } = req.params
  switch (name) {
    case 'transport/send': {
      // args: { to, content, media? }. TODO: transmit to your provider. Fetch media bytes
      // out-of-band from MEDIA_ENDPOINT by handle — NEVER put bytes in the wire JSON.
      void API_BASE
      return { content: [{ type: 'text', text: 'sent' }] }
    }
    case 'transport/drain': {
      // args: { sinceCursor }. Return entries with cursor > sinceCursor, non-consuming.
      const since = Number((args as { sinceCursor?: string }).sinceCursor ?? 0)
      const messages = backlog.filter((m) => Number(m.cursor) > since)
      return {
        content: [{ type: 'text', text: JSON.stringify({ messages }) }],
      }
    }
    default:
      throw new Error(`unknown tool: ${name}`)
  }
})

await mcp.connect(new StdioServerTransport())
console.error('[my-connector] up (skeleton — fill in the TODOs)')
