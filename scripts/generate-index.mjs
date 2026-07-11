// Generate transports/index.json — the ONE-fetch pool listing the framework consumes.
//
//   node scripts/generate-index.mjs          # (re)write transports/index.json
//   node scripts/generate-index.mjs --check  # exit 1 if the committed index is stale
//
// The index is a deterministic projection of each example's CAPABILITY.md
// frontmatter (no timestamps — regenerating on an unchanged tree is a no-op,
// which is what lets CI diff it). Honesty is enforced HERE too: an example
// whose `menu:` block is missing description/requirements/effort FAILS the
// generation — a dishonest or vague menu line can't enter the library.
import { readFileSync, writeFileSync, readdirSync, existsSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parse } from 'yaml'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const examplesDir = join(root, 'transports', 'examples')
const contractFile = join(root, 'transports', 'CONTRACT_VERSION')
const indexFile = join(root, 'transports', 'index.json')

const errors = []

function frontmatter(path) {
  const text = readFileSync(path, 'utf8')
  if (!text.startsWith('---\n')) return null
  const end = text.indexOf('\n---', 4)
  if (end === -1) return null
  return parse(text.slice(4, end))
}

const contract_version = readFileSync(contractFile, 'utf8').trim()
if (!/^\d+\.\d+$/.test(contract_version)) {
  console.error(`CONTRACT_VERSION malformed: "${contract_version}"`)
  process.exit(2)
}

const connectors = []
for (const entry of readdirSync(examplesDir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
  if (!entry.isDirectory()) continue
  const capPath = join(examplesDir, entry.name, 'CAPABILITY.md')
  if (!existsSync(capPath)) {
    errors.push(`${entry.name}: no CAPABILITY.md`)
    continue
  }
  const fm = frontmatter(capPath)
  if (!fm) {
    errors.push(`${entry.name}: unreadable frontmatter`)
    continue
  }
  const menu = fm.menu
  if (!menu || typeof menu !== 'object') {
    errors.push(`${entry.name}: missing menu: metadata (description/requirements/effort) — a menu line must be honest or absent`)
    continue
  }
  for (const field of ['description', 'effort']) {
    if (typeof menu[field] !== 'string' || menu[field].trim() === '')
      errors.push(`${entry.name}: menu.${field} missing or empty`)
  }
  if (!Array.isArray(menu.requirements) || menu.requirements.length === 0 || menu.requirements.some((r) => typeof r !== 'string' || r.trim() === ''))
    errors.push(`${entry.name}: menu.requirements must be a non-empty list of plain strings`)

  connectors.push({
    name: fm.name ?? entry.name,
    path: `transports/examples/${entry.name}`,
    backend: fm.backend ?? null,
    upstream: fm.upstream ?? null,
    roles: fm.roles ?? [],
    menu: {
      label: typeof menu.label === 'string' && menu.label.trim() !== '' ? menu.label.trim() : (fm.name ?? entry.name).replace(/^./, (c) => c.toUpperCase()),
      description: menu.description,
      requirements: menu.requirements,
      effort: menu.effort,
      impact: typeof menu.impact === 'number' ? menu.impact : null,
    },
    requires_env: fm.requires?.env ?? [],
  })
}

if (errors.length > 0) {
  console.error('index generation FAILED:')
  for (const e of errors) console.error(`  - ${e}`)
  process.exit(1)
}

const index = { schema: 1, contract_version, connectors }
const output = JSON.stringify(index, null, 2) + '\n'

if (process.argv.includes('--check')) {
  const committed = existsSync(indexFile) ? readFileSync(indexFile, 'utf8') : ''
  if (committed !== output) {
    console.error('transports/index.json is STALE — run: node scripts/generate-index.mjs')
    process.exit(1)
  }
  console.log(`index in sync (${connectors.length} connectors, contract ${contract_version})`)
} else {
  writeFileSync(indexFile, output)
  console.log(`wrote transports/index.json (${connectors.length} connectors, contract ${contract_version})`)
}
