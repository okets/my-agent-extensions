/**
 * Logic test for the outbound link-preview builder (no network, no WhatsApp).
 *   node_modules/.bin/tsx src/link-preview.test.ts
 * Exits non-zero on the first failed assertion.
 */
import { buildLinkPreview, extractFirstUrl, isPrivateAddress } from './link-preview.js'

let failures = 0
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) console.log(`PASS ${name}`)
  else { failures++; console.error(`FAIL ${name} ${detail}`) }
}
const quiet = { warn: () => {} }
const pub = async () => '93.184.216.34'
const good = async () => ({ url: 'https://example.com/', title: 'Example', description: 'Desc', images: ['https://example.com/i.png'] })
const png = Buffer.from('fake-image-bytes')
const okFetch = (async () => new Response(png, { status: 200 })) as unknown as typeof fetch

check('extractFirstUrl trims punctuation', extractFirstUrl('see https://a.com/x, ok') === 'https://a.com/x')
check('extractFirstUrl none', extractFirstUrl('no link here') === undefined)
check('private: 127.0.0.1', isPrivateAddress('127.0.0.1'))
check('private: 10.x / 192.168 / 172.16 / 169.254', ['10.1.1.1', '192.168.0.4', '172.20.0.1', '169.254.169.254'].every(isPrivateAddress))
check('private: ::1 / ::ffff:127.0.0.1 / fd00::1', ['::1', '::ffff:127.0.0.1', 'fd00::1'].every(isPrivateAddress))
check('public: 93.184.216.34', !isPrivateAddress('93.184.216.34'))

const p1 = await buildLinkPreview('hi https://example.com/', { getLinkPreview: good, makeThumbnail: async () => Buffer.from('jpg'), fetchImpl: okFetch, resolveHost: pub, ...quiet })
check('happy path: title/description/thumbnail', p1?.title === 'Example' && p1.description === 'Desc' && p1.jpegThumbnail?.toString() === 'jpg' && p1['matched-text'] === 'https://example.com/')

check('no url → null', (await buildLinkPreview('plain', { ...quiet })) === null)
check('fetch throws → null', (await buildLinkPreview('https://x.com', { getLinkPreview: async () => { throw new Error('blocked 403') }, ...quiet })) === null)
check('no title → null', (await buildLinkPreview('https://x.com', { getLinkPreview: async () => ({ url: 'https://x.com', images: [] }), ...quiet })) === null)

const p2 = await buildLinkPreview('https://example.com', { getLinkPreview: good, makeThumbnail: async () => { throw new Error('bad image') }, fetchImpl: okFetch, resolveHost: pub, ...quiet })
check('thumbnail failure → preview without thumbnail', p2?.title === 'Example' && p2.jpegThumbnail === undefined)

let imgFetched = false
const p3 = await buildLinkPreview('https://example.com', { getLinkPreview: async () => ({ url: 'https://example.com', title: 'T', images: ['http://127.0.0.1/x.png'] }), makeThumbnail: async () => Buffer.from('j'), fetchImpl: (async () => { imgFetched = true; return new Response(png) }) as unknown as typeof fetch, resolveHost: async (h) => (h === '127.0.0.1' ? '127.0.0.1' : '93.184.216.34'), ...quiet })
check('SSRF: private thumbnail URL never fetched', p3?.title === 'T' && !imgFetched && p3.jpegThumbnail === undefined)

// Slow thumbnail download → card WITHOUT picture, delivered near the image budget (not the whole 10 s).
{
  const logs: string[] = []
  const hang = ((_u: string, init?: { signal?: AbortSignal }) => new Promise((_r, rej) => init?.signal?.addEventListener('abort', () => rej(new Error('aborted'))))) as unknown as typeof fetch
  const t = Date.now()
  const p = await buildLinkPreview('https://example.com', { getLinkPreview: good, makeThumbnail: async () => Buffer.from('j'), fetchImpl: hang, resolveHost: pub, warn: (m) => logs.push(m) }, { imageTimeoutMs: 200, overallTimeoutMs: 5000 })
  const dt2 = Date.now() - t
  check('slow thumbnail download → title card, no picture, fast', p?.title === 'Example' && p.jpegThumbnail === undefined && dt2 < 1000, `took ${dt2}ms`)
  check('slow thumbnail log names the stage + timings', logs.some((l) => l.includes('without picture') && l.includes('page=') && l.includes('imageDns=')), logs.join('|'))
}

// Slow sharp past the overall deadline → page card returned (title kept), log says sharp was running.
{
  const logs: string[] = []
  const t = Date.now()
  const p = await buildLinkPreview('https://example.com', { getLinkPreview: good, makeThumbnail: () => new Promise(() => {}), fetchImpl: okFetch, resolveHost: pub, warn: (m) => logs.push(m) }, { overallTimeoutMs: 400, imageTimeoutMs: 5000 })
  const dt3 = Date.now() - t
  check('deadline after page stage → card without picture', p?.title === 'Example' && p.jpegThumbnail === undefined && dt3 < 900, `took ${dt3}ms`)
  check('deadline log says sharp still running', logs.some((l) => l.includes('still running: sharp')), logs.join('|'))
}

// Hung page → plain text; the log names the page stage.
{
  const logs: string[] = []
  const p = await buildLinkPreview('https://slow.example', { getLinkPreview: () => new Promise(() => {}), warn: (m) => logs.push(m) }, { overallTimeoutMs: 200 })
  check('hung page → null and log says page still running', p === null && logs.some((l) => l.includes('timed out after 200ms') && l.includes('still running: page')), logs.join('|'))
}

// Defaults are the new generous budgets.
{
  const src = (await import('node:fs')).readFileSync(new URL('./link-preview.ts', import.meta.url), 'utf8')
  check('default budgets: overall 10000 / page 8000 / image 4000', /overallTimeoutMs \?\? 10000/.test(src) && /pageTimeoutMs \?\? 8000/.test(src) && /imageTimeoutMs \?\? 4000/.test(src))
}

const t0 = Date.now()
const p4 = await buildLinkPreview('https://slow.example', { getLinkPreview: () => new Promise(() => {}), ...quiet }, { overallTimeoutMs: 300 })
const dt = Date.now() - t0
check('hung site → null within timeout', p4 === null && dt < 800, `took ${dt}ms`)

if (failures > 0) { console.error(`${failures} failure(s)`); process.exit(1) }
console.log('link-preview.test: all passed')
