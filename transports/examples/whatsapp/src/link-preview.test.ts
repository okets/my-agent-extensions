/**
 * Logic test for the outbound link-preview builder (no network, no WhatsApp).
 *   node_modules/.bin/tsx src/link-preview.test.ts
 * Exits non-zero on the first failed assertion.
 */
import { buildLinkPreview, extractFirstUrl, isPrivateAddress, MAX_REDIRECTS } from './link-preview.js'

import { isIP } from 'node:net'

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

// ---- Redirect handling (cross-host allowed, capped, every hop guarded) ----------------------------
// Fake link-preview-js 3.2.0 behaviour: resolveDNSHost on the start URL; on a redirect response call
// handleRedirects(base, forwarded) and throw the library's real error when it returns false; it does
// NOT follow further hops itself (so a 2nd redirect would be parsed as a page — here it just throws).
function fakeLib(site: Record<string, string | { title: string }>, requested: string[]) {
  return async (u: string, o: Record<string, unknown>): Promise<unknown> => {
    u = new URL(u).toString() // the real library normalises the URL it is given (bare host → trailing slash)
    await (o.resolveDNSHost as (x: string) => Promise<string>)(u)
    requested.push(u)
    const r = site[u]
    if (r === undefined) throw new Error('404 ' + u)
    if (typeof r === 'string') {
      // like the real library, hand the handler an ABSOLUTE url (a relative Location is resolved against the request)
      if (!(o.handleRedirects as (a: string, b: string) => boolean)(u, new URL(r, u).toString())) throw new Error('link-preview-js could not handle redirect')
      throw new Error('fake lib: unexpected accepted redirect (3.2.0 would fetch once more and parse)')
    }
    return { url: u, title: r.title, images: [] }
  }
}
const hostIp = async (h: string) => (isIP(h) ? h : h === 'metadata.example' ? '169.254.169.254' : ['localhost', 'intranet.example'].includes(h) ? '127.0.0.1' : '93.184.216.34')

// Today's failure reproduced: www.cnn.com → edition.cnn.com (different host) must now give a card.
{
  const requested: string[] = [], logs: string[] = []
  const lib = fakeLib({ 'https://www.cnn.com/': 'https://edition.cnn.com/', 'https://edition.cnn.com/': { title: 'CNN' } }, requested)
  const p = await buildLinkPreview('https://www.cnn.com', { getLinkPreview: lib, resolveHost: hostIp, warn: (m) => logs.push(m) })
  check('redirect: cross-host public redirect (www.cnn.com → edition.cnn.com) → card', p?.title === 'CNN' && p['canonical-url'] === 'https://edition.cnn.com/' && requested.length === 2, logs.join('|'))
}
// Multi-hop chain within the cap (http → https → other host → final): 3 redirects.
{
  const requested: string[] = []
  const lib = fakeLib({ 'http://a.example/': 'https://a.example/', 'https://a.example/': 'https://www.b.example/x', 'https://www.b.example/x': 'https://c.example/y', 'https://c.example/y': { title: 'Final' } }, requested)
  const p = await buildLinkPreview('http://a.example/', { getLinkPreview: lib, resolveHost: hostIp, ...quiet })
  check('redirect: chain of 3 hops across hosts → card', p?.title === 'Final' && requested.length === 4)
}
// Relative Location headers (path-absolute and path-relative) are resolved against the requesting URL.
{
  const requested: string[] = []
  const lib = fakeLib({ 'https://rel.example/a/b': '/news/today', 'https://rel.example/news/today': 'next', 'https://rel.example/news/next': { title: 'Rel' } }, requested)
  const p = await buildLinkPreview('https://rel.example/a/b', { getLinkPreview: lib, resolveHost: hostIp, ...quiet })
  check('redirect: relative Location headers resolved → card', p?.title === 'Rel' && requested.length === 3, requested.join(','))
}
// A private target reached at a LATER hop (not the first) is refused too, and never requested.
{
  const requested: string[] = [], logs: string[] = []
  const lib = fakeLib({ 'https://a.example/': 'https://b.example/', 'https://b.example/': 'https://c.example/', 'https://c.example/': 'http://169.254.169.254/latest', 'http://169.254.169.254/latest': { title: 'SECRET' } }, requested)
  const p = await buildLinkPreview('https://a.example/', { getLinkPreview: lib, resolveHost: hostIp, warn: (m) => logs.push(m) })
  check('redirect: private address on hop 3 → refused, never requested', p === null && !requested.includes('http://169.254.169.254/latest') && requested.length === 3 && logs.some((l) => l.includes('refused')), `${requested.join(',')} | ${logs.join('|')}`)
}
// Exactly at the cap (5 redirects) still works; 6 is refused.
{
  const mk = (n: number) => { const s: Record<string, string | { title: string }> = {}; for (let i = 0; i < n; i++) s[`https://h${i}.example/`] = `https://h${i + 1}.example/`; s[`https://h${n}.example/`] = { title: 'End' }; return s }
  const r5: string[] = [], r6: string[] = [], logs: string[] = []
  const p5 = await buildLinkPreview('https://h0.example/', { getLinkPreview: fakeLib(mk(5), r5), resolveHost: hostIp, ...quiet })
  const p6 = await buildLinkPreview('https://h0.example/', { getLinkPreview: fakeLib(mk(6), r6), resolveHost: hostIp, warn: (m) => logs.push(m) })
  check('redirect: exactly 5 redirects (the cap) → card', MAX_REDIRECTS === 5 && p5?.title === 'End' && r5.length === 6)
  check('redirect: chain longer than the cap (6) → plain text, last hop never requested', p6 === null && r6.length === 6 && !r6.includes('https://h6.example/') && logs.some((l) => l.includes('more than 5 redirects')), `${r6.length} ${logs.join('|')}`)
}
// Redirect loop → refused at the cap (no endless fetching).
{
  const requested: string[] = [], logs: string[] = []
  const p = await buildLinkPreview('https://loop.example/', { getLinkPreview: fakeLib({ 'https://loop.example/': 'https://loop2.example/', 'https://loop2.example/': 'https://loop.example/' }, requested), resolveHost: hostIp, warn: (m) => logs.push(m) })
  check('redirect: loop → plain text, bounded requests', p === null && requested.length === 6 && logs.some((l) => l.includes('more than 5 redirects')))
}
// Redirect to a loopback / private / link-local address (literal IP, and a name that resolves to one) → refused, never requested.
for (const [label, target] of [['loopback IP', 'http://127.0.0.1/admin'], ['private-resolving name', 'http://intranet.example/'], ['link-local metadata (name)', 'http://metadata.example/latest'], ['link-local IP', 'http://169.254.169.254/'], ['localhost name', 'http://localhost:8080/']] as const) {
  const requested: string[] = [], logs: string[] = []
  const lib = fakeLib({ 'https://start.example/': target, [target]: { title: 'SECRET' } }, requested)
  const p = await buildLinkPreview('https://start.example/', { getLinkPreview: lib, resolveHost: hostIp, warn: (m) => logs.push(m) })
  check(`redirect: to ${label} → refused, plain text, target never requested`, p === null && !requested.includes(target) && logs.some((l) => l.includes('refused')), `${requested.join(',')} | ${logs.join('|')}`)
}
// Redirect to a non-http scheme → refused.
for (const target of ['ftp://files.example/x', 'file:///etc/passwd', 'javascript:alert(1)', 'gopher://x.example/']) {
  const requested: string[] = [], logs: string[] = []
  const lib = fakeLib({ 'https://start.example/': target }, requested)
  const p = await buildLinkPreview('https://start.example/', { getLinkPreview: lib, resolveHost: hostIp, warn: (m) => logs.push(m) })
  check(`redirect: to ${target.split(':')[0]}: scheme → refused, plain text`, p === null && requested.length === 1 && logs.some((l) => l.includes('non-http')), `${requested.join(',')} | ${logs.join('|')}`)
}
// A redirect landing on a page whose thumbnail is private is still guarded on the thumbnail URL.
{
  let imgFetched = false
  const lib = async (u: string, o: Record<string, unknown>) => { await (o.resolveDNSHost as (x: string) => Promise<string>)(u); return { url: 'https://edition.cnn.com/', title: 'CNN', images: ['http://127.0.0.1/x.png'] } }
  const p = await buildLinkPreview('https://www.cnn.com', { getLinkPreview: lib, makeThumbnail: async () => Buffer.from('j'), fetchImpl: (async () => { imgFetched = true; return new Response(png) }) as unknown as typeof fetch, resolveHost: hostIp, ...quiet })
  check('redirect: private thumbnail on a redirected page still refused', p?.title === 'CNN' && !imgFetched && p.jpegThumbnail === undefined)
}
// The options the real library receives: manual redirect mode, our handler, DNS guard, shared page budget.
{
  let seen: Record<string, unknown> | undefined
  await buildLinkPreview('https://example.com', { getLinkPreview: async (_u, o) => { seen = o; return { url: 'https://example.com', title: 'T' } }, resolveHost: pub, ...quiet }, { pageTimeoutMs: 8000 })
  check('redirect: library gets manual mode + handleRedirects + resolveDNSHost + ≤8000ms timeout', seen?.followRedirects === 'manual' && typeof seen.handleRedirects === 'function' && typeof seen.resolveDNSHost === 'function' && typeof seen.timeout === 'number' && (seen.timeout as number) <= 8000 && (seen.timeout as number) > 7000)
}
// Source-level: the old same-host-only rule is gone.
{
  const src = (await import('node:fs')).readFileSync(new URL('./link-preview.ts', import.meta.url), 'utf8')
  check('redirect: same-host-only rule removed from source', !/'www\.' \+ b/.test(src))
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
