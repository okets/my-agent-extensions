/**
 * WhatsApp connector outbound LINK PREVIEW builder.
 *
 * When an outgoing text contains a URL, build a rich preview (title, description, small JPEG
 * thumbnail) and hand it to Baileys as `sendMessage(to, { text, linkPreview })`. Passing
 * `linkPreview` explicitly (an object, or `null`) means Baileys does NOT run its own default
 * fetch — so the timeout, the SSRF guard and the fallback below are the ones that apply.
 *
 * GUARANTEES
 *   - NEVER throws. Any failure (site blocks us, no title, bad image, missing package, timeout)
 *     resolves to `null` → the caller sends plain text with no preview.
 *   - NEVER delays the send past `overallTimeoutMs` (hard Promise.race over the whole build).
 *   - Degrades in steps: no picture (slow/failed thumbnail) → title+description card → plain text.
 *   - Only public http(s) hosts are fetched: loopback / private / link-local addresses are refused
 *     (SSRF guard on the first URL, on every redirect hop and on the thumbnail URL).
 *   - Redirects are followed ACROSS hosts (www.cnn.com → edition.cnn.com) but only up to
 *     MAX_REDIRECTS hops, only to http(s), and every hop is re-checked before it is requested.
 *   - Thumbnails use `sharp` only (Baileys' peer dependency, already resolved in the lockfile) — resize to a small JPEG. No other image dep.
 *
 * link-preview-js and sharp are imported lazily and are injectable, so the logic is unit-tested
 * without network (link-preview.test.ts).
 */
import { lookup } from 'node:dns/promises'
import { isIP } from 'node:net'

export interface BuiltLinkPreview {
  'canonical-url': string
  'matched-text': string
  title: string
  description?: string
  jpegThumbnail?: Buffer
}

export interface LinkPreviewDeps {
  /** link-preview-js `getLinkPreview` (injectable for tests). */
  getLinkPreview?: (url: string, opts: Record<string, unknown>) => Promise<unknown>
  /** Returns a small JPEG for the given image bytes (injectable for tests). */
  makeThumbnail?: (bytes: Buffer) => Promise<Buffer>
  /** Fetch used for the thumbnail image (injectable for tests). */
  fetchImpl?: typeof fetch
  /** Resolve a hostname to an IP address (injectable for tests). */
  resolveHost?: (hostname: string) => Promise<string>
  /** Warn sink (stderr). */
  warn?: (msg: string) => void
}

export interface LinkPreviewOptions {
  /** Hard ceiling on the WHOLE build (page fetch + thumbnail). Default 10000 ms. */
  overallTimeoutMs?: number
  /** Timeout for the page fetch. Default 8000 ms. */
  pageTimeoutMs?: number
  /** Timeout for the thumbnail image fetch. Default 4000 ms (also capped by what is left of the overall budget). */
  imageTimeoutMs?: number
  /** Thumbnail width in px. Default 192 (WhatsApp's own link-thumbnail size). */
  thumbnailWidth?: number
  /** Max image bytes we will download for a thumbnail. Default 3 MB. */
  maxImageBytes?: number
}

const URL_REGEX = /https?:\/\/[^\s<>"')\]]+/i
const BROWSER_UA =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 WhatsApp/2.24'

/** First http(s) URL in a text, with trailing sentence punctuation trimmed. `undefined` if none. */
export function extractFirstUrl(text: string): string | undefined {
  const m = URL_REGEX.exec(text)
  if (!m) return undefined
  return m[0].replace(/[.,;:!?*_~`]+$/, '')
}

/** True for loopback / private / link-local / unspecified / unique-local addresses. */
export function isPrivateAddress(addr: string): boolean {
  let a = addr.trim().toLowerCase()
  if (a.startsWith('[') && a.endsWith(']')) a = a.slice(1, -1)
  if (a.startsWith('::ffff:')) a = a.slice(7) // IPv4-mapped IPv6
  const kind = isIP(a)
  if (kind === 4) {
    const [p, q] = a.split('.').map(Number)
    return (
      p === 0 || p === 10 || p === 127 ||
      (p === 100 && q >= 64 && q <= 127) ||
      (p === 169 && q === 254) ||
      (p === 172 && q >= 16 && q <= 31) ||
      (p === 192 && q === 168) ||
      p >= 224
    )
  }
  if (kind === 6) {
    return a === '::' || a === '::1' || a.startsWith('fc') || a.startsWith('fd') || /^fe[89ab]/.test(a)
  }
  return false
}

async function defaultResolveHost(hostname: string): Promise<string> {
  if (isIP(hostname)) return hostname
  const r = await lookup(hostname)
  return r.address
}

async function assertPublicUrl(url: string, resolveHost: (h: string) => Promise<string>): Promise<void> {
  const u = new URL(url)
  if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new Error('not http(s)')
  const host = u.hostname.replace(/^\[|\]$/g, '')
  if (host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local') || host.endsWith('.internal')) {
    throw new Error('refused: local host')
  }
  const addr = await resolveHost(host)
  if (isPrivateAddress(addr)) throw new Error('refused: private address')
}

/** Max redirects followed for the page (cross-host allowed). The first request does not count. */
export const MAX_REDIRECTS = 5

/**
 * Fetch the page with `getLinkPreview`, following up to MAX_REDIRECTS redirects, ACROSS hosts
 * (www.cnn.com → edition.cnn.com, http → https, …).
 *
 * link-preview-js 3.2.0 follows at most ONE redirect hop itself (it fetches the forwarded URL once
 * and parses whatever comes back), and calls `handleRedirects(base, forwarded)` for that hop. To
 * support a longer chain with a hard cap we do the following loop: `handleRedirects` records the
 * `Location` target (after refusing non-http(s) schemes) and returns false, which makes the library
 * stop with "could not handle redirect"; we then start a fresh `getLinkPreview` on the recorded target.
 * Every hop therefore starts with the library's own `resolveDNSHost` check (private / loopback /
 * link-local refused) AND our `assertPublicUrl` (scheme + local-name + address) before any request.
 * The page budget is shared by all hops (each hop gets what is left of it).
 */
async function fetchPageWithRedirects(
  startUrl: string,
  getLinkPreview: NonNullable<LinkPreviewDeps['getLinkPreview']>,
  resolveHost: (h: string) => Promise<string>,
  pageTimeoutMs: number,
): Promise<unknown> {
  const pageDeadline = Date.now() + pageTimeoutMs
  let current = startUrl
  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    if (hop > 0) await assertPublicUrl(current, resolveHost) // hop 0 is covered by resolveDNSHost below
    const left = pageDeadline - Date.now()
    if (left <= 0) throw new Error('page budget used up while following redirects')
    const handBack: { next?: string } = {} // set by handleRedirects (object, so control-flow typing stays honest)
    try {
      return await getLinkPreview(current, {
        timeout: left,
        followRedirects: 'manual',
        handleRedirects: (_base: string, forwarded: string) => {
          // Throws (→ preview unavailable) on anything that is not a plain http(s) URL.
          const u = new URL(forwarded)
          if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new Error('refused: redirect to non-http(s) scheme')
          handBack.next = u.toString()
          return false // hand the hop back to this loop; the next iteration re-checks it
        },
        resolveDNSHost: async (u: string) => {
          const addr = await resolveHost(new URL(u).hostname)
          if (isPrivateAddress(addr)) throw new Error('refused: private address')
          return addr
        },
        headers: { 'user-agent': BROWSER_UA, 'accept-language': 'en-US,en;q=0.8' },
      })
    } catch (e) {
      if (!handBack.next) throw e // a real failure, not a redirect hand-back
      if (hop === MAX_REDIRECTS) throw new Error(`refused: more than ${MAX_REDIRECTS} redirects`)
      current = handBack.next
    }
  }
  throw new Error(`refused: more than ${MAX_REDIRECTS} redirects`) // unreachable; keeps the type checker honest
}

async function defaultMakeThumbnail(bytes: Buffer, width: number): Promise<Buffer> {
  const mod = (await import('sharp')) as unknown as { default: (b: Buffer) => any }
  return mod.default(bytes).rotate().resize({ width, withoutEnlargement: true }).jpeg({ quality: 60 }).toBuffer()
}

type Timed = <T>(key: keyof PreviewTimings, fn: () => Promise<T>) => Promise<T>

async function fetchThumbnail(
  imageUrl: string,
  deps: Required<Pick<LinkPreviewDeps, 'fetchImpl' | 'resolveHost'>> & { makeThumbnail: (b: Buffer) => Promise<Buffer> },
  opts: Required<LinkPreviewOptions>,
  timed: Timed,
  expired: () => boolean,
): Promise<Buffer | undefined> {
  if (opts.imageTimeoutMs <= 0) throw new Error('no time left')
  await timed('imageDns', () => assertPublicUrl(imageUrl, deps.resolveHost))
  const ctl = new AbortController()
  const timer = setTimeout(() => ctl.abort(), opts.imageTimeoutMs)
  try {
    const bytes = await timed('imageDownload', async () => {
      const res = await deps.fetchImpl(imageUrl, {
        signal: ctl.signal,
        redirect: 'follow',
        headers: { 'user-agent': BROWSER_UA, accept: 'image/*' },
      })
      if (!res.ok) return undefined
      const declared = Number(res.headers.get('content-length') ?? 0)
      if (declared > opts.maxImageBytes) return undefined
      const buf = Buffer.from(await res.arrayBuffer())
      return buf.length === 0 || buf.length > opts.maxImageBytes ? undefined : buf
    })
    if (!bytes || expired()) return undefined
    return await timed('sharp', () => deps.makeThumbnail(bytes))
  } finally {
    clearTimeout(timer)
  }
}

/** Per-stage wall-clock timings (ms) of one preview build; a missing key = that stage never started/finished. */
export interface PreviewTimings {
  /** lazy import of link-preview-js */
  load?: number
  /** page fetch + parse (link-preview-js), includes its DNS check and redirects */
  page?: number
  /** thumbnail: DNS check of the image host */
  imageDns?: number
  /** thumbnail: image download */
  imageDownload?: number
  /** thumbnail: sharp resize */
  sharp?: number
}

function fmtTimings(t: PreviewTimings, stage: string | undefined): string {
  const parts = (['load', 'page', 'imageDns', 'imageDownload', 'sharp'] as const)
    .map((k) => (t[k] === undefined ? undefined : `${k}=${t[k]}ms`))
    .filter(Boolean)
  return `${parts.join(' ') || 'no stage finished'}${stage ? `; still running: ${stage}` : ''}`
}

/** Pre-load link-preview-js and sharp (native module) so the first real preview doesn't pay the cold start. Never throws. */
export async function warmLinkPreview(): Promise<void> {
  try { await import('link-preview-js') } catch { /* surfaced on first real use */ }
  try { await import('sharp') } catch { /* surfaced on first real use */ }
}

/**
 * Build a link preview for the first URL in `text`. Resolves to `null` (never rejects) when there is
 * no URL, the fetch fails or is blocked, or the page has no title.
 *
 * Degradation order: the page (title/description) is what makes a card. The thumbnail is best-effort
 * with its OWN budget (`imageTimeoutMs`, and never past the overall deadline): if it is slow or fails
 * the card is still returned, just without a picture. If the overall deadline hits after the page
 * stage finished, the title/description card is returned; only if the page stage itself is unfinished
 * does it fall back to plain text (null). Every fallback log line carries per-stage timings.
 */
export async function buildLinkPreview(
  text: string,
  deps: LinkPreviewDeps = {},
  options: LinkPreviewOptions = {},
): Promise<BuiltLinkPreview | null> {
  const opts: Required<LinkPreviewOptions> = {
    overallTimeoutMs: options.overallTimeoutMs ?? 10000,
    pageTimeoutMs: options.pageTimeoutMs ?? 8000,
    imageTimeoutMs: options.imageTimeoutMs ?? 4000,
    thumbnailWidth: options.thumbnailWidth ?? 192,
    maxImageBytes: options.maxImageBytes ?? 3 * 1024 * 1024,
  }
  const warn = deps.warn ?? ((m: string) => console.warn(m))
  const url = extractFirstUrl(text)
  if (!url) return null

  const started = Date.now()
  const timings: PreviewTimings = {}
  let stage: string | undefined
  let partial: BuiltLinkPreview | undefined // set as soon as the page stage yields a title
  const timed = async <T>(key: keyof PreviewTimings, fn: () => Promise<T>): Promise<T> => {
    const t0 = Date.now()
    stage = key
    try {
      return await fn()
    } finally {
      timings[key] = Date.now() - t0
      stage = undefined
    }
  }

  let timer: ReturnType<typeof setTimeout> | undefined
  let expired = false
  const deadline = new Promise<'deadline'>((resolve) => {
    timer = setTimeout(() => { expired = true; resolve('deadline') }, opts.overallTimeoutMs)
  })

  const work = (async (): Promise<BuiltLinkPreview | null> => {
    try {
      const resolveHost = deps.resolveHost ?? defaultResolveHost
      const getLinkPreview =
        deps.getLinkPreview ??
        (await timed('load', async () =>
          ((await import('link-preview-js')) as unknown as { getLinkPreview: NonNullable<LinkPreviewDeps['getLinkPreview']> })
            .getLinkPreview))
      const info = (await timed('page', () => fetchPageWithRedirects(url, getLinkPreview, resolveHost, opts.pageTimeoutMs))) as
        { url?: string; title?: string; description?: string; images?: string[]; mediaType?: string } | undefined

      if (!info || !info.title) return null
      const preview: BuiltLinkPreview = {
        'canonical-url': info.url || url,
        'matched-text': url,
        title: info.title,
        ...(info.description ? { description: info.description } : {}),
      }
      partial = preview
      const image = Array.isArray(info.images) ? info.images[0] : undefined
      if (image) {
        try {
          const makeThumbnail = deps.makeThumbnail ?? ((b: Buffer) => defaultMakeThumbnail(b, opts.thumbnailWidth))
          // The thumbnail never gets more than its own budget, and never runs past the overall deadline.
          const left = Math.max(0, opts.overallTimeoutMs - (Date.now() - started) - 150)
          const budget = Math.min(opts.imageTimeoutMs, left)
          const thumb = await fetchThumbnail(
            new URL(image, info.url || url).toString(),
            { fetchImpl: deps.fetchImpl ?? fetch, resolveHost, makeThumbnail },
            { ...opts, imageTimeoutMs: budget },
            timed,
            () => expired,
          )
          if (thumb && thumb.length > 0 && !expired) preview.jpegThumbnail = thumb
        } catch (e) {
          // Thumbnail is best-effort: title + description still make a useful preview.
          warn(`[whatsapp] link preview thumbnail skipped (${e instanceof Error ? e.message : String(e)}) — sending card without picture; ${fmtTimings(timings, stage)}`)
        }
      }
      return preview
    } catch (e) {
      warn(`[whatsapp] link preview unavailable (${e instanceof Error ? e.message : String(e)}) — sending plain text; ${fmtTimings(timings, stage)}`)
      return null
    }
  })()

  try {
    const r = await Promise.race([work, deadline])
    if (r === 'deadline') {
      // `work` is still running and will keep mutating `partial`; snapshot what exists NOW.
      if (partial) {
        warn(`[whatsapp] link preview hit ${opts.overallTimeoutMs}ms — sending card without picture; ${fmtTimings(timings, stage)}`)
        const { jpegThumbnail: _drop, ...card } = partial
        return card
      }
      warn(`[whatsapp] link preview timed out after ${opts.overallTimeoutMs}ms — sending plain text; ${fmtTimings(timings, stage)}`)
      return null
    }
    if (r) warn(`[whatsapp] link preview built in ${Date.now() - started}ms (${fmtTimings(timings, undefined)})${r.jpegThumbnail ? '' : ' — no picture'}`)
    return r
  } finally {
    if (timer) clearTimeout(timer)
  }
}
