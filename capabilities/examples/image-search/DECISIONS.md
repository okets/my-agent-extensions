# Image-search capability — design decisions

> The design record for the image-search capability: why it is shaped the way it is — its
> provider strategy, its keyless-by-default posture, its output contract, and its certification
> discipline. This record exists so future builders (of this capability or of any analogous
> "capability calls a web API and hands results back to the framework" capability) can see the
> reasoning, not just the resulting code.

## Provenance & build discipline

Authored by the Working Agent during its capability-build loop, under **build-not-copy**
discipline: the capability was authored against the `image-search` well-known type and the
framework's script-capability contract, then **certified offline before it was enabled**. The
`.enabled` activation marker is written LAST, only after certification passes — a broken capability
never goes live. (That activation marker is per-install runtime state and is intentionally not part
of this shared example.)

## The framework contract this capability fills

A `provides: image-search` script capability has ONE job: given a text query, **call a search API
and return candidate image *metadata*** (URLs, titles, dimensions, license) as JSON on stdout. It
does **not** download images. The framework fetches the chosen image from the returned URL. This
split is the whole reason the capability is a tiny stateless script and not an MCP server: no state,
no binary I/O over the wire, one clean responsibility.

## D1 — Two providers: a keyed primary and a keyless fallback

- **Google Programmable Search (CSE)** is the primary when `GOOGLE_CSE_API_KEY` and `GOOGLE_CSE_ID`
  are present in the environment — broadest coverage, free tier ~100 queries/day.
- **Openverse** (`https://api.openverse.org/v1/images/`) is the fallback: free, keyless, CC-licensed
  results, stable public API. It runs automatically when the Google keys are absent, when Google
  errors, or when Google returns zero results.

The fallback chain is the load-bearing idea. It means the capability is **useful the instant it's
installed** (Openverse needs no setup) and **upgrades itself** the moment a user adds Google keys —
no code change, no re-build. Adapt this shape to whatever providers fit your user: a keyed
"best-quality" primary + a keyless "always-works" fallback is a pattern worth stealing.

## D2 — `requires.env` is EMPTY (keyless by default)

`requires.env: []`. This is deliberate. Declaring the Google keys as *required* would mark the
capability degraded until a user pastes an API key — a bad first impression for an ability that
works fine keyless. Instead the keys are read **opportunistically** from the environment: absent →
Openverse; present → Google-then-Openverse. An empty `requires.env` on a capability with an optional
premium provider is the honest declaration: nothing is required to be useful.

## D3 — Output contract: single-line JSON, metadata only, honest exit codes

`search.sh <query> [count]` prints one line of JSON to stdout:

```json
{ "results": [ { "url": "…", "title": "…", "thumbnail": "…", "width": 1200, "height": 800, "source": "…", "license": "cc-by" } ] }
```

- Exit **0** on success — *including zero results* (an empty `results` array is a valid answer, not a
  failure). The framework decides what to do with an empty list.
- Exit **1** only on a real error (both providers unreachable/unusable), with a human-readable message
  on **stderr** and no JSON on stdout. Keeping errors off stdout means the framework never has to
  parse an error as if it were a result.

`count` is clamped to 1–10 (Google CSE's per-request ceiling) so a caller can't ask for a page size
the upstream will reject.

## D4 — Certification is OFFLINE and structural, never a live provider round-trip

The capability certifies by validating its **contract shape** offline — canned input in, assert the
output is well-formed JSON with a `results` array — rather than by curling a live provider with a
real key. A cert that hit the network would be non-deterministic (rate limits, outages, key state)
and would leak credentials into the test path. `smoke.sh` is the *live* health check (it really
queries Openverse for "cat" and asserts a `results` array); the framework cert is the *offline*
one. Keep those two separate.

## Honest degrade

If both providers fail, the script exits 1 with a plain message — it does not fabricate results or
hang. The brain still has its own image-fetch safety net (e.g. a browser fallback), so a failed
search degrades to "I couldn't find one that way," never to a crash.
