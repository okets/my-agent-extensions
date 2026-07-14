---
name: image-search
provides: image-search
interface: script
multi_instance: false
interaction: tool
requires:
  env: []
---

# Image Search

Searches the web for real photos and images matching a text query. Returns candidate image
metadata (URLs, titles, dimensions) — the framework fetches the chosen image, the capability
script only calls the search API.

## Providers

- **Google Programmable Search (CSE)** — primary, when `GOOGLE_CSE_API_KEY` and `GOOGLE_CSE_ID`
  are set in the environment. Broadest coverage; free tier 100 queries/day.
- **Openverse** — free, keyless fallback. CC-licensed images. Used automatically when Google
  keys are absent or on any Google failure / zero results.

No keys are required — Openverse works out of the box.

## Usage

```bash
scripts/search.sh <query> [count]
```

- `query` — text description of what to find (e.g. `clown red nose`)
- `count` — optional max number of candidates (default: 5, max: 10)

## Output

Single-line JSON on stdout:

```json
{ "results": [ { "url": "https://…/photo.jpg", "title": "…", "thumbnail": "…", "width": 1200, "height": 800, "source": "…", "license": "cc-by" } ] }
```

Exit 0 on success (including zero results). Exit 1 on real errors (details on stderr).

## Configuration

Edit `config.yaml` to tune defaults:

```yaml
default_count: 5
safe_search: "active"
```
