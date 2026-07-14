#!/usr/bin/env bash
# search.sh — image-search capability script
# Usage: search.sh <query> [count]
# Output: single-line JSON { "results": [...] } on stdout
# Errors: human message on stderr, exit 1 (no JSON)
set -euo pipefail

QUERY="${1:-}"
COUNT="${2:-}"

if [[ -z "$QUERY" ]]; then
  echo "Usage: search.sh <query> [count]" >&2
  exit 1
fi

# Read config
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$SCRIPT_DIR/../config.yaml"

DEFAULT_COUNT=5
SAFE_SEARCH="active"
if [[ -f "$CONFIG" ]]; then
  _dc="$(grep -E '^default_count:' "$CONFIG" 2>/dev/null | head -1 | sed 's/default_count:[[:space:]]*//' | tr -d '"' || true)"
  _ss="$(grep -E '^safe_search:' "$CONFIG" 2>/dev/null | head -1 | sed 's/safe_search:[[:space:]]*//' | tr -d '"' || true)"
  [[ -n "$_dc" ]] && DEFAULT_COUNT="$_dc"
  [[ -n "$_ss" ]] && SAFE_SEARCH="$_ss"
fi

[[ -z "$COUNT" ]] && COUNT="$DEFAULT_COUNT"
# Clamp to 1–10
COUNT=$(( COUNT < 1 ? 1 : COUNT > 10 ? 10 : COUNT ))

# ── Helper: Openverse fallback ────────────────────────────────────────────────
search_openverse() {
  local query="$1"
  local count="$2"
  local encoded
  encoded="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$query" 2>/dev/null || printf '%s' "$query" | sed 's/ /+/g')"

  local url="https://api.openverse.org/v1/images/?q=${encoded}&page_size=${count}&license_type=all"
  local response
  response="$(curl -sf --max-time 15 \
    -H "User-Agent: my-agent/image-search-capability" \
    "$url" 2>/dev/null)" || return 1

  # Transform Openverse response → capability format
  echo "$response" | jq -c '
    { "results": [
        .results[]? |
        {
          "url":       (.url // empty),
          "title":     (.title // ""),
          "thumbnail": (.thumbnail // ""),
          "width":     (.width // null),
          "height":    (.height // null),
          "source":    (.foreign_landing_url // ""),
          "license":   (.license // "")
        } | select(.url != null)
      ]
    }
  '
}

# ── Helper: Google CSE ────────────────────────────────────────────────────────
search_google() {
  local query="$1"
  local count="$2"
  local api_key="$3"
  local cse_id="$4"
  local encoded
  encoded="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$query" 2>/dev/null || printf '%s' "$query" | sed 's/ /+/g')"

  # Google CSE max 10 per request
  local url="https://www.googleapis.com/customsearch/v1?key=${api_key}&cx=${cse_id}&q=${encoded}&searchType=image&num=${count}&safe=${SAFE_SEARCH}"
  local response
  response="$(curl -sf --max-time 15 "$url" 2>/dev/null)" || return 1

  # Check for API error
  if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
    local errmsg
    errmsg="$(echo "$response" | jq -r '.error.message // "unknown error"')"
    echo "Google CSE error: $errmsg" >&2
    return 1
  fi

  local result_count
  result_count="$(echo "$response" | jq '.items | length' 2>/dev/null || echo 0)"
  if [[ "$result_count" -eq 0 ]]; then
    return 1  # Signal "no results" so caller falls back to Openverse
  fi

  # Transform Google response → capability format
  echo "$response" | jq -c '
    { "results": [
        .items[]? |
        {
          "url":       (.link // empty),
          "title":     (.title // ""),
          "thumbnail": (.image.thumbnailLink // ""),
          "width":     (.image.width // null),
          "height":    (.image.height // null),
          "source":    (.image.contextLink // ""),
          "license":   ""
        } | select(.url != null)
      ]
    }
  '
}

# ── Main logic ────────────────────────────────────────────────────────────────
GOOGLE_KEY="${GOOGLE_CSE_API_KEY:-}"
GOOGLE_CX="${GOOGLE_CSE_ID:-}"

RESULT=""

if [[ -n "$GOOGLE_KEY" && -n "$GOOGLE_CX" ]]; then
  # Try Google first
  if RESULT="$(search_google "$QUERY" "$COUNT" "$GOOGLE_KEY" "$GOOGLE_CX" 2>&1)"; then
    # Verify it looks like valid JSON with results
    if echo "$RESULT" | jq -e 'has("results")' > /dev/null 2>&1; then
      echo "$RESULT"
      exit 0
    fi
  fi
  # Google failed or returned zero results — fall through to Openverse
  RESULT=""
fi

# Openverse (free keyless fallback or primary when no Google keys)
if RESULT="$(search_openverse "$QUERY" "$COUNT" 2>/dev/null)"; then
  if echo "$RESULT" | jq -e 'has("results")' > /dev/null 2>&1; then
    echo "$RESULT"
    exit 0
  fi
fi

# Both failed
echo "Image search failed: both Google CSE and Openverse returned no usable results for query: $QUERY" >&2
exit 1
