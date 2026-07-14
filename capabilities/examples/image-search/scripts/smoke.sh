#!/usr/bin/env bash
# smoke.sh — live health check for image-search capability
# Queries "cat", validates the JSON has a results array.
# Exit 0 = healthy, non-zero = broken.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

OUTPUT="$("$DIR/search.sh" "cat" 3)"
echo "$OUTPUT" | jq -e 'has("results") and (.results | type == "array")' > /dev/null
