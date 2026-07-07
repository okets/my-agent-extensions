#!/usr/bin/env bash
# Dependency-freshness check.
#
# WARNS when a connector's runtime dependency sits on the npm `legacy` dist-tag — the
# Baileys-6.7.23→legacy→pairing-401 class, caught proactively instead of at a dead pairing code.
# A stale/legacy protocol library is a top cause of a registration 400/401 against today's servers.
#
# WARN-ONLY (never fails) + best-effort: registry/network errors are skipped, not fatal — a connector
# must still build + certify offline. This check is deliberately OUTSIDE the offline conformance cert
# (which stays deterministic/network-free); run it at authoring/build time when the registry is reachable.
#
# Usage: check-dep-freshness.sh <connector-folder>
set -uo pipefail
FOLDER="${1:?usage: check-dep-freshness.sh <connector-folder>}"
PKG="$FOLDER/package.json"
[ -f "$PKG" ] || { echo "[dep-freshness] no package.json at $FOLDER — skipping"; exit 0; }

deps=$(node -e 'const p=JSON.parse(require("fs").readFileSync(process.argv[1],"utf8"));console.log(Object.keys(p.dependencies||{}).join("\n"))' "$PKG" 2>/dev/null) \
  || { echo "[dep-freshness] could not read package.json deps — skipping"; exit 0; }
[ -z "${deps//[$'\n']/}" ] && { echo "[dep-freshness] no runtime deps."; exit 0; }

warned=0
while IFS= read -r dep; do
  [ -z "$dep" ] && continue
  # Installed version = the folder-local node_modules (the source of truth post-setup).
  inst=$(node -e 'try{console.log(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).version)}catch(e){}' "$FOLDER/node_modules/$dep/package.json" 2>/dev/null)
  # dist-tags from the registry (network; best-effort — unreachable → skip this dep, not fatal).
  legacy=$(npm view "$dep" dist-tags.legacy 2>/dev/null) || { echo "[dep-freshness] $dep: registry unreachable — skipped"; continue; }
  latest=$(npm view "$dep" dist-tags.latest 2>/dev/null)
  if [ -n "$inst" ] && [ -n "$legacy" ] && [ "$inst" = "$legacy" ]; then
    echo "[dep-freshness] ⚠  $dep@$inst is the npm 'legacy' dist-tag (latest=${latest:-?}) — UPGRADE before debugging the wire: a stale protocol lib is a top cause of pairing/registration 400/401."
    warned=1
  fi
done <<< "$deps"

[ "$warned" = 0 ] && echo "[dep-freshness] OK — no runtime dep is on a 'legacy' dist-tag."
exit 0
