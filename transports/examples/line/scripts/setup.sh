#!/usr/bin/env bash
# Folder-local dependency install (the browser-chrome / desktop-x11 pattern; M10-S2 decision).
# NODE_PATH is dead for ESM — @modelcontextprotocol/sdk resolves from THIS folder's node_modules via
# Node/tsx's normal upward filesystem walk. No node_modules may sit in any ancestor of the folder.
set -euo pipefail
cd "$(dirname "$0")/.."

# Prefer the deterministic `npm ci` when a lockfile is present; fall back to `npm install`
# (first author / no lockfile). Either way, deps land in this folder's own node_modules.
if [ -f package-lock.json ]; then
  npm ci || npm install
else
  npm install
fi

echo "line connector: folder-local deps installed."
