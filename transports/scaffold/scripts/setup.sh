#!/usr/bin/env bash
# Folder-local dependency install (the browser-chrome pattern): node_modules lives inside this
# connector folder, never in an ancestor (NODE_PATH is a dead end for ESM). The conformance CLI
# runs this before spawning the connector.
set -euo pipefail
cd "$(dirname "$0")/.."
npm ci || npm install
