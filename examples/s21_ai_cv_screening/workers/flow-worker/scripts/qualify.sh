#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

npm ci
npm audit --audit-level=moderate
npm test
node scripts/verify-package.mjs
node scripts/qualify-cloud-config.mjs

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
npx wrangler deploy --dry-run --config deploy/extraction-worker/wrangler.toml --outdir "$out/extraction"
npx wrangler deploy --dry-run --config deploy/wrangler.toml --outdir "$out/flow"
npx wrangler deploy --dry-run --config mock-provider/wrangler.toml --outdir "$out/provider"
