#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tests="$root/workerd-tests"
rm -rf "$root/build" "$root/build-test" "$tests/build" "$tests/build-production"
bash "$root/scripts/build.sh"
cp -R "$root/build" "$tests/build-production"
cp "$root/deploy/public-callback/src/index.mjs" "$tests/build-production/public.mjs"
# Keep the production build at build/. The explicitly named fixture artifact is
# produced separately and copied only to the Miniflare fixture directory.
mv "$root/build" "$root/build-production-tmp"
bash "$root/scripts/build.sh" --features test-fixtures
mv "$root/build" "$root/build-test"
mv "$root/build-production-tmp" "$root/build"
cp -R "$root/build-test" "$tests/build"
cp "$root/deploy/public-callback/src/index.mjs" "$tests/build/public.mjs"
