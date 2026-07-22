#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tool_manifest="$root/tools/worker-build-0.8.1/Cargo.toml"

# The source and lockfile are vendored so the generated WASM is not dependent
# on whichever incompatible worker-build happens to be installed globally.
if ! grep -q '^version = "0.8.1"$' "$tool_manifest"; then
  printf 'broker-workers requires the pinned worker-build 0.8.1 source\n' >&2
  exit 1
fi
# getrandom 0.3 requires both the wasm_js feature (enabled by broker-workers)
# and an explicit backend cfg for wasm32-unknown-unknown.
export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS="${CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS:+$CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS }--cfg getrandom_backend=\"wasm_js\""
exec cargo run --locked --offline --manifest-path "$tool_manifest" \
  --bin worker-build -- --release "$root" "$@"
