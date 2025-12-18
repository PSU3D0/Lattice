#!/usr/bin/env bash
set -euo pipefail

cargo run -p flows-cli -- bindings lock generate --example s4_preflight --out examples/s4_preflight/bindings.lock.json --bind resource::kv=memory
