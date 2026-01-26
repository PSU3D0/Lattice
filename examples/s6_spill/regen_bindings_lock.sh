#!/usr/bin/env bash
set -euo pipefail

cargo run -p flows-cli -- bindings lock generate --example s6_spill --out examples/s6_spill/bindings.lock.json --bind resource::blob=memory
