#!/usr/bin/env bash
# ensure-worker-build.sh
#
# Guard wrapper invoked by wrangler `[build] command` (and the workerd-tests
# npm `build` script). It makes `worker-build` available exactly once instead of
# running `cargo install` on every `wrangler dev`/`deploy`/test invocation, then
# delegates to it.
#
# Behaviour:
#   - If `worker-build` is already on PATH and satisfies REQUIRED_WORKER_BUILD,
#     it is used as-is (no network, no compile).
#   - Otherwise, and ONLY in an environment that opts in (CI=1 or
#     LATTICE_AUTO_INSTALL_WORKER_BUILD=1), it runs `cargo install` once.
#   - Otherwise it fails fast with a one-line install instruction, so a fresh
#     contributor sees exactly one obvious extra step rather than a silent
#     multi-minute rebuild on every command.
#
# Any arguments are forwarded to `worker-build` (default: --release).
set -euo pipefail

# Minimum acceptable worker-build version. Bump deliberately.
REQUIRED_WORKER_BUILD="${REQUIRED_WORKER_BUILD:-0.7.0}"
# The version cargo installs when we need to install.
INSTALL_WORKER_BUILD="${INSTALL_WORKER_BUILD:-^0.7}"

log() { printf '[ensure-worker-build] %s\n' "$*" >&2; }

# Compare two dotted versions: returns 0 if $1 >= $2.
version_ge() {
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]
}

have_acceptable_worker_build() {
  command -v worker-build >/dev/null 2>&1 || return 1
  local v
  v="$(worker-build --version 2>/dev/null | awk '{print $NF}')" || return 1
  [ -n "$v" ] || return 1
  version_ge "$v" "$REQUIRED_WORKER_BUILD"
}

if ! have_acceptable_worker_build; then
  if [ "${CI:-}" = "true" ] || [ "${CI:-}" = "1" ] || [ "${LATTICE_AUTO_INSTALL_WORKER_BUILD:-}" = "1" ]; then
    log "worker-build missing or older than ${REQUIRED_WORKER_BUILD}; installing worker-build@${INSTALL_WORKER_BUILD} (one-time)"
    cargo install -q "worker-build@${INSTALL_WORKER_BUILD}"
  else
    log "worker-build (>= ${REQUIRED_WORKER_BUILD}) not found on PATH."
    log "Install it once with:"
    log "    cargo install worker-build@${INSTALL_WORKER_BUILD}"
    log "Or re-run with LATTICE_AUTO_INSTALL_WORKER_BUILD=1 to auto-install."
    exit 1
  fi
fi

if [ "$#" -eq 0 ]; then
  exec worker-build --release
else
  exec worker-build "$@"
fi
