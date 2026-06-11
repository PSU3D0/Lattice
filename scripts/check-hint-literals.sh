#!/usr/bin/env bash
# Grep gate (packet A1): `"resource::` string literals are only allowed in the
# module that defines the typed hint vocabulary, dag_core::EffectHint.
#
# Everything else must obtain canonical hint strings through
# `dag_core::EffectHint::as_str()` (or the capability `HINT_*` constants that
# are derived from it), so the macro layer, kernel-plan validation, and host
# preflight can never drift apart on hint spelling. Tests that intentionally
# exercise unknown-hint handling build their typo strings via runtime concat
# (e.g. `["resource", "::http_raed"].concat()`) instead of literals.
#
# Run via `mise run hint-gate` or `bash scripts/check-hint-literals.sh`.
set -euo pipefail

cd "$(dirname "$0")/.."

PATTERN='"resource::'

# The single module allowed to define the canonical literals.
DEFINING_MODULE='crates/dag-core/src/effect_hint.rs'

# Allowlist: files with KNOWN literals that are outside packet A1's edit scope
# or intentionally exercise raw hint strings. Each entry carries the follow-up
# owner. Shrink this list; never grow it without a packet note.
ALLOWLIST=(
  # flows-cli: --bind parsing & docs; owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/src/main.rs'
  'crates/cli/tests/bundle.rs'
  'crates/cli/tests/bindings_lock.rs'
  'crates/cli/tests/run_local.rs'
  # dag-macros golden tests pin canonical emission strings; convert to
  # capabilities::*::HINT_* consts in a macro-test cleanup packet.
  'crates/dag-macros/tests/flow_macro.rs'
  'crates/dag-macros/tests/node_hints.rs'
  # kernel-exec in-module test constant; outside A1 scope.
  'crates/kernel-exec/src/lib.rs'
  # flow-bundle manifest fixture uses `"kind": "resource::dedupe"` (manifest
  # capability kind field, a different surface from NodeIR hints).
  'crates/flow-bundle/src/lib.rs'
)

is_allowed() {
  local file="$1"
  [[ "$file" == "$DEFINING_MODULE" ]] && return 0
  for entry in "${ALLOWLIST[@]}"; do
    [[ "$file" == "$entry" ]] && return 0
  done
  return 1
}

violations=0
while IFS= read -r line; do
  file="${line%%:*}"
  if ! is_allowed "$file"; then
    if [[ $violations -eq 0 ]]; then
      echo "hint-gate: \"resource:: string literals found outside ${DEFINING_MODULE}:" >&2
    fi
    echo "  $line" >&2
    violations=$((violations + 1))
  fi
done < <(
  grep -RIn --include='*.rs' \
    --exclude-dir=target --exclude-dir=.sessions --exclude-dir=node_modules \
    -F "$PATTERN" \
    crates examples connectors 2>/dev/null || true
)

if [[ $violations -gt 0 ]]; then
  echo "hint-gate: FAIL ($violations literal(s)). Emit hints via dag_core::EffectHint::as_str()" >&2
  echo "or the capability HINT_* constants; see impl-docs/error-codes.md (EFFECT202)." >&2
  exit 1
fi

echo "hint-gate: OK (no stray \"resource:: literals)"
