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
  # T4: run_schedule.rs authors a bindings.lock fixture for the s15 cron-canary
  # golden; the `resource::http`/`resource::kv` strings are lock manifest
  # `provides`/`use` keys (same surface as bindings_lock.rs above), not NodeIR
  # effect-hint literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_schedule.rs'
  # N3 (pilot clone): run_schedule_s16.rs authors the s16 multi-connector
  # bindings.lock fixture; the `resource::http`/`resource::kv` strings are the
  # same lock manifest `provides`/`use` keys as run_schedule.rs above, not
  # NodeIR effect-hint literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_schedule_s16.rs'
  # N4-T9 (clone): run_schedule_s19.rs authors the s19 sheets+gmail
  # bindings.lock fixture; the `resource::http`/`resource::kv` strings are the
  # same lock manifest `provides`/`use` keys as run_schedule_s16.rs above, not
  # NodeIR effect-hint literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_schedule_s19.rs'
  # N4-T6 (clone): run_local_s17.rs authors the s17 sheets+telegram
  # bindings.lock fixture for the manual-trigger golden; the
  # `resource::http`/`resource::kv` strings are the same lock manifest
  # `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR effect-hint
  # literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s17.rs'
  # N4-T10 (clone): run_local_s20_form_signup_notify.rs authors the s20
  # sheets+slack bindings.lock fixture for the webhook-trigger golden; the
  # `resource::http` strings are the same lock manifest `provides`/`use` keys
  # as run_schedule_s16.rs above, not NodeIR effect-hint literals. Owned by the
  # CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s20_form_signup_notify.rs'
  # N4-T7 (clone): run_local_s18.rs authors the s18 airtable+sheets+notion
  # bindings.lock fixture for the webhook-trigger golden; the
  # `resource::http`/`resource::kv` strings are the same lock manifest
  # `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR effect-hint
  # literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s18.rs'
  # H4 (connector.http acceptance): run_local_s26_http_longtail.rs authors the
  # s26 connector.http bindings.lock fixture for the run + render goldens; the
  # `resource::http`/`resource::kv` strings are the same lock manifest
  # `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR effect-hint
  # literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s26_http_longtail.rs'
  # N4-T3 (clone): run_local_s22.rs authors the s22 sheets+llm+slack
  # bindings.lock fixture for the webhook-trigger golden; the
  # `resource::http`/`resource::kv` strings are the same lock manifest
  # `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR effect-hint
  # literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s22.rs'
  # N4-T5 (clone): run_local_s25.rs authors the s25 sheets+llm+gmail
  # bindings.lock fixture for the manual-trigger golden; the
  # `resource::http`/`resource::kv` strings are the same lock manifest
  # `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR effect-hint
  # literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s25.rs'
  # N4-T2 (clone): run_local_s21.rs authors the s21 llm+sheets+gmail
  # bindings.lock fixture for the AI-CV-screening webhook golden; the
  # `resource::http`/`resource::kv` strings are the same lock manifest
  # `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR effect-hint
  # literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s21.rs'
  # N4-T4 (clone): run_local_s23_crm_event_notify.rs authors the s23
  # sheets+gmail+slack bindings.lock fixture for the webhook CRM-event-router
  # golden; the `resource::http`/`resource::kv` strings are the same lock
  # manifest `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR
  # effect-hint literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s23_crm_event_notify.rs'
  # N4-T8 (clone): run_local_s24.rs authors the s24 hunter+sheets+gmail+discord
  # bindings.lock fixture for the lead-intake webhook golden; the
  # `resource::http`/`resource::kv` strings are the same lock manifest
  # `provides`/`use` keys as run_schedule_s16.rs above, not NodeIR effect-hint
  # literals. Owned by the CLI packet (LOCK-BUILD/cli).
  'crates/cli/tests/run_local_s24.rs'
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
