//! trybuild UI diagnostic suite (packet B1-d: cost containment).
//!
//! The full suite is ~45 compile-the-world cases and dominates the
//! `cargo test -p dag-macros` wall clock (~10s of ~11s warm on a 24-core box,
//! several minutes on small CI runners). To keep the local edit→test loop cheap
//! we split it:
//!
//! * `tests/ui-smoke/` — a small representative subset (one case per distinct
//!   diagnostic family) that ALWAYS runs.
//! * `tests/ui-full/` — the remaining cases, run only when
//!   `LATTICE_TRYBUILD_FULL=1` is set in the environment (always set in CI).
//!
//! No case is ever deleted or silently truncated: when the full suite is gated
//! off, the harness prints a note counting exactly how many cases were skipped.
//! CI exercises every case by setting `LATTICE_TRYBUILD_FULL=1` on the test step.
//!
//! Invariants asserted below:
//! * smoke + full == the canonical total (`TOTAL_UI_CASES`); this catches a case
//!   being added/removed without updating the split.

/// Total number of UI `.rs` cases across both directories. Keep in sync when
/// adding/removing cases — `case_count_is_conserved` fails loudly otherwise.
const TOTAL_UI_CASES: usize = 45;

fn count_rs_cases(dir: &str) -> usize {
    std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {dir}: {e}"))
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("rs"))
        .count()
}

/// Guard against silent case loss: the two split directories must always sum to
/// the canonical total. This runs unconditionally (cheap stat-only check), so a
/// dropped or stray case is caught even on the fast default path.
#[test]
fn case_count_is_conserved() {
    let smoke = count_rs_cases("tests/ui-smoke");
    let full = count_rs_cases("tests/ui-full");
    assert_eq!(
        smoke + full,
        TOTAL_UI_CASES,
        "trybuild UI case count drifted: ui-smoke={smoke} + ui-full={full} != {TOTAL_UI_CASES}. \
         Update TOTAL_UI_CASES if this change is intentional."
    );
}

/// Smoke subset: always runs. One case per distinct diagnostic family so a
/// regression in any major macro path is caught on the fast local loop.
#[test]
fn ui_smoke() {
    let t = trybuild::TestCases::new();

    // Happy path: a minimal node! still compiles.
    t.pass("tests/ui-smoke/node_missing_name.rs");

    // Effects-conflict family (EFFECT201): effectful resource under Pure node,
    // plus the invalid-effect-value parse error.
    t.compile_fail("tests/ui-smoke/node_effect_hint_conflict.rs");
    t.compile_fail("tests/ui-smoke/node_invalid_effect.rs");

    // Determinism-conflict family (DET302).
    t.compile_fail("tests/ui-smoke/node_determinism_hint_conflict.rs");

    // Buffer/control edge errors (DAG206 missing-edge).
    t.compile_fail("tests/ui-smoke/workflow_buffer_missing_edge.rs");

    // Switch control family (distinct macro path from buffer, DAG206).
    t.compile_fail("tests/ui-smoke/workflow_switch_missing_edge.rs");

    // connect!() type mismatch (rustc E0277 surfaced through the macro).
    t.compile_fail("tests/ui-smoke/workflow_connect_requires_into.rs");

    // Registration/alias error (DAG205 duplicate alias).
    t.compile_fail("tests/ui-smoke/workflow_duplicate_alias.rs");
}

/// Full suite: the remaining cases. Gated behind `LATTICE_TRYBUILD_FULL=1`
/// (always set in CI). When unset, prints a note counting the skipped cases so
/// the omission is never silent.
#[test]
fn ui_full() {
    if std::env::var_os("LATTICE_TRYBUILD_FULL").as_deref() != Some(std::ffi::OsStr::new("1")) {
        let skipped = count_rs_cases("tests/ui-full");
        eprintln!(
            "ui_full: SKIPPING {skipped} extended trybuild UI cases \
             (set LATTICE_TRYBUILD_FULL=1 to run them; CI always does). \
             The {} smoke cases in ui_smoke still ran.",
            TOTAL_UI_CASES - skipped
        );
        return;
    }

    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui-full/node_missing_metadata.rs");
    t.compile_fail("tests/ui-full/node_db_effect_conflict.rs");
    t.compile_fail("tests/ui-full/node_queue_effect_conflict.rs");
    t.compile_fail("tests/ui-full/node_blob_determinism_conflict.rs");
    t.compile_fail("tests/ui-full/flow_enum_not_enum.rs");
    t.compile_fail("tests/ui-full/workflow_unknown_alias.rs");
    t.compile_fail("tests/ui-full/workflow_connect_requires_typed_binding.rs");
    t.compile_fail("tests/ui-full/workflow_timeout_missing_edge.rs");
    t.compile_fail("tests/ui-full/workflow_timeout_invalid_key.rs");
    t.compile_fail("tests/ui-full/workflow_delivery_missing_edge.rs");
    t.compile_fail("tests/ui-full/workflow_delivery_invalid_mode.rs");
    t.compile_fail("tests/ui-full/workflow_delivery_duplicate.rs");
    t.compile_fail("tests/ui-full/workflow_buffer_invalid_key.rs");
    t.compile_fail("tests/ui-full/workflow_buffer_duplicate.rs");
    t.compile_fail("tests/ui-full/workflow_spill_missing_edge.rs");
    t.compile_fail("tests/ui-full/workflow_spill_invalid_key.rs");
    t.compile_fail("tests/ui-full/workflow_spill_duplicate.rs");
    t.compile_fail("tests/ui-full/workflow_delivery_mode_string.rs");
    t.compile_fail("tests/ui-full/workflow_buffer_max_items_string.rs");
    t.compile_fail("tests/ui-full/workflow_spill_tier_ident.rs");
    t.compile_fail("tests/ui-full/workflow_switch_duplicate_source.rs");
    t.compile_fail("tests/ui-full/workflow_switch_invalid_key.rs");
    t.compile_fail("tests/ui-full/workflow_switch_duplicate_case_key.rs");
    t.compile_fail("tests/ui-full/workflow_switch_empty_cases.rs");
    t.compile_fail("tests/ui-full/workflow_switch_selector_pointer_non_string.rs");
    t.compile_fail("tests/ui-full/workflow_switch_case_target_not_ident.rs");
    t.compile_fail("tests/ui-full/workflow_switch_default_not_ident.rs");
    t.pass("tests/ui-full/workflow_switch_trailing_comma.rs");
    if !cfg!(feature = "flow-registry") {
        t.pass("tests/ui-full/flow_bundle_host_gated.rs");
    }

    t.compile_fail("tests/ui-full/workflow_if_missing_edge.rs");
    t.compile_fail("tests/ui-full/workflow_if_duplicate_source.rs");
    t.compile_fail("tests/ui-full/workflow_if_invalid_key.rs");
    t.compile_fail("tests/ui-full/workflow_if_selector_pointer_non_string.rs");
    t.compile_fail("tests/ui-full/workflow_if_then_not_ident.rs");
    t.compile_fail("tests/ui-full/workflow_if_else_not_ident.rs");
    t.compile_fail("tests/ui-full/workflow_if_selector_pointer_invalid.rs");
    t.pass("tests/ui-full/workflow_if_trailing_comma.rs");
}
