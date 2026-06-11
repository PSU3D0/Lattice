//! Golden-fixture tests for the static FlowRequirements manifest (packet C1).
//!
//! The fixtures under `tests/fixtures/` are handwritten expected manifests
//! for representative example flows. They double as the seed goldens for the
//! `flows bundle requirements` CLI command (packet C3): the CLI must emit
//! byte-equivalent JSON (modulo formatting) for the same flows.
//!
//! If one of these tests fails after an intentional IR/metadata change,
//! update the fixture by hand and record the requirement delta in the packet
//! report — the diff IS the review surface.

use kernel_plan::{derive_requirements, validate};

fn assert_matches_fixture(flow: &dag_core::FlowIR, fixture: &str) {
    let ir = validate(flow).expect("flow should validate");
    let requirements = derive_requirements(&ir);
    let actual = serde_json::to_value(&requirements).expect("serialize requirements");
    let expected: serde_json::Value =
        serde_json::from_str(fixture).expect("fixture should be valid JSON");
    assert_eq!(
        actual,
        expected,
        "derived requirements drifted from golden fixture;\nactual:\n{}",
        serde_json::to_string_pretty(&actual).expect("pretty actual"),
    );
}

#[test]
fn s1_echo_requirements_match_golden() {
    assert_matches_fixture(
        &example_s1_echo::flow(),
        include_str!("fixtures/s1_echo.requirements.json"),
    );
}

#[test]
fn s12_sheetport_quote_bound_requirements_match_golden() {
    assert_matches_fixture(
        &example_s12_sheetport_quote::bound_flow(),
        include_str!("fixtures/s12_sheetport_quote_bound.requirements.json"),
    );
}

#[test]
fn s12_sheetport_quote_internal_requirements_match_golden() {
    assert_matches_fixture(
        &example_s12_sheetport_quote::internal_flow(),
        include_str!("fixtures/s12_sheetport_quote_internal.requirements.json"),
    );
}

#[test]
fn requirements_manifest_round_trips_via_schema_types() {
    let ir = validate(&example_s12_sheetport_quote::bound_flow()).expect("validate");
    let requirements = derive_requirements(&ir);
    let json = serde_json::to_value(&requirements).expect("serialize");
    let back: dag_core::FlowRequirements = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, requirements);
}
