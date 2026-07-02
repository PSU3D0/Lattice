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

/// Hand-built schedule (cron) flow — the T1 golden for
/// impl-docs/spec/schedule-trigger.md. Hand-built rather than macro-authored
/// so the golden also proves the manifest derivation works on IR produced
/// without dag-macros.
fn tick_report_schedule_flow() -> dag_core::FlowIR {
    use dag_core::prelude::*;
    use dag_core::{NodeKind, SchemaSpec};

    let mut builder = FlowBuilder::new(
        "tick_report_schedule",
        Version::new(1, 0, 0),
        dag_core::Profile::Web,
    );
    builder.summary(Some("Cron-fired tick fanning into a pure report node"));

    let tick_spec = dag_core::NodeSpec::inline(
        "tests::schedule::tick",
        "Tick",
        SchemaSpec::Named("ScheduledEvent"),
        SchemaSpec::Named("ScheduledEvent"),
        Effects::ReadOnly,
        Determinism::Strict,
        Some("Cron tick trigger"),
    );
    let report_spec = dag_core::NodeSpec::inline(
        "tests::schedule::report",
        "Report",
        SchemaSpec::Named("ScheduledEvent"),
        SchemaSpec::Named("TickReport"),
        Effects::Pure,
        Determinism::Strict,
        Some("Summarize the tick"),
    );

    let tick = builder.add_node("tick", &tick_spec).expect("tick");
    let report = builder.add_node("report", &report_spec).expect("report");
    builder.connect(&tick, &report);

    let mut flow = builder.build();
    if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "tick") {
        node.kind = NodeKind::Trigger;
    }
    flow.metadata.entrypoints = vec![dag_core::EntrypointMetadata {
        trigger_alias: "tick".to_string(),
        capture_alias: "report".to_string(),
        route_path: None,
        method: None,
        route_aliases: Vec::new(),
        schedule: Some("*/5 * * * *".to_string()),
    }];
    flow
}

#[test]
fn tick_report_schedule_requirements_match_golden() {
    assert_matches_fixture(
        &tick_report_schedule_flow(),
        include_str!("fixtures/tick_report_schedule.requirements.json"),
    );
}

#[test]
fn schedule_requirements_round_trip_via_schema_types() {
    let ir = validate(&tick_report_schedule_flow()).expect("validate");
    let requirements = derive_requirements(&ir);
    let json = serde_json::to_value(&requirements).expect("serialize");
    let back: dag_core::FlowRequirements = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, requirements);
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
