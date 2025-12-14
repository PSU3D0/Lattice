use dag_core::prelude::*;
use dag_macros::workflow;
use kernel_plan::validate;

const TRIGGER_SPEC: NodeSpec = NodeSpec {
    identifier: "tests::trigger",
    name: "Trigger",
    kind: NodeKind::Trigger,
    summary: None,
    in_schema: SchemaSpec::Opaque,
    out_schema: SchemaSpec::Opaque,
    effects: Effects::Pure,
    determinism: Determinism::Strict,
    determinism_hints: &[],
    effect_hints: &[],
};

const SINK_SPEC: NodeSpec = NodeSpec::inline(
    "tests::sink",
    "Sink",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

workflow! {
    name: delivery_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = &TRIGGER_SPEC;
    let sink = &SINK_SPEC;

    connect!(trigger -> sink);
    delivery!(trigger -> sink, mode = exactly_once);
}

workflow! {
    name: buffer_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = &TRIGGER_SPEC;
    let sink = &SINK_SPEC;

    connect!(trigger -> sink);
    buffer!(trigger -> sink, max_items = 10);
}

workflow! {
    name: spill_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = &TRIGGER_SPEC;
    let sink = &SINK_SPEC;

    connect!(trigger -> sink);
    buffer!(trigger -> sink, max_items = 10);
    spill!(trigger -> sink, tier = "local", threshold_bytes = 65536);
}

workflow! {
    name: spill_unbounded_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = &TRIGGER_SPEC;
    let sink = &SINK_SPEC;

    connect!(trigger -> sink);
    spill!(trigger -> sink, tier = "local");
}

#[test]
fn workflow_delivery_emits_edge_delivery() {
    let flow = delivery_flow();
    assert_eq!(flow.edges.len(), 1);
    assert_eq!(flow.edges[0].delivery, Delivery::ExactlyOnce);
}

#[test]
fn workflow_buffer_emits_edge_buffer_max_items() {
    let flow = buffer_flow();
    assert_eq!(flow.edges.len(), 1);
    assert_eq!(flow.edges[0].buffer.max_items, Some(10));
}

#[test]
fn workflow_spill_emits_edge_spill_policy() {
    let flow = spill_flow();
    assert_eq!(flow.edges.len(), 1);
    assert_eq!(flow.edges[0].buffer.max_items, Some(10));
    assert_eq!(flow.edges[0].buffer.spill_tier.as_deref(), Some("local"));
    assert_eq!(flow.edges[0].buffer.spill_threshold_bytes, Some(65536));
}

#[test]
fn workflow_delivery_exactly_once_triggers_validator_prereqs() {
    let flow = delivery_flow();
    let diagnostics = validate(&flow).expect_err("expected validation errors");
    let codes: Vec<&str> = diagnostics.iter().map(|d| d.code.code).collect();

    assert!(codes.contains(&"EXACT001"), "missing EXACT001: {codes:?}");
    assert!(codes.contains(&"EXACT002"), "missing EXACT002: {codes:?}");
    assert!(codes.contains(&"EXACT003"), "missing EXACT003: {codes:?}");
}

#[test]
fn workflow_spill_without_buffer_triggers_spill001() {
    let flow = spill_unbounded_flow();
    let diagnostics = validate(&flow).expect_err("expected validation errors");
    let codes: Vec<&str> = diagnostics.iter().map(|d| d.code.code).collect();

    assert!(codes.contains(&"SPILL001"), "missing SPILL001: {codes:?}");
}
