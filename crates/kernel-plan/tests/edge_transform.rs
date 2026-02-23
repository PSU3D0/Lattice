use dag_core::prelude::*;
use dag_core::{EdgeTransformIR, EdgeTransformKind};
use kernel_plan::validate;

#[test]
fn test_planner_accepts_into_transform_for_supported_pair() {
    let mut builder = FlowBuilder::new("edge_transform", Version::new(1, 0, 0), Profile::Web);

    let producer_spec = NodeSpec::inline(
        "tests::producer",
        "Producer",
        SchemaSpec::Opaque,
        SchemaSpec::Named("u32"),
        Effects::Pure,
        Determinism::Strict,
        Some("emit u32 payloads"),
    );
    let consumer_spec = NodeSpec::inline(
        "tests::consumer",
        "Consumer",
        SchemaSpec::Named("u64"),
        SchemaSpec::Opaque,
        Effects::Pure,
        Determinism::Strict,
        Some("consume u64 payloads"),
    );

    let producer = builder
        .add_node("producer", &producer_spec)
        .expect("add producer");
    let consumer = builder
        .add_node("consumer", &consumer_spec)
        .expect("add consumer");
    builder.connect(&producer, &consumer);

    let mut flow = builder.build();
    flow.edges[0].transform = Some(EdgeTransformIR {
        kind: EdgeTransformKind::Into,
    });

    let report = validate(&flow);
    assert!(report.is_ok(), "unexpected diagnostics: {:?}", report.err());
}

#[test]
fn test_planner_rejects_into_transform_for_unsupported_pair() {
    let mut builder = FlowBuilder::new("edge_transform", Version::new(1, 0, 0), Profile::Web);

    let producer_spec = NodeSpec::inline(
        "tests::producer",
        "Producer",
        SchemaSpec::Opaque,
        SchemaSpec::Named("String"),
        Effects::Pure,
        Determinism::Strict,
        Some("emit string payloads"),
    );
    let consumer_spec = NodeSpec::inline(
        "tests::consumer",
        "Consumer",
        SchemaSpec::Named("u32"),
        SchemaSpec::Opaque,
        Effects::Pure,
        Determinism::Strict,
        Some("consume u32 payloads"),
    );

    let producer = builder
        .add_node("producer", &producer_spec)
        .expect("add producer");
    let consumer = builder
        .add_node("consumer", &consumer_spec)
        .expect("add consumer");
    builder.connect(&producer, &consumer);

    let mut flow = builder.build();
    flow.edges[0].transform = Some(EdgeTransformIR {
        kind: EdgeTransformKind::Into,
    });

    let diagnostics = validate(&flow).expect_err("expected DAG201 mismatch diagnostic");
    assert!(diagnostics.iter().any(|diag| diag.code.code == "DAG201"));
}
