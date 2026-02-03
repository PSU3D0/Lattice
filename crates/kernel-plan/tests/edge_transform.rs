use dag_core::prelude::*;
use dag_core::{EdgeTransformIR, EdgeTransformKind};
use kernel_plan::validate;

#[test]
fn test_planner_accepts_into_transform_with_incompatible_schemas() {
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
        SchemaSpec::Named("i64"),
        SchemaSpec::Opaque,
        Effects::Pure,
        Determinism::Strict,
        Some("consume i64 payloads"),
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
