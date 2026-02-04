use std::collections::BTreeMap;

use dag_core::{
    Determinism, DurabilityProfile, Effects, FlowIR, FlowId, FlowMetadata, FlowPolicies, NodeIR,
    NodeId, NodeKind, Profile, SchemaRef,
};
use flow_bundle::expand_subflow_ir;
use semver::Version;

fn subflow_node(alias: &str, subflow_id: &str) -> NodeIR {
    NodeIR {
        id: NodeId::new(format!("node://{alias}")),
        alias: alias.to_string(),
        identifier: format!("subflow::{subflow_id}::entry"),
        name: format!("Subflow {subflow_id}"),
        kind: NodeKind::Subflow,
        summary: None,
        in_schema: SchemaRef::Opaque,
        out_schema: SchemaRef::Opaque,
        effects: Effects::Effectful,
        determinism: Determinism::Nondeterministic,
        idempotency: Default::default(),
        durability: DurabilityProfile::default(),
        determinism_hints: Vec::new(),
        effect_hints: Vec::new(),
        subflow_ir: None,
    }
}

fn flow_ir(name: &str, nodes: Vec<NodeIR>) -> FlowIR {
    let version = Version::parse("1.0.0").expect("version");
    FlowIR {
        id: FlowId::new(name, &version),
        name: name.to_string(),
        version,
        profile: Profile::Dev,
        summary: None,
        nodes,
        edges: Vec::new(),
        control_surfaces: Vec::new(),
        checkpoints: Vec::new(),
        policies: FlowPolicies::default(),
        metadata: FlowMetadata::default(),
        artifacts: Vec::new(),
    }
}

#[test]
fn expanded_ir_rejects_subflow_cycles() {
    let root = flow_ir("root", vec![subflow_node("call_a", "subflow.alpha")]);
    let subflow_a = flow_ir(
        "subflow.alpha",
        vec![subflow_node("call_b", "subflow.beta")],
    );
    let subflow_b = flow_ir(
        "subflow.beta",
        vec![subflow_node("call_a", "subflow.alpha")],
    );

    let mut subflows = BTreeMap::new();
    subflows.insert("subflow.alpha".to_string(), subflow_a);
    subflows.insert("subflow.beta".to_string(), subflow_b);

    let result = expand_subflow_ir(&root, &subflows);
    assert!(result.is_err(), "expected subflow cycle to be rejected");
}
