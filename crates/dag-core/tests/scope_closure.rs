use std::collections::{BTreeMap, BTreeSet};

use dag_core::prelude::Version;
use dag_core::{
    BrokerAuthority, BrokerOperationBudget, ConnectorOpRefIR, ConnectorResolutionModeDecl,
    Determinism, Effects, FlowBuilder, FlowIR, FlowRequirements, NodeKind, NodeSpec, Profile,
    SchemaSpec,
};

fn operation(operation_id: &str, connector_id: &str) -> ConnectorOpRefIR {
    ConnectorOpRefIR {
        operation_id: operation_id.to_string(),
        connector_id: connector_id.to_string(),
        roles: Vec::new(),
        default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
        selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
        supported_resolution_modes: vec![ConnectorResolutionModeDecl::BoundConnection],
    }
}

fn authority(contract_id: &str, slot: &str) -> BrokerAuthority {
    BrokerAuthority::new(
        vec![BrokerOperationBudget {
            contract_id: contract_id.to_string(),
            semantic_effect_slots: vec![slot.to_string()],
            max_logical_calls: 1,
            max_dispatch_attempts_per_call: 1,
            connection_aggregate_key: None,
        }],
        None,
        BTreeMap::new(),
    )
    .unwrap()
}

fn one_node_flow(name: &str, alias: &str) -> FlowIR {
    let spec = NodeSpec::inline(
        "tests::brokered_operation",
        "Brokered operation",
        SchemaSpec::Opaque,
        SchemaSpec::Opaque,
        Effects::Effectful,
        Determinism::BestEffort,
        None,
    );
    let mut builder = FlowBuilder::new(name, Version::new(1, 0, 0), Profile::Web);
    builder.add_node(alias, &spec).unwrap();
    builder.build()
}

#[test]
#[ignore = "P2 will fix: FlowRequirements scans direct nodes and omits connector operations embedded in subflow_ir"]
fn subflow_scope_closure_includes_nested_and_parent_brokered_operations() {
    // Mutation: change derive_connectors back to iterating only flow.nodes after recursive subflow
    // traversal is added. This assertion must then lose Gmail and fail.
    let mut nested = one_node_flow("nested_gmail", "send");
    nested.nodes[0].connector_ops.push(operation(
        "connector.google.gmail.send_message",
        "connector.google.gmail",
    ));
    nested.nodes[0].broker_authority = Some(authority(
        "connector.google.gmail.send_message@1",
        "send_message",
    ));

    let mut parent = one_node_flow("parent_sheets", "append");
    parent.nodes[0].connector_ops.push(operation(
        "connector.google.sheets.append_row",
        "connector.google.sheets",
    ));
    parent.nodes[0].broker_authority = Some(authority(
        "connector.google.sheets.append_row@1",
        "append_row",
    ));

    let mut subflow_spec = NodeSpec::inline(
        "tests::nested_gmail_subflow",
        "Nested Gmail subflow",
        SchemaSpec::Opaque,
        SchemaSpec::Opaque,
        Effects::Effectful,
        Determinism::BestEffort,
        None,
    );
    subflow_spec.kind = NodeKind::Subflow;
    let mut builder = FlowBuilder::new("composed", Version::new(1, 0, 0), Profile::Web);
    builder.add_node("gmail_subflow", &subflow_spec).unwrap();
    let mut composed = builder.build();
    composed.nodes[0].subflow_ir = Some(Box::new(nested));
    composed.nodes.extend(parent.nodes);

    let operation_ids = FlowRequirements::derive(&composed)
        .unwrap()
        .connectors
        .into_iter()
        .flat_map(|connector| connector.operations)
        .map(|operation| operation.operation_id)
        .collect::<BTreeSet<_>>();

    assert_eq!(
        operation_ids,
        BTreeSet::from([
            "connector.google.gmail.send_message".to_string(),
            "connector.google.sheets.append_row".to_string(),
        ])
    );
}

#[test]
fn known_gap_broker_authority_validation_accepts_bogus_declared_operation_version() {
    // Mutation: make validate_for_node compare the declared operation's exact supported contract
    // version instead of stripping @version. This characterization test must then fail.
    let mut flow = one_node_flow("version_gap", "send");
    flow.nodes[0].connector_ops.push(operation(
        "connector.google.gmail.send_message",
        "connector.google.gmail",
    ));
    flow.nodes[0].broker_authority = Some(authority(
        "connector.google.gmail.send_message@999999",
        "send_message",
    ));

    assert!(flow.validate_broker_authority().is_ok());
}
