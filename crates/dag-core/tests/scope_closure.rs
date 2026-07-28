use std::collections::{BTreeMap, BTreeSet};

use dag_core::prelude::Version;
use dag_core::{
    BrokerAuthority, BrokerContractIdentityIR, BrokerOperationBudget, ConnectorOpRefIR,
    ConnectorResolutionModeDecl, ContractScopeDescriptor, Determinism, Effects, FlowBuilder,
    FlowIR, FlowRequirements, NodeKind, NodeSpec, Profile, RequirementsError, SchemaSpec,
    ScopeClosureResolution,
};
use sha2::{Digest, Sha256};

fn operation(operation_id: &str, connector_id: &str) -> ConnectorOpRefIR {
    ConnectorOpRefIR {
        operation_id: operation_id.to_string(),
        connector_id: connector_id.to_string(),
        broker_contract: None,
        roles: Vec::new(),
        default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
        selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
        supported_resolution_modes: vec![ConnectorResolutionModeDecl::BoundConnection],
    }
}

fn contracted_operation(
    operation_id: &str,
    connector_id: &str,
    contract_id: &str,
    contract_hash: &str,
) -> ConnectorOpRefIR {
    let mut operation = operation(operation_id, connector_id);
    operation.broker_contract = Some(BrokerContractIdentityIR {
        contract_id: contract_id.to_string(),
        contract_hash: contract_hash.to_string(),
    });
    operation
}

fn descriptor_json(contract_id: &str, scopes: &[&str]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "auth_role": "oauth",
        "broker_abi_version": "1",
        "contract_id": contract_id,
        "effect_class": "effectful",
        "input_schema_hash": format!("sha256:{}", "1".repeat(64)),
        "minimum_scopes": scopes,
        "output_schema_hash": format!("sha256:{}", "2".repeat(64)),
        "response_data_policy": {
            "fields": ["id"],
            "kind": "json_projection",
            "max_bytes": 1024
        },
        "semantic_effect_slots": ["execute"]
    }))
    .unwrap()
}

fn descriptor(contract_id: &str, scopes: &[&str]) -> (String, ContractScopeDescriptor) {
    let json = descriptor_json(contract_id, scopes);
    let hash = format!("sha256:{}", hex::encode(Sha256::digest(&json)));
    let verified = ContractScopeDescriptor::verify_json(&json, &hash).unwrap();
    (hash, verified)
}

fn flow_hash(flow: &FlowIR) -> String {
    let bytes = serde_json::to_vec_pretty(flow).unwrap();
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn authority_with_key(contract_id: &str, slot: &str, key: Option<&str>) -> BrokerAuthority {
    BrokerAuthority::new(
        vec![BrokerOperationBudget {
            contract_id: contract_id.to_string(),
            semantic_effect_slots: vec![slot.to_string()],
            max_logical_calls: 1,
            max_dispatch_attempts_per_call: 1,
            connection_aggregate_key: key.map(str::to_string),
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
fn subflow_scope_closure_includes_nested_and_parent_brokered_operations() {
    let (gmail_hash, gmail_descriptor) =
        descriptor("connector.google.gmail.send_message@1", &["gmail.send"]);
    let (sheets_hash, sheets_descriptor) =
        descriptor("connector.google.sheets.append_row@1", &["spreadsheets"]);
    let mut nested = one_node_flow("nested_gmail", "send");
    nested.nodes[0].connector_ops.push(contracted_operation(
        "connector.google.gmail.send_message",
        "connector.google.gmail",
        "connector.google.gmail.send_message@1",
        &gmail_hash,
    ));
    nested.nodes[0].broker_authority = Some(authority_with_key(
        "connector.google.gmail.send_message@1",
        "send_message",
        Some("google-mail"),
    ));

    let mut parent = one_node_flow("parent_sheets", "append");
    parent.nodes[0].connector_ops.push(contracted_operation(
        "connector.google.sheets.append_row",
        "connector.google.sheets",
        "connector.google.sheets.append_row@1",
        &sheets_hash,
    ));
    parent.nodes[0].broker_authority = Some(authority_with_key(
        "connector.google.sheets.append_row@1",
        "append_row",
        Some("google-sheets"),
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

    let requirements = FlowRequirements::derive(&composed).unwrap();
    let operation_ids = requirements
        .connectors
        .iter()
        .flat_map(|connector| &connector.operations)
        .map(|operation| operation.operation_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        operation_ids,
        BTreeSet::from([
            "connector.google.gmail.send_message".to_string(),
            "connector.google.sheets.append_row".to_string(),
        ])
    );

    let error = requirements
        .clone()
        .with_flow_ir_hash(flow_hash(&composed))
        .resolve_scope_closure(&composed, &[gmail_descriptor.clone()])
        .unwrap_err();
    assert!(matches!(
        error,
        RequirementsError::UnknownContractHash { .. }
    ));

    let resolved = requirements
        .with_flow_ir_hash(flow_hash(&composed))
        .resolve_scope_closure(&composed, &[gmail_descriptor, sheets_descriptor])
        .unwrap();
    assert_eq!(resolved.scope_resolution, ScopeClosureResolution::Resolved);
    let closures = resolved
        .scope_closures
        .into_iter()
        .map(|closure| {
            (
                closure.connection_aggregate_key.unwrap(),
                closure.required_scopes,
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        closures,
        BTreeMap::from([
            ("google-mail".to_string(), vec!["gmail.send".to_string()]),
            (
                "google-sheets".to_string(),
                vec!["spreadsheets".to_string()]
            ),
        ])
    );
}

#[test]
fn forged_subset_descriptor_cannot_claim_the_pinned_hash() {
    let (pinned_hash, _) = descriptor("connector.test.write@1", &["scope.a", "scope.b"]);
    let forged = descriptor_json("connector.test.write@1", &["scope.a"]);
    assert!(matches!(
        ContractScopeDescriptor::verify_json(&forged, &pinned_hash),
        Err(RequirementsError::ContractDescriptorHashMismatch { .. })
    ));
}

#[test]
fn resolution_is_bound_to_exact_serialized_flow_ir() {
    let (hash, descriptor) = descriptor("connector.test.write@1", &["scope.a"]);
    let mut flow_a = one_node_flow("same", "write");
    flow_a.nodes[0].connector_ops.push(contracted_operation(
        "connector.test.write",
        "connector.test",
        "connector.test.write@1",
        &hash,
    ));
    let requirements = FlowRequirements::derive(&flow_a)
        .unwrap()
        .with_flow_ir_hash(flow_hash(&flow_a));

    let mut flow_b = flow_a.clone();
    flow_b.nodes[0].broker_authority = Some(authority_with_key(
        "connector.test.write@1",
        "execute",
        Some("different-partition"),
    ));
    assert!(matches!(
        requirements.resolve_scope_closure(&flow_b, &[descriptor]),
        Err(RequirementsError::ScopeResolutionFlowMismatch)
    ));
}

#[test]
fn unexpanded_subflow_is_visible_but_does_not_fail_identity_derivation() {
    let mut flow = one_node_flow("composed", "child");
    flow.nodes[0].kind = NodeKind::Subflow;
    let requirements = FlowRequirements::derive(&flow).expect("identity derivation must succeed");
    assert_eq!(
        requirements.scope_resolution,
        ScopeClosureResolution::Unresolved {
            unresolved_subflows: vec!["child".to_string()]
        }
    );
    assert!(matches!(
        requirements
            .with_flow_ir_hash(flow_hash(&flow))
            .resolve_scope_closure(&flow, &[]),
        Err(RequirementsError::MissingSubflowIr { .. })
    ));
}

#[test]
fn broker_authority_without_pinned_contract_identity_fails_resolution() {
    let mut flow = one_node_flow("stale", "write");
    flow.nodes[0]
        .connector_ops
        .push(operation("connector.test.write", "connector.test"));
    flow.nodes[0].broker_authority = Some(authority_with_key(
        "connector.test.write@1",
        "execute",
        Some("connection"),
    ));
    let requirements = FlowRequirements::derive(&flow).unwrap();
    assert!(matches!(
        requirements.scope_resolution,
        ScopeClosureResolution::Unresolved { .. }
    ));
    let requirements = requirements.with_flow_ir_hash(flow_hash(&flow));
    assert!(matches!(
        requirements.resolve_scope_closure(&flow, &[]),
        Err(RequirementsError::MissingPinnedContractIdentity { .. })
    ));
}

#[test]
fn subflow_cycle_and_depth_fail_closed() {
    let mut cycle = one_node_flow("cycle", "self_ref");
    cycle.nodes[0].kind = NodeKind::Subflow;
    cycle.nodes[0].subflow_ir = Some(Box::new(one_node_flow("cycle", "nested")));
    assert!(matches!(
        FlowRequirements::derive(&cycle),
        Err(RequirementsError::SubflowCycle { .. })
    ));

    let mut nested = one_node_flow("depth_leaf", "leaf");
    for depth in (0..65).rev() {
        let mut parent = one_node_flow(&format!("depth_{depth}"), "child");
        parent.nodes[0].kind = NodeKind::Subflow;
        parent.nodes[0].subflow_ir = Some(Box::new(nested));
        nested = parent;
    }
    assert!(matches!(
        FlowRequirements::derive(&nested),
        Err(RequirementsError::SubflowDepthExceeded { .. })
    ));
}

#[test]
fn known_gap_broker_authority_validation_accepts_bogus_declared_operation_version() {
    let mut flow = one_node_flow("version_gap", "send");
    flow.nodes[0].connector_ops.push(operation(
        "connector.google.gmail.send_message",
        "connector.google.gmail",
    ));
    flow.nodes[0].broker_authority = Some(authority_with_key(
        "connector.google.gmail.send_message@999999",
        "send_message",
        None,
    ));

    assert!(flow.validate_broker_authority().is_ok());
}
