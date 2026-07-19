use std::collections::{BTreeMap, BTreeSet};

use broker_core::artifacts::{
    AggregateCeiling, AggregateCeilings, Assurance, Budget, FlowAuthorityManifest, NodeAuthority,
    OperationAuthority, PrincipalKind, PrincipalRef,
};
use dag_core::{BrokerContractMetadata, ConnectorOpMetadata, ConnectorRoleKindDecl};
use kernel_plan::ValidatedIR;

#[derive(Clone, Debug)]
pub struct ManifestProvenance<'a> {
    pub org_id: &'a str,
    pub principal_kind: PrincipalKind,
    pub principal_id: &'a str,
}

/// Host registry metadata for one operation declared by one IR node.
#[derive(Clone, Copy, Debug)]
pub struct NodeOperationMetadata<'a> {
    pub node_alias: &'a str,
    pub operation: &'a ConnectorOpMetadata,
    pub contract: Option<BrokerContractMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AuthorityManifestError {
    #[error("Flow IR hash is invalid")]
    InvalidFlowIrHash,
    #[error("manifest provenance is invalid")]
    InvalidProvenance,
    #[error("Flow IR broker authority is invalid")]
    InvalidBrokerAuthority,
    #[error("broker node connector metadata is missing")]
    MissingConnectorMetadata,
    #[error("broker node connector metadata does not match Flow IR")]
    ConnectorMetadataMismatch,
    #[error("broker operation contract metadata is missing")]
    MissingContractMetadata,
    #[error("broker operation contract metadata is invalid")]
    InvalidContractMetadata,
    #[error("broker operation contract does not match authority")]
    ContractMismatch,
    #[error("broker metadata contains a duplicate operation")]
    DuplicateMetadata,
    #[error("broker aggregate ceilings conflict between nodes")]
    ConflictingAggregateCeilings,
    #[error("broker dispatch-attempt budget exceeds the protocol range")]
    DispatchBudgetOutOfRange,
    #[error("connector-free flows do not have an authority manifest")]
    NoBrokerAuthority,
}

/// Purely derive a Broker V1 manifest from validated Flow IR and host-supplied
/// connector registry metadata. `flow_ir_hash` is accepted verbatim from the
/// bundle assembly hashing path; this function never serializes or hashes IR.
pub fn derive_authority_manifest(
    validated: &ValidatedIR,
    flow_ir_hash: &str,
    provenance: ManifestProvenance<'_>,
    metadata: &[NodeOperationMetadata<'_>],
) -> Result<FlowAuthorityManifest, AuthorityManifestError> {
    if !valid_hash(flow_ir_hash) {
        return Err(AuthorityManifestError::InvalidFlowIrHash);
    }
    if provenance.org_id.is_empty() || provenance.principal_id.is_empty() {
        return Err(AuthorityManifestError::InvalidProvenance);
    }
    let flow = validated.flow();
    flow.validate_broker_authority()
        .map_err(|_| AuthorityManifestError::InvalidBrokerAuthority)?;
    validate_metadata_uniqueness(metadata)?;

    let mut nodes = BTreeMap::new();
    let mut flow_ceiling = None;
    let mut connection_ceilings = BTreeMap::new();
    for node in flow
        .nodes
        .iter()
        .filter(|node| node.broker_authority.is_some())
    {
        let authority = node.broker_authority.as_ref().expect("filtered authority");
        merge_flow_ceiling(
            &mut flow_ceiling,
            authority.flow_aggregate_max_logical_calls(),
        )?;
        merge_connection_ceilings(
            &mut connection_ceilings,
            authority.connection_aggregate_max_logical_calls(),
        )?;

        let mut operations = Vec::new();
        for budget in authority.operation_budgets() {
            let operation_id = budget
                .contract_id
                .rsplit_once('@')
                .map(|(id, _)| id)
                .ok_or(AuthorityManifestError::InvalidContractMetadata)?;
            let registry = metadata
                .iter()
                .find(|entry| {
                    entry.node_alias == node.alias && entry.operation.operation_id == operation_id
                })
                .ok_or(AuthorityManifestError::MissingConnectorMetadata)?;
            let ir_op = node
                .connector_ops
                .iter()
                .find(|entry| entry.operation_id == operation_id)
                .ok_or(AuthorityManifestError::MissingConnectorMetadata)?;
            if !operation_matches_ir(registry.operation, ir_op) {
                return Err(AuthorityManifestError::ConnectorMetadataMismatch);
            }
            let contract = registry
                .contract
                .ok_or(AuthorityManifestError::MissingContractMetadata)?;
            if contract.contract_id != budget.contract_id {
                return Err(AuthorityManifestError::ContractMismatch);
            }
            if !valid_hash(contract.contract_hash) {
                return Err(AuthorityManifestError::InvalidContractMetadata);
            }
            let attempts = u8::try_from(budget.max_dispatch_attempts_per_call)
                .map_err(|_| AuthorityManifestError::DispatchBudgetOutOfRange)?;
            if attempts == 0 {
                return Err(AuthorityManifestError::DispatchBudgetOutOfRange);
            }
            operations.push(OperationAuthority {
                contract_id: contract.contract_id.to_string(),
                contract_hash: contract.contract_hash.to_string(),
                call_budget: Budget {
                    max_logical_calls: budget.max_logical_calls,
                    max_dispatch_attempts_per_call: attempts,
                },
                minimum_assurance: Assurance::BrokeredCount,
                required_attenuations: Vec::new(),
                connection_aggregate_key: budget.connection_aggregate_key.clone(),
                extensions: Default::default(),
            });
        }
        operations.sort_by(|left, right| left.contract_hash.cmp(&right.contract_hash));
        nodes.insert(
            node.alias.clone(),
            NodeAuthority {
                node_id: node.id.0.clone(),
                operations,
                extensions: Default::default(),
            },
        );
    }
    if nodes.is_empty() {
        return Err(AuthorityManifestError::NoBrokerAuthority);
    }
    let aggregate_ceilings = if flow_ceiling.is_none() && connection_ceilings.is_empty() {
        None
    } else {
        Some(AggregateCeilings {
            flow: flow_ceiling.map(ceiling),
            connections: connection_ceilings
                .into_iter()
                .map(|(key, value)| (key, ceiling(value)))
                .collect(),
            extensions: Default::default(),
        })
    };
    Ok(FlowAuthorityManifest {
        schema_version: "0.1".into(),
        critical_fields: Vec::new(),
        org_id: provenance.org_id.into(),
        principal: PrincipalRef {
            kind: provenance.principal_kind,
            id: provenance.principal_id.into(),
        },
        flow_ir_hash: flow_ir_hash.into(),
        nodes,
        aggregate_ceilings,
        extensions: Default::default(),
    })
}

fn operation_matches_ir(metadata: &ConnectorOpMetadata, ir: &dag_core::ConnectorOpRefIR) -> bool {
    metadata.operation_id == ir.operation_id
        && metadata.connector_id == ir.connector_id
        && metadata.resolution.default_mode == ir.default_resolution_mode
        && metadata.resolution.supported_modes == ir.supported_resolution_modes
        && ir
            .supported_resolution_modes
            .contains(&ir.selected_resolution_mode)
        && metadata.roles.len() == ir.roles.len()
        && metadata.roles.iter().zip(&ir.roles).all(|(left, right)| {
            role_kind(left.kind) == role_kind(right.kind)
                && left.name == right.name
                && left.expected_handle_kind == right.expected_handle_kind
                && left.required == right.required
        })
}

fn role_kind(value: ConnectorRoleKindDecl) -> u8 {
    match value {
        ConnectorRoleKindDecl::OutboundAuth => 0,
        ConnectorRoleKindDecl::ProvisioningAuth => 1,
        ConnectorRoleKindDecl::InboundVerifier => 2,
        ConnectorRoleKindDecl::EndpointProfile => 3,
    }
}

fn validate_metadata_uniqueness(
    metadata: &[NodeOperationMetadata<'_>],
) -> Result<(), AuthorityManifestError> {
    let mut seen = BTreeSet::new();
    for entry in metadata {
        if !seen.insert((entry.node_alias, entry.operation.operation_id)) {
            return Err(AuthorityManifestError::DuplicateMetadata);
        }
    }
    Ok(())
}

fn merge_flow_ceiling(
    current: &mut Option<u64>,
    candidate: Option<u64>,
) -> Result<(), AuthorityManifestError> {
    if let Some(candidate) = candidate {
        if current.is_some_and(|current| current != candidate) {
            return Err(AuthorityManifestError::ConflictingAggregateCeilings);
        }
        *current = Some(candidate);
    }
    Ok(())
}

fn merge_connection_ceilings(
    current: &mut BTreeMap<String, u64>,
    candidates: &BTreeMap<String, u64>,
) -> Result<(), AuthorityManifestError> {
    for (key, value) in candidates {
        if current.get(key).is_some_and(|current| current != value) {
            return Err(AuthorityManifestError::ConflictingAggregateCeilings);
        }
        current.insert(key.clone(), *value);
    }
    Ok(())
}

fn ceiling(max_logical_calls: u64) -> AggregateCeiling {
    AggregateCeiling {
        max_logical_calls,
        extensions: Default::default(),
    }
}

fn valid_hash(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}
