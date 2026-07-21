use std::collections::{BTreeMap, BTreeSet};

use broker_core::artifacts::{
    AggregateCeiling, AggregateCeilings, Assurance, Budget, FlowAuthorityManifest, NodeAuthority,
    OperationAuthority, ParsedArtifact, PrincipalKind, PrincipalRef,
};
use dag_core::{BrokerContractMetadata, ConnectorOpMetadata, ConnectorRoleKindDecl};
use kernel_plan::ValidatedIR;
use sha2::{Digest, Sha256};

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
    #[error("Flow IR hash does not match the exact supplied bytes")]
    FlowIrHashMismatch,
    #[error("supplied Flow IR bytes do not represent the validated IR")]
    FlowIrBytesMismatch,
    #[error("derived authority manifest failed protocol artifact validation")]
    InvalidDerivedArtifact,
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

#[derive(Clone, Copy, Debug)]
pub struct ApprovedAuthorityContract<'a> {
    pub contract_id: &'a str,
    pub contract_hash: &'a str,
}

#[derive(Debug)]
pub struct VerifiedAuthorityManifest {
    pub manifest: ParsedArtifact<FlowAuthorityManifest>,
    pub flow_id: String,
}

/// Verify caller-supplied canonical Flow IR and authority bytes against the
/// same B3 authority vocabulary used by derivation. Registry contract hashes
/// are injected by the broker, never by the caller.
pub fn verify_authority_manifest(
    flow_ir_bytes: &[u8],
    declared_flow_ir_hash: &str,
    manifest_bytes: &[u8],
    provenance: ManifestProvenance<'_>,
    approved_contracts: &[ApprovedAuthorityContract<'_>],
) -> Result<VerifiedAuthorityManifest, AuthorityManifestError> {
    if !valid_hash(declared_flow_ir_hash) {
        return Err(AuthorityManifestError::InvalidFlowIrHash);
    }
    let actual_hash = format!("sha256:{}", hex::encode(Sha256::digest(flow_ir_bytes)));
    if actual_hash != declared_flow_ir_hash {
        return Err(AuthorityManifestError::FlowIrHashMismatch);
    }
    let canonical_ir = broker_core::canonical::canonicalize_bounded(flow_ir_bytes, 1024 * 1024)
        .map_err(|_| AuthorityManifestError::FlowIrBytesMismatch)?;
    if canonical_ir.as_bytes() != flow_ir_bytes {
        return Err(AuthorityManifestError::FlowIrBytesMismatch);
    }
    let flow: dag_core::FlowIR = serde_json::from_slice(flow_ir_bytes)
        .map_err(|_| AuthorityManifestError::FlowIrBytesMismatch)?;
    let validated =
        kernel_plan::validate(&flow).map_err(|_| AuthorityManifestError::InvalidBrokerAuthority)?;
    validated
        .flow()
        .validate_broker_authority()
        .map_err(|_| AuthorityManifestError::InvalidBrokerAuthority)?;
    let manifest: ParsedArtifact<FlowAuthorityManifest> =
        broker_core::artifacts::parse(manifest_bytes)
            .map_err(|_| AuthorityManifestError::InvalidDerivedArtifact)?;
    if manifest.canonical_bytes() != manifest_bytes
        || manifest.view.org_id != provenance.org_id
        || manifest.view.principal.kind != provenance.principal_kind
        || manifest.view.principal.id != provenance.principal_id
        || manifest.view.flow_ir_hash != declared_flow_ir_hash
    {
        return Err(AuthorityManifestError::InvalidProvenance);
    }
    let broker_nodes = flow
        .nodes
        .iter()
        .filter(|node| node.broker_authority.is_some())
        .collect::<Vec<_>>();
    if manifest.view.nodes.len() != broker_nodes.len() {
        return Err(AuthorityManifestError::InvalidBrokerAuthority);
    }
    let mut expected_flow_ceiling = None;
    let mut expected_connections = BTreeMap::new();
    for node in broker_nodes {
        let authority = node
            .broker_authority
            .as_ref()
            .ok_or(AuthorityManifestError::InvalidBrokerAuthority)?;
        merge_flow_ceiling(
            &mut expected_flow_ceiling,
            authority.flow_aggregate_max_logical_calls(),
        )?;
        merge_connection_ceilings(
            &mut expected_connections,
            authority.connection_aggregate_max_logical_calls(),
        )?;
        let supplied = manifest
            .view
            .nodes
            .get(&node.alias)
            .ok_or(AuthorityManifestError::InvalidBrokerAuthority)?;
        if supplied.node_id != node.id.0
            || supplied.operations.len() != authority.operation_budgets().len()
        {
            return Err(AuthorityManifestError::InvalidBrokerAuthority);
        }
        for budget in authority.operation_budgets() {
            let operation = supplied
                .operations
                .iter()
                .find(|operation| operation.contract_id == budget.contract_id)
                .ok_or(AuthorityManifestError::ContractMismatch)?;
            let approved = approved_contracts
                .iter()
                .find(|approved| approved.contract_id == budget.contract_id)
                .ok_or(AuthorityManifestError::MissingContractMetadata)?;
            if operation.contract_hash != approved.contract_hash
                || operation.call_budget.max_logical_calls != budget.max_logical_calls
                || operation.call_budget.max_dispatch_attempts_per_call
                    != budget.max_dispatch_attempts_per_call
                || operation.minimum_assurance != Assurance::BrokeredCount
                || !operation.required_attenuations.is_empty()
                || operation.connection_aggregate_key != budget.connection_aggregate_key
            {
                return Err(AuthorityManifestError::ContractMismatch);
            }
        }
    }
    let (actual_flow, actual_connections) = manifest
        .view
        .aggregate_ceilings
        .as_ref()
        .map(|aggregate| {
            (
                aggregate.flow.as_ref().map(|value| value.max_logical_calls),
                aggregate
                    .connections
                    .iter()
                    .map(|(key, value)| (key.clone(), value.max_logical_calls))
                    .collect(),
            )
        })
        .unwrap_or((None, BTreeMap::new()));
    if actual_flow != expected_flow_ceiling || actual_connections != expected_connections {
        return Err(AuthorityManifestError::ConflictingAggregateCeilings);
    }
    Ok(VerifiedAuthorityManifest {
        manifest,
        flow_id: flow.id.0,
    })
}

/// Purely derive a Broker V1 manifest from validated Flow IR, the exact Flow
/// IR artifact bytes used by bundle assembly, and host-supplied connector
/// registry metadata. The declared hash must match those exact bytes.
pub fn derive_authority_manifest(
    validated: &ValidatedIR,
    flow_ir_bytes: &[u8],
    flow_ir_hash: &str,
    provenance: ManifestProvenance<'_>,
    metadata: &[NodeOperationMetadata<'_>],
) -> Result<FlowAuthorityManifest, AuthorityManifestError> {
    if !valid_hash(flow_ir_hash) {
        return Err(AuthorityManifestError::InvalidFlowIrHash);
    }
    let actual_hash = format!("sha256:{}", hex::encode(Sha256::digest(flow_ir_bytes)));
    if actual_hash != flow_ir_hash {
        return Err(AuthorityManifestError::FlowIrHashMismatch);
    }
    broker_core::canonical::canonicalize_bounded(flow_ir_bytes, 1024 * 1024)
        .map_err(|_| AuthorityManifestError::FlowIrBytesMismatch)?;
    let supplied: serde_json::Value = serde_json::from_slice(flow_ir_bytes)
        .map_err(|_| AuthorityManifestError::FlowIrBytesMismatch)?;
    let expected = serde_json::to_value(validated.flow())
        .map_err(|_| AuthorityManifestError::FlowIrBytesMismatch)?;
    if supplied != expected {
        return Err(AuthorityManifestError::FlowIrBytesMismatch);
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
    let manifest = FlowAuthorityManifest {
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
    };
    let bytes = broker_core::canonical::from_serde(&manifest, broker_core::artifacts::MANIFEST_MAX)
        .map_err(|_| AuthorityManifestError::InvalidDerivedArtifact)?
        .into_bytes();
    let parsed: broker_core::artifacts::ParsedArtifact<FlowAuthorityManifest> =
        broker_core::artifacts::parse(&bytes)
            .map_err(|_| AuthorityManifestError::InvalidDerivedArtifact)?;
    Ok(parsed.view)
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
