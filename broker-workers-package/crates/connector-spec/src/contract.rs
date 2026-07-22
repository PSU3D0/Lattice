use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::{
    ActionSurface, BrokerRequestPlan, ConnectorManifest, FieldDecl, FieldKind,
    OperationContractDescriptor, TypeDecl, V2AuthProfileRequirementDescriptor,
    V2OperationRequirementDescriptor,
};

/// Maximum canonical byte length of a contract descriptor or one schema
/// closure. This matches the Broker V1 operation/request-plan limit.
pub const MAX_CONTRACT_DESCRIPTOR_BYTES: usize = 256 * 1024;
/// Maximum number of type declarations reachable from one action schema.
pub const MAX_SCHEMA_DECLARATIONS: usize = 1024;

/// Return the exact deterministic operation-contract descriptor.
///
/// The contract identity includes SHA-256 hashes of canonical input and output
/// schema closures. A closure contains the action's root type name and every
/// transitively referenced object/enum declaration, so changing a nested
/// schema changes the contract hash even when the semantic contract fields do
/// not. Unicode strings are retained byte-for-byte and canonicalized with the
/// workspace's full RFC 8785 implementation.
pub fn operation_contract_descriptor(
    manifest: &ConnectorManifest,
    action: &ActionSurface,
) -> Result<OperationContractDescriptor, ContractCanonicalizationError> {
    let contract = action
        .contract
        .as_ref()
        .ok_or(ContractCanonicalizationError::MissingContract)?;
    let input_schema_hash = type_schema_hash(manifest, &action.input)?;
    let output_schema_hash = type_schema_hash(manifest, &action.output)?;
    Ok(contract.descriptor_with_schema_hashes(input_schema_hash, output_schema_hash))
}

pub fn v2_auth_profile_requirement_descriptor(
    manifest: &ConnectorManifest,
    profile_name: &str,
) -> Result<V2AuthProfileRequirementDescriptor, ContractCanonicalizationError> {
    let requirement = manifest
        .credential_plane
        .as_ref()
        .and_then(|plane| plane.auth_profiles.get(profile_name))
        .cloned()
        .ok_or(ContractCanonicalizationError::MissingV2Declaration)?;
    let requirement_hash = canonical_value_hash(&requirement)?;
    Ok(V2AuthProfileRequirementDescriptor {
        connector_id: manifest.connector.id.clone(),
        profile_name: profile_name.to_string(),
        requirement,
        requirement_hash,
    })
}

pub fn v2_operation_requirement_descriptor(
    manifest: &ConnectorManifest,
    action: &ActionSurface,
) -> Result<V2OperationRequirementDescriptor, ContractCanonicalizationError> {
    let requirements = action
        .credential_requirements
        .as_deref()
        .cloned()
        .ok_or(ContractCanonicalizationError::MissingV2Declaration)?;
    let requirements_hash = canonical_value_hash(&requirements)?;
    Ok(V2OperationRequirementDescriptor {
        connector_id: manifest.connector.id.clone(),
        operation_id: action.identifier.clone(),
        input_schema_hash: type_schema_hash(manifest, &action.input)?,
        output_schema_hash: type_schema_hash(manifest, &action.output)?,
        requirements,
        requirements_hash,
    })
}

pub fn canonical_value_hash<T: serde::Serialize>(
    value: &T,
) -> Result<String, ContractCanonicalizationError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| ContractCanonicalizationError::UnsupportedDescriptorDomain)?;
    let canonical = jcs_canonical::canonicalize_bounded(&bytes, MAX_CONTRACT_DESCRIPTOR_BYTES)
        .map_err(|_| ContractCanonicalizationError::DescriptorLimitExceeded)?;
    hash_bytes(canonical.as_bytes())
}

/// Canonicalize the complete contract-hash preimage using RFC 8785 JCS.
pub fn canonical_contract_json(
    descriptor: &OperationContractDescriptor,
) -> Result<Vec<u8>, ContractCanonicalizationError> {
    let bytes = serde_json::to_vec(descriptor)
        .map_err(|_| ContractCanonicalizationError::UnsupportedDescriptorDomain)?;
    jcs_canonical::canonicalize_bounded(&bytes, MAX_CONTRACT_DESCRIPTOR_BYTES)
        .map(|value| value.into_bytes())
        .map_err(|_| ContractCanonicalizationError::DescriptorLimitExceeded)
}

/// Derive the schema-inclusive identity for one manifest action.
pub fn contract_hash(
    manifest: &ConnectorManifest,
    action: &ActionSurface,
) -> Result<String, ContractCanonicalizationError> {
    let descriptor = operation_contract_descriptor(manifest, action)?;
    descriptor_hash(&descriptor)
}

/// Hash an already generated descriptor after enforcing canonical limits.
pub fn descriptor_hash(
    descriptor: &OperationContractDescriptor,
) -> Result<String, ContractCanonicalizationError> {
    hash_bytes(&canonical_contract_json(descriptor)?)
}

/// Hash the complete declarative provider plan, including typed query
/// mappings and a pinned trusted-adapter reference.
pub fn request_plan_hash(
    plan: &BrokerRequestPlan,
) -> Result<String, ContractCanonicalizationError> {
    let bytes = serde_json::to_vec(plan)
        .map_err(|_| ContractCanonicalizationError::UnsupportedDescriptorDomain)?;
    let canonical = jcs_canonical::canonicalize_bounded(&bytes, MAX_CONTRACT_DESCRIPTOR_BYTES)
        .map_err(|_| ContractCanonicalizationError::DescriptorLimitExceeded)?;
    hash_bytes(canonical.as_bytes())
}

pub fn type_schema_hash(
    manifest: &ConnectorManifest,
    root: &str,
) -> Result<String, ContractCanonicalizationError> {
    #[derive(serde::Serialize)]
    struct SchemaClosure<'a> {
        declarations: BTreeMap<&'a str, &'a TypeDecl>,
        root: &'a str,
    }

    let mut pending = vec![root];
    let mut seen = BTreeSet::new();
    let mut declarations = BTreeMap::new();
    while let Some(name) = pending.pop() {
        if !seen.insert(name) {
            continue;
        }
        if seen.len() > MAX_SCHEMA_DECLARATIONS {
            return Err(ContractCanonicalizationError::DescriptorLimitExceeded);
        }
        let declaration = manifest
            .type_decl(name)
            .ok_or(ContractCanonicalizationError::MissingSchema)?;
        declarations.insert(name, declaration);
        if let TypeDecl::Object { fields } = declaration {
            for field in fields.values() {
                collect_references(field, &mut pending);
            }
        }
    }

    let source = serde_json::to_vec(&SchemaClosure { declarations, root })
        .map_err(|_| ContractCanonicalizationError::UnsupportedDescriptorDomain)?;
    let canonical = jcs_canonical::canonicalize_bounded(&source, MAX_CONTRACT_DESCRIPTOR_BYTES)
        .map_err(|_| ContractCanonicalizationError::DescriptorLimitExceeded)?;
    hash_bytes(canonical.as_bytes())
}

fn collect_references<'a>(field: &'a FieldDecl, pending: &mut Vec<&'a str>) {
    match field.kind {
        FieldKind::ObjectRef | FieldKind::EnumRef => {
            if let Some(target) = field.target.as_deref() {
                pending.push(target);
            }
        }
        FieldKind::List => {
            if let Some(item) = field.item.as_deref() {
                collect_references(item, pending);
            }
        }
        _ => {}
    }
}

fn hash_bytes(bytes: &[u8]) -> Result<String, ContractCanonicalizationError> {
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContractCanonicalizationError {
    #[error("action does not declare an operation contract")]
    MissingContract,
    #[error("action references a missing schema declaration")]
    MissingSchema,
    #[error("manifest does not declare the requested V2 requirement")]
    MissingV2Declaration,
    #[error("contract descriptor exceeds its documented size or count limit")]
    DescriptorLimitExceeded,
    #[error("contract descriptor is outside the supported canonical JSON domain")]
    UnsupportedDescriptorDomain,
}
