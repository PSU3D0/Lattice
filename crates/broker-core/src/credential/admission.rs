use crate::{BrokerError, canonical};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) type LifecycleArtifacts = BTreeMap<String, Vec<Value>>;

fn artifact<'a>(artifacts: &'a LifecycleArtifacts, name: &str) -> Result<&'a Value, BrokerError> {
    let values = artifacts.get(name).ok_or(BrokerError::Brk109)?;
    if values.len() != 1 {
        return Err(BrokerError::Brk109);
    }
    Ok(&values[0])
}

fn differs(left: &Value, right: &Value, fields: &[&str]) -> bool {
    fields
        .iter()
        .any(|field| left.get(*field) != right.get(*field))
}

fn hash_of(value: &Value) -> Result<String, BrokerError> {
    Ok(canonical::from_serde(value, 1024 * 1024)?.sha256())
}

pub(crate) fn lifecycle_relationship_rejections(
    artifacts: &LifecycleArtifacts,
    context: &Value,
) -> Result<BTreeSet<String>, BrokerError> {
    let mut errors = BTreeSet::new();
    let hashes: BTreeMap<String, String> = artifacts
        .iter()
        .filter(|(_, values)| values.len() == 1)
        .map(|(name, values)| Ok((name.clone(), hash_of(&values[0])?)))
        .collect::<Result<_, BrokerError>>()?;
    let observation = artifact(artifacts, "AuthorizationObservation")?;
    let grant = artifact(artifacts, "ProviderGrantVersion")?;
    let adoption = artifact(artifacts, "ProviderGrantAdoptionRecord")?;
    let standing = artifact(artifacts, "StandingAuthority")?;
    let contracts = artifact(artifacts, "ContractSet")?;
    let policy = artifact(artifacts, "PolicyInstance")?;
    let definitions = artifacts
        .get("RegistryDefinition")
        .filter(|values| !values.is_empty())
        .ok_or(BrokerError::Brk109)?;
    let decisions = artifacts
        .get("RegistryDecision")
        .filter(|values| !values.is_empty())
        .ok_or(BrokerError::Brk109)?;
    let vector = artifact(artifacts, "RegistryDecisionVector")?;
    let binding = artifact(artifacts, "CorrectedBindingAttestation")?;
    let admission = artifact(artifacts, "DispatchAdmission")?;
    let receipt = artifact(artifacts, "InvocationReceipt")?;
    if super::receipt::verify_lifecycle_terminal_consistency(receipt).is_err() {
        errors.insert("receipt_terminal_state_inconsistent".into());
    }
    let inventory = artifact(artifacts, "LegacyAttemptInventory")?;
    let acceptance = grant
        .get("provider_grant_acceptance")
        .ok_or(BrokerError::Brk109)?;
    let expected_hash = |name: &str| hashes.get(name).map(String::as_str);

    let mut definitions_by_ref = BTreeMap::new();
    for definition in definitions {
        let reference = definition
            .get("registry_definition_ref")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?;
        if definitions_by_ref.insert(reference, definition).is_some() {
            errors.insert("registry_definition_duplicate".into());
        }
    }
    let mut decisions_by_ref = BTreeMap::new();
    for decision in decisions {
        let reference = decision
            .get("registry_decision_ref")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?;
        if decisions_by_ref.insert(reference, decision).is_some() {
            errors.insert("registry_decision_duplicate".into());
        }
    }
    let entries = vector
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(BrokerError::Brk109)?;
    let mut used_definitions = BTreeSet::new();
    let mut used_decisions = BTreeSet::new();
    let mut entry_pairs = BTreeSet::new();
    for entry in entries {
        let definition_ref = entry
            .get("entry_ref")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?;
        let decision_ref = entry
            .get("decision_ref")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?;
        if !entry_pairs.insert((definition_ref, decision_ref)) {
            errors.insert("registry_decision_vector_duplicate_entry".into());
            continue;
        }
        let (Some(definition), Some(decision)) = (
            definitions_by_ref.get(definition_ref),
            decisions_by_ref.get(decision_ref),
        ) else {
            errors.insert("registry_decision_vector_unbacked_entry".into());
            errors.insert("registry_decision_vector_mismatch".into());
            continue;
        };
        if !used_definitions.insert(definition_ref) || !used_decisions.insert(decision_ref) {
            errors.insert("registry_decision_vector_duplicate_entry".into());
        }
        if decision.get("tenant_id") != definition.get("tenant_id")
            || decision.get("deployment_id") != definition.get("deployment_id")
            || vector.get("tenant_id") != decision.get("tenant_id")
            || vector.get("deployment_id") != decision.get("deployment_id")
        {
            errors.insert("registry_decision_vector_tenant_deployment_mismatch".into());
        }
        if decision.get("registry_definition_ref") != definition.get("registry_definition_ref")
            || decision.get("definition_hash") != definition.get("definition_hash")
            || decision
                .get("registry_definition_artifact_hash")
                .and_then(Value::as_str)
                != Some(hash_of(definition)?.as_str())
        {
            errors.insert("registry_definition_decision_mismatch".into());
        }
        if entry.get("definition_hash") != decision.get("definition_hash")
            || entry.get("decision_epoch") != decision.get("decision_epoch")
            || entry.get("decision_hash").and_then(Value::as_str)
                != Some(hash_of(decision)?.as_str())
        {
            errors.insert("registry_decision_vector_mismatch".into());
        }
    }
    if used_definitions.len() != definitions.len() || used_decisions.len() != decisions.len() {
        errors.insert("registry_decision_vector_extra_backing_artifact".into());
    }

    if differs(
        grant,
        observation,
        &[
            "tenant_id",
            "provider",
            "auth_profile_ref",
            "auth_profile_version",
            "oauth_client_audience_commitment",
            "account_subject_commitment",
        ],
    ) {
        errors.insert("observation_grant_account_mismatch".into());
    }
    if grant.get("source_observation_ref") != observation.get("observation_ref")
        || grant.get("source_observation_hash").and_then(Value::as_str)
            != expected_hash("AuthorizationObservation")
    {
        errors.insert("grant_source_observation_mismatch".into());
    }
    if acceptance.get("observed_claims_commitment")
        != observation.get("normalized_claims_commitment")
    {
        errors.insert("grant_observed_claims_mismatch".into());
    }
    if acceptance.get("accepted_claims_commitment") != grant.get("accepted_claims_commitment") {
        errors.insert("grant_nested_accepted_claims_mismatch".into());
    }
    for field in [
        "provider",
        "auth_profile_ref",
        "auth_profile_version",
        "oauth_client_audience_commitment",
        "account_subject_commitment",
        "provider_grant_lineage_ref",
        "accepted_claims_profile_ref",
        "accepted_claims_commitment",
    ] {
        if acceptance.get(field) != grant.get(field) {
            errors.insert("grant_nested_authority_partition_mismatch".into());
        }
    }
    if grant
        .get("operation_claim_coverage")
        .and_then(Value::as_str)
        != Some("subset_or_equal")
    {
        errors.insert("operation_claim_coverage_not_subset_or_equal".into());
    }
    if grant.get("provider").and_then(Value::as_str) == Some("google")
        && acceptance.get("relation").and_then(Value::as_str) != Some("exact")
    {
        errors.insert("google_provider_grant_acceptance_not_exact".into());
    }

    if adoption.get("provider_grant_version_ref") != grant.get("provider_grant_version_ref") {
        errors.insert("adoption_grant_ref_mismatch".into());
    }
    if differs(
        adoption,
        grant,
        &[
            "tenant_id",
            "provider_grant_lineage_ref",
            "account_subject_commitment",
            "custody_ref",
            "source_observation_ref",
            "source_observation_hash",
            "provider_authority_epoch",
        ],
    ) {
        errors.insert("adoption_grant_authority_mismatch".into());
    }
    if adoption
        .get("provider_grant_version_hash")
        .and_then(Value::as_str)
        != expected_hash("ProviderGrantVersion")
    {
        errors.insert("adoption_grant_hash_mismatch".into());
    }

    if binding.get("provider_grant_version_ref") != grant.get("provider_grant_version_ref") {
        errors.insert("binding_grant_ref_mismatch".into());
    }
    if binding.get("provider_grant_lineage_ref") != grant.get("provider_grant_lineage_ref") {
        errors.insert("binding_grant_lineage_mismatch".into());
    }
    if binding.get("account_subject_commitment") != grant.get("account_subject_commitment") {
        errors.insert("binding_grant_account_mismatch".into());
    }
    if binding
        .get("provider_grant_version_hash")
        .and_then(Value::as_str)
        != expected_hash("ProviderGrantVersion")
    {
        errors.insert("binding_grant_hash_mismatch".into());
    }
    if binding.pointer("/connection_budget_partition/provider_grant_lineage_ref")
        != grant.get("provider_grant_lineage_ref")
    {
        errors.insert("binding_connection_budget_lineage_mismatch".into());
    }
    for field in [
        "provider",
        "auth_profile_ref",
        "auth_profile_version",
        "account_subject_commitment",
    ] {
        if binding.pointer(&format!("/account_budget_partition/{field}")) != grant.get(field) {
            errors.insert("binding_account_budget_partition_mismatch".into());
        }
    }
    let acl = context.get("acl_record").ok_or(BrokerError::Brk109)?;
    let mut acl_canonical = acl.as_object().cloned().ok_or(BrokerError::Brk109)?;
    acl_canonical.remove("record_commitment");
    let record_hash = acl_canonical
        .remove("record_hash")
        .ok_or(BrokerError::Brk109)?;
    if record_hash.as_str() != Some(hash_of(&Value::Object(acl_canonical))?.as_str()) {
        errors.insert("acl_record_hash_mismatch".into());
    }
    for (artifact_field, acl_field, reason) in [
        ("tenant_id", "tenant_id", "binding_acl_record_mismatch"),
        (
            "deployment_id",
            "deployment_id",
            "binding_acl_record_mismatch",
        ),
        (
            "actor_subject_commitment",
            "actor_subject_commitment",
            "binding_acl_record_mismatch",
        ),
        (
            "provider_grant_version_ref",
            "provider_grant_version_ref",
            "binding_acl_record_mismatch",
        ),
        (
            "account_subject_commitment",
            "account_subject_commitment",
            "binding_acl_record_mismatch",
        ),
        ("acl_epoch", "acl_epoch", "binding_acl_record_mismatch"),
        (
            "acl_selector_hash",
            "selector_hash",
            "binding_acl_selector_mismatch",
        ),
        (
            "acl_record_commitment",
            "record_commitment",
            "binding_acl_record_mismatch",
        ),
        (
            "acl_record_hash",
            "record_hash",
            "binding_acl_record_mismatch",
        ),
    ] {
        if binding.get(artifact_field) != acl.get(acl_field) {
            errors.insert(reason.into());
        }
    }
    for (name, reference, hash_field, target) in [
        (
            "StandingAuthority",
            "standing_authority_ref",
            "standing_authority_hash",
            standing,
        ),
        (
            "ContractSet",
            "contract_set_ref",
            "contract_set_hash",
            contracts,
        ),
        (
            "PolicyInstance",
            "policy_instance_ref",
            "policy_instance_hash",
            policy,
        ),
        (
            "RegistryDecisionVector",
            "registry_vector_ref",
            "registry_vector_hash",
            vector,
        ),
    ] {
        if binding.get(reference) != target.get(reference)
            || binding.get(hash_field).and_then(Value::as_str) != expected_hash(name)
        {
            errors.insert("binding_current_head_mismatch".into());
        }
        if target.get("tenant_id") != binding.get("tenant_id")
            || target.get("deployment_id") != binding.get("deployment_id")
        {
            errors.insert("binding_head_tenant_deployment_mismatch".into());
        }
    }
    if binding.get("registry_vector_epoch") != vector.get("vector_epoch") {
        errors.insert("binding_registry_vector_epoch_mismatch".into());
    }

    if admission.get("tenant_id") != binding.get("tenant_id")
        || admission.get("deployment_id") != binding.get("deployment_id")
    {
        errors.insert("admission_binding_tenant_deployment_mismatch".into());
    }
    if admission.get("binding_ref") != binding.get("binding_ref") {
        errors.insert("admission_binding_ref_mismatch".into());
    }
    if admission.get("binding_hash").and_then(Value::as_str)
        != expected_hash("CorrectedBindingAttestation")
    {
        errors.insert("admission_binding_hash_mismatch".into());
    }
    if differs(
        admission,
        grant,
        &[
            "provider_grant_version_ref",
            "provider_grant_lineage_ref",
            "account_subject_commitment",
        ],
    ) || admission
        .get("provider_grant_version_hash")
        .and_then(Value::as_str)
        != expected_hash("ProviderGrantVersion")
    {
        errors.insert("admission_provider_grant_mismatch".into());
    }
    for field in [
        "actor_subject_commitment",
        "acl_epoch",
        "acl_selector_hash",
        "acl_record_commitment",
        "acl_record_hash",
    ] {
        if admission.get(field) != binding.get(field) {
            errors.insert("admission_acl_mismatch".into());
        }
    }
    if admission.get("registry_vector_ref") != vector.get("registry_vector_ref")
        || admission
            .get("registry_vector_hash")
            .and_then(Value::as_str)
            != expected_hash("RegistryDecisionVector")
        || admission.get("registry_vector_epoch") != vector.get("vector_epoch")
    {
        errors.insert("admission_registry_vector_mismatch".into());
    }
    if admission.get("connection_budget_partition") != binding.get("connection_budget_partition") {
        errors.insert("admission_connection_budget_lineage_mismatch".into());
    }
    if admission.get("account_budget_partition") != binding.get("account_budget_partition") {
        errors.insert("admission_account_budget_partition_mismatch".into());
    }
    let connection_partition_present = admission
        .pointer("/run_budget_snapshot/connection_partitions")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("provider_grant_lineage_ref") == grant.get("provider_grant_lineage_ref")
            })
        });
    if !connection_partition_present {
        errors.insert("admission_run_ledger_connection_partition_missing".into());
    }
    let account_partition_present = admission
        .pointer("/run_budget_snapshot/account_partitions")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                !differs(
                    item,
                    grant,
                    &[
                        "provider",
                        "auth_profile_ref",
                        "auth_profile_version",
                        "account_subject_commitment",
                    ],
                )
            })
        });
    if !account_partition_present {
        errors.insert("admission_run_ledger_account_partition_missing".into());
    }
    let admitted_contract = contracts
        .get("contracts")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("contract_id") == admission.get("contract_id")
                    && item.get("contract_hash") == admission.get("contract_hash")
            })
        });
    if !admitted_contract {
        errors.insert("admission_contract_not_in_contract_set".into());
    }

    for field in [
        "tenant_id",
        "deployment_id",
        "run_id",
        "logical_effect_id",
        "attempt",
        "provider_grant_version_ref",
        "provider_grant_lineage_ref",
        "account_subject_commitment",
        "actor_subject_commitment",
        "acl_epoch",
        "acl_selector_hash",
        "acl_record_commitment",
        "acl_record_hash",
        "binding_ref",
        "canonical_input_commitment",
        "registry_vector_ref",
        "registry_vector_hash",
        "registry_vector_epoch",
    ] {
        if receipt.get(field) != admission.get(field) {
            errors.insert("receipt_admission_fields_mismatch".into());
        }
    }
    if receipt.get("dispatch_admission_ref") != admission.get("admission_ref") {
        errors.insert("receipt_admission_hash_mismatch".into());
    }
    if receipt
        .get("provider_grant_version_hash")
        .and_then(Value::as_str)
        != expected_hash("ProviderGrantVersion")
    {
        errors.insert("receipt_provider_grant_hash_mismatch".into());
    }
    for (field, target) in [
        ("standing_authority_hash", "StandingAuthority"),
        ("contract_set_hash", "ContractSet"),
        ("policy_instance_hash", "PolicyInstance"),
        ("registry_vector_hash", "RegistryDecisionVector"),
    ] {
        if receipt.get(field).and_then(Value::as_str) != expected_hash(target) {
            errors.insert("receipt_current_head_mismatch".into());
        }
    }
    if receipt
        .get("dispatch_admission_hash")
        .and_then(Value::as_str)
        != expected_hash("DispatchAdmission")
    {
        errors.insert("receipt_admission_hash_mismatch".into());
    }
    if receipt.get("account_subject_commitment") != admission.get("account_subject_commitment") {
        errors.insert("receipt_account_mismatch".into());
    }

    let snapshot = context
        .get("legacy_staging_snapshot")
        .ok_or(BrokerError::Brk109)?;
    if snapshot.get("snapshot_hash").and_then(Value::as_str)
        != Some(hash_of(snapshot.get("snapshot_record").ok_or(BrokerError::Brk109)?)?.as_str())
    {
        errors.insert("legacy_snapshot_record_hash_mismatch".into());
    }
    if inventory
        .get("source_attempt_count")
        .and_then(Value::as_u64)
        != inventory
            .get("entries")
            .and_then(Value::as_array)
            .map(|items| items.len() as u64)
    {
        errors.insert("legacy_source_count_mismatch".into());
    }
    if inventory.get("accepted_staging_snapshot_ref") != snapshot.get("snapshot_ref")
        || inventory.get("accepted_staging_snapshot_hash") != snapshot.get("snapshot_hash")
    {
        errors.insert("legacy_snapshot_mismatch".into());
    }
    let source_tuple = |item: &Value| {
        (
            item.get("source_kind")
                .and_then(Value::as_str)
                .map(str::to_owned),
            item.get("source_record_ref")
                .and_then(Value::as_str)
                .map(str::to_owned),
            item.get("source_record_hash")
                .and_then(Value::as_str)
                .map(str::to_owned),
        )
    };
    let expected_sources: BTreeSet<_> = snapshot
        .get("sources")
        .and_then(Value::as_array)
        .ok_or(BrokerError::Brk109)?
        .iter()
        .map(source_tuple)
        .collect();
    let actual_sources: BTreeSet<_> = inventory
        .get("entries")
        .and_then(Value::as_array)
        .ok_or(BrokerError::Brk109)?
        .iter()
        .map(source_tuple)
        .collect();
    if actual_sources != expected_sources {
        errors.insert("legacy_source_record_mismatch".into());
    }
    Ok(errors)
}

/// Opaque authority proof returned only by raw-byte graph verification.
///
/// Directly deserialized protocol data cannot be promoted to authority.
///
/// ```compile_fail
/// use broker_core::credential::{
///     admission::VerifiedLifecycleGraph,
///     lifecycle::InvocationReceiptLs1,
/// };
/// fn promote(receipt: InvocationReceiptLs1) -> VerifiedLifecycleGraph {
///     receipt
/// }
/// ```
pub struct VerifiedLifecycleGraph {
    _private: (),
}

const REQUIRED_LIFECYCLE_SCHEMAS: [&str; 18] = [
    "LS1AuthorityModelCutover",
    "LS1AuthorizationObservation",
    "LS1ProviderGrantVersion",
    "LS1ProviderGrantAdoptionRecord",
    "LS1ConnectionAliasRecord",
    "LS1StandingAuthority",
    "LS1ContractSet",
    "LS1PolicyInstance",
    "LS1RegistryDefinition",
    "LS1RegistryDecision",
    "LS1RegistryDecisionVector",
    "LS1CeilingAmendment",
    "LS1CorrectedBindingAttestation",
    "LS1DispatchAdmission",
    "LS1InvocationReceipt",
    "LS1LegacyAttemptInventory",
    "LS1ReceiptVerificationKeyset",
    "LS1ReceiptKeyCompromiseRecord",
];

pub fn verify_lifecycle_graph_bytes(
    artifacts: &[(&str, &[u8])],
    relationship_context: &[u8],
    pinned_verifying_keys: &BTreeMap<String, crate::signing::BrokerVerifyingKey>,
) -> Result<VerifiedLifecycleGraph, BrokerError> {
    let mut parsed: LifecycleArtifacts = BTreeMap::new();
    for (schema, bytes) in artifacts {
        if !REQUIRED_LIFECYCLE_SCHEMAS.contains(schema) {
            return Err(BrokerError::Brk109);
        }
        let maximum = if *schema == "LS1InvocationReceipt" {
            64 * 1024
        } else {
            canonical::MAX_OPERATION_BYTES
        };
        let canonical = canonical::canonicalize_bounded(bytes, maximum)?;
        let value: Value =
            serde_json::from_slice(canonical.as_bytes()).map_err(|_| BrokerError::Brk109)?;
        let class = schema.strip_prefix("LS1").ok_or(BrokerError::Brk109)?;
        if value.get("artifact_type").and_then(Value::as_str) != Some(class) {
            return Err(BrokerError::Brk109);
        }
        let key_id = value
            .get("key_id")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?;
        let key = pinned_verifying_keys
            .get(key_id)
            .ok_or(BrokerError::Brk109)?;
        super::signing::verify_lifecycle_value(schema, &value, key)
            .map_err(|_| BrokerError::Brk109)?;
        let values = parsed.entry(class.to_owned()).or_default();
        if !matches!(class, "RegistryDefinition" | "RegistryDecision") && !values.is_empty() {
            return Err(BrokerError::Brk109);
        }
        values.push(value);
    }
    if REQUIRED_LIFECYCLE_SCHEMAS.iter().any(|schema| {
        schema
            .strip_prefix("LS1")
            .is_none_or(|class| !parsed.contains_key(class))
    }) {
        return Err(BrokerError::Brk109);
    }
    let context =
        canonical::canonicalize_bounded(relationship_context, canonical::MAX_OPERATION_BYTES)?;
    let context: Value =
        serde_json::from_slice(context.as_bytes()).map_err(|_| BrokerError::Brk109)?;
    if !lifecycle_relationship_rejections(&parsed, &context)?.is_empty() {
        return Err(BrokerError::Brk109);
    }
    Ok(VerifiedLifecycleGraph { _private: () })
}
