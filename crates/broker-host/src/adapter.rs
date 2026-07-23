use std::{collections::BTreeMap, sync::Arc};

use broker_core::{
    BrokerError,
    artifacts::SignatureEnvelope,
    credential::{
        ParsedV2, parse,
        registry::{RegistryDecisionV2, RegistryDefinitionV2},
        signing::{REGISTRY_DECISION_DOMAIN, REGISTRY_DEFINITION_DOMAIN},
    },
    signing::BrokerVerifyingKey,
};
use connector_spec::TrustedAdapterPin;

use crate::BrokerHostError;

/// Credential-blind operation planner installed by the application composition
/// root. Implementations receive only public input and public authority facts.
pub trait CredentialBlindPlannerImplementation: Send + Sync {
    fn plan(
        &self,
        input: &serde_json::Value,
        public_authority: &serde_json::Value,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerError>;
}

/// Credential-blind response projector installed by the application root.
pub trait ResponseProjectorImplementation: Send + Sync {
    fn project(
        &self,
        scrubbed_response: &serde_json::Value,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerError>;
}

/// Privileged firewall implementation. The returned value must contain no
/// credential material and is the only value visible to a projector.
pub trait ResponseFirewallImplementation: Send + Sync {
    fn scrub(&self, bounded_response: &[u8]) -> Result<serde_json::Value, BrokerError>;
}

struct VerifiedImplementation<T: ?Sized> {
    definition: ParsedV2<RegistryDefinitionV2>,
    decision: ParsedV2<RegistryDecisionV2>,
    implementation: Arc<T>,
}
impl<T: ?Sized> Clone for VerifiedImplementation<T> {
    fn clone(&self) -> Self {
        Self {
            definition: self.definition.clone(),
            decision: self.decision.clone(),
            implementation: Arc::clone(&self.implementation),
        }
    }
}

/// Provider-neutral registry of signed, currently-approved host
/// implementations. There is no provider discovery or public mutation path;
/// the authenticated application composition root installs exact entries.
#[derive(Clone, Default)]
pub struct TrustedAdapterRegistry {
    planners: BTreeMap<
        (String, String, String),
        VerifiedImplementation<dyn CredentialBlindPlannerImplementation>,
    >,
    projectors: BTreeMap<String, VerifiedImplementation<dyn ResponseProjectorImplementation>>,
    firewalls: BTreeMap<String, VerifiedImplementation<dyn ResponseFirewallImplementation>>,
    checked_at: String,
}

impl std::fmt::Debug for TrustedAdapterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrustedAdapterRegistry")
            .field("planners", &self.planners.len())
            .field("projectors", &self.projectors.len())
            .field("firewalls", &self.firewalls.len())
            .finish()
    }
}

impl TrustedAdapterRegistry {
    pub fn empty() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn install_planner(
        &mut self,
        definition_bytes: &[u8],
        decision_bytes: &[u8],
        publisher_key: &BrokerVerifyingKey,
        decision_key: &BrokerVerifyingKey,
        now: &str,
        contract_hash: &str,
        implementation: Arc<dyn CredentialBlindPlannerImplementation>,
    ) -> Result<(), BrokerHostError> {
        let verified = verify_implementation(
            definition_bytes,
            decision_bytes,
            publisher_key,
            decision_key,
            now,
            "capsule_planner",
            Some(contract_hash),
            implementation,
        )?;
        let definition = verified.definition.view.as_value();
        let key = (
            text(definition, "entry_ref")?.to_owned(),
            text(definition, "version")?.to_owned(),
            text(&definition["class_payload"], "implementation_digest")?.to_owned(),
        );
        if self.planners.insert(key, verified).is_some() {
            return Err(BrokerHostError::RegistryRejected);
        }
        self.update_checked_at(now)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn install_projector(
        &mut self,
        definition_bytes: &[u8],
        decision_bytes: &[u8],
        publisher_key: &BrokerVerifyingKey,
        decision_key: &BrokerVerifyingKey,
        now: &str,
        contract_hash: &str,
        implementation: Arc<dyn ResponseProjectorImplementation>,
    ) -> Result<(), BrokerHostError> {
        let verified = verify_implementation(
            definition_bytes,
            decision_bytes,
            publisher_key,
            decision_key,
            now,
            "response_projector",
            Some(contract_hash),
            implementation,
        )?;
        if self
            .projectors
            .insert(contract_hash.to_owned(), verified)
            .is_some()
        {
            return Err(BrokerHostError::RegistryRejected);
        }
        self.update_checked_at(now)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn install_firewall(
        &mut self,
        definition_bytes: &[u8],
        decision_bytes: &[u8],
        publisher_key: &BrokerVerifyingKey,
        decision_key: &BrokerVerifyingKey,
        now: &str,
        policy_hash: &str,
        implementation: Arc<dyn ResponseFirewallImplementation>,
    ) -> Result<(), BrokerHostError> {
        let verified = verify_implementation(
            definition_bytes,
            decision_bytes,
            publisher_key,
            decision_key,
            now,
            "privileged_response_firewall",
            None,
            implementation,
        )?;
        let policies = verified.definition.view.as_value()["class_payload"]
            .get("supported_policy_hashes")
            .and_then(serde_json::Value::as_array)
            .ok_or(BrokerHostError::RegistryRejected)?;
        if !policies
            .iter()
            .any(|value| value.as_str() == Some(policy_hash))
            || self
                .firewalls
                .insert(policy_hash.to_owned(), verified)
                .is_some()
        {
            return Err(BrokerHostError::RegistryRejected);
        }
        self.update_checked_at(now)?;
        Ok(())
    }

    pub(crate) fn adapt(
        &self,
        pin: &TrustedAdapterPin,
        input: &serde_json::Value,
        authority_facts: &serde_json::Value,
        now: &str,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerHostError> {
        let key = (
            pin.trusted_adapter_id.clone(),
            pin.implementation_version.clone(),
            pin.implementation_hash.clone(),
        );
        let entry = self
            .planners
            .get(&key)
            .ok_or(BrokerHostError::DescriptorMismatch)?;
        ensure_current(entry.decision.view.as_value(), now)?;
        entry
            .implementation
            .plan(input, authority_facts)
            .map_err(Into::into)
    }

    pub fn project(
        &self,
        contract_hash: &str,
        scrubbed_response: &serde_json::Value,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerHostError> {
        self.project_at(contract_hash, scrubbed_response, &self.checked_at)
    }

    pub fn project_at(
        &self,
        contract_hash: &str,
        scrubbed_response: &serde_json::Value,
        now: &str,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerHostError> {
        let entry = self
            .projectors
            .get(contract_hash)
            .ok_or(BrokerHostError::RegistryRejected)?;
        ensure_current(entry.decision.view.as_value(), now)?;
        entry
            .implementation
            .project(scrubbed_response)
            .map_err(Into::into)
    }

    pub fn firewall_and_project(
        &self,
        policy_hash: &str,
        contract_hash: &str,
        bounded_response: &[u8],
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerHostError> {
        let firewall = self
            .firewalls
            .get(policy_hash)
            .ok_or(BrokerHostError::RegistryRejected)?;
        ensure_current(firewall.decision.view.as_value(), &self.checked_at)?;
        let scrubbed = firewall.implementation.scrub(bounded_response)?;
        self.project(contract_hash, &scrubbed)
    }

    /// Apply a newer signed decision (including revocation) to every exact
    /// implementation with the same entry ref. Old approved decisions cannot
    /// remain executable after this application-root refresh.
    pub fn apply_decision(
        &mut self,
        decision_bytes: &[u8],
        decision_key: &BrokerVerifyingKey,
        now: &str,
    ) -> Result<(), BrokerHostError> {
        verify_raw_signature(REGISTRY_DECISION_DOMAIN, decision_bytes, decision_key)?;
        let decision = parse::<RegistryDecisionV2>(decision_bytes)
            .map_err(|_| BrokerHostError::RegistryRejected)?;
        if decision.canonical_bytes() != decision_bytes
            || decision
                .view
                .as_value()
                .get("not_before")
                .and_then(serde_json::Value::as_str)
                .is_none_or(|value| value > now)
            || decision
                .view
                .as_value()
                .get("expires_at")
                .and_then(serde_json::Value::as_str)
                .is_none_or(|value| value <= now)
        {
            return Err(BrokerHostError::RegistryRejected);
        }
        let entry_ref = text(decision.view.as_value(), "entry_ref")?;
        let mut matched = false;
        for entry in self.planners.values_mut() {
            matched |= refresh_entry(entry, entry_ref, &decision)?;
        }
        for entry in self.projectors.values_mut() {
            matched |= refresh_entry(entry, entry_ref, &decision)?;
        }
        for entry in self.firewalls.values_mut() {
            matched |= refresh_entry(entry, entry_ref, &decision)?;
        }
        if !matched {
            return Err(BrokerHostError::RegistryRejected);
        }
        self.checked_at = now.to_owned();
        Ok(())
    }

    fn update_checked_at(&mut self, now: &str) -> Result<(), BrokerHostError> {
        if !self.checked_at.is_empty() && self.checked_at != now {
            return Err(BrokerHostError::RegistryRejected);
        }
        self.checked_at = now.to_owned();
        Ok(())
    }
}

fn verify_implementation<T: ?Sized>(
    definition_bytes: &[u8],
    decision_bytes: &[u8],
    publisher_key: &BrokerVerifyingKey,
    decision_key: &BrokerVerifyingKey,
    now: &str,
    expected_class: &str,
    contract_hash: Option<&str>,
    implementation: Arc<T>,
) -> Result<VerifiedImplementation<T>, BrokerHostError> {
    // Signature first: only the envelope is decoded before verification. No
    // registry claim influences class, key, or implementation selection.
    verify_raw_signature(REGISTRY_DEFINITION_DOMAIN, definition_bytes, publisher_key)?;
    verify_raw_signature(REGISTRY_DECISION_DOMAIN, decision_bytes, decision_key)?;
    let definition = parse::<RegistryDefinitionV2>(definition_bytes)
        .map_err(|_| BrokerHostError::RegistryRejected)?;
    let decision = parse::<RegistryDecisionV2>(decision_bytes)
        .map_err(|_| BrokerHostError::RegistryRejected)?;
    if definition.canonical_bytes() != definition_bytes
        || decision.canonical_bytes() != decision_bytes
    {
        return Err(BrokerHostError::RegistryRejected);
    }
    let d = definition.view.as_value();
    let approval = decision.view.as_value();
    if d.get("class").and_then(serde_json::Value::as_str) != Some(expected_class)
        || d.get("class") != d.pointer("/class_payload/kind")
        || approval.get("entry_ref") != d.get("entry_ref")
        || approval.get("version") != d.get("version")
        || approval
            .get("definition_hash")
            .and_then(serde_json::Value::as_str)
            != Some(definition.content_hash().as_str())
        || approval
            .get("approval_status")
            .and_then(serde_json::Value::as_str)
            != Some("approved")
        || approval
            .get("revocation_status")
            .and_then(serde_json::Value::as_str)
            != Some("active")
        || approval
            .get("not_before")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|value| value > now)
        || approval
            .get("expires_at")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|value| value <= now)
    {
        return Err(BrokerHostError::RegistryRejected);
    }
    if let Some(contract_hash) = contract_hash {
        let supported = d
            .pointer("/class_payload/supported_contract_hashes")
            .and_then(serde_json::Value::as_array)
            .ok_or(BrokerHostError::RegistryRejected)?;
        if !supported
            .iter()
            .any(|value| value.as_str() == Some(contract_hash))
        {
            return Err(BrokerHostError::RegistryRejected);
        }
    }
    Ok(VerifiedImplementation {
        definition,
        decision,
        implementation,
    })
}

fn verify_raw_signature(
    domain: &str,
    bytes: &[u8],
    key: &BrokerVerifyingKey,
) -> Result<(), BrokerHostError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| BrokerHostError::RegistryRejected)?;
    let signature: SignatureEnvelope = serde_json::from_value(
        value
            .get("signature")
            .cloned()
            .ok_or(BrokerHostError::RegistryRejected)?,
    )
    .map_err(|_| BrokerHostError::RegistryRejected)?;
    key.verify_json(domain, bytes, &signature)
        .map_err(|_| BrokerHostError::RegistryRejected)
}

fn refresh_entry<T: ?Sized>(
    entry: &mut VerifiedImplementation<T>,
    entry_ref: &str,
    next: &ParsedV2<RegistryDecisionV2>,
) -> Result<bool, BrokerHostError> {
    let definition = entry.definition.view.as_value();
    if definition
        .get("entry_ref")
        .and_then(serde_json::Value::as_str)
        != Some(entry_ref)
    {
        return Ok(false);
    }
    let old = entry.decision.view.as_value();
    let new = next.view.as_value();
    if new.get("version") != definition.get("version")
        || new
            .get("definition_hash")
            .and_then(serde_json::Value::as_str)
            != Some(entry.definition.content_hash().as_str())
        || new
            .get("approval_epoch")
            .and_then(serde_json::Value::as_u64)
            < old
                .get("approval_epoch")
                .and_then(serde_json::Value::as_u64)
        || new
            .get("revocation_epoch")
            .and_then(serde_json::Value::as_u64)
            < old
                .get("revocation_epoch")
                .and_then(serde_json::Value::as_u64)
        || (new.get("approval_status") != old.get("approval_status")
            && new
                .get("approval_epoch")
                .and_then(serde_json::Value::as_u64)
                <= old
                    .get("approval_epoch")
                    .and_then(serde_json::Value::as_u64))
        || (new.get("revocation_status") != old.get("revocation_status")
            && new
                .get("revocation_epoch")
                .and_then(serde_json::Value::as_u64)
                <= old
                    .get("revocation_epoch")
                    .and_then(serde_json::Value::as_u64))
    {
        return Err(BrokerHostError::RegistryRejected);
    }
    entry.decision = next.clone();
    Ok(true)
}

fn ensure_current(decision: &serde_json::Value, now: &str) -> Result<(), BrokerHostError> {
    if decision
        .get("approval_status")
        .and_then(serde_json::Value::as_str)
        == Some("approved")
        && decision
            .get("revocation_status")
            .and_then(serde_json::Value::as_str)
            == Some("active")
        && decision
            .get("not_before")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value <= now)
        && decision
            .get("expires_at")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value > now)
    {
        Ok(())
    } else {
        Err(BrokerHostError::RegistryRejected)
    }
}

fn text<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str, BrokerHostError> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or(BrokerHostError::RegistryRejected)
}
