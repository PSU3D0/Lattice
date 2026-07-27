use broker_auth::{ActivationEngine, ApprovedRegistry};
use broker_core::BrokerError;
use sha2::{Digest, Sha256};

use crate::{management::ManagementProfile, registry::StaticRegistry};

#[derive(Clone)]
pub struct InstalledContract {
    pub contract_id: &'static str,
    pub contract_hash: &'static str,
    pub semantic_effect_slot: &'static str,
    pub required_claim: &'static str,
    pub origin: &'static str,
    pub implementation_hash: &'static str,
    pub descriptor: &'static [u8],
    pub authority_facts_jcs: &'static [u8],
    pub planner: broker_auth::RegistryPin,
    pub projector: broker_auth::RegistryPin,
    pub response_firewall: broker_auth::RegistryPin,
}

pub struct InstalledProfilePins {
    pub auth_driver: broker_auth::RegistryPin,
    pub custodian: broker_auth::RegistryPin,
    pub transport: broker_auth::RegistryPin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationDriverDispatch {
    OAuthAuthorizationCodePkce,
    StaticSecret,
    WorkloadBinding,
    ExternalCustodian,
}

pub fn activation_driver_dispatch(now: &str) -> Result<ActivationDriverDispatch, BrokerError> {
    let _registry = verified_google_registry(now)?;
    let descriptor: serde_json::Value = serde_json::from_slice(&installed_profile_descriptor()?)
        .map_err(|_| BrokerError::Brk004)?;
    match descriptor
        .pointer("/activation_kind/kind")
        .and_then(serde_json::Value::as_str)
    {
        Some("oauth_authorization_code_pkce") => {
            Ok(ActivationDriverDispatch::OAuthAuthorizationCodePkce)
        }
        Some("static_secret") => Ok(ActivationDriverDispatch::StaticSecret),
        Some("workload_binding") => Ok(ActivationDriverDispatch::WorkloadBinding),
        Some("external_custodian") => Ok(ActivationDriverDispatch::ExternalCustodian),
        _ => Err(BrokerError::Brk108),
    }
}

pub struct InstalledProviderPlane {
    pub management: ManagementProfile,
    pub auth_profile_pin: broker_auth::RegistryPin,
    pub custodian_pin: broker_auth::RegistryPin,
    pub transport_pin: broker_auth::RegistryPin,
    pub registry_decision_set_hash: String,
    pub token_service_binding: &'static str,
    pub provider_service_binding: &'static str,
}

pub fn installed_provider_plane(now: &str) -> Result<InstalledProviderPlane, BrokerError> {
    let composition = provider_google::verified_provider_plane(now)?;
    let bundle = provider_google::signed_registry::signed_registry_bundle()?;
    let mut registry_evidence = bundle
        .seeds
        .iter()
        .map(|(definition, decision)| {
            format!(
                "sha256:{}:sha256:{}",
                hex::encode(Sha256::digest(definition)),
                hex::encode(Sha256::digest(decision))
            )
        })
        .collect::<Vec<_>>();
    registry_evidence.sort();
    let registry_decision_set_hash = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(registry_evidence.join("\n").as_bytes()))
    );
    let auth_profile_pin = broker_auth::RegistryPin {
        entry_ref: bundle.profile_entry_ref.into(),
        version: composition.profile.version.clone(),
        definition_hash: composition.profile.definition_hash.clone(),
        approval_epoch: 1,
        revocation_epoch: 0,
    };
    let custodian_pin = composition.profile.custodian.clone();
    let transport_pin = composition.profile.transport.clone();
    let management = ManagementProfile {
        connector_ref: composition.profile.connector_ref,
        auth_profile_ref: format!(
            "{}@{}",
            composition.profile.profile_ref, composition.profile.version
        ),
        execution_lane: provider_google::EXECUTION_LANE.into(),
        custody: provider_google::CUSTODY_LOCATION.into(),
        normalized_claims: composition
            .profile
            .contract_claims
            .values()
            .flat_map(|claims| claims.iter().cloned())
            .chain(composition.profile.connection_claims.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
        token_service_binding: composition.token_service_binding.into(),
    };
    Ok(InstalledProviderPlane {
        management,
        auth_profile_pin,
        custodian_pin,
        transport_pin,
        registry_decision_set_hash,
        token_service_binding: composition.token_service_binding,
        provider_service_binding: composition.provider_service_binding,
    })
}

fn neutral_contract(adapter: provider_google::AdapterRegistration) -> InstalledContract {
    InstalledContract {
        contract_id: adapter.contract_id,
        contract_hash: adapter.contract_hash,
        semantic_effect_slot: adapter.semantic_effect_slot,
        required_claim: adapter.required_claim,
        origin: adapter.origin,
        implementation_hash: adapter.implementation_hash,
        descriptor: adapter.descriptor,
        authority_facts_jcs: adapter.authority_facts_jcs,
        planner: adapter.planner,
        projector: adapter.projector,
        response_firewall: adapter.response_firewall,
    }
}

pub fn installed_profile_pins(now: &str) -> Result<InstalledProfilePins, BrokerError> {
    let profile = provider_google::verified_provider_plane(now)?.profile;
    Ok(InstalledProfilePins {
        auth_driver: profile.auth_driver,
        custodian: profile.custodian,
        transport: profile.transport,
    })
}

pub fn installed_contract(contract_id: &str, now: &str) -> Result<InstalledContract, BrokerError> {
    provider_google::contract_registration(contract_id, now).map(neutral_contract)
}

pub fn installed_contracts(now: &str) -> Result<Vec<InstalledContract>, BrokerError> {
    Ok(provider_google::contract_registrations(now)?
        .into_iter()
        .map(neutral_contract)
        .collect())
}

pub fn authority_facts_for_input(
    contract_id: &str,
    input: &serde_json::Value,
) -> Result<Vec<u8>, BrokerError> {
    let facts = match contract_id {
        "connector.google.sheets.append_row@1" => {
            let mut headers = input
                .get("row")
                .and_then(serde_json::Value::as_object)
                .ok_or(BrokerError::Brk001)?
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            headers.sort();
            serde_json::json!({"google":{"sheets":{"headers": headers}}})
        }
        "connector.google.gmail.send_message@1" => serde_json::json!({"allowed":true}),
        _ => return Err(BrokerError::Brk004),
    };
    Ok(broker_core::canonical::from_serde(&facts, 64 * 1024)?.into_bytes())
}

pub fn installed_profile_descriptor() -> Result<Vec<u8>, BrokerError> {
    let bundle = provider_google::signed_registry::signed_registry_bundle()?;
    for (definition, _) in bundle.seeds {
        let parsed = broker_core::credential::parse::<
            broker_core::credential::registry::RegistryDefinitionV2,
        >(&definition)?;
        if parsed
            .view
            .as_value()
            .get("entry_ref")
            .and_then(serde_json::Value::as_str)
            == Some(bundle.profile_entry_ref)
        {
            let descriptor = parsed
                .view
                .as_value()
                .pointer("/class_payload/descriptor")
                .ok_or(BrokerError::Brk004)?;
            return Ok(broker_core::canonical::from_serde(descriptor, 1024 * 1024)?.into_bytes());
        }
    }
    Err(BrokerError::Brk004)
}

pub fn trusted_host_registry(
    now: &str,
) -> Result<broker_host::TrustedAdapterRegistry, BrokerError> {
    provider_google::host_registry(now)
}

/// The only provider-aware worker composition root. Generic worker modules
/// consume the returned profile/registry data and never branch on providers.
pub fn verified_google_registry(now: &str) -> Result<StaticRegistry, BrokerError> {
    let bundle = provider_google::signed_registry::signed_registry_bundle()?;
    StaticRegistry::load(
        &bundle.seeds,
        &bundle.publisher_roots,
        &bundle.decision_roots,
        now,
    )
}

pub fn approved_activation_engine(
    commitment_key: [u8; 32],
    recipient_key_id: &str,
    now: &str,
) -> Result<ActivationEngine, BrokerError> {
    let _verified = verified_google_registry(now)?;
    Ok(ActivationEngine::new(
        ApprovedRegistry::load_static([provider_google::verified_provider_plane(now)?.profile])?,
        commitment_key,
        recipient_key_id,
    ))
}

pub fn management_profile(now: &str) -> Result<ManagementProfile, BrokerError> {
    installed_provider_plane(now).map(|plane| plane.management)
}
