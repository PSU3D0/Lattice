use broker_auth::{ActivationEngine, ApprovedRegistry};
use broker_core::BrokerError;

use crate::{management::ManagementProfile, registry::StaticRegistry};

/// The only provider-aware worker composition root. Generic worker modules
/// consume the returned profile/registry data and never branch on providers.
pub fn verified_google_registry(now: &str) -> Result<StaticRegistry, BrokerError> {
    let bundle = provider_google::signed_registry::deterministic_signed_registry()?;
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
) -> Result<ActivationEngine, BrokerError> {
    let now = "2026-07-21T00:00:00Z";
    let _verified = verified_google_registry(now)?;
    Ok(ActivationEngine::new(
        ApprovedRegistry::load_static([provider_google::verified_google_v1(now)?.profile])?,
        commitment_key,
        recipient_key_id,
    ))
}

pub fn legacy_management_profile() -> Result<ManagementProfile, BrokerError> {
    let composition = provider_google::verified_google_v1("2026-07-21T00:00:00Z")?;
    Ok(ManagementProfile {
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
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
        token_service_binding: composition.token_service_binding.into(),
    })
}
