use std::collections::BTreeMap;

use broker_core::{BrokerError, signing::BrokerVerifyingKey};
#[cfg(test)]
use broker_core::{
    canonical,
    credential::{parse, registry::RegistryDefinitionV2, signing::REGISTRY_DECISION_DOMAIN},
    signing::BrokerSigner,
};

const KEY_ID: &str = "google-registry-root-1";
#[cfg(test)]
const HASH_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PROFILE_ENTRY: &str = "registry.auth-profile.google.workspace.oauth2";

pub struct SignedRegistryBundle {
    pub seeds: Vec<(Vec<u8>, Vec<u8>)>,
    pub publisher_roots: BTreeMap<String, BrokerVerifyingKey>,
    pub decision_roots: BTreeMap<String, BrokerVerifyingKey>,
    pub profile_entry_ref: &'static str,
}

pub fn signed_registry_bundle() -> Result<SignedRegistryBundle, BrokerError> {
    #[derive(serde::Deserialize)]
    struct StaticBundle {
        seeds: Vec<StaticSeed>,
    }
    #[derive(serde::Deserialize)]
    struct StaticSeed {
        definition: String,
        decision: String,
    }
    let source: StaticBundle =
        serde_json::from_slice(include_bytes!("google-registry-bundle.json"))
            .map_err(|_| BrokerError::Brk401)?;
    let verifying = BrokerVerifyingKey::from_bytes(
        KEY_ID,
        [
            250, 72, 52, 20, 127, 110, 105, 12, 54, 147, 239, 246, 19, 54, 4, 100, 3, 205, 138,
            226, 161, 79, 49, 179, 196, 7, 53, 133, 105, 35, 149, 101,
        ],
    )?;
    Ok(SignedRegistryBundle {
        seeds: source
            .seeds
            .into_iter()
            .map(|seed| (seed.definition.into_bytes(), seed.decision.into_bytes()))
            .collect(),
        publisher_roots: BTreeMap::from([(KEY_ID.into(), verifying.clone())]),
        decision_roots: BTreeMap::from([(KEY_ID.into(), verifying)]),
        profile_entry_ref: PROFILE_ENTRY,
    })
}

#[cfg(test)]
pub fn deterministic_decision(
    entry_ref: &str,
    definition_hash: &str,
    approval_status: &str,
    revocation_status: &str,
) -> Result<Vec<u8>, BrokerError> {
    sign_decision(
        &BrokerSigner::from_seed(KEY_ID, [41; 32]),
        entry_ref,
        definition_hash,
        approval_status,
        revocation_status,
    )
}

#[cfg(test)]
fn sign_decision(
    signer: &BrokerSigner,
    entry_ref: &str,
    definition_hash: &str,
    approval_status: &str,
    revocation_status: &str,
) -> Result<Vec<u8>, BrokerError> {
    let mut value = serde_json::json!({
        "schema_version":"0.2","critical_fields":[],"extensions":{},
        "entry_ref":entry_ref,"version":"1","definition_hash":definition_hash,
        "approval_status":approval_status,"approval_epoch":1,
        "revocation_status":revocation_status,"revocation_epoch":if revocation_status == "active" { 0 } else { 1 },
        "authority_ref":"lattice.registry.approver","authority_key_id":KEY_ID,
        "policy_hash":HASH_A,"not_before":"2026-01-01T00:00:00Z",
        "expires_at":"2035-01-01T00:00:00Z",
        "signature":{"alg":"Ed25519","key_id":KEY_ID,"value":"placeholder"}
    });
    value["signature"] = serde_json::to_value(signer.sign_json(
        REGISTRY_DECISION_DOMAIN,
        &serde_json::to_vec(&value).map_err(|_| BrokerError::Brk401)?,
    )?)
    .map_err(|_| BrokerError::Brk401)?;
    Ok(canonical::from_serde(&value, 1024 * 1024)?.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::credential::{registry::RegistryDecisionV2, signing::verify_signed};

    #[test]
    fn all_seed_records_have_valid_signatures_hashes_and_classes() {
        let bundle = signed_registry_bundle().unwrap();
        assert_eq!(bundle.seeds.len(), 10);
        for (definition, decision) in bundle.seeds {
            let definition = parse::<RegistryDefinitionV2>(&definition).unwrap();
            let decision = parse::<RegistryDecisionV2>(&decision).unwrap();
            verify_signed(&definition, &bundle.publisher_roots[KEY_ID]).unwrap();
            verify_signed(&decision, &bundle.decision_roots[KEY_ID]).unwrap();
            assert_eq!(
                decision.view.as_value()["definition_hash"],
                definition.content_hash()
            );
            assert_eq!(
                definition.view.as_value()["class"],
                definition.view.as_value()["class_payload"]["kind"]
            );
        }
    }
}
