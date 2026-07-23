use std::collections::BTreeMap;

use broker_core::{
    BrokerError, canonical,
    credential::{
        parse,
        registry::RegistryDefinitionV2,
        signing::{REGISTRY_DECISION_DOMAIN, REGISTRY_DEFINITION_DOMAIN},
    },
    signing::{BrokerSigner, BrokerVerifyingKey},
};
use serde_json::Value;

use crate::{GMAIL_CONTRACT_HASH, PROFILE_REF, PROFILE_VERSION, SHEETS_CONTRACT_HASH};
use connector_google_platform::broker::{
    GMAIL_RFC822_ADAPTER_HASH, GMAIL_RFC822_ADAPTER_ID, SHEETS_APPEND_ROW_ADAPTER_HASH,
    SHEETS_APPEND_ROW_ADAPTER_ID,
};

const KEY_ID: &str = "google-registry-root-1";
const HASH_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PROFILE_ENTRY: &str = "registry.auth-profile.google.workspace.oauth2";
const DRIVER_ENTRY: &str = "auth-driver.google.oauth2";
const NORMALIZER_ENTRY: &str = "claim-normalizer.google.oauth-scopes";
const CUSTODIAN_ENTRY: &str = "custodian.google.oauth2";
const TRANSPORT_ENTRY: &str = "transport.google.https";
const FIREWALL_ENTRY: &str = "firewall.google.oauth-token";

pub struct SignedRegistryBundle {
    pub seeds: Vec<(Vec<u8>, Vec<u8>)>,
    pub publisher_roots: BTreeMap<String, BrokerVerifyingKey>,
    pub decision_roots: BTreeMap<String, BrokerVerifyingKey>,
    pub profile_entry_ref: &'static str,
}

pub fn deterministic_signed_registry() -> Result<SignedRegistryBundle, BrokerError> {
    let publisher = BrokerSigner::from_seed(KEY_ID, [41; 32]);
    let authority = BrokerSigner::from_seed(KEY_ID, [41; 32]);
    let mut definitions = Vec::new();
    for (entry, class, payload) in [
        (DRIVER_ENTRY, "auth_driver", auth_driver_payload()),
        (NORMALIZER_ENTRY, "claim_normalizer", normalizer_payload()),
        (CUSTODIAN_ENTRY, "custodian", custodian_payload()),
        (TRANSPORT_ENTRY, "transport", transport_payload()),
        (
            FIREWALL_ENTRY,
            "privileged_response_firewall",
            firewall_payload(),
        ),
        (
            GMAIL_RFC822_ADAPTER_ID,
            "capsule_planner",
            planner_payload(GMAIL_CONTRACT_HASH, GMAIL_RFC822_ADAPTER_HASH),
        ),
        (
            "projector.connector.google.gmail.send_message@1",
            "response_projector",
            projector_payload(GMAIL_CONTRACT_HASH, GMAIL_RFC822_ADAPTER_HASH),
        ),
        (
            SHEETS_APPEND_ROW_ADAPTER_ID,
            "capsule_planner",
            planner_payload(SHEETS_CONTRACT_HASH, SHEETS_APPEND_ROW_ADAPTER_HASH),
        ),
        (
            "projector.connector.google.sheets.append_row@1",
            "response_projector",
            projector_payload(SHEETS_CONTRACT_HASH, SHEETS_APPEND_ROW_ADAPTER_HASH),
        ),
    ] {
        let signed = sign_definition(&publisher, entry, class, payload)?;
        definitions.push((entry, class, signed));
    }
    let hash_for = |entry: &str| -> Result<String, BrokerError> {
        let bytes = definitions
            .iter()
            .find(|candidate| candidate.0 == entry)
            .map(|candidate| candidate.2.as_slice())
            .ok_or(BrokerError::Brk401)?;
        Ok(parse::<RegistryDefinitionV2>(bytes)?.content_hash())
    };
    let descriptor = profile_descriptor(&hash_for(DRIVER_ENTRY)?, &hash_for(NORMALIZER_ENTRY)?);
    let descriptor_hash = canonical::from_serde(&descriptor, 1024 * 1024)?.sha256();
    let profile_payload = serde_json::json!({
        "kind":"auth_profile",
        "descriptor":descriptor,
        "descriptor_hash":descriptor_hash,
        "conformance_corpus_hash":HASH_A,
        "conformance_result_hash":HASH_A
    });
    definitions.push((
        PROFILE_ENTRY,
        "auth_profile",
        sign_definition(&publisher, PROFILE_ENTRY, "auth_profile", profile_payload)?,
    ));

    let mut seeds = Vec::with_capacity(definitions.len());
    for (entry, _, definition) in definitions {
        let definition_hash = parse::<RegistryDefinitionV2>(&definition)?.content_hash();
        seeds.push((
            definition,
            sign_decision(&authority, entry, &definition_hash, "approved", "active")?,
        ));
    }
    let verifying = publisher.verifying_key();
    Ok(SignedRegistryBundle {
        seeds,
        publisher_roots: BTreeMap::from([(KEY_ID.into(), verifying.clone())]),
        decision_roots: BTreeMap::from([(KEY_ID.into(), verifying)]),
        profile_entry_ref: PROFILE_ENTRY,
    })
}

fn sign_definition(
    signer: &BrokerSigner,
    entry_ref: &str,
    class: &str,
    payload: Value,
) -> Result<Vec<u8>, BrokerError> {
    let mut value = serde_json::json!({
        "schema_version":"0.2","critical_fields":[],"extensions":{},
        "entry_ref":entry_ref,"version":"1","class":class,"class_payload":payload,
        "publisher_ref":"lattice.google.registry","publisher_key_id":KEY_ID,
        "published_at":"2026-01-01T00:00:00Z",
        "signature":{"alg":"Ed25519","key_id":KEY_ID,"value":"placeholder"}
    });
    value["signature"] = serde_json::to_value(signer.sign_json(
        REGISTRY_DEFINITION_DOMAIN,
        &serde_json::to_vec(&value).map_err(|_| BrokerError::Brk401)?,
    )?)
    .map_err(|_| BrokerError::Brk401)?;
    let bytes = serde_json::to_vec(&value).map_err(|_| BrokerError::Brk401)?;
    parse::<RegistryDefinitionV2>(&bytes).map(|parsed| parsed.canonical_bytes().to_vec())
}

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

fn common(kind: &str) -> Value {
    serde_json::json!({
        "kind":kind,"interface_version":"1","implementation_digest":HASH_A,
        "conformance_corpus_hash":HASH_A,"conformance_result_hash":HASH_A
    })
}
fn auth_driver_payload() -> Value {
    let mut value = common("auth_driver");
    let object = value.as_object_mut().expect("object");
    object.insert(
        "supported_scheme_refs".into(),
        serde_json::json!(["credential.oauth2.authorization_code_pkce@1"]),
    );
    object.insert(
        "supported_profile_refs".into(),
        serde_json::json!([{"profile_ref":PROFILE_REF,"version":PROFILE_VERSION}]),
    );
    object.insert(
        "privileged_response_capabilities".into(),
        serde_json::json!(["token_response_extract"]),
    );
    value
}
fn normalizer_payload() -> Value {
    let mut value = common("claim_normalizer");
    let object = value.as_object_mut().expect("object");
    object.insert(
        "input_vocabulary_ref".into(),
        Value::String("google.oauth.scope".into()),
    );
    object.insert("input_schema_hash".into(), Value::String(HASH_A.into()));
    object.insert(
        "normalized_output_schema_hash".into(),
        Value::String(HASH_A.into()),
    );
    object.insert(
        "relation_result_schema_hash".into(),
        Value::String(HASH_A.into()),
    );
    value
}
fn custodian_payload() -> Value {
    let mut value = common("custodian");
    let object = value.as_object_mut().expect("object");
    object.insert("mode".into(), Value::String("local".into()));
    object.insert("service_identity_commitment".into(), commitment());
    object.insert(
        "supported_scheme_refs".into(),
        serde_json::json!(["credential.oauth2.authorization_code_pkce@1"]),
    );
    object.insert(
        "destruction_evidence_kind".into(),
        Value::String("sealed_store_confirmation".into()),
    );
    value
}
fn transport_payload() -> Value {
    let mut value = common("transport");
    let object = value.as_object_mut().expect("object");
    object.insert("mode".into(), Value::String("local_https".into()));
    object.insert("service_identity_commitment".into(), commitment());
    object.insert("tls_policy_hash".into(), Value::String(HASH_A.into()));
    value
}
fn firewall_payload() -> Value {
    let mut value = common("privileged_response_firewall");
    let object = value.as_object_mut().expect("object");
    object.insert(
        "supported_policy_hashes".into(),
        serde_json::json!([HASH_A]),
    );
    object.insert("maximum_response_bytes".into(), Value::from(65536));
    value
}
fn planner_payload(contract_hash: &str, implementation_digest: &str) -> Value {
    let mut value = common("capsule_planner");
    value["implementation_digest"] = Value::String(implementation_digest.into());
    let object = value.as_object_mut().expect("object");
    object.insert(
        "implementation_kind".into(),
        Value::String("native_component".into()),
    );
    object.insert(
        "supported_contract_hashes".into(),
        serde_json::json!([contract_hash]),
    );
    value
}
fn projector_payload(contract_hash: &str, implementation_digest: &str) -> Value {
    let mut value = planner_payload(contract_hash, implementation_digest);
    value["kind"] = Value::String("response_projector".into());
    value
}
fn commitment() -> Value {
    serde_json::json!({"alg":"hmac-sha256","key_id":"google-service-commitment","verification_tier":"public","value":"hmac-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
}
fn profile_descriptor(driver_hash: &str, normalizer_hash: &str) -> Value {
    serde_json::json!({
      "schema_version":"0.2","critical_fields":[],"extensions":{},
      "profile_ref":PROFILE_REF,"version":PROFILE_VERSION,
      "scheme_ref":"credential.oauth2.authorization_code_pkce@1",
      "activation_kind":{"kind":"oauth_authorization_code_pkce","response_mode":"query","pkce_method":"S256"},
      "scheme_config":{"kind":"oauth2_authorization_code_pkce","authorization_endpoint_key":"authorization","token_endpoint_key":"token","revocation_endpoint_key":"revocation","principal_discovery_endpoint_key":"principal_discovery","response_mode":"query","pkce_method":"S256","token_endpoint_auth_method":"client_secret_post","client_auth_material_kind":"deployment_secret_binding","refresh_rotation_mode":"rotating_cas","response_firewall_policy_hash":HASH_A},
      "public_config_schema_hash":HASH_A,"authorization_claims_schema_hash":HASH_A,
      "public_claims_projection_policy":{"kind":"none"},
      "trusted_auth_driver":{"entry_ref":DRIVER_ENTRY,"version":"1","definition_hash":driver_hash,"approval_epoch":1,"revocation_epoch":0},
      "claim_normalizer":{"entry_ref":NORMALIZER_ENTRY,"version":"1","definition_hash":normalizer_hash,"approval_epoch":1,"revocation_epoch":0},
      "endpoint_policy_schema_hash":HASH_A,
      "lifecycle_capabilities":["activate","destroy","discover_principal","refresh","revoke","rotate"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::credential::{registry::RegistryDecisionV2, signing::verify_signed};

    #[test]
    fn all_seed_records_have_valid_signatures_hashes_and_classes() {
        let bundle = deterministic_signed_registry().unwrap();
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
