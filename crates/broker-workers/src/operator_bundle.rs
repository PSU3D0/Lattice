use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use broker_core::{
    BrokerError, artifacts::SignatureEnvelope, canonical, credential::signing::domain_for_schema,
    signing::BrokerVerifyingKey,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const BUNDLE_DOMAIN: &str = "lattice.operator-artifact-bundle.v1";
const REQUIRED: [&str; 6] = [
    "deployment_standing_authority",
    "deployment_contract_set",
    "registry_definitions",
    "registry_decisions",
    "historical_inventory",
    "historical_key_evidence",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bundle {
    schema_version: String,
    key_id: String,
    public_key_b64u: String,
    not_before: String,
    expires_at: String,
    revoked_at: Option<String>,
    activation_recipient: ActivationRecipient,
    artifacts: BTreeMap<String, Vec<Entry>>,
    signature: SignatureEnvelope,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationRecipient {
    key_id: String,
    public_key_b64u: String,
    suite: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    schema: String,
    hash: String,
    canonical_jcs: String,
}

pub fn verify_operator_bundle(
    exact: &[u8],
    trust_key_id: &str,
    trust_public_key_b64u: &str,
    now: &str,
) -> Result<String, BrokerError> {
    let canonical_bundle = canonical::canonicalize(exact)?;
    if canonical_bundle.as_bytes() != exact {
        return Err(BrokerError::Brk001);
    }
    let bundle: Bundle = serde_json::from_slice(exact).map_err(|_| BrokerError::Brk001)?;
    if bundle.schema_version != "1"
        || bundle.key_id != trust_key_id
        || bundle.public_key_b64u != trust_public_key_b64u
        || bundle.revoked_at.is_some()
        || bundle.not_before.as_str() > now
        || now >= bundle.expires_at.as_str()
    {
        return Err(BrokerError::Brk109);
    }
    let key_bytes = URL_SAFE_NO_PAD
        .decode(trust_public_key_b64u)
        .map_err(|_| BrokerError::Brk001)?;
    let key = BrokerVerifyingKey::from_bytes(
        trust_key_id,
        key_bytes.try_into().map_err(|_| BrokerError::Brk001)?,
    )?;
    key.verify_json(BUNDLE_DOMAIN, exact, &bundle.signature)?;
    if bundle.activation_recipient.key_id.is_empty()
        || bundle.activation_recipient.suite != "DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM"
        || URL_SAFE_NO_PAD
            .decode(&bundle.activation_recipient.public_key_b64u)
            .map_err(|_| BrokerError::Brk001)?
            .len()
            != 32
    {
        return Err(BrokerError::Brk004);
    }
    for required in REQUIRED {
        if bundle.artifacts.get(required).is_none_or(Vec::is_empty) {
            return Err(BrokerError::Brk004);
        }
    }
    for entries in bundle.artifacts.values() {
        for entry in entries {
            let artifact = canonical::canonicalize(entry.canonical_jcs.as_bytes())?;
            if artifact.as_bytes() != entry.canonical_jcs.as_bytes()
                || entry.hash
                    != format!(
                        "sha256:{}",
                        hex::encode(Sha256::digest(artifact.as_bytes()))
                    )
            {
                return Err(BrokerError::Brk109);
            }
            verify_artifact(&key, &entry.schema, artifact.as_bytes()).map_err(|error| {
                eprintln!("artifact schema rejected: {}", entry.schema);
                error
            })?;
        }
    }
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(exact))))
}

fn verify_artifact(
    key: &BrokerVerifyingKey,
    schema: &str,
    canonical_artifact: &[u8],
) -> Result<(), BrokerError> {
    let domain = domain_for_schema(schema).ok_or(BrokerError::Brk004)?;
    let value: Value =
        serde_json::from_slice(canonical_artifact).map_err(|_| BrokerError::Brk001)?;
    if !schema.starts_with("HistoricalKey")
        && value.get("schema_version").and_then(Value::as_str) != Some("0.2")
    {
        return Err(BrokerError::Brk004);
    }
    match schema {
        "StandingAuthority" => {
            broker_core::credential::parse::<broker_core::credential::StandingAuthorityV2>(
                canonical_artifact,
            )?;
        }
        "ContractSet" => {
            broker_core::credential::parse::<broker_core::credential::ContractSetV2>(
                canonical_artifact,
            )?;
        }
        "RegistryDefinition" => {
            broker_core::credential::parse::<
                broker_core::credential::registry::RegistryDefinitionV2,
            >(canonical_artifact)?;
        }
        "RegistryDecision" => {
            broker_core::credential::parse::<broker_core::credential::registry::RegistryDecisionV2>(
                canonical_artifact,
            )?;
        }
        "LegacyAdmissionInventory" => {
            broker_core::credential::parse::<
                broker_core::credential::legacy::LegacyAdmissionInventoryV2,
            >(canonical_artifact)?;
        }
        "LegacyInventoryDecision" => {
            broker_core::credential::parse::<
                broker_core::credential::legacy::LegacyInventoryDecisionV2,
            >(canonical_artifact)?;
        }
        "HistoricalKeyValidityEvidence" => {
            broker_core::credential::parse::<
                broker_core::credential::legacy::HistoricalKeyValidityEvidenceV2,
            >(canonical_artifact)?;
        }
        "HistoricalKeyRevocationEvidence" => {
            broker_core::credential::parse::<
                broker_core::credential::legacy::HistoricalKeyRevocationEvidenceV2,
            >(canonical_artifact)?;
        }
        "HistoricalVerificationKeyArchive" => {
            broker_core::credential::parse::<
                broker_core::credential::legacy::HistoricalVerificationKeyArchiveV2,
            >(canonical_artifact)?;
        }
        _ => return Err(BrokerError::Brk004),
    }
    let signature: SignatureEnvelope =
        serde_json::from_value(value.get("signature").cloned().ok_or(BrokerError::Brk004)?)
            .map_err(|_| BrokerError::Brk004)?;
    key.verify_json(domain, canonical_artifact, &signature)?;
    if schema == "HistoricalVerificationKeyArchive" {
        for (field, nested_schema) in [
            ("validity_evidence", "HistoricalKeyValidityEvidence"),
            ("revocation_evidence", "HistoricalKeyRevocationEvidence"),
        ] {
            let nested = value.get(field).ok_or(BrokerError::Brk004)?;
            let exact = canonical::from_serde(nested, 256 * 1024)?;
            verify_artifact(key, nested_schema, exact.as_bytes())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::signing::BrokerSigner;
    fn bundle(signer: &BrokerSigner) -> String {
        let vectors: Value = serde_json::from_str(include_str!(
            "../../../impl-docs/spec/credential-plane-protocol-vectors.json"
        ))
        .unwrap();
        let all = vectors["signed_artifact_vectors"].as_array().unwrap();
        let specs = [
            ("deployment_standing_authority", "StandingAuthority"),
            ("deployment_contract_set", "ContractSet"),
            ("registry_definitions", "RegistryDefinition"),
            ("registry_decisions", "RegistryDecision"),
            ("historical_inventory", "LegacyAdmissionInventory"),
            (
                "historical_key_evidence",
                "HistoricalVerificationKeyArchive",
            ),
        ];
        let mut artifacts = serde_json::Map::new();
        for (name, schema) in specs {
            let artifact =
                all.iter().find(|v| v["artifact_schema"] == schema).unwrap()["artifact"].clone();
            let exact = canonical::from_serde(&artifact, usize::MAX).unwrap();
            artifacts.insert(name.into(), serde_json::json!([{"schema":schema,"hash":format!("sha256:{}",hex::encode(Sha256::digest(exact.as_bytes()))),"canonical_jcs":std::str::from_utf8(exact.as_bytes()).unwrap()}]));
        }
        let mut value = serde_json::json!({"schema_version":"1","key_id":"test-ed25519-1","public_key_b64u":URL_SAFE_NO_PAD.encode(signer.verifying_key().to_bytes()),"not_before":"2020-01-01T00:00:00Z","expires_at":"2030-01-01T00:00:00Z","revoked_at":null,"activation_recipient":{"key_id":"activation-test","public_key_b64u":URL_SAFE_NO_PAD.encode([9u8;32]),"suite":"DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM"},"artifacts":artifacts,"signature":{"alg":"Ed25519","key_id":"test-ed25519-1","value":"pending"}});
        let exact = canonical::from_serde(&value, usize::MAX).unwrap();
        value["signature"] =
            serde_json::to_value(signer.sign_json(BUNDLE_DOMAIN, exact.as_bytes()).unwrap())
                .unwrap();
        String::from_utf8(
            canonical::from_serde(&value, usize::MAX)
                .unwrap()
                .as_bytes()
                .to_vec(),
        )
        .unwrap()
    }
    #[test]
    fn valid_bundle_and_all_fail_closed_cases() {
        let mut seed = [0u8; 32];
        for (index, byte) in seed.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let signer = BrokerSigner::from_seed("test-ed25519-1", seed);
        let key = URL_SAFE_NO_PAD.encode(signer.verifying_key().to_bytes());
        let exact = bundle(&signer);
        assert!(
            verify_operator_bundle(
                exact.as_bytes(),
                "test-ed25519-1",
                &key,
                "2026-01-01T00:00:00Z"
            )
            .is_ok()
        );
        let wrong = URL_SAFE_NO_PAD.encode(
            BrokerSigner::from_seed("test-ed25519-1", [8; 32])
                .verifying_key()
                .to_bytes(),
        );
        assert!(
            verify_operator_bundle(
                exact.as_bytes(),
                "test-ed25519-1",
                &wrong,
                "2026-01-01T00:00:00Z"
            )
            .is_err()
        );
        assert!(
            verify_operator_bundle(
                exact.as_bytes(),
                "test-ed25519-1",
                &key,
                "2031-01-01T00:00:00Z"
            )
            .is_err()
        );
        let mut value: Value = serde_json::from_str(&exact).unwrap();
        value["revoked_at"] = Value::String("2025-01-01T00:00:00Z".into());
        let revoked = canonical::from_serde(&value, usize::MAX).unwrap();
        assert!(
            verify_operator_bundle(
                revoked.as_bytes(),
                "test-ed25519-1",
                &key,
                "2026-01-01T00:00:00Z"
            )
            .is_err()
        );
        let tampered = exact.replace("StandingAuthority", "ContractSet");
        assert!(
            verify_operator_bundle(
                tampered.as_bytes(),
                "test-ed25519-1",
                &key,
                "2026-01-01T00:00:00Z"
            )
            .is_err()
        );
    }
}
