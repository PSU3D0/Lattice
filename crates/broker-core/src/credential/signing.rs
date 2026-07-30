use super::{ParsedV2, SchemaType};
use crate::{
    BrokerError,
    artifacts::SignatureEnvelope,
    canonical,
    signing::{BrokerSigner, BrokerVerifyingKey},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const AUTHORITY_MODEL_REVISION: &str = "lifecycle-separated-1";
const LS1_PREFIX: &str = "lattice.credential-plane.0.2.lifecycle-separated-1.";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LifecycleSignature {
    algorithm: LifecycleAlgorithm,
    key_id: String,
    value: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum LifecycleAlgorithm {
    Ed25519,
}

pub(crate) fn lifecycle_domain_for_schema(schema: &str) -> Option<&'static str> {
    Some(match schema {
        "LS1AuthorityModelCutover" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "authority-model-cutover"
        ),
        "LS1AuthorizationObservation" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "authorization-observation"
        ),
        "LS1ProviderGrantVersion" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "provider-grant-version"
        ),
        "LS1ProviderGrantAdoptionRecord" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "provider-grant-adoption"
        ),
        "LS1ConnectionAliasRecord" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "connection-alias-record"
        ),
        "LS1StandingAuthority" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "standing-authority"
        ),
        "LS1ContractSet" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "contract-set"
        ),
        "LS1PolicyInstance" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "policy-instance"
        ),
        "LS1RegistryDefinition" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "registry-definition"
        ),
        "LS1RegistryDecision" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "registry-decision"
        ),
        "LS1RegistryDecisionVector" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "registry-decision-vector"
        ),
        "LS1CeilingAmendment" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "ceiling-amendment"
        ),
        "LS1CorrectedBindingAttestation" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "binding-attestation"
        ),
        "LS1DispatchAdmission" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "dispatch-admission"
        ),
        "LS1InvocationReceipt" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "invocation-receipt"
        ),
        "LS1LegacyAttemptInventory" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "legacy-attempt-inventory"
        ),
        "LS1ReceiptVerificationKeyset" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "receipt-verification-keyset"
        ),
        "LS1ReceiptKeyCompromiseRecord" => concat!(
            "lattice.credential-plane.0.2.lifecycle-separated-1.",
            "receipt-key-compromise"
        ),
        _ => return None,
    })
}

pub(crate) fn verify_lifecycle_value(
    schema: &str,
    value: &serde_json::Value,
    key: &BrokerVerifyingKey,
) -> Result<(), BrokerError> {
    super::model::validate(schema, value)?;
    let domain = lifecycle_domain_for_schema(schema).ok_or(BrokerError::Brk004)?;
    if !domain.starts_with(LS1_PREFIX)
        || value.get("schema_version").and_then(|item| item.as_str()) != Some("0.2")
        || value
            .get("authority_model_revision")
            .and_then(|item| item.as_str())
            != Some(AUTHORITY_MODEL_REVISION)
        || value.get("artifact_type").and_then(|item| item.as_str()) != schema.strip_prefix("LS1")
    {
        return Err(BrokerError::Brk004);
    }
    let signature: LifecycleSignature =
        serde_json::from_value(value.get("signature").cloned().ok_or(BrokerError::Brk004)?)
            .map_err(|_| BrokerError::Brk004)?;
    if signature.algorithm != LifecycleAlgorithm::Ed25519
        || value.get("key_id").and_then(|item| item.as_str()) != Some(signature.key_id.as_str())
    {
        return Err(BrokerError::Brk004);
    }
    let envelope = SignatureEnvelope {
        alg: crate::artifacts::SignatureAlg::Ed25519,
        key_id: signature.key_id,
        value: signature.value,
    };
    let canonical = canonical::from_serde(value, 1024 * 1024)?;
    key.verify_json(domain, canonical.as_bytes(), &envelope)
}

#[cfg(test)]
pub(crate) fn verify_lifecycle_value_in_domain(
    value: &serde_json::Value,
    domain: &str,
    key: &BrokerVerifyingKey,
) -> Result<(), BrokerError> {
    let signature: LifecycleSignature =
        serde_json::from_value(value.get("signature").cloned().ok_or(BrokerError::Brk004)?)
            .map_err(|_| BrokerError::Brk004)?;
    let envelope = SignatureEnvelope {
        alg: crate::artifacts::SignatureAlg::Ed25519,
        key_id: signature.key_id,
        value: signature.value,
    };
    let canonical = canonical::from_serde(value, 1024 * 1024)?;
    key.verify_json(domain, canonical.as_bytes(), &envelope)
}

public_type!(RemoteCustodyEnvelopeV2, RemoteEnvelopeTag, "RemoteEnvelope");

pub const REGISTRY_DEFINITION_DOMAIN: &str = "lattice.registry-definition.v0.2";
pub const REGISTRY_DECISION_DOMAIN: &str = "lattice.registry-decision.v0.2";
pub const DEPLOYMENT_ENDPOINT_SET_DOMAIN: &str = "lattice.deployment-endpoint-set.v0.2";
pub const DEPLOYMENT_PUBLIC_CONFIG_DOMAIN: &str = "lattice.deployment-public-config.v0.2";
pub const STANDING_AUTHORITY_DOMAIN: &str = "lattice.standing-authority.v0.2";
pub const CONTRACT_SET_DOMAIN: &str = "lattice.contract-set.v0.2";
pub const PRIVATE_MATERIAL_SUBMISSION_DOMAIN: &str = "lattice.private-material-submission.v0.2";
pub const LEGACY_ADMISSION_INVENTORY_DOMAIN: &str = "lattice.legacy-admission-inventory.v0.2";
pub const LEGACY_INVENTORY_DECISION_DOMAIN: &str = "lattice.legacy-inventory-decision.v0.2";
pub const HISTORICAL_KEY_VALIDITY_DOMAIN: &str = "lattice.historical-key-validity-evidence.v0.2";
pub const HISTORICAL_KEY_REVOCATION_DOMAIN: &str =
    "lattice.historical-key-revocation-evidence.v0.2";
pub const HISTORICAL_KEY_ARCHIVE_DOMAIN: &str = "lattice.historical-verification-key-archive.v0.2";
pub const BINDING_ATTESTATION_DOMAIN: &str = "lattice.binding-attestation.v0.2";
pub const NODE_LEASE_DOMAIN: &str = "lattice.node-lease.v0.2";
pub const INVOCATION_RECEIPT_DOMAIN: &str = "lattice.invocation-receipt.v0.2";
pub const REMOTE_CUSTODY_ENVELOPE_DOMAIN: &str = "lattice.remote-custody-envelope.v0.2";

pub fn domain_for_schema(schema: &str) -> Option<&'static str> {
    Some(match schema {
        "RegistryDefinition" => REGISTRY_DEFINITION_DOMAIN,
        "RegistryDecision" => REGISTRY_DECISION_DOMAIN,
        "DeploymentEndpointSet" => DEPLOYMENT_ENDPOINT_SET_DOMAIN,
        "DeploymentPublicConfig" => DEPLOYMENT_PUBLIC_CONFIG_DOMAIN,
        "StandingAuthority" => STANDING_AUTHORITY_DOMAIN,
        "ContractSet" => CONTRACT_SET_DOMAIN,
        "PrivateMaterialSubmission" => PRIVATE_MATERIAL_SUBMISSION_DOMAIN,
        "LegacyAdmissionInventory" => LEGACY_ADMISSION_INVENTORY_DOMAIN,
        "LegacyInventoryDecision" => LEGACY_INVENTORY_DECISION_DOMAIN,
        "HistoricalKeyValidityEvidence" => HISTORICAL_KEY_VALIDITY_DOMAIN,
        "HistoricalKeyRevocationEvidence" => HISTORICAL_KEY_REVOCATION_DOMAIN,
        "HistoricalVerificationKeyArchive" => HISTORICAL_KEY_ARCHIVE_DOMAIN,
        "BindingAttestation" => BINDING_ATTESTATION_DOMAIN,
        "NodeLease" => NODE_LEASE_DOMAIN,
        "InvocationReceipt" => INVOCATION_RECEIPT_DOMAIN,
        "RemoteEnvelope" => REMOTE_CUSTODY_ENVELOPE_DOMAIN,
        _ => return None,
    })
}

pub fn verify_signed<T: SchemaType>(
    parsed: &ParsedV2<T>,
    key: &BrokerVerifyingKey,
) -> Result<(), BrokerError> {
    let signature: SignatureEnvelope = serde_json::from_value(
        parsed
            .view
            .value()
            .get("signature")
            .cloned()
            .ok_or(BrokerError::Brk004)?,
    )
    .map_err(|_| BrokerError::Brk004)?;
    let domain = domain_for_schema(T::SCHEMA).ok_or(BrokerError::Brk004)?;
    if T::SCHEMA == "RemoteEnvelope" {
        key.verify_preimage(&remote_preimage(parsed.view.value())?, &signature)
    } else {
        key.verify_json(domain, parsed.canonical_bytes(), &signature)
    }
}

pub fn sign_remote_envelope(
    signer: &BrokerSigner,
    unsigned_envelope: &serde_json::Value,
) -> Result<SignatureEnvelope, BrokerError> {
    Ok(signer.sign_preimage(&remote_preimage(unsigned_envelope)?))
}

pub fn remote_preimage(envelope: &serde_json::Value) -> Result<Vec<u8>, BrokerError> {
    let mut aad = envelope.as_object().cloned().ok_or(BrokerError::Brk001)?;
    let ciphertext = aad
        .remove("ciphertext")
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or(BrokerError::Brk001)?;
    aad.remove("signature");
    aad.remove("aad_hash");
    let aad = canonical::from_serde(&aad, 1024 * 1024)?;
    let digest = Sha256::digest(aad.as_bytes());
    if envelope
        .get("aad_hash")
        .and_then(|v| v.as_str())
        .is_some_and(|h| h != format!("sha256:{}", hex::encode(digest)))
    {
        return Err(BrokerError::Brk109);
    }
    let ciphertext = URL_SAFE_NO_PAD
        .decode(ciphertext)
        .map_err(|_| BrokerError::Brk001)?;
    let mut preimage =
        Vec::with_capacity(REMOTE_CUSTODY_ENVELOPE_DOMAIN.len() + 1 + 32 + ciphertext.len());
    preimage.extend_from_slice(REMOTE_CUSTODY_ENVELOPE_DOMAIN.as_bytes());
    preimage.push(0);
    preimage.extend_from_slice(&digest);
    preimage.extend_from_slice(&ciphertext);
    Ok(preimage)
}
