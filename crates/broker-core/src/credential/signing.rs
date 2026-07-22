use super::{ParsedV2, SchemaType};
use crate::{
    BrokerError,
    artifacts::SignatureEnvelope,
    canonical,
    signing::{BrokerSigner, BrokerVerifyingKey},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};

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
