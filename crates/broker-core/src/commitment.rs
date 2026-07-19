use crate::{
    BrokerError,
    artifacts::{CommitmentAlg, CommitmentEnvelope, VerificationTier},
    canonical,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;

type HmacSha256 = Hmac<Sha256>;
const DOMAIN: &[u8] = b"lattice.commitment.v0.1";
const OPENING_DOMAIN: &[u8] = b"lattice.commitment-opening.v0.1";

pub struct CommitmentKey {
    key_id: String,
    root: [u8; 32],
}
impl CommitmentKey {
    pub fn new(key_id: impl Into<String>, root: [u8; 32]) -> Result<Self, BrokerError> {
        let key_id = key_id.into();
        if key_id.is_empty() {
            return Err(BrokerError::Brk001);
        }
        Ok(Self { key_id, root })
    }
    pub fn key_id(&self) -> &str {
        &self.key_id
    }
    pub fn commit(
        &self,
        org_id: &str,
        field_name: &str,
        salt_context: &[u8],
        value: &[u8],
        tier: VerificationTier,
    ) -> Result<(CommitmentEnvelope, DisclosureKey), BrokerError> {
        validate_parts(org_id, field_name, salt_context, value)?;
        let key = derive_key(&self.root, org_id, field_name, salt_context)?;
        let value = calculate(&key, org_id, field_name, salt_context, value)?;
        Ok((
            CommitmentEnvelope {
                alg: CommitmentAlg::HmacSha256,
                key_id: self.key_id.clone(),
                verification_tier: Some(tier),
                value: format!("hmac-sha256:{}", hex::encode(value)),
                extensions: Default::default(),
            },
            DisclosureKey {
                key_id: self.key_id.clone(),
                org_id: org_id.to_owned(),
                field_name: field_name.to_owned(),
                salt_context: salt_context.to_vec(),
                key,
            },
        ))
    }
}
impl fmt::Debug for CommitmentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitmentKey")
            .field("key_id", &self.key_id)
            .field("root", &"[REDACTED]")
            .finish()
    }
}

/// A purpose-scoped opening key. The commitment key is derived as
/// HMAC(root, `lattice.commitment-opening.v0.1` || the commitment scope), so
/// disclosing this value cannot open another field or recover the tenant root.
pub struct DisclosureKey {
    key_id: String,
    org_id: String,
    field_name: String,
    salt_context: Vec<u8>,
    key: [u8; 32],
}
impl DisclosureKey {
    pub fn opens(&self, envelope: &CommitmentEnvelope, value: &[u8]) -> bool {
        if envelope.key_id != self.key_id || envelope.alg != CommitmentAlg::HmacSha256 {
            return false;
        }
        calculate(
            &self.key,
            &self.org_id,
            &self.field_name,
            &self.salt_context,
            value,
        )
        .ok()
        .is_some_and(|actual| envelope.value == format!("hmac-sha256:{}", hex::encode(actual)))
    }
}
impl fmt::Debug for DisclosureKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DisclosureKey")
            .field("key_id", &self.key_id)
            .field("scope", &"[SCOPED]")
            .finish()
    }
}

pub fn receipt_salt_context(
    issuer: &str,
    run_id: &str,
    node_id: &str,
    logical_effect_id: &str,
    dispatch_attempt: u64,
    field_name: &str,
) -> Result<Vec<u8>, BrokerError> {
    canonical::from_serde(
        &(
            issuer,
            run_id,
            node_id,
            logical_effect_id,
            dispatch_attempt,
            field_name,
        ),
        canonical::MAX_OPERATION_BYTES,
    )
    .map(canonical::CanonicalJson::into_bytes)
}
pub fn account_salt_context(issuer: &str, connection_ref: &str) -> Result<Vec<u8>, BrokerError> {
    canonical::from_serde(
        &(issuer, connection_ref, "account_commitment"),
        canonical::MAX_OPERATION_BYTES,
    )
    .map(canonical::CanonicalJson::into_bytes)
}
fn derive_key(
    root: &[u8; 32],
    org_id: &str,
    field: &str,
    salt: &[u8],
) -> Result<[u8; 32], BrokerError> {
    let mut mac = HmacSha256::new_from_slice(root).map_err(|_| BrokerError::Brk401)?;
    mac.update(OPENING_DOMAIN);
    mac.update(&[0]);
    length32(org_id.as_bytes(), &mut mac)?;
    length32(field.as_bytes(), &mut mac)?;
    length32(salt, &mut mac)?;
    Ok(mac.finalize().into_bytes().into())
}
fn calculate(
    key: &[u8; 32],
    org: &str,
    field_name: &str,
    salt: &[u8],
    value: &[u8],
) -> Result<[u8; 32], BrokerError> {
    validate_parts(org, field_name, salt, value)?;
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| BrokerError::Brk401)?;
    mac.update(DOMAIN);
    mac.update(&[0]);
    length32(org.as_bytes(), &mut mac)?;
    length32(field_name.as_bytes(), &mut mac)?;
    length32(salt, &mut mac)?;
    let len = u64::try_from(value.len()).map_err(|_| BrokerError::Brk001)?;
    mac.update(&len.to_be_bytes());
    mac.update(value);
    Ok(mac.finalize().into_bytes().into())
}
fn length32(value: &[u8], mac: &mut HmacSha256) -> Result<(), BrokerError> {
    let len = u32::try_from(value.len()).map_err(|_| BrokerError::Brk001)?;
    mac.update(&len.to_be_bytes());
    mac.update(value);
    Ok(())
}
fn validate_parts(org: &str, field: &str, salt: &[u8], value: &[u8]) -> Result<(), BrokerError> {
    if org.is_empty()
        || field.is_empty()
        || salt.is_empty()
        || value.len() > canonical::MAX_OPERATION_BYTES
    {
        Err(BrokerError::Brk001)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contexts_scope_openings() {
        let root = CommitmentKey::new("v1", [7; 32]).unwrap();
        let a = receipt_salt_context("i", "r", "n", "sha256:e", 1, "input").unwrap();
        let b = receipt_salt_context("i", "r", "n", "sha256:e", 1, "response").unwrap();
        let (ca, ka) = root
            .commit(
                "o",
                "input",
                &a,
                b"secret",
                VerificationTier::VerifierWithDisclosure,
            )
            .unwrap();
        let (cb, kb) = root
            .commit("o", "response", &b, b"secret", VerificationTier::BrokerOnly)
            .unwrap();
        assert_ne!(ca.value, cb.value);
        assert!(ka.opens(&ca, b"secret"));
        assert!(!ka.opens(&cb, b"secret"));
        assert!(kb.opens(&cb, b"secret"));
        assert!(!format!("{root:?}{ka:?}").contains(&hex::encode([7; 32])));
    }
}
