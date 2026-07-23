use crate::{
    BrokerError,
    artifacts::{SignatureAlg, SignatureEnvelope},
    canonical,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use std::fmt;

pub const BINDING_DOMAIN: &str = "lattice.binding-attestation.v0.1";
pub const RECEIPT_DOMAIN: &str = "lattice.invocation-receipt.v0.1";

pub struct BrokerSigner {
    key_id: String,
    key: SigningKey,
}
impl BrokerSigner {
    pub fn from_seed(key_id: impl Into<String>, seed: [u8; 32]) -> Self {
        Self {
            key_id: key_id.into(),
            key: SigningKey::from_bytes(&seed),
        }
    }
    pub fn key_id(&self) -> &str {
        &self.key_id
    }
    pub fn verifying_key(&self) -> BrokerVerifyingKey {
        BrokerVerifyingKey {
            key_id: self.key_id.clone(),
            key: self.key.verifying_key(),
        }
    }
    pub fn sign_json(
        &self,
        domain: &str,
        complete_json: &[u8],
    ) -> Result<SignatureEnvelope, BrokerError> {
        let payload = preimage(domain, complete_json)?;
        let signature = self.key.sign(&payload);
        Ok(SignatureEnvelope {
            alg: SignatureAlg::Ed25519,
            key_id: self.key_id.clone(),
            value: URL_SAFE_NO_PAD.encode(signature.to_bytes()),
        })
    }

    pub(crate) fn sign_preimage(&self, payload: &[u8]) -> SignatureEnvelope {
        let signature = self.key.sign(payload);
        SignatureEnvelope {
            alg: SignatureAlg::Ed25519,
            key_id: self.key_id.clone(),
            value: URL_SAFE_NO_PAD.encode(signature.to_bytes()),
        }
    }
}
impl fmt::Debug for BrokerSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerSigner")
            .field("key_id", &self.key_id)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct BrokerVerifyingKey {
    key_id: String,
    key: VerifyingKey,
}
impl BrokerVerifyingKey {
    pub fn to_bytes(&self) -> [u8; 32] {
        self.key.to_bytes()
    }

    pub fn from_bytes(key_id: impl Into<String>, bytes: [u8; 32]) -> Result<Self, BrokerError> {
        Ok(Self {
            key_id: key_id.into(),
            key: VerifyingKey::from_bytes(&bytes).map_err(|_| BrokerError::Brk001)?,
        })
    }

    pub fn verify_json(
        &self,
        domain: &str,
        complete_json: &[u8],
        envelope: &SignatureEnvelope,
    ) -> Result<(), BrokerError> {
        if envelope.alg != SignatureAlg::Ed25519 || envelope.key_id != self.key_id {
            return Err(BrokerError::Brk004);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(&envelope.value)
            .map_err(|_| BrokerError::Brk001)?;
        if URL_SAFE_NO_PAD.encode(&bytes) != envelope.value {
            return Err(BrokerError::Brk001);
        }
        let signature = Signature::from_slice(&bytes).map_err(|_| BrokerError::Brk001)?;
        self.key
            .verify_strict(&preimage(domain, complete_json)?, &signature)
            .map_err(|_| BrokerError::Brk109)
    }

    pub(crate) fn verify_preimage(
        &self,
        payload: &[u8],
        envelope: &SignatureEnvelope,
    ) -> Result<(), BrokerError> {
        if envelope.alg != SignatureAlg::Ed25519 || envelope.key_id != self.key_id {
            return Err(BrokerError::Brk004);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(&envelope.value)
            .map_err(|_| BrokerError::Brk001)?;
        if bytes.len() != 64 || URL_SAFE_NO_PAD.encode(&bytes) != envelope.value {
            return Err(BrokerError::Brk001);
        }
        let signature = Signature::from_slice(&bytes).map_err(|_| BrokerError::Brk001)?;
        self.key
            .verify_strict(payload, &signature)
            .map_err(|_| BrokerError::Brk109)
    }
}

pub fn preimage(domain: &str, complete_json: &[u8]) -> Result<Vec<u8>, BrokerError> {
    if !domain.is_ascii() {
        return Err(BrokerError::Brk001);
    }
    let canonical = canonical::canonicalize(complete_json)?;
    let mut value: serde_json::Value =
        serde_json::from_slice(canonical.as_bytes()).map_err(|_| BrokerError::Brk001)?;
    value
        .as_object_mut()
        .ok_or(BrokerError::Brk001)?
        .remove("signature")
        .ok_or(BrokerError::Brk001)?;
    let unsigned = canonical::from_serde(&value, usize::MAX)?;
    let mut result = Vec::with_capacity(domain.len() + unsigned.as_bytes().len() + 1);
    result.extend_from_slice(domain.as_bytes());
    result.push(0);
    result.extend_from_slice(unsigned.as_bytes());
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip_unknown_and_tamper() {
        let signer = BrokerSigner::from_seed("k", [3; 32]);
        let json = br#"{"schema_version":"0.1","unknown":{"x":1},"signature":{"alg":"Ed25519","key_id":"k","value":"placeholder"}}"#;
        let signature = signer.sign_json(RECEIPT_DOMAIN, json).unwrap();
        let signed =
            serde_json::json!({"schema_version":"0.1","unknown":{"x":1},"signature":signature});
        let bytes = serde_json::to_vec(&signed).unwrap();
        signer
            .verifying_key()
            .verify_json(RECEIPT_DOMAIN, &bytes, &signature)
            .unwrap();
        let tampered = String::from_utf8(bytes)
            .unwrap()
            .replace("\"x\":1", "\"x\":2");
        assert!(
            signer
                .verifying_key()
                .verify_json(RECEIPT_DOMAIN, tampered.as_bytes(), &signature)
                .is_err()
        );
        assert!(
            signer
                .verifying_key()
                .verify_json(
                    BINDING_DOMAIN,
                    serde_json::to_vec(&signed).unwrap().as_slice(),
                    &signature
                )
                .is_err()
        );
    }
}
