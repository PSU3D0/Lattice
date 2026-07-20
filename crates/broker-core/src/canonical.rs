//! Public compatibility facade for the crypto-free JCS implementation.
use crate::BrokerError;
pub use jcs_canonical::{
    MAX_ARRAY_ELEMENTS, MAX_DEPTH, MAX_EXACT_INTEGER, MAX_NAME_BYTES, MAX_OBJECT_MEMBERS,
    MAX_OPERATION_BYTES, MAX_STRING_BYTES, Value,
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalJson(jcs_canonical::CanonicalJson);
impl CanonicalJson {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_bytes()
    }
    pub fn sha256(&self) -> String {
        format!("sha256:{}", hex::encode(Sha256::digest(self.as_bytes())))
    }
}

pub fn canonicalize(input: &[u8]) -> Result<CanonicalJson, BrokerError> {
    jcs_canonical::canonicalize(input)
        .map(CanonicalJson)
        .map_err(|_| BrokerError::Brk001)
}
pub fn canonicalize_bounded(input: &[u8], max: usize) -> Result<CanonicalJson, BrokerError> {
    jcs_canonical::canonicalize_bounded(input, max)
        .map(CanonicalJson)
        .map_err(|_| BrokerError::Brk001)
}
pub fn from_serde<T: serde::Serialize>(
    value: &T,
    max: usize,
) -> Result<CanonicalJson, BrokerError> {
    jcs_canonical::from_serde(value, max)
        .map(CanonicalJson)
        .map_err(|_| BrokerError::Brk001)
}
