use crate::BrokerError;
use sha2::{Digest, Sha256};

const DOMAIN: &[u8] = b"lattice.logical-effect.v0.1";

pub fn derive(
    run_id: &str,
    node_id: &str,
    activation_ordinal: u64,
    semantic_effect_slot: &str,
) -> Result<String, BrokerError> {
    validate_id(run_id)?;
    validate_id(node_id)?;
    if semantic_effect_slot.is_empty()
        || semantic_effect_slot.len() > 128
        || !semantic_effect_slot.is_ascii()
    {
        return Err(BrokerError::Brk001);
    }
    let mut preimage = Vec::with_capacity(
        DOMAIN.len() + run_id.len() + node_id.len() + semantic_effect_slot.len() + 21,
    );
    preimage.extend_from_slice(DOMAIN);
    preimage.push(0);
    field(run_id.as_bytes(), &mut preimage)?;
    field(node_id.as_bytes(), &mut preimage)?;
    preimage.extend_from_slice(&activation_ordinal.to_be_bytes());
    field(semantic_effect_slot.as_bytes(), &mut preimage)?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(preimage))))
}
fn validate_id(value: &str) -> Result<(), BrokerError> {
    if value.is_empty() || value.len() > 1024 {
        Err(BrokerError::Brk001)
    } else {
        Ok(())
    }
}
fn field(value: &[u8], out: &mut Vec<u8>) -> Result<(), BrokerError> {
    let len = u32::try_from(value.len()).map_err(|_| BrokerError::Brk001)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

pub fn digest_bytes(effect_id: &str) -> Result<[u8; 32], BrokerError> {
    let encoded = effect_id
        .strip_prefix("sha256:")
        .ok_or(BrokerError::Brk001)?;
    if encoded.len() != 64
        || !encoded
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(BrokerError::Brk001);
    }
    let bytes = hex::decode(encoded).map_err(|_| BrokerError::Brk001)?;
    bytes.try_into().map_err(|_| BrokerError::Brk001)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_and_bounded() {
        assert_eq!(
            derive("r", "n", 1, "slot").unwrap(),
            derive("r", "n", 1, "slot").unwrap()
        );
        assert_ne!(
            derive("r", "n", 1, "slot").unwrap(),
            derive("r", "n", 2, "slot").unwrap()
        );
        assert!(derive("r", "n", 0, "").is_err());
    }
}
