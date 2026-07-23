use aes::{
    Aes256,
    cipher::{BlockEncrypt, KeyInit, generic_array::GenericArray},
};
use hkdf::Hkdf;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use broker_core::BrokerError;

const KEM_ID: [u8; 2] = 0x0020u16.to_be_bytes();
const KDF_ID: [u8; 2] = 0x0001u16.to_be_bytes();
const AEAD_ID: [u8; 2] = 0x0002u16.to_be_bytes();
const VERSION: &[u8] = b"HPKE-v1";
const INFO: &[u8] = b"lattice.generic-activation-envelope.v0.2";

pub fn public_key(private_key: &[u8; 32]) -> [u8; 32] {
    PublicKey::from(&StaticSecret::from(*private_key)).to_bytes()
}

#[cfg(test)]
pub fn seal(
    recipient_public_key: &[u8; 32],
    ephemeral_private_key: [u8; 32],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<([u8; 32], Vec<u8>), BrokerError> {
    let ephemeral = StaticSecret::from(ephemeral_private_key);
    let encapsulated = PublicKey::from(&ephemeral).to_bytes();
    let shared = Zeroizing::new(
        ephemeral
            .diffie_hellman(&PublicKey::from(*recipient_public_key))
            .to_bytes(),
    );
    if bool::from(shared.ct_eq(&[0; 32])) {
        return Err(BrokerError::Brk109);
    }
    let (key, nonce) = key_schedule(&shared, &encapsulated, recipient_public_key)?;
    Ok((encapsulated, aes_256_gcm_seal(&key, &nonce, aad, plaintext)))
}

pub fn open(
    recipient_private_key: &[u8; 32],
    encapsulated_key: &[u8; 32],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<Vec<u8>>, BrokerError> {
    let recipient = StaticSecret::from(*recipient_private_key);
    let recipient_public = PublicKey::from(&recipient).to_bytes();
    let shared = Zeroizing::new(
        recipient
            .diffie_hellman(&PublicKey::from(*encapsulated_key))
            .to_bytes(),
    );
    if bool::from(shared.ct_eq(&[0; 32])) {
        return Err(BrokerError::Brk109);
    }
    let (key, nonce) = key_schedule(&shared, encapsulated_key, &recipient_public)?;
    aes_256_gcm_open(&key, &nonce, aad, ciphertext).map(Zeroizing::new)
}

fn key_schedule(
    dh: &[u8; 32],
    encapsulated: &[u8; 32],
    recipient_public: &[u8; 32],
) -> Result<(Zeroizing<[u8; 32]>, [u8; 12]), BrokerError> {
    let kem_suite = [b"KEM".as_slice(), &KEM_ID].concat();
    let mut kem_context = Vec::with_capacity(64);
    kem_context.extend_from_slice(encapsulated);
    kem_context.extend_from_slice(recipient_public);
    let eae_prk = labeled_extract(&[], &kem_suite, b"eae_prk", dh);
    let shared_secret = labeled_expand::<32>(&eae_prk, &kem_suite, b"shared_secret", &kem_context)?;
    let suite = [b"HPKE".as_slice(), &KEM_ID, &KDF_ID, &AEAD_ID].concat();
    let psk_id_hash = labeled_extract(&[], &suite, b"psk_id_hash", &[]);
    let info_hash = labeled_extract(&[], &suite, b"info_hash", INFO);
    let mut context = Vec::with_capacity(65);
    context.push(0);
    context.extend_from_slice(&psk_id_hash);
    context.extend_from_slice(&info_hash);
    let secret = labeled_extract(&shared_secret, &suite, b"secret", &[]);
    Ok((
        Zeroizing::new(labeled_expand::<32>(&secret, &suite, b"key", &context)?),
        labeled_expand::<12>(&secret, &suite, b"base_nonce", &context)?,
    ))
}
fn labeled_extract(salt: &[u8], suite: &[u8], label: &[u8], ikm: &[u8]) -> [u8; 32] {
    let labeled = [VERSION, suite, label, ikm].concat();
    let (prk, _) = Hkdf::<Sha256>::extract(Some(salt), &labeled);
    let mut out = [0; 32];
    out.copy_from_slice(prk.as_slice());
    out
}
fn labeled_expand<const N: usize>(
    prk: &[u8],
    suite: &[u8],
    label: &[u8],
    info: &[u8],
) -> Result<[u8; N], BrokerError> {
    let length = u16::try_from(N)
        .map_err(|_| BrokerError::Brk401)?
        .to_be_bytes();
    let labeled = [&length, VERSION, suite, label, info].concat();
    let hkdf = Hkdf::<Sha256>::from_prk(prk).map_err(|_| BrokerError::Brk401)?;
    let mut out = [0; N];
    hkdf.expand(&labeled, &mut out)
        .map_err(|_| BrokerError::Brk401)?;
    Ok(out)
}
fn encrypt_block(cipher: &Aes256, block: [u8; 16]) -> [u8; 16] {
    let mut block = GenericArray::clone_from_slice(&block);
    cipher.encrypt_block(&mut block);
    block.into()
}
fn increment_counter(counter: &mut [u8; 16]) {
    let value = u32::from_be_bytes(counter[12..].try_into().expect("counter"));
    counter[12..].copy_from_slice(&value.wrapping_add(1).to_be_bytes())
}
fn ghash(h: [u8; 16], aad: &[u8], ciphertext: &[u8]) -> [u8; 16] {
    let h = u128::from_be_bytes(h);
    let mut y = 0u128;
    for input in [aad, ciphertext] {
        for chunk in input.chunks(16) {
            let mut block = [0; 16];
            block[..chunk.len()].copy_from_slice(chunk);
            y = gf_mul(y ^ u128::from_be_bytes(block), h);
        }
    }
    let lengths = ((aad.len() as u128 * 8) << 64) | (ciphertext.len() as u128 * 8);
    gf_mul(y ^ lengths, h).to_be_bytes()
}
fn gf_mul(x: u128, mut y: u128) -> u128 {
    let mut z = 0;
    for bit in 0..128 {
        if (x >> (127 - bit)) & 1 == 1 {
            z ^= y;
        }
        y = if y & 1 == 0 {
            y >> 1
        } else {
            (y >> 1) ^ (0xe1u128 << 120)
        };
    }
    z
}
#[cfg(test)]
fn aes_256_gcm_seal(key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let cipher = Aes256::new(GenericArray::from_slice(key));
    let h = encrypt_block(&cipher, [0; 16]);
    let mut j0 = [0; 16];
    j0[..12].copy_from_slice(nonce);
    j0[15] = 1;
    let mut counter = j0;
    let mut ciphertext = plaintext.to_vec();
    for chunk in ciphertext.chunks_mut(16) {
        increment_counter(&mut counter);
        let stream = encrypt_block(&cipher, counter);
        for (byte, mask) in chunk.iter_mut().zip(stream) {
            *byte ^= mask;
        }
    }
    let auth = ghash(h, aad, &ciphertext);
    let mask = encrypt_block(&cipher, j0);
    ciphertext.extend(auth.into_iter().zip(mask).map(|(a, b)| a ^ b));
    ciphertext
}
fn aes_256_gcm_open(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    input: &[u8],
) -> Result<Vec<u8>, BrokerError> {
    if input.len() < 16 {
        return Err(BrokerError::Brk109);
    }
    let split = input.len() - 16;
    let (ciphertext, tag) = input.split_at(split);
    let cipher = Aes256::new(GenericArray::from_slice(key));
    let h = encrypt_block(&cipher, [0; 16]);
    let mut j0 = [0; 16];
    j0[..12].copy_from_slice(nonce);
    j0[15] = 1;
    let auth = ghash(h, aad, ciphertext);
    let mask = encrypt_block(&cipher, j0);
    let expected: Vec<_> = auth.into_iter().zip(mask).map(|(a, b)| a ^ b).collect();
    if !bool::from(expected.ct_eq(tag)) {
        return Err(BrokerError::Brk109);
    }
    let mut counter = j0;
    let mut plaintext = ciphertext.to_vec();
    for chunk in plaintext.chunks_mut(16) {
        increment_counter(&mut counter);
        let stream = encrypt_block(&cipher, counter);
        for (byte, mask) in chunk.iter_mut().zip(stream) {
            *byte ^= mask;
        }
    }
    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vector_and_tamper() {
        let sk = [7; 32];
        let pk = public_key(&sk);
        let (enc, ct) = seal(&pk, [9; 32], b"aad", b"private").unwrap();
        assert_eq!(open(&sk, &enc, b"aad", &ct).unwrap().as_slice(), b"private");
        let mut bad = ct;
        bad[0] ^= 1;
        assert_eq!(
            open(&sk, &enc, b"aad", &bad).unwrap_err(),
            BrokerError::Brk109
        );
    }
}
