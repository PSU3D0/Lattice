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
const INFO: &[u8] = b"lattice.remote-custody-envelope.v0.2";

pub(crate) fn public_key(private_key: &[u8; 32]) -> [u8; 32] {
    PublicKey::from(&StaticSecret::from(*private_key)).to_bytes()
}

pub(crate) fn seal(
    recipient_public_key: &[u8; 32],
    ephemeral_private_key: [u8; 32],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<([u8; 32], Vec<u8>), BrokerError> {
    let ephemeral = StaticSecret::from(ephemeral_private_key);
    let encapsulated = PublicKey::from(&ephemeral).to_bytes();
    let recipient = PublicKey::from(*recipient_public_key);
    let shared = Zeroizing::new(ephemeral.diffie_hellman(&recipient).to_bytes());
    if bool::from(shared.ct_eq(&[0; 32])) {
        return Err(BrokerError::Brk109);
    }
    let (key, nonce) = key_schedule(&shared, &encapsulated, recipient_public_key)?;
    let ciphertext = aes_256_gcm_seal(&key, &nonce, aad, plaintext);
    Ok((encapsulated, ciphertext))
}

pub(crate) fn open(
    recipient_private_key: &[u8; 32],
    encapsulated_key: &[u8; 32],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Zeroizing<Vec<u8>>, BrokerError> {
    let recipient = StaticSecret::from(*recipient_private_key);
    let recipient_public = PublicKey::from(&recipient).to_bytes();
    let encapsulated = PublicKey::from(*encapsulated_key);
    let shared = Zeroizing::new(recipient.diffie_hellman(&encapsulated).to_bytes());
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
    let key = Zeroizing::new(labeled_expand::<32>(&secret, &suite, b"key", &context)?);
    let nonce = labeled_expand::<12>(&secret, &suite, b"base_nonce", &context)?;
    Ok((key, nonce))
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
    ciphertext.extend(auth.into_iter().zip(mask).map(|(left, right)| left ^ right));
    ciphertext
}

fn aes_256_gcm_open(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    ciphertext_and_tag: &[u8],
) -> Result<Vec<u8>, BrokerError> {
    if ciphertext_and_tag.len() < 16 {
        return Err(BrokerError::Brk109);
    }
    let split = ciphertext_and_tag.len() - 16;
    let (ciphertext, supplied_tag) = ciphertext_and_tag.split_at(split);
    let cipher = Aes256::new(GenericArray::from_slice(key));
    let h = encrypt_block(&cipher, [0; 16]);
    let mut j0 = [0; 16];
    j0[..12].copy_from_slice(nonce);
    j0[15] = 1;
    let auth = ghash(h, aad, ciphertext);
    let mask = encrypt_block(&cipher, j0);
    let expected: Vec<u8> = auth.into_iter().zip(mask).map(|(a, b)| a ^ b).collect();
    if !bool::from(expected.ct_eq(supplied_tag)) {
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

fn encrypt_block(cipher: &Aes256, block: [u8; 16]) -> [u8; 16] {
    let mut block = GenericArray::clone_from_slice(&block);
    cipher.encrypt_block(&mut block);
    block.into()
}

fn increment_counter(counter: &mut [u8; 16]) {
    let value = u32::from_be_bytes(counter[12..].try_into().expect("four-byte counter"));
    counter[12..].copy_from_slice(&value.wrapping_add(1).to_be_bytes());
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
    let mut z = 0u128;
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
mod tests {
    use super::*;

    #[test]
    fn aes_gcm_matches_nist_empty_vector() {
        let key = [0; 32];
        let nonce = [0; 12];
        let sealed = aes_256_gcm_seal(&key, &nonce, &[], &[]);
        assert_eq!(hex(&sealed), "530f8afbc74536b9a963b4f1c4cb738b");
        assert_eq!(
            aes_256_gcm_open(&key, &nonce, &[], &sealed).unwrap(),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn aes_gcm_matches_nist_single_block_vector() {
        let key = [0; 32];
        let nonce = [0; 12];
        let sealed = aes_256_gcm_seal(&key, &nonce, &[], &[0; 16]);
        assert_eq!(
            hex(&sealed),
            "cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919"
        );
    }

    #[test]
    fn deterministic_hpke_vector_and_tamper() {
        let recipient_private = [7; 32];
        let recipient_public = public_key(&recipient_private);
        let (enc, ciphertext) = seal(&recipient_public, [9; 32], b"aad", b"private").unwrap();
        assert_eq!(
            hex(&enc),
            "57db4b359f23ae5e146e4e2512056704722506348c150c14753d0c933d04d421"
        );
        assert_eq!(
            hex(&ciphertext),
            "a8d18667f51909ccd84f0589bc32cddd5c5bb3afe1b059"
        );
        assert_eq!(
            &*open(&recipient_private, &enc, b"aad", &ciphertext).unwrap(),
            b"private"
        );
        let mut tampered = ciphertext;
        tampered[0] ^= 1;
        assert_eq!(
            open(&recipient_private, &enc, b"aad", &tampered).unwrap_err(),
            BrokerError::Brk109
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
