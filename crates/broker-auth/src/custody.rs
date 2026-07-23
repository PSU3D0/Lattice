use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use broker_core::{
    BrokerError,
    artifacts::VerificationTier,
    commitment::CommitmentKey,
    credential::{
        CredentialLeaseV2, SecretEnvelopeV2,
        commitment::{CommitmentContextV2, ValueEncoding, commit_v2},
    },
};
use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

use crate::{
    activation::PrivateMaterial,
    profile::{AuthProfile, NormalizedClaims, RegistryPin},
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FencePhase {
    V1Authoritative,
    V2Prepared,
    V2Authoritative,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionStatus {
    Pending,
    Active,
    Rotating,
    Blocked,
    Revoked,
    Destroying,
    Destroyed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RotationPhase {
    Prepared,
    ProviderRequestRecorded,
    ProviderResultObserved,
    NewMaterialSealed,
    AuthorityReconciled,
    Switched,
    RetirementEnqueued,
    OldMaterialDestroyed,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterialMetadata {
    pub generation: u64,
    pub envelope_hash: String,
    pub not_before: i64,
    pub expires_at: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoritySnapshot {
    pub org_id: String,
    pub connection_ref: String,
    pub profile_ref: String,
    pub profile_version: String,
    pub authority_view_hash: String,
    pub authority_epoch: u64,
    pub authorization_claims_commitment: String,
    pub principal_commitment: String,
    pub status: ConnectionStatus,
    pub current_material_generation: u64,
    pub active_material: Option<MaterialMetadata>,
    pub retiring_material: Vec<MaterialMetadata>,
    pub destruction_confirmations: BTreeMap<u64, String>,
    pub fence: FencePhase,
    pub cas_version: u64,
}

struct SealedGeneration {
    metadata: MaterialMetadata,
    envelope_jcs: Zeroizing<Vec<u8>>,
    nonce: [u8; 12],
    ciphertext: Zeroizing<Vec<u8>>,
}
impl Drop for SealedGeneration {
    fn drop(&mut self) {
        self.nonce.zeroize();
    }
}

#[derive(Clone, Debug)]
pub struct RotationRecord {
    pub rotation_ref: String,
    pub old_generation: u64,
    pub new_generation: u64,
    pub expected_authority_epoch: u64,
    pub phase: RotationPhase,
    pub provider_request_hash: Option<String>,
    pub provider_result_commitment: Option<String>,
    pub sealed_envelope_hash: Option<String>,
    pub reconciliation_hash: Option<String>,
    pub switch_hash: Option<String>,
    pub retirement_hash: Option<String>,
    pub destruction_hash: Option<String>,
    pub completion_hash: Option<String>,
    pub cas_version: u64,
}

pub enum ProviderRotationResult {
    Observed {
        material: PrivateMaterial,
        claims: NormalizedClaims,
        expires_at: Option<i64>,
    },
    DefinitelyFailed,
    CrossingUncertain,
}

pub struct CredentialVault {
    root_key: Zeroizing<[u8; 32]>,
    commitment_key: Zeroizing<[u8; 32]>,
    org_id: String,
    connection_ref: String,
    profile_ref: String,
    profile_version: String,
    profile_definition_hash: String,
    scheme_ref: String,
    profile_pin: RegistryPin,
    custody_location: String,
    claims: NormalizedClaims,
    claims_commitment: String,
    principal_commitment: String,
    authority_view_hash: String,
    authority_epoch: u64,
    generations: BTreeMap<u64, SealedGeneration>,
    active_generation: Option<u64>,
    retiring: Vec<u64>,
    confirmations: BTreeMap<u64, String>,
    status: ConnectionStatus,
    fence: FencePhase,
    v2_lease_ever_issued: bool,
    v2_rotation_ever_started: bool,
    v1_leasing_disabled: bool,
    lease_uses: BTreeSet<(String, u8)>,
    rotation: Option<RotationRecord>,
    cas_version: u64,
}

impl CredentialVault {
    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        root_key: [u8; 32],
        commitment_key: [u8; 32],
        org_id: impl Into<String>,
        connection_ref: impl Into<String>,
        profile: &AuthProfile,
        profile_pin: RegistryPin,
        custody_location: impl Into<String>,
        claims: NormalizedClaims,
        principal: &PrivateMaterial,
        material: PrivateMaterial,
        now: i64,
        expires_at: Option<i64>,
    ) -> Result<Self, BrokerError> {
        let org_id = org_id.into();
        let connection_ref = connection_ref.into();
        if org_id.is_empty() || connection_ref.is_empty() {
            return Err(BrokerError::Brk001);
        }
        let claims_bytes = serde_json::to_vec(&claims).map_err(|_| BrokerError::Brk401)?;
        let commitment = CommitmentKey::new("credential-plane-v2", commitment_key)?;
        let claims_commitment = commit_v2(
            &commitment,
            &org_id,
            &CommitmentContextV2::AuthorizationClaims {
                connection_ref: connection_ref.clone(),
                authority_epoch: 1,
            },
            &claims_bytes,
            ValueEncoding::Jcs,
            VerificationTier::Public,
        )?
        .0
        .value;
        let principal_commitment = principal.expose(|value| {
            commit_v2(
                &commitment,
                &org_id,
                &CommitmentContextV2::PrincipalAccountSubject {
                    connection_ref: connection_ref.clone(),
                    authority_epoch: 1,
                },
                value,
                ValueEncoding::OpaqueBytes,
                VerificationTier::Public,
            )
            .map(|result| result.0.value)
        })?;
        let authority_view_hash = hash(
            format!(
                "{org_id}\0{connection_ref}\0{}\0{}\0{claims_commitment}\0{principal_commitment}",
                profile.profile_ref, profile.definition_hash
            )
            .as_bytes(),
        );
        let mut vault = Self {
            root_key: Zeroizing::new(root_key),
            commitment_key: Zeroizing::new(commitment_key),
            org_id,
            connection_ref,
            profile_ref: profile.profile_ref.clone(),
            profile_version: profile.version.clone(),
            profile_definition_hash: profile.definition_hash.clone(),
            scheme_ref: profile.scheme_ref.clone(),
            profile_pin,
            custody_location: custody_location.into(),
            claims,
            claims_commitment,
            principal_commitment,
            authority_view_hash,
            authority_epoch: 1,
            generations: BTreeMap::new(),
            active_generation: None,
            retiring: vec![],
            confirmations: BTreeMap::new(),
            status: ConnectionStatus::Pending,
            fence: FencePhase::V1Authoritative,
            v2_lease_ever_issued: false,
            v2_rotation_ever_started: false,
            v1_leasing_disabled: false,
            lease_uses: BTreeSet::new(),
            rotation: None,
            cas_version: 0,
        };
        vault.seal_generation(1, material, now, expires_at)?;
        vault.readback(1)?;
        vault.fence = FencePhase::V2Prepared;
        Ok(vault)
    }

    #[cfg(any(test, feature = "test-v2-fence"))]
    pub fn activate_v2_after_test_seal(&mut self) -> Result<(), BrokerError> {
        if self.fence != FencePhase::V2Prepared || !self.generations.contains_key(&1) {
            return Err(BrokerError::Brk106);
        }
        self.readback(1)?;
        self.fence = FencePhase::V2Authoritative;
        self.v1_leasing_disabled = true;
        self.active_generation = Some(1);
        self.status = ConnectionStatus::Active;
        self.cas_version += 1;
        Ok(())
    }

    pub fn rollback_prepared(&mut self) -> Result<(), BrokerError> {
        if self.fence != FencePhase::V2Prepared
            || self.v2_lease_ever_issued
            || self.v2_rotation_ever_started
        {
            return Err(BrokerError::Brk106);
        }
        self.generations.clear();
        self.fence = FencePhase::V1Authoritative;
        self.cas_version += 1;
        Ok(())
    }

    pub fn lease(
        &mut self,
        effect_grant_hash: &str,
        dispatch_attempt: u8,
        now: i64,
        ttl_seconds: i64,
    ) -> Result<CredentialLeaseV2, BrokerError> {
        if self.fence != FencePhase::V2Authoritative
            || !self.v1_leasing_disabled
            || self.status != ConnectionStatus::Active
            || dispatch_attempt == 0
        {
            return Err(BrokerError::Brk106);
        }
        if !self
            .lease_uses
            .insert((effect_grant_hash.into(), dispatch_attempt))
        {
            return Err(BrokerError::Brk203);
        }
        let generation = self.active_generation.ok_or(BrokerError::Brk106)?;
        let material = self.readback(generation)?;
        self.v2_lease_ever_issued = true;
        let value = serde_json::json!({"private_codec_version":"0.2","critical_fields":[],"extensions":{},"lease_ref":format!("lease_{}_{dispatch_attempt}",&effect_grant_hash[effect_grant_hash.len().saturating_sub(16)..]),"effect_grant_hash":effect_grant_hash,"dispatch_attempt":dispatch_attempt,"org_id":self.org_id,"connection_ref":self.connection_ref,"scheme_ref":self.scheme_ref,"auth_profile_pin":self.profile_pin,"material_kind":"credential_material","authority_view_hash":self.authority_view_hash,"authority_epoch":self.authority_epoch,"leased_material_generation":generation,"issued_at":timestamp(now),"expires_at":timestamp(now.checked_add(ttl_seconds).ok_or(BrokerError::Brk401)?),"use_limit":1,"private_material_b64u":URL_SAFE_NO_PAD.encode(&*material)});
        serde_json::from_value(value).map_err(|_| BrokerError::Brk109)
    }

    pub fn begin_rotation(
        &mut self,
        rotation_ref: impl Into<String>,
        expected_cas: u64,
    ) -> Result<&RotationRecord, BrokerError> {
        if self.fence != FencePhase::V2Authoritative
            || self.status != ConnectionStatus::Active
            || expected_cas != self.cas_version
        {
            return Err(BrokerError::Brk204);
        }
        let old = self.active_generation.ok_or(BrokerError::Brk106)?;
        let new = old.checked_add(1).ok_or(BrokerError::Brk401)?;
        self.v2_rotation_ever_started = true;
        self.status = ConnectionStatus::Rotating;
        self.cas_version += 1;
        self.rotation = Some(RotationRecord {
            rotation_ref: rotation_ref.into(),
            old_generation: old,
            new_generation: new,
            expected_authority_epoch: self.authority_epoch,
            phase: RotationPhase::Prepared,
            provider_request_hash: None,
            provider_result_commitment: None,
            sealed_envelope_hash: None,
            reconciliation_hash: None,
            switch_hash: None,
            retirement_hash: None,
            destruction_hash: None,
            completion_hash: None,
            cas_version: 0,
        });
        Ok(self.rotation.as_ref().unwrap())
    }

    pub fn record_provider_request(&mut self, request_jcs: &[u8]) -> Result<(), BrokerError> {
        let r = self.rotation.as_mut().ok_or(BrokerError::Brk103)?;
        if r.phase != RotationPhase::Prepared {
            return Err(BrokerError::Brk204);
        }
        r.provider_request_hash = Some(hash(request_jcs));
        r.phase = RotationPhase::ProviderRequestRecorded;
        r.cas_version += 1;
        Ok(())
    }

    pub fn observe_rotation(
        &mut self,
        result: ProviderRotationResult,
        now: i64,
    ) -> Result<(), BrokerError> {
        if self
            .rotation
            .as_ref()
            .is_none_or(|r| r.phase != RotationPhase::ProviderRequestRecorded)
        {
            return Err(BrokerError::Brk204);
        }
        match result {
            ProviderRotationResult::DefinitelyFailed => {
                self.status = ConnectionStatus::Active;
                self.rotation = None;
                self.cas_version += 1;
                Err(BrokerError::Brk401)
            }
            ProviderRotationResult::CrossingUncertain => {
                self.status = ConnectionStatus::Blocked;
                self.authority_epoch = self
                    .authority_epoch
                    .checked_add(1)
                    .ok_or(BrokerError::Brk401)?;
                self.cas_version += 1;
                Err(BrokerError::Brk401)
            }
            ProviderRotationResult::Observed {
                material,
                claims,
                expires_at,
            } => {
                if self.claims != claims {
                    self.status = ConnectionStatus::Blocked;
                    self.authority_epoch += 1;
                    return Err(BrokerError::Brk109);
                }
                let rotation_ref = self
                    .rotation
                    .as_ref()
                    .map(|rotation| rotation.rotation_ref.clone())
                    .ok_or(BrokerError::Brk103)?;
                let commitment_key =
                    CommitmentKey::new("credential-plane-v2", *self.commitment_key)?;
                let commitment = commit_v2(
                    &commitment_key,
                    &self.org_id,
                    &CommitmentContextV2::RotationProviderResult {
                        connection_ref: self.connection_ref.clone(),
                        rotation_ref,
                    },
                    br#"{"outcome":"observed"}"#,
                    ValueEncoding::Jcs,
                    VerificationTier::Public,
                )?
                .0
                .value;
                {
                    let r = self.rotation.as_mut().unwrap();
                    r.provider_result_commitment = Some(commitment);
                    r.phase = RotationPhase::ProviderResultObserved;
                    r.cas_version += 1;
                }
                let generation = self.rotation.as_ref().unwrap().new_generation;
                self.seal_generation(generation, material, now, expires_at)?;
                self.readback(generation)?;
                let envelope_hash = self.generations[&generation].metadata.envelope_hash.clone();
                {
                    let r = self.rotation.as_mut().unwrap();
                    r.sealed_envelope_hash = Some(envelope_hash);
                    r.phase = RotationPhase::NewMaterialSealed;
                    r.cas_version += 1;
                    r.reconciliation_hash = Some(hash(self.authority_view_hash.as_bytes()));
                    r.phase = RotationPhase::AuthorityReconciled;
                    r.cas_version += 1;
                }
                Ok(())
            }
        }
    }

    pub fn switch_rotation(&mut self, expected_cas: u64) -> Result<(), BrokerError> {
        if expected_cas != self.cas_version {
            return Err(BrokerError::Brk204);
        }
        let (old, new) = match self.rotation.as_ref() {
            Some(r) if r.phase == RotationPhase::AuthorityReconciled => {
                (r.old_generation, r.new_generation)
            }
            _ => return Err(BrokerError::Brk204),
        };
        if self.retiring.len() >= 2 {
            return Err(BrokerError::Brk401);
        }
        self.active_generation = Some(new);
        self.retiring.push(old);
        let r = self.rotation.as_mut().unwrap();
        r.switch_hash = Some(hash(format!("{old}->{new}").as_bytes()));
        r.phase = RotationPhase::Switched;
        r.cas_version += 1;
        r.retirement_hash = Some(hash(format!("retire:{old}").as_bytes()));
        r.phase = RotationPhase::RetirementEnqueued;
        r.cas_version += 1;
        self.status = ConnectionStatus::Active;
        self.cas_version += 1;
        Ok(())
    }

    pub fn confirm_retirement(
        &mut self,
        generation: u64,
        evidence_hash: String,
    ) -> Result<(), BrokerError> {
        if !self.retiring.contains(&generation) || !valid_hash(&evidence_hash) {
            return Err(BrokerError::Brk109);
        }
        self.generations.remove(&generation);
        self.retiring.retain(|g| *g != generation);
        self.confirmations.insert(generation, evidence_hash.clone());
        if let Some(r) = self.rotation.as_mut()
            && r.old_generation == generation
            && r.phase == RotationPhase::RetirementEnqueued
        {
            r.destruction_hash = Some(evidence_hash);
            r.phase = RotationPhase::OldMaterialDestroyed;
            r.cas_version += 1;
            r.completion_hash = Some(hash(r.rotation_ref.as_bytes()));
            r.phase = RotationPhase::Complete;
            r.cas_version += 1;
        }
        self.cas_version += 1;
        Ok(())
    }

    pub fn revoke(&mut self) -> Result<(), BrokerError> {
        self.authority_epoch = self
            .authority_epoch
            .checked_add(1)
            .ok_or(BrokerError::Brk401)?;
        self.status = ConnectionStatus::Revoked;
        self.cas_version += 1;
        Ok(())
    }
    pub fn destroy(&mut self, confirmations: BTreeMap<u64, String>) -> Result<(), BrokerError> {
        self.status = ConnectionStatus::Destroying;
        let all = self.generations.keys().copied().collect::<Vec<_>>();
        if all.iter().any(|g| !confirmations.contains_key(g)) {
            return Err(BrokerError::Brk109);
        }
        self.generations.clear();
        self.active_generation = None;
        self.retiring.clear();
        self.confirmations.extend(confirmations);
        self.status = ConnectionStatus::Destroyed;
        self.cas_version += 1;
        Ok(())
    }
    pub fn snapshot(&self) -> AuthoritySnapshot {
        AuthoritySnapshot {
            org_id: self.org_id.clone(),
            connection_ref: self.connection_ref.clone(),
            profile_ref: self.profile_ref.clone(),
            profile_version: self.profile_version.clone(),
            authority_view_hash: self.authority_view_hash.clone(),
            authority_epoch: self.authority_epoch,
            authorization_claims_commitment: self.claims_commitment.clone(),
            principal_commitment: self.principal_commitment.clone(),
            status: self.status,
            current_material_generation: self.active_generation.unwrap_or(0),
            active_material: self
                .active_generation
                .and_then(|g| self.generations.get(&g).map(|s| s.metadata.clone())),
            retiring_material: self
                .retiring
                .iter()
                .filter_map(|g| self.generations.get(g).map(|s| s.metadata.clone()))
                .collect(),
            destruction_confirmations: self.confirmations.clone(),
            fence: self.fence,
            cas_version: self.cas_version,
        }
    }
    pub fn rotation(&self) -> Option<&RotationRecord> {
        self.rotation.as_ref()
    }

    fn seal_generation(
        &mut self,
        generation: u64,
        material: PrivateMaterial,
        now: i64,
        expires_at: Option<i64>,
    ) -> Result<(), BrokerError> {
        if generation == 0
            || self.generations.contains_key(&generation)
            || self
                .generations
                .keys()
                .next_back()
                .is_some_and(|g| generation <= *g)
        {
            return Err(BrokerError::Brk106);
        }
        let nonce = nonce_for(&self.root_key[..], &self.connection_ref, generation);
        let aad=serde_json::to_vec(&serde_json::json!({"org_id":self.org_id,"connection_ref":self.connection_ref,"profile_definition_hash":self.profile_definition_hash,"authority_view_hash":self.authority_view_hash,"authority_epoch":self.authority_epoch,"generation":generation})).map_err(|_|BrokerError::Brk401)?;
        let ciphertext = material.expose(|m| {
            ChaCha20Poly1305::new((&*self.root_key).into())
                .encrypt((&nonce).into(), Payload { msg: m, aad: &aad })
                .map_err(|_| BrokerError::Brk401)
        })?;
        let envelope = serde_json::json!({"private_codec_version":"0.2","critical_fields":[],"extensions":{},"scheme_ref":self.scheme_ref,"auth_profile_pin":self.profile_pin,"material_kind":"credential_material","sealing_key_id":"custody-root-v2","org_id":self.org_id,"connection_ref":self.connection_ref,"custody_location":self.custody_location,"authority_view_hash":self.authority_view_hash,"authority_epoch":self.authority_epoch,"material_generation":generation,"created_at":timestamp(now),"not_before":timestamp(now),"expires_at":expires_at.map(timestamp),"nonce":URL_SAFE_NO_PAD.encode(nonce),"ciphertext":URL_SAFE_NO_PAD.encode(&ciphertext),"authenticated_context_hash":hash(&aad)});
        let envelope_jcs = serde_json::to_vec(&envelope).map_err(|_| BrokerError::Brk401)?;
        let _: SecretEnvelopeV2 =
            serde_json::from_slice(&envelope_jcs).map_err(|_| BrokerError::Brk109)?;
        let metadata = MaterialMetadata {
            generation,
            envelope_hash: hash(&envelope_jcs),
            not_before: now,
            expires_at,
        };
        self.generations.insert(
            generation,
            SealedGeneration {
                metadata,
                envelope_jcs: Zeroizing::new(envelope_jcs),
                nonce,
                ciphertext: Zeroizing::new(ciphertext),
            },
        );
        Ok(())
    }
    fn readback(&self, generation: u64) -> Result<Zeroizing<Vec<u8>>, BrokerError> {
        let sealed = self
            .generations
            .get(&generation)
            .ok_or(BrokerError::Brk103)?;
        let _: SecretEnvelopeV2 =
            serde_json::from_slice(&sealed.envelope_jcs).map_err(|_| BrokerError::Brk109)?;
        let aad=serde_json::to_vec(&serde_json::json!({"org_id":self.org_id,"connection_ref":self.connection_ref,"profile_definition_hash":self.profile_definition_hash,"authority_view_hash":self.authority_view_hash,"authority_epoch":self.authority_epoch,"generation":generation})).map_err(|_|BrokerError::Brk401)?;
        ChaCha20Poly1305::new((&*self.root_key).into())
            .decrypt(
                (&sealed.nonce).into(),
                Payload {
                    msg: &sealed.ciphertext,
                    aad: &aad,
                },
            )
            .map(Zeroizing::new)
            .map_err(|_| BrokerError::Brk109)
    }
}

fn hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex(&Sha256::digest(bytes)))
}
fn valid_hash(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut o = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        o.push(H[(b >> 4) as usize] as char);
        o.push(H[(b & 15) as usize] as char)
    }
    o
}
fn nonce_for(key: &[u8], connection: &str, generation: u64) -> [u8; 12] {
    let mut m = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC key");
    m.update(connection.as_bytes());
    m.update(&generation.to_be_bytes());
    let out = m.finalize().into_bytes();
    let mut n = [0; 12];
    n.copy_from_slice(&out[..12]);
    n
}
fn timestamp(seconds: i64) -> String {
    // Civil date conversion, UTC.
    let days = seconds.div_euclid(86400);
    let sod = seconds.rem_euclid(86400);
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    y += if m <= 2 { 1 } else { 0 };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}
