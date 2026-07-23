use std::collections::{BTreeMap, BTreeSet};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use broker_core::BrokerError;
use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::profile::{
    ActivationKind, ApprovedRegistry, AuthProfile, AuthScheme, NormalizedClaims, RegistryPin,
};

type HmacSha256 = Hmac<Sha256>;

pub trait NonceSource {
    fn nonce(&mut self, purpose: &str) -> Result<String, BrokerError>;
}

#[derive(Default)]
pub struct DeterministicNonceSource(u64);
impl NonceSource for DeterministicNonceSource {
    fn nonce(&mut self, purpose: &str) -> Result<String, BrokerError> {
        self.0 = self.0.checked_add(1).ok_or(BrokerError::Brk401)?;
        Ok(format!("{purpose}_{:032x}", self.0))
    }
}

#[derive(Clone, Debug)]
pub struct ActivationRequest {
    pub org_id: String,
    pub operator_id: String,
    pub connector_ref: String,
    pub profile_ref: String,
    pub profile_version: String,
    pub standing_authority_ref: String,
    pub standing_authority_hash: String,
    pub contract_set_ref: String,
    pub contract_set_hash: String,
    pub contract_ids: BTreeSet<String>,
    pub deployment_id: String,
    pub execution_lane: String,
    pub custody_location: String,
    pub request_jti: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NextAction {
    OpenUrl {
        activation_ref: String,
        expires_at: i64,
        correlation_handle: String,
        url: String,
    },
    SubmitPrivateMaterial {
        activation_ref: String,
        expires_at: i64,
        channel_ref: String,
        recipient_key_id: String,
        submission_schema_hash: String,
    },
    BindExternalCustodian {
        activation_ref: String,
        expires_at: i64,
        challenge: String,
        allowed_custodians: Vec<RegistryPin>,
    },
    PresentWorkloadAssertion {
        activation_ref: String,
        expires_at: i64,
        channel_ref: String,
        recipient_key_id: String,
        audience: String,
        nonce: String,
        assertion_schema_hash: String,
    },
    Poll {
        activation_ref: String,
        expires_at: i64,
        retry_after_ms: u32,
    },
    Complete {
        activation_ref: String,
        expires_at: i64,
        connection_ref: String,
        authority_view_hash: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicActivationSnapshot {
    pub activation_ref: String,
    pub profile_ref: String,
    pub profile_version: String,
    pub status: &'static str,
    pub claims_commitment: String,
    pub public_claims: Option<BTreeMap<String, bool>>,
    pub dynamic_source_refs: Vec<RegistryPin>,
    pub policy_refs: Vec<RegistryPin>,
}

pub struct PrivateMaterial(Vec<u8>);
impl PrivateMaterial {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, BrokerError> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > 1024 * 1024 {
            return Err(BrokerError::Brk001);
        }
        Ok(Self(bytes))
    }
    pub(crate) fn expose<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.0)
    }
}
impl std::fmt::Debug for PrivateMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrivateMaterial([REDACTED])")
    }
}
impl Drop for PrivateMaterial {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedSubmission {
    pub channel_ref: String,
    pub request_jti: String,
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
    pub aad_hash: String,
}
impl std::fmt::Debug for EncryptedSubmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedSubmission")
            .field("channel_ref", &self.channel_ref)
            .field("request_jti", &self.request_jti)
            .field("ciphertext", &"[ENCRYPTED]")
            .finish()
    }
}

pub fn encrypt_submission(
    key: &[u8; 32],
    channel_ref: &str,
    request_jti: &str,
    aad: &SubmissionAad,
    nonce: [u8; 12],
    plaintext: &[u8],
) -> Result<EncryptedSubmission, BrokerError> {
    if plaintext.is_empty() || plaintext.len() > 1024 * 1024 {
        return Err(BrokerError::Brk001);
    }
    let aad_bytes = aad.bytes(channel_ref, request_jti)?;
    let ciphertext = ChaCha20Poly1305::new(key.into())
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: plaintext,
                aad: &aad_bytes,
            },
        )
        .map_err(|_| BrokerError::Brk109)?;
    Ok(EncryptedSubmission {
        channel_ref: channel_ref.into(),
        request_jti: request_jti.into(),
        nonce,
        ciphertext,
        aad_hash: hash(&aad_bytes),
    })
}

#[derive(Clone, Debug)]
pub struct SubmissionAad {
    pub org_id: String,
    pub operator_id: String,
    pub activation_ref: String,
    pub schema_hash: String,
    pub expires_at: i64,
}
impl SubmissionAad {
    fn bytes(&self, channel_ref: &str, request_jti: &str) -> Result<Vec<u8>, BrokerError> {
        serde_json::to_vec(&serde_json::json!({
            "activation_ref": self.activation_ref, "channel_ref": channel_ref,
            "expires_at": self.expires_at, "operator_id": self.operator_id,
            "org_id": self.org_id, "request_jti": request_jti, "schema_hash": self.schema_hash,
        }))
        .map_err(|_| BrokerError::Brk401)
    }
}

#[derive(Clone, Debug)]
pub struct OAuthCallback {
    pub correlation_handle: String,
    pub state: String,
    pub code: Option<String>,
    pub error: Option<String>,
}

pub struct TokenExchangeRequest<'a> {
    pub profile: &'a AuthProfile,
    pub endpoint: &'a str,
    pub redirect_uri: &'a str,
    pub code: &'a str,
    pub pkce_verifier: &'a str,
    pub expected_claims: &'a NormalizedClaims,
}
impl std::fmt::Debug for TokenExchangeRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenExchangeRequest([REDACTED])")
    }
}

pub struct WorkloadExchangeRequest<'a> {
    pub profile: &'a AuthProfile,
    pub endpoint: &'a str,
    pub assertion: &'a [u8],
    pub expected_claims: &'a NormalizedClaims,
}
impl std::fmt::Debug for WorkloadExchangeRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkloadExchangeRequest([REDACTED])")
    }
}

pub struct WorkloadPresentation<'a> {
    pub now: i64,
    pub issued_at: i64,
    pub issuer: &'a str,
    pub audience: &'a str,
    pub nonce: &'a str,
}

pub struct ActivationMaterial {
    pub material: PrivateMaterial,
    pub normalized_claims: NormalizedClaims,
    pub principal_subject: PrivateMaterial,
}
impl std::fmt::Debug for ActivationMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActivationMaterial([REDACTED])")
    }
}

pub trait TokenService: Send + Sync {
    fn exchange_authorization_code(
        &self,
        request: TokenExchangeRequest<'_>,
    ) -> Result<ActivationMaterial, BrokerError>;
    fn exchange_workload_assertion(
        &self,
        request: WorkloadExchangeRequest<'_>,
    ) -> Result<ActivationMaterial, BrokerError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Awaiting,
    Claimed,
    Active,
    RestartRequired,
    Failed,
}

struct ActivationRecord {
    request: ActivationRequest,
    profile_definition_hash: String,
    claims: NormalizedClaims,
    claims_commitment: String,
    status: Status,
    expires_at: i64,
    correlation_handle: Option<String>,
    state_hash: Option<[u8; 32]>,
    pkce_verifier: Option<Zeroizing<String>>,
    channel_ref: Option<String>,
    channel_key: Option<Zeroizing<[u8; 32]>>,
    channel_schema_hash: Option<String>,
    challenge: Option<String>,
    workload_nonce: Option<String>,
    connection_ref: Option<String>,
    authority_view_hash: Option<String>,
    dynamic_source_refs: Vec<RegistryPin>,
    policy_refs: Vec<RegistryPin>,
}

pub struct ActivationEngine {
    registry: ApprovedRegistry,
    records: BTreeMap<String, ActivationRecord>,
    correlations: BTreeMap<String, String>,
    request_jtis: BTreeSet<(String, String)>,
    private_jtis: BTreeSet<(String, String)>,
    commitment_key: Zeroizing<[u8; 32]>,
    recipient_key_id: String,
}

impl ActivationEngine {
    pub fn new(
        registry: ApprovedRegistry,
        commitment_key: [u8; 32],
        recipient_key_id: impl Into<String>,
    ) -> Self {
        Self {
            registry,
            records: BTreeMap::new(),
            correlations: BTreeMap::new(),
            request_jtis: BTreeSet::new(),
            private_jtis: BTreeSet::new(),
            commitment_key: Zeroizing::new(commitment_key),
            recipient_key_id: recipient_key_id.into(),
        }
    }

    pub fn registry(&self) -> &ApprovedRegistry {
        &self.registry
    }

    pub fn create(
        &mut self,
        request: ActivationRequest,
        now: i64,
        nonces: &mut dyn NonceSource,
    ) -> Result<NextAction, BrokerError> {
        if request.org_id.is_empty()
            || request.operator_id.is_empty()
            || request.request_jti.is_empty()
            || request.standing_authority_hash.is_empty()
            || request.contract_set_hash.is_empty()
        {
            return Err(BrokerError::Brk001);
        }
        if !self
            .request_jtis
            .insert((request.org_id.clone(), request.request_jti.clone()))
        {
            return Err(BrokerError::Brk203);
        }
        let profile = self.registry.profile_for_connector(
            &request.connector_ref,
            &request.profile_ref,
            &request.profile_version,
        )?;
        let claims = profile.derive_claims(
            &request.connector_ref,
            request.contract_ids.iter().map(String::as_str),
        )?;
        let activation_ref = nonces.nonce("activation")?;
        let expires_at = now.checked_add(600).ok_or(BrokerError::Brk401)?;
        let claims_commitment =
            claims_commitment(&self.commitment_key[..], &activation_ref, &claims)?;
        let mut record = ActivationRecord {
            request,
            profile_definition_hash: profile.definition_hash.clone(),
            claims,
            claims_commitment,
            status: Status::Awaiting,
            expires_at,
            correlation_handle: None,
            state_hash: None,
            pkce_verifier: None,
            channel_ref: None,
            channel_key: None,
            channel_schema_hash: None,
            challenge: None,
            workload_nonce: None,
            connection_ref: None,
            authority_view_hash: None,
            dynamic_source_refs: vec![],
            policy_refs: vec![],
        };
        let action = match &profile.activation {
            ActivationKind::OAuthAuthorizationCodePkce => {
                let correlation = nonces.nonce("correlation")?;
                let state = nonces.nonce("state")?;
                let verifier = Zeroizing::new(nonces.nonce("pkce")?);
                let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
                let endpoint_key = match &profile.scheme {
                    AuthScheme::OAuthPkce {
                        authorization_endpoint_key,
                        ..
                    } => authorization_endpoint_key,
                    _ => return Err(BrokerError::Brk004),
                };
                let endpoint = profile.endpoint(endpoint_key)?;
                let url = format!(
                    "{}?code_challenge={}&code_challenge_method=S256&redirect_uri={}&response_type=code&scope={}&state={}",
                    endpoint,
                    pct(&challenge),
                    pct(&profile.callback_uri),
                    pct(&record
                        .claims
                        .values
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" ")),
                    pct(&state)
                );
                record.correlation_handle = Some(correlation.clone());
                record.state_hash = Some(Sha256::digest(state.as_bytes()).into());
                record.pkce_verifier = Some(verifier);
                self.correlations
                    .insert(correlation.clone(), activation_ref.clone());
                NextAction::OpenUrl {
                    activation_ref: activation_ref.clone(),
                    expires_at,
                    correlation_handle: correlation,
                    url,
                }
            }
            ActivationKind::SecretSubmission => {
                let (channel_ref, key) = channel(nonces)?;
                record.channel_ref = Some(channel_ref.clone());
                record.channel_key = Some(Zeroizing::new(key));
                record.channel_schema_hash = Some(profile.material_schema_hash.clone());
                NextAction::SubmitPrivateMaterial {
                    activation_ref: activation_ref.clone(),
                    expires_at,
                    channel_ref,
                    recipient_key_id: self.recipient_key_id.clone(),
                    submission_schema_hash: profile.material_schema_hash.clone(),
                }
            }
            ActivationKind::ExternalCustodianBinding => {
                let challenge = nonces.nonce("custodian_challenge")?;
                record.challenge = Some(challenge.clone());
                let allowed = match &profile.scheme {
                    AuthScheme::ExternalCustodian { allowed_custodians } => {
                        allowed_custodians.clone()
                    }
                    _ => return Err(BrokerError::Brk004),
                };
                NextAction::BindExternalCustodian {
                    activation_ref: activation_ref.clone(),
                    expires_at,
                    challenge,
                    allowed_custodians: allowed,
                }
            }
            ActivationKind::WorkloadBinding => {
                let (channel_ref, key) = channel(nonces)?;
                let nonce = nonces.nonce("workload_nonce")?;
                let (audience, schema) = match &profile.scheme {
                    AuthScheme::WorkloadTokenExchange { audience, .. } => (
                        audience.clone(),
                        profile
                            .assertion_schema_hash
                            .clone()
                            .ok_or(BrokerError::Brk004)?,
                    ),
                    _ => return Err(BrokerError::Brk004),
                };
                record.channel_ref = Some(channel_ref.clone());
                record.channel_key = Some(Zeroizing::new(key));
                record.channel_schema_hash = Some(schema.clone());
                record.workload_nonce = Some(nonce.clone());
                NextAction::PresentWorkloadAssertion {
                    activation_ref: activation_ref.clone(),
                    expires_at,
                    channel_ref,
                    recipient_key_id: self.recipient_key_id.clone(),
                    audience,
                    nonce,
                    assertion_schema_hash: schema,
                }
            }
            ActivationKind::None => return Err(BrokerError::Brk004),
        };
        self.records.insert(activation_ref, record);
        Ok(action)
    }

    pub fn submission_context(
        &self,
        activation_ref: &str,
    ) -> Result<(SubmissionAad, [u8; 32]), BrokerError> {
        let record = self
            .records
            .get(activation_ref)
            .ok_or(BrokerError::Brk103)?;
        let key = record.channel_key.as_ref().ok_or(BrokerError::Brk109)?;
        Ok((
            SubmissionAad {
                org_id: record.request.org_id.clone(),
                operator_id: record.request.operator_id.clone(),
                activation_ref: activation_ref.into(),
                schema_hash: record
                    .channel_schema_hash
                    .clone()
                    .ok_or(BrokerError::Brk109)?,
                expires_at: record.expires_at,
            },
            **key,
        ))
    }

    pub fn submit_private_material(
        &mut self,
        activation_ref: &str,
        submission: EncryptedSubmission,
        now: i64,
    ) -> Result<PrivateMaterial, BrokerError> {
        let record = self
            .records
            .get_mut(activation_ref)
            .ok_or(BrokerError::Brk103)?;
        if record.status != Status::Awaiting
            || now >= record.expires_at
            || record.channel_ref.as_deref() != Some(&submission.channel_ref)
        {
            return Err(BrokerError::Brk109);
        }
        if !self
            .private_jtis
            .insert((activation_ref.into(), submission.request_jti.clone()))
        {
            return Err(BrokerError::Brk203);
        }
        let aad = SubmissionAad {
            org_id: record.request.org_id.clone(),
            operator_id: record.request.operator_id.clone(),
            activation_ref: activation_ref.into(),
            schema_hash: record
                .channel_schema_hash
                .clone()
                .ok_or(BrokerError::Brk109)?,
            expires_at: record.expires_at,
        };
        let aad_bytes = aad.bytes(&submission.channel_ref, &submission.request_jti)?;
        if !bool::from(
            hash(&aad_bytes)
                .as_bytes()
                .ct_eq(submission.aad_hash.as_bytes()),
        ) {
            return Err(BrokerError::Brk109);
        }
        record.status = Status::Claimed;
        let key = record.channel_key.take().ok_or(BrokerError::Brk109)?;
        let plaintext = ChaCha20Poly1305::new((&*key).into())
            .decrypt(
                (&submission.nonce).into(),
                Payload {
                    msg: &submission.ciphertext,
                    aad: &aad_bytes,
                },
            )
            .map_err(|_| BrokerError::Brk109)?;
        Ok(PrivateMaterial(plaintext))
    }

    pub fn universal_callback(
        &mut self,
        callback: OAuthCallback,
        now: i64,
        service: &dyn TokenService,
    ) -> Result<NextAction, BrokerError> {
        let activation_ref = self
            .correlations
            .get(&callback.correlation_handle)
            .cloned()
            .ok_or(BrokerError::Brk109)?;
        let record = self
            .records
            .get_mut(&activation_ref)
            .ok_or(BrokerError::Brk103)?;
        if record.status == Status::Active {
            return complete_action(&activation_ref, record);
        }
        if record.status != Status::Awaiting || now >= record.expires_at {
            return Err(BrokerError::Brk203);
        }
        record.status = Status::Claimed;
        let state_hash: [u8; 32] = Sha256::digest(callback.state.as_bytes()).into();
        if !bool::from(state_hash.ct_eq(&record.state_hash.ok_or(BrokerError::Brk109)?))
            || callback.error.is_some()
        {
            record.status = Status::Failed;
            return Err(BrokerError::Brk109);
        }
        let code = callback
            .code
            .as_deref()
            .filter(|code| !code.is_empty())
            .ok_or(BrokerError::Brk109)?;
        let profile = self
            .registry
            .profile(&record.request.profile_ref, &record.request.profile_version)?;
        if profile.definition_hash != record.profile_definition_hash {
            return Err(BrokerError::Brk106);
        }
        let token_key = match &profile.scheme {
            AuthScheme::OAuthPkce {
                token_endpoint_key, ..
            } => token_endpoint_key,
            _ => return Err(BrokerError::Brk004),
        };
        let verifier = record.pkce_verifier.take().ok_or(BrokerError::Brk109)?;
        let result = service.exchange_authorization_code(TokenExchangeRequest {
            profile,
            endpoint: profile.endpoint(token_key)?,
            redirect_uri: &profile.callback_uri,
            code,
            pkce_verifier: &verifier,
            expected_claims: &record.claims,
        });
        match result {
            Ok(material) => self.finish(&activation_ref, material, now),
            Err(BrokerError::Brk401) => {
                record.status = Status::RestartRequired;
                Err(BrokerError::Brk401)
            }
            Err(error) => {
                record.status = Status::Failed;
                Err(error)
            }
        }
    }

    pub fn complete_secret(
        &mut self,
        activation_ref: &str,
        material: PrivateMaterial,
        principal: PrivateMaterial,
        now: i64,
    ) -> Result<NextAction, BrokerError> {
        let claims = self
            .records
            .get(activation_ref)
            .ok_or(BrokerError::Brk103)?
            .claims
            .clone();
        self.finish(
            activation_ref,
            ActivationMaterial {
                material,
                normalized_claims: claims,
                principal_subject: principal,
            },
            now,
        )
    }

    pub fn complete_workload(
        &mut self,
        activation_ref: &str,
        assertion: PrivateMaterial,
        presentation: WorkloadPresentation<'_>,
        service: &dyn TokenService,
    ) -> Result<NextAction, BrokerError> {
        let WorkloadPresentation {
            now,
            issued_at,
            issuer,
            audience,
            nonce,
        } = presentation;
        let record = self
            .records
            .get(activation_ref)
            .ok_or(BrokerError::Brk103)?;
        if !matches!(record.status, Status::Awaiting | Status::Claimed) {
            return Err(BrokerError::Brk203);
        }
        let profile = self
            .registry
            .profile(&record.request.profile_ref, &record.request.profile_version)?;
        let (endpoint_key, issuers, expected_audience, max_age) = match &profile.scheme {
            AuthScheme::WorkloadTokenExchange {
                exchange_endpoint_key,
                trusted_issuers,
                audience,
                maximum_assertion_age_seconds,
            } => (
                exchange_endpoint_key,
                trusted_issuers,
                audience,
                *maximum_assertion_age_seconds,
            ),
            _ => return Err(BrokerError::Brk004),
        };
        if !issuers.contains(issuer)
            || audience != expected_audience
            || record.workload_nonce.as_deref() != Some(nonce)
            || issued_at > now
            || now.saturating_sub(issued_at) > max_age as i64
        {
            return Err(BrokerError::Brk109);
        }
        let result = assertion.expose(|bytes| {
            service.exchange_workload_assertion(WorkloadExchangeRequest {
                profile,
                endpoint: profile.endpoint(endpoint_key)?,
                assertion: bytes,
                expected_claims: &record.claims,
            })
        })?;
        self.finish(activation_ref, result, now)
    }

    pub fn bind_external(
        &mut self,
        activation_ref: &str,
        custodian: &RegistryPin,
        challenge: &str,
        proof_valid: bool,
        now: i64,
    ) -> Result<NextAction, BrokerError> {
        let record = self
            .records
            .get(activation_ref)
            .ok_or(BrokerError::Brk103)?;
        let profile = self
            .registry
            .profile(&record.request.profile_ref, &record.request.profile_version)?;
        let allowed = match &profile.scheme {
            AuthScheme::ExternalCustodian { allowed_custodians } => allowed_custodians,
            _ => return Err(BrokerError::Brk004),
        };
        if now >= record.expires_at
            || record.challenge.as_deref() != Some(challenge)
            || !proof_valid
            || !allowed.contains(custodian)
        {
            return Err(BrokerError::Brk109);
        }
        self.complete_secret(
            activation_ref,
            PrivateMaterial::new(serde_json::to_vec(custodian).map_err(|_| BrokerError::Brk401)?)?,
            PrivateMaterial::new(custodian.definition_hash.as_bytes().to_vec())?,
            now,
        )
    }

    fn finish(
        &mut self,
        activation_ref: &str,
        material: ActivationMaterial,
        _now: i64,
    ) -> Result<NextAction, BrokerError> {
        let record = self
            .records
            .get_mut(activation_ref)
            .ok_or(BrokerError::Brk103)?;
        if record.status == Status::Active {
            return complete_action(activation_ref, record);
        }
        if record.claims.compare(&material.normalized_claims)
            != crate::profile::ClaimRelation::Equal
        {
            record.status = Status::Failed;
            return Err(BrokerError::Brk109);
        }
        let connection_ref = format!("connection_{}", &hash(activation_ref.as_bytes())[7..39]);
        let principal_commitment = material.principal_subject.expose(|bytes| {
            keyed_hash(
                &self.commitment_key[..],
                b"principal",
                activation_ref.as_bytes(),
                bytes,
            )
        });
        let authority_view_hash = hash(
            format!(
                "{}\0{}\0{}\0{}\0{}",
                record.request.org_id,
                profile_key(record),
                record.claims_commitment,
                principal_commitment,
                record.request.standing_authority_hash
            )
            .as_bytes(),
        );
        drop(material);
        record.connection_ref = Some(connection_ref);
        record.authority_view_hash = Some(authority_view_hash);
        record.status = Status::Active;
        complete_action(activation_ref, record)
    }

    pub fn snapshot(&self, activation_ref: &str) -> Result<PublicActivationSnapshot, BrokerError> {
        let record = self
            .records
            .get(activation_ref)
            .ok_or(BrokerError::Brk103)?;
        let profile = self
            .registry
            .profile(&record.request.profile_ref, &record.request.profile_version)?;
        Ok(PublicActivationSnapshot {
            activation_ref: activation_ref.into(),
            profile_ref: record.request.profile_ref.clone(),
            profile_version: record.request.profile_version.clone(),
            status: match record.status {
                Status::Awaiting => "awaiting_action",
                Status::Claimed => "action_claimed",
                Status::Active => "active",
                Status::RestartRequired => "restart_required",
                Status::Failed => "failed",
            },
            claims_commitment: record.claims_commitment.clone(),
            public_claims: record.claims.safe_projection(&profile.public_claims),
            dynamic_source_refs: record.dynamic_source_refs.clone(),
            policy_refs: record.policy_refs.clone(),
        })
    }
}

fn channel(nonces: &mut dyn NonceSource) -> Result<(String, [u8; 32]), BrokerError> {
    let channel = nonces.nonce("private_channel")?;
    let seed = nonces.nonce("private_channel_key")?;
    Ok((channel, Sha256::digest(seed.as_bytes()).into()))
}
fn complete_action(
    activation_ref: &str,
    record: &ActivationRecord,
) -> Result<NextAction, BrokerError> {
    Ok(NextAction::Complete {
        activation_ref: activation_ref.into(),
        expires_at: record.expires_at,
        connection_ref: record.connection_ref.clone().ok_or(BrokerError::Brk401)?,
        authority_view_hash: record
            .authority_view_hash
            .clone()
            .ok_or(BrokerError::Brk401)?,
    })
}
fn profile_key(record: &ActivationRecord) -> String {
    format!(
        "{}@{}",
        record.request.profile_ref, record.request.profile_version
    )
}
fn claims_commitment(
    key: &[u8],
    activation_ref: &str,
    claims: &NormalizedClaims,
) -> Result<String, BrokerError> {
    let bytes = serde_json::to_vec(claims).map_err(|_| BrokerError::Brk401)?;
    Ok(keyed_hash(
        key,
        b"authorization_claims",
        activation_ref.as_bytes(),
        &bytes,
    ))
}
fn keyed_hash(key: &[u8], purpose: &[u8], context: &[u8], value: &[u8]) -> String {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC key");
    mac.update(purpose);
    mac.update(&[0]);
    mac.update(context);
    mac.update(&[0]);
    mac.update(value);
    format!("hmac-sha256:{}", hex_encode(&mac.finalize().into_bytes()))
}
fn hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex_encode(&Sha256::digest(bytes)))
}
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 15) as usize] as char);
    }
    out
}
fn pct(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char)
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}
