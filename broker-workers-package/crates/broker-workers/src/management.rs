use crate::protocol::{
    ConnectionIntentRequest, OAUTH_STATE_TTL_SECONDS, SESSION_TTL_SECONDS, SessionExchangeRequest,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, VerifyingKey};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt};
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

type HmacSha256 = Hmac<Sha256>;

pub const CONNECTOR_REF: &str = "connector.google.workspace@1";
pub const AUTH_PROFILE_REF: &str = "auth.google.workspace.oauth2@1";
pub const EXECUTION_LANE: &str = "semantic_broker";
pub const CUSTODY: &str = "hosted_broker";

pub const GOOGLE_SCOPES: [&str; 2] = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/spreadsheets",
];

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ManagementError {
    #[error("management request rejected")]
    Rejected,
    #[error("management state unavailable")]
    Unavailable,
}

#[derive(Clone)]
pub struct DeploymentKey(String);

impl DeploymentKey {
    pub fn parse(value: impl Into<String>) -> Result<Self, ManagementError> {
        let value = value.into();
        if !value.starts_with("lbk_")
            || value.len() < 52
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(ManagementError::Rejected);
        }
        Ok(Self(value))
    }

    fn expose(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for DeploymentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DeploymentKey([REDACTED])")
    }
}

impl Drop for DeploymentKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeploymentKeyRecord {
    pub org_id: String,
    pub deployment_id: String,
    pub key_hash: [u8; 32],
    pub expires_at: i64,
    pub revoked: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_ref: String,
    pub org_id: String,
    pub deployment_id: String,
    pub pop_key_thumbprint: String,
    pub pop_public_key: [u8; 32],
    pub expires_at: i64,
    pub revoked: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IntentRecord {
    pub intent_ref: String,
    pub org_id: String,
    pub connector_ref: String,
    pub auth_profile_ref: String,
    pub execution_lane: String,
    pub custody: String,
    pub oauth_state_hash: [u8; 32],
    pub expires_at: i64,
    pub consumed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OAuthResolution {
    pub intent_ref: String,
    pub org_id: String,
    pub connector_ref: String,
    pub auth_profile_ref: String,
    pub scopes: Vec<String>,
    pub token_service_binding: String,
}

pub trait IdSource {
    fn opaque(&mut self, prefix: &str) -> Result<String, ManagementError>;
}

pub fn keyed_hash(pepper: &[u8], purpose: &[u8], value: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(pepper).expect("HMAC accepts arbitrary key lengths");
    mac.update(purpose);
    mac.update(&[0]);
    mac.update(value);
    mac.finalize().into_bytes().into()
}

pub fn deployment_key_hash(pepper: &[u8], key: &DeploymentKey) -> [u8; 32] {
    keyed_hash(pepper, b"deployment-key", key.expose())
}

pub fn constant_time_matches(expected: &[u8; 32], actual: &[u8; 32]) -> bool {
    bool::from(expected.ct_eq(actual))
}

#[derive(Debug, Default)]
pub struct ManagementState {
    deployment_keys: Vec<DeploymentKeyRecord>,
    sessions: BTreeMap<String, SessionRecord>,
    intents: BTreeMap<String, IntentRecord>,
}

impl ManagementState {
    pub fn install_deployment_key(
        &mut self,
        pepper: &[u8],
        org_id: impl Into<String>,
        deployment_id: impl Into<String>,
        key: DeploymentKey,
        expires_at: i64,
    ) -> Result<(), ManagementError> {
        let org_id = org_id.into();
        let deployment_id = deployment_id.into();
        if org_id.is_empty() || deployment_id.is_empty() || expires_at <= 0 {
            return Err(ManagementError::Rejected);
        }
        self.deployment_keys.push(DeploymentKeyRecord {
            org_id,
            deployment_id,
            key_hash: keyed_hash(pepper, b"deployment-key", key.expose()),
            expires_at,
            revoked: false,
        });
        Ok(())
    }

    pub fn exchange_session(
        &mut self,
        pepper: &[u8],
        request: SessionExchangeRequest,
        now: i64,
        ids: &mut dyn IdSource,
    ) -> Result<SessionRecord, ManagementError> {
        let key = DeploymentKey::parse(request.deployment_key.clone())?;
        let candidate = keyed_hash(pepper, b"deployment-key", key.expose());
        let record = self
            .deployment_keys
            .iter()
            .find(|record| constant_time_matches(&record.key_hash, &candidate))
            .filter(|record| !record.revoked && now < record.expires_at)
            .ok_or(ManagementError::Rejected)?;
        let deployment_key_id = format!("sha256:{}", hex::encode(candidate));
        let public_key = verify_exchange_request(&request, &deployment_key_id, now)?;
        let session = SessionRecord {
            session_ref: ids.opaque("session_")?,
            org_id: record.org_id.clone(),
            deployment_id: record.deployment_id.clone(),
            pop_key_thumbprint: public_key_thumbprint(&public_key),
            pop_public_key: public_key,
            expires_at: now
                .checked_add(SESSION_TTL_SECONDS)
                .ok_or(ManagementError::Unavailable)?,
            revoked: false,
        };
        self.sessions
            .insert(session.session_ref.clone(), session.clone());
        Ok(session)
    }

    pub fn session(&self, session_ref: &str, now: i64) -> Result<&SessionRecord, ManagementError> {
        let session = self
            .sessions
            .get(session_ref)
            .filter(|session| !session.revoked && now < session.expires_at)
            .ok_or(ManagementError::Rejected)?;
        Ok(session)
    }

    pub fn create_intent(
        &mut self,
        request: ConnectionIntentRequest,
        org_id: &str,
        now: i64,
        ids: &mut dyn IdSource,
    ) -> Result<(IntentRecord, String), ManagementError> {
        validate_intent(&request)?;
        let state = ids.opaque("oauth_state_")?;
        let intent = IntentRecord {
            intent_ref: ids.opaque("intent_")?,
            org_id: org_id.to_string(),
            connector_ref: request.connector_ref,
            auth_profile_ref: request.auth_profile_ref,
            execution_lane: request.execution_lane,
            custody: request.custody,
            oauth_state_hash: Sha256::digest(state.as_bytes()).into(),
            expires_at: now
                .checked_add(OAUTH_STATE_TTL_SECONDS)
                .ok_or(ManagementError::Unavailable)?,
            consumed: false,
        };
        self.intents
            .insert(intent.intent_ref.clone(), intent.clone());
        Ok((intent, state))
    }

    pub fn consume_oauth_state(
        &mut self,
        state: &str,
        now: i64,
    ) -> Result<OAuthResolution, ManagementError> {
        let candidate: [u8; 32] = Sha256::digest(state.as_bytes()).into();
        let intent = self
            .intents
            .values_mut()
            .find(|intent| constant_time_matches(&intent.oauth_state_hash, &candidate))
            .filter(|intent| !intent.consumed && now < intent.expires_at)
            .ok_or(ManagementError::Rejected)?;
        intent.consumed = true;
        Ok(OAuthResolution {
            intent_ref: intent.intent_ref.clone(),
            org_id: intent.org_id.clone(),
            connector_ref: intent.connector_ref.clone(),
            auth_profile_ref: intent.auth_profile_ref.clone(),
            scopes: GOOGLE_SCOPES.iter().map(|value| (*value).into()).collect(),
            token_service_binding: "GOOGLE_TOKEN_SERVICE".into(),
        })
    }
}

pub fn validate_intent(request: &ConnectionIntentRequest) -> Result<(), ManagementError> {
    if request.connector_ref != CONNECTOR_REF
        || request.auth_profile_ref != AUTH_PROFILE_REF
        || request.execution_lane != EXECUTION_LANE
        || request.custody != CUSTODY
    {
        return Err(ManagementError::Rejected);
    }
    Ok(())
}

const EXCHANGE_DOMAIN: &[u8] = b"lattice.session-exchange.ed25519.v1";
const REQUEST_DOMAIN: &[u8] = b"lattice.authenticated-request.ed25519.v1";

fn framed(domain: &[u8], fields: &[&[u8]]) -> Result<Vec<u8>, ManagementError> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(domain);
    out.push(0);
    for field in fields {
        let len = u32::try_from(field.len()).map_err(|_| ManagementError::Rejected)?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(field);
    }
    Ok(out)
}

pub fn exchange_transcript(
    deployment_key_id: &str,
    public_key: &str,
    nonce: &str,
    timestamp: i64,
    audience: &str,
) -> Result<Vec<u8>, ManagementError> {
    let timestamp = timestamp.to_string();
    framed(
        EXCHANGE_DOMAIN,
        &[
            deployment_key_id.as_bytes(),
            public_key.as_bytes(),
            nonce.as_bytes(),
            timestamp.as_bytes(),
            audience.as_bytes(),
        ],
    )
}

#[allow(clippy::too_many_arguments)]
pub fn request_transcript(
    session_ref: &str,
    audience: &str,
    method: &str,
    normalized_path_query: &str,
    body_hash: &[u8; 32],
    timestamp: i64,
    jti: &str,
) -> Result<Vec<u8>, ManagementError> {
    let body_hash = format!("sha256:{}", hex::encode(body_hash));
    let timestamp = timestamp.to_string();
    framed(
        REQUEST_DOMAIN,
        &[
            session_ref.as_bytes(),
            audience.as_bytes(),
            method.as_bytes(),
            normalized_path_query.as_bytes(),
            body_hash.as_bytes(),
            timestamp.as_bytes(),
            jti.as_bytes(),
        ],
    )
}

pub fn decode_public_key(value: &str) -> Result<[u8; 32], ManagementError> {
    if value.len() > 64 || value.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(ManagementError::Rejected);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| ManagementError::Rejected)?;
    if URL_SAFE_NO_PAD.encode(&bytes) != value {
        return Err(ManagementError::Rejected);
    }
    <[u8; 32]>::try_from(bytes).map_err(|_| ManagementError::Rejected)
}

pub fn public_key_thumbprint(key: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(key)))
}

pub fn verify_ed25519(
    key: &[u8; 32],
    transcript: &[u8],
    signature: &str,
) -> Result<(), ManagementError> {
    if signature.len() > 128 || signature.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(ManagementError::Rejected);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| ManagementError::Rejected)?;
    if URL_SAFE_NO_PAD.encode(&bytes) != signature {
        return Err(ManagementError::Rejected);
    }
    let signature = Signature::from_slice(&bytes).map_err(|_| ManagementError::Rejected)?;
    VerifyingKey::from_bytes(key)
        .map_err(|_| ManagementError::Rejected)?
        .verify_strict(transcript, &signature)
        .map_err(|_| ManagementError::Rejected)
}

pub fn fresh_timestamp(timestamp: i64, now: i64) -> bool {
    timestamp
        .checked_sub(now)
        .is_some_and(|delta| delta.unsigned_abs() <= crate::protocol::POP_CLOCK_SKEW_SECONDS as u64)
}

pub fn verify_exchange_request(
    request: &SessionExchangeRequest,
    deployment_key_id: &str,
    now: i64,
) -> Result<[u8; 32], ManagementError> {
    if request.audience != crate::protocol::SESSION_EXCHANGE_AUDIENCE
        || !fresh_timestamp(request.timestamp, now)
        || !(16..=128).contains(&request.client_nonce.len())
        || !request.client_nonce.is_ascii()
        || request
            .client_nonce
            .bytes()
            .any(|byte| byte.is_ascii_whitespace())
    {
        return Err(ManagementError::Rejected);
    }
    let key = decode_public_key(&request.client_public_key)?;
    let transcript = exchange_transcript(
        deployment_key_id,
        &request.client_public_key,
        &request.client_nonce,
        request.timestamp,
        &request.audience,
    )?;
    verify_ed25519(&key, &transcript, &request.signature)?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    struct Sequence(u64);
    impl IdSource for Sequence {
        fn opaque(&mut self, prefix: &str) -> Result<String, ManagementError> {
            self.0 += 1;
            Ok(format!("{prefix}{:032x}", self.0))
        }
    }

    fn key() -> String {
        format!("lbk_{}", "a".repeat(64))
    }

    fn session_request(pepper: &[u8], now: i64) -> SessionExchangeRequest {
        let signing = SigningKey::from_bytes(&[7; 32]);
        let public = URL_SAFE_NO_PAD.encode(signing.verifying_key().to_bytes());
        let candidate = keyed_hash(pepper, b"deployment-key", key().as_bytes());
        let key_id = format!("sha256:{}", hex::encode(candidate));
        let nonce = "exchange-nonce-0000000000000001".to_string();
        let audience = crate::protocol::SESSION_EXCHANGE_AUDIENCE.to_string();
        let transcript = exchange_transcript(&key_id, &public, &nonce, now, &audience).unwrap();
        SessionExchangeRequest {
            deployment_key: key(),
            client_public_key: public,
            client_nonce: nonce,
            timestamp: now,
            audience,
            signature: URL_SAFE_NO_PAD.encode(signing.sign(&transcript).to_bytes()),
        }
    }

    #[test]
    fn deployment_key_is_hashed_and_sessions_are_short_lived_and_pop_bound() {
        let mut state = ManagementState::default();
        let pepper = b"test-pepper";
        state
            .install_deployment_key(
                pepper,
                "org-1",
                "deployment-1",
                DeploymentKey::parse(key()).unwrap(),
                10_000,
            )
            .unwrap();
        let mut ids = Sequence(0);
        let session = state
            .exchange_session(pepper, session_request(pepper, 100), 100, &mut ids)
            .unwrap();
        assert_eq!(session.expires_at, 100 + SESSION_TTL_SECONDS);
        assert!(state.session(&session.session_ref, 101).is_ok());
        assert_eq!(
            state
                .session(&session.session_ref, session.expires_at)
                .unwrap_err(),
            ManagementError::Rejected
        );
        let public = format!("{state:?} {session:?}");
        assert!(!public.contains(&key()));
    }

    #[test]
    fn intents_fail_closed_and_oauth_state_is_single_use_and_expiring() {
        let mut state = ManagementState::default();
        let mut ids = Sequence(0);
        let request = ConnectionIntentRequest {
            connector_ref: CONNECTOR_REF.into(),
            auth_profile_ref: AUTH_PROFILE_REF.into(),
            execution_lane: EXECUTION_LANE.into(),
            custody: CUSTODY.into(),
        };
        let (_, oauth_state) = state
            .create_intent(request, "org-1", 100, &mut ids)
            .unwrap();
        let resolved = state.consume_oauth_state(&oauth_state, 101).unwrap();
        assert_eq!(resolved.org_id, "org-1");
        assert_eq!(resolved.scopes, GOOGLE_SCOPES);
        assert_eq!(
            state.consume_oauth_state(&oauth_state, 102).unwrap_err(),
            ManagementError::Rejected
        );

        let unknown = ConnectionIntentRequest {
            connector_ref: CONNECTOR_REF.into(),
            auth_profile_ref: "unknown".into(),
            execution_lane: EXECUTION_LANE.into(),
            custody: CUSTODY.into(),
        };
        assert_eq!(
            state
                .create_intent(unknown, "org-1", 100, &mut ids)
                .unwrap_err(),
            ManagementError::Rejected
        );
    }

    #[test]
    fn base64url_keys_and_signatures_require_canonical_spelling_and_strict_ed25519() {
        let signing = SigningKey::from_bytes(&[9; 32]);
        let key = URL_SAFE_NO_PAD.encode(signing.verifying_key().to_bytes());
        assert_eq!(
            decode_public_key(&key).unwrap(),
            signing.verifying_key().to_bytes()
        );
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut noncanonical_key = key.clone();
        let key_tail = alphabet
            .iter()
            .position(|value| *value == key.as_bytes()[42])
            .unwrap();
        assert_eq!(key_tail % 4, 0);
        noncanonical_key.replace_range(
            42..43,
            std::str::from_utf8(&alphabet[key_tail + 1..key_tail + 2]).unwrap(),
        );
        assert_eq!(
            decode_public_key(&noncanonical_key),
            Err(ManagementError::Rejected)
        );

        let transcript = b"strict-ed25519-transcript";
        let signature = URL_SAFE_NO_PAD.encode(signing.sign(transcript).to_bytes());
        verify_ed25519(&signing.verifying_key().to_bytes(), transcript, &signature).unwrap();
        let last = signature.len() - 1;
        let signature_tail = alphabet
            .iter()
            .position(|value| *value == signature.as_bytes()[last])
            .unwrap();
        assert_eq!(signature_tail % 16, 0);
        let mut noncanonical_signature = signature;
        noncanonical_signature.replace_range(
            last..,
            std::str::from_utf8(&alphabet[signature_tail + 1..signature_tail + 2]).unwrap(),
        );
        assert_eq!(
            verify_ed25519(
                &signing.verifying_key().to_bytes(),
                transcript,
                &noncanonical_signature
            ),
            Err(ManagementError::Rejected)
        );
    }

    #[test]
    fn serde_rejects_caller_supplied_provider_configuration() {
        let body = serde_json::json!({
            "connector_ref": CONNECTOR_REF,
            "auth_profile_ref": AUTH_PROFILE_REF,
            "execution_lane": EXECUTION_LANE,
            "custody": CUSTODY,
            "token_url": "https://evil.example/token",
            "scopes": ["admin"]
        });
        assert!(serde_json::from_value::<ConnectionIntentRequest>(body).is_err());
    }
}
