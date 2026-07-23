use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::hpke;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use broker_core::{
    BrokerError, canonical,
    credential::{
        InMemoryReplayState, RemoteAuthorizeDispatchPrivateV2, RemoteDispatchResultPrivateV2,
        ReplayState,
        signing::{RemoteCustodyEnvelopeV2, sign_remote_envelope, verify_signed},
    },
    signing::{BrokerSigner, BrokerVerifyingKey},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServicePin {
    pub entry_ref: String,
    pub version: String,
    pub definition_hash: String,
    pub approval_epoch: u64,
    pub revocation_epoch: u64,
}

pub struct RemoteServiceIdentity {
    pin: ServicePin,
    peer_pin: ServicePin,
    key_id: String,
    peer_key_id: String,
    signer: BrokerSigner,
    peer_verifying_key: BrokerVerifyingKey,
    hpke_private_key: [u8; 32],
    peer_hpke_public_key: [u8; 32],
}
impl RemoteServiceIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pin: ServicePin,
        peer_pin: ServicePin,
        key_id: impl Into<String>,
        peer_key_id: impl Into<String>,
        signer: BrokerSigner,
        peer_verifying_key: BrokerVerifyingKey,
        hpke_private_key: [u8; 32],
        peer_hpke_public_key: [u8; 32],
    ) -> Self {
        Self {
            pin,
            peer_pin,
            key_id: key_id.into(),
            peer_key_id: peer_key_id.into(),
            signer,
            peer_verifying_key,
            hpke_private_key,
            peer_hpke_public_key,
        }
    }

    pub fn pin(&self) -> &ServicePin {
        &self.pin
    }
}

impl std::fmt::Debug for RemoteServiceIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteServiceIdentity")
            .field("pin", &self.pin)
            .field("keys", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct RemoteRequestMetadata {
    pub request_ref: String,
    pub org_id: String,
    pub connection_ref: String,
    pub effect_grant_hash: String,
    pub logical_effect_id: String,
    pub dispatch_attempt: u8,
    pub authority_view_hash: String,
    pub minimum_material_generation: u64,
    pub nonce_jti: String,
    pub issued_at: String,
    pub expires_at: String,
}

pub struct RemotePrivateRequest {
    pub unauthenticated_plan_jcs: Vec<u8>,
    pub contract_hash: String,
    pub auth_profile_pin: ServicePin,
    pub broker_context_jcs: Vec<u8>,
    pub endpoint_set_hash: String,
    pub response_firewall_policy_hash: String,
}
impl std::fmt::Debug for RemotePrivateRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemotePrivateRequest([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashPoint {
    None,
    AfterPrepared,
    AfterProviderCrossing,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DispatchPhase {
    Prepared,
    Dispatched,
    Terminal,
}
#[derive(Clone, Debug)]
struct DispatchRecord {
    request_hash: String,
    phase: DispatchPhase,
    result: Option<Vec<u8>>,
}

#[derive(Default)]
pub struct RemoteDurableState {
    replay: InMemoryReplayState,
    dispatches: Mutex<BTreeMap<String, DispatchRecord>>,
}

pub struct RemoteCustodianMock {
    identity: RemoteServiceIdentity,
    state: Arc<RemoteDurableState>,
}
impl RemoteCustodianMock {
    pub fn new(identity: RemoteServiceIdentity) -> Self {
        Self::with_state(identity, Arc::new(RemoteDurableState::default()))
    }

    pub fn with_state(identity: RemoteServiceIdentity, state: Arc<RemoteDurableState>) -> Self {
        Self { identity, state }
    }

    pub fn seal_request(
        sender: &RemoteServiceIdentity,
        recipient: &ServicePin,
        metadata: &RemoteRequestMetadata,
        private: RemotePrivateRequest,
        ephemeral_private_key: [u8; 32],
    ) -> Result<Vec<u8>, BrokerError> {
        if recipient != &sender.peer_pin {
            return Err(BrokerError::Brk106);
        }
        let plan = canonical::canonicalize_bounded(&private.unauthenticated_plan_jcs, 256 * 1024)?;
        let context = canonical::canonicalize_bounded(&private.broker_context_jcs, 64 * 1024)?;
        let plaintext = serde_json::json!({"private_codec_version":"0.2","critical_fields":[],"extensions":{},"unauthenticated_plan_jcs_b64u":URL_SAFE_NO_PAD.encode(plan.as_bytes()),"unauthenticated_plan_hash":plan.sha256(),"contract_hash":private.contract_hash,"auth_profile_pin":private.auth_profile_pin,"broker_context_jcs_b64u":URL_SAFE_NO_PAD.encode(context.as_bytes()),"endpoint_set_hash":private.endpoint_set_hash,"response_firewall_policy_hash":private.response_firewall_policy_hash,"logical_effect_id":metadata.logical_effect_id,"dispatch_attempt":metadata.dispatch_attempt});
        let _: RemoteAuthorizeDispatchPrivateV2 =
            serde_json::from_value(plaintext.clone()).map_err(|_| BrokerError::Brk004)?;
        seal_envelope(
            sender,
            recipient,
            metadata,
            "request",
            ephemeral_private_key,
            &serde_json::to_vec(&plaintext).map_err(|_| BrokerError::Brk401)?,
        )
    }

    pub fn authorize_and_dispatch(
        &self,
        envelope_bytes: &[u8],
        now: &str,
        crash: CrashPoint,
        dispatch: impl FnOnce(&Value) -> Result<Value, BrokerError>,
    ) -> Result<Vec<u8>, BrokerError> {
        let parsed = broker_core::credential::parse::<RemoteCustodyEnvelopeV2>(envelope_bytes)?;
        verify_signed(&parsed, &self.identity.peer_verifying_key)?;
        let outer = parsed.view.as_value();
        if outer.get("direction").and_then(Value::as_str) != Some("request")
            || outer.get("recipient")
                != Some(&serde_json::to_value(&self.identity.pin).map_err(|_| BrokerError::Brk401)?)
            || outer.get("sender")
                != Some(
                    &serde_json::to_value(&self.identity.peer_pin)
                        .map_err(|_| BrokerError::Brk401)?,
                )
            || text(outer, "issued_at")? > now
            || text(outer, "expires_at")? <= now
        {
            return Err(BrokerError::Brk106);
        }
        let sender = outer
            .pointer("/sender/entry_ref")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk004)?;
        let request_ref = text(outer, "request_ref")?;
        let nonce_jti = text(outer, "nonce_jti")?;
        let request_hash = hash(envelope_bytes);
        let reservation =
            self.state
                .replay
                .reserve(sender, request_ref, nonce_jti, &request_hash)?;
        if let Some(recorded) = reservation.recorded_result() {
            return Ok(recorded.to_vec());
        }
        let mut dispatches = self
            .state
            .dispatches
            .lock()
            .map_err(|_| BrokerError::Brk401)?;
        if let Some(existing) = dispatches.get(request_ref) {
            if existing.request_hash != request_hash {
                return Err(BrokerError::Brk203);
            }
            if let Some(result) = &existing.result {
                return Ok(result.clone());
            }
        } else {
            dispatches.insert(
                request_ref.into(),
                DispatchRecord {
                    request_hash: request_hash.clone(),
                    phase: DispatchPhase::Prepared,
                    result: None,
                },
            );
        }
        if crash == CrashPoint::AfterPrepared {
            return Err(BrokerError::Brk401);
        }
        let plaintext = open_envelope(&self.identity, outer)?;
        let private: Value = serde_json::from_slice(&plaintext).map_err(|_| BrokerError::Brk004)?;
        let _: RemoteAuthorizeDispatchPrivateV2 =
            serde_json::from_value(private.clone()).map_err(|_| BrokerError::Brk004)?;
        if private.get("logical_effect_id") != outer.get("logical_effect_id")
            || private.get("dispatch_attempt") != outer.get("dispatch_attempt")
        {
            return Err(BrokerError::Brk109);
        }
        dispatches.get_mut(request_ref).unwrap().phase = DispatchPhase::Dispatched;
        let (outcome, scrubbed, crossing) = if crash == CrashPoint::AfterProviderCrossing {
            ("ambiguous", serde_json::json!({}), "dispatched")
        } else {
            match dispatch(&private) {
                Ok(value) => ("confirmed", value, "terminal"),
                Err(_) => ("failed", serde_json::json!({}), "terminal"),
            }
        };
        let scrubbed = canonical::from_serde(&scrubbed, 256 * 1024)?;
        let result_private = serde_json::json!({"private_codec_version":"0.2","critical_fields":[],"extensions":{},"outcome":outcome,"leased_material_generation":outer.get("minimum_material_generation").cloned().ok_or(BrokerError::Brk004)?,"provider_crossing_state":crossing,"firewall_evidence_hash":hash(scrubbed.as_bytes()),"scrubbed_response_jcs_b64u":URL_SAFE_NO_PAD.encode(scrubbed.as_bytes()),"private_credential_update_envelope_hash":null,"durable_result_record_hash":hash(format!("{request_ref}:{outcome}").as_bytes())});
        let _: RemoteDispatchResultPrivateV2 =
            serde_json::from_value(result_private.clone()).map_err(|_| BrokerError::Brk004)?;
        let response_meta = metadata_from_outer(outer)?;
        let sender_identity = &self.identity;
        let recipient: ServicePin =
            serde_json::from_value(outer.get("sender").cloned().ok_or(BrokerError::Brk004)?)
                .map_err(|_| BrokerError::Brk004)?;
        let ephemeral_private_key = response_ephemeral_key(&self.identity, request_ref);
        let response = seal_envelope(
            sender_identity,
            &recipient,
            &response_meta,
            "response",
            ephemeral_private_key,
            &serde_json::to_vec(&result_private).map_err(|_| BrokerError::Brk401)?,
        )?;
        let record = dispatches.get_mut(request_ref).unwrap();
        record.phase = DispatchPhase::Terminal;
        record.result = Some(response.clone());
        drop(dispatches);
        self.state
            .replay
            .record_terminal(&reservation, response.clone())?;
        Ok(response)
    }

    pub fn open_response(
        identity: &RemoteServiceIdentity,
        response: &[u8],
        expected: &RemoteRequestMetadata,
        now: &str,
    ) -> Result<Value, BrokerError> {
        let parsed = broker_core::credential::parse::<RemoteCustodyEnvelopeV2>(response)?;
        verify_signed(&parsed, &identity.peer_verifying_key)?;
        let outer = parsed.view.as_value();
        if outer.get("direction").and_then(Value::as_str) != Some("response")
            || outer.get("recipient")
                != Some(&serde_json::to_value(&identity.pin).map_err(|_| BrokerError::Brk401)?)
            || outer.get("sender")
                != Some(&serde_json::to_value(&identity.peer_pin).map_err(|_| BrokerError::Brk401)?)
            || text(outer, "request_ref")? != expected.request_ref
            || text(outer, "org_id")? != expected.org_id
            || text(outer, "connection_ref")? != expected.connection_ref
            || text(outer, "effect_grant_hash")? != expected.effect_grant_hash
            || text(outer, "logical_effect_id")? != expected.logical_effect_id
            || outer.get("dispatch_attempt").and_then(Value::as_u64)
                != Some(expected.dispatch_attempt.into())
            || text(outer, "authority_view_hash")? != expected.authority_view_hash
            || outer
                .get("minimum_material_generation")
                .and_then(Value::as_u64)
                != Some(expected.minimum_material_generation)
            || text(outer, "nonce_jti")? != expected.nonce_jti
            || text(outer, "issued_at")? > now
            || text(outer, "expires_at")? <= now
        {
            return Err(BrokerError::Brk109);
        }
        let plaintext = open_envelope(identity, parsed.view.as_value())?;
        let value: Value = serde_json::from_slice(&plaintext).map_err(|_| BrokerError::Brk004)?;
        let _: RemoteDispatchResultPrivateV2 =
            serde_json::from_value(value.clone()).map_err(|_| BrokerError::Brk004)?;
        Ok(value)
    }
}

fn seal_envelope(
    sender: &RemoteServiceIdentity,
    recipient: &ServicePin,
    metadata: &RemoteRequestMetadata,
    direction: &str,
    ephemeral_private_key: [u8; 32],
    plaintext: &[u8],
) -> Result<Vec<u8>, BrokerError> {
    let encapsulated_key = hpke::public_key(&ephemeral_private_key);
    let mut outer = serde_json::json!({"schema_version":"0.2","critical_fields":[],"extensions":{},"direction":direction,"request_ref":metadata.request_ref,"org_id":metadata.org_id,"connection_ref":metadata.connection_ref,"effect_grant_hash":metadata.effect_grant_hash,"logical_effect_id":metadata.logical_effect_id,"dispatch_attempt":metadata.dispatch_attempt,"authority_view_hash":metadata.authority_view_hash,"minimum_material_generation":metadata.minimum_material_generation,"sender":sender.pin,"recipient":recipient,"recipient_key_id":sender.peer_key_id,"encapsulated_key":URL_SAFE_NO_PAD.encode(encapsulated_key),"nonce_jti":metadata.nonce_jti,"issued_at":metadata.issued_at,"expires_at":metadata.expires_at});
    let aad = canonical::from_serde(&outer, 1024 * 1024)?;
    let (actual_encapsulated, ciphertext) = hpke::seal(
        &sender.peer_hpke_public_key,
        ephemeral_private_key,
        aad.as_bytes(),
        plaintext,
    )?;
    if actual_encapsulated != encapsulated_key {
        return Err(BrokerError::Brk401);
    }
    outer["ciphertext"] = URL_SAFE_NO_PAD.encode(ciphertext).into();
    outer["aad_hash"] = hash(aad.as_bytes()).into();
    outer["signature"] = serde_json::to_value(sign_remote_envelope(&sender.signer, &outer)?)
        .map_err(|_| BrokerError::Brk401)?;
    let parsed = broker_core::credential::parse::<RemoteCustodyEnvelopeV2>(
        &serde_json::to_vec(&outer).map_err(|_| BrokerError::Brk401)?,
    )?;
    Ok(parsed.canonical_bytes().to_vec())
}

fn open_envelope(
    identity: &RemoteServiceIdentity,
    outer: &Value,
) -> Result<Zeroizing<Vec<u8>>, BrokerError> {
    if text(outer, "recipient_key_id")? != identity.key_id {
        return Err(BrokerError::Brk106);
    }
    let mut aad = outer.as_object().cloned().ok_or(BrokerError::Brk004)?;
    let ciphertext = aad
        .remove("ciphertext")
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or(BrokerError::Brk004)?;
    aad.remove("aad_hash");
    aad.remove("signature");
    let aad = canonical::from_serde(&aad, 1024 * 1024)?;
    if text(outer, "aad_hash")? != hash(aad.as_bytes()) {
        return Err(BrokerError::Brk109);
    }
    let ciphertext = decode_exact::<1048576>(&ciphertext)?;
    let encapsulated = decode_exact_array::<32>(text(outer, "encapsulated_key")?)?;
    hpke::open(
        &identity.hpke_private_key,
        &encapsulated,
        aad.as_bytes(),
        &ciphertext,
    )
}

fn decode_exact<const MAX: usize>(value: &str) -> Result<Vec<u8>, BrokerError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| BrokerError::Brk004)?;
    if decoded.len() > MAX || URL_SAFE_NO_PAD.encode(&decoded) != value {
        return Err(BrokerError::Brk004);
    }
    Ok(decoded)
}

fn decode_exact_array<const N: usize>(value: &str) -> Result<[u8; N], BrokerError> {
    decode_exact::<N>(value)?
        .try_into()
        .map_err(|_| BrokerError::Brk004)
}

fn response_ephemeral_key(identity: &RemoteServiceIdentity, request_ref: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(identity.hpke_private_key);
    digest.update(b"\0response\0");
    digest.update(request_ref.as_bytes());
    digest.finalize().into()
}
fn metadata_from_outer(v: &Value) -> Result<RemoteRequestMetadata, BrokerError> {
    Ok(RemoteRequestMetadata {
        request_ref: text(v, "request_ref")?.into(),
        org_id: text(v, "org_id")?.into(),
        connection_ref: text(v, "connection_ref")?.into(),
        effect_grant_hash: text(v, "effect_grant_hash")?.into(),
        logical_effect_id: text(v, "logical_effect_id")?.into(),
        dispatch_attempt: v
            .get("dispatch_attempt")
            .and_then(Value::as_u64)
            .ok_or(BrokerError::Brk004)? as u8,
        authority_view_hash: text(v, "authority_view_hash")?.into(),
        minimum_material_generation: v
            .get("minimum_material_generation")
            .and_then(Value::as_u64)
            .ok_or(BrokerError::Brk004)?,
        nonce_jti: text(v, "nonce_jti")?.into(),
        issued_at: text(v, "issued_at")?.into(),
        expires_at: text(v, "expires_at")?.into(),
    })
}
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str, BrokerError> {
    v.get(k).and_then(Value::as_str).ok_or(BrokerError::Brk004)
}
fn hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex(&Sha256::digest(bytes)))
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
