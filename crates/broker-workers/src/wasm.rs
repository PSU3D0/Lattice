use crate::{
    credential_state::{CredentialStateV2, PublicRecordSchema},
    durable::{
        LedgerAuthority, LedgerCommand, LedgerPersistence, LedgerReply, apply_persisted_command,
    },
    management::{
        DeploymentKey, constant_time_matches, deployment_key_hash, fresh_timestamp, keyed_hash,
        public_key_thumbprint, request_transcript, validate_intent, verify_ed25519,
        verify_exchange_request,
    },
    protocol::{
        BROKER_REQUEST_AUDIENCE, ConnectionIntentRequest, ConnectionIntentResponse,
        MAX_INVOKE_BODY, MAX_MANAGEMENT_BODY, NextAction, PublicError, SESSION_TTL_SECONDS,
        SessionExchangeRequest, SessionExchangeResponse,
    },
    refresh::{
        AcquireResult, CompleteResult, ConnectionRegistration, ConnectionTokenState, RefreshResult,
    },
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use broker_core::{
    BrokerError,
    ledger::{LedgerSnapshot, LedgerTail},
};
use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use js_sys::JsString;
use provider_google::{
    CONNECTOR_REF, CUSTODY_LOCATION as CUSTODY, EXECUTION_LANE, GMAIL_SCOPE,
    LEGACY_PROFILE_REF as AUTH_PROFILE_REF, SHEETS_SCOPE,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use wasm_bindgen::{JsValue, closure::Closure, prelude::wasm_bindgen};
#[cfg(feature = "test-fixtures")]
use worker::D1Database;
use worker::{Context, Env, Method, Request, RequestInit, Response, State, durable_object, event};

#[cfg(feature = "test-fixtures")]
mod test_fixtures;
#[cfg(feature = "test-fixtures")]
mod v2_test;

const LEDGER_SNAPSHOT_KEY: &str = "broker:ledger:snapshot:v1";
const LEDGER_TAIL_KEY: &str = "broker:ledger:tail:v1";
const REFRESH_STORAGE_KEY: &str = "broker:refresh:v1";
const CREDENTIAL_STATE_V2_KEY: &str = "broker:credential-state:v2";
const JSON_CONTENT_TYPE: &str = "application/json";
const APPROVED_SCOPES: [&str; 2] = [GMAIL_SCOPE, SHEETS_SCOPE];

#[derive(Deserialize, Serialize)]
struct LedgerEnvelope {
    authority: LedgerAuthority,
    command: LedgerCommand,
}

#[derive(Serialize)]
struct DoErrorReply {
    error: &'static str,
}

#[durable_object]
pub struct BrokerLedgerDurableObject {
    state: State,
}

impl worker::DurableObject for BrokerLedgerDurableObject {
    fn new(state: State, _env: Env) -> Self {
        Self { state }
    }

    async fn fetch(&self, mut request: Request) -> worker::Result<Response> {
        let envelope: LedgerEnvelope = match bounded_json(&mut request, MAX_INVOKE_BODY).await {
            Ok(value) => value,
            Err(_) => return do_error(BrokerError::Brk001),
        };
        let snapshot = self
            .state
            .storage()
            .get::<LedgerSnapshot>(LEDGER_SNAPSHOT_KEY)
            .await?;
        let tail = self
            .state
            .storage()
            .get::<LedgerTail>(LEDGER_TAIL_KEY)
            .await?
            .unwrap_or(LedgerTail {
                start_sequence: 0,
                records: Vec::new(),
            });
        let persistence = LedgerPersistence { snapshot, tail };
        match apply_persisted_command(&envelope.authority, &persistence, envelope.command) {
            Ok(result) => {
                if result.compacted {
                    // worker 0.8.1 documents put_multiple as one implicit,
                    // isolated storage transaction. Never expose a snapshot
                    // without the command-bearing tail from the same state.
                    #[derive(Serialize)]
                    struct AtomicLedgerWrite {
                        #[serde(rename = "broker:ledger:snapshot:v1")]
                        snapshot: LedgerSnapshot,
                        #[serde(rename = "broker:ledger:tail:v1")]
                        tail: LedgerTail,
                    }
                    self.state
                        .storage()
                        .put_multiple(AtomicLedgerWrite {
                            snapshot: result
                                .persistence
                                .snapshot
                                .clone()
                                .ok_or_else(|| worker_rust_error("ledger snapshot"))?,
                            tail: result.persistence.tail.clone(),
                        })
                        .await?;
                } else if result.persistence.tail != persistence.tail {
                    self.state
                        .storage()
                        .put(LEDGER_TAIL_KEY, result.persistence.tail.clone())
                        .await?;
                }
                Response::from_json(&result.reply)
            }
            Err(error) => do_error(error),
        }
    }
}

fn do_error(error: BrokerError) -> worker::Result<Response> {
    Response::from_json(&DoErrorReply {
        error: error.code(),
    })
    .map(|response| response.with_status(409))
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RefreshCommand {
    Register {
        registration: ConnectionRegistration,
    },
    Acquire {
        now: i64,
        lease_id: String,
    },
    Complete {
        lease_id: String,
        expected_epoch: u64,
        now: i64,
        result: RefreshResult,
    },
    Revoke,
    Metadata,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RefreshReply {
    Registered,
    Acquired {
        result: AcquireResult,
    },
    Completed {
        result: CompleteResult,
    },
    Revoked {
        revocation_epoch: u64,
    },
    Metadata {
        org_id: String,
        connection_ref: String,
        account_commitment: String,
        effective_scopes: Vec<String>,
        revocation_epoch: u64,
        revoked: bool,
    },
    Error {
        code: String,
    },
}

#[derive(Serialize, Deserialize)]
struct SealedRefreshState {
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

#[durable_object]
pub struct ConnectionRefreshDurableObject {
    state: State,
    env: Env,
}

impl worker::DurableObject for ConnectionRefreshDurableObject {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut request: Request) -> worker::Result<Response> {
        let command: RefreshCommand = match bounded_json(&mut request, MAX_MANAGEMENT_BODY).await {
            Ok(value) => value,
            Err(_) => return refresh_error("BRK001"),
        };
        let stored = match self
            .state
            .storage()
            .get::<SealedRefreshState>(REFRESH_STORAGE_KEY)
            .await?
        {
            Some(sealed) => Some(open_refresh_state(&self.env, sealed)?),
            None => None,
        };
        let (reply, next) = match command {
            RefreshCommand::Register { registration } if stored.is_none() => {
                match ConnectionTokenState::register(registration) {
                    Ok(state) => (RefreshReply::Registered, Some(state)),
                    Err(_) => return refresh_error("BRK401"),
                }
            }
            RefreshCommand::Register { registration } => {
                let state = stored.ok_or_else(|| worker_rust_error("refresh"))?;
                if !state.matches_registration(&registration) {
                    return refresh_error("BRK109");
                }
                (RefreshReply::Registered, Some(state))
            }
            RefreshCommand::Acquire { now, lease_id } => {
                let mut state = match stored {
                    Some(state) => state,
                    None => return refresh_error("BRK401"),
                };
                match state.acquire(now, lease_id) {
                    Ok(result) => (RefreshReply::Acquired { result }, Some(state)),
                    Err(crate::refresh::RefreshError::Revoked) => {
                        return refresh_error("BRK106");
                    }
                    Err(_) => return refresh_error("BRK401"),
                }
            }
            RefreshCommand::Complete {
                lease_id,
                expected_epoch,
                now,
                result,
            } => {
                let mut state = match stored {
                    Some(state) => state,
                    None => return refresh_error("BRK401"),
                };
                match state.complete_refresh(&lease_id, expected_epoch, now, result) {
                    Ok(result) => (RefreshReply::Completed { result }, Some(state)),
                    Err(crate::refresh::RefreshError::Revoked) => {
                        return refresh_error("BRK106");
                    }
                    Err(crate::refresh::RefreshError::Conflict) => {
                        return refresh_error("BRK204");
                    }
                    Err(_) => return refresh_error("BRK401"),
                }
            }
            RefreshCommand::Revoke => match stored {
                Some(mut state) => {
                    let epoch = state.revoke().map_err(|_| worker_rust_error("refresh"))?;
                    (
                        RefreshReply::Revoked {
                            revocation_epoch: epoch,
                        },
                        Some(state),
                    )
                }
                // Idempotent compensation of a route that was reserved but
                // never registered is already clean.
                None => (
                    RefreshReply::Revoked {
                        revocation_epoch: 0,
                    },
                    None,
                ),
            },
            RefreshCommand::Metadata => {
                let state = match stored {
                    Some(state) => state,
                    None => return refresh_error("BRK401"),
                };
                let reply = RefreshReply::Metadata {
                    org_id: state.org_id.clone(),
                    connection_ref: state.connection_ref.clone(),
                    account_commitment: state.account_commitment.clone(),
                    effective_scopes: state.effective_scopes.iter().cloned().collect(),
                    revocation_epoch: state.revocation_epoch,
                    revoked: state.revoked,
                };
                (reply, Some(state))
            }
        };
        if let Some(next) = next {
            self.state
                .storage()
                .put(REFRESH_STORAGE_KEY, seal_refresh_state(&self.env, &next)?)
                .await?;
        }
        Response::from_json(&reply)
    }
}

fn refresh_error(code: &'static str) -> worker::Result<Response> {
    Response::from_json(&RefreshReply::Error { code: code.into() })
        .map(|response| response.with_status(409))
}

fn seal_refresh_state(
    env: &Env,
    state: &ConnectionTokenState,
) -> worker::Result<SealedRefreshState> {
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    let mut nonce = [0u8; 12];
    getrandom::getrandom(&mut nonce).map_err(|_| worker_rust_error("custody"))?;
    let plaintext = serde_json::to_vec(state).map_err(|_| worker_rust_error("custody"))?;
    let cipher = ChaCha20Poly1305::new((&key).into());
    let ciphertext = cipher
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: &plaintext,
                aad: b"lattice.google.connection-state.v1",
            },
        )
        .map_err(|_| worker_rust_error("custody"))?;
    Ok(SealedRefreshState { nonce, ciphertext })
}

fn open_refresh_state(
    env: &Env,
    sealed: SealedRefreshState,
) -> worker::Result<ConnectionTokenState> {
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    let cipher = ChaCha20Poly1305::new((&key).into());
    let plaintext = cipher
        .decrypt(
            (&sealed.nonce).into(),
            Payload {
                msg: &sealed.ciphertext,
                aad: b"lattice.google.connection-state.v1",
            },
        )
        .map_err(|_| worker_rust_error("custody"))?;
    serde_json::from_slice(&plaintext).map_err(|_| worker_rust_error("custody"))
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum CredentialStateCommand {
    Initialize {
        fence_json: Vec<u8>,
    },
    PutPublic {
        record_ref: String,
        schema: PublicRecordSchema,
        canonical_json: Vec<u8>,
    },
    SealMaterial {
        generation: u64,
        sealed_envelope: Vec<u8>,
    },
    PutRotationJournal {
        canonical_json: Vec<u8>,
    },
    AdvanceFence {
        canonical_json: Vec<u8>,
    },
    #[cfg(feature = "test-fixtures")]
    ActivateV2ForTest {
        canonical_json: Vec<u8>,
    },
    #[cfg(feature = "test-fixtures")]
    SealRotatedMaterialForTest {
        generation: u64,
        sealed_envelope: Vec<u8>,
    },
    #[cfg(feature = "test-fixtures")]
    ReadMaterialForTest {
        generation: u64,
    },
    Read,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CredentialStateEnvelope {
    org_id: String,
    connection_ref: String,
    command: CredentialStateCommand,
}

#[derive(Deserialize, Serialize)]
struct CredentialStateReply {
    initialized: bool,
    public_record_count: usize,
    material_generations: Vec<(u64, String)>,
    fence_json: Vec<u8>,
    rotation_journal_json: Option<Vec<u8>>,
    #[cfg(feature = "test-fixtures")]
    sealed_material_for_internal_test: Option<Vec<u8>>,
}

#[durable_object]
pub struct CredentialStateDurableObject {
    state: State,
}

impl worker::DurableObject for CredentialStateDurableObject {
    fn new(state: State, _env: Env) -> Self {
        Self { state }
    }

    async fn fetch(&self, mut request: Request) -> worker::Result<Response> {
        let envelope: CredentialStateEnvelope =
            match bounded_json(&mut request, MAX_MANAGEMENT_BODY).await {
                Ok(value) => value,
                Err(_) => return do_error(BrokerError::Brk001),
            };
        let existing = self
            .state
            .storage()
            .get::<CredentialStateV2>(CREDENTIAL_STATE_V2_KEY)
            .await?;
        let mut state = match (existing, &envelope.command) {
            (None, CredentialStateCommand::Initialize { fence_json }) => {
                CredentialStateV2::initialize(
                    envelope.org_id.clone(),
                    envelope.connection_ref.clone(),
                    fence_json,
                )
            }
            (Some(state), CredentialStateCommand::Initialize { fence_json }) => {
                match CredentialStateV2::initialize(
                    envelope.org_id.clone(),
                    envelope.connection_ref.clone(),
                    fence_json,
                ) {
                    Ok(candidate)
                        if state.org_id == candidate.org_id
                            && state.connection_ref == candidate.connection_ref
                            && state.fence_json == candidate.fence_json =>
                    {
                        Ok(state)
                    }
                    Ok(_) => Err(BrokerError::Brk203),
                    Err(error) => Err(error),
                }
            }
            (Some(state), _) => Ok(state),
            (None, _) => Err(BrokerError::Brk103),
        };
        let state = match state.as_mut() {
            Ok(state) => state,
            Err(error) => return do_error(*error),
        };
        if state.org_id != envelope.org_id || state.connection_ref != envelope.connection_ref {
            return do_error(BrokerError::Brk107);
        }
        let mut changed = matches!(envelope.command, CredentialStateCommand::Initialize { .. });
        #[cfg(feature = "test-fixtures")]
        let mut sealed_material_for_internal_test = None;
        let result = match envelope.command {
            CredentialStateCommand::Initialize { .. } | CredentialStateCommand::Read => Ok(()),
            CredentialStateCommand::PutPublic {
                record_ref,
                schema,
                canonical_json,
            } => state
                .put_public(&envelope.org_id, &record_ref, schema, &canonical_json)
                .map(|_| {
                    changed = true;
                }),
            CredentialStateCommand::SealMaterial {
                generation,
                sealed_envelope,
            } => state
                .seal_material(&envelope.org_id, generation, sealed_envelope)
                .map(|_| {
                    changed = true;
                }),
            CredentialStateCommand::PutRotationJournal { canonical_json } => state
                .put_rotation_journal(&envelope.org_id, &canonical_json)
                .map(|_| {
                    changed = true;
                }),
            CredentialStateCommand::AdvanceFence { canonical_json } => state
                .advance_fence(&envelope.org_id, &canonical_json)
                .map(|_| {
                    changed = true;
                }),
            #[cfg(feature = "test-fixtures")]
            CredentialStateCommand::ActivateV2ForTest { canonical_json } => state
                .activate_v2_for_test(&envelope.org_id, &canonical_json)
                .map(|_| {
                    changed = true;
                }),
            #[cfg(feature = "test-fixtures")]
            CredentialStateCommand::SealRotatedMaterialForTest {
                generation,
                sealed_envelope,
            } => state
                .seal_rotated_material_for_test(&envelope.org_id, generation, sealed_envelope)
                .map(|_| {
                    changed = true;
                }),
            #[cfg(feature = "test-fixtures")]
            CredentialStateCommand::ReadMaterialForTest { generation } => state
                .material_for_internal_test(&envelope.org_id, generation)
                .map(|material| {
                    sealed_material_for_internal_test = Some(material);
                }),
        };
        if let Err(error) = result {
            return do_error(error);
        }
        if changed {
            self.state
                .storage()
                .put(CREDENTIAL_STATE_V2_KEY, &*state)
                .await?;
        }
        Response::from_json(&CredentialStateReply {
            initialized: true,
            public_record_count: state.public_records.len(),
            material_generations: state
                .sealed_material
                .iter()
                .map(|(generation, material)| (*generation, material.envelope_hash.clone()))
                .collect(),
            fence_json: state.fence_json.clone(),
            rotation_journal_json: state.rotation_journal_json.clone(),
            #[cfg(feature = "test-fixtures")]
            sealed_material_for_internal_test,
        })
    }
}

#[event(fetch)]
async fn fetch(mut request: Request, env: Env, _context: Context) -> worker::Result<Response> {
    let method = request.method();
    let path = request.path();
    if matches!(method, Method::Get | Method::Delete)
        && reject_nonempty_body(&mut request).await.is_err()
    {
        return json(&PublicError::invalid(), 400);
    }
    match (method, path.as_str()) {
        (Method::Get, "/health") => json(&serde_json::json!({"status":"ok"}), 200),
        (Method::Get, "/ready") => readiness(&env).await,
        (Method::Post, "/v1/sessions") => create_session(&mut request, &env).await,
        (Method::Post, "/v1/connection-intents") => {
            create_connection_intent(&mut request, &env).await
        }
        (Method::Get, "/v1/oauth/callback") => oauth_callback(&request, &env).await,
        (Method::Post, "/v1/bindings") => install_binding(&mut request, &env).await,
        (Method::Post, "/internal/v1/bootstrap") => bootstrap(&mut request, &env).await,
        (Method::Post, "/internal/v1/grants") => issue_grant(&mut request, &env).await,
        (Method::Post, "/internal/v1/invoke") => invoke(&mut request, &env).await,
        #[cfg(feature = "test-fixtures")]
        (Method::Post, concat!("/__", "test/fixtures")) => {
            test_fixtures::provision_fixture(&mut request, &env).await
        }
        #[cfg(feature = "test-fixtures")]
        (Method::Post, concat!("/__", "test/credential-state-v2")) => {
            test_fixtures::credential_state_v2(&mut request, &env).await
        }
        #[cfg(feature = "test-fixtures")]
        (Method::Post, concat!("/__", "test/v2-activation")) => {
            v2_test::route(&mut request, &env).await
        }
        _ if path.starts_with("/v1/connections/") => connection_route(&request, &env).await,
        _ if path.starts_with("/v1/receipts/") => receipt_route(&request, &env).await,
        _ => json(&PublicError::invalid(), 404),
    }
}

fn header_secret_matches(request: &Request, env: &Env, header: &str, binding: &str) -> bool {
    let actual = request.headers().get(header).ok().flatten();
    let expected = env.secret(binding).ok().map(|value| value.to_string());
    actual.zip(expected).is_some_and(|(actual, expected)| {
        let actual: [u8; 32] = Sha256::digest(actual.as_bytes()).into();
        let expected: [u8; 32] = Sha256::digest(expected.as_bytes()).into();
        constant_time_matches(&expected, &actual)
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapRequest {
    org_id: String,
    deployment_id: String,
    expires_at: i64,
}

async fn bootstrap(request: &mut Request, env: &Env) -> worker::Result<Response> {
    if !header_secret_matches(
        request,
        env,
        "x-lattice-bootstrap-auth",
        "DEPLOYMENT_BOOTSTRAP_AUTH",
    ) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let body: BootstrapRequest = match bounded_json(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.org_id.is_empty()
        || body.deployment_id.is_empty()
        || body.org_id.len() > 256
        || body.deployment_id.len() > 256
        || body.expires_at <= now_seconds()
    {
        return json(&PublicError::invalid(), 400);
    }
    let mut raw = [0u8; 48];
    getrandom::getrandom(&mut raw).map_err(|_| worker_rust_error("bootstrap"))?;
    let plaintext = format!("lbk_{}", URL_SAFE_NO_PAD.encode(raw));
    let key =
        DeploymentKey::parse(plaintext.clone()).map_err(|_| worker_rust_error("bootstrap"))?;
    let pepper = env.secret("KEY_HASH_PEPPER")?.to_string();
    let key_hash = hex::encode(deployment_key_hash(pepper.as_bytes(), &key));
    #[derive(Deserialize)]
    struct InsertedKey {
        key_hash: String,
    }
    let inserted = env
        .d1("BROKER_DB")?
        .prepare(
            "INSERT INTO deployment_keys (org_id, deployment_id, key_hash, expires_at, revoked) \
             SELECT ?, ?, ?, ?, 0 WHERE NOT EXISTS (SELECT 1 FROM deployment_keys) \
             RETURNING key_hash",
        )
        .bind(&[
            JsValue::from_str(&body.org_id),
            JsValue::from_str(&body.deployment_id),
            JsValue::from_str(&key_hash),
            JsValue::from_f64(body.expires_at as f64),
        ])?
        .first::<InsertedKey>(None)
        .await?;
    if !inserted.is_some_and(|row| {
        let left = hex::decode(row.key_hash).unwrap_or_default();
        let right = hex::decode(&key_hash).unwrap_or_default();
        constant_time_bytes_equal(&left, &right)
    }) {
        return json(&PublicError::broker(BrokerError::Brk109), 409);
    }
    json(
        &serde_json::json!({
            "deployment_key": plaintext,
            "org_id": body.org_id,
            "deployment_id": body.deployment_id,
            "expires_at": body.expires_at
        }),
        201,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InternalGrantRequest {
    org_id: String,
    deployment_id: String,
    session_ref: String,
    binding_ref: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    node_id: String,
    node_alias: String,
    run_id: String,
    operation_contract: String,
}

async fn issue_grant(request: &mut Request, env: &Env) -> worker::Result<Response> {
    if !header_secret_matches(
        request,
        env,
        "x-lattice-service-auth",
        "INVOKE_SERVICE_AUTH",
    ) {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let body: InternalGrantRequest = match bounded_json(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if [
        &body.org_id,
        &body.deployment_id,
        &body.session_ref,
        &body.binding_ref,
        &body.bundle_id,
        &body.flow_ir_hash,
        &body.binding_lock_hash,
        &body.flow_id,
        &body.node_id,
        &body.node_alias,
        &body.run_id,
        &body.operation_contract,
    ]
    .iter()
    .any(|value| value.is_empty() || value.len() > 256)
        || !valid_sha256(&body.flow_ir_hash)
        || !valid_sha256(&body.binding_lock_hash)
    {
        return json(&PublicError::invalid(), 400);
    }
    #[derive(Deserialize)]
    struct AuthorityRow {
        connection_ref: String,
        attestation_json: String,
        contract_set_json: String,
        authority_manifest_json: String,
        authority_manifest_hash: String,
        account_commitment: String,
        revocation_epoch: u64,
        pop_key_thumbprint: String,
    }
    let db = env.d1("BROKER_DB")?;
    let row = db
        .prepare(
            "SELECT b.connection_ref, b.attestation_json, b.contract_set_json, \
                    b.authority_manifest_json, b.authority_manifest_hash, \
                    c.account_commitment, c.revocation_epoch, s.pop_key_thumbprint \
             FROM bindings b \
             JOIN connections c ON c.org_id = b.org_id AND c.connection_ref = b.connection_ref \
             JOIN sessions s ON s.org_id = b.org_id AND s.deployment_id = b.deployment_id \
             WHERE b.org_id = ? AND b.deployment_id = ? AND b.binding_ref = ? \
               AND b.bundle_id = ? AND b.flow_ir_hash = ? AND b.binding_lock_hash = ? \
               AND b.flow_id = ? AND b.revoked = 0 AND c.status = 'active' \
               AND s.session_ref = ? AND s.revoked = 0 AND s.expires_at > ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(&body.org_id),
            JsValue::from_str(&body.deployment_id),
            JsValue::from_str(&body.binding_ref),
            JsValue::from_str(&body.bundle_id),
            JsValue::from_str(&body.flow_ir_hash),
            JsValue::from_str(&body.binding_lock_hash),
            JsValue::from_str(&body.flow_id),
            JsValue::from_str(&body.session_ref),
            JsValue::from_f64(now_seconds() as f64),
        ])?
        .first::<AuthorityRow>(None)
        .await?;
    let Some(row) = row else {
        return json(&PublicError::broker(BrokerError::Brk109), 403);
    };
    let installed: Vec<String> =
        serde_json::from_str(&row.contract_set_json).map_err(|_| worker_rust_error("authority"))?;
    if !installed
        .iter()
        .any(|contract| contract == &body.operation_contract)
    {
        return json(&PublicError::broker(BrokerError::Brk108), 403);
    }
    let binding: broker_core::artifacts::ParsedArtifact<
        broker_core::artifacts::BindingAttestation,
    > = broker_core::artifacts::parse(row.attestation_json.as_bytes())
        .map_err(|_| worker_rust_error("authority"))?;
    let signer = broker_core::signing::BrokerSigner::from_seed(
        "broker-binding-v1",
        secret_32(env, "BINDING_SIGNING_SEED")?,
    );
    signer
        .verifying_key()
        .verify_json(
            broker_core::signing::BINDING_DOMAIN,
            row.attestation_json.as_bytes(),
            &binding.view.signature,
        )
        .map_err(|_| worker_rust_error("authority"))?;
    if binding.view.validate_active_at(&now_rfc3339()).is_err() {
        return json(&PublicError::broker(BrokerError::Brk109), 403);
    }
    let authority: broker_core::artifacts::ParsedArtifact<
        broker_core::artifacts::FlowAuthorityManifest,
    > = broker_core::artifacts::parse(row.authority_manifest_json.as_bytes())
        .map_err(|_| worker_rust_error("authority"))?;
    if authority.canonical_bytes() != row.authority_manifest_json.as_bytes()
        || authority.content_hash() != row.authority_manifest_hash
        || binding.view.authority_manifest_hash.as_deref()
            != Some(row.authority_manifest_hash.as_str())
        || authority.view.org_id != body.org_id
        || authority.view.principal.id != body.deployment_id
        || authority.view.flow_ir_hash != body.flow_ir_hash
    {
        return json(&PublicError::broker(BrokerError::Brk109), 403);
    }
    let node_authority = match authority.view.nodes.get(&body.node_alias) {
        Some(node) if node.node_id == body.node_id => node,
        _ => return json(&PublicError::broker(BrokerError::Brk107), 403),
    };
    let operation_authority = match node_authority
        .operations
        .iter()
        .find(|operation| operation.contract_id == body.operation_contract)
    {
        Some(operation) => operation,
        None => return json(&PublicError::broker(BrokerError::Brk108), 403),
    };
    let supported = binding
        .view
        .supported_contracts
        .iter()
        .find(|contract| contract.contract_id == body.operation_contract)
        .ok_or_else(|| worker_rust_error("authority"))?;
    let required_scope = match provider_google::adapter(&body.operation_contract) {
        Ok(adapter) => adapter.required_claim,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk108), 403),
    };
    if !binding
        .view
        .scope_alignment
        .actual_scopes
        .iter()
        .any(|scope| scope == required_scope)
        || binding.view.account_commitment.value != row.account_commitment
        || binding.view.revocation_epoch != row.revocation_epoch
    {
        return json(&PublicError::broker(BrokerError::Brk109), 403);
    }
    let flow_aggregate_limit = authority
        .view
        .aggregate_ceilings
        .as_ref()
        .and_then(|ceilings| ceilings.flow.as_ref())
        .map(|ceiling| ceiling.max_logical_calls);
    let connection_aggregate_key = operation_authority.connection_aggregate_key.clone();
    let connection_aggregate_limit = match &connection_aggregate_key {
        Some(key) => match authority
            .view
            .aggregate_ceilings
            .as_ref()
            .and_then(|ceilings| ceilings.connections.get(key))
        {
            Some(ceiling) => Some(ceiling.max_logical_calls),
            None => return json(&PublicError::broker(BrokerError::Brk109), 403),
        },
        None => None,
    };
    let allocated_logical_calls = operation_authority.call_budget.max_logical_calls;
    let grant_node_id = body.node_id.clone();
    let grant_operation_contract = body.operation_contract.clone();
    let grant_ref = opaque_id("grant_")?;
    let grant = broker_core::artifacts::ExecutionGrant {
        schema_version: "0.1".into(),
        critical_fields: vec![],
        org_id: body.org_id.clone(),
        principal: broker_core::artifacts::PrincipalRef {
            kind: broker_core::artifacts::PrincipalKind::Deployment,
            id: body.deployment_id.clone(),
        },
        grant_ref: grant_ref.clone(),
        authority_manifest_hash: Some(row.authority_manifest_hash.clone()),
        issuer: "broker-workers-v1".into(),
        audience: "broker-execution".into(),
        channel_binding: broker_core::artifacts::ChannelBinding {
            method: broker_core::artifacts::ChannelMethod::WorkersPrivateBinding,
            key_thumbprint: row.pop_key_thumbprint,
            session_id: body.session_ref,
        },
        subject: broker_core::artifacts::GrantSubject::FlowNodeRun {
            bundle_id: body.bundle_id,
            flow_ir_hash: body.flow_ir_hash,
            binding_lock_hash: body.binding_lock_hash,
            flow_id: body.flow_id,
            node_id: body.node_id,
            node_alias: body.node_alias,
            run_id: body.run_id,
        },
        operation_contract: body.operation_contract,
        contract_hash: supported.contract_hash.clone(),
        connection_ref: row.connection_ref,
        provider: binding.view.provider.clone(),
        account_commitment: binding.view.account_commitment.clone(),
        roles: binding.view.roles.clone(),
        scopes: binding.view.scope_alignment.actual_scopes.clone(),
        budgets: broker_core::artifacts::GrantBudgets {
            logical_calls: operation_authority.call_budget.max_logical_calls,
            dispatch_attempts_per_call: operation_authority
                .call_budget
                .max_dispatch_attempts_per_call,
        },
        aggregate_budgets: None,
        minimum_assurance: operation_authority.minimum_assurance,
        required_attenuations: operation_authority.required_attenuations.clone(),
        revocation_epoch: row.revocation_epoch,
        not_before: rfc3339_from_seconds(now_seconds() - 1),
        expires_at: rfc3339_from_seconds(now_seconds() + 300),
        jti: opaque_id("grant_jti_")?,
        extensions: Default::default(),
    };
    let canonical = broker_core::canonical::from_serde(&grant, broker_core::artifacts::GRANT_MAX)
        .map_err(|_| worker_rust_error("grant"))?
        .into_bytes();
    broker_core::grant::ExecutionGrantRecord::parse_canonical(&canonical)
        .map_err(|_| worker_rust_error("grant"))?;
    #[derive(Deserialize)]
    struct InsertedGrant {
        grant_ref: String,
    }
    let now = now_seconds();
    let flow_limit = flow_aggregate_limit
        .map(|value| JsValue::from_f64(value as f64))
        .unwrap_or(JsValue::NULL);
    let connection_key = connection_aggregate_key
        .as_ref()
        .map(|value| JsValue::from_str(value))
        .unwrap_or(JsValue::NULL);
    let connection_limit = connection_aggregate_limit
        .map(|value| JsValue::from_f64(value as f64))
        .unwrap_or(JsValue::NULL);
    let inserted = db.prepare(
        "INSERT INTO grants \
         (org_id, grant_ref, binding_ref, canonical_grant, node_id, operation_contract, \
          allocated_logical_calls, flow_aggregate_limit, connection_aggregate_key, \
          connection_aggregate_limit, expires_at, revoked) \
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0 \
         WHERE COALESCE((SELECT SUM(allocated_logical_calls) FROM grants \
                         WHERE org_id = ? AND binding_ref = ? AND node_id = ? \
                           AND operation_contract = ? AND revoked = 0), 0) + ? <= ? \
           AND (? IS NULL OR COALESCE((SELECT SUM(allocated_logical_calls) FROM grants \
                                      WHERE org_id = ? AND binding_ref = ? AND revoked = 0), 0) + ? <= ?) \
           AND (? IS NULL OR COALESCE((SELECT SUM(allocated_logical_calls) FROM grants \
                                      WHERE org_id = ? AND binding_ref = ? \
                                        AND connection_aggregate_key = ? AND revoked = 0), 0) + ? <= ?) \
         RETURNING grant_ref",
    )
    .bind(&[
        JsValue::from_str(&body.org_id),
        JsValue::from_str(&grant_ref),
        JsValue::from_str(&body.binding_ref),
        JsValue::from_str(std::str::from_utf8(&canonical).map_err(|_| worker_rust_error("grant"))?),
        JsValue::from_str(&grant_node_id),
        JsValue::from_str(&grant_operation_contract),
        JsValue::from_f64(allocated_logical_calls as f64),
        flow_limit.clone(),
        connection_key.clone(),
        connection_limit.clone(),
        JsValue::from_f64((now + 300) as f64),
        JsValue::from_str(&body.org_id),
        JsValue::from_str(&body.binding_ref),
        JsValue::from_str(&grant_node_id),
        JsValue::from_str(&grant_operation_contract),
        JsValue::from_f64(allocated_logical_calls as f64),
        JsValue::from_f64(allocated_logical_calls as f64),
        flow_limit.clone(),
        JsValue::from_str(&body.org_id),
        JsValue::from_str(&body.binding_ref),
        JsValue::from_f64(allocated_logical_calls as f64),
        flow_limit,
        connection_key.clone(),
        JsValue::from_str(&body.org_id),
        JsValue::from_str(&body.binding_ref),
        connection_key,
        JsValue::from_f64(allocated_logical_calls as f64),
        connection_limit,
    ])?
    .first::<InsertedGrant>(None)
    .await?;
    if !inserted.is_some_and(|inserted| inserted.grant_ref == grant_ref) {
        return json(&PublicError::broker(BrokerError::Brk201), 409);
    }
    json(
        &serde_json::json!({"grant_ref": grant_ref, "expires_in": 300}),
        201,
    )
}

async fn readiness(env: &Env) -> worker::Result<Response> {
    let db = match env.d1("BROKER_DB") {
        Ok(db) => db,
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    match db
        .prepare("SELECT version FROM broker_schema LIMIT 1")
        .first::<i64>(Some("version"))
        .await
    {
        Ok(Some(1)) => json(&serde_json::json!({"status":"ready"}), 200),
        _ => json(&PublicError::unavailable(), 503),
    }
}

async fn create_session(request: &mut Request, env: &Env) -> worker::Result<Response> {
    let body: SessionExchangeRequest = match bounded_json(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let key = match DeploymentKey::parse(body.deployment_key.clone()) {
        Ok(key) => key,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk101), 401),
    };
    let pepper = match env.secret("KEY_HASH_PEPPER") {
        Ok(value) => value.to_string(),
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    let key_hash = hex::encode(deployment_key_hash(pepper.as_bytes(), &key));
    let db = match env.d1("BROKER_DB") {
        Ok(db) => db,
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    #[derive(Clone, Deserialize)]
    struct KeyRow {
        org_id: String,
        deployment_id: String,
        key_hash: String,
    }
    let now = now_seconds();
    let row = db
        .prepare(
            "SELECT org_id, deployment_id, key_hash FROM deployment_keys \
             WHERE key_hash = ? AND revoked = 0 AND expires_at > ? LIMIT 1",
        )
        .bind(&[JsValue::from_str(&key_hash), JsValue::from_f64(now as f64)])?
        .first::<KeyRow>(None)
        .await;
    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => return json(&PublicError::broker(BrokerError::Brk101), 401),
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    let candidate = hex::decode(&key_hash)
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .ok_or_else(|| worker_rust_error("key hash"))?;
    let stored = hex::decode(&row.key_hash)
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok());
    if !stored
        .as_ref()
        .is_some_and(|stored| constant_time_matches(stored, &candidate))
    {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let deployment_key_id = format!("sha256:{key_hash}");
    let public_key = match verify_exchange_request(&body, &deployment_key_id, now) {
        Ok(key) => key,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk102), 401),
    };
    // Reserving the exchange nonce before creating the session is intentional:
    // a storage failure may consume a nonce, but can never make it replayable.
    if db
        .prepare(
            "INSERT INTO session_exchange_nonces (deployment_key_id, client_nonce, timestamp) \
             VALUES (?, ?, ?)",
        )
        .bind(&[
            JsValue::from_str(&deployment_key_id),
            JsValue::from_str(&body.client_nonce),
            JsValue::from_f64(body.timestamp as f64),
        ])?
        .run()
        .await
        .is_err()
    {
        return json(&PublicError::broker(BrokerError::Brk102), 401);
    }
    let session_ref = match opaque_id("session_") {
        Ok(value) => value,
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    let pop_key_thumbprint = public_key_thumbprint(&public_key);
    let pop_public_key = URL_SAFE_NO_PAD.encode(public_key);
    let expires_at = now + SESSION_TTL_SECONDS;
    let statement = db
        .prepare(
            "INSERT INTO sessions \
             (session_ref, org_id, deployment_id, pop_key_thumbprint, pop_public_key, expires_at, revoked) \
             VALUES (?, ?, ?, ?, ?, ?, 0)",
        )
        .bind(&[
            JsValue::from_str(&session_ref),
            JsValue::from_str(&row.org_id),
            JsValue::from_str(&row.deployment_id),
            JsValue::from_str(&pop_key_thumbprint),
            JsValue::from_str(&pop_public_key),
            JsValue::from_f64(expires_at as f64),
        ])?;
    if statement.run().await.is_err() {
        return json(&PublicError::unavailable(), 503);
    }
    json(
        &SessionExchangeResponse {
            session_ref,
            expires_at,
        },
        201,
    )
}

struct OAuthProfile {
    authorize_endpoint: String,
    client_id: String,
    redirect_uri: String,
    scopes: Vec<String>,
}

fn oauth_profile(env: &Env, connector: &str, auth_profile: &str) -> worker::Result<OAuthProfile> {
    if connector != CONNECTOR_REF || auth_profile != AUTH_PROFILE_REF {
        return Err(worker_rust_error("oauth profile"));
    }
    let authorize_endpoint = env
        .var(provider_google::AUTHORIZATION_ENDPOINT_BINDING)?
        .to_string();
    let parsed = worker::Url::parse(&authorize_endpoint)?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() || parsed.query().is_some() {
        return Err(worker_rust_error("oauth profile"));
    }
    let client_id = env
        .var(provider_google::OAUTH_CLIENT_ID_BINDING)?
        .to_string();
    let redirect_uri = env.var("OAUTH_REDIRECT_URI")?.to_string();
    let redirect = worker::Url::parse(&redirect_uri)?;
    if client_id.is_empty()
        || client_id.len() > 512
        || redirect.scheme() != "https"
        || redirect.path() != "/v1/oauth/callback"
    {
        return Err(worker_rust_error("oauth profile"));
    }
    Ok(OAuthProfile {
        authorize_endpoint,
        client_id,
        redirect_uri,
        scopes: APPROVED_SCOPES
            .iter()
            .map(|scope| (*scope).into())
            .collect(),
    })
}

fn seal_oauth_verifier(
    env: &Env,
    intent_ref: &str,
    verifier: &[u8],
) -> worker::Result<(String, String)> {
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    let mut nonce = [0u8; 12];
    getrandom::getrandom(&mut nonce).map_err(|_| worker_rust_error("oauth state"))?;
    let cipher = ChaCha20Poly1305::new((&key).into());
    let ciphertext = cipher
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: verifier,
                aad: intent_ref.as_bytes(),
            },
        )
        .map_err(|_| worker_rust_error("oauth state"))?;
    Ok((
        URL_SAFE_NO_PAD.encode(nonce),
        URL_SAFE_NO_PAD.encode(ciphertext),
    ))
}

fn seal_activation_payload(
    env: &Env,
    intent_ref: &str,
    payload: &[u8],
) -> worker::Result<(String, String)> {
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    let mut nonce = [0u8; 12];
    getrandom::getrandom(&mut nonce).map_err(|_| worker_rust_error("activation state"))?;
    let cipher = ChaCha20Poly1305::new((&key).into());
    let aad = format!("{intent_ref}\0activation-v1");
    let ciphertext = cipher
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: payload,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| worker_rust_error("activation state"))?;
    Ok((
        URL_SAFE_NO_PAD.encode(nonce),
        URL_SAFE_NO_PAD.encode(ciphertext),
    ))
}

fn open_activation_payload(
    env: &Env,
    intent_ref: &str,
    nonce: &str,
    ciphertext: &str,
) -> worker::Result<Vec<u8>> {
    let nonce_bytes = URL_SAFE_NO_PAD
        .decode(nonce)
        .map_err(|_| worker_rust_error("activation state"))?;
    if URL_SAFE_NO_PAD.encode(&nonce_bytes) != nonce {
        return Err(worker_rust_error("activation state"));
    }
    let nonce =
        <[u8; 12]>::try_from(nonce_bytes).map_err(|_| worker_rust_error("activation state"))?;
    let ciphertext_bytes = URL_SAFE_NO_PAD
        .decode(ciphertext)
        .map_err(|_| worker_rust_error("activation state"))?;
    if URL_SAFE_NO_PAD.encode(&ciphertext_bytes) != ciphertext {
        return Err(worker_rust_error("activation state"));
    }
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    let aad = format!("{intent_ref}\0activation-v1");
    ChaCha20Poly1305::new((&key).into())
        .decrypt(
            (&nonce).into(),
            Payload {
                msg: &ciphertext_bytes,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| worker_rust_error("activation state"))
}

fn open_oauth_verifier(
    env: &Env,
    intent_ref: &str,
    nonce: &str,
    ciphertext: &str,
) -> worker::Result<String> {
    let nonce = URL_SAFE_NO_PAD
        .decode(nonce)
        .ok()
        .and_then(|bytes| <[u8; 12]>::try_from(bytes).ok())
        .ok_or_else(|| worker_rust_error("oauth state"))?;
    let ciphertext = URL_SAFE_NO_PAD
        .decode(ciphertext)
        .map_err(|_| worker_rust_error("oauth state"))?;
    let key = secret_32(env, "CUSTODY_ROOT_KEY")?;
    let plaintext = ChaCha20Poly1305::new((&key).into())
        .decrypt(
            (&nonce).into(),
            Payload {
                msg: &ciphertext,
                aad: intent_ref.as_bytes(),
            },
        )
        .map_err(|_| worker_rust_error("oauth state"))?;
    String::from_utf8(plaintext).map_err(|_| worker_rust_error("oauth state"))
}

async fn create_connection_intent(request: &mut Request, env: &Env) -> worker::Result<Response> {
    let exact_body = match bounded_body(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact_body).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let body: ConnectionIntentRequest = match parse_json(&exact_body) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let management_profile = match crate::composition::legacy_management_profile() {
        Ok(profile) => profile,
        Err(error) => return json(&PublicError::broker(error), 503),
    };
    if validate_intent(&body, &management_profile).is_err() {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    let profile = match oauth_profile(env, &body.connector_ref, &body.auth_profile_ref) {
        Ok(profile) => profile,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk109), 400),
    };
    let intent_ref = opaque_id("intent_")?;
    let state = opaque_id("oauth_state_")?;
    let verifier = opaque_id("pkce_")?;
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let (pkce_nonce, pkce_ciphertext) = seal_oauth_verifier(env, &intent_ref, verifier.as_bytes())?;
    let state_hash = hex::encode(Sha256::digest(state.as_bytes()));
    let expires_at = now_seconds() + crate::protocol::OAUTH_STATE_TTL_SECONDS;
    let db = env.d1("BROKER_DB")?;
    let result = db
        .prepare(
            "INSERT INTO connection_intents \
             (intent_ref, org_id, connector_ref, auth_profile_ref, execution_lane, custody, \
              oauth_state_hash, pkce_nonce, pkce_ciphertext, expires_at, status) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
        )
        .bind(&[
            JsValue::from_str(&intent_ref),
            JsValue::from_str(&session.org_id),
            JsValue::from_str(CONNECTOR_REF),
            JsValue::from_str(AUTH_PROFILE_REF),
            JsValue::from_str(EXECUTION_LANE),
            JsValue::from_str(CUSTODY),
            JsValue::from_str(&state_hash),
            JsValue::from_str(&pkce_nonce),
            JsValue::from_str(&pkce_ciphertext),
            JsValue::from_f64(expires_at as f64),
        ])?
        .run()
        .await;
    if result.is_err() {
        return json(&PublicError::unavailable(), 503);
    }
    let mut authorization_url = worker::Url::parse(&profile.authorize_endpoint)
        .map_err(|_| worker_rust_error("oauth profile"))?;
    {
        let mut query = authorization_url.query_pairs_mut();
        query.append_pair("client_id", &profile.client_id);
        query.append_pair("redirect_uri", &profile.redirect_uri);
        query.append_pair("scope", &profile.scopes.join(" "));
        query.append_pair("state", &state);
        query.append_pair("response_type", "code");
        query.append_pair("code_challenge", &challenge);
        query.append_pair("code_challenge_method", "S256");
    }
    json(
        &ConnectionIntentResponse {
            intent_ref,
            next_action: NextAction::OpenUrl {
                url: authorization_url.to_string(),
            },
        },
        201,
    )
}

#[derive(Deserialize)]
struct SessionRow {
    session_ref: String,
    org_id: String,
    deployment_id: String,
    pop_key_thumbprint: String,
    pop_public_key: String,
}

async fn authenticate(
    request: &Request,
    env: &Env,
    exact_body: &[u8],
) -> Result<SessionRow, Response> {
    let session_ref = request
        .headers()
        .get("authorization")
        .ok()
        .flatten()
        .and_then(|value| value.strip_prefix("Session ").map(ToOwned::to_owned));
    let jti = request.headers().get("x-lattice-pop-jti").ok().flatten();
    let timestamp = request
        .headers()
        .get("x-lattice-pop-timestamp")
        .ok()
        .flatten()
        .and_then(|value| value.parse::<i64>().ok());
    let signature = request
        .headers()
        .get("x-lattice-pop-signature")
        .ok()
        .flatten();
    let (Some(session_ref), Some(jti), Some(timestamp), Some(signature)) =
        (session_ref, jti, timestamp, signature)
    else {
        return Err(json_value(&PublicError::broker(BrokerError::Brk102), 401));
    };
    let now = now_seconds();
    if !fresh_timestamp(timestamp, now)
        || !(16..=128).contains(&jti.len())
        || !jti.is_ascii()
        || jti.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(json_value(&PublicError::broker(BrokerError::Brk102), 401));
    }
    let db = env
        .d1("BROKER_DB")
        .map_err(|_| json_value(&PublicError::unavailable(), 503))?;
    let row = db
        .prepare(
            "SELECT session_ref, org_id, deployment_id, pop_key_thumbprint, pop_public_key FROM sessions \
             WHERE session_ref = ? AND revoked = 0 AND expires_at > ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(&session_ref),
            JsValue::from_f64(now as f64),
        ])
        .map_err(|_| json_value(&PublicError::unavailable(), 503))?
        .first::<SessionRow>(None)
        .await
        .map_err(|_| json_value(&PublicError::unavailable(), 503))?
        .ok_or_else(|| json_value(&PublicError::broker(BrokerError::Brk102), 401))?;
    let public_key = crate::management::decode_public_key(&row.pop_public_key)
        .map_err(|_| json_value(&PublicError::unavailable(), 503))?;
    let request_target = exact_request_target(request)
        .map_err(|_| json_value(&PublicError::broker(BrokerError::Brk102), 401))?;
    let body_hash: [u8; 32] = Sha256::digest(exact_body).into();
    let transcript = request_transcript(
        &row.session_ref,
        BROKER_REQUEST_AUDIENCE,
        &request.method().to_string(),
        &request_target,
        &body_hash,
        timestamp,
        &jti,
    )
    .map_err(|_| json_value(&PublicError::broker(BrokerError::Brk102), 401))?;
    verify_ed25519(&public_key, &transcript, &signature)
        .map_err(|_| json_value(&PublicError::broker(BrokerError::Brk102), 401))?;
    // The uniqueness constraint is the replay boundary. It is reserved only
    // after cryptographic verification and before any custodian/DO/provider I/O.
    db.prepare("INSERT INTO session_request_jtis (session_ref, jti, timestamp) VALUES (?, ?, ?)")
        .bind(&[
            JsValue::from_str(&row.session_ref),
            JsValue::from_str(&jti),
            JsValue::from_f64(timestamp as f64),
        ])
        .map_err(|_| json_value(&PublicError::unavailable(), 503))?
        .run()
        .await
        .map_err(|_| json_value(&PublicError::broker(BrokerError::Brk102), 401))?;
    Ok(row)
}

/// The PoP request-target is the URL runtime's serialized pathname plus the
/// raw serialized query. It is not decoded, sorted, or re-encoded: ordering,
/// duplicate keys, `+` versus `%20`, and percent-escape spelling are signed.
fn exact_request_target(request: &Request) -> worker::Result<String> {
    let url = request.url()?;
    Ok(match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_string(),
    })
}

fn d1_changed_once(result: &worker::D1Result) -> bool {
    result.success() && result.meta().ok().flatten().and_then(|meta| meta.changes) == Some(1)
}

async fn mark_intent_failed(db: &worker::D1Database, intent_ref: &str, code: &str) {
    if let Ok(statement) = db
        .prepare(
            "UPDATE connection_intents SET status = 'failed', failure_code = ? \
             WHERE intent_ref = ? AND status <> 'ready'",
        )
        .bind(&[JsValue::from_str(code), JsValue::from_str(intent_ref)])
    {
        let _ = statement.run().await;
    }
}

fn valid_account_id(value: &str) -> bool {
    if !(3..=512).contains(&value.len())
        || value != value.trim()
        || value != value.to_ascii_lowercase()
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return false;
    }
    let mut parts = value.split('@');
    let local = parts.next().unwrap_or_default();
    let domain = parts.next().unwrap_or_default();
    !local.is_empty()
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && parts.next().is_none()
}

async fn bounded_response_bytes(response: &mut Response, max: usize) -> worker::Result<Vec<u8>> {
    use futures::TryStreamExt;
    if response
        .headers()
        .get("content-length")?
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > max)
    {
        return Err(worker_rust_error("response too large"));
    }
    let mut stream = response.stream()?;
    let mut bytes = Vec::with_capacity(max.min(4096));
    while let Some(chunk) = stream.try_next().await? {
        if chunk.len() > max.saturating_sub(bytes.len()) {
            // Drop the stream immediately without extending the aggregate
            // buffer. This bounds allocation even without Content-Length.
            return Err(worker_rust_error("response too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn bounded_response_json<T: DeserializeOwned>(
    response: &mut Response,
    max: usize,
) -> worker::Result<T> {
    parse_json(&bounded_response_bytes(response, max).await?)
}

fn inject_activation_crash(request: &Request, phase: &str) -> bool {
    #[cfg(feature = "test-fixtures")]
    {
        return test_fixtures::inject_activation_crash(request, phase);
    }
    #[cfg(not(feature = "test-fixtures"))]
    {
        let _ = (request, phase);
        false
    }
}

async fn oauth_callback(request: &Request, env: &Env) -> worker::Result<Response> {
    let url = request.url()?;
    let state = url
        .query_pairs()
        .find(|(name, _)| name == "state")
        .map(|(_, value)| value.to_string());
    let code = url
        .query_pairs()
        .find(|(name, _)| name == "code")
        .map(|(_, value)| value.to_string());
    let (Some(state), Some(code)) = (state, code) else {
        return json(&PublicError::invalid(), 400);
    };
    if state.len() > 256 || code.len() > 4096 || !state.is_ascii() || !code.is_ascii() {
        return json(&PublicError::invalid(), 400);
    }
    #[derive(Deserialize)]
    struct IntentRow {
        intent_ref: String,
        org_id: String,
        connector_ref: String,
        auth_profile_ref: String,
        pkce_nonce: String,
        pkce_ciphertext: String,
        status: String,
        activation_phase: Option<String>,
        exchange_nonce: Option<String>,
        exchange_ciphertext: Option<String>,
        activation_connection_ref: Option<String>,
        activation_route: Option<String>,
        activation_account_commitment: Option<String>,
        activation_scopes_json: Option<String>,
        activation_nonce: Option<String>,
        activation_ciphertext: Option<String>,
    }
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ExchangePayload {
        code: String,
        code_verifier: String,
        client_id: String,
        redirect_uri: String,
    }
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ActivationPayload {
        connection_ref: String,
        route: String,
        account_commitment: String,
        scopes: std::collections::BTreeSet<String>,
        refresh_token: String,
        access_token: String,
        access_expires_at: i64,
    }
    let db = env.d1("BROKER_DB")?;
    let state_hash = hex::encode(Sha256::digest(state.as_bytes()));
    const INTENT_COLUMNS: &str = "intent_ref, org_id, connector_ref, auth_profile_ref, pkce_nonce, pkce_ciphertext, status, activation_phase, exchange_nonce, exchange_ciphertext, activation_connection_ref, activation_route, activation_account_commitment, activation_scopes_json, activation_nonce, activation_ciphertext";
    let initial = db
        .prepare(&format!(
            "SELECT {INTENT_COLUMNS} FROM connection_intents \
             WHERE oauth_state_hash = ? AND status IN ('pending', 'claimed', 'activating', 'cleanup_pending') \
               AND (status = 'cleanup_pending' OR expires_at > ?) LIMIT 1"
        ))
        .bind(&[
            JsValue::from_str(&state_hash),
            JsValue::from_f64(now_seconds() as f64),
        ])?
        .first::<IntentRow>(None)
        .await?;
    let Some(initial) = initial else {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    };
    let row = if initial.status == "pending" {
        let profile = match oauth_profile(env, &initial.connector_ref, &initial.auth_profile_ref) {
            Ok(profile) => profile,
            Err(_) => {
                mark_intent_failed(&db, &initial.intent_ref, "profile_unavailable").await;
                return json(&PublicError::unavailable(), 503);
            }
        };
        let code_verifier = match open_oauth_verifier(
            env,
            &initial.intent_ref,
            &initial.pkce_nonce,
            &initial.pkce_ciphertext,
        ) {
            Ok(value) => value,
            Err(_) => {
                mark_intent_failed(&db, &initial.intent_ref, "state_unavailable").await;
                return json(&PublicError::unavailable(), 503);
            }
        };
        let exchange = ExchangePayload {
            code: code.clone(),
            code_verifier,
            client_id: profile.client_id,
            redirect_uri: profile.redirect_uri,
        };
        let exchange_bytes =
            serde_json::to_vec(&exchange).map_err(|_| worker_rust_error("exchange state"))?;
        let (exchange_nonce, exchange_ciphertext) = seal_activation_payload(
            env,
            &format!("{}\0exchange", initial.intent_ref),
            &exchange_bytes,
        )?;
        let claimed = db
            .prepare(&format!(
                "UPDATE connection_intents SET status = 'claimed', activation_phase = 'exchange_pending', \
                 exchange_nonce = ?, exchange_ciphertext = ? \
                 WHERE intent_ref = ? AND status = 'pending' RETURNING {INTENT_COLUMNS}"
            ))
            .bind(&[
                JsValue::from_str(&exchange_nonce),
                JsValue::from_str(&exchange_ciphertext),
                JsValue::from_str(&initial.intent_ref),
            ])?
            .first::<IntentRow>(None)
            .await?;
        let Some(claimed) = claimed else {
            return json(&PublicError::broker(BrokerError::Brk109), 409);
        };
        if inject_activation_crash(request, "claimed") {
            return json(&PublicError::unavailable(), 599);
        }
        claimed
    } else {
        initial
    };

    if row.status == "cleanup_pending" {
        let (Some(connection_ref), Some(route)) = (
            row.activation_connection_ref.as_deref(),
            row.activation_route.as_deref(),
        ) else {
            return json(&PublicError::unavailable(), 503);
        };
        let cleaned = retry_activation_cleanup(
            env,
            &db,
            &row.org_id,
            &row.intent_ref,
            connection_ref,
            route,
            false,
        )
        .await;
        return if cleaned {
            json(&PublicError::broker(BrokerError::Brk109), 400)
        } else {
            json(&PublicError::unavailable(), 503)
        };
    }

    let payload = if row.status == "activating" {
        let (Some(nonce), Some(ciphertext)) = (&row.activation_nonce, &row.activation_ciphertext)
        else {
            mark_intent_failed(&db, &row.intent_ref, "activation_state_missing").await;
            return json(&PublicError::unavailable(), 503);
        };
        let plaintext = match open_activation_payload(env, &row.intent_ref, nonce, ciphertext) {
            Ok(value) => value,
            Err(_) => {
                mark_intent_failed(&db, &row.intent_ref, "activation_state_unavailable").await;
                return json(&PublicError::unavailable(), 503);
            }
        };
        let payload: ActivationPayload = match serde_json::from_slice(&plaintext) {
            Ok(value) => value,
            Err(_) => return json(&PublicError::unavailable(), 503),
        };
        let scopes_json =
            serde_json::to_string(&payload.scopes).map_err(|_| worker_rust_error("scopes"))?;
        if row.activation_connection_ref.as_deref() != Some(payload.connection_ref.as_str())
            || row.activation_route.as_deref() != Some(payload.route.as_str())
            || row.activation_account_commitment.as_deref()
                != Some(payload.account_commitment.as_str())
            || row.activation_scopes_json.as_deref() != Some(scopes_json.as_str())
            || !matches!(
                row.activation_phase.as_deref(),
                Some("route_reserved" | "credential_registered" | "connection_inserted")
            )
        {
            mark_intent_failed(&db, &row.intent_ref, "activation_state_mismatch").await;
            return json(&PublicError::unavailable(), 503);
        }
        payload
    } else {
        let (Some(exchange_nonce), Some(exchange_ciphertext)) =
            (&row.exchange_nonce, &row.exchange_ciphertext)
        else {
            mark_restart_required(&db, &row.intent_ref, "exchange_state_missing").await;
            return json(&PublicError::broker(BrokerError::Brk109), 400);
        };
        let exchange_bytes = match open_activation_payload(
            env,
            &format!("{}\0exchange", row.intent_ref),
            exchange_nonce,
            exchange_ciphertext,
        ) {
            Ok(value) => value,
            Err(_) => {
                mark_restart_required(&db, &row.intent_ref, "exchange_state_unavailable").await;
                return json(&PublicError::broker(BrokerError::Brk109), 400);
            }
        };
        let exchange: ExchangePayload = match serde_json::from_slice(&exchange_bytes) {
            Ok(value) => value,
            Err(_) => {
                mark_restart_required(&db, &row.intent_ref, "exchange_state_invalid").await;
                return json(&PublicError::broker(BrokerError::Brk109), 400);
            }
        };
        if !matches!(
            row.activation_phase.as_deref(),
            Some("exchange_pending" | "exchange_inflight")
        ) {
            mark_restart_required(&db, &row.intent_ref, "exchange_phase_invalid").await;
            return json(&PublicError::broker(BrokerError::Brk109), 400);
        }
        if row.activation_phase.as_deref() == Some("exchange_pending") {
            let inflight = db
                .prepare(
                    "UPDATE connection_intents SET activation_phase = 'exchange_inflight' \
                     WHERE intent_ref = ? AND status = 'claimed' AND activation_phase = 'exchange_pending'",
                )
                .bind(&[JsValue::from_str(&row.intent_ref)])?
                .run()
                .await?;
            if !d1_changed_once(&inflight) {
                return json(&PublicError::unavailable(), 503);
            }
        }
        if inject_activation_crash(request, "exchange_inflight") {
            return json(&PublicError::unavailable(), 599);
        }
        let service = match env.service(provider_google::TOKEN_SERVICE_BINDING) {
            Ok(service) => service,
            Err(_) => return json(&PublicError::unavailable(), 503),
        };
        // The pinned token service treats intent_ref as an idempotency key and
        // returns the cached exchange result after a Worker crash.
        let exchange_request = serde_json::json!({
            "code": exchange.code,
            "intent_ref": row.intent_ref,
            "grant_type": "authorization_code",
            "code_verifier": exchange.code_verifier,
            "client_id": exchange.client_id,
            "redirect_uri": exchange.redirect_uri
        });
        let mut response = match service_json_timeout(
            &service,
            "http://token.internal/exchange",
            &exchange_request,
            10_000,
        )
        .await
        {
            Ok(response) if response.status_code() == 200 => response,
            _ => {
                mark_restart_required(&db, &row.intent_ref, "exchange_ambiguous").await;
                return json(&PublicError::broker(BrokerError::Brk109), 400);
            }
        };
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TokenExchange {
            refresh_token: String,
            access_token: String,
            expires_in: u64,
            scopes: Vec<String>,
            account_id: String,
        }
        let token: TokenExchange = match bounded_response_json(
            &mut response,
            crate::protocol::MAX_PROVIDER_RESPONSE,
        )
        .await
        {
            Ok(value) => value,
            Err(_) => {
                mark_restart_required(&db, &row.intent_ref, "exchange_response_ambiguous").await;
                return json(&PublicError::broker(BrokerError::Brk109), 400);
            }
        };
        if inject_activation_crash(request, "before_route_reserved") {
            return json(&PublicError::unavailable(), 599);
        }
        let expected_scopes = APPROVED_SCOPES
            .iter()
            .map(|scope| (*scope).to_string())
            .collect::<std::collections::BTreeSet<_>>();
        let scopes = token
            .scopes
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        if expected_scopes != scopes
            || token.refresh_token.is_empty()
            || token.access_token.is_empty()
            || !valid_account_id(&token.account_id)
        {
            mark_restart_required(&db, &row.intent_ref, "identity_or_scope_rejected").await;
            return json(&PublicError::broker(BrokerError::Brk109), 400);
        }
        let normalized_account = token.account_id.trim().to_ascii_lowercase();
        let commitment_key = match secret_32(env, "COMMITMENT_KEY") {
            Ok(value) => value,
            Err(_) => return json(&PublicError::unavailable(), 503),
        };
        let account_material = format!(
            "{}\0{}\0{}",
            row.org_id, row.connector_ref, normalized_account
        );
        let account_commitment = format!(
            "hmac-sha256:{}",
            hex::encode(keyed_hash(
                &commitment_key,
                b"provider-account",
                account_material.as_bytes()
            ))
        );
        let connection_ref = opaque_id("connection_")?;
        let route = refresh_route(&row.org_id, &connection_ref);
        let payload = ActivationPayload {
            connection_ref,
            route,
            account_commitment,
            scopes,
            refresh_token: token.refresh_token,
            access_token: token.access_token,
            access_expires_at: now_seconds() + i64::try_from(token.expires_in).unwrap_or(0),
        };
        let plaintext =
            serde_json::to_vec(&payload).map_err(|_| worker_rust_error("activation state"))?;
        let (nonce, ciphertext) = seal_activation_payload(env, &row.intent_ref, &plaintext)?;
        let scopes_json =
            serde_json::to_string(&payload.scopes).map_err(|_| worker_rust_error("scopes"))?;
        let reserved = db.prepare(
            "UPDATE connection_intents SET status = 'activating', activation_phase = 'route_reserved', \
             activation_connection_ref = ?, activation_route = ?, activation_account_commitment = ?, \
             activation_scopes_json = ?, activation_nonce = ?, activation_ciphertext = ? \
             WHERE intent_ref = ? AND status = 'claimed' AND activation_phase = 'exchange_inflight'"
        ).bind(&[
            JsValue::from_str(&payload.connection_ref), JsValue::from_str(&payload.route),
            JsValue::from_str(&payload.account_commitment), JsValue::from_str(&scopes_json),
            JsValue::from_str(&nonce), JsValue::from_str(&ciphertext), JsValue::from_str(&row.intent_ref),
        ])?.run().await?;
        if !d1_changed_once(&reserved) {
            return json(&PublicError::unavailable(), 503);
        }
        payload
    };

    if inject_activation_crash(request, "route_reserved") {
        return json(&PublicError::unavailable(), 599);
    }
    // The durable route and encrypted recovery payload are committed before
    // this first external credential write. Register/revoke are idempotent.
    let registration = ConnectionRegistration {
        org_id: row.org_id.clone(),
        connection_ref: payload.connection_ref.clone(),
        account_commitment: payload.account_commitment.clone(),
        granted_scopes: payload.scopes.clone(),
        refresh_token: crate::refresh::SecretBytes::new(payload.refresh_token.as_bytes().to_vec()),
        access_token: Some(crate::refresh::SecretBytes::new(
            payload.access_token.as_bytes().to_vec(),
        )),
        access_expires_at: Some(payload.access_expires_at),
        revocation_epoch: 0,
    };
    if do_request::<RefreshReply>(
        env,
        "CONNECTION_REFRESH_DO",
        &payload.route,
        &RefreshCommand::Register { registration },
    )
    .await
    .is_err()
    {
        compensate_activation(
            env,
            &db,
            &row.org_id,
            &row.intent_ref,
            &payload.connection_ref,
            &payload.route,
            "credential_store_failed",
            inject_activation_crash(request, "revoke_failure"),
        )
        .await;
        return json(&PublicError::unavailable(), 503);
    }
    if inject_activation_crash(request, "credential_registered_unrecorded") {
        return json(&PublicError::unavailable(), 599);
    }
    if inject_activation_crash(request, "cleanup_revoke_failure") {
        compensate_activation(
            env,
            &db,
            &row.org_id,
            &row.intent_ref,
            &payload.connection_ref,
            &payload.route,
            "injected_activation_failure",
            true,
        )
        .await;
        return json(&PublicError::unavailable(), 503);
    }
    let phase = db.prepare(
        "UPDATE connection_intents SET activation_phase = 'credential_registered' \
         WHERE intent_ref = ? AND status = 'activating' AND activation_phase IN ('route_reserved', 'credential_registered')"
    ).bind(&[JsValue::from_str(&row.intent_ref)])?.run().await;
    if phase.as_ref().ok().is_none_or(|result| !result.success()) {
        compensate_activation(
            env,
            &db,
            &row.org_id,
            &row.intent_ref,
            &payload.connection_ref,
            &payload.route,
            "activation_phase_failed",
            inject_activation_crash(request, "revoke_failure"),
        )
        .await;
        return json(&PublicError::unavailable(), 503);
    }
    if inject_activation_crash(request, "credential_registered") {
        return json(&PublicError::unavailable(), 599);
    }
    let scopes_json =
        serde_json::to_string(&payload.scopes).map_err(|_| worker_rust_error("scopes"))?;
    let inserted = db.prepare(
        "INSERT OR IGNORE INTO connections \
         (org_id, connection_ref, intent_ref, connector_ref, auth_profile_ref, execution_lane, custody, \
          account_commitment, actual_scopes_json, refresh_do_route, revocation_epoch, status) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'activating')"
    ).bind(&[
        JsValue::from_str(&row.org_id), JsValue::from_str(&payload.connection_ref), JsValue::from_str(&row.intent_ref),
        JsValue::from_str(CONNECTOR_REF), JsValue::from_str(AUTH_PROFILE_REF), JsValue::from_str(EXECUTION_LANE),
        JsValue::from_str(CUSTODY), JsValue::from_str(&payload.account_commitment), JsValue::from_str(&scopes_json),
        JsValue::from_str(&payload.route),
    ])?.run().await;
    if inserted
        .as_ref()
        .ok()
        .is_none_or(|result| !result.success())
    {
        compensate_activation(
            env,
            &db,
            &row.org_id,
            &row.intent_ref,
            &payload.connection_ref,
            &payload.route,
            "connection_insert_failed",
            inject_activation_crash(request, "revoke_failure"),
        )
        .await;
        return json(&PublicError::unavailable(), 503);
    }
    if inject_activation_crash(request, "connection_inserted") {
        return json(&PublicError::unavailable(), 599);
    }
    #[derive(Deserialize)]
    struct ActivationConnection {
        intent_ref: String,
        account_commitment: String,
        actual_scopes_json: String,
        refresh_do_route: String,
        status: String,
    }
    let exact = db.prepare(
        "SELECT intent_ref, account_commitment, actual_scopes_json, refresh_do_route, status FROM connections \
         WHERE org_id = ? AND connection_ref = ? LIMIT 1"
    ).bind(&[JsValue::from_str(&row.org_id), JsValue::from_str(&payload.connection_ref)])?
      .first::<ActivationConnection>(None).await?;
    if !exact.is_some_and(|value| {
        value.intent_ref == row.intent_ref
            && value.account_commitment == payload.account_commitment
            && value.actual_scopes_json == scopes_json
            && value.refresh_do_route == payload.route
            && matches!(value.status.as_str(), "activating" | "active")
    }) {
        compensate_activation(
            env,
            &db,
            &row.org_id,
            &row.intent_ref,
            &payload.connection_ref,
            &payload.route,
            "connection_mismatch",
            inject_activation_crash(request, "revoke_failure"),
        )
        .await;
        return json(&PublicError::unavailable(), 503);
    }
    let phase_statement = db
        .prepare(
            "UPDATE connection_intents SET activation_phase = 'connection_inserted' \
         WHERE intent_ref = ? AND status = 'activating'",
        )
        .bind(&[JsValue::from_str(&row.intent_ref)])?;
    let activate_connection = db.prepare(
        "UPDATE connections SET status = 'active' WHERE org_id = ? AND connection_ref = ? AND status IN ('activating', 'active')"
    ).bind(&[JsValue::from_str(&row.org_id), JsValue::from_str(&payload.connection_ref)])?;
    let activate_intent = db
        .prepare(
            "UPDATE connection_intents SET status = 'ready', failure_code = NULL \
         WHERE intent_ref = ? AND status = 'activating'",
        )
        .bind(&[JsValue::from_str(&row.intent_ref)])?;
    let activated = db
        .batch(vec![phase_statement, activate_connection, activate_intent])
        .await;
    if activated
        .as_ref()
        .ok()
        .is_none_or(|results| results.len() != 3 || results.iter().any(|result| !result.success()))
    {
        compensate_activation(
            env,
            &db,
            &row.org_id,
            &row.intent_ref,
            &payload.connection_ref,
            &payload.route,
            "activation_failed",
            inject_activation_crash(request, "revoke_failure"),
        )
        .await;
        return json(&PublicError::unavailable(), 503);
    }
    json(
        &serde_json::json!({"connection_ref": payload.connection_ref, "status": "active"}),
        200,
    )
}

async fn mark_restart_required(db: &worker::D1Database, intent_ref: &str, failure: &str) {
    if let Ok(statement) = db
        .prepare(
            "UPDATE connection_intents SET status = 'restart_required', failure_code = ?, \
             exchange_nonce = NULL, exchange_ciphertext = NULL \
             WHERE intent_ref = ? AND status IN ('claimed', 'pending')",
        )
        .bind(&[JsValue::from_str(failure), JsValue::from_str(intent_ref)])
    {
        let _ = statement.run().await;
    }
}

async fn retry_activation_cleanup(
    env: &Env,
    db: &worker::D1Database,
    org_id: &str,
    intent_ref: &str,
    connection_ref: &str,
    route: &str,
    simulate_revoke_failure: bool,
) -> bool {
    if simulate_revoke_failure {
        return false;
    }
    if do_request::<RefreshReply>(env, "CONNECTION_REFRESH_DO", route, &RefreshCommand::Revoke)
        .await
        .is_err()
    {
        return false;
    }
    let delete = match db
        .prepare("DELETE FROM connections WHERE org_id = ? AND connection_ref = ?")
        .bind(&[JsValue::from_str(org_id), JsValue::from_str(connection_ref)])
    {
        Ok(statement) => statement,
        Err(_) => return false,
    };
    let finish = match db
        .prepare(
            "UPDATE connection_intents SET status = 'restart_required', failure_code = 'activation_cleanup_completed', \
             exchange_nonce = NULL, exchange_ciphertext = NULL, activation_nonce = NULL, activation_ciphertext = NULL \
             WHERE intent_ref = ? AND status = 'cleanup_pending'",
        )
        .bind(&[JsValue::from_str(intent_ref)])
    {
        Ok(statement) => statement,
        Err(_) => return false,
    };
    db.batch(vec![delete, finish])
        .await
        .ok()
        .is_some_and(|results| results.len() == 2 && results.iter().all(|result| result.success()))
}

async fn compensate_activation(
    env: &Env,
    db: &worker::D1Database,
    org_id: &str,
    intent_ref: &str,
    connection_ref: &str,
    route: &str,
    failure: &str,
    simulate_revoke_failure: bool,
) -> bool {
    let pending = db
        .prepare(
            "UPDATE connection_intents SET status = 'cleanup_pending', activation_phase = 'cleanup_pending', failure_code = ? \
             WHERE intent_ref = ? AND status IN ('activating', 'cleanup_pending')",
        )
        .bind(&[JsValue::from_str(failure), JsValue::from_str(intent_ref)])
        .ok();
    if pending.is_none()
        || pending
            .unwrap()
            .run()
            .await
            .ok()
            .is_none_or(|result| !result.success())
    {
        return false;
    }
    retry_activation_cleanup(
        env,
        db,
        org_id,
        intent_ref,
        connection_ref,
        route,
        simulate_revoke_failure,
    )
    .await
}

async fn connection_route(request: &Request, env: &Env) -> worker::Result<Response> {
    let session = match authenticate(request, env, &[]).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let connection_ref = request
        .path()
        .trim_start_matches("/v1/connections/")
        .to_string();
    if connection_ref.is_empty() || connection_ref.len() > 128 {
        return json(&PublicError::invalid(), 400);
    }
    #[derive(Deserialize)]
    struct ConnectionRow {
        status: String,
        revocation_epoch: u64,
        refresh_do_route: String,
    }
    let db = env.d1("BROKER_DB")?;
    let row = db
        .prepare(
            "SELECT status, revocation_epoch, refresh_do_route FROM connections \
             WHERE org_id = ? AND connection_ref = ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&connection_ref),
        ])?
        .first::<ConnectionRow>(None)
        .await?;
    let Some(row) = row else {
        return json(&PublicError::broker(BrokerError::Brk109), 404);
    };
    match request.method() {
        Method::Get => json(
            &serde_json::json!({
                "connection_ref": connection_ref,
                "status": row.status,
                "revocation_epoch": row.revocation_epoch
            }),
            200,
        ),
        Method::Delete => {
            let reply: RefreshReply = do_request(
                env,
                "CONNECTION_REFRESH_DO",
                &row.refresh_do_route,
                &RefreshCommand::Revoke,
            )
            .await?;
            let epoch = match reply {
                RefreshReply::Revoked { revocation_epoch } => revocation_epoch,
                _ => return json(&PublicError::unavailable(), 503),
            };
            db.prepare(
                "UPDATE connections SET status = 'revoked', revocation_epoch = ? \
                 WHERE org_id = ? AND connection_ref = ?",
            )
            .bind(&[
                JsValue::from_f64(epoch as f64),
                JsValue::from_str(&session.org_id),
                JsValue::from_str(&connection_ref),
            ])?
            .run()
            .await?;
            json(
                &serde_json::json!({"connection_ref": connection_ref, "status":"revoked"}),
                200,
            )
        }
        _ => json(&PublicError::invalid(), 405),
    }
}

async fn install_binding(request: &mut Request, env: &Env) -> worker::Result<Response> {
    let exact_body = match bounded_body(request, MAX_MANAGEMENT_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact_body).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let body: crate::protocol::InstallBindingRequest = match parse_json(&exact_body) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.deployment_id != session.deployment_id
        || body.contracts.is_empty()
        || body.contracts.len() > 2
        || body
            .contracts
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != body.contracts.len()
        || [&body.bundle_id, &body.flow_id]
            .iter()
            .any(|value| value.is_empty() || value.len() > 256)
        || body
            .contracts
            .iter()
            .any(|contract| provider_google::adapter(contract).is_err())
        || !valid_sha256(&body.flow_ir_hash)
        || !valid_sha256(&body.binding_lock_hash)
        || body.flow_ir_json.len() > broker_core::artifacts::MANIFEST_MAX
        || body.authority_manifest_json.len() > broker_core::artifacts::MANIFEST_MAX
    {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    let composition = provider_google::google_v1();
    let approved_contracts = composition
        .adapters
        .iter()
        .map(|adapter| broker_host::ApprovedAuthorityContract {
            contract_id: adapter.contract_id,
            contract_hash: adapter.contract_hash,
        })
        .collect::<Vec<_>>();
    let verified_authority = match broker_host::verify_authority_manifest(
        body.flow_ir_json.as_bytes(),
        &body.flow_ir_hash,
        body.authority_manifest_json.as_bytes(),
        broker_host::ManifestProvenance {
            org_id: &session.org_id,
            principal_kind: broker_core::artifacts::PrincipalKind::Deployment,
            principal_id: &session.deployment_id,
        },
        &approved_contracts,
    ) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk109), 400),
    };
    if verified_authority.flow_id != body.flow_id {
        return json(&PublicError::broker(BrokerError::Brk107), 400);
    }
    let authority_contracts = verified_authority
        .manifest
        .view
        .nodes
        .values()
        .flat_map(|node| {
            node.operations
                .iter()
                .map(|operation| operation.contract_id.clone())
        })
        .collect::<std::collections::BTreeSet<_>>();
    if authority_contracts
        != body
            .contracts
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    {
        return json(&PublicError::broker(BrokerError::Brk108), 400);
    }
    let authority_manifest_hash = verified_authority.manifest.content_hash();
    let db = env.d1("BROKER_DB")?;
    #[derive(Deserialize)]
    struct ConnectionRow {
        account_commitment: String,
        actual_scopes_json: String,
        refresh_do_route: String,
        revocation_epoch: u64,
        status: String,
    }
    let connection = db
        .prepare(
            "SELECT account_commitment, actual_scopes_json, refresh_do_route, revocation_epoch, status FROM connections \
             WHERE org_id = ? AND connection_ref = ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&body.connection_ref),
        ])?
        .first::<ConnectionRow>(None)
        .await?;
    let Some(connection) = connection.filter(|value| value.status == "active") else {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    };
    let metadata: RefreshReply = do_request(
        env,
        "CONNECTION_REFRESH_DO",
        &connection.refresh_do_route,
        &RefreshCommand::Metadata,
    )
    .await?;
    let (live_account, live_scopes, live_epoch, live_revoked) = match metadata {
        RefreshReply::Metadata {
            org_id,
            connection_ref,
            account_commitment,
            effective_scopes,
            revocation_epoch,
            revoked,
        } if org_id == session.org_id && connection_ref == body.connection_ref => (
            account_commitment,
            effective_scopes,
            revocation_epoch,
            revoked,
        ),
        _ => return json(&PublicError::unavailable(), 503),
    };
    let stored_scopes: Vec<String> = serde_json::from_str(&connection.actual_scopes_json)
        .map_err(|_| worker_rust_error("connection metadata"))?;
    if live_revoked
        || live_account != connection.account_commitment
        || live_epoch != connection.revocation_epoch
        || live_scopes
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            != stored_scopes.iter().cloned().collect()
    {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    let binding_ref = opaque_id("binding_")?;
    let binding_signer = broker_core::signing::BrokerSigner::from_seed(
        "broker-binding-v1",
        secret_32(env, "BINDING_SIGNING_SEED")?,
    );
    let mut required_scopes = Vec::new();
    let mut supported_contracts = Vec::new();
    let mut origins = Vec::new();
    for contract in &body.contracts {
        let adapter = match provider_google::adapter(contract) {
            Ok(adapter) => adapter,
            Err(_) => return json(&PublicError::broker(BrokerError::Brk108), 400),
        };
        required_scopes.push(adapter.required_claim.to_string());
        origins.push(adapter.origin.to_string());
        supported_contracts.push(broker_core::artifacts::SupportedContract {
            contract_id: contract.clone(),
            contract_hash: adapter.contract_hash.into(),
            observed_plugin_module_sha256: Some(adapter.implementation_hash.into()),
            attenuation_profiles: vec![],
            extensions: Default::default(),
        });
    }
    required_scopes.sort();
    required_scopes.dedup();
    if !required_scopes
        .iter()
        .all(|scope| live_scopes.iter().any(|actual| actual == scope))
    {
        return json(&PublicError::broker(BrokerError::Brk109), 400);
    }
    origins.sort();
    origins.dedup();
    supported_contracts.sort_by(|left, right| left.contract_hash.cmp(&right.contract_hash));
    let placeholder = broker_core::artifacts::SignatureEnvelope {
        alg: broker_core::artifacts::SignatureAlg::Ed25519,
        key_id: binding_signer.key_id().into(),
        value: String::new(),
    };
    let mut attestation = broker_core::artifacts::BindingAttestation {
        schema_version: "0.1".into(),
        critical_fields: vec![],
        org_id: session.org_id.clone(),
        principal: broker_core::artifacts::PrincipalRef {
            kind: broker_core::artifacts::PrincipalKind::Broker,
            id: "broker-workers-v1".into(),
        },
        issuer: "broker-workers-v1".into(),
        broker_key_id: binding_signer.key_id().into(),
        lane: EXECUTION_LANE.into(),
        authority_manifest_hash: Some(authority_manifest_hash.clone()),
        connection_ref: body.connection_ref.clone(),
        provider: "google".into(),
        account_commitment: broker_core::artifacts::CommitmentEnvelope {
            alg: broker_core::artifacts::CommitmentAlg::HmacSha256,
            key_id: "account-commitment-v1".into(),
            verification_tier: None,
            value: connection.account_commitment,
            extensions: Default::default(),
        },
        roles: std::collections::BTreeMap::from([(
            "outbound_auth.google_workspace_auth".into(),
            "oauth2.access_token".into(),
        )]),
        scope_alignment: broker_core::artifacts::ScopeAlignment {
            required_scopes,
            actual_scopes: live_scopes.clone(),
            satisfied: true,
            extensions: Default::default(),
        },
        supported_contracts,
        endpoint_origins: origins,
        revocation_epoch: connection.revocation_epoch,
        not_before: now_rfc3339(),
        observed_at: now_rfc3339(),
        expires_at: rfc3339_from_seconds(now_seconds() + 3600),
        signature: placeholder,
        extensions: std::collections::BTreeMap::from([
            (
                "bundle_id".into(),
                serde_json::Value::String(body.bundle_id.clone()),
            ),
            (
                "flow_ir_hash".into(),
                serde_json::Value::String(body.flow_ir_hash.clone()),
            ),
            (
                "binding_lock_hash".into(),
                serde_json::Value::String(body.binding_lock_hash.clone()),
            ),
            (
                "flow_id".into(),
                serde_json::Value::String(body.flow_id.clone()),
            ),
            (
                "deployment_id".into(),
                serde_json::Value::String(session.deployment_id.clone()),
            ),
        ]),
    };
    let unsigned =
        broker_core::canonical::from_serde(&attestation, broker_core::artifacts::BINDING_MAX)
            .map_err(|_| worker_rust_error("binding"))?
            .into_bytes();
    attestation.signature = binding_signer
        .sign_json(broker_core::signing::BINDING_DOMAIN, &unsigned)
        .map_err(|_| worker_rust_error("binding"))?;
    let attestation_json =
        broker_core::canonical::from_serde(&attestation, broker_core::artifacts::BINDING_MAX)
            .map_err(|_| worker_rust_error("binding"))?
            .into_bytes();
    let parsed: broker_core::artifacts::ParsedArtifact<broker_core::artifacts::BindingAttestation> =
        broker_core::artifacts::parse(&attestation_json)
            .map_err(|_| worker_rust_error("binding"))?;
    binding_signer
        .verifying_key()
        .verify_json(
            broker_core::signing::BINDING_DOMAIN,
            &attestation_json,
            &parsed.view.signature,
        )
        .map_err(|_| worker_rust_error("binding"))?;
    let contract_set_json =
        serde_json::to_string(&body.contracts).map_err(|_| worker_rust_error("binding"))?;
    db.prepare(
        "INSERT INTO bindings \
         (org_id, binding_ref, connection_ref, deployment_id, bundle_id, flow_ir_hash, \
          binding_lock_hash, flow_id, contract_set_json, flow_ir_json, \
          authority_manifest_json, authority_manifest_hash, attestation_json, revoked) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
    )
    .bind(&[
        JsValue::from_str(&session.org_id),
        JsValue::from_str(&binding_ref),
        JsValue::from_str(&body.connection_ref),
        JsValue::from_str(&session.deployment_id),
        JsValue::from_str(&body.bundle_id),
        JsValue::from_str(&body.flow_ir_hash),
        JsValue::from_str(&body.binding_lock_hash),
        JsValue::from_str(&body.flow_id),
        JsValue::from_str(&contract_set_json),
        JsValue::from_str(&body.flow_ir_json),
        JsValue::from_str(&body.authority_manifest_json),
        JsValue::from_str(&authority_manifest_hash),
        JsValue::from_str(&String::from_utf8_lossy(&attestation_json)),
    ])?
    .run()
    .await?;
    json(
        &serde_json::json!({
            "binding_ref": binding_ref,
            "attestation": attestation
        }),
        201,
    )
}

async fn invoke(request: &mut Request, env: &Env) -> worker::Result<Response> {
    let service_auth = request
        .headers()
        .get("x-lattice-service-auth")
        .ok()
        .flatten();
    let expected = env
        .secret("INVOKE_SERVICE_AUTH")
        .ok()
        .map(|value| value.to_string());
    let service_auth_ok = service_auth
        .zip(expected)
        .is_some_and(|(actual, expected)| {
            let actual: [u8; 32] = Sha256::digest(actual.as_bytes()).into();
            let expected: [u8; 32] = Sha256::digest(expected.as_bytes()).into();
            constant_time_matches(&expected, &actual)
        });
    if !service_auth_ok {
        return json(&PublicError::broker(BrokerError::Brk101), 401);
    }
    let exact_body = match bounded_body(request, MAX_INVOKE_BODY).await {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    let session = match authenticate(request, env, &exact_body).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let body: crate::protocol::InvokeRequest = match parse_json(&exact_body) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::invalid(), 400),
    };
    if body.session_ref != session.session_ref
        || body.grant_ref.is_empty()
        || body.activation_ordinal == 0
        || !valid_sha256(&body.flow_ir_hash)
        || !valid_sha256(&body.binding_lock_hash)
    {
        return json(&PublicError::broker(BrokerError::Brk107), 400);
    }
    let db = env.d1("BROKER_DB")?;
    #[derive(Deserialize)]
    struct GrantRow {
        canonical_grant: String,
        attestation_json: String,
        binding_bundle_id: String,
        binding_flow_ir_hash: String,
        binding_lock_hash: String,
        binding_flow_id: String,
        contract_set_json: String,
        connection_ref: String,
        account_commitment: String,
        refresh_do_route: String,
        actual_scopes_json: String,
        revocation_epoch: u64,
        connection_status: String,
    }
    let row = db
        .prepare(
            "SELECT g.canonical_grant, b.attestation_json, b.bundle_id AS binding_bundle_id, \
                    b.flow_ir_hash AS binding_flow_ir_hash, b.binding_lock_hash, \
                    b.flow_id AS binding_flow_id, b.contract_set_json, c.connection_ref, \
                    c.account_commitment, c.refresh_do_route, c.actual_scopes_json, \
                    c.revocation_epoch, c.status AS connection_status \
             FROM grants g \
             JOIN bindings b ON b.org_id = g.org_id AND b.binding_ref = g.binding_ref AND b.revoked = 0 \
             JOIN connections c ON c.org_id = b.org_id AND c.connection_ref = b.connection_ref \
             WHERE g.org_id = ? AND g.grant_ref = ? AND g.revoked = 0 AND g.expires_at > ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&body.grant_ref),
            JsValue::from_f64(now_seconds() as f64),
        ])?
        .first::<GrantRow>(None)
        .await?;
    let Some(row) = row.filter(|row| row.connection_status == "active") else {
        return json(&PublicError::broker(BrokerError::Brk103), 404);
    };
    if row.binding_bundle_id != body.bundle_id
        || row.binding_flow_ir_hash != body.flow_ir_hash
        || row.binding_lock_hash != body.binding_lock_hash
        || row.binding_flow_id != body.flow_id
    {
        return json(&PublicError::broker(BrokerError::Brk107), 403);
    }
    let binding: broker_core::artifacts::ParsedArtifact<
        broker_core::artifacts::BindingAttestation,
    > = match broker_core::artifacts::parse(row.attestation_json.as_bytes()) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    let binding_signer = broker_core::signing::BrokerSigner::from_seed(
        "broker-binding-v1",
        secret_32(env, "BINDING_SIGNING_SEED")?,
    );
    if binding_signer
        .verifying_key()
        .verify_json(
            broker_core::signing::BINDING_DOMAIN,
            row.attestation_json.as_bytes(),
            &binding.view.signature,
        )
        .is_err()
    {
        return json(&PublicError::unavailable(), 503);
    }
    let installed_contracts: Vec<String> = match serde_json::from_str(&row.contract_set_json) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    let grant = match broker_core::grant::ExecutionGrantRecord::parse_canonical(
        row.canonical_grant.as_bytes(),
    ) {
        Ok(grant) => grant,
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    if !installed_contracts
        .iter()
        .any(|contract| contract == &grant.grant().operation_contract)
        || binding.view.org_id != session.org_id
        || binding.view.connection_ref != row.connection_ref
        || binding.view.account_commitment.value != row.account_commitment
        || binding.view.revocation_epoch != row.revocation_epoch
    {
        return json(&PublicError::broker(BrokerError::Brk109), 403);
    }
    let authority = broker_core::grant::bootstrap_host_authority();
    let scope = match authority.trusted_scope(
        session.org_id.clone(),
        session.deployment_id.clone(),
        body.bundle_id.clone(),
        body.flow_ir_hash.clone(),
        body.binding_lock_hash.clone(),
        body.flow_id.clone(),
        body.node_id.clone(),
        body.node_alias.clone(),
        body.run_id.clone(),
    ) {
        Ok(scope) => scope,
        Err(error) => return json(&PublicError::broker(error), 400),
    };
    // This evidence is derived only after Ed25519 verification and JTI
    // reservation; no caller-supplied bearer proof enters the engine.
    let verified_pop_evidence = Sha256::digest(
        [
            b"lattice.verified-request-pop.v1".as_slice(),
            session.session_ref.as_bytes(),
            session.pop_public_key.as_bytes(),
        ]
        .concat(),
    )
    .to_vec();
    let pop = match authority.pop_session(
        grant.grant().channel_binding.method.clone(),
        session.pop_key_thumbprint.clone(),
        session.session_ref.clone(),
        verified_pop_evidence.clone(),
    ) {
        Ok(pop) => pop,
        Err(error) => return json(&PublicError::broker(error), 401),
    };
    let verifier = match broker_core::grant::ConfiguredPopVerifier::new(verified_pop_evidence) {
        Ok(verifier) => verifier,
        Err(error) => return json(&PublicError::broker(error), 401),
    };
    let grant_view = grant.grant();
    let adapter = match provider_google::adapter(&grant_view.operation_contract) {
        Ok(adapter) if adapter.contract_hash == grant_view.contract_hash => adapter,
        _ => return json(&PublicError::broker(BrokerError::Brk108), 403),
    };
    let descriptor_bytes = adapter.descriptor;
    let semantic_slot = adapter.semantic_effect_slot;
    let authority_facts = adapter.authority_facts_jcs;
    let provider_origin = adapter.origin;
    let expected_effect = match broker_core::effect_id::derive(
        &body.run_id,
        &body.node_id,
        body.activation_ordinal,
        semantic_slot,
    ) {
        Ok(effect) => effect,
        Err(error) => return json(&PublicError::broker(error), 400),
    };
    if expected_effect != body.logical_effect_id {
        return json(&PublicError::broker(BrokerError::Brk107), 403);
    }
    let canonical_input = match broker_core::canonical::from_serde(
        &body.input,
        broker_core::canonical::MAX_OPERATION_BYTES,
    ) {
        Ok(value) => value.into_bytes(),
        Err(error) => return json(&PublicError::broker(error), 400),
    };
    let descriptor: connector_spec::BrokerDispatchDescriptor =
        match serde_json::from_slice(descriptor_bytes) {
            Ok(value) => value,
            Err(_) => return json(&PublicError::unavailable(), 503),
        };
    if !connector_spec::validate_broker_dispatch_descriptor(&descriptor)
        || descriptor.request_plan.origin != provider_origin
    {
        return json(&PublicError::unavailable(), 503);
    }
    let (approved_implementation, approved_module_hash, approved_policy_hash) =
        match broker_host::descriptor_implementation_facts(&descriptor) {
            Ok(value) => value,
            Err(_) => return json(&PublicError::unavailable(), 503),
        };
    let actual_scopes = match serde_json::from_str::<Vec<String>>(&row.actual_scopes_json) {
        Ok(value) => value.into_iter().collect(),
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    let live = broker_core::custodian::ConnectionMetadata {
        connection_ref: row.connection_ref.clone(),
        provider: binding.view.provider.clone(),
        account_commitment: binding.view.account_commitment.clone(),
        roles: binding.view.roles.clone(),
        scopes: actual_scopes,
        revocation_epoch: row.revocation_epoch,
    };
    let trust = broker_core::engine::StaticTrustRegistry {
        approval: broker_core::engine::ImplementationApproval {
            contract_hash: grant_view.contract_hash.clone(),
            implementation: approved_implementation.clone(),
            plugin_module_sha256: approved_module_hash.clone(),
            trust_tier: broker_core::artifacts::PluginTrustTier::LatticeFirstParty,
            policy_hash: approved_policy_hash.clone(),
        },
        contract_id: grant_view.operation_contract.clone(),
        binding_issuer: binding.view.issuer.clone(),
        binding_key_id: binding.view.broker_key_id.clone(),
        verifier: binding_signer.verifying_key(),
    };
    if let Err(error) = broker_core::engine::validate_admission(
        &grant,
        &scope,
        &pop,
        &verifier,
        &binding,
        &live,
        &trust,
        &now_rfc3339(),
    ) {
        return json(&PublicError::broker(error), 403);
    }
    let host_registry = match provider_google::host_registry("2026-07-21T00:00:00Z") {
        Ok(registry) => registry,
        Err(error) => return json(&PublicError::broker(error), 503),
    };
    let template = match broker_host::descriptor_plan_template(
        &descriptor,
        &canonical_input,
        authority_facts,
        &host_registry,
        &body.logical_effect_id,
        &now_rfc3339(),
    ) {
        Ok(template) => template,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk301), 400),
    };
    let facts = match broker_core::canonical::canonicalize_bounded(
        authority_facts,
        broker_core::canonical::MAX_OPERATION_BYTES,
    ) {
        Ok(value) => value,
        Err(_) => return json(&PublicError::unavailable(), 503),
    };
    let plan_hash = format!("sha256:{}", hex::encode(Sha256::digest(&template)));
    let facts_hash = format!("sha256:{}", hex::encode(Sha256::digest(facts.as_bytes())));
    let reservation_key = broker_core::ledger::ReservationKey {
        org_id: session.org_id.clone(),
        deployment_id: session.deployment_id.clone(),
        flow_ir_hash: body.flow_ir_hash.clone(),
        run_id: body.run_id.clone(),
        node_id: body.node_id.clone(),
        logical_effect_id: body.logical_effect_id.clone(),
        operation_contract: grant_view.operation_contract.clone(),
        connection_ref: row.connection_ref.clone(),
    };
    let ledger_authority = LedgerAuthority {
        org_id: session.org_id.clone(),
        grant_ref: body.grant_ref.clone(),
    };
    let route = ledger_authority
        .route_name()
        .map_err(|_| worker_rust_error("ledger"))?;
    let reserve: LedgerReply = match do_request(
        env,
        "BROKER_LEDGER_DO",
        &route,
        &LedgerEnvelope {
            authority: ledger_authority.clone(),
            command: LedgerCommand::Reserve {
                request: broker_core::ledger::ReserveRequest {
                    key: reservation_key.clone(),
                    canonical_input: canonical_input.clone(),
                    max_logical_calls: grant_view.budgets.logical_calls,
                    max_dispatch_attempts: grant_view.budgets.dispatch_attempts_per_call,
                    lease_deadline: now_seconds() + crate::protocol::INVOCATION_LEASE_SECONDS,
                },
                now: now_seconds(),
            },
        },
    )
    .await
    {
        Ok(reply) => reply,
        Err(_) => return json(&PublicError::broker(BrokerError::Brk203), 409),
    };
    let snapshot = match reserve.snapshot {
        Some(snapshot) => snapshot,
        None => return json(&PublicError::unavailable(), 503),
    };
    if reserve.acquired == Some(false) {
        use broker_core::ledger::{InvocationState, TerminalOutcome};
        let now = now_seconds();
        let issued_snapshot = match &snapshot.state {
            InvocationState::ReceiptIssued { .. } => snapshot.clone(),
            InvocationState::Reserved | InvocationState::Planned(_)
                if now >= snapshot.lease_deadline =>
            {
                let released: LedgerReply = do_request(
                    env,
                    "BROKER_LEDGER_DO",
                    &route,
                    &LedgerEnvelope {
                        authority: ledger_authority.clone(),
                        command: LedgerCommand::Release {
                            key: reservation_key.clone(),
                            lease_token: snapshot.lease_token,
                            now,
                            expired: true,
                        },
                    },
                )
                .await?;
                let released = released
                    .snapshot
                    .ok_or_else(|| worker_rust_error("ledger"))?;
                let receipt = build_receipt(
                    env,
                    &grant,
                    &body.logical_effect_id,
                    &released,
                    &approved_policy_hash,
                    &approved_module_hash,
                    broker_core::artifacts::Outcome::Rejected,
                    None,
                    None,
                    &canonical_input,
                    &[],
                    None,
                    false,
                )?;
                do_request::<LedgerReply>(
                    env,
                    "BROKER_LEDGER_DO",
                    &route,
                    &LedgerEnvelope {
                        authority: ledger_authority.clone(),
                        command: LedgerCommand::IssueReleasedReceipt {
                            key: reservation_key.clone(),
                            receipt: receipt.canonical_receipt,
                        },
                    },
                )
                .await?
                .snapshot
                .ok_or_else(|| worker_rust_error("ledger"))?
            }
            InvocationState::Dispatched { .. } => {
                let receipt = build_receipt(
                    env,
                    &grant,
                    &body.logical_effect_id,
                    &snapshot,
                    &approved_policy_hash,
                    &approved_module_hash,
                    broker_core::artifacts::Outcome::Ambiguous,
                    Some(plan_hash.clone()),
                    Some(facts_hash.clone()),
                    &canonical_input,
                    &[],
                    None,
                    true,
                )?;
                do_request::<LedgerReply>(
                    env,
                    "BROKER_LEDGER_DO",
                    &route,
                    &LedgerEnvelope {
                        authority: ledger_authority.clone(),
                        command: LedgerCommand::Finish {
                            key: reservation_key.clone(),
                            outcome: TerminalOutcome::Ambiguous,
                            receipt: receipt.canonical_receipt,
                            response_projection: Vec::new(),
                            provider_request_id: None,
                        },
                    },
                )
                .await?;
                do_request::<LedgerReply>(
                    env,
                    "BROKER_LEDGER_DO",
                    &route,
                    &LedgerEnvelope {
                        authority: ledger_authority.clone(),
                        command: LedgerCommand::IssueReceipt {
                            key: reservation_key.clone(),
                        },
                    },
                )
                .await?
                .snapshot
                .ok_or_else(|| worker_rust_error("ledger"))?
            }
            InvocationState::Terminal(
                TerminalOutcome::ReleasedExpired | TerminalOutcome::ReleasedCancelled,
            ) => {
                let receipt = build_receipt(
                    env,
                    &grant,
                    &body.logical_effect_id,
                    &snapshot,
                    &approved_policy_hash,
                    &approved_module_hash,
                    broker_core::artifacts::Outcome::Rejected,
                    None,
                    None,
                    &canonical_input,
                    &[],
                    None,
                    false,
                )?;
                do_request::<LedgerReply>(
                    env,
                    "BROKER_LEDGER_DO",
                    &route,
                    &LedgerEnvelope {
                        authority: ledger_authority.clone(),
                        command: LedgerCommand::IssueReleasedReceipt {
                            key: reservation_key.clone(),
                            receipt: receipt.canonical_receipt,
                        },
                    },
                )
                .await?
                .snapshot
                .ok_or_else(|| worker_rust_error("ledger"))?
            }
            InvocationState::Terminal(_) => do_request::<LedgerReply>(
                env,
                "BROKER_LEDGER_DO",
                &route,
                &LedgerEnvelope {
                    authority: ledger_authority.clone(),
                    command: LedgerCommand::IssueReceipt {
                        key: reservation_key.clone(),
                    },
                },
            )
            .await?
            .snapshot
            .ok_or_else(|| worker_rust_error("ledger"))?,
            _ => return json(&PublicError::broker(BrokerError::Brk204), 409),
        };
        let receipt = match issued_snapshot.state {
            InvocationState::ReceiptIssued { receipt, .. } => receipt,
            _ => return json(&PublicError::unavailable(), 503),
        };
        let receipt_ref = receipt_ref(
            &session.org_id,
            &session.deployment_id,
            &body.grant_ref,
            &reservation_key,
        )?;
        project_receipt_index(
            &db,
            env,
            &session.org_id,
            &session.deployment_id,
            &receipt_ref,
            &body.grant_ref,
            &reservation_key,
            &receipt,
        )
        .await?;
        let value = verified_receipt_value(env, &receipt)?;
        return json(
            &crate::protocol::InvokeResponse {
                receipt_ref,
                receipt: value,
                redelivery: true,
            },
            200,
        );
    }
    let planned: LedgerReply = do_request(
        env,
        "BROKER_LEDGER_DO",
        &route,
        &LedgerEnvelope {
            authority: ledger_authority.clone(),
            command: LedgerCommand::Plan {
                key: reservation_key.clone(),
                lease_token: snapshot.lease_token,
                now: now_seconds(),
                data: broker_core::ledger::PlannedData {
                    request_plan_hash: plan_hash.clone(),
                    authority_facts_hash: facts_hash.clone(),
                    implementation: approved_implementation,
                    endpoint: provider_origin.into(),
                    next_attempt: 0,
                },
            },
        },
    )
    .await?;
    let _ = planned;
    let (access, acquired_epoch) = match acquire_access_token(env, &row.refresh_do_route).await {
        Ok(token) => token,
        Err(error) => {
            let _: LedgerReply = do_request(
                env,
                "BROKER_LEDGER_DO",
                &route,
                &LedgerEnvelope {
                    authority: ledger_authority,
                    command: LedgerCommand::Release {
                        key: reservation_key,
                        lease_token: snapshot.lease_token,
                        now: now_seconds(),
                        expired: false,
                    },
                },
            )
            .await?;
            return json(&PublicError::broker(error), 503);
        }
    };
    // Reconcile custody and D1 immediately before the dispatched CAS. A
    // concurrent revoke/epoch or scope change therefore prevents provider I/O.
    let live: RefreshReply = do_request(
        env,
        "CONNECTION_REFRESH_DO",
        &row.refresh_do_route,
        &RefreshCommand::Metadata,
    )
    .await?;
    let (live_account, live_scopes, live_epoch) = match live {
        RefreshReply::Metadata {
            org_id,
            connection_ref,
            account_commitment,
            effective_scopes,
            revocation_epoch,
            revoked: false,
        } if org_id == session.org_id && connection_ref == row.connection_ref => {
            (account_commitment, effective_scopes, revocation_epoch)
        }
        _ => {
            let _: LedgerReply = do_request(
                env,
                "BROKER_LEDGER_DO",
                &route,
                &LedgerEnvelope {
                    authority: ledger_authority,
                    command: LedgerCommand::Release {
                        key: reservation_key,
                        lease_token: snapshot.lease_token,
                        now: now_seconds(),
                        expired: false,
                    },
                },
            )
            .await?;
            return json(&PublicError::broker(BrokerError::Brk106), 409);
        }
    };
    #[derive(Deserialize)]
    struct LiveConnectionRow {
        status: String,
        account_commitment: String,
        actual_scopes_json: String,
        revocation_epoch: u64,
    }
    let fresh = db
        .prepare(
            "SELECT status, account_commitment, actual_scopes_json, revocation_epoch \
             FROM connections WHERE org_id = ? AND connection_ref = ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&row.connection_ref),
        ])?
        .first::<LiveConnectionRow>(None)
        .await?;
    let live_scope_set = live_scopes
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let expected_scope_set = serde_json::from_str::<Vec<String>>(&row.actual_scopes_json)
        .map_err(|_| worker_rust_error("scopes"))?
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let reconciled = fresh.is_some_and(|fresh| {
        fresh.status == "active"
            && fresh.account_commitment == row.account_commitment
            && fresh.account_commitment == live_account
            && fresh.revocation_epoch == row.revocation_epoch
            && fresh.revocation_epoch == acquired_epoch
            && fresh.revocation_epoch == live_epoch
            && serde_json::from_str::<Vec<String>>(&fresh.actual_scopes_json)
                .map(|scopes| {
                    scopes
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        == expected_scope_set
                })
                .unwrap_or(false)
            && live_scope_set == expected_scope_set
    });
    if !reconciled {
        let _: LedgerReply = do_request(
            env,
            "BROKER_LEDGER_DO",
            &route,
            &LedgerEnvelope {
                authority: ledger_authority,
                command: LedgerCommand::Release {
                    key: reservation_key,
                    lease_token: snapshot.lease_token,
                    now: now_seconds(),
                    expired: false,
                },
            },
        )
        .await?;
        return json(&PublicError::broker(BrokerError::Brk106), 409);
    }
    let pre_dispatch_live = broker_core::custodian::ConnectionMetadata {
        connection_ref: row.connection_ref.clone(),
        provider: binding.view.provider.clone(),
        account_commitment: binding.view.account_commitment.clone(),
        roles: binding.view.roles.clone(),
        scopes: live_scope_set,
        revocation_epoch: live_epoch,
    };
    if broker_core::engine::validate_admission(
        &grant,
        &scope,
        &pop,
        &verifier,
        &binding,
        &pre_dispatch_live,
        &trust,
        &now_rfc3339(),
    )
    .is_err()
    {
        let _: LedgerReply = do_request(
            env,
            "BROKER_LEDGER_DO",
            &route,
            &LedgerEnvelope {
                authority: ledger_authority,
                command: LedgerCommand::Release {
                    key: reservation_key,
                    lease_token: snapshot.lease_token,
                    now: now_seconds(),
                    expired: false,
                },
            },
        )
        .await?;
        return json(&PublicError::broker(BrokerError::Brk109), 403);
    }
    let dispatched: LedgerReply = do_request(
        env,
        "BROKER_LEDGER_DO",
        &route,
        &LedgerEnvelope {
            authority: ledger_authority.clone(),
            command: LedgerCommand::MarkDispatched {
                key: reservation_key.clone(),
                lease_token: snapshot.lease_token,
                now: now_seconds(),
            },
        },
    )
    .await?;
    let dispatched_snapshot = dispatched
        .snapshot
        .ok_or_else(|| worker_rust_error("ledger"))?;
    let (response_projection, provider_request_id, outcome) =
        match dispatch_provider(env, &template, &descriptor, &access).await {
            Ok((projection, request_id)) => (
                projection,
                request_id,
                broker_core::artifacts::Outcome::Confirmed,
            ),
            Err(ProviderDispatchFailure::Definite) => {
                (Vec::new(), None, broker_core::artifacts::Outcome::Failed)
            }
            Err(ProviderDispatchFailure::Ambiguous) => {
                (Vec::new(), None, broker_core::artifacts::Outcome::Ambiguous)
            }
        };
    let issued = build_receipt(
        env,
        &grant,
        &body.logical_effect_id,
        &dispatched_snapshot,
        &approved_policy_hash,
        &approved_module_hash,
        outcome.clone(),
        Some(plan_hash),
        Some(facts_hash),
        &canonical_input,
        &response_projection,
        provider_request_id.clone(),
        true,
    )?;
    if verified_receipt_value(env, &issued.canonical_receipt).is_err() {
        return json(&PublicError::unavailable(), 503);
    }
    let terminal = match outcome {
        broker_core::artifacts::Outcome::Confirmed => {
            broker_core::ledger::TerminalOutcome::Confirmed
        }
        broker_core::artifacts::Outcome::Failed => broker_core::ledger::TerminalOutcome::Failed,
        _ => broker_core::ledger::TerminalOutcome::Ambiguous,
    };
    let _: LedgerReply = do_request(
        env,
        "BROKER_LEDGER_DO",
        &route,
        &LedgerEnvelope {
            authority: ledger_authority.clone(),
            command: LedgerCommand::Finish {
                key: reservation_key.clone(),
                outcome: terminal,
                receipt: issued.canonical_receipt.clone(),
                response_projection,
                provider_request_id,
            },
        },
    )
    .await?;
    let _: LedgerReply = do_request(
        env,
        "BROKER_LEDGER_DO",
        &route,
        &LedgerEnvelope {
            authority: ledger_authority,
            command: LedgerCommand::IssueReceipt {
                key: reservation_key.clone(),
            },
        },
    )
    .await?;
    let receipt_ref = receipt_ref(
        &session.org_id,
        &session.deployment_id,
        &body.grant_ref,
        &reservation_key,
    )?;
    project_receipt_index(
        &db,
        env,
        &session.org_id,
        &session.deployment_id,
        &receipt_ref,
        &body.grant_ref,
        &reservation_key,
        &issued.canonical_receipt,
    )
    .await?;
    let receipt =
        serde_json::to_value(&issued.receipt).map_err(|_| worker_rust_error("receipt"))?;
    json(
        &crate::protocol::InvokeResponse {
            receipt_ref,
            receipt,
            redelivery: false,
        },
        200,
    )
}

async fn receipt_route(request: &Request, env: &Env) -> worker::Result<Response> {
    let session = match authenticate(request, env, &[]).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    if request.method() != Method::Get {
        return json(&PublicError::invalid(), 405);
    }
    let receipt_ref = request
        .path()
        .trim_start_matches("/v1/receipts/")
        .to_string();
    #[derive(Deserialize)]
    struct ReceiptRow {
        receipt_json: String,
    }
    let row = env
        .d1("BROKER_DB")?
        .prepare(
            "SELECT receipt_json FROM receipts \
             WHERE org_id = ? AND deployment_id = ? AND receipt_ref = ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(&session.org_id),
            JsValue::from_str(&session.deployment_id),
            JsValue::from_str(&receipt_ref),
        ])?
        .first::<ReceiptRow>(None)
        .await?;
    match row {
        Some(row) => match verified_receipt_value(env, row.receipt_json.as_bytes()) {
            Ok(receipt) => json(
                &serde_json::json!({"receipt_ref":receipt_ref,"receipt":receipt}),
                200,
            ),
            Err(_) => json(&PublicError::unavailable(), 503),
        },
        None => json(&PublicError::broker(BrokerError::Brk103), 404),
    }
}

struct WorkerClock;

impl broker_core::grant::Clock for WorkerClock {
    fn now_rfc3339(&self) -> String {
        now_rfc3339()
    }

    fn monotonic_seconds(&self) -> i64 {
        now_seconds()
    }
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = globalThis, js_name = setTimeout)]
    fn set_timeout(callback: &JsValue, milliseconds: i32) -> i32;
}

async fn sleep_ms(milliseconds: i32) -> worker::Result<()> {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let callback = Closure::once_into_js(move || {
            let _ = resolve.call0(&JsValue::UNDEFINED);
        });
        set_timeout(&callback, milliseconds);
    });
    wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .map(|_| ())
        .map_err(|_| worker_rust_error("refresh wait"))
}

async fn acquire_access_token(
    env: &Env,
    route: &str,
) -> Result<(crate::refresh::SecretBytes, u64), BrokerError> {
    for _ in 0..32 {
        let reply: RefreshReply = do_request(
            env,
            "CONNECTION_REFRESH_DO",
            route,
            &RefreshCommand::Acquire {
                now: now_seconds(),
                lease_id: opaque_id("refresh_lease_").map_err(|_| BrokerError::Brk401)?,
            },
        )
        .await
        .map_err(|_| BrokerError::Brk401)?;
        match reply {
            RefreshReply::Acquired {
                result:
                    AcquireResult::Ready {
                        access_token,
                        revocation_epoch,
                    },
            } => return Ok((access_token, revocation_epoch)),
            RefreshReply::Acquired {
                result:
                    AcquireResult::Refresh {
                        lease_id,
                        expected_epoch,
                        refresh_token,
                    },
            } => {
                let service = env
                    .service(provider_google::TOKEN_SERVICE_BINDING)
                    .map_err(|_| BrokerError::Brk401)?;
                let payload = serde_json::json!({
                    "grant_type": "refresh_token",
                    "refresh_token": URL_SAFE_NO_PAD.encode(refresh_token.expose_to_internal_binding())
                });
                // Ten seconds is a hard bound and is strictly shorter than
                // the fifteen-second refresh lease.
                let result = match service_json_timeout(
                    &service,
                    "http://token.internal/refresh",
                    &payload,
                    10_000,
                )
                .await
                {
                    Ok(mut response) if response.status_code() == 200 => {
                        #[derive(Deserialize)]
                        #[serde(deny_unknown_fields)]
                        struct TokenRefresh {
                            access_token: String,
                            expires_in: u64,
                            scopes: Vec<String>,
                        }
                        match bounded_response_json::<TokenRefresh>(
                            &mut response,
                            crate::protocol::MAX_PROVIDER_RESPONSE,
                        )
                        .await
                        {
                            Ok(value) if !value.access_token.is_empty() => RefreshResult::Success {
                                access_token: crate::refresh::SecretBytes::new(
                                    value.access_token.into_bytes(),
                                ),
                                expires_in: value.expires_in,
                                scopes: value.scopes.into_iter().collect(),
                            },
                            _ => RefreshResult::Unavailable,
                        }
                    }
                    Ok(response) if response.status_code() == 400 => RefreshResult::InvalidGrant,
                    _ => RefreshResult::Unavailable,
                };
                let completed: RefreshReply = do_request(
                    env,
                    "CONNECTION_REFRESH_DO",
                    route,
                    &RefreshCommand::Complete {
                        lease_id,
                        expected_epoch,
                        now: now_seconds(),
                        result,
                    },
                )
                .await
                .map_err(|_| BrokerError::Brk401)?;
                match completed {
                    RefreshReply::Completed {
                        result: CompleteResult::Ready { .. },
                    } => continue,
                    RefreshReply::Completed {
                        result: CompleteResult::Blocked { .. },
                    } => return Err(BrokerError::Brk106),
                    _ => return Err(BrokerError::Brk401),
                }
            }
            RefreshReply::Acquired {
                result: AcquireResult::Waiting { retry_after_ms },
            } => {
                sleep_ms(i32::try_from(retry_after_ms.min(100)).unwrap_or(100))
                    .await
                    .map_err(|_| BrokerError::Brk401)?;
                continue;
            }
            RefreshReply::Error { code } if code == "BRK106" => return Err(BrokerError::Brk106),
            _ => return Err(BrokerError::Brk401),
        }
    }
    Err(BrokerError::Brk401)
}

enum ProviderDispatchFailure {
    Definite,
    Ambiguous,
}

impl From<worker::Error> for ProviderDispatchFailure {
    fn from(_: worker::Error) -> Self {
        Self::Ambiguous
    }
}

async fn dispatch_provider(
    env: &Env,
    template: &[u8],
    descriptor: &connector_spec::BrokerDispatchDescriptor,
    access: &crate::refresh::SecretBytes,
) -> Result<(Vec<u8>, Option<String>), ProviderDispatchFailure> {
    let plan: serde_json::Value =
        serde_json::from_slice(template).map_err(|_| worker_rust_error("plan"))?;
    let path = plan
        .get("path")
        .and_then(serde_json::Value::as_str)
        .filter(|path| path.starts_with('/'))
        .ok_or_else(|| worker_rust_error("plan"))?;
    let mut url = worker::Url::parse(&format!("http://provider.internal{path}"))
        .map_err(|_| ProviderDispatchFailure::Ambiguous)?;
    if let Some(query) = plan.get("query").and_then(serde_json::Value::as_object) {
        let mut pairs = url.query_pairs_mut();
        for (name, value) in query {
            let value = value.as_str().ok_or_else(|| worker_rust_error("plan"))?;
            pairs.append_pair(name, value);
        }
    }
    let method = match plan.get("method").and_then(serde_json::Value::as_str) {
        Some("GET") => Method::Get,
        Some("POST") => Method::Post,
        Some("PUT") => Method::Put,
        Some("PATCH") => Method::Patch,
        Some("DELETE") => Method::Delete,
        _ => return Err(worker_rust_error("plan").into()),
    };
    let headers = plan
        .get("headers")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| worker_rust_error("plan"))?;
    let header_value = |name: &str| {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .and_then(|(_, value)| value.as_str())
    };
    if header_value("accept") != Some(JSON_CONTENT_TYPE)
        || header_value("content-type") != Some(JSON_CONTENT_TYPE)
        || header_value("authorization").is_some()
    {
        return Err(worker_rust_error("plan").into());
    }
    let body = serde_json::to_string(plan.get("body").unwrap_or(&serde_json::Value::Null))
        .map_err(|_| worker_rust_error("plan"))?;
    let mut init = RequestInit::new();
    init.with_method(method);
    init.with_body(Some(JsString::from(body).into()));
    let mut request = Request::new_with_init(url.as_str(), &init)?;
    for (name, value) in headers {
        let value = value.as_str().ok_or_else(|| worker_rust_error("plan"))?;
        request.headers_mut()?.set(name, value)?;
    }
    request.headers_mut()?.set(
        "authorization",
        &format!(
            "Bearer {}",
            String::from_utf8_lossy(access.expose_to_internal_binding())
        ),
    )?;
    let service = env.service(provider_google::PROVIDER_SERVICE_BINDING)?;
    let mut response = service.fetch_request(request).await?;
    let request_id = response
        .headers()
        .get("x-request-id")?
        .filter(|value| value.len() <= 128 && value.is_ascii());
    if !(200..300).contains(&response.status_code()) {
        return Err(if (400..500).contains(&response.status_code()) {
            ProviderDispatchFailure::Definite
        } else {
            ProviderDispatchFailure::Ambiguous
        });
    }
    let bytes =
        bounded_response_bytes(&mut response, crate::protocol::MAX_PROVIDER_RESPONSE).await?;
    let projection = broker_host::descriptor_response_projection(descriptor, &bytes)
        .map_err(|_| worker_rust_error("provider"))?;
    Ok((projection, request_id))
}

fn secret_32(env: &Env, binding: &str) -> worker::Result<[u8; 32]> {
    let value = env.secret(binding)?.to_string();
    let bytes = hex::decode(value).map_err(|_| worker_rust_error("secret"))?;
    <[u8; 32]>::try_from(bytes).map_err(|_| worker_rust_error("secret"))
}

#[allow(clippy::too_many_arguments)]
fn build_receipt(
    env: &Env,
    grant: &broker_core::grant::ExecutionGrantRecord,
    logical_effect_id: &str,
    state: &broker_core::ledger::EntrySnapshot,
    policy_hash: &str,
    module_hash: &str,
    outcome: broker_core::artifacts::Outcome,
    request_plan_hash: Option<String>,
    authority_facts_hash: Option<String>,
    canonical_input: &[u8],
    response_projection: &[u8],
    provider_request_id: Option<String>,
    dispatched: bool,
) -> worker::Result<broker_core::receipt::IssuedReceipt> {
    let signer = broker_core::signing::BrokerSigner::from_seed(
        "broker-receipt-v1",
        secret_32(env, "RECEIPT_SIGNING_SEED")?,
    );
    let commitments = broker_core::commitment::CommitmentKey::new(
        "broker-commitment-v1",
        secret_32(env, "COMMITMENT_KEY")?,
    )
    .map_err(|_| worker_rust_error("commitment"))?;
    broker_core::receipt::ReceiptIssuer {
        signer: &signer,
        commitments: &commitments,
        clock: &WorkerClock,
        broker_principal_id: "broker-workers-v1",
    }
    .issue(broker_core::receipt::ReceiptIssueRequest {
        grant,
        logical_effect_id,
        state,
        implementation: broker_core::receipt::ReceiptImplementation {
            policy_hash,
            plugin_module_sha256: module_hash,
            plugin_trust_tier: broker_core::artifacts::PluginTrustTier::LatticeFirstParty,
        },
        outcome,
        request_plan_hash,
        authority_facts_hash,
        canonical_input,
        response_projection,
        provider_request_id,
        dispatched,
    })
    .map_err(|_| worker_rust_error("receipt"))
}

async fn project_receipt_index(
    db: &worker::D1Database,
    env: &Env,
    org_id: &str,
    deployment_id: &str,
    receipt_ref: &str,
    grant_ref: &str,
    reservation: &broker_core::ledger::ReservationKey,
    receipt: &[u8],
) -> worker::Result<()> {
    verified_receipt_value(env, receipt)?;
    let receipt_text = std::str::from_utf8(receipt).map_err(|_| worker_rust_error("receipt"))?;
    let reservation_hash = reservation_identity_hash(reservation)?;
    db.prepare(
        "INSERT OR IGNORE INTO receipts \
         (org_id, deployment_id, receipt_ref, grant_ref, reservation_identity_hash, receipt_json) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&[
        JsValue::from_str(org_id),
        JsValue::from_str(deployment_id),
        JsValue::from_str(receipt_ref),
        JsValue::from_str(grant_ref),
        JsValue::from_str(&reservation_hash),
        JsValue::from_str(receipt_text),
    ])?
    .run()
    .await?;
    #[derive(Deserialize)]
    struct IndexedReceipt {
        grant_ref: String,
        reservation_identity_hash: String,
        receipt_json: String,
    }
    let indexed = db
        .prepare(
            "SELECT grant_ref, reservation_identity_hash, receipt_json FROM receipts \
             WHERE org_id = ? AND deployment_id = ? AND receipt_ref = ? LIMIT 1",
        )
        .bind(&[
            JsValue::from_str(org_id),
            JsValue::from_str(deployment_id),
            JsValue::from_str(receipt_ref),
        ])?
        .first::<IndexedReceipt>(None)
        .await?
        .ok_or_else(|| worker_rust_error("receipt index"))?;
    if indexed.grant_ref != grant_ref
        || indexed.reservation_identity_hash != reservation_hash
        || !constant_time_bytes_equal(indexed.receipt_json.as_bytes(), receipt)
    {
        return Err(worker_rust_error("receipt index"));
    }
    Ok(())
}

fn constant_time_bytes_equal(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && bool::from(left.ct_eq(right))
}

fn verified_receipt_value(env: &Env, bytes: &[u8]) -> worker::Result<serde_json::Value> {
    let parsed: broker_core::artifacts::ParsedArtifact<broker_core::artifacts::InvocationReceipt> =
        broker_core::artifacts::parse(bytes).map_err(|_| worker_rust_error("receipt"))?;
    let signer = broker_core::signing::BrokerSigner::from_seed(
        "broker-receipt-v1",
        secret_32(env, "RECEIPT_SIGNING_SEED")?,
    );
    signer
        .verifying_key()
        .verify_json(
            broker_core::signing::RECEIPT_DOMAIN,
            bytes,
            &parsed.view.signature,
        )
        .map_err(|_| worker_rust_error("receipt"))?;
    parsed
        .view
        .validate_semantics()
        .map_err(|_| worker_rust_error("receipt"))?;
    serde_json::to_value(parsed.view).map_err(|_| worker_rust_error("receipt"))
}

fn reservation_identity_hash(
    reservation: &broker_core::ledger::ReservationKey,
) -> worker::Result<String> {
    let bytes = broker_core::canonical::from_serde(
        reservation,
        broker_core::canonical::MAX_OPERATION_BYTES,
    )
    .map_err(|_| worker_rust_error("receipt identity"))?;
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(bytes.as_bytes()))
    ))
}

fn receipt_ref(
    org_id: &str,
    deployment_id: &str,
    grant_ref: &str,
    reservation: &broker_core::ledger::ReservationKey,
) -> worker::Result<String> {
    let reservation_hash = reservation_identity_hash(reservation)?;
    let mut hash = Sha256::new();
    hash.update(b"lattice.receipt-ref.v2");
    for field in [org_id, deployment_id, grant_ref, reservation_hash.as_str()] {
        hash.update([0]);
        hash.update(field.as_bytes());
    }
    Ok(format!(
        "receipt_{}",
        URL_SAFE_NO_PAD.encode(hash.finalize())
    ))
}

fn now_rfc3339() -> String {
    rfc3339_from_seconds(now_seconds())
}

fn rfc3339_from_seconds(seconds: i64) -> String {
    js_sys::Date::new(&JsValue::from_f64(seconds as f64 * 1000.0))
        .to_iso_string()
        .as_string()
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".into())
        .replace(".000Z", "Z")
}

async fn bounded_body(request: &mut Request, max: usize) -> worker::Result<Vec<u8>> {
    let content_type = request.headers().get("content-type")?.unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(JSON_CONTENT_TYPE))
    {
        return Err(worker_rust_error("content type"));
    }
    if request
        .headers()
        .get("content-length")?
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > max)
    {
        return Err(worker_rust_error("body too large"));
    }
    use futures::TryStreamExt;
    let mut stream = request.stream()?;
    let mut bytes = Vec::with_capacity(max.min(4096));
    while let Some(chunk) = stream.try_next().await? {
        if chunk.len() > max.saturating_sub(bytes.len()) {
            return Err(worker_rust_error("body too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn reject_nonempty_body(request: &mut Request) -> worker::Result<()> {
    if request
        .headers()
        .get("content-length")?
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length != 0)
    {
        return Err(worker_rust_error("body forbidden"));
    }
    use futures::TryStreamExt;
    let mut stream = match request.stream() {
        Ok(stream) => stream,
        Err(worker::Error::RustError(_)) => return Ok(()),
        Err(error) => return Err(error),
    };
    if stream.try_next().await?.is_some() {
        return Err(worker_rust_error("body forbidden"));
    }
    Ok(())
}

fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> worker::Result<T> {
    serde_json::from_slice(bytes).map_err(|_| worker_rust_error("invalid json"))
}

async fn bounded_json<T: DeserializeOwned>(request: &mut Request, max: usize) -> worker::Result<T> {
    let bytes = bounded_body(request, max).await?;
    parse_json(&bytes)
}

async fn service_json_timeout(
    fetcher: &worker::Fetcher,
    url: &str,
    value: &impl Serialize,
    timeout_ms: i32,
) -> worker::Result<Response> {
    use futures::future::{Either, select};
    let fetch = Box::pin(service_json(fetcher, url, value));
    let timeout = Box::pin(sleep_ms(timeout_ms));
    match select(fetch, timeout).await {
        Either::Left((response, _)) => response,
        Either::Right((_timeout, _)) => Err(worker_rust_error("service timeout")),
    }
}

async fn service_json(
    fetcher: &worker::Fetcher,
    url: &str,
    value: &impl Serialize,
) -> worker::Result<Response> {
    let text = serde_json::to_string(value).map_err(|_| worker_rust_error("json"))?;
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(JsString::from(text).into()));
    let mut request = Request::new_with_init(url, &init)?;
    request
        .headers_mut()?
        .set("content-type", JSON_CONTENT_TYPE)?;
    fetcher.fetch_request(request).await
}

async fn do_request<T: DeserializeOwned>(
    env: &Env,
    binding: &str,
    route: &str,
    value: &impl Serialize,
) -> worker::Result<T> {
    let namespace = env.durable_object(binding)?;
    let id = namespace.id_from_name(route)?;
    let stub = id.get_stub()?;
    let text = serde_json::to_string(value).map_err(|_| worker_rust_error("json"))?;
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(JsString::from(text).into()));
    let mut request = Request::new_with_init("http://do.internal/", &init)?;
    request
        .headers_mut()?
        .set("content-type", JSON_CONTENT_TYPE)?;
    let mut response = stub.fetch_with_request(request).await?;
    if response.status_code() >= 400 {
        return Err(worker_rust_error("durable state"));
    }
    response.json().await
}

fn refresh_route(org_id: &str, connection_ref: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"lattice.connection-refresh.v1");
    hash.update([0]);
    hash.update(org_id.as_bytes());
    hash.update([0]);
    hash.update(connection_ref.as_bytes());
    format!("connection-{}", hex::encode(hash.finalize()))
}

fn opaque_id(prefix: &str) -> worker::Result<String> {
    let mut bytes = [0u8; 24];
    getrandom::getrandom(&mut bytes).map_err(|_| worker_rust_error("random"))?;
    Ok(format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn valid_sha256(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn now_seconds() -> i64 {
    (js_sys::Date::now() / 1000.0).floor() as i64
}

fn json(value: &impl Serialize, status: u16) -> worker::Result<Response> {
    Response::from_json(value).map(|response| response.with_status(status))
}

fn json_value(value: &impl Serialize, status: u16) -> Response {
    Response::from_json(value)
        .map(|response| response.with_status(status))
        .unwrap_or_else(|_| Response::error("broker unavailable", 503).expect("static response"))
}

fn worker_rust_error(_context: &str) -> worker::Error {
    worker::Error::RustError("broker unavailable".into())
}
