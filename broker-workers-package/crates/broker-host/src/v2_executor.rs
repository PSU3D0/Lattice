use std::{collections::BTreeMap, sync::Mutex};

use broker_core::{
    BrokerError,
    artifacts::SignatureEnvelope,
    credential::{
        ParsedV2, grant::ExecutionGrantV2, parse, receipt::InvocationReceiptV2,
        signing::INVOCATION_RECEIPT_DOMAIN,
    },
    signing::BrokerVerifyingKey,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{BrokerHostError, ExactGrantRefV2};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V2AttemptStage {
    PrePlanning,
    PostPlanningPreDispatch,
    Dispatch(u8),
}

#[derive(Clone)]
pub struct V2DispatchEvidence {
    pub grant: ParsedV2<ExecutionGrantV2>,
    pub canonical_grant: Vec<u8>,
    pub canonical_input_commitment: Value,
    pub implementations: BTreeMap<String, Value>,
    pub evaluator_implementations: BTreeMap<String, Value>,
    pub evaluator_output_hashes: Vec<String>,
    pub claims: Value,
    pub attempt_stage: V2AttemptStage,
    pub exact_material_generation: Option<u64>,
}

#[derive(Clone)]
pub struct V2ConnectorOutcome {
    pub receipt: ParsedV2<InvocationReceiptV2>,
    pub canonical_receipt: Vec<u8>,
    pub redelivery: bool,
}

#[derive(Clone)]
pub struct V2RemoteInvokeRequest {
    pub grant_ref: ExactGrantRefV2,
    pub canonical_input: Vec<u8>,
    pub attempt_stage: V2AttemptStage,
}
pub struct V2RemoteInvokeResponse {
    pub canonical_receipt: Vec<u8>,
}

pub trait V2BrokerTransport: Send + Sync {
    fn invoke_v2(
        &self,
        request: V2RemoteInvokeRequest,
    ) -> Result<V2RemoteInvokeResponse, BrokerHostError>;
}

#[derive(Clone, Default)]
pub struct V2ReceiptTrustStore {
    keys: BTreeMap<(String, String), BrokerVerifyingKey>,
}
impl V2ReceiptTrustStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(
        &mut self,
        issuer: impl Into<String>,
        key_id: impl Into<String>,
        key: BrokerVerifyingKey,
    ) -> Result<(), BrokerHostError> {
        if self
            .keys
            .insert((issuer.into(), key_id.into()), key)
            .is_some()
        {
            return Err(BrokerHostError::ReceiptVerificationFailed);
        }
        Ok(())
    }
}

pub struct RemoteV2ConnectorExecutor<T> {
    transport: T,
    trust: V2ReceiptTrustStore,
    receipts: Mutex<BTreeMap<String, String>>,
}
impl<T: V2BrokerTransport> RemoteV2ConnectorExecutor<T> {
    pub fn new(transport: T, trust: V2ReceiptTrustStore) -> Self {
        Self {
            transport,
            trust,
            receipts: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn invoke(
        &self,
        grant_ref: &ExactGrantRefV2,
        canonical_input: &[u8],
        expected: &V2DispatchEvidence,
    ) -> Result<V2ConnectorOutcome, BrokerHostError> {
        if expected
            .grant
            .view
            .as_value()
            .get("grant_ref")
            .and_then(Value::as_str)
            != Some(grant_ref.as_str())
            || expected.canonical_grant != expected.grant.canonical_bytes()
        {
            return Err(BrokerHostError::ReceiptVerificationFailed);
        }
        let canonical = broker_core::canonical::canonicalize_bounded(
            canonical_input,
            broker_core::canonical::MAX_OPERATION_BYTES,
        )?;
        if canonical.as_bytes() != canonical_input {
            return Err(BrokerError::Brk001.into());
        }
        let response = self.transport.invoke_v2(V2RemoteInvokeRequest {
            grant_ref: grant_ref.clone(),
            canonical_input: canonical_input.to_vec(),
            attempt_stage: expected.attempt_stage,
        })?;
        verify_v2_receipt(
            &response.canonical_receipt,
            expected,
            &self.trust,
            &self.receipts,
        )
    }
}

/// Local execution uses the same opaque grant-ref request and exact receipt
/// verifier as remote execution. A handler cannot trigger a V1 fallback.
pub struct LocalV2ConnectorExecutor<H> {
    handler: H,
    trust: V2ReceiptTrustStore,
    receipts: Mutex<BTreeMap<String, String>>,
}
impl<H> LocalV2ConnectorExecutor<H>
where
    H: Fn(V2RemoteInvokeRequest) -> Result<V2RemoteInvokeResponse, BrokerHostError> + Send + Sync,
{
    pub fn new(handler: H, trust: V2ReceiptTrustStore) -> Self {
        Self {
            handler,
            trust,
            receipts: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn invoke(
        &self,
        grant_ref: &ExactGrantRefV2,
        canonical_input: &[u8],
        expected: &V2DispatchEvidence,
    ) -> Result<V2ConnectorOutcome, BrokerHostError> {
        if expected
            .grant
            .view
            .as_value()
            .get("grant_ref")
            .and_then(Value::as_str)
            != Some(grant_ref.as_str())
            || expected.canonical_grant != expected.grant.canonical_bytes()
        {
            return Err(BrokerHostError::ReceiptVerificationFailed);
        }
        let canonical = broker_core::canonical::canonicalize_bounded(
            canonical_input,
            broker_core::canonical::MAX_OPERATION_BYTES,
        )?;
        if canonical.as_bytes() != canonical_input {
            return Err(BrokerError::Brk001.into());
        }
        let response = (self.handler)(V2RemoteInvokeRequest {
            grant_ref: grant_ref.clone(),
            canonical_input: canonical_input.to_vec(),
            attempt_stage: expected.attempt_stage,
        })?;
        verify_v2_receipt(
            &response.canonical_receipt,
            expected,
            &self.trust,
            &self.receipts,
        )
    }
}

fn verify_v2_receipt(
    canonical_receipt: &[u8],
    expected: &V2DispatchEvidence,
    trust: &V2ReceiptTrustStore,
    seen: &Mutex<BTreeMap<String, String>>,
) -> Result<V2ConnectorOutcome, BrokerHostError> {
    // Signature first. Only issuer/key id/signature envelope are decoded for
    // selecting an already-pinned key; no receipt authority is promoted.
    let untrusted: Value = serde_json::from_slice(canonical_receipt)
        .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
    let issuer = untrusted
        .get("issuer")
        .and_then(Value::as_str)
        .ok_or(BrokerHostError::ReceiptVerificationFailed)?;
    let key_id = untrusted
        .get("broker_key_id")
        .and_then(Value::as_str)
        .ok_or(BrokerHostError::ReceiptVerificationFailed)?;
    let key = trust
        .keys
        .get(&(issuer.to_owned(), key_id.to_owned()))
        .ok_or(BrokerHostError::ReceiptVerificationFailed)?;
    let signature: SignatureEnvelope = serde_json::from_value(
        untrusted
            .get("signature")
            .cloned()
            .ok_or(BrokerHostError::ReceiptVerificationFailed)?,
    )
    .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
    key.verify_json(INVOCATION_RECEIPT_DOMAIN, canonical_receipt, &signature)
        .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
    let receipt = parse::<InvocationReceiptV2>(canonical_receipt)
        .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
    if receipt.canonical_bytes() != canonical_receipt {
        return Err(BrokerHostError::ReceiptVerificationFailed);
    }
    let r = receipt.view.as_value();
    let g = expected.grant.view.as_value();
    let grant_hash = hash(&expected.canonical_grant);
    for (receipt_field, grant_field) in [
        ("org_id", "org_id"),
        ("principal", "principal"),
        ("issuer", "issuer"),
        ("deployment_id", "deployment_id"),
        ("contract_hash", "contract_hash"),
        ("subject", "subject"),
        ("grant_scope", "grant_scope"),
        ("authority_view_hash", "authority_view_hash"),
        ("authority_epoch", "authority_epoch"),
        ("auth_profile_ref", "auth_profile_ref"),
        ("auth_profile_pin", "auth_profile_pin"),
        (
            "authorization_claims_commitment",
            "authorization_claims_commitment",
        ),
        ("principal_commitments", "principal_commitments"),
        ("broker_instance_commitment", "broker_instance_commitment"),
        ("policy_instance_hashes", "policy_instance_hashes"),
        ("custodian", "custodian"),
        ("transport", "transport"),
        ("execution_lane", "execution_lane"),
        ("custody_location", "custody_location"),
    ] {
        if r.get(receipt_field) != g.get(grant_field) {
            return Err(BrokerHostError::ReceiptVerificationFailed);
        }
    }
    let derivation_hash = broker_core::canonical::from_serde(&g["derivation_evidence"], 64 * 1024)
        .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?
        .sha256();
    if r.get("grant_hash").and_then(Value::as_str) != Some(grant_hash.as_str())
        || r.get("derivation_evidence_hash").and_then(Value::as_str)
            != Some(derivation_hash.as_str())
        || r.get("canonical_input_commitment") != Some(&expected.canonical_input_commitment)
        || r.get("implementations")
            != Some(
                &serde_json::to_value(&expected.implementations)
                    .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?,
            )
        || r.get("evaluator_implementations")
            != Some(
                &serde_json::to_value(&expected.evaluator_implementations)
                    .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?,
            )
        || r.get("evaluator_output_hashes")
            != Some(
                &serde_json::to_value(&expected.evaluator_output_hashes)
                    .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?,
            )
        || r.get("claims") != Some(&expected.claims)
        || expected.implementations.len() != 6
        || ![
            "planner",
            "projector",
            "auth_driver",
            "custodian",
            "transport",
            "privileged_response_firewall",
        ]
        .iter()
        .all(|key| expected.implementations.contains_key(*key))
        || expected.evaluator_implementations.len() != expected.evaluator_output_hashes.len()
    {
        return Err(BrokerHostError::ReceiptVerificationFailed);
    }
    let predicates = parse_models::<broker_core::credential::policy::AssurancePredicateV2>(
        g.get("required_assurance_predicates")
            .and_then(Value::as_array)
            .ok_or(BrokerHostError::ReceiptVerificationFailed)?,
    )?;
    let assurance = parse_models::<broker_core::credential::policy::AssuranceEvidenceV2>(
        r.get("assurance_evidence")
            .and_then(Value::as_array)
            .ok_or(BrokerHostError::ReceiptVerificationFailed)?,
    )?;
    broker_core::credential::policy::verify_assurance(&predicates, &assurance)
        .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
    match expected.attempt_stage {
        V2AttemptStage::PrePlanning => {
            if r.get("dispatch_attempt").and_then(Value::as_u64) != Some(0)
                || r.get("pre_dispatch_stage").and_then(Value::as_str) != Some("pre_planning")
            {
                return Err(BrokerHostError::ReceiptVerificationFailed);
            }
        }
        V2AttemptStage::PostPlanningPreDispatch => {
            if r.get("dispatch_attempt").and_then(Value::as_u64) != Some(0)
                || r.get("pre_dispatch_stage").and_then(Value::as_str)
                    != Some("post_planning_pre_dispatch")
            {
                return Err(BrokerHostError::ReceiptVerificationFailed);
            }
        }
        V2AttemptStage::Dispatch(attempt) => {
            let generation = r
                .pointer("/leased_material_generation/generation")
                .and_then(Value::as_u64);
            if attempt == 0
                || r.get("dispatch_attempt").and_then(Value::as_u64) != Some(u64::from(attempt))
                || r.get("pre_dispatch_stage").is_some()
                || generation != expected.exact_material_generation
                || generation.is_none_or(|value| {
                    value
                        < g.get("minimum_material_generation")
                            .and_then(Value::as_u64)
                            .unwrap_or(u64::MAX)
                })
            {
                return Err(BrokerHostError::ReceiptVerificationFailed);
            }
        }
    }
    let effect = r
        .pointer("/grant_scope/logical_effect_id")
        .and_then(Value::as_str)
        .ok_or(BrokerHostError::ReceiptVerificationFailed)?;
    let attempt = r
        .get("dispatch_attempt")
        .and_then(Value::as_u64)
        .ok_or(BrokerHostError::ReceiptVerificationFailed)?;
    let delivery_key = format!("{grant_hash}:{effect}:{attempt}");
    let receipt_hash = hash(canonical_receipt);
    let mut seen = seen.lock().map_err(|_| BrokerError::Brk401)?;
    let redelivery = match seen.get(&delivery_key) {
        Some(existing) if existing == &receipt_hash => true,
        Some(_) => return Err(BrokerHostError::ReceiptVerificationFailed),
        None => {
            seen.insert(delivery_key, receipt_hash);
            false
        }
    };
    Ok(V2ConnectorOutcome {
        receipt,
        canonical_receipt: canonical_receipt.to_vec(),
        redelivery,
    })
}

fn parse_models<T>(values: &[Value]) -> Result<Vec<T>, BrokerHostError>
where
    T: broker_core::credential::SchemaType + serde::de::DeserializeOwned,
{
    values
        .iter()
        .map(|value| {
            let canonical = broker_core::canonical::from_serde(value, T::MAX_BYTES)
                .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
            broker_core::credential::parse::<T>(canonical.as_bytes())
                .map(|parsed| parsed.view)
                .map_err(|_| BrokerHostError::ReceiptVerificationFailed)
        })
        .collect()
}

fn hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}
