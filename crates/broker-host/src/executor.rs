use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use broker_core::{
    BrokerError,
    artifacts::{Assurance, ChannelMethod, InvocationReceipt, Outcome, ParsedArtifact},
    commitment::CommitmentKey,
    custodian::{AccessMaterial, ConnectionMetadata, CredentialCustodian, SyntheticCustodian},
    dispatch::{DispatchResult, MockDispatcher, ProviderDispatcher, ScriptedDispatch},
    effect_id,
    engine::{
        BrokerEngine, FixedTemplatePlanner, ImplementationApproval, InvokeRequest, TrustRegistry,
    },
    grant::{
        Clock, ConfiguredPopVerifier, FixedClock, GrantIssuer, GrantStore, PopVerifier,
        SequenceIds, StandingEnvelope,
    },
    ledger::InMemoryLedger,
    signing::{BrokerSigner, BrokerVerifyingKey, RECEIPT_DOMAIN},
};
use connector_spec::{
    BrokerDispatchDescriptor, descriptor_hash, validate_broker_dispatch_descriptor,
};

use crate::{BrokerBindingEvidence, BrokerHostError, TrustedHostScope};

/// Opaque reference to a descriptor-registered semantic operation. Its fields
/// cannot be forged or amended by invocation callers.
#[derive(Clone, Debug)]
pub struct BrokerOperation {
    contract_id: String,
    contract_hash: String,
    connection_alias: String,
    semantic_effect_slot: String,
    descriptor: BrokerDispatchDescriptor,
    approval: ImplementationApproval,
    max_logical_calls: u64,
    max_dispatch_attempts_per_call: u8,
}

impl BrokerOperation {
    pub fn contract_id(&self) -> &str {
        &self.contract_id
    }
    pub fn contract_hash(&self) -> &str {
        &self.contract_hash
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorOutcome {
    pub outcome: Outcome,
    pub receipt: InvocationReceipt,
    pub canonical_receipt: Vec<u8>,
    pub redelivery: bool,
}

/// The only execution interface for an explicitly broker-resolved operation.
pub trait ConnectorExecutor: Send + Sync {
    fn invoke(
        &self,
        host_scope: &TrustedHostScope,
        operation: &BrokerOperation,
        canonical_input: &[u8],
    ) -> Result<ConnectorOutcome, BrokerHostError>;
}

#[derive(Clone, Debug)]
struct DescriptorEntry {
    descriptor: BrokerDispatchDescriptor,
    approval: ImplementationApproval,
    max_logical_calls: u64,
    max_dispatch_attempts_per_call: u8,
}

/// Registry of B1-generated descriptors, keyed by exact contract id/hash.
#[derive(Default)]
pub struct BrokerDescriptorRegistry {
    entries: BTreeMap<(String, String), DescriptorEntry>,
}

impl BrokerDescriptorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_generated(
        &mut self,
        descriptor_bytes: &[u8],
        approval: ImplementationApproval,
        max_logical_calls: u64,
        max_dispatch_attempts_per_call: u8,
    ) -> Result<(), BrokerHostError> {
        if descriptor_bytes.len() > connector_spec::MAX_CONTRACT_DESCRIPTOR_BYTES
            || max_logical_calls == 0
            || max_dispatch_attempts_per_call == 0
        {
            return Err(BrokerHostError::DescriptorMismatch);
        }
        let descriptor: BrokerDispatchDescriptor = serde_json::from_slice(descriptor_bytes)
            .map_err(|_| BrokerHostError::DescriptorMismatch)?;
        let expected = descriptor_hash(&descriptor.contract)
            .map_err(|_| BrokerHostError::DescriptorMismatch)?;
        if !validate_broker_dispatch_descriptor(&descriptor)
            || expected != descriptor.contract_hash
            || descriptor.contract_hash != approval.contract_hash
            || connector_spec::request_plan_hash(&descriptor.request_plan)
                .ok()
                .as_deref()
                != Some(descriptor.request_plan_hash.as_str())
            || descriptor.response_data_policy != descriptor.contract.response_data_policy
            || descriptor.response_data_policy.max_bytes == 0
            || descriptor.response_data_policy.max_bytes > 256 * 1024
            || broker_core::dispatch::validate_https_origin(&descriptor.request_plan.origin)
                .is_err()
        {
            return Err(BrokerHostError::DescriptorMismatch);
        }
        let key = (
            descriptor.contract.contract_id.clone(),
            descriptor.contract_hash.clone(),
        );
        if self
            .entries
            .insert(
                key,
                DescriptorEntry {
                    descriptor,
                    approval,
                    max_logical_calls,
                    max_dispatch_attempts_per_call,
                },
            )
            .is_some()
        {
            return Err(BrokerHostError::DescriptorMismatch);
        }
        Ok(())
    }

    pub fn operation(
        &self,
        contract_id: &str,
        contract_hash: &str,
        connection_alias: impl Into<String>,
        semantic_effect_slot: impl Into<String>,
    ) -> Result<BrokerOperation, BrokerHostError> {
        let entry = self
            .entries
            .get(&(contract_id.into(), contract_hash.into()))
            .ok_or(BrokerHostError::DescriptorMismatch)?;
        let slot = semantic_effect_slot.into();
        if !entry.contract_semantic_slots().contains(&slot) {
            return Err(BrokerHostError::DescriptorMismatch);
        }
        let connection_alias = connection_alias.into();
        if connection_alias.is_empty() {
            return Err(BrokerHostError::DescriptorMismatch);
        }
        Ok(BrokerOperation {
            contract_id: contract_id.into(),
            contract_hash: contract_hash.into(),
            connection_alias,
            semantic_effect_slot: slot,
            descriptor: entry.descriptor.clone(),
            approval: entry.approval.clone(),
            max_logical_calls: entry.max_logical_calls,
            max_dispatch_attempts_per_call: entry.max_dispatch_attempts_per_call,
        })
    }
}

impl DescriptorEntry {
    fn contract_semantic_slots(&self) -> &Vec<String> {
        &self.descriptor.contract.semantic_effect_slots
    }
}

pub struct LocalBrokerConfig {
    pub evidence: BrokerBindingEvidence,
    pub descriptors: BrokerDescriptorRegistry,
    pub clock: FixedClock,
    pub dispatch_scripts: Vec<ScriptedDispatch>,
    pub authority_facts: Vec<u8>,
    pub custodian_epoch: u64,
    pub synthetic_secret: Vec<u8>,
    pub pop_method: ChannelMethod,
    pub pop_key_thumbprint: String,
    pub pop_session_id: String,
    pub pop_proof: Vec<u8>,
    pub expected_pop_proof: Vec<u8>,
    pub grant_issuer: String,
    pub grant_not_before: String,
    pub grant_expires_at: String,
    pub max_grant_lifetime_seconds: u64,
    pub receipt_signer: BrokerSigner,
    pub commitments: CommitmentKey,
    pub broker_principal_id: String,
}

struct CountingCustodian<C> {
    inner: C,
    metadata_calls: AtomicUsize,
    material_accesses: AtomicUsize,
}

impl<C: CredentialCustodian> CredentialCustodian for CountingCustodian<C> {
    fn connection_metadata(&self) -> Result<ConnectionMetadata, BrokerError> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.connection_metadata()
    }
    fn validate_scopes(&self, required: &BTreeSet<String>) -> Result<(), BrokerError> {
        self.inner.validate_scopes(required)
    }
    fn with_access_material<T>(
        &self,
        use_material: impl FnOnce(AccessMaterial<'_>) -> Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        self.material_accesses.fetch_add(1, Ordering::SeqCst);
        self.inner.with_access_material(use_material)
    }
    fn refresh(&self) -> Result<(), BrokerError> {
        self.inner.refresh()
    }
    fn revoke(&self) -> Result<u64, BrokerError> {
        self.inner.revoke()
    }
}

struct HostTrustRegistry {
    approvals: BTreeMap<(String, String), ImplementationApproval>,
    binding_keys: BTreeMap<(String, String), BrokerVerifyingKey>,
}
impl TrustRegistry for HostTrustRegistry {
    fn resolve(
        &self,
        contract_id: &str,
        contract_hash: &str,
    ) -> Result<ImplementationApproval, BrokerError> {
        self.approvals
            .get(&(contract_id.into(), contract_hash.into()))
            .cloned()
            .ok_or(BrokerError::Brk108)
    }
    fn binding_key(&self, issuer: &str, key_id: &str) -> Option<BrokerVerifyingKey> {
        self.binding_keys
            .get(&(issuer.into(), key_id.into()))
            .cloned()
    }
}

struct PolicyDispatcher<'a> {
    inner: &'a MockDispatcher,
    max_bytes: usize,
    exceeded: &'a AtomicBool,
}
impl ProviderDispatcher for PolicyDispatcher<'_> {
    fn dispatch(
        &self,
        plan: &broker_core::dispatch::FinalRequestPlan,
        material: AccessMaterial<'_>,
    ) -> Result<DispatchResult, BrokerError> {
        let result = self.inner.dispatch(plan, material)?;
        if matches!(&result, DispatchResult::Confirmed(value) if value.bounded_projection.len() > self.max_bytes)
        {
            self.exceeded.store(true, Ordering::SeqCst);
            return Err(BrokerError::Brk305);
        }
        Ok(result)
    }
}

pub struct LocalBrokerExecutor<C = SyntheticCustodian> {
    evidence: BrokerBindingEvidence,
    descriptors: BrokerDescriptorRegistry,
    trusted_adapters: crate::TrustedAdapterRegistry,
    ledger: InMemoryLedger,
    dispatcher: MockDispatcher,
    custodian: CountingCustodian<C>,
    trust: HostTrustRegistry,
    clock: FixedClock,
    signer: BrokerSigner,
    commitments: CommitmentKey,
    pop_verifier: ConfiguredPopVerifier,
    pop: broker_core::grant::PopSession,
    grant_store: GrantStore,
    ids: SequenceIds,
    grant_issuer: String,
    grant_not_before: String,
    grant_expires_at: String,
    max_grant_lifetime_seconds: u64,
    authority_facts: Vec<u8>,
    broker_principal_id: String,
}

impl LocalBrokerExecutor<SyntheticCustodian> {
    pub fn new(mut config: LocalBrokerConfig) -> Result<Self, BrokerHostError> {
        let first_alias = config
            .evidence
            .entries_alias_for_bootstrap()
            .ok_or(BrokerHostError::MissingBindingEvidence)?;
        let lock = config
            .evidence
            .lock_for_alias(first_alias)
            .ok_or(BrokerHostError::MissingBindingEvidence)?;
        let custodian = SyntheticCustodian::new(
            lock.connection_ref(),
            lock.provider(),
            "locked-account",
            lock.actual_scopes().to_vec(),
            std::mem::take(&mut config.synthetic_secret),
        );
        custodian.set_account_commitment(lock.account_commitment().clone())?;
        custodian.set_epoch(config.custodian_epoch)?;
        Self::new_with_custodian(config, custodian, crate::TrustedAdapterRegistry::empty())
    }
}

impl<C: CredentialCustodian> LocalBrokerExecutor<C> {
    pub(crate) fn new_with_custodian(
        config: LocalBrokerConfig,
        custodian: C,
        trusted_adapters: crate::TrustedAdapterRegistry,
    ) -> Result<Self, BrokerHostError> {
        let approvals = config
            .descriptors
            .entries
            .iter()
            .map(|((id, hash), entry)| ((id.clone(), hash.clone()), entry.approval.clone()))
            .collect();
        let binding_keys = config
            .evidence
            .trust_keys()
            .into_iter()
            .map(|(issuer, key_id, key)| ((issuer, key_id), key))
            .collect();
        let authority = broker_core::grant::bootstrap_host_authority();
        let pop = authority.pop_session(
            config.pop_method,
            config.pop_key_thumbprint,
            config.pop_session_id,
            config.pop_proof,
        )?;
        Ok(Self {
            evidence: config.evidence,
            descriptors: config.descriptors,
            trusted_adapters,
            ledger: InMemoryLedger::new(),
            dispatcher: MockDispatcher::new(config.dispatch_scripts),
            custodian: CountingCustodian {
                inner: custodian,
                metadata_calls: AtomicUsize::new(0),
                material_accesses: AtomicUsize::new(0),
            },
            trust: HostTrustRegistry {
                approvals,
                binding_keys,
            },
            clock: config.clock,
            signer: config.receipt_signer,
            commitments: config.commitments,
            pop_verifier: ConfiguredPopVerifier::new(config.expected_pop_proof)?,
            pop,
            grant_store: GrantStore::default(),
            ids: SequenceIds::new(1),
            grant_issuer: config.grant_issuer,
            grant_not_before: config.grant_not_before,
            grant_expires_at: config.grant_expires_at,
            max_grant_lifetime_seconds: config.max_grant_lifetime_seconds,
            authority_facts: config.authority_facts,
            broker_principal_id: config.broker_principal_id,
        })
    }

    pub fn operation(
        &self,
        contract_id: &str,
        contract_hash: &str,
        connection_alias: impl Into<String>,
        semantic_effect_slot: impl Into<String>,
    ) -> Result<BrokerOperation, BrokerHostError> {
        self.descriptors.operation(
            contract_id,
            contract_hash,
            connection_alias,
            semantic_effect_slot,
        )
    }
    pub fn dispatch_count(&self) -> usize {
        self.dispatcher.recorded_plans().len()
    }
    pub fn custodian_call_count(&self) -> usize {
        self.custodian.metadata_calls.load(Ordering::SeqCst)
            + self.custodian.material_accesses.load(Ordering::SeqCst)
    }
    pub fn recorded_plans(&self) -> Vec<Vec<u8>> {
        self.dispatcher.recorded_plans()
    }
}

impl<C: CredentialCustodian> ConnectorExecutor for LocalBrokerExecutor<C> {
    fn invoke(
        &self,
        host_scope: &TrustedHostScope,
        operation: &BrokerOperation,
        canonical_input: &[u8],
    ) -> Result<ConnectorOutcome, BrokerHostError> {
        host_scope.compare_guest_payload(canonical_input)?;
        let registered = self
            .descriptors
            .entries
            .get(&(
                operation.contract_id.clone(),
                operation.contract_hash.clone(),
            ))
            .ok_or(BrokerHostError::DescriptorMismatch)?;
        if registered.descriptor != operation.descriptor
            || registered.approval.contract_hash != operation.approval.contract_hash
        {
            return Err(BrokerHostError::DescriptorMismatch);
        }

        // Signature, lock fields, descriptor binding, and PoP all fail before
        // touching the custodian. Live connection metadata and epoch are then
        // resolved from the custodian rather than accepted from the operation.
        let binding = self.evidence.verify_preflight(
            &operation.connection_alias,
            &operation.contract_id,
            &operation.contract_hash,
            &self.clock.now_rfc3339(),
        )?;
        if !self.pop_verifier.verify(&self.pop) {
            return Err(BrokerError::Brk102.into());
        }
        let live = self.custodian.connection_metadata()?;
        if live.revocation_epoch != binding.attestation.view.revocation_epoch {
            return Err(BrokerError::Brk106.into());
        }
        if live.connection_ref != binding.attestation.view.connection_ref
            || binding.attestation.view.endpoint_origins.first()
                != Some(&operation.descriptor.request_plan.origin)
        {
            return Err(BrokerHostError::DescriptorMismatch);
        }

        let envelope = StandingEnvelope {
            operation_contract: operation.contract_id.clone(),
            contract_hash: operation.contract_hash.clone(),
            max_logical_calls: operation.max_logical_calls,
            max_dispatch_attempts_per_call: operation.max_dispatch_attempts_per_call,
            minimum_assurance: Assurance::BrokeredCount,
            required_attenuations: Vec::new(),
            max_grant_lifetime_seconds: self.max_grant_lifetime_seconds,
        };
        let issuer = GrantIssuer {
            issuer: &self.grant_issuer,
            clock: &self.clock,
            ids: &self.ids,
            store: &self.grant_store,
            pop_verifier: &self.pop_verifier,
        };
        let issued = issuer.issue(
            host_scope.as_core(),
            &envelope,
            &binding.attestation.view,
            &self.pop,
            operation.max_logical_calls,
            operation.max_dispatch_attempts_per_call,
            self.grant_not_before.clone(),
            self.grant_expires_at.clone(),
        )?;
        let grant = self.grant_store.get(&issued.grant_ref)?;
        let logical_effect_id = effect_id::derive(
            host_scope.run_id(),
            host_scope.node_id(),
            host_scope.activation_ordinal(),
            &operation.semantic_effect_slot,
        )?;
        let template = descriptor_plan_template(
            &operation.descriptor,
            canonical_input,
            &self.authority_facts,
            &self.trusted_adapters,
            &logical_effect_id,
            &self.clock.now_rfc3339(),
        )?;
        let planner = FixedTemplatePlanner {
            template,
            facts: self.authority_facts.clone(),
            implementation: operation.approval.implementation.clone(),
        };
        let exceeded = AtomicBool::new(false);
        let dispatcher = PolicyDispatcher {
            inner: &self.dispatcher,
            max_bytes: operation.descriptor.response_data_policy.max_bytes as usize,
            exceeded: &exceeded,
        };
        let engine = BrokerEngine {
            ledger: &self.ledger,
            planner: &planner,
            dispatcher: &dispatcher,
            custodian: &self.custodian,
            trust: &self.trust,
            clock: &self.clock,
            signer: &self.signer,
            commitments: &self.commitments,
            broker_principal_id: &self.broker_principal_id,
            pop_verifier: &self.pop_verifier,
        };
        let result = engine.invoke(InvokeRequest {
            scope: host_scope.as_core(),
            grant: &grant,
            binding: &binding.attestation,
            pop: &self.pop,
            logical_effect_id: &logical_effect_id,
            canonical_input,
            lease_seconds: 30,
        })?;
        if exceeded.load(Ordering::SeqCst) {
            return Err(BrokerHostError::ResponsePolicyExceeded);
        }
        Ok(ConnectorOutcome {
            outcome: result.receipt.outcome.clone(),
            receipt: result.receipt,
            canonical_receipt: result.canonical_receipt,
            redelivery: result.redelivery,
        })
    }
}

fn descriptor_plan_template(
    descriptor: &BrokerDispatchDescriptor,
    canonical_input: &[u8],
    authority_facts: &[u8],
    adapters: &crate::TrustedAdapterRegistry,
    logical_effect_id: &str,
    now: &str,
) -> Result<Vec<u8>, BrokerHostError> {
    use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

    let input: serde_json::Value = serde_json::from_slice(canonical_input)
        .map_err(|_| BrokerHostError::Broker(BrokerError::Brk001))?;
    let facts: serde_json::Value = serde_json::from_slice(authority_facts)
        .map_err(|_| BrokerHostError::Broker(BrokerError::Brk301))?;
    let object = match &descriptor.request_plan.trusted_adapter {
        Some(pin) => adapters.adapt(pin, &input, &facts)?,
        None => input
            .as_object()
            .cloned()
            .ok_or(BrokerHostError::Broker(BrokerError::Brk001))?,
    };
    let mut body = serde_json::Map::new();
    for (wire, field) in &descriptor.request_plan.body {
        let value = object
            .get(field)
            .cloned()
            .ok_or(BrokerHostError::Broker(BrokerError::Brk301))?;
        body.insert(wire.clone(), value);
    }
    let mut path = descriptor.request_plan.path_template.clone();
    let mut uses_idempotency = false;
    for (name, declaration) in &descriptor.request_plan.placeholders {
        let replacement = match declaration.kind.as_str() {
            "idempotency_key" => {
                uses_idempotency = true;
                utf8_percent_encode(
                    &broker_slot_value("idempotency_key", logical_effect_id, now)?,
                    NON_ALPHANUMERIC,
                )
                .to_string()
            }
            "input" => {
                let field = declaration
                    .input_field
                    .as_ref()
                    .ok_or(BrokerHostError::DescriptorMismatch)?;
                let value = provider_scalar(
                    object
                        .get(field)
                        .ok_or(BrokerHostError::DescriptorMismatch)?,
                )?;
                utf8_percent_encode(&value, NON_ALPHANUMERIC).to_string()
            }
            "timestamp" | "boundary" => utf8_percent_encode(
                &broker_slot_value(&declaration.kind, logical_effect_id, now)?,
                NON_ALPHANUMERIC,
            )
            .to_string(),
            _ => return Err(BrokerHostError::DescriptorMismatch),
        };
        path = path.replace(&format!("{{{name}}}"), &replacement);
    }
    let mut query = serde_json::Map::new();
    for (name, declaration) in &descriptor.request_plan.query {
        let value = match declaration.kind.as_str() {
            "static" => serde_json::Value::String(
                declaration
                    .value
                    .clone()
                    .ok_or(BrokerHostError::DescriptorMismatch)?,
            ),
            "input" => {
                let field = declaration
                    .input_field
                    .as_ref()
                    .ok_or(BrokerHostError::DescriptorMismatch)?;
                serde_json::Value::String(provider_scalar(
                    object
                        .get(field)
                        .ok_or(BrokerHostError::DescriptorMismatch)?,
                )?)
            }
            "idempotency_key" => {
                uses_idempotency = true;
                serde_json::Value::String(broker_slot_value(
                    "idempotency_key",
                    logical_effect_id,
                    now,
                )?)
            }
            "timestamp" | "boundary" => serde_json::Value::String(broker_slot_value(
                &declaration.kind,
                logical_effect_id,
                now,
            )?),
            _ => return Err(BrokerHostError::DescriptorMismatch),
        };
        query.insert(name.clone(), value);
    }
    let value = serde_json::json!({
        "body": body,
        "headers": descriptor.request_plan.static_headers,
        "idempotency": uses_idempotency.then(|| serde_json::json!({"$broker":"idempotency_key"})),
        "method": descriptor.request_plan.method.as_str(),
        "path": path,
        "query": query,
    });
    broker_core::canonical::from_serde(&value, broker_core::canonical::MAX_OPERATION_BYTES)
        .map(|value| value.into_bytes())
        .map_err(Into::into)
}

fn broker_slot_value(
    kind: &str,
    logical_effect_id: &str,
    now: &str,
) -> Result<String, BrokerHostError> {
    let key = broker_core::dispatch::idempotency_key(logical_effect_id)?;
    match kind {
        "idempotency_key" => Ok(key),
        "timestamp" => Ok(now.to_string()),
        "boundary" => Ok(format!("lattice-{key}")),
        _ => Err(BrokerHostError::DescriptorMismatch),
    }
}

fn provider_scalar(value: &serde_json::Value) -> Result<String, BrokerHostError> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        _ => Err(BrokerHostError::Broker(BrokerError::Brk301)),
    }
}

#[derive(Clone, Debug)]
pub struct RemoteInvokeRequest {
    pub host_scope: TrustedHostScope,
    pub operation: BrokerOperation,
    pub canonical_input: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct RemoteInvokeResponse {
    pub outcome: ConnectorOutcome,
}
pub trait BrokerTransport: Send + Sync {
    fn invoke(&self, request: RemoteInvokeRequest)
    -> Result<RemoteInvokeResponse, BrokerHostError>;
}

#[derive(Clone)]
struct ReceiptTrustEntry {
    verifier: BrokerVerifyingKey,
    grant_hashes: BTreeSet<String>,
}
#[derive(Clone, Default)]
pub struct ReceiptTrustStore {
    entries: BTreeMap<(String, String), ReceiptTrustEntry>,
    connection_epochs: BTreeMap<String, u64>,
}
impl ReceiptTrustStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert_key(
        &mut self,
        issuer: impl Into<String>,
        key_id: impl Into<String>,
        verifier: BrokerVerifyingKey,
        grant_hashes: impl IntoIterator<Item = String>,
    ) -> Result<(), BrokerHostError> {
        let key = (issuer.into(), key_id.into());
        if self
            .entries
            .insert(
                key,
                ReceiptTrustEntry {
                    verifier,
                    grant_hashes: grant_hashes.into_iter().collect(),
                },
            )
            .is_some()
        {
            return Err(BrokerHostError::ReceiptVerificationFailed);
        }
        Ok(())
    }
    pub fn pin_connection_epoch(&mut self, alias: impl Into<String>, epoch: u64) {
        self.connection_epochs.insert(alias.into(), epoch);
    }
}

pub struct RemoteBrokerExecutor<T, C> {
    transport: T,
    evidence: BrokerBindingEvidence,
    clock: C,
    receipts: ReceiptTrustStore,
}
impl<T: BrokerTransport, C: Clock> RemoteBrokerExecutor<T, C> {
    pub fn new(
        transport: T,
        evidence: BrokerBindingEvidence,
        clock: C,
        receipts: ReceiptTrustStore,
    ) -> Self {
        Self {
            transport,
            evidence,
            clock,
            receipts,
        }
    }
    pub fn transport(&self) -> &T {
        &self.transport
    }
}
impl<T: BrokerTransport, C: Clock> ConnectorExecutor for RemoteBrokerExecutor<T, C> {
    fn invoke(
        &self,
        host_scope: &TrustedHostScope,
        operation: &BrokerOperation,
        canonical_input: &[u8],
    ) -> Result<ConnectorOutcome, BrokerHostError> {
        host_scope.compare_guest_payload(canonical_input)?;
        let epoch = self
            .receipts
            .connection_epochs
            .get(&operation.connection_alias)
            .copied()
            .ok_or(BrokerHostError::ReceiptVerificationFailed)?;
        self.evidence.verify(
            &operation.connection_alias,
            &operation.contract_id,
            &operation.contract_hash,
            epoch,
            &self.clock.now_rfc3339(),
        )?;
        let response = self.transport.invoke(RemoteInvokeRequest {
            host_scope: host_scope.clone(),
            operation: operation.clone(),
            canonical_input: canonical_input.to_vec(),
        })?;
        verify_remote_outcome(&response.outcome, host_scope, operation, &self.receipts)?;
        Ok(response.outcome)
    }
}

fn verify_remote_outcome(
    outcome: &ConnectorOutcome,
    scope: &TrustedHostScope,
    operation: &BrokerOperation,
    trust: &ReceiptTrustStore,
) -> Result<(), BrokerHostError> {
    let parsed: ParsedArtifact<InvocationReceipt> =
        broker_core::artifacts::parse(&outcome.canonical_receipt)
            .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
    if parsed.canonical_bytes() != outcome.canonical_receipt
        || serde_json::to_value(&parsed.view).ok() != serde_json::to_value(&outcome.receipt).ok()
        || parsed.view.outcome != outcome.outcome
        || parsed.view.contract_hash != operation.contract_hash
        || parsed.view.policy_hash != operation.approval.policy_hash
        || parsed.view.plugin_module_sha256 != operation.approval.plugin_module_sha256
        || parsed.view.plugin_trust_tier != operation.approval.trust_tier
        || !scope.matches_receipt(&parsed.view)
    {
        return Err(BrokerHostError::ReceiptVerificationFailed);
    }
    let entry = trust
        .entries
        .get(&(
            parsed.view.issuer.clone(),
            parsed.view.broker_key_id.clone(),
        ))
        .ok_or(BrokerHostError::ReceiptVerificationFailed)?;
    if !entry.grant_hashes.contains(&parsed.view.grant_hash) {
        return Err(BrokerHostError::ReceiptVerificationFailed);
    }
    entry
        .verifier
        .verify_json(
            RECEIPT_DOMAIN,
            &outcome.canonical_receipt,
            &parsed.view.signature,
        )
        .map_err(|_| BrokerHostError::ReceiptVerificationFailed)?;
    parsed
        .view
        .validate_semantics()
        .map_err(|_| BrokerHostError::ReceiptVerificationFailed)
}

pub struct UnavailableBrokerTransport;
impl BrokerTransport for UnavailableBrokerTransport {
    fn invoke(
        &self,
        _request: RemoteInvokeRequest,
    ) -> Result<RemoteInvokeResponse, BrokerHostError> {
        Err(BrokerHostError::ExecutorUnavailable)
    }
}

pub struct TestBrokerTransport {
    responses: Mutex<VecDeque<Result<RemoteInvokeResponse, BrokerHostError>>>,
    calls: AtomicUsize,
}
impl TestBrokerTransport {
    pub fn new(
        responses: impl IntoIterator<Item = Result<RemoteInvokeResponse, BrokerHostError>>,
    ) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
            calls: AtomicUsize::new(0),
        }
    }
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
impl BrokerTransport for TestBrokerTransport {
    fn invoke(
        &self,
        _request: RemoteInvokeRequest,
    ) -> Result<RemoteInvokeResponse, BrokerHostError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.responses
            .lock()
            .map_err(|_| BrokerHostError::Broker(BrokerError::Brk401))?
            .pop_front()
            .unwrap_or(Err(BrokerHostError::ExecutorUnavailable))
    }
}
