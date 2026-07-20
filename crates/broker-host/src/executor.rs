use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use broker_core::{
    BrokerError,
    artifacts::{InvocationReceipt, Outcome},
    commitment::CommitmentKey,
    custodian::{AccessMaterial, ConnectionMetadata, CredentialCustodian, SyntheticCustodian},
    dispatch::{MockDispatcher, ScriptedDispatch},
    effect_id,
    engine::{
        BrokerEngine, FixedTemplatePlanner, ImplementationApproval, InvokeRequest, TrustRegistry,
    },
    grant::{Clock, ConfiguredPopVerifier, ExecutionGrantRecord, FixedClock, PopSession},
    ledger::InMemoryLedger,
    signing::{BrokerSigner, BrokerVerifyingKey},
};

use crate::{BrokerBindingEvidence, BrokerHostError, TrustedHostScope};

#[derive(Clone, Debug)]
pub struct BrokerOperation {
    pub contract_id: String,
    pub contract_hash: String,
    pub connection_alias: String,
    pub semantic_effect_slot: String,
    pub grant: ExecutionGrantRecord,
    pub pop: PopSession,
    pub current_revocation_epoch: u64,
}

#[derive(Clone, Debug)]
pub struct ConnectorOutcome {
    pub outcome: Outcome,
    pub receipt: InvocationReceipt,
    pub canonical_receipt: Vec<u8>,
    pub redelivery: bool,
}

/// The only execution interface for an explicitly broker-resolved operation.
/// Implementations return an error when unavailable; callers must not route
/// that error into the direct connector credential path.
pub trait ConnectorExecutor: Send + Sync {
    fn invoke(
        &self,
        host_scope: &TrustedHostScope,
        operation: &BrokerOperation,
        canonical_input: &[u8],
    ) -> Result<ConnectorOutcome, BrokerHostError>;
}

#[derive(Clone, Debug)]
pub struct LocalContractApproval {
    pub contract_id: String,
    pub approval: ImplementationApproval,
}

pub struct LocalBrokerConfig {
    pub evidence: BrokerBindingEvidence,
    pub contracts: Vec<LocalContractApproval>,
    pub clock: FixedClock,
    pub dispatch_scripts: Vec<ScriptedDispatch>,
    pub request_template: Vec<u8>,
    pub authority_facts: Vec<u8>,
    pub implementation: String,
    pub endpoint_origin: String,
    pub connection_ref: String,
    pub provider: String,
    pub account_subject: String,
    pub scopes: Vec<String>,
    pub synthetic_secret: Vec<u8>,
    pub expected_pop_proof: Vec<u8>,
    pub receipt_signer: BrokerSigner,
    pub commitments: CommitmentKey,
    pub broker_principal_id: String,
}

struct CountingSyntheticCustodian {
    inner: SyntheticCustodian,
    accesses: AtomicUsize,
}

impl CountingSyntheticCustodian {
    fn new(config: &LocalBrokerConfig) -> Self {
        Self {
            inner: SyntheticCustodian::new(
                &config.connection_ref,
                &config.provider,
                &config.account_subject,
                config.scopes.clone(),
                config.synthetic_secret.clone(),
            ),
            accesses: AtomicUsize::new(0),
        }
    }
}

impl CredentialCustodian for CountingSyntheticCustodian {
    fn connection_metadata(&self) -> Result<ConnectionMetadata, BrokerError> {
        self.inner.connection_metadata()
    }

    fn validate_scopes(
        &self,
        required: &std::collections::BTreeSet<String>,
    ) -> Result<(), BrokerError> {
        self.inner.validate_scopes(required)
    }

    fn with_access_material<T>(
        &self,
        use_material: impl FnOnce(AccessMaterial<'_>) -> Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        self.accesses.fetch_add(1, Ordering::SeqCst);
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

/// Deterministic in-process Broker V1 host path. It deliberately uses only the
/// broker-core in-memory ledger, synthetic custodian, and mock dispatcher.
pub struct LocalBrokerExecutor {
    evidence: BrokerBindingEvidence,
    ledger: InMemoryLedger,
    planner: FixedTemplatePlanner,
    dispatcher: MockDispatcher,
    custodian: CountingSyntheticCustodian,
    trust: HostTrustRegistry,
    clock: FixedClock,
    signer: BrokerSigner,
    commitments: CommitmentKey,
    pop_verifier: ConfiguredPopVerifier,
    broker_principal_id: String,
}

impl LocalBrokerExecutor {
    pub fn new(config: LocalBrokerConfig) -> Result<Self, BrokerHostError> {
        let mut approvals = BTreeMap::new();
        for contract in &config.contracts {
            if contract.contract_id.is_empty()
                || approvals
                    .insert(
                        (
                            contract.contract_id.clone(),
                            contract.approval.contract_hash.clone(),
                        ),
                        contract.approval.clone(),
                    )
                    .is_some()
            {
                return Err(BrokerError::Brk108.into());
            }
        }
        let binding_keys = config
            .evidence
            .trust_keys()
            .into_iter()
            .map(|(issuer, key_id, key)| ((issuer, key_id), key))
            .collect();
        let custodian = CountingSyntheticCustodian::new(&config);
        let configured_epoch = config
            .evidence
            .configured_epoch(&config.connection_ref)
            .unwrap_or(0);
        custodian.inner.set_epoch(configured_epoch)?;
        Ok(Self {
            evidence: config.evidence,
            ledger: InMemoryLedger::new(),
            planner: FixedTemplatePlanner {
                template: config.request_template,
                facts: config.authority_facts,
                implementation: config.implementation,
            },
            dispatcher: MockDispatcher::new(config.dispatch_scripts),
            custodian,
            trust: HostTrustRegistry {
                approvals,
                binding_keys,
            },
            clock: config.clock,
            signer: config.receipt_signer,
            commitments: config.commitments,
            pop_verifier: ConfiguredPopVerifier::new(config.expected_pop_proof)?,
            broker_principal_id: config.broker_principal_id,
        })
    }

    pub fn dispatch_count(&self) -> usize {
        self.dispatcher.recorded_plans().len()
    }

    pub fn custodian_access_count(&self) -> usize {
        self.custodian.accesses.load(Ordering::SeqCst)
    }
}

impl ConnectorExecutor for LocalBrokerExecutor {
    fn invoke(
        &self,
        host_scope: &TrustedHostScope,
        operation: &BrokerOperation,
        canonical_input: &[u8],
    ) -> Result<ConnectorOutcome, BrokerHostError> {
        host_scope.compare_guest_payload(canonical_input)?;
        if operation.contract_id != operation.grant.grant.operation_contract
            || operation.contract_hash != operation.grant.grant.contract_hash
        {
            return Err(BrokerError::Brk107.into());
        }
        let binding = self.evidence.verify(
            &operation.connection_alias,
            &operation.contract_id,
            &operation.contract_hash,
            operation.current_revocation_epoch,
            &self.clock.now_rfc3339(),
        )?;
        let core_scope = host_scope.as_core()?;
        let logical_effect_id = effect_id::derive(
            host_scope.run_id(),
            host_scope.node_id(),
            host_scope.activation_ordinal(),
            &operation.semantic_effect_slot,
        )?;
        let engine = BrokerEngine {
            ledger: &self.ledger,
            planner: &self.planner,
            dispatcher: &self.dispatcher,
            custodian: &self.custodian,
            trust: &self.trust,
            clock: &self.clock,
            signer: &self.signer,
            commitments: &self.commitments,
            broker_principal_id: &self.broker_principal_id,
            pop_verifier: &self.pop_verifier,
        };
        let result = engine.invoke(InvokeRequest {
            scope: &core_scope,
            grant: &operation.grant,
            binding: binding.attestation,
            pop: &operation.pop,
            logical_effect_id: &logical_effect_id,
            canonical_input,
            lease_seconds: 30,
        })?;
        Ok(ConnectorOutcome {
            outcome: result.receipt.outcome.clone(),
            receipt: result.receipt,
            canonical_receipt: result.canonical_receipt,
            redelivery: result.redelivery,
        })
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

pub struct RemoteBrokerExecutor<T> {
    transport: T,
    evidence: BrokerBindingEvidence,
    now: String,
}

impl<T: BrokerTransport> RemoteBrokerExecutor<T> {
    pub fn new(transport: T, evidence: BrokerBindingEvidence, now: impl Into<String>) -> Self {
        Self {
            transport,
            evidence,
            now: now.into(),
        }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }
}

impl<T: BrokerTransport> ConnectorExecutor for RemoteBrokerExecutor<T> {
    fn invoke(
        &self,
        host_scope: &TrustedHostScope,
        operation: &BrokerOperation,
        canonical_input: &[u8],
    ) -> Result<ConnectorOutcome, BrokerHostError> {
        host_scope.compare_guest_payload(canonical_input)?;
        self.evidence.verify(
            &operation.connection_alias,
            &operation.contract_id,
            &operation.contract_hash,
            operation.current_revocation_epoch,
            &self.now,
        )?;
        self.transport
            .invoke(RemoteInvokeRequest {
                host_scope: host_scope.clone(),
                operation: operation.clone(),
                canonical_input: canonical_input.to_vec(),
            })
            .map(|response| response.outcome)
    }
}

/// Placeholder for the later HTTP packet. It always fails and has no direct,
/// ambient-HTTP, or local fallback behavior.
pub struct UnavailableBrokerTransport;

impl BrokerTransport for UnavailableBrokerTransport {
    fn invoke(
        &self,
        _request: RemoteInvokeRequest,
    ) -> Result<RemoteInvokeResponse, BrokerHostError> {
        Err(BrokerHostError::ExecutorUnavailable)
    }
}

/// Deterministic request/response transport for host integration tests.
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
