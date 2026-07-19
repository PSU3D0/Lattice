use crate::{BrokerError, artifacts::*, canonical};
use std::{collections::BTreeMap, sync::Mutex};

pub trait Clock: Send + Sync {
    fn now_rfc3339(&self) -> String;
    fn monotonic_seconds(&self) -> i64 {
        0
    }
}
#[derive(Clone, Debug)]
pub struct FixedClock(pub String);
impl Clock for FixedClock {
    fn now_rfc3339(&self) -> String {
        self.0.clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedHostScope {
    org_id: String,
    principal_id: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    node_id: String,
    node_alias: String,
    run_id: String,
}
impl TrustedHostScope {
    #[allow(clippy::too_many_arguments)]
    pub fn from_authenticated_host(
        org_id: impl Into<String>,
        principal_id: impl Into<String>,
        bundle_id: impl Into<String>,
        flow_ir_hash: impl Into<String>,
        binding_lock_hash: impl Into<String>,
        flow_id: impl Into<String>,
        node_id: impl Into<String>,
        node_alias: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Result<Self, BrokerError> {
        let value = Self {
            org_id: org_id.into(),
            principal_id: principal_id.into(),
            bundle_id: bundle_id.into(),
            flow_ir_hash: flow_ir_hash.into(),
            binding_lock_hash: binding_lock_hash.into(),
            flow_id: flow_id.into(),
            node_id: node_id.into(),
            node_alias: node_alias.into(),
            run_id: run_id.into(),
        };
        if [
            &value.org_id,
            &value.principal_id,
            &value.bundle_id,
            &value.flow_ir_hash,
            &value.binding_lock_hash,
            &value.flow_id,
            &value.node_id,
            &value.node_alias,
            &value.run_id,
        ]
        .iter()
        .any(|v| v.is_empty())
        {
            return Err(BrokerError::Brk101);
        }
        Ok(value)
    }
    pub fn org_id(&self) -> &str {
        &self.org_id
    }
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
    pub fn node_alias(&self) -> &str {
        &self.node_alias
    }
    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }
}

#[derive(Clone, Debug)]
pub struct StandingEnvelope {
    pub operation_contract: String,
    pub contract_hash: String,
    pub max_logical_calls: u64,
    pub max_dispatch_attempts_per_call: u8,
    pub minimum_assurance: Assurance,
    pub required_attenuations: Vec<String>,
    pub max_grant_lifetime_seconds: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PopSession {
    pub method: ChannelMethod,
    pub key_thumbprint: String,
    pub session_id: String,
    pub proof: Vec<u8>,
}
pub trait PopVerifier: Send + Sync {
    fn verify(&self, session: &PopSession) -> bool;
}
#[derive(Clone, Debug)]
pub struct ExactPopVerifier {
    pub expected_proof: Vec<u8>,
}
impl PopVerifier for ExactPopVerifier {
    fn verify(&self, session: &PopSession) -> bool {
        session.proof == self.expected_proof
    }
}
pub trait OpaqueIdGenerator: Send + Sync {
    fn next_opaque_id(&self, prefix: &str) -> Result<String, BrokerError>;
}
pub struct SequenceIds(Mutex<u128>);
impl SequenceIds {
    pub fn new(seed: u128) -> Self {
        Self(Mutex::new(seed))
    }
}
impl OpaqueIdGenerator for SequenceIds {
    fn next_opaque_id(&self, prefix: &str) -> Result<String, BrokerError> {
        let mut value = self.0.lock().map_err(|_| BrokerError::Brk401)?;
        let result = format!("{prefix}{:032x}", *value);
        *value = value.checked_add(1).ok_or(BrokerError::Brk401)?;
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub struct IssuedGrant {
    pub grant_ref: String,
    pub jti: String,
}
#[derive(Default)]
pub struct GrantStore {
    records: Mutex<BTreeMap<String, ParsedArtifact<ExecutionGrant>>>,
}
impl GrantStore {
    pub fn get(&self, grant_ref: &str) -> Result<ExecutionGrantRecord, BrokerError> {
        let records = self.records.lock().map_err(|_| BrokerError::Brk401)?;
        let parsed = records.get(grant_ref).ok_or(BrokerError::Brk103)?;
        Ok(ExecutionGrantRecord {
            grant: parsed.view.clone(),
            canonical_bytes: parsed.canonical_bytes().to_vec(),
        })
    }
    fn insert(&self, parsed: ParsedArtifact<ExecutionGrant>) -> Result<(), BrokerError> {
        let mut records = self.records.lock().map_err(|_| BrokerError::Brk401)?;
        if records
            .insert(parsed.view.grant_ref.clone(), parsed)
            .is_some()
        {
            return Err(BrokerError::Brk401);
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct ExecutionGrantRecord {
    pub grant: ExecutionGrant,
    canonical_bytes: Vec<u8>,
}
impl ExecutionGrantRecord {
    pub fn from_grant(grant: &ExecutionGrant) -> Result<Self, BrokerError> {
        let bytes = canonical::from_serde(grant, GRANT_MAX)?.into_bytes();
        let parsed: ParsedArtifact<ExecutionGrant> = crate::artifacts::parse(&bytes)?;
        Ok(Self {
            grant: parsed.view,
            canonical_bytes: bytes,
        })
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    pub fn hash(&self) -> String {
        use sha2::Digest;
        format!(
            "sha256:{}",
            hex::encode(sha2::Sha256::digest(&self.canonical_bytes))
        )
    }
}

pub struct GrantIssuer<'a> {
    pub issuer: &'a str,
    pub clock: &'a dyn Clock,
    pub ids: &'a dyn OpaqueIdGenerator,
    pub store: &'a GrantStore,
}
impl GrantIssuer<'_> {
    #[allow(clippy::too_many_arguments)]
    pub fn issue(
        &self,
        scope: &TrustedHostScope,
        envelope: &StandingEnvelope,
        binding: &BindingAttestation,
        pop: &PopSession,
        verifier: &dyn PopVerifier,
        logical_calls: u64,
        attempts: u8,
        not_before: String,
        expires_at: String,
    ) -> Result<IssuedGrant, BrokerError> {
        if !verifier.verify(pop) {
            return Err(BrokerError::Brk102);
        }
        let now = crate::artifacts::timestamp_seconds(&self.clock.now_rfc3339())?;
        if crate::artifacts::timestamp_seconds(&binding.expires_at)? <= now {
            return Err(BrokerError::Brk105);
        }
        if binding.lane != "semantic_broker" || !binding.scope_alignment.satisfied {
            return Err(BrokerError::Brk109);
        }
        if logical_calls == 0
            || logical_calls > envelope.max_logical_calls
            || attempts == 0
            || attempts > envelope.max_dispatch_attempts_per_call
        {
            return Err(BrokerError::Brk109);
        }
        let not_before_seconds = crate::artifacts::timestamp_seconds(&not_before)?;
        let expires_at_seconds = crate::artifacts::timestamp_seconds(&expires_at)?;
        if expires_at_seconds <= not_before_seconds
            || expires_at_seconds - not_before_seconds > envelope.max_grant_lifetime_seconds as i64
        {
            return Err(BrokerError::Brk109);
        }
        if binding.connection_ref.is_empty()
            || !binding.supported_contracts.iter().any(|c| {
                c.contract_id == envelope.operation_contract
                    && c.contract_hash == envelope.contract_hash
            })
        {
            return Err(BrokerError::Brk109);
        }
        let grant_ref = self.ids.next_opaque_id("grant_")?;
        let jti = self.ids.next_opaque_id("jti_")?;
        let grant = ExecutionGrant {
            schema_version: "0.1".into(),
            critical_fields: vec![],
            org_id: scope.org_id.clone(),
            principal: PrincipalRef {
                kind: PrincipalKind::Deployment,
                id: scope.principal_id.clone(),
            },
            grant_ref: grant_ref.clone(),
            issuer: self.issuer.into(),
            audience: "broker-execution".into(),
            channel_binding: ChannelBinding {
                method: pop.method.clone(),
                key_thumbprint: pop.key_thumbprint.clone(),
                session_id: pop.session_id.clone(),
            },
            subject: GrantSubject::FlowNodeRun {
                bundle_id: scope.bundle_id.clone(),
                flow_ir_hash: scope.flow_ir_hash.clone(),
                binding_lock_hash: scope.binding_lock_hash.clone(),
                flow_id: scope.flow_id.clone(),
                node_id: scope.node_id.clone(),
                node_alias: scope.node_alias.clone(),
                run_id: scope.run_id.clone(),
            },
            operation_contract: envelope.operation_contract.clone(),
            contract_hash: envelope.contract_hash.clone(),
            connection_ref: binding.connection_ref.clone(),
            budgets: GrantBudgets {
                logical_calls,
                dispatch_attempts_per_call: attempts,
            },
            aggregate_budgets: None,
            minimum_assurance: envelope.minimum_assurance,
            required_attenuations: envelope.required_attenuations.clone(),
            revocation_epoch: binding.revocation_epoch,
            not_before,
            expires_at,
            jti: jti.clone(),
            extensions: Default::default(),
        };
        let bytes = canonical::from_serde(&grant, GRANT_MAX)?.into_bytes();
        let parsed = crate::artifacts::parse(&bytes)?;
        self.store.insert(parsed)?;
        Ok(IssuedGrant { grant_ref, jti })
    }
}

pub fn validate_grant(
    record: &ExecutionGrantRecord,
    scope: &TrustedHostScope,
    pop: &PopSession,
    verifier: &dyn PopVerifier,
    current_epoch: u64,
    now: &str,
) -> Result<(), BrokerError> {
    let grant = &record.grant;
    if !verifier.verify(pop)
        || grant.channel_binding.method != pop.method
        || grant.channel_binding.key_thumbprint != pop.key_thumbprint
        || grant.channel_binding.session_id != pop.session_id
    {
        return Err(BrokerError::Brk102);
    }
    let now = crate::artifacts::timestamp_seconds(now)?;
    if now < crate::artifacts::timestamp_seconds(&grant.not_before)? {
        return Err(BrokerError::Brk104);
    }
    if now >= crate::artifacts::timestamp_seconds(&grant.expires_at)? {
        return Err(BrokerError::Brk105);
    }
    if current_epoch != grant.revocation_epoch {
        return Err(BrokerError::Brk106);
    }
    let (bundle, flow_hash, lock_hash, flow, node, alias, run) = grant.subject.flow_node_run();
    if grant.org_id != scope.org_id
        || grant.principal.id != scope.principal_id
        || bundle != scope.bundle_id
        || flow_hash != scope.flow_ir_hash
        || lock_hash != scope.binding_lock_hash
        || flow != scope.flow_id
        || node != scope.node_id
        || alias != scope.node_alias
        || run != scope.run_id
    {
        return Err(BrokerError::Brk107);
    }
    Ok(())
}
