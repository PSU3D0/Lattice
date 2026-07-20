use crate::{BrokerError, artifacts::*, canonical};
use std::{collections::BTreeMap, sync::Mutex};

/// Broker-owned clock. Both wall and monotonic readings are mandatory: no
/// zero/default monotonic clock is permitted at the lease boundary.
pub trait Clock: Send + Sync {
    fn now_rfc3339(&self) -> String;
    fn monotonic_seconds(&self) -> i64;
}
#[derive(Clone, Debug)]
pub struct FixedClock(pub String);
impl Clock for FixedClock {
    fn now_rfc3339(&self) -> String {
        self.0.clone()
    }
    fn monotonic_seconds(&self) -> i64 {
        crate::artifacts::timestamp_seconds(&self.0).expect("validated fixed test clock")
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
    pub(crate) fn from_authenticated_host(
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
/// Opaque proof-of-possession session; fields are never caller-constructible.
///
/// ```compile_fail
/// use broker_core::grant::PopSession;
/// let forged = PopSession { proof: b"guest".to_vec() };
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PopSession {
    pub(crate) method: ChannelMethod,
    pub(crate) key_thumbprint: String,
    pub(crate) session_id: String,
    pub(crate) proof: Vec<u8>,
}

/// Opaque capability minted at the authenticated broker-host bootstrap
/// boundary. It is the only production constructor for trusted scopes and PoP
/// sessions; ordinary invocation callers never receive it.
pub struct HostAuthority {
    _sealed: (),
}

/// Enter the broker host's authenticated bootstrap boundary.
#[doc(hidden)]
pub fn bootstrap_host_authority() -> HostAuthority {
    HostAuthority { _sealed: () }
}

impl HostAuthority {
    #[allow(clippy::too_many_arguments)]
    pub fn trusted_scope(
        &self,
        org_id: impl Into<String>,
        principal_id: impl Into<String>,
        bundle_id: impl Into<String>,
        flow_ir_hash: impl Into<String>,
        binding_lock_hash: impl Into<String>,
        flow_id: impl Into<String>,
        node_id: impl Into<String>,
        node_alias: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Result<TrustedHostScope, BrokerError> {
        TrustedHostScope::from_authenticated_host(
            org_id,
            principal_id,
            bundle_id,
            flow_ir_hash,
            binding_lock_hash,
            flow_id,
            node_id,
            node_alias,
            run_id,
        )
    }

    pub fn pop_session(
        &self,
        method: ChannelMethod,
        key_thumbprint: impl Into<String>,
        session_id: impl Into<String>,
        proof: Vec<u8>,
    ) -> Result<PopSession, BrokerError> {
        let session = PopSession {
            method,
            key_thumbprint: key_thumbprint.into(),
            session_id: session_id.into(),
            proof,
        };
        if session.key_thumbprint.is_empty()
            || session.session_id.is_empty()
            || session.proof.is_empty()
            || session.proof.len() > 1024
        {
            return Err(BrokerError::Brk102);
        }
        Ok(session)
    }
}

impl PopSession {
    #[cfg(any(test, feature = "test_fixtures"))]
    pub fn test_fixture(
        method: ChannelMethod,
        key_thumbprint: impl Into<String>,
        session_id: impl Into<String>,
        proof: Vec<u8>,
    ) -> Result<Self, BrokerError> {
        bootstrap_host_authority().pop_session(method, key_thumbprint, session_id, proof)
    }
}
pub trait PopVerifier: Send + Sync {
    fn verify(&self, session: &PopSession) -> bool;
}
#[derive(Clone)]
pub struct ConfiguredPopVerifier {
    expected_proof: Vec<u8>,
}
impl ConfiguredPopVerifier {
    pub fn new(expected_proof: Vec<u8>) -> Result<Self, BrokerError> {
        if expected_proof.is_empty() || expected_proof.len() > 1024 {
            return Err(BrokerError::Brk102);
        }
        Ok(Self { expected_proof })
    }
}
impl PopVerifier for ConfiguredPopVerifier {
    fn verify(&self, session: &PopSession) -> bool {
        use subtle::ConstantTimeEq;
        session.proof.len() == self.expected_proof.len()
            && bool::from(session.proof.ct_eq(&self.expected_proof))
    }
}
impl Drop for ConfiguredPopVerifier {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.expected_proof.zeroize();
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct ExactPopVerifier {
    pub expected_proof: Vec<u8>,
}
#[cfg(test)]
impl PopVerifier for ExactPopVerifier {
    fn verify(&self, session: &PopSession) -> bool {
        use subtle::ConstantTimeEq;
        session.proof.len() == self.expected_proof.len()
            && bool::from(session.proof.ct_eq(&self.expected_proof))
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
    pub(crate) grant: ExecutionGrant,
    canonical_bytes: Vec<u8>,
}
impl ExecutionGrantRecord {
    #[cfg(any(test, feature = "test_fixtures"))]
    pub fn from_grant(grant: &ExecutionGrant) -> Result<Self, BrokerError> {
        let bytes = canonical::from_serde(grant, GRANT_MAX)?.into_bytes();
        let parsed: ParsedArtifact<ExecutionGrant> = crate::artifacts::parse(&bytes)?;
        Ok(Self {
            grant: parsed.view,
            canonical_bytes: bytes,
        })
    }
    pub fn grant(&self) -> &ExecutionGrant {
        &self.grant
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
    pub pop_verifier: &'a dyn PopVerifier,
}
impl GrantIssuer<'_> {
    #[allow(clippy::too_many_arguments)]
    pub fn issue(
        &self,
        scope: &TrustedHostScope,
        envelope: &StandingEnvelope,
        binding: &BindingAttestation,
        pop: &PopSession,
        logical_calls: u64,
        attempts: u8,
        not_before: String,
        expires_at: String,
    ) -> Result<IssuedGrant, BrokerError> {
        if !self.pop_verifier.verify(pop) {
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
            provider: binding.provider.clone(),
            account_commitment: binding.account_commitment.clone(),
            roles: binding.roles.clone(),
            scopes: binding.scope_alignment.actual_scopes.clone(),
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
