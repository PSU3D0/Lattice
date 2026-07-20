use crate::{BrokerError, canonical};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};

pub const MANIFEST_MAX: usize = 1024 * 1024;
pub const BINDING_MAX: usize = 1024 * 1024;
pub const GRANT_MAX: usize = 64 * 1024;
pub const RECEIPT_MAX: usize = 64 * 1024;

pub type Extensions = BTreeMap<String, serde_json::Value>;

#[derive(Clone, Debug)]
pub struct ParsedArtifact<T> {
    pub view: T,
    canonical: canonical::CanonicalJson,
}
impl<T> ParsedArtifact<T> {
    pub fn canonical_bytes(&self) -> &[u8] {
        self.canonical.as_bytes()
    }
    pub fn content_hash(&self) -> String {
        use sha2::Digest;
        format!(
            "sha256:{}",
            hex::encode(sha2::Sha256::digest(self.canonical.as_bytes()))
        )
    }
}

pub trait Artifact: DeserializeOwned {
    const MAX_BYTES: usize;
    const KNOWN_ROOTS: &'static [&'static str];
    fn schema_version(&self) -> &str;
    fn critical_fields(&self) -> &[String];
    fn validate_vocabulary(&self) -> Result<(), BrokerError> {
        Ok(())
    }
}

pub fn parse<T: Artifact>(source: &[u8]) -> Result<ParsedArtifact<T>, BrokerError> {
    let canonical = canonical::canonicalize_bounded(source, T::MAX_BYTES)?;
    let raw: serde_json::Value =
        serde_json::from_slice(canonical.as_bytes()).map_err(|_| BrokerError::Brk001)?;
    let view: T = serde_json::from_slice(canonical.as_bytes()).map_err(|_| BrokerError::Brk004)?;
    if view.schema_version() != "0.1" {
        return Err(BrokerError::Brk002);
    }
    validate_json_primitives(&raw, 0)?;
    check_critical(&raw, view.critical_fields(), T::KNOWN_ROOTS)?;
    view.validate_vocabulary()?;
    Ok(ParsedArtifact { view, canonical })
}

fn check_critical(
    raw: &serde_json::Value,
    pointers: &[String],
    known: &[&str],
) -> Result<(), BrokerError> {
    let mut previous: Option<&str> = None;
    for pointer in pointers {
        if pointer.is_empty()
            || !pointer.starts_with('/')
            || invalid_pointer(pointer)
            || previous.is_some_and(|p| p >= pointer.as_str())
            || raw.pointer(pointer).is_none()
        {
            return Err(BrokerError::Brk003);
        }
        let mut segments = pointer[1..].split('/');
        let root = segments
            .next()
            .unwrap_or_default()
            .replace("~1", "/")
            .replace("~0", "~");
        // Standard fields never need to be marked critical in 0.1. Nested
        // pointers name extensions unless a later schema explicitly registers
        // their complete path; fail closed rather than accepting by prefix.
        if !known.contains(&root.as_str()) || segments.next().is_some() {
            return Err(BrokerError::Brk003);
        }
        previous = Some(pointer);
    }
    Ok(())
}
fn invalid_pointer(pointer: &str) -> bool {
    pointer
        .as_bytes()
        .windows(2)
        .any(|w| w[0] == b'~' && !matches!(w[1], b'0' | b'1'))
        || pointer.ends_with('~')
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    Service,
    Deployment,
    Broker,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PrincipalRef {
    pub kind: PrincipalKind,
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Budget {
    pub max_logical_calls: u64,
    pub max_dispatch_attempts_per_call: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationAuthority {
    pub contract_id: String,
    pub contract_hash: String,
    pub call_budget: Budget,
    pub minimum_assurance: Assurance,
    pub required_attenuations: Vec<String>,
    #[serde(default)]
    pub connection_aggregate_key: Option<String>,
    #[serde(flatten)]
    pub extensions: Extensions,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeAuthority {
    pub node_id: String,
    pub operations: Vec<OperationAuthority>,
    #[serde(flatten)]
    pub extensions: Extensions,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateCeiling {
    pub max_logical_calls: u64,
    #[serde(flatten)]
    pub extensions: Extensions,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateCeilings {
    #[serde(default)]
    pub flow: Option<AggregateCeiling>,
    #[serde(default)]
    pub connections: BTreeMap<String, AggregateCeiling>,
    #[serde(flatten)]
    pub extensions: Extensions,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FlowAuthorityManifest {
    pub schema_version: String,
    pub critical_fields: Vec<String>,
    pub org_id: String,
    pub principal: PrincipalRef,
    pub flow_ir_hash: String,
    pub nodes: BTreeMap<String, NodeAuthority>,
    #[serde(default)]
    pub aggregate_ceilings: Option<AggregateCeilings>,
    #[serde(flatten)]
    pub extensions: Extensions,
}
impl Artifact for FlowAuthorityManifest {
    const MAX_BYTES: usize = MANIFEST_MAX;
    const KNOWN_ROOTS: &'static [&'static str] = &[
        "schema_version",
        "critical_fields",
        "org_id",
        "principal",
        "flow_ir_hash",
        "nodes",
        "aggregate_ceilings",
    ];
    fn schema_version(&self) -> &str {
        &self.schema_version
    }
    fn critical_fields(&self) -> &[String] {
        &self.critical_fields
    }
    fn validate_vocabulary(&self) -> Result<(), BrokerError> {
        validate_hash(&self.flow_ir_hash)?;
        if self.nodes.is_empty() || self.nodes.len() > 4096 {
            return Err(BrokerError::Brk001);
        }
        for node in self.nodes.values() {
            if node.operations.is_empty() {
                return Err(BrokerError::Brk001);
            }
            let mut hashes = BTreeSet::new();
            for operation in &node.operations {
                validate_hash(&operation.contract_hash)?;
                if operation.call_budget.max_logical_calls == 0
                    || operation.call_budget.max_logical_calls > canonical::MAX_EXACT_INTEGER as u64
                    || operation.call_budget.max_dispatch_attempts_per_call == 0
                    || !hashes.insert(&operation.contract_hash)
                {
                    return Err(BrokerError::Brk001);
                }
                sorted_unique(&operation.required_attenuations)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Assurance {
    Direct,
    BrokeredCount,
    BrokeredSemantic,
    ProviderEnforced,
    Verifiable,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CommitmentEnvelope {
    pub alg: CommitmentAlg,
    pub key_id: String,
    #[serde(default)]
    pub verification_tier: Option<VerificationTier>,
    pub value: String,
    #[serde(flatten)]
    pub extensions: Extensions,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CommitmentAlg {
    HmacSha256,
}
impl Default for CommitmentEnvelope {
    fn default() -> Self {
        Self {
            alg: CommitmentAlg::HmacSha256,
            key_id: String::new(),
            verification_tier: None,
            value: String::new(),
            extensions: Default::default(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum VerificationTier {
    Public,
    VerifierWithDisclosure,
    BrokerOnly,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScopeAlignment {
    pub required_scopes: Vec<String>,
    pub actual_scopes: Vec<String>,
    pub satisfied: bool,
    #[serde(flatten)]
    pub extensions: Extensions,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SupportedContract {
    pub contract_id: String,
    pub contract_hash: String,
    #[serde(default)]
    pub observed_plugin_module_sha256: Option<String>,
    pub attenuation_profiles: Vec<String>,
    #[serde(flatten)]
    pub extensions: Extensions,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SignatureEnvelope {
    pub alg: SignatureAlg,
    pub key_id: String,
    pub value: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum SignatureAlg {
    Ed25519,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BindingAttestation {
    pub schema_version: String,
    pub critical_fields: Vec<String>,
    pub org_id: String,
    pub principal: PrincipalRef,
    pub issuer: String,
    pub broker_key_id: String,
    pub lane: String,
    pub connection_ref: String,
    pub provider: String,
    pub account_commitment: CommitmentEnvelope,
    pub roles: BTreeMap<String, String>,
    pub scope_alignment: ScopeAlignment,
    pub supported_contracts: Vec<SupportedContract>,
    pub endpoint_origins: Vec<String>,
    pub revocation_epoch: u64,
    pub observed_at: String,
    pub expires_at: String,
    pub signature: SignatureEnvelope,
    #[serde(flatten)]
    pub extensions: Extensions,
}
impl Artifact for BindingAttestation {
    const MAX_BYTES: usize = BINDING_MAX;
    const KNOWN_ROOTS: &'static [&'static str] = &[
        "schema_version",
        "critical_fields",
        "org_id",
        "principal",
        "issuer",
        "broker_key_id",
        "lane",
        "connection_ref",
        "provider",
        "account_commitment",
        "roles",
        "scope_alignment",
        "supported_contracts",
        "endpoint_origins",
        "revocation_epoch",
        "observed_at",
        "expires_at",
        "signature",
    ];
    fn schema_version(&self) -> &str {
        &self.schema_version
    }
    fn critical_fields(&self) -> &[String] {
        &self.critical_fields
    }
    fn validate_vocabulary(&self) -> Result<(), BrokerError> {
        if self.principal.kind != PrincipalKind::Broker
            || self.lane != "semantic_broker"
            || self.signature.key_id != self.broker_key_id
            || self.account_commitment.alg != CommitmentAlg::HmacSha256
        {
            return Err(BrokerError::Brk004);
        }
        if !self.scope_alignment.satisfied
            || self.roles.is_empty()
            || self.supported_contracts.is_empty()
            || timestamp_seconds(&self.expires_at)? <= timestamp_seconds(&self.observed_at)?
        {
            return Err(BrokerError::Brk109);
        }
        for (role, kind) in &self.roles {
            if !(role == "role" || role.starts_with("outbound_auth."))
                || !matches!(kind.as_str(), "oauth2.access_token" | "synthetic.secret")
            {
                return Err(BrokerError::Brk004);
            }
        }
        sorted_unique(&self.scope_alignment.required_scopes)?;
        sorted_unique(&self.scope_alignment.actual_scopes)?;
        sorted_unique(&self.endpoint_origins)?;
        let mut hashes = BTreeSet::new();
        for contract in &self.supported_contracts {
            validate_hash(&contract.contract_hash)?;
            if !hashes.insert(&contract.contract_hash) {
                return Err(BrokerError::Brk001);
            }
            sorted_unique(&contract.attenuation_profiles)?;
        }
        for origin in &self.endpoint_origins {
            crate::dispatch::validate_https_origin(origin).map_err(|_| BrokerError::Brk001)?;
        }
        validate_commitment(&self.account_commitment)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChannelMethod {
    Mtls,
    Spiffe,
    DeploymentKey,
    WorkersPrivateBinding,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ChannelBinding {
    pub method: ChannelMethod,
    pub key_thumbprint: String,
    pub session_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GrantSubject {
    FlowNodeRun {
        bundle_id: String,
        flow_ir_hash: String,
        binding_lock_hash: String,
        flow_id: String,
        node_id: String,
        node_alias: String,
        run_id: String,
    },
}
impl GrantSubject {
    pub fn flow_node_run(&self) -> (&str, &str, &str, &str, &str, &str, &str) {
        match self {
            Self::FlowNodeRun {
                bundle_id,
                flow_ir_hash,
                binding_lock_hash,
                flow_id,
                node_id,
                node_alias,
                run_id,
            } => (
                bundle_id,
                flow_ir_hash,
                binding_lock_hash,
                flow_id,
                node_id,
                node_alias,
                run_id,
            ),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrantBudgets {
    pub logical_calls: u64,
    pub dispatch_attempts_per_call: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateBudgets {
    pub flow_logical_calls: u64,
    pub connection_logical_calls: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionGrant {
    pub schema_version: String,
    pub critical_fields: Vec<String>,
    pub org_id: String,
    pub principal: PrincipalRef,
    pub grant_ref: String,
    pub issuer: String,
    pub audience: String,
    pub channel_binding: ChannelBinding,
    pub subject: GrantSubject,
    pub operation_contract: String,
    pub contract_hash: String,
    pub connection_ref: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub account_commitment: CommitmentEnvelope,
    #[serde(default)]
    pub roles: BTreeMap<String, String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub budgets: GrantBudgets,
    #[serde(default)]
    pub aggregate_budgets: Option<AggregateBudgets>,
    pub minimum_assurance: Assurance,
    pub required_attenuations: Vec<String>,
    pub revocation_epoch: u64,
    pub not_before: String,
    pub expires_at: String,
    pub jti: String,
    #[serde(flatten)]
    pub extensions: Extensions,
}
impl Artifact for ExecutionGrant {
    const MAX_BYTES: usize = GRANT_MAX;
    const KNOWN_ROOTS: &'static [&'static str] = &[
        "schema_version",
        "critical_fields",
        "org_id",
        "principal",
        "grant_ref",
        "issuer",
        "audience",
        "channel_binding",
        "subject",
        "operation_contract",
        "contract_hash",
        "connection_ref",
        "provider",
        "account_commitment",
        "roles",
        "scopes",
        "budgets",
        "aggregate_budgets",
        "minimum_assurance",
        "required_attenuations",
        "revocation_epoch",
        "not_before",
        "expires_at",
        "jti",
    ];
    fn schema_version(&self) -> &str {
        &self.schema_version
    }
    fn critical_fields(&self) -> &[String] {
        &self.critical_fields
    }
    fn validate_vocabulary(&self) -> Result<(), BrokerError> {
        if self.principal.kind != PrincipalKind::Deployment || self.audience != "broker-execution" {
            return Err(BrokerError::Brk004);
        }
        if self.budgets.logical_calls == 0
            || self.budgets.logical_calls > canonical::MAX_EXACT_INTEGER as u64
            || self.budgets.dispatch_attempts_per_call == 0
            || timestamp_seconds(&self.expires_at)? <= timestamp_seconds(&self.not_before)?
        {
            return Err(BrokerError::Brk001);
        }
        validate_hash(&self.contract_hash)?;
        validate_commitment(&self.account_commitment)?;
        if self.roles.is_empty() {
            return Err(BrokerError::Brk109);
        }
        sorted_unique(&self.scopes)?;
        validate_opaque_id(&self.grant_ref)?;
        validate_opaque_id(&self.jti)?;
        validate_opaque_id(&self.channel_binding.session_id)?;
        sorted_unique(&self.required_attenuations)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PluginTrustTier {
    LatticeFirstParty,
    OperatorApproved,
    SignedThirdParty,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Confirmed,
    Rejected,
    Failed,
    Ambiguous,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptClaims {
    pub trusted_host_scope_authenticated: bool,
    pub broker_admission_enforced: bool,
    pub provider_dispatch_observed: bool,
    pub remote_durable_state_proven: bool,
    pub verifiable_execution_proven: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvocationReceipt {
    pub schema_version: String,
    pub critical_fields: Vec<String>,
    pub org_id: String,
    pub principal: PrincipalRef,
    pub issuer: String,
    pub broker_key_id: String,
    pub grant_hash: String,
    pub policy_hash: String,
    pub contract_hash: String,
    pub plugin_module_sha256: String,
    pub plugin_trust_tier: PluginTrustTier,
    pub bundle_id: String,
    pub flow_ir_hash: String,
    pub binding_lock_hash: String,
    pub flow_id: String,
    pub run_id: String,
    pub node_id: String,
    pub node_alias: String,
    pub logical_effect_id: String,
    pub dispatch_attempt: u64,
    pub connection_commitment: CommitmentEnvelope,
    pub canonical_input_commitment: CommitmentEnvelope,
    #[serde(default)]
    pub request_plan_hash: Option<String>,
    #[serde(default)]
    pub authority_facts_hash: Option<String>,
    pub budget_before: u64,
    pub budget_after: u64,
    #[serde(default)]
    pub provider_request_id: Option<String>,
    pub response_commitment: CommitmentEnvelope,
    pub outcome: Outcome,
    pub claims: ReceiptClaims,
    pub issued_at: String,
    pub signature: SignatureEnvelope,
    #[serde(flatten)]
    pub extensions: Extensions,
}
impl Artifact for InvocationReceipt {
    const MAX_BYTES: usize = RECEIPT_MAX;
    const KNOWN_ROOTS: &'static [&'static str] = &[
        "schema_version",
        "critical_fields",
        "org_id",
        "principal",
        "issuer",
        "broker_key_id",
        "grant_hash",
        "policy_hash",
        "contract_hash",
        "plugin_module_sha256",
        "plugin_trust_tier",
        "bundle_id",
        "flow_ir_hash",
        "binding_lock_hash",
        "flow_id",
        "run_id",
        "node_id",
        "node_alias",
        "logical_effect_id",
        "dispatch_attempt",
        "connection_commitment",
        "canonical_input_commitment",
        "request_plan_hash",
        "authority_facts_hash",
        "budget_before",
        "budget_after",
        "provider_request_id",
        "response_commitment",
        "outcome",
        "claims",
        "issued_at",
        "signature",
    ];
    fn schema_version(&self) -> &str {
        &self.schema_version
    }
    fn critical_fields(&self) -> &[String] {
        &self.critical_fields
    }
    fn validate_vocabulary(&self) -> Result<(), BrokerError> {
        self.validate_semantics()?;
        if self.principal.kind != PrincipalKind::Broker
            || self.signature.key_id != self.broker_key_id
            || self.claims.remote_durable_state_proven
            || self.claims.verifiable_execution_proven
        {
            return Err(BrokerError::Brk004);
        }
        if self
            .provider_request_id
            .as_ref()
            .is_some_and(|v| v.len() > 128 || !v.bytes().all(|b| (0x20..=0x7e).contains(&b)))
        {
            return Err(BrokerError::Brk001);
        }
        Ok(())
    }
}

impl InvocationReceipt {
    /// Single strict semantic validator used before signing and after signature
    /// verification/redelivery.
    pub fn validate_semantics(&self) -> Result<(), BrokerError> {
        for hash in [
            &self.grant_hash,
            &self.policy_hash,
            &self.contract_hash,
            &self.plugin_module_sha256,
        ] {
            validate_hash(hash)?;
        }
        if self.budget_before < self.budget_after {
            return Err(BrokerError::Brk001);
        }
        let dispatched = self.dispatch_attempt > 0;
        if dispatched != self.claims.provider_dispatch_observed
            || dispatched != self.request_plan_hash.is_some()
            || dispatched != self.authority_facts_hash.is_some()
            || (!dispatched && self.outcome != Outcome::Rejected)
        {
            return Err(BrokerError::Brk004);
        }
        if let Some(hash) = &self.request_plan_hash {
            validate_hash(hash)?;
        }
        if let Some(hash) = &self.authority_facts_hash {
            validate_hash(hash)?;
        }
        validate_commitment(&self.connection_commitment)?;
        validate_commitment(&self.canonical_input_commitment)?;
        validate_commitment(&self.response_commitment)?;
        Ok(())
    }
}

fn validate_json_primitives(value: &serde_json::Value, depth: usize) -> Result<(), BrokerError> {
    if depth > canonical::MAX_DEPTH {
        return Err(BrokerError::Brk001);
    }
    match value {
        serde_json::Value::String(value) => validate_string(value),
        serde_json::Value::Array(values) => {
            if values.len() > canonical::MAX_ARRAY_ELEMENTS {
                return Err(BrokerError::Brk001);
            }
            for value in values {
                validate_json_primitives(value, depth + 1)?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            if values.len() > canonical::MAX_OBJECT_MEMBERS {
                return Err(BrokerError::Brk001);
            }
            for (key, value) in values {
                validate_string(key)?;
                validate_json_primitives(value, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
fn validate_string(value: &str) -> Result<(), BrokerError> {
    if value.is_empty() || value.len() > 1024 {
        Err(BrokerError::Brk001)
    } else {
        Ok(())
    }
}
fn validate_opaque_id(value: &str) -> Result<(), BrokerError> {
    validate_string(value)?;
    if value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        Ok(())
    } else {
        Err(BrokerError::Brk001)
    }
}
fn validate_hash(value: &str) -> Result<(), BrokerError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(BrokerError::Brk001);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        Err(BrokerError::Brk001)
    } else {
        Ok(())
    }
}
fn validate_commitment(value: &CommitmentEnvelope) -> Result<(), BrokerError> {
    let Some(hex) = value.value.strip_prefix("hmac-sha256:") else {
        return Err(BrokerError::Brk001);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        Err(BrokerError::Brk001)
    } else {
        Ok(())
    }
}

fn sorted_unique(values: &[String]) -> Result<(), BrokerError> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        Err(BrokerError::Brk004)
    } else {
        Ok(())
    }
}

pub(crate) fn timestamp_seconds(value: &str) -> Result<i64, BrokerError> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return Err(BrokerError::Brk001);
    }
    let number = |start: usize, end: usize| -> Result<i64, BrokerError> {
        let part = &value[start..end];
        if !part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(BrokerError::Brk001);
        }
        part.parse().map_err(|_| BrokerError::Brk001)
    };
    let year = number(0, 4)?;
    let month = number(5, 7)?;
    let day = number(8, 10)?;
    let hour = number(11, 13)?;
    let minute = number(14, 16)?;
    let second = number(17, 19)?;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return Err(BrokerError::Brk001);
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day == 0 || day > days_in_month[(month - 1) as usize] {
        return Err(BrokerError::Brk001);
    }
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let adjusted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    Ok(days * 86_400 + hour * 3_600 + minute * 60 + second)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn critical_and_version_fail_closed() {
        let unknown = br#"{"schema_version":"0.1","critical_fields":["/future"],"org_id":"o","principal":{"kind":"deployment","id":"d"},"grant_ref":"g","issuer":"i","audience":"broker-execution","channel_binding":{"method":"mtls","key_thumbprint":"h","session_id":"s"},"subject":{"kind":"flow_node_run","bundle_id":"b","flow_ir_hash":"h","binding_lock_hash":"h","flow_id":"f","node_id":"n","node_alias":"a","run_id":"r"},"operation_contract":"c","contract_hash":"h","connection_ref":"c","budgets":{"logical_calls":1,"dispatch_attempts_per_call":1},"minimum_assurance":"brokered_count","required_attenuations":[],"revocation_epoch":0,"not_before":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:01:00Z","jti":"j","future":true}"#;
        assert_eq!(
            parse::<ExecutionGrant>(unknown).unwrap_err(),
            BrokerError::Brk003
        );
        let old = String::from_utf8(unknown.to_vec())
            .unwrap()
            .replace("\"0.1\"", "\"9\"");
        assert_eq!(
            parse::<ExecutionGrant>(old.as_bytes()).unwrap_err(),
            BrokerError::Brk002
        );
        let unknown_subject = String::from_utf8(unknown.to_vec())
            .unwrap()
            .replace("flow_node_run", "future_subject");
        assert_eq!(
            parse::<ExecutionGrant>(unknown_subject.as_bytes()).unwrap_err(),
            BrokerError::Brk004
        );
    }

    #[test]
    fn receipt_claims_are_a_closed_five_boolean_object() {
        let exact = br#"{"trusted_host_scope_authenticated":true,"broker_admission_enforced":true,"provider_dispatch_observed":true,"remote_durable_state_proven":false,"verifiable_execution_proven":false}"#;
        assert!(serde_json::from_slice::<ReceiptClaims>(exact).is_ok());
        let additional = br#"{"trusted_host_scope_authenticated":true,"broker_admission_enforced":true,"provider_dispatch_observed":true,"remote_durable_state_proven":false,"verifiable_execution_proven":false,"future":false}"#;
        assert!(serde_json::from_slice::<ReceiptClaims>(additional).is_err());
        let missing = br#"{"trusted_host_scope_authenticated":true,"broker_admission_enforced":true,"provider_dispatch_observed":true,"remote_durable_state_proven":false}"#;
        assert!(serde_json::from_slice::<ReceiptClaims>(missing).is_err());
    }
}
