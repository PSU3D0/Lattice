use std::collections::BTreeMap;
use std::fmt;

use dag_core::{Determinism, Effects};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorManifest {
    pub connector: ConnectorMetadata,
    pub profiles: ConnectorProfiles,
    pub types: BTreeMap<String, TypeDecl>,
    pub surfaces: Vec<SurfaceDecl>,
}

impl ConnectorManifest {
    pub fn type_decl(&self, name: &str) -> Option<&TypeDecl> {
        self.types.get(name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMetadata {
    pub id: String,
    pub vendor: String,
    pub family: String,
    pub version: String,
    #[serde(rename = "crate")]
    pub crate_name: String,
    pub summary: String,
}

impl ConnectorMetadata {
    pub fn output_path(&self) -> String {
        format!("crates/connectors/{}/{}", self.vendor, self.family)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectorProfiles {
    #[serde(default)]
    pub outbound_auth: BTreeMap<String, OutboundAuthProfile>,
    #[serde(default)]
    pub endpoint_profiles: BTreeMap<String, EndpointProfile>,
    #[serde(default)]
    pub provisioning_auth: BTreeMap<String, ReservedProfile>,
    #[serde(default)]
    pub inbound_verifiers: BTreeMap<String, ReservedProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutboundAuthProfile {
    Bearer {
        handle_kind: String,
    },
    ApiKeyHeader {
        header_name: String,
        #[serde(default)]
        prefix: Option<String>,
        handle_kind: String,
    },
    ApiKeyQuery {
        query_name: String,
        handle_kind: String,
    },
    #[serde(rename = "oauth2")]
    OAuth2 {
        handle_kind: String,
    },
    ServiceAccountJwt {
        handle_kind: String,
    },
    SessionBootstrap {
        handle_kind: String,
    },
    SignedRequest {
        handle_kind: String,
    },
}

impl OutboundAuthProfile {
    pub fn handle_kind(&self) -> &str {
        match self {
            OutboundAuthProfile::Bearer { handle_kind }
            | OutboundAuthProfile::ApiKeyHeader { handle_kind, .. }
            | OutboundAuthProfile::ApiKeyQuery { handle_kind, .. }
            | OutboundAuthProfile::OAuth2 { handle_kind }
            | OutboundAuthProfile::ServiceAccountJwt { handle_kind }
            | OutboundAuthProfile::SessionBootstrap { handle_kind }
            | OutboundAuthProfile::SignedRequest { handle_kind } => handle_kind,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            OutboundAuthProfile::Bearer { .. } => "bearer",
            OutboundAuthProfile::ApiKeyHeader { .. } => "api_key_header",
            OutboundAuthProfile::ApiKeyQuery { .. } => "api_key_query",
            OutboundAuthProfile::OAuth2 { .. } => "oauth2",
            OutboundAuthProfile::ServiceAccountJwt { .. } => "service_account_jwt",
            OutboundAuthProfile::SessionBootstrap { .. } => "session_bootstrap",
            OutboundAuthProfile::SignedRequest { .. } => "signed_request",
        }
    }

    pub fn supports_codegen(&self) -> bool {
        matches!(
            self,
            OutboundAuthProfile::Bearer { .. }
                | OutboundAuthProfile::ApiKeyHeader { .. }
                | OutboundAuthProfile::ApiKeyQuery { .. }
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReservedProfile {
    pub kind: String,
    #[serde(default)]
    pub handle_kind: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointProfile {
    pub base_url: String,
    #[serde(default)]
    pub default_headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TypeDecl {
    Object { fields: BTreeMap<String, FieldDecl> },
    Enum { variants: Vec<String> },
}

impl TypeDecl {
    pub fn as_object_fields(&self) -> Option<&BTreeMap<String, FieldDecl>> {
        match self {
            TypeDecl::Object { fields } => Some(fields),
            TypeDecl::Enum { .. } => None,
        }
    }

    pub fn as_enum_variants(&self) -> Option<&[String]> {
        match self {
            TypeDecl::Enum { variants } => Some(variants.as_slice()),
            TypeDecl::Object { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDecl {
    #[serde(rename = "type")]
    pub kind: FieldKind,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub default: Option<DefaultValue>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub item: Option<Box<FieldDecl>>,
    #[serde(default)]
    pub escape_hatch_reason: Option<String>,
}

impl FieldDecl {
    pub fn is_numeric(&self) -> bool {
        matches!(
            self.kind,
            FieldKind::U32 | FieldKind::U64 | FieldKind::I64 | FieldKind::F64
        )
    }

    pub fn list_item(&self) -> Option<&FieldDecl> {
        self.item.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    String,
    Bool,
    U32,
    U64,
    I64,
    F64,
    Bytes,
    List,
    ObjectRef,
    EnumRef,
    Json,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DefaultValue {
    Bool(bool),
    U32(u32),
    U64(u64),
    I64(i64),
    F64(f64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SurfaceDecl {
    Action(ActionSurface),
    PollingTrigger(PollingTriggerSurface),
    WebhookTrigger(WebhookTriggerSurface),
}

impl SurfaceDecl {
    pub fn identifier(&self) -> &str {
        match self {
            SurfaceDecl::Action(surface) => &surface.identifier,
            SurfaceDecl::PollingTrigger(surface) => &surface.identifier,
            SurfaceDecl::WebhookTrigger(surface) => &surface.identifier,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            SurfaceDecl::Action(surface) => &surface.name,
            SurfaceDecl::PollingTrigger(surface) => &surface.name,
            SurfaceDecl::WebhookTrigger(surface) => &surface.name,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            SurfaceDecl::Action(_) => "action",
            SurfaceDecl::PollingTrigger(_) => "polling_trigger",
            SurfaceDecl::WebhookTrigger(_) => "webhook_trigger",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionSurface {
    pub identifier: String,
    pub name: String,
    pub summary: String,
    pub input: String,
    pub output: String,
    #[serde(default)]
    pub auth: Option<String>,
    pub endpoint: String,
    pub effects: EffectLevel,
    pub determinism: DeterminismLevel,
    #[serde(default)]
    pub resources: Vec<ResourceRequirement>,
    #[serde(default)]
    pub implementation: ActionImplementation,
    #[serde(default)]
    pub request: Option<RequestMapping>,
    #[serde(default)]
    pub pagination: Option<PaginationDecl>,
    #[serde(default)]
    pub response: Option<ResponseDecl>,
    /// Optional Broker V1 semantic capsule contract. Legacy actions omit it.
    #[serde(default)]
    pub contract: Option<OperationContract>,
    /// Broker-owned declarative provider request plan, separate from the
    /// direct-runtime `request` mapping during additive migration.
    #[serde(default)]
    pub broker_request: Option<BrokerRequestPlan>,
}

impl ActionSurface {
    pub fn response(&self) -> ResponseDecl {
        self.response.clone().unwrap_or_default()
    }

    pub fn request(&self) -> Option<&RequestMapping> {
        self.request.as_ref()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionImplementation {
    #[default]
    RequestMapped,
    HandwrittenSemantic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollingTriggerSurface {
    pub identifier: String,
    pub name: String,
    pub output: String,
    #[serde(default)]
    pub auth: Option<String>,
    #[serde(default)]
    pub lifecycle: Option<String>,
    #[serde(default)]
    pub poll: Option<ReservedTriggerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookTriggerSurface {
    pub identifier: String,
    pub name: String,
    pub output: String,
    #[serde(default)]
    pub provisioning_auth: Option<String>,
    #[serde(default)]
    pub verifier: Option<String>,
    #[serde(default)]
    pub lifecycle: Option<String>,
    #[serde(default)]
    pub webhook: Option<ReservedTriggerConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReservedTriggerConfig {
    #[serde(flatten)]
    pub fields: BTreeMap<String, serde_yaml::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMapping {
    pub method: RequestMethod,
    pub path_template: String,
    #[serde(default)]
    pub path_params: BTreeMap<String, String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub body: BTreeMap<String, String>,
    #[serde(default)]
    pub headers: BTreeMap<String, StaticHeaderDecl>,
}

/// Stable, implementation-independent Broker V1 operation contract.
///
/// [`OperationContractDescriptor`] is the exact contract-hash preimage. In
/// particular, conformance-corpus hashes and adapter/module hashes are not
/// contract semantics and MUST NOT be added to that descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationContract {
    /// Grammar: `[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+@[1-9][0-9]*`;
    /// the pre-`@` portion must exactly equal the operation identifier.
    pub contract_id: String,
    pub effect_class: EffectLevel,
    pub auth_role: String,
    #[serde(default)]
    pub minimum_scopes: Vec<String>,
    pub response_data_policy: ResponseDataPolicy,
    pub broker_abi_version: String,
    /// Sorted unique 1-128 byte identifiers over `[A-Za-z0-9._-]`.
    pub semantic_effect_slots: Vec<String>,
}

/// Exact deterministic descriptor hashed to identify an operation contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OperationContractDescriptor {
    pub auth_role: String,
    pub broker_abi_version: String,
    pub contract_id: String,
    pub effect_class: String,
    pub minimum_scopes: Vec<String>,
    pub response_data_policy: ResponseDataPolicy,
    pub semantic_effect_slots: Vec<String>,
}

impl OperationContract {
    pub fn descriptor(&self) -> OperationContractDescriptor {
        OperationContractDescriptor {
            auth_role: self.auth_role.clone(),
            broker_abi_version: self.broker_abi_version.clone(),
            contract_id: self.contract_id.clone(),
            effect_class: self.effect_class.as_broker_name().to_string(),
            minimum_scopes: self.minimum_scopes.clone(),
            response_data_policy: self.response_data_policy.clone(),
            semantic_effect_slots: self.semantic_effect_slots.clone(),
        }
    }
}

/// Bounded projection of provider response data exposed to the flow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseDataPolicy {
    /// Top-level response field names admitted by the projection.
    pub fields: Vec<String>,
    pub kind: ResponseDataPolicyKind,
    /// Maximum projected canonical JSON size. Broker V1 caps this at 256 KiB.
    pub max_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseDataPolicyKind {
    JsonProjection,
}

/// Declarative Broker V1 provider request-plan template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRequestPlan {
    pub method: RequestMethod,
    pub origin: String,
    pub path_template: String,
    #[serde(default)]
    pub placeholders: BTreeMap<String, RequestPlaceholderDecl>,
    #[serde(default)]
    pub static_headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: BTreeMap<String, String>,
}

/// A placeholder is either input-derived (`kind: input`, with
/// `input_field`) or broker-filled (`idempotency_key`, `timestamp`, or
/// `boundary`). `kind` remains a string so validation can issue a stable
/// fail-closed diagnostic for unknown future vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestPlaceholderDecl {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticHeaderDecl {
    #[serde(rename = "const")]
    pub const_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationDecl {
    pub kind: PaginationKind,
    pub enabled_from: String,
    pub page_size_param: String,
    pub page_size: u32,
    #[serde(default)]
    pub max_items_from: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaginationKind {
    LinkHeaderNext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseDecl {
    pub kind: ResponseKind,
    pub root_path: String,
}

impl Default for ResponseDecl {
    fn default() -> Self {
        Self {
            kind: ResponseKind::JsonBody,
            root_path: "body".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseKind {
    JsonBody,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectLevel {
    Pure,
    ReadOnly,
    Effectful,
}

impl EffectLevel {
    pub fn as_dag_core(self) -> Effects {
        match self {
            EffectLevel::Pure => Effects::Pure,
            EffectLevel::ReadOnly => Effects::ReadOnly,
            EffectLevel::Effectful => Effects::Effectful,
        }
    }

    pub const fn as_macro_name(self) -> &'static str {
        match self {
            EffectLevel::Pure => "Pure",
            EffectLevel::ReadOnly => "ReadOnly",
            EffectLevel::Effectful => "Effectful",
        }
    }

    pub const fn as_broker_name(self) -> &'static str {
        match self {
            EffectLevel::Pure => "pure",
            EffectLevel::ReadOnly => "read_only",
            EffectLevel::Effectful => "effectful",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match normalize_enum_token(value).as_str() {
            "pure" => Some(EffectLevel::Pure),
            "readonly" => Some(EffectLevel::ReadOnly),
            "effectful" => Some(EffectLevel::Effectful),
            _ => None,
        }
    }
}

impl Serialize for EffectLevel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_macro_name())
    }
}

impl<'de> Deserialize<'de> for EffectLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        EffectLevel::from_str(&raw)
            .ok_or_else(|| D::Error::custom(format!("unsupported effects value `{raw}`")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeterminismLevel {
    Strict,
    Stable,
    BestEffort,
    Nondeterministic,
}

impl DeterminismLevel {
    pub fn as_dag_core(self) -> Determinism {
        match self {
            DeterminismLevel::Strict => Determinism::Strict,
            DeterminismLevel::Stable => Determinism::Stable,
            DeterminismLevel::BestEffort => Determinism::BestEffort,
            DeterminismLevel::Nondeterministic => Determinism::Nondeterministic,
        }
    }

    pub const fn as_macro_name(self) -> &'static str {
        match self {
            DeterminismLevel::Strict => "Strict",
            DeterminismLevel::Stable => "Stable",
            DeterminismLevel::BestEffort => "BestEffort",
            DeterminismLevel::Nondeterministic => "Nondeterministic",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match normalize_enum_token(value).as_str() {
            "strict" => Some(DeterminismLevel::Strict),
            "stable" => Some(DeterminismLevel::Stable),
            "besteffort" => Some(DeterminismLevel::BestEffort),
            "nondeterministic" => Some(DeterminismLevel::Nondeterministic),
            _ => None,
        }
    }
}

impl Serialize for DeterminismLevel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_macro_name())
    }
}

impl<'de> Deserialize<'de> for DeterminismLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        DeterminismLevel::from_str(&raw)
            .ok_or_else(|| D::Error::custom(format!("unsupported determinism value `{raw}`")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestMethod {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
}

impl RequestMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            RequestMethod::Get => "GET",
            RequestMethod::Head => "HEAD",
            RequestMethod::Post => "POST",
            RequestMethod::Put => "PUT",
            RequestMethod::Patch => "PATCH",
            RequestMethod::Delete => "DELETE",
        }
    }

    pub const fn requires_write(self) -> bool {
        matches!(
            self,
            RequestMethod::Post | RequestMethod::Put | RequestMethod::Patch | RequestMethod::Delete
        )
    }
}

impl Serialize for RequestMethod {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RequestMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        match raw.to_ascii_uppercase().as_str() {
            "GET" => Ok(RequestMethod::Get),
            "HEAD" => Ok(RequestMethod::Head),
            "POST" => Ok(RequestMethod::Post),
            "PUT" => Ok(RequestMethod::Put),
            "PATCH" => Ok(RequestMethod::Patch),
            "DELETE" => Ok(RequestMethod::Delete),
            _ => Err(D::Error::custom(format!(
                "unsupported request method `{raw}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceRequirement {
    HttpRead,
    HttpWrite,
}

impl ResourceRequirement {
    pub const fn manifest_value(self) -> &'static str {
        match self {
            ResourceRequirement::HttpRead => "http_read(capabilities::http::HttpRead)",
            ResourceRequirement::HttpWrite => "http_write(capabilities::http::HttpWrite)",
        }
    }

    pub const fn macro_fragment(self) -> &'static str {
        self.manifest_value()
    }

    pub const fn minimum_effects(self) -> Effects {
        match self {
            ResourceRequirement::HttpRead => Effects::ReadOnly,
            ResourceRequirement::HttpWrite => Effects::Effectful,
        }
    }

    pub const fn minimum_determinism(self) -> Determinism {
        match self {
            ResourceRequirement::HttpRead | ResourceRequirement::HttpWrite => {
                Determinism::BestEffort
            }
        }
    }
}

impl Serialize for ResourceRequirement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.manifest_value())
    }
}

impl<'de> Deserialize<'de> for ResourceRequirement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        match raw.as_str() {
            "http_read(capabilities::http::HttpRead)" => Ok(ResourceRequirement::HttpRead),
            "http_write(capabilities::http::HttpWrite)" => Ok(ResourceRequirement::HttpWrite),
            _ => Err(D::Error::custom(format!(
                "unsupported resource requirement `{raw}`"
            ))),
        }
    }
}

fn normalize_enum_token(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

impl fmt::Display for EffectLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_macro_name())
    }
}

impl fmt::Display for DeterminismLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_macro_name())
    }
}
