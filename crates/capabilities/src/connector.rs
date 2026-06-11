use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::http::HttpRequest;

/// Connector-resolved effect hints recorded at bindings.lock generation time,
/// keyed by node alias.
///
/// These are the *connection-dependent* capability requirements of
/// bound-connection connector ops (e.g. a SheetPort connection storing its
/// workbook in blob storage requires `resource::blob::read`). They are
/// resolved ONCE, when the lock is generated, and carried as data from then
/// on: preflight reads them from the resource view (packet C2) and never
/// calls [`ConnectorRuntime::resolve_required_effect_hints`] itself.
///
/// A node alias *present* in the map (even with an empty set) means "resolved
/// and recorded at lock time"; an *absent* alias means "resolution was never
/// recorded" and bound-connection preflight fails closed for that node when a
/// lock-backed resource view is in use.
pub type ConnectorResolvedEffectHints = BTreeMap<String, BTreeSet<dag_core::EffectHint>>;

/// Runtime scope attached to the currently executing connector node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorBindingScope {
    pub flow_id: String,
    pub node_alias: String,
    pub node_identifier: String,
    pub connector_id: String,
}

impl ConnectorBindingScope {
    pub fn new(
        flow_id: impl Into<String>,
        node_alias: impl Into<String>,
        node_identifier: impl Into<String>,
        connector_id: impl Into<String>,
    ) -> Self {
        Self {
            flow_id: flow_id.into(),
            node_alias: node_alias.into(),
            node_identifier: node_identifier.into(),
            connector_id: connector_id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorRoleKind {
    OutboundAuth,
    ProvisioningAuth,
    InboundVerifier,
    EndpointProfile,
}

#[derive(Debug, Clone, Copy)]
pub struct OutboundAuthProfileDescriptor {
    pub connector_id: &'static str,
    pub name: &'static str,
    /// Transitional Phase-1 dev-adapter hint; not part of the intended long-term
    /// host-agnostic connector binding contract.
    pub env_var: &'static str,
    pub kind: OutboundAuthKind,
}

#[derive(Debug, Clone, Copy)]
pub enum OutboundAuthKind {
    Bearer {
        handle_kind: &'static str,
    },
    ApiKeyHeader {
        header_name: &'static str,
        prefix: Option<&'static str>,
        handle_kind: &'static str,
    },
    ApiKeyQuery {
        query_name: &'static str,
        handle_kind: &'static str,
    },
    Unsupported {
        kind_name: &'static str,
        handle_kind: &'static str,
    },
}

impl OutboundAuthKind {
    pub const fn kind_name(self) -> &'static str {
        match self {
            OutboundAuthKind::Bearer { .. } => "bearer",
            OutboundAuthKind::ApiKeyHeader { .. } => "api_key_header",
            OutboundAuthKind::ApiKeyQuery { .. } => "api_key_query",
            OutboundAuthKind::Unsupported { kind_name, .. } => kind_name,
        }
    }

    pub const fn handle_kind(self) -> &'static str {
        match self {
            OutboundAuthKind::Bearer { handle_kind }
            | OutboundAuthKind::ApiKeyHeader { handle_kind, .. }
            | OutboundAuthKind::ApiKeyQuery { handle_kind, .. }
            | OutboundAuthKind::Unsupported { handle_kind, .. } => handle_kind,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EndpointProfileDescriptor {
    pub connector_id: &'static str,
    pub name: &'static str,
    /// Transitional Phase-1 dev-adapter hint; not part of the intended long-term
    /// host-agnostic connector binding contract.
    pub env_base_url_var: &'static str,
    pub base_url: &'static str,
    pub default_headers: &'static [(&'static str, &'static str)],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedEndpointProfile {
    pub base_url: String,
    pub default_headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedConnectorConnection {
    pub connection_name: Option<String>,
    pub connector_id: String,
    pub config: JsonValue,
}

#[derive(Debug, Error)]
pub enum ConnectorRuntimeError {
    #[error("connector auth profile `{role_name}` requires local env override `{env_var}`")]
    MissingAuthOverride {
        role_name: &'static str,
        env_var: &'static str,
    },
    #[error("connector auth profile `{role_name}` uses unsupported auth kind `{kind}`")]
    UnsupportedAuthKind {
        role_name: &'static str,
        kind: &'static str,
    },
    #[error("connector endpoint profile `{role_name}` is invalid: {reason}")]
    InvalidEndpointProfile {
        role_name: &'static str,
        reason: String,
    },
    #[error(transparent)]
    Provider(#[from] anyhow::Error),
}

#[async_trait]
pub trait ConnectorRuntime: Send + Sync {
    async fn apply_outbound_auth(
        &self,
        scope: &ConnectorBindingScope,
        profile: &OutboundAuthProfileDescriptor,
        request: &mut HttpRequest,
    ) -> Result<(), ConnectorRuntimeError>;

    async fn resolve_endpoint_profile(
        &self,
        scope: &ConnectorBindingScope,
        profile: &EndpointProfileDescriptor,
    ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError>;

    async fn resolve_connection(
        &self,
        _scope: &ConnectorBindingScope,
    ) -> Result<Option<ResolvedConnectorConnection>, ConnectorRuntimeError> {
        Ok(None)
    }

    async fn resolve_required_effect_hints(
        &self,
        _scope: &ConnectorBindingScope,
        _selected_mode: dag_core::ConnectorResolutionModeDecl,
    ) -> Result<Vec<String>, ConnectorRuntimeError> {
        Ok(Vec::new())
    }
}

/// Opcode family reserved for connector runtime bridge operations.
///
/// Encoding: `(family << 16) | op_id`.
pub const OP_FAMILY_CONNECTOR: u32 = 3;
pub const OP_CONNECTOR_GET_SCOPE: u32 = (OP_FAMILY_CONNECTOR << 16) | 1;
pub const OP_CONNECTOR_APPLY_OUTBOUND_AUTH: u32 = (OP_FAMILY_CONNECTOR << 16) | 2;
pub const OP_CONNECTOR_RESOLVE_ENDPOINT_PROFILE: u32 = (OP_FAMILY_CONNECTOR << 16) | 3;
pub const OP_CONNECTOR_RESOLVE_CONNECTION: u32 = (OP_FAMILY_CONNECTOR << 16) | 4;

#[cfg(target_arch = "wasm32")]
const RESP_OK: u8 = 0;
#[cfg(target_arch = "wasm32")]
const RESP_NOT_FOUND: u8 = 1;
#[cfg(target_arch = "wasm32")]
const RESP_ERR: u8 = 2;

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TransportOutboundAuthKind {
    Bearer {
        handle_kind: String,
    },
    ApiKeyHeader {
        header_name: String,
        prefix: Option<String>,
        handle_kind: String,
    },
    ApiKeyQuery {
        query_name: String,
        handle_kind: String,
    },
    Unsupported {
        kind_name: String,
        handle_kind: String,
    },
}

#[cfg(target_arch = "wasm32")]
impl From<OutboundAuthKind> for TransportOutboundAuthKind {
    fn from(value: OutboundAuthKind) -> Self {
        match value {
            OutboundAuthKind::Bearer { handle_kind } => Self::Bearer {
                handle_kind: handle_kind.to_string(),
            },
            OutboundAuthKind::ApiKeyHeader {
                header_name,
                prefix,
                handle_kind,
            } => Self::ApiKeyHeader {
                header_name: header_name.to_string(),
                prefix: prefix.map(str::to_string),
                handle_kind: handle_kind.to_string(),
            },
            OutboundAuthKind::ApiKeyQuery {
                query_name,
                handle_kind,
            } => Self::ApiKeyQuery {
                query_name: query_name.to_string(),
                handle_kind: handle_kind.to_string(),
            },
            OutboundAuthKind::Unsupported {
                kind_name,
                handle_kind,
            } => Self::Unsupported {
                kind_name: kind_name.to_string(),
                handle_kind: handle_kind.to_string(),
            },
        }
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransportOutboundAuthProfileDescriptor {
    connector_id: String,
    name: String,
    kind: TransportOutboundAuthKind,
}

#[cfg(target_arch = "wasm32")]
impl From<&OutboundAuthProfileDescriptor> for TransportOutboundAuthProfileDescriptor {
    fn from(value: &OutboundAuthProfileDescriptor) -> Self {
        Self {
            connector_id: value.connector_id.to_string(),
            name: value.name.to_string(),
            kind: value.kind.into(),
        }
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransportEndpointProfileDescriptor {
    connector_id: String,
    name: String,
    base_url: String,
    default_headers: Vec<(String, String)>,
}

#[cfg(target_arch = "wasm32")]
impl From<&EndpointProfileDescriptor> for TransportEndpointProfileDescriptor {
    fn from(value: &EndpointProfileDescriptor) -> Self {
        Self {
            connector_id: value.connector_id.to_string(),
            name: value.name.to_string(),
            base_url: value.base_url.to_string(),
            default_headers: value
                .default_headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        }
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Serialize, Deserialize)]
struct ApplyOutboundAuthRequest {
    scope: ConnectorBindingScope,
    profile: TransportOutboundAuthProfileDescriptor,
    request: HttpRequest,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Serialize, Deserialize)]
struct ResolveEndpointProfileRequest {
    scope: ConnectorBindingScope,
    profile: TransportEndpointProfileDescriptor,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Serialize, Deserialize)]
struct ResolveConnectionRequest {
    scope: ConnectorBindingScope,
}

#[cfg(target_arch = "wasm32")]
fn decode_remote_scope(
    bytes: &[u8],
) -> Result<Option<ConnectorBindingScope>, ConnectorRuntimeError> {
    if bytes.is_empty() {
        return Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            "invalid remote connector scope response: empty"
        )));
    }
    match bytes[0] {
        RESP_OK => serde_json::from_slice(&bytes[1..])
            .map(Some)
            .map_err(|err| {
                ConnectorRuntimeError::Provider(anyhow::anyhow!(
                    "invalid remote connector scope payload: {err}"
                ))
            }),
        RESP_NOT_FOUND => Ok(None),
        RESP_ERR => Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            decode_remote_error_message(&bytes[1..])
        ))),
        other => Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            "invalid remote connector scope status {other}"
        ))),
    }
}

#[cfg(target_arch = "wasm32")]
fn decode_remote_connection(
    bytes: &[u8],
) -> Result<Option<ResolvedConnectorConnection>, ConnectorRuntimeError> {
    if bytes.is_empty() {
        return Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            "invalid remote connector connection response: empty"
        )));
    }
    match bytes[0] {
        RESP_OK => serde_json::from_slice(&bytes[1..])
            .map(Some)
            .map_err(|err| {
                ConnectorRuntimeError::Provider(anyhow::anyhow!(
                    "invalid remote connector connection payload: {err}"
                ))
            }),
        RESP_NOT_FOUND => Ok(None),
        RESP_ERR => Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            decode_remote_error_message(&bytes[1..])
        ))),
        other => Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            "invalid remote connector connection status {other}"
        ))),
    }
}

#[cfg(target_arch = "wasm32")]
fn decode_remote_error_message(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "connector runtime error".to_string();
    }
    match std::str::from_utf8(bytes) {
        Ok(message) => message.to_string(),
        Err(_) => "connector runtime error (non-utf8)".to_string(),
    }
}

#[cfg(target_arch = "wasm32")]
fn decode_remote_success<T>(bytes: &[u8], label: &str) -> Result<T, ConnectorRuntimeError>
where
    T: for<'de> Deserialize<'de>,
{
    if bytes.is_empty() {
        return Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            "invalid remote connector response for {label}: empty"
        )));
    }
    match bytes[0] {
        RESP_OK => serde_json::from_slice(&bytes[1..]).map_err(|err| {
            ConnectorRuntimeError::Provider(anyhow::anyhow!(
                "invalid remote connector success payload for {label}: {err}"
            ))
        }),
        RESP_ERR => Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            decode_remote_error_message(&bytes[1..])
        ))),
        other => Err(ConnectorRuntimeError::Provider(anyhow::anyhow!(
            "invalid remote connector status {other} for {label}"
        ))),
    }
}

/// Remote connector runtime implementation for wasm guest bundles.
#[cfg(target_arch = "wasm32")]
#[derive(Debug, Default, Clone, Copy)]
pub struct RemoteConnectorRuntime;

#[cfg(target_arch = "wasm32")]
impl RemoteConnectorRuntime {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait]
impl ConnectorRuntime for RemoteConnectorRuntime {
    async fn apply_outbound_auth(
        &self,
        scope: &ConnectorBindingScope,
        profile: &OutboundAuthProfileDescriptor,
        request: &mut HttpRequest,
    ) -> Result<(), ConnectorRuntimeError> {
        let payload = serde_json::to_vec(&ApplyOutboundAuthRequest {
            scope: scope.clone(),
            profile: TransportOutboundAuthProfileDescriptor::from(profile),
            request: request.clone(),
        })
        .map_err(|err| {
            ConnectorRuntimeError::Provider(anyhow::anyhow!(
                "failed to encode remote connector auth request: {err}"
            ))
        })?;

        let response = crate::wasm_transport::cap_call(OP_CONNECTOR_APPLY_OUTBOUND_AUTH, &payload)
            .map_err(|err| ConnectorRuntimeError::Provider(anyhow::anyhow!(err.to_string())))?;
        *request = decode_remote_success(&response, "apply_outbound_auth")?;
        Ok(())
    }

    async fn resolve_endpoint_profile(
        &self,
        scope: &ConnectorBindingScope,
        profile: &EndpointProfileDescriptor,
    ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
        let payload = serde_json::to_vec(&ResolveEndpointProfileRequest {
            scope: scope.clone(),
            profile: TransportEndpointProfileDescriptor::from(profile),
        })
        .map_err(|err| {
            ConnectorRuntimeError::Provider(anyhow::anyhow!(
                "failed to encode remote connector endpoint request: {err}"
            ))
        })?;

        let response =
            crate::wasm_transport::cap_call(OP_CONNECTOR_RESOLVE_ENDPOINT_PROFILE, &payload)
                .map_err(|err| ConnectorRuntimeError::Provider(anyhow::anyhow!(err.to_string())))?;
        decode_remote_success(&response, "resolve_endpoint_profile")
    }

    async fn resolve_connection(
        &self,
        scope: &ConnectorBindingScope,
    ) -> Result<Option<ResolvedConnectorConnection>, ConnectorRuntimeError> {
        let payload = serde_json::to_vec(&ResolveConnectionRequest {
            scope: scope.clone(),
        })
        .map_err(|err| {
            ConnectorRuntimeError::Provider(anyhow::anyhow!(
                "failed to encode remote connector connection request: {err}"
            ))
        })?;

        let response =
            crate::wasm_transport::cap_call(OP_CONNECTOR_RESOLVE_CONNECTION, &payload)
                .map_err(|err| ConnectorRuntimeError::Provider(anyhow::anyhow!(err.to_string())))?;
        decode_remote_connection(&response)
    }
}

/// Fetch the current connector binding scope from the host runtime when running
/// inside a wasm guest bundle.
#[cfg(target_arch = "wasm32")]
pub fn current_remote_scope() -> Result<Option<ConnectorBindingScope>, ConnectorRuntimeError> {
    let response = crate::wasm_transport::cap_call(OP_CONNECTOR_GET_SCOPE, &[])
        .map_err(|err| ConnectorRuntimeError::Provider(anyhow::anyhow!(err.to_string())))?;
    decode_remote_scope(&response)
}
