use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::http::HttpRequest;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEndpointProfile {
    pub base_url: String,
    pub default_headers: Vec<(String, String)>,
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
}
