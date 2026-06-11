//! Shared OpenAI-style connector client setup.
//!
//! Examples (s11, s13, ...) historically each copied ~100 lines of identical
//! settings-resolution + fallback + bearer-extraction code to stand up an
//! OpenAI-compatible client. That duplication is exactly where agents introduce
//! silent drift, so the resolution/fallback half lives here once.
//!
//! This module deliberately does **not** depend on any `llm-*` crate: it returns
//! a plain [`OpenAiSettings`] (`api_key` + `base_url`) and leaves the thin
//! provider-client construction (a handful of lines wiring
//! `OpenAIClient::<LatticeHttpClient>::builder()`) to the caller. That keeps
//! `connectors-std` free of provider/HTTP-bridge dependencies while still
//! collapsing the bulk of the boilerplate.
//!
//! Resolution always flows through the node's *granted* scoped resources
//! (`connector_runtime()` + `connector_scope()`), never around them: if those
//! accessors are absent (e.g. wasm, or a node that did not declare the
//! connector), the helper falls back to environment / compiled defaults instead
//! of fabricating capability access.

use dag_core::{NodeError, NodeResult};

use capabilities::connector::{EndpointProfileDescriptor, OutboundAuthProfileDescriptor};
use capabilities::http::{HttpMethod, HttpRequest};

use crate::{
    apply_outbound_auth_with_context, current_connector_context, resolve_endpoint_with_context,
};

/// Resolved base URL + bearer key for an OpenAI-compatible endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenAiSettings {
    pub api_key: String,
    pub base_url: String,
}

/// Compiled fallback values used when no connector runtime/scope is granted
/// (for example under `wasm32`, or before bindings are wired in local dev).
///
/// `env_*` fields name the environment variables consulted on non-wasm targets;
/// `default_*` fields are the baked-in last resort.
#[derive(Clone, Copy, Debug)]
pub struct OpenAiFallback {
    pub env_api_key_var: &'static str,
    pub default_api_key: &'static str,
    pub env_base_url_var: &'static str,
    pub default_base_url: &'static str,
}

impl OpenAiFallback {
    fn api_key(&self) -> String {
        env_or_default(self.env_api_key_var, self.default_api_key)
    }

    fn base_url(&self) -> String {
        env_or_default(self.env_base_url_var, self.default_base_url)
    }

    fn settings(&self) -> OpenAiSettings {
        OpenAiSettings {
            api_key: self.api_key(),
            base_url: self.base_url(),
        }
    }
}

/// Resolve OpenAI settings from the node's granted connector runtime, falling
/// back to environment / compiled defaults when no runtime+scope is in scope.
///
/// The `action` label is used only for diagnostics if a granted runtime later
/// fails to resolve the endpoint or apply auth.
pub async fn resolve_openai_settings(
    action: &'static str,
    endpoint: &'static EndpointProfileDescriptor,
    auth: &'static OutboundAuthProfileDescriptor,
    fallback: OpenAiFallback,
) -> NodeResult<OpenAiSettings> {
    match resolve_via_connector_runtime(action, endpoint, auth).await? {
        Some(settings) => Ok(settings),
        None => Ok(fallback.settings()),
    }
}

/// Attempt resolution through the scoped connector runtime. Returns `Ok(None)`
/// (so the caller can fall back) when no runtime/scope is granted; returns
/// `Err` only when a granted runtime fails.
async fn resolve_via_connector_runtime(
    action: &'static str,
    endpoint: &'static EndpointProfileDescriptor,
    auth: &'static OutboundAuthProfileDescriptor,
) -> NodeResult<Option<OpenAiSettings>> {
    use crate::errors::ConnectorRuntimeError;

    let connector_context = match current_connector_context(action).await {
        Ok(context) => context,
        // No granted runtime/scope (or no resource context at all): this is the
        // ambient/wasm path, fall back rather than fail.
        Err(
            ConnectorRuntimeError::MissingConnectorRuntime { .. }
            | ConnectorRuntimeError::MissingConnectorScope { .. }
            | ConnectorRuntimeError::MissingResourceContext,
        ) => return Ok(None),
        Err(other) => return Err(node_error(other)),
    };

    let endpoint = resolve_endpoint_with_context(endpoint, &connector_context)
        .await
        .map_err(node_error)?;

    let mut request = HttpRequest::new(HttpMethod::Get, endpoint.base_url.clone());
    apply_outbound_auth_with_context(auth, &mut request, &connector_context)
        .await
        .map_err(node_error)?;
    let api_key = bearer_api_key(&request)?;

    Ok(Some(OpenAiSettings {
        api_key,
        base_url: endpoint.base_url,
    }))
}

/// Extract the bearer token a connector runtime injected into the request's
/// `Authorization` header.
fn bearer_api_key(request: &HttpRequest) -> NodeResult<String> {
    let header = request
        .headers
        .get("authorization")
        .or_else(|| request.headers.get("Authorization"))
        .ok_or_else(|| NodeError::new("missing authorization header from connector runtime"))?;
    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| NodeError::new(format!("unsupported authorization header `{header}`")))?;
    Ok(token.to_string())
}

/// Read an environment variable on native targets, or the compiled default on
/// `wasm32` (where process env is unavailable).
pub fn env_or_default(var: &str, default: &'static str) -> String {
    #[cfg(target_arch = "wasm32")]
    {
        let _ = var;
        default.to_string()
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var(var).unwrap_or_else(|_| default.to_string())
    }
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use capabilities::ResourceBag;
    use capabilities::connector::{
        ConnectorBindingScope, ConnectorRuntime,
        ConnectorRuntimeError as HostConnectorRuntimeError, EndpointProfileDescriptor,
        OutboundAuthKind, OutboundAuthProfileDescriptor, ResolvedEndpointProfile,
    };
    use capabilities::context;
    use std::sync::Arc;

    const TEST_ACTION: &str = "connector.openai.test";
    const TEST_ENDPOINT: EndpointProfileDescriptor = EndpointProfileDescriptor {
        connector_id: "connector.openai",
        name: "default_api",
        env_base_url_var: "LATTICE_TEST_OPENAI_BASE_URL",
        base_url: "https://api.openai.test/v1",
        default_headers: &[],
    };
    const TEST_AUTH: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {
        connector_id: "connector.openai",
        name: "default_auth",
        env_var: "LATTICE_TEST_OPENAI_API_KEY",
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };
    const TEST_FALLBACK: OpenAiFallback = OpenAiFallback {
        env_api_key_var: "LATTICE_TEST_OPENAI_FALLBACK_KEY",
        default_api_key: "fallback-key",
        env_base_url_var: "LATTICE_TEST_OPENAI_FALLBACK_BASE_URL",
        default_base_url: "https://fallback.openai.test/v1",
    };

    /// Runtime that injects a bearer token and echoes the endpoint base url.
    struct GrantingRuntime {
        token: &'static str,
        base_url: &'static str,
    }

    #[async_trait]
    impl ConnectorRuntime for GrantingRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            request: &mut HttpRequest,
        ) -> Result<(), HostConnectorRuntimeError> {
            request
                .headers
                .insert("authorization".to_string(), format!("Bearer {}", self.token));
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, HostConnectorRuntimeError> {
            Ok(ResolvedEndpointProfile {
                base_url: self.base_url.to_string(),
                default_headers: Vec::new(),
            })
        }
    }

    /// Runtime that resolves the endpoint but injects a non-bearer auth header,
    /// exercising the error path in `bearer_api_key`.
    struct NonBearerRuntime;

    #[async_trait]
    impl ConnectorRuntime for NonBearerRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            request: &mut HttpRequest,
        ) -> Result<(), HostConnectorRuntimeError> {
            request
                .headers
                .insert("authorization".to_string(), "Basic abc123".to_string());
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, HostConnectorRuntimeError> {
            Ok(ResolvedEndpointProfile {
                base_url: "https://api.openai.test/v1".to_string(),
                default_headers: Vec::new(),
            })
        }
    }

    /// Runtime that injects an auth header but omits the bearer token entirely.
    struct MissingHeaderRuntime;

    #[async_trait]
    impl ConnectorRuntime for MissingHeaderRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            _request: &mut HttpRequest,
        ) -> Result<(), HostConnectorRuntimeError> {
            // Intentionally do not insert any authorization header.
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, HostConnectorRuntimeError> {
            Ok(ResolvedEndpointProfile {
                base_url: "https://api.openai.test/v1".to_string(),
                default_headers: Vec::new(),
            })
        }
    }

    /// Runtime whose endpoint resolution fails.
    struct EndpointFailureRuntime;

    #[async_trait]
    impl ConnectorRuntime for EndpointFailureRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            _request: &mut HttpRequest,
        ) -> Result<(), HostConnectorRuntimeError> {
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, HostConnectorRuntimeError> {
            Err(HostConnectorRuntimeError::InvalidEndpointProfile {
                role_name: profile.name,
                reason: "endpoint lookup failed".to_string(),
            })
        }
    }

    fn scoped_bag<R: ConnectorRuntime + 'static>(runtime: Arc<R>) -> Arc<ResourceBag> {
        Arc::new(
            ResourceBag::new()
                .with_connector_runtime(runtime)
                .with_connector_scope(ConnectorBindingScope::new(
                    "flow://openai-helper-tests",
                    "node-a",
                    TEST_ACTION,
                    "connector.openai",
                )),
        )
    }

    #[tokio::test]
    async fn resolves_settings_through_granted_runtime() {
        let bag = scoped_bag(Arc::new(GrantingRuntime {
            token: "granted-key",
            base_url: "https://granted.openai.test/v1",
        }));

        let settings = context::with_resources(bag, async {
            resolve_openai_settings(TEST_ACTION, &TEST_ENDPOINT, &TEST_AUTH, TEST_FALLBACK)
                .await
                .expect("settings resolve")
        })
        .await;

        assert_eq!(settings.api_key, "granted-key");
        assert_eq!(settings.base_url, "https://granted.openai.test/v1");
    }

    #[tokio::test]
    async fn falls_back_when_no_connector_runtime_is_granted() {
        // No connector runtime / scope in the bag: the helper must fall back to
        // compiled defaults rather than fabricate capability access.
        let bag = Arc::new(ResourceBag::new());

        let settings = context::with_resources(bag, async {
            resolve_openai_settings(TEST_ACTION, &TEST_ENDPOINT, &TEST_AUTH, TEST_FALLBACK)
                .await
                .expect("settings fall back")
        })
        .await;

        assert_eq!(settings.api_key, TEST_FALLBACK.default_api_key);
        assert_eq!(settings.base_url, TEST_FALLBACK.default_base_url);
    }

    #[tokio::test]
    async fn falls_back_when_no_resource_context_at_all() {
        // Called entirely outside a resource scope: still falls back, never panics.
        let settings =
            resolve_openai_settings(TEST_ACTION, &TEST_ENDPOINT, &TEST_AUTH, TEST_FALLBACK)
                .await
                .expect("settings fall back without context");

        assert_eq!(settings.api_key, TEST_FALLBACK.default_api_key);
        assert_eq!(settings.base_url, TEST_FALLBACK.default_base_url);
    }

    #[tokio::test]
    async fn granted_runtime_with_non_bearer_header_is_an_error() {
        let bag = scoped_bag(Arc::new(NonBearerRuntime));

        let err = context::with_resources(bag, async {
            resolve_openai_settings(TEST_ACTION, &TEST_ENDPOINT, &TEST_AUTH, TEST_FALLBACK)
                .await
                .expect_err("non-bearer header should error, not silently fall back")
        })
        .await;

        assert!(
            err.to_string().contains("unsupported authorization header"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn granted_runtime_with_missing_auth_header_is_an_error() {
        let bag = scoped_bag(Arc::new(MissingHeaderRuntime));

        let err = context::with_resources(bag, async {
            resolve_openai_settings(TEST_ACTION, &TEST_ENDPOINT, &TEST_AUTH, TEST_FALLBACK)
                .await
                .expect_err("missing auth header should error")
        })
        .await;

        assert!(
            err.to_string().contains("missing authorization header"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn granted_runtime_endpoint_failure_propagates() {
        let bag = scoped_bag(Arc::new(EndpointFailureRuntime));

        let err = context::with_resources(bag, async {
            resolve_openai_settings(TEST_ACTION, &TEST_ENDPOINT, &TEST_AUTH, TEST_FALLBACK)
                .await
                .expect_err("endpoint failure should propagate, not fall back")
        })
        .await;

        assert!(
            err.to_string().contains("endpoint lookup failed"),
            "unexpected error: {err}"
        );
    }
}
