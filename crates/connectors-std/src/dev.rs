use async_trait::async_trait;
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
    OutboundAuthProfileDescriptor, ResolvedEndpointProfile,
};
use capabilities::http::HttpRequest;

use crate::auth::apply_static_outbound_auth;

/// Development adapter that preserves the current env-backed connector behavior.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvConnectorRuntime;

#[async_trait]
impl ConnectorRuntime for EnvConnectorRuntime {
    async fn apply_outbound_auth(
        &self,
        _scope: &ConnectorBindingScope,
        profile: &OutboundAuthProfileDescriptor,
        request: &mut HttpRequest,
    ) -> Result<(), ConnectorRuntimeError> {
        let secret = resolve_secret(profile)?;
        apply_static_outbound_auth(request, profile, secret)
    }

    async fn resolve_endpoint_profile(
        &self,
        _scope: &ConnectorBindingScope,
        profile: &EndpointProfileDescriptor,
    ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
        #[cfg(not(target_arch = "wasm32"))]
        let base_url = std::env::var(profile.env_base_url_var)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| profile.base_url.to_string());

        #[cfg(target_arch = "wasm32")]
        let base_url = profile.base_url.to_string();

        Ok(ResolvedEndpointProfile {
            base_url,
            default_headers: profile
                .default_headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        })
    }
}

fn resolve_secret(
    profile: &OutboundAuthProfileDescriptor,
) -> Result<String, ConnectorRuntimeError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var(profile.env_var).map_err(|_| ConnectorRuntimeError::MissingAuthOverride {
            role_name: profile.name,
            env_var: profile.env_var,
        })
    }

    #[cfg(target_arch = "wasm32")]
    {
        let _ = profile;
        Err(ConnectorRuntimeError::MissingAuthOverride {
            role_name: profile.name,
            env_var: profile.env_var,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use capabilities::connector::{EndpointProfileDescriptor, OutboundAuthKind};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn env_runtime_applies_api_key_header_prefix() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let profile = OutboundAuthProfileDescriptor {
            connector_id: "connector.demo",
            name: "demo",
            env_var: "LATTICE_CONNECTOR_AUTH_DEMO",
            kind: OutboundAuthKind::ApiKeyHeader {
                header_name: "X-Api-Key",
                prefix: Some("Token"),
                handle_kind: "raw.secret",
            },
        };
        let mut request =
            HttpRequest::new(capabilities::http::HttpMethod::Get, "https://example.test");
        unsafe {
            std::env::set_var(profile.env_var, "abc123");
        }

        EnvConnectorRuntime
            .apply_outbound_auth(
                &ConnectorBindingScope::new("flow", "node", "tests::node", "connector.demo"),
                &profile,
                &mut request,
            )
            .await
            .expect("auth applied");

        assert_eq!(
            request.headers.get("X-Api-Key"),
            Some(&"Token abc123".to_string())
        );
        unsafe {
            std::env::remove_var(profile.env_var);
        }
    }

    #[tokio::test]
    async fn env_runtime_resolves_endpoint_override() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let profile = EndpointProfileDescriptor {
            connector_id: "connector.demo",
            name: "demo_endpoint",
            env_base_url_var: "LATTICE_CONNECTOR_ENDPOINT_DEMO_BASE_URL",
            base_url: "https://default.test",
            default_headers: &[("Accept", "application/json")],
        };
        unsafe {
            std::env::set_var(profile.env_base_url_var, "https://override.test");
        }

        let resolved = EnvConnectorRuntime
            .resolve_endpoint_profile(
                &ConnectorBindingScope::new("flow", "node", "tests::node", "connector.demo"),
                &profile,
            )
            .await
            .expect("endpoint resolved");

        assert_eq!(resolved.base_url, "https://override.test");
        assert_eq!(
            resolved.default_headers,
            vec![("Accept".to_string(), "application/json".to_string())]
        );
        unsafe {
            std::env::remove_var(profile.env_base_url_var);
        }
    }
}
