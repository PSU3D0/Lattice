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

/// In-memory [`capabilities::dedupe::DedupeStore`] for offline
/// duplicate-injection tests (connector verification harness).
///
/// Only the Redis-backed dedupe store exists outside tests; connector
/// verification needs a deterministic, dependency-free store so idempotency
/// evidence (exactly-once under duplicate delivery, verified via
/// `testing-harness-idem`) can run in default CI. TTL semantics match the
/// `put_if_absent`/`forget` contract: a reservation blocks duplicates until
/// the TTL elapses, after which the key may be reserved again.
///
/// Native-only: `std::time::Instant` is unavailable on `wasm32-unknown-unknown`,
/// and harness tests only run natively.
#[cfg(not(target_arch = "wasm32"))]
pub use memory_dedupe::MemoryDedupeStore;

#[cfg(not(target_arch = "wasm32"))]
mod memory_dedupe {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use capabilities::Capability;
    use capabilities::dedupe::{DedupeError, DedupeStore};

    /// See the re-export documentation on [`super::MemoryDedupeStore`].
    #[derive(Debug, Default)]
    pub struct MemoryDedupeStore {
        entries: Mutex<HashMap<Vec<u8>, Instant>>,
    }

    impl MemoryDedupeStore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    impl Capability for MemoryDedupeStore {
        fn name(&self) -> &'static str {
            "dedupe.memory.dev"
        }
    }

    #[async_trait]
    impl DedupeStore for MemoryDedupeStore {
        async fn put_if_absent(&self, key: &[u8], ttl: Duration) -> Result<bool, DedupeError> {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| DedupeError::Other("dedupe mutex poisoned".to_string()))?;
            let now = Instant::now();
            match entries.get(key) {
                Some(deadline) if *deadline > now => Ok(false),
                _ => {
                    entries.insert(key.to_vec(), now + ttl);
                    Ok(true)
                }
            }
        }

        async fn forget(&self, key: &[u8]) -> Result<(), DedupeError> {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| DedupeError::Other("dedupe mutex poisoned".to_string()))?;
            entries.remove(key);
            Ok(())
        }
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

    #[tokio::test]
    async fn memory_dedupe_store_blocks_duplicates_until_ttl_expiry() {
        use capabilities::dedupe::DedupeStore;
        use std::time::Duration;

        let store = MemoryDedupeStore::new();
        let ttl = Duration::from_millis(30);

        assert!(store.put_if_absent(b"key", ttl).await.expect("reserve"));
        assert!(!store.put_if_absent(b"key", ttl).await.expect("duplicate"));
        assert!(!store.put_if_absent(b"key", ttl).await.expect("duplicate"));

        tokio::time::sleep(ttl + Duration::from_millis(10)).await;
        assert!(
            store
                .put_if_absent(b"key", ttl)
                .await
                .expect("post-ttl reserve")
        );

        store.forget(b"key").await.expect("forget");
        assert!(
            store
                .put_if_absent(b"key", ttl)
                .await
                .expect("post-forget reserve")
        );
    }
}
