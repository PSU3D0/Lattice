use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;

pub mod http {
    use super::*;

    pub const HINT_HTTP: &str = "resource::http";
    pub const HINT_HTTP_READ: &str = "resource::http::read";
    pub const HINT_HTTP_WRITE: &str = "resource::http::write";

    static REGISTRATION: OnceLock<()> = OnceLock::new();

    /// Ensure HTTP capability hints are registered with the shared effect/determinism registries.
    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_HTTP_READ,
                    dag_core::Effects::ReadOnly,
                    "HTTP reads reach external systems; declare effects = ReadOnly or Effectful.",
                ),
            );
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_HTTP_WRITE,
                    dag_core::Effects::Effectful,
                    "HTTP writes are effectful; declare effects = Effectful and provide idempotency keys.",
                ),
            );
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_HTTP,
                    dag_core::Determinism::BestEffort,
                    "HTTP calls vary across retries; downgrade determinism or pin responses via caching.",
                ),
            );
        });
    }

    /// HTTP method supported by canonical client implementations.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "UPPERCASE")]
    pub enum HttpMethod {
        Get,
        Head,
        Post,
        Put,
        Patch,
        Delete,
    }

    impl HttpMethod {
        pub fn as_str(self) -> &'static str {
            match self {
                HttpMethod::Get => "GET",
                HttpMethod::Head => "HEAD",
                HttpMethod::Post => "POST",
                HttpMethod::Put => "PUT",
                HttpMethod::Patch => "PATCH",
                HttpMethod::Delete => "DELETE",
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct HttpHeaders(pub HashMap<String, String>);

    impl HttpHeaders {
        pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
            self.0.insert(key.into(), value.into());
        }

        pub fn get(&self, key: &str) -> Option<&String> {
            self.0.get(key)
        }

        pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
            self.0.iter()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HttpRequest {
        pub method: HttpMethod,
        pub url: String,
        #[serde(default)]
        pub headers: HttpHeaders,
        pub body: Option<Vec<u8>>,
        pub timeout_ms: Option<u64>,
    }

    impl HttpRequest {
        pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
            Self {
                method,
                url: url.into(),
                headers: HttpHeaders::default(),
                body: None,
                timeout_ms: None,
            }
        }

        pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
            self.body = Some(body.into());
            self
        }

        pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.headers.insert(key, value);
            self
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HttpResponse {
        pub status: u16,
        #[serde(default)]
        pub headers: HttpHeaders,
        pub body: Vec<u8>,
    }

    impl HttpResponse {
        pub fn is_success(&self) -> bool {
            (200..300).contains(&self.status)
        }
    }

    /// Canonical error type surfaced by HTTP capabilities.
    #[derive(Debug, thiserror::Error)]
    pub enum HttpError {
        #[error("transport error: {0}")]
        Transport(#[from] anyhow::Error),
        #[error("request timed out after {0}ms")]
        Timeout(u64),
        #[error("invalid response: {0}")]
        InvalidResponse(String),
    }

    pub type HttpResult<T> = Result<T, HttpError>;

    #[async_trait]
    pub trait HttpRead: Send + Sync {
        async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse>;
    }

    #[async_trait]
    pub trait HttpWrite: Send + Sync {
        async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse>;
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn ensure_http_registration_is_idempotent() {
            ensure_registered();
            ensure_registered();

            let effect_read = dag_core::effects_registry::constraint_for_hint(HINT_HTTP_READ)
                .expect("http read constraint registered");
            assert_eq!(effect_read.minimum, dag_core::Effects::ReadOnly);

            let effect_write = dag_core::effects_registry::constraint_for_hint(HINT_HTTP_WRITE)
                .expect("http write constraint registered");
            assert_eq!(effect_write.minimum, dag_core::Effects::Effectful);

            let det = dag_core::determinism::constraint_for_hint(HINT_HTTP)
                .expect("http determinism constraint registered");
            assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
        }
    }
}
