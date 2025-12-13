use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub mod hints;

/// Marker trait implemented by all capability providers.
pub trait Capability: Send + Sync + 'static {
    /// Human-friendly capability identifier.
    fn name(&self) -> &'static str;
}

/// Unified view of capabilities exposed to node execution contexts.
pub trait ResourceAccess: Send + Sync + 'static {
    fn http_read(&self) -> Option<&dyn http::HttpRead> {
        None
    }

    fn http_write(&self) -> Option<&dyn http::HttpWrite> {
        None
    }

    fn clock(&self) -> Option<&dyn clock::Clock> {
        None
    }

    fn cache(&self) -> Option<&dyn cache::Cache> {
        None
    }

    fn kv(&self) -> Option<&dyn kv::KeyValue> {
        None
    }

    fn blob(&self) -> Option<&dyn blob::BlobStore> {
        None
    }

    fn queue(&self) -> Option<&dyn queue::Queue> {
        None
    }

    fn dedupe_store(&self) -> Option<&dyn dedupe::DedupeStore> {
        None
    }
}

/// Mutable collection of capability providers surfaced to the executor.
#[derive(Default, Clone)]
pub struct ResourceBag {
    http_read: Option<Arc<dyn http::HttpRead>>,
    http_write: Option<Arc<dyn http::HttpWrite>>,
    clock: Option<Arc<dyn clock::Clock>>,
    cache: Option<Arc<dyn cache::Cache>>,
    kv: Option<Arc<dyn kv::KeyValue>>,
    blob: Option<Arc<dyn blob::BlobStore>>,
    queue: Option<Arc<dyn queue::Queue>>,
    dedupe: Option<Arc<dyn dedupe::DedupeStore>>,
}

impl ResourceBag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_http_read<T>(mut self, capability: Arc<T>) -> Self
    where
        T: http::HttpRead + 'static,
    {
        let capability: Arc<dyn http::HttpRead> = capability;
        self.http_read = Some(capability);
        self
    }

    pub fn with_http_write<T>(mut self, capability: Arc<T>) -> Self
    where
        T: http::HttpWrite + 'static,
    {
        let capability: Arc<dyn http::HttpWrite> = capability;
        self.http_write = Some(capability);
        self
    }

    pub fn with_clock<T>(mut self, capability: Arc<T>) -> Self
    where
        T: clock::Clock + 'static,
    {
        let capability: Arc<dyn clock::Clock> = capability;
        self.clock = Some(capability);
        self
    }

    pub fn with_cache<T>(mut self, capability: Arc<T>) -> Self
    where
        T: cache::Cache + 'static,
    {
        let capability: Arc<dyn cache::Cache> = capability;
        self.cache = Some(capability);
        self
    }

    pub fn with_kv<T>(mut self, capability: Arc<T>) -> Self
    where
        T: kv::KeyValue + 'static,
    {
        let capability: Arc<dyn kv::KeyValue> = capability;
        self.kv = Some(capability);
        self
    }

    pub fn with_blob<T>(mut self, capability: Arc<T>) -> Self
    where
        T: blob::BlobStore + 'static,
    {
        let capability: Arc<dyn blob::BlobStore> = capability;
        self.blob = Some(capability);
        self
    }

    pub fn with_queue<T>(mut self, capability: Arc<T>) -> Self
    where
        T: queue::Queue + 'static,
    {
        let capability: Arc<dyn queue::Queue> = capability;
        self.queue = Some(capability);
        self
    }

    pub fn with_dedupe<T>(mut self, capability: Arc<T>) -> Self
    where
        T: dedupe::DedupeStore + 'static,
    {
        let capability: Arc<dyn dedupe::DedupeStore> = capability;
        self.dedupe = Some(capability);
        self
    }
}

/// Utilities for scoping capability access to the current execution.
pub mod context {
    use super::ResourceAccess;
    use std::future::Future;
    use std::sync::Arc;

    use tokio::task_local;

    task_local! {
        static CURRENT_RESOURCES: Arc<dyn ResourceAccess>;
    }

    /// Execute `future` with the provided resource access scoped to the current task.
    pub async fn with_resources<Fut, R>(resources: Arc<dyn ResourceAccess>, future: Fut) -> R
    where
        Fut: Future<Output = R>,
    {
        CURRENT_RESOURCES.scope(resources, future).await
    }

    /// Invoke the callback with the currently scoped resource access, if present.
    pub fn with_current<F, R>(callback: F) -> Option<R>
    where
        F: FnOnce(&dyn ResourceAccess) -> R,
    {
        CURRENT_RESOURCES
            .try_with(|resources| callback(resources.as_ref()))
            .ok()
    }

    /// Invoke the async callback with the currently scoped resource access, if present.
    pub async fn with_current_async<F, Fut, R>(callback: F) -> Option<R>
    where
        F: FnOnce(Arc<dyn ResourceAccess>) -> Fut,
        Fut: Future<Output = R>,
    {
        let resources = CURRENT_RESOURCES.try_with(Arc::clone).ok()?;
        Some(callback(resources).await)
    }

    /// Clone the currently scoped resource access handle, if any.
    pub fn current_handle() -> Option<Arc<dyn ResourceAccess>> {
        CURRENT_RESOURCES.try_with(Arc::clone).ok()
    }
}

impl ResourceAccess for ResourceBag {
    fn http_read(&self) -> Option<&dyn http::HttpRead> {
        self.http_read
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn http::HttpRead)
    }

    fn http_write(&self) -> Option<&dyn http::HttpWrite> {
        self.http_write
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn http::HttpWrite)
    }

    fn clock(&self) -> Option<&dyn clock::Clock> {
        self.clock
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn clock::Clock)
    }

    fn cache(&self) -> Option<&dyn cache::Cache> {
        self.cache
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn cache::Cache)
    }

    fn kv(&self) -> Option<&dyn kv::KeyValue> {
        self.kv
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn kv::KeyValue)
    }

    fn blob(&self) -> Option<&dyn blob::BlobStore> {
        self.blob
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn blob::BlobStore)
    }

    fn queue(&self) -> Option<&dyn queue::Queue> {
        self.queue
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn queue::Queue)
    }

    fn dedupe_store(&self) -> Option<&dyn dedupe::DedupeStore> {
        self.dedupe
            .as_ref()
            .map(|cap| cap.as_ref() as &dyn dedupe::DedupeStore)
    }
}

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

pub mod clock {
    use super::*;
    use std::time::SystemTime;

    pub const HINT_CLOCK: &str = "resource::clock";
    static REGISTRATION: OnceLock<()> = OnceLock::new();

    /// Ensure the shared determinism hint for clocks is registered.
    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_CLOCK,
                    dag_core::Determinism::BestEffort,
                    "Clock access is nondeterministic; declare determinism = BestEffort or lower.",
                ),
            );
        });
    }

    /// Abstract clock capability used by runtimes.
    pub trait Clock: Capability {
        fn now(&self) -> SystemTime;
    }

    /// Wall-clock implementation backed by `SystemTime::now`.
    #[derive(Default)]
    pub struct SystemClock;

    impl Capability for SystemClock {
        fn name(&self) -> &'static str {
            "clock.system"
        }
    }

    impl Clock for SystemClock {
        fn now(&self) -> SystemTime {
            SystemTime::now()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn register_clock_hint_once() {
            ensure_registered();
            ensure_registered();
            let constraint =
                dag_core::determinism::constraint_for_hint(HINT_CLOCK).expect("clock hint");
            assert_eq!(constraint.minimum, dag_core::Determinism::BestEffort);
        }

        #[test]
        fn system_clock_produces_time() {
            ensure_registered();
            let clock = SystemClock;
            let now = clock.now();
            assert!(now.elapsed().is_ok());
        }
    }
}

pub mod dedupe {
    use super::*;
    use std::time::Duration;

    pub const HINT_DEDUPE: &str = "resource::dedupe";
    pub const HINT_DEDUPE_WRITE: &str = "resource::dedupe::write";

    static REGISTRATION: OnceLock<()> = OnceLock::new();

    /// Register effect/determinism constraints for dedupe stores.
    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_DEDUPE_WRITE,
                    dag_core::Effects::Effectful,
                    "Dedupe stores persist state; declare effects = Effectful when binding.",
                ),
            );
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_DEDUPE,
                    dag_core::Determinism::BestEffort,
                    "Dedupe lookups depend on external state; downgrade determinism or provide proofs.",
                ),
            );
        });
    }

    /// Errors surfaced by dedupe store capabilities.
    #[derive(Debug, thiserror::Error)]
    pub enum DedupeError {
        #[error("async runtime not available to execute redis operations")]
        RuntimeUnavailable,
        #[error("operation failed: {0}")]
        Other(String),
    }

    #[async_trait]
    pub trait DedupeStore: Capability {
        async fn put_if_absent(&self, key: &[u8], ttl: Duration) -> Result<bool, DedupeError>;
        async fn forget(&self, key: &[u8]) -> Result<(), DedupeError>;
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn registers_constraints_once() {
            ensure_registered();
            ensure_registered();
            let effect = dag_core::effects_registry::constraint_for_hint(HINT_DEDUPE_WRITE)
                .expect("dedupe effect constraint");
            assert_eq!(effect.minimum, dag_core::Effects::Effectful);
            let det = dag_core::determinism::constraint_for_hint(HINT_DEDUPE)
                .expect("dedupe determinism constraint");
            assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
        }
    }
}

pub mod cache {
    use super::*;

    /// Shared cache capability error type.
    #[derive(Debug, thiserror::Error)]
    pub enum CacheError {
        #[error("value not found")]
        NotFound,
        #[error("operation failed: {0}")]
        Other(String),
    }

    /// Cache capability interface surfaced to nodes.
    pub trait Cache: Capability {
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, CacheError>;
        fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError>;
        fn remove(&self, key: &[u8]) -> Result<(), CacheError>;
    }

    struct Entry {
        value: Vec<u8>,
        expires_at: Option<Instant>,
    }

    /// Simple process-local cache backed by a mutexed HashMap.
    pub struct MemoryCache {
        entries: Mutex<HashMap<Vec<u8>, Entry>>,
    }

    impl MemoryCache {
        pub fn new() -> Self {
            Self {
                entries: Mutex::new(HashMap::new()),
            }
        }

        fn is_expired(expires_at: Option<Instant>) -> bool {
            match expires_at {
                Some(deadline) => Instant::now() > deadline,
                None => false,
            }
        }
    }

    impl Default for MemoryCache {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Capability for MemoryCache {
        fn name(&self) -> &'static str {
            "cache.memory"
        }
    }

    impl Cache for MemoryCache {
        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, CacheError> {
            let mut entries = self.entries.lock().expect("cache mutex poisoned");
            if let Some(entry) = entries.get(key) {
                if Self::is_expired(entry.expires_at) {
                    entries.remove(key);
                    return Ok(None);
                }
                return Ok(Some(entry.value.clone()));
            }
            Ok(None)
        }

        fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
            let mut entries = self.entries.lock().expect("cache mutex poisoned");
            let expires_at = ttl.map(|duration| Instant::now() + duration);
            entries.insert(
                key.to_vec(),
                Entry {
                    value: value.to_vec(),
                    expires_at,
                },
            );
            Ok(())
        }

        fn remove(&self, key: &[u8]) -> Result<(), CacheError> {
            let mut entries = self.entries.lock().expect("cache mutex poisoned");
            entries.remove(key);
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::time::Duration;

        #[test]
        fn remembers_and_expires_values() {
            let cache = MemoryCache::default();
            cache
                .set(b"alpha", b"one", Some(Duration::from_millis(10)))
                .expect("set alpha");
            assert_eq!(
                cache.get(b"alpha").expect("get alpha"),
                Some(b"one".to_vec())
            );
            std::thread::sleep(Duration::from_millis(15));
            assert_eq!(cache.get(b"alpha").expect("get alpha"), None);
        }
    }
}

pub mod db {
    use super::*;

    pub const HINT_DB: &str = "resource::db";
    pub const HINT_DB_READ: &str = "resource::db::read";
    pub const HINT_DB_WRITE: &str = "resource::db::write";

    static REGISTRATION: OnceLock<()> = OnceLock::new();

    /// Register effect/determinism constraints for relational database access.
    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_DB_READ,
                    dag_core::Effects::ReadOnly,
                    "Database reads reach external state; declare effects = ReadOnly or Effectful.",
                ),
            );
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_DB_WRITE,
                    dag_core::Effects::Effectful,
                    "Database writes mutate external systems; declare effects = Effectful and supply idempotency.",
                ),
            );
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_DB,
                    dag_core::Determinism::BestEffort,
                    "Database results can vary across retries; downgrade determinism or pin revisions.",
                ),
            );
        });
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn registers_constraints_once() {
            ensure_registered();
            ensure_registered();
            let read = dag_core::effects_registry::constraint_for_hint(HINT_DB_READ)
                .expect("db read constraint");
            assert_eq!(read.minimum, dag_core::Effects::ReadOnly);
            let write = dag_core::effects_registry::constraint_for_hint(HINT_DB_WRITE)
                .expect("db write constraint");
            assert_eq!(write.minimum, dag_core::Effects::Effectful);
            let det = dag_core::determinism::constraint_for_hint(HINT_DB).expect("db determinism");
            assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
        }
    }
}

pub mod kv {
    use super::*;
    use std::collections::HashMap;

    pub const HINT_KV: &str = "resource::kv";
    pub const HINT_KV_READ: &str = "resource::kv::read";
    pub const HINT_KV_WRITE: &str = "resource::kv::write";

    static REGISTRATION: OnceLock<()> = OnceLock::new();

    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_KV_READ,
                    dag_core::Effects::ReadOnly,
                    "KV reads access external state; declare effects = ReadOnly or stronger.",
                ),
            );
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_KV_WRITE,
                    dag_core::Effects::Effectful,
                    "KV writes are effectful; declare effects = Effectful and ensure dedupe/idempotency.",
                ),
            );
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_KV,
                    dag_core::Determinism::BestEffort,
                    "KV values may change between executions; downgrade determinism or pin versions.",
                ),
            );
        });
    }

    /// Errors surfaced by key-value capabilities.
    #[derive(Debug, thiserror::Error)]
    pub enum KvError {
        #[error("value not found")]
        NotFound,
        #[error("operation failed: {0}")]
        Other(String),
    }

    /// Generic key-value interface exposed to nodes.
    #[async_trait]
    pub trait KeyValue: Capability {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError>;
        async fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError>;
        async fn delete(&self, key: &str) -> Result<(), KvError>;
    }

    struct KvEntry {
        value: Vec<u8>,
        expires_at: Option<Instant>,
    }

    /// Simple in-memory KV store for tests and local dev.
    pub struct MemoryKv {
        entries: Mutex<HashMap<String, KvEntry>>,
    }

    impl MemoryKv {
        pub fn new() -> Self {
            Self {
                entries: Mutex::new(HashMap::new()),
            }
        }

        fn is_expired(expires_at: Option<Instant>) -> bool {
            match expires_at {
                Some(deadline) => Instant::now() > deadline,
                None => false,
            }
        }
    }

    impl Default for MemoryKv {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Capability for MemoryKv {
        fn name(&self) -> &'static str {
            "kv.memory"
        }
    }

    #[async_trait]
    impl KeyValue for MemoryKv {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
            let mut entries = self.entries.lock().expect("kv mutex poisoned");
            if let Some(entry) = entries.get(key) {
                if Self::is_expired(entry.expires_at) {
                    entries.remove(key);
                    return Ok(None);
                }
                return Ok(Some(entry.value.clone()));
            }
            Ok(None)
        }

        async fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError> {
            let mut entries = self.entries.lock().expect("kv mutex poisoned");
            let expires_at = ttl.map(|duration| Instant::now() + duration);
            entries.insert(
                key.to_owned(),
                KvEntry {
                    value: value.to_vec(),
                    expires_at,
                },
            );
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<(), KvError> {
            let mut entries = self.entries.lock().expect("kv mutex poisoned");
            entries.remove(key).map(|_| ()).ok_or(KvError::NotFound)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn registers_constraints_once() {
            ensure_registered();
            ensure_registered();
            let read = dag_core::effects_registry::constraint_for_hint(HINT_KV_READ)
                .expect("kv read constraint");
            assert_eq!(read.minimum, dag_core::Effects::ReadOnly);
            let write = dag_core::effects_registry::constraint_for_hint(HINT_KV_WRITE)
                .expect("kv write constraint");
            assert_eq!(write.minimum, dag_core::Effects::Effectful);
            let det = dag_core::determinism::constraint_for_hint(HINT_KV).expect("kv determinism");
            assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
        }

        #[tokio::test]
        async fn memory_kv_honours_ttl() {
            let kv = MemoryKv::new();
            kv.put("k", b"v", Some(Duration::from_millis(5)))
                .await
                .expect("put");
            assert_eq!(kv.get("k").await.expect("get"), Some(b"v".to_vec()));
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(kv.get("k").await.expect("expired"), None);
        }

        #[tokio::test]
        async fn memory_kv_delete_returns_not_found() {
            let kv = MemoryKv::new();
            assert!(matches!(kv.delete("missing").await, Err(KvError::NotFound)));
        }
    }
}

pub mod blob {
    use super::*;
    use std::collections::HashMap;

    pub const HINT_BLOB: &str = "resource::blob";
    pub const HINT_BLOB_READ: &str = "resource::blob::read";
    pub const HINT_BLOB_WRITE: &str = "resource::blob::write";

    static REGISTRATION: OnceLock<()> = OnceLock::new();

    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_BLOB_READ,
                    dag_core::Effects::ReadOnly,
                    "Blob reads access external storage; declare effects = ReadOnly or stronger.",
                ),
            );
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_BLOB_WRITE,
                    dag_core::Effects::Effectful,
                    "Blob writes mutate external storage; declare effects = Effectful and supply idempotency.",
                ),
            );
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_BLOB,
                    dag_core::Determinism::BestEffort,
                    "Blob storage responses can change over time; downgrade determinism or pin versions.",
                ),
            );
        });
    }

    /// Errors exposed by blob storage capabilities.
    #[derive(Debug, thiserror::Error)]
    pub enum BlobError {
        #[error("object not found")]
        NotFound,
        #[error("operation failed: {0}")]
        Other(String),
    }

    /// Blob storage interface consumed by nodes.
    #[async_trait]
    pub trait BlobStore: Capability {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BlobError>;
        async fn put(&self, key: &str, contents: &[u8]) -> Result<(), BlobError>;
        async fn delete(&self, key: &str) -> Result<(), BlobError>;
    }

    /// In-memory blob store for tests and local workflows.
    pub struct MemoryBlobStore {
        objects: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MemoryBlobStore {
        pub fn new() -> Self {
            Self {
                objects: Mutex::new(HashMap::new()),
            }
        }
    }

    impl Default for MemoryBlobStore {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Capability for MemoryBlobStore {
        fn name(&self) -> &'static str {
            "blob.memory"
        }
    }

    #[async_trait]
    impl BlobStore for MemoryBlobStore {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BlobError> {
            let objects = self.objects.lock().expect("blob mutex poisoned");
            Ok(objects.get(key).cloned())
        }

        async fn put(&self, key: &str, contents: &[u8]) -> Result<(), BlobError> {
            let mut objects = self.objects.lock().expect("blob mutex poisoned");
            objects.insert(key.to_owned(), contents.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<(), BlobError> {
            let mut objects = self.objects.lock().expect("blob mutex poisoned");
            objects.remove(key).map(|_| ()).ok_or(BlobError::NotFound)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn registers_constraints_once() {
            ensure_registered();
            ensure_registered();
            let read = dag_core::effects_registry::constraint_for_hint(HINT_BLOB_READ)
                .expect("blob read constraint");
            assert_eq!(read.minimum, dag_core::Effects::ReadOnly);
            let write = dag_core::effects_registry::constraint_for_hint(HINT_BLOB_WRITE)
                .expect("blob write constraint");
            assert_eq!(write.minimum, dag_core::Effects::Effectful);
            let det =
                dag_core::determinism::constraint_for_hint(HINT_BLOB).expect("blob determinism");
            assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
        }

        #[tokio::test]
        async fn memory_blob_round_trip() {
            let store = MemoryBlobStore::new();
            assert_eq!(store.get("missing").await.unwrap(), None);
            store.put("key", b"bytes").await.unwrap();
            assert_eq!(store.get("key").await.unwrap(), Some(b"bytes".to_vec()));
            store.delete("key").await.unwrap();
            assert_eq!(store.get("key").await.unwrap(), None);
        }

        #[tokio::test]
        async fn memory_blob_delete_missing() {
            let store = MemoryBlobStore::new();
            assert!(matches!(
                store.delete("missing").await,
                Err(BlobError::NotFound)
            ));
        }
    }
}

pub mod queue {
    use super::*;
    use std::collections::VecDeque;

    pub const HINT_QUEUE: &str = "resource::queue";
    pub const HINT_QUEUE_PUBLISH: &str = "resource::queue::publish";
    pub const HINT_QUEUE_CONSUME: &str = "resource::queue::consume";

    static REGISTRATION: OnceLock<()> = OnceLock::new();

    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_QUEUE_PUBLISH,
                    dag_core::Effects::Effectful,
                    "Queue publishes are effectful; ensure effects = Effectful with dedupe keys.",
                ),
            );
            dag_core::effects_registry::register_effect_constraint(
                dag_core::effects_registry::EffectConstraint::new(
                    HINT_QUEUE_CONSUME,
                    dag_core::Effects::ReadOnly,
                    "Queue consumption acknowledges messages; treat as at least ReadOnly.",
                ),
            );
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_QUEUE,
                    dag_core::Determinism::BestEffort,
                    "Queue ordering and visibility vary; downgrade determinism or add sequence checks.",
                ),
            );
        });
    }

    /// Errors raised by queue capabilities.
    #[derive(Debug, thiserror::Error)]
    pub enum QueueError {
        #[error("operation failed: {0}")]
        Other(String),
    }

    /// Queue capability interface covering enqueue/dequeue.
    pub trait Queue: Capability {
        fn enqueue(&self, payload: Vec<u8>) -> Result<(), QueueError>;
        fn dequeue(&self) -> Result<Option<Vec<u8>>, QueueError>;
    }

    /// In-memory queue backed by VecDeque for tests.
    pub struct MemoryQueue {
        entries: Mutex<VecDeque<Vec<u8>>>,
    }

    impl MemoryQueue {
        pub fn new() -> Self {
            Self {
                entries: Mutex::new(VecDeque::new()),
            }
        }
    }

    impl Default for MemoryQueue {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Capability for MemoryQueue {
        fn name(&self) -> &'static str {
            "queue.memory"
        }
    }

    impl Queue for MemoryQueue {
        fn enqueue(&self, payload: Vec<u8>) -> Result<(), QueueError> {
            let mut entries = self.entries.lock().expect("queue mutex poisoned");
            entries.push_back(payload);
            Ok(())
        }

        fn dequeue(&self) -> Result<Option<Vec<u8>>, QueueError> {
            let mut entries = self.entries.lock().expect("queue mutex poisoned");
            Ok(entries.pop_front())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn registers_constraints_once() {
            ensure_registered();
            ensure_registered();
            let publish = dag_core::effects_registry::constraint_for_hint(HINT_QUEUE_PUBLISH)
                .expect("queue publish constraint");
            assert_eq!(publish.minimum, dag_core::Effects::Effectful);
            let consume = dag_core::effects_registry::constraint_for_hint(HINT_QUEUE_CONSUME)
                .expect("queue consume constraint");
            assert_eq!(consume.minimum, dag_core::Effects::ReadOnly);
            let det =
                dag_core::determinism::constraint_for_hint(HINT_QUEUE).expect("queue determinism");
            assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
        }

        #[test]
        fn memory_queue_round_trip() {
            let queue = MemoryQueue::new();
            assert_eq!(queue.dequeue().unwrap(), None);
            queue.enqueue(b"a".to_vec()).unwrap();
            queue.enqueue(b"b".to_vec()).unwrap();
            assert_eq!(queue.dequeue().unwrap(), Some(b"a".to_vec()));
            assert_eq!(queue.dequeue().unwrap(), Some(b"b".to_vec()));
            assert_eq!(queue.dequeue().unwrap(), None);
        }
    }
}

pub mod rng {
    use super::*;

    pub const HINT_RNG: &str = "resource::rng";

    static REGISTRATION: OnceLock<()> = OnceLock::new();

    pub fn ensure_registered() {
        REGISTRATION.get_or_init(|| {
            dag_core::determinism::register_determinism_constraint(
                dag_core::determinism::DeterminismConstraint::new(
                    HINT_RNG,
                    dag_core::Determinism::BestEffort,
                    "Randomness is nondeterministic; downgrade determinism or inject fixed seeds.",
                ),
            );
        });
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn registers_constraints_once() {
            ensure_registered();
            ensure_registered();
            let det =
                dag_core::determinism::constraint_for_hint(HINT_RNG).expect("rng determinism");
            assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
        }
    }
}

#[cfg(test)]
mod bag_tests {
    use super::*;
    use std::sync::Arc;

    struct NullHttp;

    #[async_trait]
    impl http::HttpRead for NullHttp {
        async fn send(&self, _request: http::HttpRequest) -> http::HttpResult<http::HttpResponse> {
            Err(http::HttpError::Timeout(0))
        }
    }

    #[async_trait]
    impl http::HttpWrite for NullHttp {
        async fn send(&self, _request: http::HttpRequest) -> http::HttpResult<http::HttpResponse> {
            Err(http::HttpError::Timeout(0))
        }
    }

    #[test]
    fn resource_bag_exposes_capabilities() {
        clock::ensure_registered();
        let bag = ResourceBag::new()
            .with_http_read(Arc::new(NullHttp))
            .with_http_write(Arc::new(NullHttp))
            .with_clock(Arc::new(clock::SystemClock))
            .with_cache(Arc::new(cache::MemoryCache::default()))
            .with_kv(Arc::new(kv::MemoryKv::new()))
            .with_blob(Arc::new(blob::MemoryBlobStore::new()))
            .with_queue(Arc::new(queue::MemoryQueue::new()));

        assert!(bag.http_read().is_some());
        assert!(bag.http_write().is_some());
        assert!(bag.clock().is_some());
        assert!(bag.cache().is_some());
        assert!(bag.kv().is_some());
        assert!(bag.blob().is_some());
        assert!(bag.queue().is_some());
    }

    #[test]
    fn context_scopes_resources() {
        use crate::kv::KeyValue;
        let kv_store = Arc::new(kv::MemoryKv::new());
        let bag = ResourceBag::new().with_kv(kv_store.clone());
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async {
            let bag_arc = Arc::new(bag);
            context::with_resources(bag_arc.clone(), async {
                context::with_current_async(|resources| async move {
                    let kv = resources.kv().expect("kv capability available");
                    kv.put("ctx", b"value", None).await.expect("kv put");
                })
                .await
                .expect("resource scope available");
            })
            .await;
        });

        assert_eq!(
            rt.block_on(async { kv_store.get("ctx").await.expect("kv get after scope") }),
            Some(b"value".to_vec())
        );
        assert!(context::with_current::<_, ()>(|_| ()).is_none());
    }
}
