//! OpenDAL-backed key-value capability.
//!
//! Provides [`OpendalKvStore`], an implementation of
//! [`capabilities::kv::KeyValue`] with optional capability metadata for TTL
//! support and consistency guarantees.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::time::Duration;

use async_trait::async_trait;
use cap_opendal_core::{OpendalError, OpendalResult, OperatorFactory};
use capabilities::{
    Capability,
    kv::{KeyValue, KvError},
};
use opendal::Operator;
use opendal::raw::{Accessor, Layer};
use serde::{Deserialize, Serialize};

/// Capability metadata describing TTL support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvTtlSupport {
    /// Backend ignores TTL hints; values never expire.
    None,
    /// Backend accepts per-write TTL values.
    PerWrite,
    /// Backend enforces a namespace-level default TTL.
    NamespaceDefault(Duration),
}

/// Consistency guarantees surfaced by the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvConsistency {
    /// Operations are strongly consistent.
    Strong,
    /// Backend provides best-effort or eventual consistency.
    Eventual,
}

/// Describes the capability guarantees for an OpenDAL-backed KV store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvCapabilityInfo {
    /// TTL support advertised by the backend.
    pub ttl: KvTtlSupport,
    /// Consistency guarantee.
    pub consistency: KvConsistency,
}

impl KvCapabilityInfo {
    /// Construct capability info.
    pub const fn new(ttl: KvTtlSupport, consistency: KvConsistency) -> Self {
        Self { ttl, consistency }
    }
}

/// Builder for OpenDAL-backed key-value stores.
pub struct KvStoreBuilder<F> {
    factory: F,
    capability_name: &'static str,
    capability_info: KvCapabilityInfo,
    transforms: Vec<Box<dyn FnOnce(Operator) -> Operator + Send>>,
    default_ttl: Option<Duration>,
    ttl_floor: Option<Duration>,
}

const DEFAULT_CAPABILITY_NAME: &str = "kv.opendal";

impl<F> KvStoreBuilder<F>
where
    F: OperatorFactory,
{
    /// Create a new builder backed by `factory`.
    pub fn new(factory: F) -> Self {
        Self {
            factory,
            capability_name: DEFAULT_CAPABILITY_NAME,
            capability_info: KvCapabilityInfo::new(KvTtlSupport::None, KvConsistency::Eventual),
            transforms: Vec::new(),
            default_ttl: None,
            ttl_floor: None,
        }
    }

    /// Override capability name.
    pub fn capability_name(mut self, capability_name: &'static str) -> Self {
        self.capability_name = capability_name;
        self
    }

    /// Set capability metadata.
    pub fn capability_info(mut self, info: KvCapabilityInfo) -> Self {
        self.capability_info = info;
        self
    }

    /// Configure namespace default TTL when backend supports it.
    pub fn default_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Minimum TTL accepted by backend (e.g., Cloudflare requires >=60s).
    pub fn ttl_floor(mut self, floor: Option<Duration>) -> Self {
        self.ttl_floor = floor;
        self
    }

    /// Apply arbitrary operator transform.
    pub fn map_operator(
        mut self,
        transform: impl FnOnce(Operator) -> Operator + Send + 'static,
    ) -> Self {
        self.transforms.push(Box::new(transform));
        self
    }

    /// Attach OpenDAL layer.
    pub fn with_layer<L>(self, layer: L) -> Self
    where
        L: Layer<Accessor> + Send + Sync + 'static,
    {
        self.map_operator(move |operator| operator.layer(layer))
    }

    /// Build the capability.
    pub async fn build(self) -> OpendalResult<OpendalKvStore> {
        let KvStoreBuilder {
            factory,
            capability_name,
            capability_info,
            transforms,
            default_ttl,
            ttl_floor,
        } = self;

        let mut operator = factory.build().await?;
        for transform in transforms {
            operator = transform(operator);
        }

        Ok(OpendalKvStore {
            operator,
            capability_name,
            capability_info,
            default_ttl,
            ttl_floor,
        })
    }
}

/// OpenDAL-backed key-value store.
pub struct OpendalKvStore {
    operator: Operator,
    capability_name: &'static str,
    capability_info: KvCapabilityInfo,
    default_ttl: Option<Duration>,
    ttl_floor: Option<Duration>,
}

impl OpendalKvStore {
    /// Create a builder from an operator factory.
    pub fn builder<F>(factory: F) -> KvStoreBuilder<F>
    where
        F: OperatorFactory,
    {
        KvStoreBuilder::new(factory)
    }

    /// Access capability metadata.
    pub fn capability(&self) -> KvCapabilityInfo {
        self.capability_info
    }

    /// Access namespace default TTL (if configured).
    pub fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl
    }

    /// Access TTL floor requirement (if any).
    pub fn ttl_floor(&self) -> Option<Duration> {
        self.ttl_floor
    }

    /// Expose the underlying operator for advanced use cases.
    pub fn operator(&self) -> &Operator {
        &self.operator
    }
}

impl Capability for OpendalKvStore {
    fn name(&self) -> &'static str {
        self.capability_name
    }
}

#[async_trait]
impl KeyValue for OpendalKvStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        match self.operator.read(key).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(err) => match OpendalError::from_opendal(err) {
                OpendalError::NotFound => Ok(None),
                other => Err(map_kv_error(other)),
            },
        }
    }

    async fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError> {
        if ttl.is_some() && !matches!(self.capability_info.ttl, KvTtlSupport::PerWrite) {
            return Err(KvError::Other(
                "backend does not support per-write TTL".to_string(),
            ));
        }

        let ttl = ttl.or(self.default_ttl);
        if let (Some(floor), Some(ttl_value)) = (self.ttl_floor, ttl)
            && ttl_value < floor
        {
            return Err(KvError::Other(format!(
                "ttl {}s below floor {}s",
                ttl_value.as_secs(),
                floor.as_secs()
            )));
        }

        self.operator
            .write(key, value.to_vec())
            .await
            .map(|_| ())
            .map_err(|err| map_kv_error(OpendalError::from_opendal(err)))
    }

    async fn delete(&self, key: &str) -> Result<(), KvError> {
        if let Err(err) = self.operator.stat(key).await {
            return match OpendalError::from_opendal(err) {
                OpendalError::NotFound => Err(KvError::NotFound),
                other => Err(map_kv_error(other)),
            };
        }

        self.operator
            .delete(key)
            .await
            .map(|_| ())
            .map_err(|err| map_kv_error(OpendalError::from_opendal(err)))
    }
}

fn map_kv_error(error: OpendalError) -> KvError {
    match error {
        OpendalError::NotFound => KvError::NotFound,
        other => KvError::Other(other.to_string()),
    }
}

#[cfg(all(test, feature = "services-memory"))]
mod tests {
    use super::*;
    use cap_opendal_core::SchemeOperatorFactory;
    use capabilities::kv::KeyValue;
    use opendal::Scheme;

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trip_memory_backend() {
        let factory = SchemeOperatorFactory::new(Scheme::Memory, [("root", "/")]);
        let store = OpendalKvStore::builder(factory)
            .capability_info(KvCapabilityInfo::new(
                KvTtlSupport::PerWrite,
                KvConsistency::Strong,
            ))
            .build()
            .await
            .expect("build kv store");

        assert_eq!(store.get("missing").await.unwrap(), None);
        store
            .put("key", b"value", Some(Duration::from_secs(5)))
            .await
            .unwrap();
        assert_eq!(store.get("key").await.unwrap(), Some(b"value".to_vec()));
        store.delete("key").await.unwrap();
        assert_eq!(store.get("key").await.unwrap(), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delete_missing_reports_not_found() {
        let factory = SchemeOperatorFactory::new(Scheme::Memory, [("root", "/")]);
        let store = OpendalKvStore::builder(factory)
            .build()
            .await
            .expect("build kv");
        let err = store.delete("missing").await.unwrap_err();
        assert!(matches!(err, KvError::NotFound));
    }
}
