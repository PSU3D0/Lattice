//! Redis-backed cache capability implementation.

use std::future::Future;
use std::time::Duration;

use cap_redis::{RedisConfig, RedisConnectionFactory};
use capabilities::Capability;
use capabilities::cache::{Cache, CacheError};
use hex::ToHex;
use tokio::runtime::Handle;
use tracing::instrument;

const DEFAULT_TTL_SECS: u64 = 300;

/// Redis-based cache with key namespacing and TTL handling.
#[derive(Clone, Debug)]
pub struct RedisCache {
    factory: RedisConnectionFactory,
    default_ttl: Option<Duration>,
}

impl RedisCache {
    /// Construct a cache from the provided configuration.
    pub fn new(config: RedisConfig) -> anyhow::Result<Self> {
        Self::with_factory(RedisConnectionFactory::new(config)?)
    }

    /// Create a cache from an existing factory (useful for tests).
    pub fn with_factory(factory: RedisConnectionFactory) -> anyhow::Result<Self> {
        Ok(Self {
            factory,
            default_ttl: Some(Duration::from_secs(DEFAULT_TTL_SECS)),
        })
    }

    /// Overrides the default TTL applied when callers do not provide one.
    pub fn with_default_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.default_ttl = ttl;
        self
    }

    fn encode_key(&self, key: &[u8]) -> String {
        key.encode_hex::<String>()
    }

    fn namespaced_key(&self, key: &[u8]) -> String {
        let encoded = self.encode_key(key);
        self.factory.namespaced_key(&encoded)
    }

    fn ttl_millis(&self, ttl: Option<Duration>) -> Option<u64> {
        ttl.or(self.default_ttl)
            .map(|duration| duration.as_millis())
            .and_then(|millis| u64::try_from(millis).ok())
    }

    fn block_on<F, T>(&self, fut: F) -> Result<T, CacheError>
    where
        F: Future<Output = Result<T, CacheError>> + Send + 'static,
        T: Send + 'static,
    {
        let handle = Handle::try_current()
            .map_err(|_| CacheError::Other("async runtime unavailable".into()))?;
        handle.block_on(fut)
    }
}

impl Capability for RedisCache {
    fn name(&self) -> &'static str {
        "cache.redis"
    }
}

impl Cache for RedisCache {
    #[instrument(skip(self, key))]
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, CacheError> {
        let factory = self.factory.clone();
        let namespaced = self.namespaced_key(key);
        self.block_on(async move {
            let mut conn = factory
                .connection()
                .await
                .map_err(|err| CacheError::Other(err.to_string()))?;
            let value: Option<Vec<u8>> = redis::cmd("GET")
                .arg(&namespaced)
                .query_async(&mut *conn)
                .await
                .map_err(|err| CacheError::Other(err.to_string()))?;
            Ok(value)
        })
    }

    #[instrument(skip(self, key, value), fields(ttl = ?ttl))]
    fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let factory = self.factory.clone();
        let namespaced = self.namespaced_key(key);
        let ttl_ms = self.ttl_millis(ttl);
        let payload = value.to_vec();
        self.block_on(async move {
            let mut conn = factory
                .connection()
                .await
                .map_err(|err| CacheError::Other(err.to_string()))?;
            let mut cmd = redis::cmd("SET");
            cmd.arg(&namespaced).arg(payload);
            if let Some(ttl_ms) = ttl_ms {
                cmd.arg("PX").arg(ttl_ms);
            }
            cmd.query_async::<()>(&mut *conn)
                .await
                .map_err(|err| CacheError::Other(err.to_string()))?;
            Ok(())
        })
    }

    #[instrument(skip(self, key))]
    fn remove(&self, key: &[u8]) -> Result<(), CacheError> {
        let factory = self.factory.clone();
        let namespaced = self.namespaced_key(key);
        self.block_on(async move {
            let mut conn = factory
                .connection()
                .await
                .map_err(|err| CacheError::Other(err.to_string()))?;
            let removed: i64 = redis::cmd("DEL")
                .arg(&namespaced)
                .query_async(&mut *conn)
                .await
                .map_err(|err| CacheError::Other(err.to_string()))?;
            if removed == 0 {
                return Err(CacheError::NotFound);
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_millis_respects_default() {
        let cache =
            RedisCache::with_factory(RedisConnectionFactory::new(RedisConfig::default()).unwrap())
                .unwrap();
        assert_eq!(cache.ttl_millis(None), Some(DEFAULT_TTL_SECS * 1000));
        assert_eq!(
            cache.ttl_millis(Some(Duration::from_secs(10))),
            Some(10_000)
        );
    }

    #[test]
    fn namespacing_encodes_binary_keys() {
        let cache = RedisCache::with_factory(
            RedisConnectionFactory::new(RedisConfig {
                url: "redis://127.0.0.1/".to_string(),
                namespace: Some("lf:test:".to_string()),
                max_connections: std::num::NonZeroUsize::new(1).unwrap(),
            })
            .unwrap(),
        )
        .unwrap();
        let key = cache.namespaced_key(b"\x01\x02");
        assert_eq!(key, "lf:test:0102");
    }
}
