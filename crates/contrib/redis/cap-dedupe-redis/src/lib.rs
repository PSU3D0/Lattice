//! Redis-backed implementation of the `DedupeStore` capability.

use std::time::Duration;

use async_trait::async_trait;
use cap_redis::{RedisConfig, RedisConnectionFactory};
use capabilities::Capability;
use capabilities::dedupe::{DedupeError, DedupeStore};
use hex::ToHex;
use tracing::instrument;

const VALUE_PRESENT: &str = "1";
const DEFAULT_TTL_SECS: u64 = 300;

/// Redis-backed dedupe store with namespaced keys and TTL floor enforcement.
#[derive(Clone, Debug)]
pub struct RedisDedupeStore {
    factory: RedisConnectionFactory,
    ttl_floor: Duration,
}

impl RedisDedupeStore {
    /// Constructs a new store from the provided configuration.
    pub fn new(config: RedisConfig) -> anyhow::Result<Self> {
        Self::with_factory(RedisConnectionFactory::new(config)?)
    }

    /// Creates a store from an existing connection factory (useful for tests).
    pub fn with_factory(factory: RedisConnectionFactory) -> anyhow::Result<Self> {
        capabilities::dedupe::ensure_registered();
        Ok(Self {
            factory,
            ttl_floor: Duration::from_secs(DEFAULT_TTL_SECS),
        })
    }

    /// Overrides the minimum TTL applied to inserted keys.
    pub fn with_ttl_floor(mut self, ttl_floor: Duration) -> Self {
        self.ttl_floor = ttl_floor;
        self
    }

    fn encode_key(&self, key: &[u8]) -> String {
        key.encode_hex::<String>()
    }

    fn namespaced_key(&self, key: &[u8]) -> String {
        let encoded = self.encode_key(key);
        self.factory.namespaced_key(&encoded)
    }

    fn ttl_for(&self, ttl: Duration) -> Duration {
        if ttl < self.ttl_floor {
            self.ttl_floor
        } else {
            ttl
        }
    }
}

impl Capability for RedisDedupeStore {
    fn name(&self) -> &'static str {
        "dedupe.redis"
    }
}

#[async_trait]
impl DedupeStore for RedisDedupeStore {
    #[instrument(skip_all, fields(ttl = ?ttl))]
    async fn put_if_absent(&self, key: &[u8], ttl: Duration) -> Result<bool, DedupeError> {
        let ttl_duration = self.ttl_for(ttl);
        let ttl_ms = u64::try_from(ttl_duration.as_millis())
            .map_err(|_| DedupeError::Other("ttl too large to fit in u64".into()))?;

        let namespaced = self.namespaced_key(key);
        let mut conn = self
            .factory
            .connection()
            .await
            .map_err(|err| DedupeError::Other(err.to_string()))?;

        let inserted: Option<String> = redis::cmd("SET")
            .arg(&namespaced)
            .arg(VALUE_PRESENT)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms)
            .query_async(&mut *conn)
            .await
            .map_err(|err| DedupeError::Other(err.to_string()))?;

        Ok(inserted.is_some())
    }

    #[instrument(skip_all)]
    async fn forget(&self, key: &[u8]) -> Result<(), DedupeError> {
        let namespaced = self.namespaced_key(key);
        let mut conn = self
            .factory
            .connection()
            .await
            .map_err(|err| DedupeError::Other(err.to_string()))?;
        let _: i64 = redis::cmd("DEL")
            .arg(&namespaced)
            .query_async(&mut *conn)
            .await
            .map_err(|err| DedupeError::Other(err.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_floor_enforced() {
        let store = RedisDedupeStore {
            factory: RedisConnectionFactory::new(RedisConfig::default()).unwrap(),
            ttl_floor: Duration::from_secs(10),
        };
        assert_eq!(
            store.ttl_for(Duration::from_secs(1)),
            Duration::from_secs(10)
        );
        assert_eq!(
            store.ttl_for(Duration::from_secs(20)),
            Duration::from_secs(20)
        );
    }

    #[test]
    fn key_encoding_uses_hex() {
        let store = RedisDedupeStore::with_factory(
            RedisConnectionFactory::new(RedisConfig::default()).unwrap(),
        )
        .unwrap();
        let encoded = store.encode_key(b"\x01\x02");
        assert_eq!(encoded, "0102");
    }
}
