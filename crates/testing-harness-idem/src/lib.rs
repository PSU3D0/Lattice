use std::time::Duration;

use capabilities::dedupe::DedupeStore;
use capabilities::kv::{KeyValue, MemoryKv};
use tokio::time::sleep;

/// Summary of an idempotency harness run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessReport {
    /// Total attempts supplied to the harness.
    pub attempts: usize,
    /// Number of times the underlying store accepted the value.
    pub applied: usize,
    /// Number of duplicate attempts that were blocked.
    pub duplicates_blocked: usize,
    /// Whether the key expired after the supplied TTL.
    pub ttl_verified: bool,
}

impl HarnessReport {
    /// Returns `true` when the harness observed exactly-once behaviour.
    pub fn passed(&self) -> bool {
        self.applied == 1
            && self.duplicates_blocked == self.attempts.saturating_sub(1)
            && self.ttl_verified
    }
}

/// Summary of a dedupe store harness run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupeHarnessReport {
    /// Total attempts made against the store before TTL expiry.
    pub attempts: usize,
    /// Number of times the store accepted the reservation.
    pub reserved: usize,
    /// Number of duplicate attempts that were rejected.
    pub duplicates_blocked: usize,
    /// Whether the key was accepted again after TTL expiry.
    pub ttl_verified: bool,
    /// Whether `forget` succeeded after the TTL verification attempt.
    pub forget_verified: bool,
}

impl DedupeHarnessReport {
    /// Returns `true` when the harness observed expected duplicate suppression semantics.
    pub fn passed(&self) -> bool {
        self.reserved == 1
            && self.duplicates_blocked == self.attempts.saturating_sub(1)
            && self.ttl_verified
            && self.forget_verified
    }
}

/// Run the idempotency harness against a [`KeyValue`] implementation.
///
/// The harness will attempt to access the same key `attempts` times; the first attempt should
/// succeed, while each subsequent attempt is expected to be rejected. After the TTL elapses, the
/// harness ensures the key expires and can be re-applied.
pub async fn verify_kv_idempotency<K>(
    kv: &K,
    key: &str,
    value: &[u8],
    ttl: Duration,
    attempts: usize,
) -> HarnessReport
where
    K: KeyValue + Sync,
{
    let mut applied = 0;
    let mut duplicates = 0;

    for _ in 0..attempts.max(1) {
        match kv.get(key).await {
            Ok(Some(_)) => {
                duplicates += 1;
            }
            Ok(None) => {
                kv.put(key, value, Some(ttl))
                    .await
                    .expect("kv::put should succeed during harness run");
                applied += 1;
            }
            Err(err) => panic!("kv::get failed during harness run: {err:?}"),
        }
    }

    tokio::time::sleep(ttl + Duration::from_millis(10)).await;
    let ttl_verified = matches!(kv.get(key).await, Ok(None));

    HarnessReport {
        attempts,
        applied,
        duplicates_blocked: duplicates,
        ttl_verified,
    }
}

/// Convenience helper that runs the harness against the in-memory KV store.
pub async fn verify_memory_kv(ttl: Duration, attempts: usize) -> HarnessReport {
    let kv = MemoryKv::new();
    verify_kv_idempotency(&kv, "idem:key", b"payload", ttl, attempts).await
}

/// Run the dedupe harness against a [`DedupeStore`] implementation.
///
/// The harness attempts to reserve the same key multiple times, expecting only
/// the first reservation to succeed. After the TTL elapses, the harness checks
/// that the key can be reserved again and that `forget` succeeds.
pub async fn verify_dedupe_store<D>(
    store: &D,
    key: &[u8],
    ttl: Duration,
    attempts: usize,
) -> DedupeHarnessReport
where
    D: DedupeStore,
{
    let mut reserved = 0;
    let mut duplicates = 0;

    for _ in 0..attempts.max(1) {
        match store.put_if_absent(key, ttl).await {
            Ok(true) => reserved += 1,
            Ok(false) => duplicates += 1,
            Err(err) => panic!("dedupe store put_if_absent failed during harness run: {err:?}"),
        }
    }

    sleep(ttl + Duration::from_millis(10)).await;
    let ttl_verified = match store.put_if_absent(key, ttl).await {
        Ok(true) => true,
        Ok(false) => false,
        Err(err) => panic!("dedupe store ttl verification failed: {err:?}"),
    };

    let forget_verified = store.forget(key).await.is_ok();

    DedupeHarnessReport {
        attempts,
        reserved,
        duplicates_blocked: duplicates,
        ttl_verified,
        forget_verified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    use cap_dedupe_redis::RedisDedupeStore;
    use cap_redis::{RedisConfig, RedisConnectionFactory};
    use testcontainers::{
        GenericImage,
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn memory_kv_enforces_idempotency_with_ttl() {
        let report = verify_memory_kv(Duration::from_millis(50), 5).await;
        assert_eq!(report.attempts, 5);
        assert_eq!(report.applied, 1);
        assert_eq!(report.duplicates_blocked, 4);
        assert!(report.ttl_verified);
        assert!(report.passed());
    }

    #[tokio::test]
    async fn harness_handles_single_attempt() {
        let kv = MemoryKv::new();
        let report =
            verify_kv_idempotency(&kv, "single", b"value", Duration::from_millis(10), 1).await;
        assert_eq!(report.applied, 1);
        assert_eq!(report.duplicates_blocked, 0);
        assert!(report.ttl_verified);
        assert!(report.passed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redis_dedupe_store_passes_harness() {
        let container = GenericImage::new("redis", "7.2")
            .with_exposed_port(6379_u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
            .expect("start redis container");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("retrieve redis port");

        let mut config = RedisConfig::default();
        config.url = format!("redis://127.0.0.1:{port}/");
        config.namespace = Some(format!("lf:dedupe:{}:", Uuid::new_v4()));
        config.max_connections = NonZeroUsize::new(4).expect("non-zero connections");

        let factory = RedisConnectionFactory::new(config.clone()).expect("redis factory");
        {
            let mut conn = factory.connection().await.expect("redis connection");
            redis::cmd("FLUSHALL")
                .query_async::<()>(&mut *conn)
                .await
                .expect("flush redis");
        }

        let store = RedisDedupeStore::with_factory(factory)
            .expect("redis dedupe store")
            .with_ttl_floor(Duration::from_millis(20));

        let report = verify_dedupe_store(&store, b"dedupe-key", Duration::from_millis(20), 4).await;

        assert_eq!(report.attempts, 4);
        assert_eq!(report.reserved, 1);
        assert_eq!(report.duplicates_blocked, 3);
        assert!(report.ttl_verified);
        assert!(report.forget_verified);
        assert!(report.passed());
    }
}
