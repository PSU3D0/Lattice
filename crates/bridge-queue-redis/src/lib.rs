//! Redis-backed queue bridge that feeds invocations into the in-process host runtime.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Error as AnyhowError;
use cap_redis::{RedisConfig, RedisConnectionFactory};
use capabilities::dedupe::{DedupeError, DedupeStore};
use host_inproc::{HostRuntime, Invocation, InvocationMetadata, InvocationParts};
use metrics::{GaugeValue, counter, gauge};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

const DEFAULT_BLOCKING_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_BACKOFF: Duration = Duration::from_millis(250);
const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_LEASE_EXTENSION: Duration = Duration::from_secs(10);
const DEFAULT_MINIMUM_DEDUPE_TTL: Duration = Duration::from_secs(300);
const LEASE_REAPER_BATCH: isize = 32;
const DEDUPE_LABEL_KEY: &str = "lf.dedupe.key";
const DEDUPE_LABEL_TTL: &str = "lf.dedupe.ttl_ms";
const DEDUPE_EXTENSION: &str = "lf.dedupe";

/// Errors surfaced by the queue bridge.
#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("redis connection error: {0}")]
    Connection(#[from] AnyhowError),
    #[error("redis command failed: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("failed to (de)serialise queue payload: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("dedupe operation failed: {0}")]
    Dedupe(String),
    #[error("workflow execution failed: {0}")]
    ExecutionFailure(String),
}

impl From<DedupeError> for BridgeError {
    fn from(err: DedupeError) -> Self {
        BridgeError::Dedupe(err.to_string())
    }
}

/// Configuration for the Redis queue bridge.
#[derive(Debug, Clone)]
pub struct QueueBridgeConfig {
    pub redis: RedisConfig,
    pub queue_key: String,
    pub metrics_queue: String,
    pub bridge_name: String,
    pub blocking_timeout: Duration,
    pub idle_backoff: Duration,
    pub visibility_timeout: Duration,
    pub lease_extension: Duration,
    pub minimum_dedupe_ttl: Duration,
}

impl QueueBridgeConfig {
    /// Configuration suitable for local development and unit tests.
    pub fn development(queue_key: impl Into<String>) -> Self {
        let queue_key = queue_key.into();
        Self {
            redis: RedisConfig::default(),
            metrics_queue: queue_key.clone(),
            bridge_name: "queue_redis".into(),
            queue_key,
            blocking_timeout: DEFAULT_BLOCKING_TIMEOUT,
            idle_backoff: DEFAULT_IDLE_BACKOFF,
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            lease_extension: DEFAULT_LEASE_EXTENSION,
            minimum_dedupe_ttl: DEFAULT_MINIMUM_DEDUPE_TTL,
        }
    }
}

/// Redis-backed bridge that exposes enqueue APIs and worker management.
#[derive(Clone)]
pub struct QueueBridge {
    runtime: HostRuntime,
    factory: RedisConnectionFactory,
    ready_key: Arc<String>,
    processing_key: Arc<String>,
    lease_key: Arc<String>,
    blocking_timeout: Duration,
    idle_backoff: Duration,
    visibility_timeout: Duration,
    lease_extension: Duration,
    minimum_dedupe_ttl: Duration,
    metrics: QueueMetrics,
    dedupe_store: Option<Arc<dyn DedupeStore + Send + Sync>>,
}

impl QueueBridge {
    /// Build a new bridge from the supplied runtime and configuration.
    pub fn new(runtime: HostRuntime, config: QueueBridgeConfig) -> anyhow::Result<Self> {
        let factory = RedisConnectionFactory::new(config.redis)?;
        let ready_key = Arc::new(factory.namespaced_key(&format!("{}:ready", config.queue_key)));
        let processing_key =
            Arc::new(factory.namespaced_key(&format!("{}:processing", config.queue_key)));
        let lease_key = Arc::new(factory.namespaced_key(&format!("{}:leases", config.queue_key)));
        let metrics = QueueMetrics::new(config.bridge_name, config.metrics_queue);

        Ok(Self {
            runtime,
            factory,
            ready_key,
            processing_key,
            lease_key,
            blocking_timeout: config.blocking_timeout,
            idle_backoff: config.idle_backoff,
            visibility_timeout: config.visibility_timeout,
            lease_extension: config.lease_extension,
            minimum_dedupe_ttl: config.minimum_dedupe_ttl,
            metrics,
            dedupe_store: None,
        })
    }

    /// Attach a dedupe store used for Delivery::ExactlyOnce enforcement.
    pub fn with_dedupe_store(mut self, store: Arc<dyn DedupeStore + Send + Sync>) -> Self {
        self.dedupe_store = Some(store);
        self
    }

    /// Enqueue an invocation onto the Redis queue.
    #[instrument(skip_all, fields(trigger = invocation_trigger(invocation.clone())))]
    pub async fn enqueue(&self, invocation: Invocation) -> Result<(), BridgeError> {
        let message = QueueMessage::from_invocation(invocation);
        let payload = serde_json::to_string(&message)?;

        let mut connection = self.factory.connection().await?;
        redis::cmd("RPUSH")
            .arg(self.ready_key.as_str())
            .arg(&payload)
            .query_async::<i64>(&mut *connection)
            .await?;
        self.metrics.record_enqueue();
        Ok(())
    }

    /// Spawn `worker_count` tasks that drain the queue until the supplied cancellation token fires.
    pub fn spawn_workers(
        &self,
        worker_count: usize,
        shutdown: CancellationToken,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::with_capacity(worker_count + 1);
        handles.push(self.spawn_reaper(shutdown.clone()));
        for id in 0..worker_count {
            let bridge = self.clone();
            let token = shutdown.clone();
            handles.push(tokio::spawn(async move {
                info!(worker = id, "queue worker started");
                bridge.worker_loop(id, token).await;
                info!(worker = id, "queue worker stopped");
            }));
        }
        handles
    }

    #[instrument(skip(self, shutdown))]
    async fn worker_loop(&self, worker_id: usize, shutdown: CancellationToken) {
        while !shutdown.is_cancelled() {
            match self.next_message().await {
                Ok(Some(mut message)) => {
                    self.metrics.record_dequeue();
                    self.metrics.increment_inflight();

                    match self.process_message(worker_id, &mut message).await {
                        Ok(()) => self.metrics.record_success(),
                        Err(err) => {
                            warn!(worker = worker_id, error = %err, "processing failed; requeueing");
                            if let Err(requeue_err) = self.requeue(&message.payload).await {
                                error!(worker = worker_id, error = %requeue_err, "failed to requeue message");
                            } else {
                                self.metrics.record_requeue();
                            }
                        }
                    }

                    self.metrics.decrement_inflight();
                }
                Ok(None) => sleep(self.idle_backoff).await,
                Err(err) => {
                    warn!(worker = worker_id, error = %err, "failed to fetch next invocation");
                    sleep(self.idle_backoff).await;
                }
            }
        }
    }

    async fn process_message(
        &self,
        worker_id: usize,
        message: &mut PendingMessage,
    ) -> Result<(), BridgeError> {
        let mut dedupe_reservation = self.reserve_dedupe(&message.message).await?;

        if matches!(dedupe_reservation, DedupeReservation::Duplicate) {
            self.metrics.record_duplicate();
            self.ack(&message.payload).await?;
            return Ok(());
        }

        let extender = self.spawn_lease_extender(message.payload.clone());
        let invocation = message.message.envelope.clone().into_invocation();

        let result = self.runtime.execute(invocation).await;
        extender.abort();

        match result {
            Ok(_) => {
                self.ack(&message.payload).await?;
                if let DedupeReservation::Reserved(reservation) = dedupe_reservation.take() {
                    reservation.commit();
                }
                Ok(())
            }
            Err(err) => {
                error!(worker = worker_id, error = %err, "workflow execution failed");
                if let DedupeReservation::Reserved(reservation) = dedupe_reservation.take() {
                    reservation.release().await?;
                }
                self.metrics.record_failure();
                Err(BridgeError::ExecutionFailure(err.to_string()))
            }
        }
    }

    async fn next_message(&self) -> Result<Option<PendingMessage>, BridgeError> {
        let timeout_secs = self.blocking_timeout.as_secs().clamp(1, u64::MAX);
        let mut connection = self.factory.connection().await?;
        let result: Option<String> = redis::cmd("BRPOPLPUSH")
            .arg(self.ready_key.as_str())
            .arg(self.processing_key.as_str())
            .arg(timeout_secs)
            .query_async(&mut *connection)
            .await?;

        let Some(payload) = result else {
            return Ok(None);
        };

        let message: QueueMessage = serde_json::from_str(&payload)?;
        self.register_lease(&payload).await?;
        Ok(Some(PendingMessage { payload, message }))
    }

    async fn register_lease(&self, payload: &str) -> Result<(), BridgeError> {
        let deadline = now_millis() + self.visibility_timeout.as_millis() as u64;
        let mut connection = self.factory.connection().await?;
        redis::cmd("ZADD")
            .arg(self.lease_key.as_str())
            .arg(deadline as f64)
            .arg(payload)
            .query_async::<()>(&mut *connection)
            .await?;
        self.metrics.record_lease_extended();
        Ok(())
    }

    async fn ack(&self, payload: &str) -> Result<(), BridgeError> {
        let mut connection = self.factory.connection().await?;
        redis::pipe()
            .cmd("LREM")
            .arg(self.processing_key.as_str())
            .arg(1)
            .arg(payload)
            .cmd("ZREM")
            .arg(self.lease_key.as_str())
            .arg(payload)
            .query_async::<()>(&mut *connection)
            .await?;
        Ok(())
    }

    async fn requeue(&self, payload: &str) -> Result<(), BridgeError> {
        let mut connection = self.factory.connection().await?;
        redis::pipe()
            .cmd("LREM")
            .arg(self.processing_key.as_str())
            .arg(1)
            .arg(payload)
            .cmd("ZREM")
            .arg(self.lease_key.as_str())
            .arg(payload)
            .cmd("LPUSH")
            .arg(self.ready_key.as_str())
            .arg(payload)
            .query_async::<()>(&mut *connection)
            .await?;
        Ok(())
    }

    fn spawn_lease_extender(&self, payload: String) -> JoinHandle<()> {
        let lease_key = Arc::clone(&self.lease_key);
        let factory = self.factory.clone();
        let interval_duration = self.lease_extension;
        let visibility_timeout = self.visibility_timeout;
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let mut ticker = interval(interval_duration);
            loop {
                ticker.tick().await;
                let mut connection = match factory.connection().await {
                    Ok(conn) => conn,
                    Err(err) => {
                        warn!(error = %err, "failed to extend lease");
                        continue;
                    }
                };
                let deadline = now_millis() + visibility_timeout.as_millis() as u64;
                if let Err(err) = redis::cmd("ZADD")
                    .arg(lease_key.as_str())
                    .arg("XX")
                    .arg(deadline as f64)
                    .arg(&payload)
                    .query_async::<()>(&mut *connection)
                    .await
                {
                    warn!(error = %err, "failed to extend lease");
                } else {
                    debug!("extended lease for queued task");
                    metrics.record_lease_extended();
                }
            }
        })
    }

    fn spawn_reaper(&self, shutdown: CancellationToken) -> JoinHandle<()> {
        let bridge = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(bridge.visibility_timeout.min(Duration::from_secs(5)));
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        match bridge.reclaim_expired().await {
                            Ok(count) if count > 0 => bridge.metrics.record_lease_expired(count as u64),
                            Ok(_) => {},
                            Err(err) => warn!(error = %err, "failed to reclaim expired leases"),
                        }
                    }
                }
            }
        })
    }

    async fn reclaim_expired(&self) -> Result<usize, BridgeError> {
        let mut connection = self.factory.connection().await?;
        let now = now_millis() as f64;
        let expired: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(self.lease_key.as_str())
            .arg("-inf")
            .arg(now)
            .arg("LIMIT")
            .arg(0)
            .arg(LEASE_REAPER_BATCH)
            .query_async(&mut *connection)
            .await?;

        if expired.is_empty() {
            return Ok(0);
        }

        let mut pipe = redis::pipe();
        for payload in &expired {
            pipe.cmd("ZREM")
                .arg(self.lease_key.as_str())
                .arg(payload)
                .cmd("LREM")
                .arg(self.processing_key.as_str())
                .arg(1)
                .arg(payload)
                .cmd("LPUSH")
                .arg(self.ready_key.as_str())
                .arg(payload);
        }
        pipe.query_async::<()>(&mut *connection).await?;
        Ok(expired.len())
    }

    async fn reserve_dedupe(
        &self,
        message: &QueueMessage,
    ) -> Result<DedupeReservation, BridgeError> {
        let Some(store) = self.dedupe_store.as_ref() else {
            return Ok(DedupeReservation::Skipped);
        };

        let Some(ref key) = message.envelope.dedupe_key else {
            return Ok(DedupeReservation::Skipped);
        };

        let ttl = message
            .envelope
            .dedupe_ttl_ms
            .map(Duration::from_millis)
            .unwrap_or(self.minimum_dedupe_ttl)
            .max(self.minimum_dedupe_ttl);

        match store.put_if_absent(key.as_bytes(), ttl).await {
            Ok(true) => Ok(DedupeReservation::Reserved(DedupeGuard::new(
                store.clone(),
                key.clone().into_bytes(),
            ))),
            Ok(false) => Ok(DedupeReservation::Duplicate),
            Err(err) => Err(BridgeError::from(err)),
        }
    }
}

#[derive(Clone)]
struct QueueMetrics {
    bridge_name: String,
    queue_name: String,
}

impl QueueMetrics {
    fn new(bridge_name: String, queue_name: String) -> Self {
        Self {
            bridge_name,
            queue_name,
        }
    }

    fn record_enqueue(&self) {
        self.emit_counter("latticeflow.queue.enqueued_total", 1);
    }

    fn record_dequeue(&self) {
        self.emit_counter("latticeflow.queue.dequeued_total", 1);
    }

    fn record_success(&self) {
        self.emit_counter("latticeflow.queue.success_total", 1);
    }

    fn record_failure(&self) {
        self.emit_counter("latticeflow.queue.failure_total", 1);
    }

    fn record_requeue(&self) {
        self.emit_counter("latticeflow.queue.requeued_total", 1);
    }

    fn record_duplicate(&self) {
        self.emit_counter("latticeflow.queue.duplicates_total", 1);
    }

    fn record_lease_extended(&self) {
        self.emit_counter("latticeflow.queue.lease_extensions_total", 1);
    }

    fn record_lease_expired(&self, count: u64) {
        self.emit_counter("latticeflow.queue.lease_expired_total", count);
    }

    fn increment_inflight(&self) {
        self.emit_gauge(
            "latticeflow.queue.processing_inflight",
            GaugeValue::Increment(1.0),
        );
    }

    fn decrement_inflight(&self) {
        self.emit_gauge(
            "latticeflow.queue.processing_inflight",
            GaugeValue::Decrement(1.0),
        );
    }

    fn emit_counter(&self, name: &'static str, value: u64) {
        let labels = self.labels();
        let counter = counter!(name, &labels);
        counter.increment(value);
    }

    fn emit_gauge(&self, name: &'static str, value: GaugeValue) {
        let labels = self.labels();
        let gauge = gauge!(name, &labels);
        match value {
            GaugeValue::Increment(delta) => gauge.increment(delta),
            GaugeValue::Decrement(delta) => gauge.decrement(delta),
            GaugeValue::Absolute(value) => gauge.set(value),
        }
    }

    fn labels(&self) -> [(&'static str, String); 2] {
        [
            ("bridge", self.bridge_name.clone()),
            ("queue", self.queue_name.clone()),
        ]
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct QueueMessage {
    id: String,
    envelope: QueueEnvelope,
}

impl QueueMessage {
    fn from_invocation(invocation: Invocation) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            envelope: QueueEnvelope::from_invocation(invocation),
        }
    }
}

#[derive(Debug)]
struct PendingMessage {
    payload: String,
    message: QueueMessage,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct QueueEnvelope {
    trigger_alias: String,
    capture_alias: String,
    payload: JsonValue,
    deadline_ms: Option<u64>,
    labels: BTreeMap<String, String>,
    extensions: BTreeMap<String, JsonValue>,
    dedupe_key: Option<String>,
    dedupe_ttl_ms: Option<u64>,
}

impl QueueEnvelope {
    fn from_invocation(invocation: Invocation) -> Self {
        let InvocationParts {
            trigger_alias,
            capture_alias,
            payload,
            deadline,
            metadata,
        } = invocation.into_parts();

        let dedupe = extract_dedupe_metadata(&metadata);

        Self {
            trigger_alias,
            capture_alias,
            payload,
            deadline_ms: deadline.map(|d| d.as_millis() as u64),
            labels: metadata.labels().clone(),
            extensions: metadata.extensions().clone(),
            dedupe_key: dedupe.key,
            dedupe_ttl_ms: dedupe.ttl_ms,
        }
    }

    fn into_invocation(self) -> Invocation {
        let deadline = self.deadline_ms.map(Duration::from_millis);
        let mut invocation = Invocation::new(self.trigger_alias, self.capture_alias, self.payload)
            .with_deadline(deadline);
        let metadata = invocation.metadata_mut();
        for (key, value) in self.labels {
            metadata.insert_label(key, value);
        }
        for (key, value) in self.extensions {
            metadata.insert_extension(key, value);
        }
        invocation
    }
}

struct DedupeMetadata {
    key: Option<String>,
    ttl_ms: Option<u64>,
}

fn extract_dedupe_metadata(metadata: &InvocationMetadata) -> DedupeMetadata {
    let key_label = metadata.labels().get(DEDUPE_LABEL_KEY).cloned();
    let ttl_label = metadata
        .labels()
        .get(DEDUPE_LABEL_TTL)
        .and_then(|value| value.parse::<u64>().ok());

    let ext = metadata.extensions().get(DEDUPE_EXTENSION);
    let key_ext = ext
        .and_then(|value| value.get("key"))
        .and_then(|value| value.as_str())
        .map(|s| s.to_string());
    let ttl_ext = ext
        .and_then(|value| value.get("ttl_ms"))
        .and_then(|value| value.as_u64());

    DedupeMetadata {
        key: key_label.or(key_ext),
        ttl_ms: ttl_label.or(ttl_ext),
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn invocation_trigger(invocation: Invocation) -> String {
    invocation.into_parts().trigger_alias
}

enum DedupeReservation {
    Skipped,
    Duplicate,
    Reserved(DedupeGuard),
}

impl DedupeReservation {
    fn take(&mut self) -> DedupeReservation {
        std::mem::replace(self, DedupeReservation::Skipped)
    }
}

struct DedupeGuard {
    store: Arc<dyn DedupeStore + Send + Sync>,
    key: Vec<u8>,
}

impl DedupeGuard {
    fn new(store: Arc<dyn DedupeStore + Send + Sync>, key: Vec<u8>) -> Self {
        Self { store, key }
    }

    async fn release(self) -> Result<(), DedupeError> {
        self.store.forget(&self.key).await
    }

    fn commit(self) {
        // Retaining the key ensures duplicates remain suppressed until TTL expiry.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use anyhow::Result;
    use dag_core::{
        Determinism, Effects, FlowBuilder, NodeError, NodeKind, NodeSpec, Profile, SchemaSpec,
    };
    use kernel_exec::{FlowExecutor, NodeRegistry};
    use kernel_plan::validate;
    use semver::Version;
    use serde_json::json;
    use testcontainers::{
        GenericImage,
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
    };
    use tokio::time::{sleep, timeout};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    #[test]
    fn envelope_round_trips_invocation() {
        let mut invocation = Invocation::new("trigger", "capture", JsonValue::Bool(true))
            .with_deadline(Some(Duration::from_secs(5)));
        let metadata = invocation.metadata_mut();
        metadata.insert_label("trace_id", "abc123");
        metadata.insert_extension("custom", serde_json::json!({ "foo": 1 }));
        metadata.insert_label(DEDUPE_LABEL_KEY, "dedupe-key");
        metadata.insert_label(DEDUPE_LABEL_TTL, "1000");

        let message = QueueMessage::from_invocation(invocation.clone());
        let reconstructed = message.envelope.clone().into_invocation();

        let parts = reconstructed.into_parts();
        assert_eq!(parts.trigger_alias, "trigger");
        assert_eq!(parts.capture_alias, "capture");
        assert_eq!(parts.payload, JsonValue::Bool(true));
        assert_eq!(parts.deadline, Some(Duration::from_secs(5)));
        assert_eq!(
            parts.metadata.labels().get("trace_id"),
            Some(&"abc123".to_string())
        );
        assert_eq!(message.envelope.dedupe_key.as_deref(), Some("dedupe-key"));
        assert_eq!(message.envelope.dedupe_ttl_ms, Some(1000));

        let original_parts = invocation.into_parts();
        assert_eq!(original_parts.trigger_alias, "trigger");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_requeues_on_execution_error() -> Result<()> {
        let container = GenericImage::new("redis", "7.2")
            .with_exposed_port(6379_u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_wait_for(WaitFor::seconds(1))
            .start()
            .await
            .expect("start redis container");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("retrieve redis port");

        sleep(Duration::from_millis(200)).await;

        let queue_key = format!("queue:{}", Uuid::new_v4());
        let namespace = format!("lf:test:{}:", Uuid::new_v4());

        let mut config = QueueBridgeConfig::development(queue_key);
        config.redis.url = format!("redis://127.0.0.1:{port}/");
        config.redis.namespace = Some(namespace);
        config.blocking_timeout = Duration::from_millis(1);
        config.idle_backoff = Duration::from_millis(10);
        config.visibility_timeout = Duration::from_millis(150);
        config.lease_extension = Duration::from_millis(50);
        config.minimum_dedupe_ttl = Duration::from_millis(200);

        let factory = RedisConnectionFactory::new(config.redis.clone())?;
        flush_db(&factory).await?;

        let attempts = Arc::new(AtomicUsize::new(0));
        let runtime = build_runtime(Some(attempts.clone()));
        let bridge = QueueBridge::new(runtime, config)?;

        let shutdown = CancellationToken::new();
        let handles = bridge.spawn_workers(1, shutdown.clone());

        let invocation = Invocation::new("trigger", "capture", json!({ "value": "payload" }));
        bridge.enqueue(invocation).await?;

        timeout(Duration::from_secs(5), async {
            loop {
                if attempts.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("worker retried after failure");

        sleep(Duration::from_millis(100)).await;

        {
            let mut conn = bridge.factory.connection().await?;
            let ready: i64 = redis::cmd("LLEN")
                .arg(bridge.ready_key.as_str())
                .query_async(&mut *conn)
                .await?;
            let processing: i64 = redis::cmd("LLEN")
                .arg(bridge.processing_key.as_str())
                .query_async(&mut *conn)
                .await?;
            assert_eq!(ready, 0, "ready queue empty after successful ack");
            assert_eq!(processing, 0, "processing queue empty after ack");
        }

        shutdown.cancel();
        for handle in handles {
            handle.await.expect("worker task shutdown cleanly");
        }

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_leases_are_reclaimed() -> Result<()> {
        let container = GenericImage::new("redis", "7.2")
            .with_exposed_port(6379_u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_wait_for(WaitFor::seconds(1))
            .start()
            .await
            .expect("start redis container");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("retrieve redis port");

        sleep(Duration::from_millis(200)).await;

        let queue_key = format!("queue:{}", Uuid::new_v4());
        let namespace = format!("lf:test:{}:", Uuid::new_v4());

        let mut config = QueueBridgeConfig::development(queue_key);
        config.redis.url = format!("redis://127.0.0.1:{port}/");
        config.redis.namespace = Some(namespace);
        config.visibility_timeout = Duration::from_millis(100);
        config.lease_extension = Duration::from_millis(40);

        let factory = RedisConnectionFactory::new(config.redis.clone())?;
        flush_db(&factory).await?;

        let runtime = build_runtime(None);
        let bridge = QueueBridge::new(runtime, config)?;

        let message = QueueMessage::from_invocation(Invocation::new(
            "trigger",
            "capture",
            json!({ "value": "lease-test" }),
        ));
        let payload = serde_json::to_string(&message)?;

        {
            let mut conn = bridge.factory.connection().await?;
            redis::pipe()
                .cmd("LPUSH")
                .arg(bridge.processing_key.as_str())
                .arg(&payload)
                .cmd("ZADD")
                .arg(bridge.lease_key.as_str())
                .arg((now_millis() - 10_000) as f64)
                .arg(&payload)
                .query_async::<()>(&mut *conn)
                .await?;
        }

        let reclaimed = bridge.reclaim_expired().await?;
        assert_eq!(reclaimed, 1);

        {
            let mut conn = bridge.factory.connection().await?;
            let ready: Vec<String> = redis::cmd("LRANGE")
                .arg(bridge.ready_key.as_str())
                .arg(0)
                .arg(-1)
                .query_async(&mut *conn)
                .await?;
            assert_eq!(ready, vec![payload]);

            let processing_len: i64 = redis::cmd("LLEN")
                .arg(bridge.processing_key.as_str())
                .query_async(&mut *conn)
                .await?;
            assert_eq!(processing_len, 0);
        }

        Ok(())
    }

    async fn flush_db(factory: &RedisConnectionFactory) -> Result<()> {
        let mut conn = factory.connection().await?;
        redis::cmd("FLUSHALL").query_async::<()>(&mut *conn).await?;
        Ok(())
    }

    fn build_runtime(counter: Option<Arc<AtomicUsize>>) -> HostRuntime {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "test::trigger",
                |payload: JsonValue| async move { Ok(payload) },
            )
            .expect("register trigger");

        let counter_clone = counter.clone();
        registry
            .register_fn("test::worker", move |payload: JsonValue| {
                let counter_clone = counter_clone.clone();
                async move {
                    if let Some(counter) = counter_clone.as_ref() {
                        let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                        if attempt == 1 {
                            return Err(NodeError::new("intentional failure"));
                        }
                    }
                    Ok(payload)
                }
            })
            .expect("register worker");

        registry
            .register_fn(
                "test::capture",
                |payload: JsonValue| async move { Ok(payload) },
            )
            .expect("register capture");

        let executor = FlowExecutor::new(Arc::new(registry));

        let mut builder = FlowBuilder::new(
            "queue_bridge_test",
            Version::parse("1.0.0").expect("parse version"),
            Profile::Queue,
        );
        builder.summary(Some("queue bridge integration test flow"));

        let trigger_spec = NodeSpec {
            identifier: "test::trigger",
            name: "Trigger",
            kind: NodeKind::Trigger,
            summary: Some("integration trigger"),
            in_schema: SchemaSpec::Opaque,
            out_schema: SchemaSpec::Opaque,
            effects: Effects::ReadOnly,
            determinism: Determinism::Strict,
            determinism_hints: &[],
            effect_hints: &[],
        };

        let worker_spec = NodeSpec::inline(
            "test::worker",
            "Worker",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Pure,
            Determinism::Strict,
            Some("worker node"),
        );

        let capture_spec = NodeSpec::inline(
            "test::capture",
            "Capture",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Pure,
            Determinism::Strict,
            Some("capture node"),
        );

        let trigger = builder
            .add_node("trigger", &trigger_spec)
            .expect("add trigger");
        let worker = builder
            .add_node("worker", &worker_spec)
            .expect("add worker");
        let capture = builder
            .add_node("capture", &capture_spec)
            .expect("add capture");
        builder.connect(&trigger, &worker);
        builder.connect(&worker, &capture);

        let ir = validate(&builder.build()).expect("flow validates");
        HostRuntime::new(executor, Arc::new(ir))
    }
}
