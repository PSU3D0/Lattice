//! Shared Redis client utilities for Lattice capability crates.
//!
//! This crate centralises configuration, connection management, and key
//! namespacing so that higher-level Redis-backed capabilities (cache, dedupe,
//! queue bridge) can compose consistently. Nothing in here is opinionated about
//! specific behaviours such as memoisation or idempotency stores; those
//! concerns live in the companion crates that depend on this shared core.

#![deny(missing_docs)]

use std::{num::NonZeroUsize, sync::Arc};

use anyhow::Context;
use redis::Client;
use redis::aio::MultiplexedConnection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Configuration for constructing Redis connections.
#[derive(Clone, Debug)]
pub struct RedisConfig {
    /// Redis connection string (e.g. `redis://localhost:6379`).
    pub url: String,
    /// Optional namespace prefix applied to every key (e.g. `lf:dev:`).
    pub namespace: Option<String>,
    /// Maximum number of concurrent connections handed out to callers.
    pub max_connections: NonZeroUsize,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1/".to_string(),
            namespace: None,
            // A single connection suits unit/integration tests; callers can
            // increase this when running real workloads.
            max_connections: NonZeroUsize::new(2).expect("non-zero"),
        }
    }
}

impl RedisConfig {
    /// Applies the configured namespace to the provided key.
    pub fn namespaced_key(&self, key: &str) -> String {
        match &self.namespace {
            Some(prefix) => format!("{prefix}{key}"),
            None => key.to_string(),
        }
    }
}

/// Factory that dispenses multiplexed Redis connections with simple pooling.
#[derive(Clone, Debug)]
pub struct RedisConnectionFactory {
    client: Client,
    permits: Arc<Semaphore>,
    config: RedisConfig,
}

impl RedisConnectionFactory {
    /// Creates a new factory from the provided configuration.
    pub fn new(config: RedisConfig) -> anyhow::Result<Self> {
        let client =
            Client::open(config.url.clone()).context("failed to create redis client from url")?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.max_connections.get())),
            client,
            config,
        })
    }

    /// Borrows a multiplexed connection from the factory.
    ///
    /// The returned [`ConnectionManager`] keeps a clone of the client so that
    /// callers can execute concurrent commands without re-arming the permit.
    ///
    /// Callers receive ownership of the multiplexed connection; dropping the
    /// wrapper releases the semaphore permit automatically.
    pub async fn connection(&self) -> anyhow::Result<RedisConnection> {
        let permit = self.permits.clone().acquire_owned().await?;
        let conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("failed to obtain multiplexed redis connection")?;
        Ok(RedisConnection {
            manager: conn,
            _permit: permit,
        })
    }

    /// Returns a copy of the configuration for external helpers.
    pub fn config(&self) -> &RedisConfig {
        &self.config
    }

    /// Applies the configured namespace to the provided key.
    pub fn namespaced_key(&self, key: &str) -> String {
        self.config.namespaced_key(key)
    }
}

/// Wrapper around `MultiplexedConnection` that keeps the permit alive.
pub struct RedisConnection {
    manager: MultiplexedConnection,
    _permit: OwnedSemaphorePermit,
}

impl std::ops::Deref for RedisConnection {
    type Target = MultiplexedConnection;

    fn deref(&self) -> &Self::Target {
        &self.manager
    }
}

impl std::ops::DerefMut for RedisConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.manager
    }
}
