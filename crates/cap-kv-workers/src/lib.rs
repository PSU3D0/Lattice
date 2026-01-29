//! Workers KV-backed key-value capability for Lattice.
//!
//! This crate wraps Cloudflare Workers KV (`worker::kv::KvStore`) and implements
//! the `KeyValue` trait from the `capabilities` crate.
//!
//! # Workers KV Characteristics
//!
//! Cloudflare Workers KV is a globally replicated, eventually consistent key-value store:
//!
//! - **Consistency**: Eventually consistent. Writes propagate globally within ~60 seconds,
//!   but reads at the same edge location are immediately consistent.
//! - **Replication**: Automatic global replication to all Cloudflare edge locations.
//! - **Caching**: Edge caching with configurable `cache_ttl` for read operations.
//! - **TTL/Expiration**: Supports per-write TTL (minimum 60 seconds) and absolute
//!   expiration timestamps.
//! - **Metadata**: Up to 1KB of JSON metadata can be stored per key.
//!
//! # Limits
//!
//! See the `MAX_*` and `MIN_*` constants for Workers KV limits.

use capabilities::Capability;
use capabilities::kv::{
    self, KeyValue, KvCapabilityInfo, KvConsistency, KvError, KvGetOptions, KvPutOptions,
    KvTtlSupport,
};

// ─────────────────────────────────────────────────────────────────────────────
// Workers KV Limits
// ─────────────────────────────────────────────────────────────────────────────
// See: https://developers.cloudflare.com/kv/platform/limits/

/// Maximum key size in bytes (512 bytes).
///
/// Keys are UTF-8 encoded strings. This limit applies to the byte length,
/// not the character count.
pub const MAX_KEY_SIZE: usize = 512;

/// Maximum value size in bytes (25 MiB).
///
/// Values can be any binary data up to 25 megabytes.
pub const MAX_VALUE_SIZE: usize = 25 * 1024 * 1024;

/// Maximum metadata size in bytes (1 KiB).
///
/// Metadata is stored as JSON and must serialize to at most 1024 bytes.
pub const MAX_METADATA_SIZE: usize = 1024;

/// Minimum TTL in seconds (60 seconds).
///
/// Workers KV enforces a minimum TTL of 60 seconds for expiring keys.
/// Attempting to set a shorter TTL will result in an error.
pub const MIN_TTL_SECONDS: u64 = 60;

/// Maximum keys returned per list operation (1000).
///
/// The `list` operation returns at most 1000 keys per call.
/// Use cursor-based pagination to retrieve more.
pub const MAX_LIST_KEYS: usize = 1000;

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;

    use async_trait::async_trait;
    use capabilities::kv::{KvListEntry, KvListOptions, KvListResponse, KvValue};
    use js_sys::{Function, Object, Promise};
    use serde::Serialize;
    use worker::wasm_bindgen_futures::JsFuture;
    use std::time::{Duration, UNIX_EPOCH};
    use worker::kv::KvStore;

    #[repr(C)]
    struct KvStoreParts {
        this: Object,
        get_function: Function,
        get_with_meta_function: Function,
        put_function: Function,
        list_function: Function,
        delete_function: Function,
    }

    impl KvStoreParts {
        fn from_store(store: &KvStore) -> &Self {
            assert_kvstore_layout();
            // SAFETY: worker::kv::KvStore is a repr(Rust) struct with stable field order in
            // workers-rs 0.7.4. We only read shared references to call list with custom options.
            unsafe { &*(store as *const KvStore as *const KvStoreParts) }
        }
    }

    fn assert_kvstore_layout() {
        let store_size = std::mem::size_of::<KvStore>();
        let parts_size = std::mem::size_of::<KvStoreParts>();
        let store_align = std::mem::align_of::<KvStore>();
        let parts_align = std::mem::align_of::<KvStoreParts>();

        if store_size != parts_size || store_align != parts_align {
            panic!(
                "worker::kv::KvStore layout mismatch (size {}, align {}) vs KvStoreParts (size {}, align {}); update cap-kv-workers for worker =0.7.4",
                store_size,
                store_align,
                parts_size,
                parts_align
            );
        }
    }

    #[derive(Serialize)]
    struct ListOptionsWithInclude {
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<Vec<&'static str>>,
    }

    /// Workers KV-backed key-value capability.
    ///
    /// This struct wraps a `worker::kv::KvStore` obtained from the Workers runtime.
    /// Create it by passing a `KvStore` from `env.kv("BINDING_NAME")`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use cap_kv_workers::WorkersKv;
    /// use capabilities::kv::KeyValue;
    ///
    /// // In your worker handler:
    /// let kv_store = env.kv("MY_KV_BINDING")?;
    /// let kv = WorkersKv::new(kv_store);
    ///
    /// // Use the KeyValue trait methods
    /// let value = kv.get("my-key").await?;
    /// kv.put("another-key", b"hello", None).await?;
    /// ```
    pub struct WorkersKv {
        store: KvStore,
    }

    impl WorkersKv {
        /// Construct a new Workers KV capability from a `KvStore`.
        ///
        /// The `KvStore` is obtained from `env.kv("BINDING_NAME")` in your worker.
        pub fn new(store: KvStore) -> Self {
            kv::ensure_registered();
            Self { store }
        }

        /// Validate key size against Workers KV limits.
        pub fn validate_key(key: &str) -> Result<(), KvError> {
            if key.is_empty() || key == "." || key == ".." {
                return Err(KvError::InvalidOptions(
                    "key must not be empty or dot path segments".to_string(),
                ));
            }
            if key.as_bytes().len() > MAX_KEY_SIZE {
                return Err(KvError::InvalidOptions(format!(
                    "key exceeds maximum size of {} bytes (got {} bytes)",
                    MAX_KEY_SIZE,
                    key.as_bytes().len()
                )));
            }
            Ok(())
        }

        /// Validate value size against Workers KV limits.
        pub fn validate_value(value: &[u8]) -> Result<(), KvError> {
            if value.len() > MAX_VALUE_SIZE {
                return Err(KvError::InvalidOptions(format!(
                    "value exceeds maximum size of {} bytes (got {} bytes)",
                    MAX_VALUE_SIZE,
                    value.len()
                )));
            }
            Ok(())
        }

        /// Validate TTL against Workers KV minimum (60 seconds).
        pub fn validate_ttl(ttl: Duration) -> Result<(), KvError> {
            if ttl.as_secs() < MIN_TTL_SECONDS {
                return Err(KvError::InvalidOptions(format!(
                    "TTL must be at least {} seconds (got {} seconds)",
                    MIN_TTL_SECONDS,
                    ttl.as_secs()
                )));
            }
            Ok(())
        }

        /// Validate cache_ttl against Workers KV minimum (60 seconds).
        pub fn validate_cache_ttl(cache_ttl: Duration) -> Result<(), KvError> {
            if cache_ttl.as_secs() < MIN_TTL_SECONDS {
                return Err(KvError::InvalidOptions(format!(
                    "cache_ttl must be at least {} seconds (got {} seconds)",
                    MIN_TTL_SECONDS,
                    cache_ttl.as_secs()
                )));
            }
            Ok(())
        }

        /// Validate metadata size against Workers KV limits.
        pub fn validate_metadata(metadata: &serde_json::Value) -> Result<(), KvError> {
            let serialized = serde_json::to_vec(metadata).map_err(|e| {
                KvError::InvalidOptions(format!("failed to serialize metadata: {}", e))
            })?;
            if serialized.len() > MAX_METADATA_SIZE {
                return Err(KvError::InvalidOptions(format!(
                    "metadata exceeds maximum size of {} bytes (got {} bytes)",
                    MAX_METADATA_SIZE,
                    serialized.len()
                )));
            }
            Ok(())
        }
    }

    impl Capability for WorkersKv {
        fn name(&self) -> &'static str {
            "kv.workers"
        }
    }

    #[async_trait(?Send)]
    impl KeyValue for WorkersKv {
        async fn get_with_options(
            &self,
            key: &str,
            options: KvGetOptions,
        ) -> Result<Option<Vec<u8>>, KvError> {
            tracing::debug!(key = %key, "cap_kv_workers.get");
            Self::validate_key(key)?;

            let mut builder = self.store.get(key);

            // Apply cache_ttl if provided (controls edge cache duration)
            if let Some(cache_ttl) = options.cache_ttl {
                Self::validate_cache_ttl(cache_ttl)?;
                builder = builder.cache_ttl(cache_ttl.as_secs() as u64);
            }

            builder.bytes().await.map_err(map_kv_error)
        }

        async fn get_with_metadata(
            &self,
            key: &str,
            options: KvGetOptions,
        ) -> Result<Option<KvValue>, KvError> {
            tracing::debug!(key = %key, "cap_kv_workers.get_with_metadata");
            Self::validate_key(key)?;

            let mut builder = self.store.get(key);

            // Apply cache_ttl if provided
            if let Some(cache_ttl) = options.cache_ttl {
                Self::validate_cache_ttl(cache_ttl)?;
                builder = builder.cache_ttl(cache_ttl.as_secs() as u64);
            }

            // bytes_with_metadata returns (Option<Vec<u8>>, Option<T>)
            let (value_opt, metadata) = builder
                .bytes_with_metadata::<serde_json::Value>()
                .await
                .map_err(map_kv_error)?;

            match value_opt {
                Some(value) => Ok(Some(KvValue { value, metadata })),
                None => Ok(None),
            }
        }

        async fn put_with_options(
            &self,
            key: &str,
            value: &[u8],
            options: KvPutOptions,
        ) -> Result<(), KvError> {
            tracing::debug!(key = %key, value_len = value.len(), "cap_kv_workers.put");
            Self::validate_key(key)?;
            Self::validate_value(value)?;

            // Validate mutual exclusivity of TTL and expires_at
            if options.ttl.is_some() && options.expires_at.is_some() {
                return Err(KvError::InvalidOptions(
                    "ttl and expires_at cannot both be set".to_string(),
                ));
            }

            // Validate TTL if provided
            if let Some(ttl) = options.ttl {
                Self::validate_ttl(ttl)?;
            }

            // Validate metadata if provided
            if let Some(ref metadata) = options.metadata {
                Self::validate_metadata(metadata)?;
            }

            let mut builder = self.store.put_bytes(key, value).map_err(map_kv_error)?;

            // Apply TTL if provided (expiration_ttl in workers-rs)
            if let Some(ttl) = options.ttl {
                builder = builder.expiration_ttl(ttl.as_secs());
            }

            // Apply absolute expiration if provided
            if let Some(expires_at) = options.expires_at {
                let unix_ts = expires_at
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| {
                        KvError::InvalidOptions(format!("invalid expiration timestamp: {}", e))
                    })?
                    .as_secs();
                builder = builder.expiration(unix_ts);
            }

            // Apply metadata if provided
            if let Some(metadata) = options.metadata {
                builder = builder.metadata(metadata).map_err(map_kv_error)?;
            }

            builder.execute().await.map_err(map_kv_error)
        }

        async fn delete(&self, key: &str) -> Result<(), KvError> {
            tracing::debug!(key = %key, "cap_kv_workers.delete");
            Self::validate_key(key)?;
            let existing = self.store.get(key).bytes().await.map_err(map_kv_error)?;
            if existing.is_none() {
                return Err(KvError::NotFound);
            }
            self.store.delete(key).await.map_err(map_kv_error)
        }

        async fn list(&self, options: KvListOptions) -> Result<KvListResponse, KvError> {
            tracing::debug!("cap_kv_workers.list");
            let include_metadata = options.include_metadata;
            let include_expiration = options.include_expiration;

            if !include_metadata && !include_expiration {
                let mut builder = self.store.list();
                if let Some(prefix) = options.prefix {
                    builder = builder.prefix(prefix);
                }
                if let Some(cursor) = options.cursor {
                    builder = builder.cursor(cursor);
                }
                if let Some(limit) = options.limit {
                    builder = builder.limit(limit.min(MAX_LIST_KEYS) as u64);
                }

                let result = builder.execute().await.map_err(map_kv_error)?;
                let keys = result
                    .keys
                    .into_iter()
                    .map(|key| KvListEntry {
                        key: key.name,
                        expires_at: None,
                        metadata: None,
                    })
                    .collect();

                return Ok(KvListResponse {
                    keys,
                    list_complete: result.list_complete,
                    cursor: result.cursor,
                });
            }

            let mut include = Vec::new();
            if include_metadata {
                include.push("metadata");
            }
            if include_expiration {
                include.push("expiration");
            }

            // workers-rs does not expose list include options; call the underlying
            // list function directly when metadata/expiration are requested.

            let list_options = ListOptionsWithInclude {
                prefix: options.prefix.clone(),
                cursor: options.cursor.clone(),
                limit: options.limit.map(|limit| limit.min(MAX_LIST_KEYS) as u64),
                include: if include.is_empty() { None } else { Some(include) },
            };

            let options_object = serde_wasm_bindgen::to_value(&list_options)
                .map_err(|e| KvError::Other(format!("failed to serialize list options: {}", e)))?;

            let parts = KvStoreParts::from_store(&self.store);
            let promise: Promise = parts
                .list_function
                .call1(&parts.this, &options_object)
                .map_err(|e| map_kv_error(worker::kv::KvError::from(e)))?
                .into();

            let value = JsFuture::from(promise)
                .await
                .map_err(|e| map_kv_error(worker::kv::KvError::from(e)))?;
            let result: worker::kv::ListResponse = serde_wasm_bindgen::from_value(value)
                .map_err(|e| KvError::Other(format!("failed to decode list response: {}", e)))?;

            let keys = result
                .keys
                .into_iter()
                .map(|key| {
                    // Convert expiration timestamp (unix seconds) to SystemTime if present
                    let expires_at = if include_expiration {
                        key.expiration
                            .map(|ts| UNIX_EPOCH + Duration::from_secs(ts))
                    } else {
                        None
                    };

                    KvListEntry {
                        key: key.name,
                        expires_at,
                        metadata: if include_metadata { key.metadata } else { None },
                    }
                })
                .collect();

            Ok(KvListResponse {
                keys,
                list_complete: result.list_complete,
                cursor: result.cursor,
            })
        }

        fn capability_info(&self) -> KvCapabilityInfo {
            // Workers KV characteristics:
            // - Supports per-write TTL (expiration/expirationTtl)
            // - Eventually consistent reads globally, strong consistency at edge
            // - Supports metadata on keys
            // - Supports list with prefix/cursor
            // - Supports cache_ttl for read operations
            // - Supports absolute expiration timestamps
            KvCapabilityInfo::new(KvTtlSupport::PerWrite, KvConsistency::Eventual)
                .with_metadata(true)
                .with_list(true)
                .with_cache_ttl(true)
                .with_expiration(true)
        }
    }

    /// Map worker::kv::KvError to capabilities::kv::KvError.
    fn map_kv_error(err: worker::kv::KvError) -> KvError {
        KvError::Other(err.to_string())
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Validation tests (run with wasm-bindgen-test)
    // ─────────────────────────────────────────────────────────────────────────────

    #[cfg(test)]
    mod tests {
        use super::*;
        use wasm_bindgen_test::wasm_bindgen_test;

        // Note: Full integration tests require a Workers runtime with actual KV bindings.
        // These tests verify the validation logic.

        #[wasm_bindgen_test]
        fn validate_key_accepts_valid_key() {
            let key = "a".repeat(MAX_KEY_SIZE);
            assert!(WorkersKv::validate_key(&key).is_ok());
        }

        #[wasm_bindgen_test]
        fn kvstore_layout_matches_parts() {
            assert_kvstore_layout();
        }

        #[wasm_bindgen_test]
        fn validate_key_rejects_empty_key() {
            let err = WorkersKv::validate_key("").unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
        }

        #[wasm_bindgen_test]
        fn validate_key_rejects_dot_keys() {
            let err = WorkersKv::validate_key(".").unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
            let err = WorkersKv::validate_key("..").unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
        }

        #[wasm_bindgen_test]
        fn validate_key_rejects_oversized_key() {
            let key = "a".repeat(MAX_KEY_SIZE + 1);
            let err = WorkersKv::validate_key(&key).unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
        }

        #[wasm_bindgen_test]
        fn validate_value_accepts_valid_value() {
            let value = vec![0u8; MAX_VALUE_SIZE];
            assert!(WorkersKv::validate_value(&value).is_ok());
        }

        #[wasm_bindgen_test]
        fn validate_value_rejects_oversized_value() {
            let value = vec![0u8; MAX_VALUE_SIZE + 1];
            let err = WorkersKv::validate_value(&value).unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
        }

        #[wasm_bindgen_test]
        fn validate_ttl_accepts_minimum_ttl() {
            let ttl = Duration::from_secs(MIN_TTL_SECONDS);
            assert!(WorkersKv::validate_ttl(ttl).is_ok());
        }

        #[wasm_bindgen_test]
        fn validate_ttl_accepts_longer_ttl() {
            let ttl = Duration::from_secs(3600);
            assert!(WorkersKv::validate_ttl(ttl).is_ok());
        }

        #[wasm_bindgen_test]
        fn validate_ttl_rejects_short_ttl() {
            let ttl = Duration::from_secs(MIN_TTL_SECONDS - 1);
            let err = WorkersKv::validate_ttl(ttl).unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
        }

        #[wasm_bindgen_test]
        fn validate_cache_ttl_accepts_minimum_cache_ttl() {
            let ttl = Duration::from_secs(MIN_TTL_SECONDS);
            assert!(WorkersKv::validate_cache_ttl(ttl).is_ok());
        }

        #[wasm_bindgen_test]
        fn validate_cache_ttl_rejects_short_cache_ttl() {
            let ttl = Duration::from_secs(MIN_TTL_SECONDS - 1);
            let err = WorkersKv::validate_cache_ttl(ttl).unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
        }

        #[wasm_bindgen_test]
        fn validate_metadata_accepts_small_metadata() {
            let metadata = serde_json::json!({"key": "value"});
            assert!(WorkersKv::validate_metadata(&metadata).is_ok());
        }

        #[wasm_bindgen_test]
        fn validate_metadata_rejects_oversized_metadata() {
            // Create metadata larger than 1024 bytes
            let large_value = "x".repeat(2000);
            let metadata = serde_json::json!({"key": large_value});
            let err = WorkersKv::validate_metadata(&metadata).unwrap_err();
            assert!(matches!(err, KvError::InvalidOptions(_)));
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::WorkersKv;

// Provide a stub for non-wasm32 targets so the crate compiles everywhere
#[cfg(not(target_arch = "wasm32"))]
pub struct WorkersKv {
    binding_name: String,
}

#[cfg(not(target_arch = "wasm32"))]
impl WorkersKv {
    pub fn new(binding_name: &str) -> Self {
        kv::ensure_registered();
        Self {
            binding_name: binding_name.to_owned(),
        }
    }

    pub fn binding_name(&self) -> &str {
        &self.binding_name
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Capability for WorkersKv {
    fn name(&self) -> &'static str {
        "kv.workers"
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl KeyValue for WorkersKv {
    async fn get_with_options(
        &self,
        _key: &str,
        _options: KvGetOptions,
    ) -> Result<Option<Vec<u8>>, KvError> {
        Err(KvError::Other("cap-kv-workers requires wasm32".to_string()))
    }

    async fn put_with_options(
        &self,
        _key: &str,
        _value: &[u8],
        _options: KvPutOptions,
    ) -> Result<(), KvError> {
        Err(KvError::Other("cap-kv-workers requires wasm32".to_string()))
    }

    async fn delete(&self, _key: &str) -> Result<(), KvError> {
        Err(KvError::Other("cap-kv-workers requires wasm32".to_string()))
    }

    fn capability_info(&self) -> KvCapabilityInfo {
        KvCapabilityInfo::new(KvTtlSupport::PerWrite, KvConsistency::Eventual)
            .with_metadata(true)
            .with_list(true)
            .with_cache_ttl(true)
            .with_expiration(true)
    }
}
