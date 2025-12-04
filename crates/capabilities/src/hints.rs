//! Static mapping between capability aliases and the resource hints they imply.
//!
//! This module is consumed at compile time by `dag-macros` so that node
//! declarations automatically inherit the correct effect/determinism hints
//! without bespoke wiring in every crate.

use crate::{blob, clock, db, dedupe, http, kv, queue, rng};

/// Canonical set of hints exported for a capability.
#[derive(Debug, Clone, Copy)]
pub struct ResourceHintSet {
    pub effect_hints: &'static [&'static str],
    pub determinism_hints: &'static [&'static str],
}

impl ResourceHintSet {
    pub const fn new(
        effect_hints: &'static [&'static str],
        determinism_hints: &'static [&'static str],
    ) -> Self {
        Self {
            effect_hints,
            determinism_hints,
        }
    }

    pub const EMPTY: ResourceHintSet = ResourceHintSet::new(&[], &[]);

    pub fn is_empty(self) -> bool {
        self.effect_hints.is_empty() && self.determinism_hints.is_empty()
    }
}

const HTTP_READ_EFFECT: [&str; 1] = [http::HINT_HTTP_READ];
const HTTP_WRITE_EFFECT: [&str; 1] = [http::HINT_HTTP_WRITE];
const HTTP_DETERMINISM: [&str; 1] = [http::HINT_HTTP];

const CLOCK_DETERMINISM: [&str; 1] = [clock::HINT_CLOCK];
const RNG_DETERMINISM: [&str; 1] = [rng::HINT_RNG];

const DB_READ_EFFECT: [&str; 1] = [db::HINT_DB_READ];
const DB_WRITE_EFFECT: [&str; 1] = [db::HINT_DB_WRITE];
const DB_DETERMINISM: [&str; 1] = [db::HINT_DB];

const KV_READ_EFFECT: [&str; 1] = [kv::HINT_KV_READ];
const KV_WRITE_EFFECT: [&str; 1] = [kv::HINT_KV_WRITE];
const KV_DETERMINISM: [&str; 1] = [kv::HINT_KV];

const QUEUE_PUBLISH_EFFECT: [&str; 1] = [queue::HINT_QUEUE_PUBLISH];
const QUEUE_CONSUME_EFFECT: [&str; 1] = [queue::HINT_QUEUE_CONSUME];
const QUEUE_DETERMINISM: [&str; 1] = [queue::HINT_QUEUE];

const BLOB_READ_EFFECT: [&str; 1] = [blob::HINT_BLOB_READ];
const BLOB_WRITE_EFFECT: [&str; 1] = [blob::HINT_BLOB_WRITE];
const BLOB_DETERMINISM: [&str; 1] = [blob::HINT_BLOB];

const DEDUPE_EFFECT: [&str; 1] = [dedupe::HINT_DEDUPE_WRITE];
const DEDUPE_DETERMINISM: [&str; 1] = [dedupe::HINT_DEDUPE];

/// Infer canonical hints for a capability declaration based on its alias and identifier.
pub fn infer(alias: &str, capability_ident: &str) -> ResourceHintSet {
    let alias_lower = alias.to_ascii_lowercase();
    let ident_lower = capability_ident.to_ascii_lowercase();

    if alias_lower.is_empty() && ident_lower.is_empty() {
        return ResourceHintSet::EMPTY;
    }

    if alias_lower.contains("clock") || ident_lower.contains("clock") {
        clock::ensure_registered();
        return ResourceHintSet::new(&[], &CLOCK_DETERMINISM);
    }

    if alias_lower.contains("rng")
        || alias_lower.contains("random")
        || ident_lower.contains("rng")
        || ident_lower.contains("random")
    {
        rng::ensure_registered();
        return ResourceHintSet::new(&[], &RNG_DETERMINISM);
    }

    if alias_lower.contains("http") || ident_lower.contains("http") {
        http::ensure_registered();
        let write_tokens = [
            "write", "post", "put", "patch", "delete", "send", "emit", "publish", "producer",
            "upsert", "insert", "update",
        ];
        if write_tokens
            .iter()
            .any(|token| alias_lower.contains(token) || ident_lower.contains(token))
        {
            return ResourceHintSet::new(&HTTP_WRITE_EFFECT, &HTTP_DETERMINISM);
        }

        let read_tokens = [
            "read", "fetch", "get", "load", "receive", "consumer", "listen",
        ];
        if read_tokens
            .iter()
            .any(|token| alias_lower.contains(token) || ident_lower.contains(token))
        {
            return ResourceHintSet::new(&HTTP_READ_EFFECT, &HTTP_DETERMINISM);
        }

        return ResourceHintSet::new(&[], &HTTP_DETERMINISM);
    }

    if alias_lower.contains("db")
        || alias_lower.contains("sql")
        || alias_lower.contains("postgres")
        || alias_lower.contains("pg")
        || ident_lower.contains("db")
        || ident_lower.contains("sql")
        || ident_lower.contains("postgres")
        || ident_lower.contains("pg")
    {
        db::ensure_registered();
        let read_tokens = ["read", "select", "fetch", "query"];
        if read_tokens
            .iter()
            .any(|token| alias_lower.contains(token) || ident_lower.contains(token))
        {
            return ResourceHintSet::new(&DB_READ_EFFECT, &DB_DETERMINISM);
        }
        return ResourceHintSet::new(&DB_WRITE_EFFECT, &DB_DETERMINISM);
    }

    if alias_lower.contains("kv")
        || alias_lower.contains("cache")
        || ident_lower.contains("kv")
        || ident_lower.contains("workers_kv")
    {
        kv::ensure_registered();
        let read_tokens = ["read", "get", "fetch", "lookup"];
        if read_tokens
            .iter()
            .any(|token| alias_lower.contains(token) || ident_lower.contains(token))
        {
            return ResourceHintSet::new(&KV_READ_EFFECT, &KV_DETERMINISM);
        }
        return ResourceHintSet::new(&KV_WRITE_EFFECT, &KV_DETERMINISM);
    }

    if alias_lower.contains("queue") || ident_lower.contains("queue") {
        queue::ensure_registered();
        let consume_tokens = ["consume", "dequeue", "receive", "poll"];
        if consume_tokens
            .iter()
            .any(|token| alias_lower.contains(token) || ident_lower.contains(token))
        {
            return ResourceHintSet::new(&QUEUE_CONSUME_EFFECT, &QUEUE_DETERMINISM);
        }
        return ResourceHintSet::new(&QUEUE_PUBLISH_EFFECT, &QUEUE_DETERMINISM);
    }

    if alias_lower.contains("dedupe")
        || alias_lower.contains("idem")
        || ident_lower.contains("dedupe")
        || ident_lower.contains("idempot")
    {
        dedupe::ensure_registered();
        return ResourceHintSet::new(&DEDUPE_EFFECT, &DEDUPE_DETERMINISM);
    }

    if alias_lower.contains("blob")
        || alias_lower.contains("object")
        || ident_lower.contains("blob")
        || ident_lower.contains("object")
        || ident_lower.contains("storage")
    {
        blob::ensure_registered();
        let read_tokens = ["read", "fetch", "get", "download"];
        if read_tokens
            .iter()
            .any(|token| alias_lower.contains(token) || ident_lower.contains(token))
        {
            return ResourceHintSet::new(&BLOB_READ_EFFECT, &BLOB_DETERMINISM);
        }
        return ResourceHintSet::new(&BLOB_WRITE_EFFECT, &BLOB_DETERMINISM);
    }

    ResourceHintSet::EMPTY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_write_derives_effect_and_determinism() {
        let hints = infer("http_sender", "HttpWrite");
        assert_eq!(hints.effect_hints, &HTTP_WRITE_EFFECT);
        assert_eq!(hints.determinism_hints, &HTTP_DETERMINISM);
    }

    #[test]
    fn clock_derives_clock_hint() {
        let hints = infer("clock", "SystemClock");
        assert!(hints.effect_hints.is_empty());
        assert_eq!(hints.determinism_hints, &CLOCK_DETERMINISM);
    }

    #[test]
    fn unknown_alias_defaults_to_empty() {
        let hints = infer("custom", "CustomCapability");
        assert!(hints.effect_hints.is_empty());
        assert!(hints.determinism_hints.is_empty());
    }

    #[test]
    fn db_read_and_write_infer_correctly() {
        let read = infer("db_read_pool", "PostgresRead");
        assert_eq!(read.effect_hints, &DB_READ_EFFECT);
        assert_eq!(read.determinism_hints, &DB_DETERMINISM);

        let write = infer("postgres_writer", "PgWrite");
        assert_eq!(write.effect_hints, &DB_WRITE_EFFECT);
        assert_eq!(write.determinism_hints, &DB_DETERMINISM);
    }

    #[test]
    fn kv_read_and_write_infer_correctly() {
        let read = infer("kv_get", "WorkersKv");
        assert_eq!(read.effect_hints, &KV_READ_EFFECT);
        assert_eq!(read.determinism_hints, &KV_DETERMINISM);

        let write = infer("kv_put", "WorkersKv");
        assert_eq!(write.effect_hints, &KV_WRITE_EFFECT);
        assert_eq!(write.determinism_hints, &KV_DETERMINISM);
    }

    #[test]
    fn queue_publish_and_consume_infer_correctly() {
        let publish = infer("queue_publish", "QueueProducer");
        assert_eq!(publish.effect_hints, &QUEUE_PUBLISH_EFFECT);
        assert_eq!(publish.determinism_hints, &QUEUE_DETERMINISM);

        let consume = infer("QUEUE_CONSUME", "QueueConsumer");
        assert_eq!(consume.effect_hints, &QUEUE_CONSUME_EFFECT);
        assert_eq!(consume.determinism_hints, &QUEUE_DETERMINISM);
    }

    #[test]
    fn blob_read_and_write_infer_correctly() {
        let read = infer("blob_fetch", "BlobReader");
        assert_eq!(read.effect_hints, &BLOB_READ_EFFECT);
        assert_eq!(read.determinism_hints, &BLOB_DETERMINISM);

        let write = infer("blob_write", "BlobWriter");
        assert_eq!(write.effect_hints, &BLOB_WRITE_EFFECT);
        assert_eq!(write.determinism_hints, &BLOB_DETERMINISM);
    }

    #[test]
    fn dedupe_infers_effectful_hint() {
        let hints = infer("dedupe_store", "RedisDedupe");
        assert_eq!(hints.effect_hints, &DEDUPE_EFFECT);
        assert_eq!(hints.determinism_hints, &DEDUPE_DETERMINISM);
    }
}
