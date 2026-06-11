//! Typed resource hints (packet A1).
//!
//! `EffectHint` is the single source of truth for the `resource::*` hint
//! vocabulary that flows declare in `NodeIR.effect_hints` /
//! `NodeIR.determinism_hints`. Hints are serialised to the SAME canonical
//! strings the platform has always emitted, so Flow IR JSON is unchanged for
//! known hints — but every consumer (kernel-plan validation, host preflight,
//! macro emission) now round-trips through this enum, so:
//!
//! - unknown/typo'd hint strings fail closed at validation time (EFFECT202)
//!   instead of silently evaporating (prefix typos) or producing a misleading
//!   `MissingCapabilities` error (suffix typos);
//! - effect/determinism conflict constraints (EFFECT201 / DET302) are derived
//!   exhaustively from the enum below, eliminating the historical
//!   `ensure_registered()` OnceLock registration-order hazard.
//!
//! GREP GATE: this module is the ONLY place in the workspace allowed to
//! contain `"resource::` string literals (enforced by
//! `scripts/check-hint-literals.sh`). Everything else must go through
//! `EffectHint::as_str()` / the capability `HINT_*` constants derived from it.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::effects::{Determinism, Effects};

/// Canonical typed resource hint.
///
/// Variants mirror the historical `resource::<family>[::<operation>]` string
/// vocabulary one-to-one. Bare family variants (e.g. [`EffectHint::Http`])
/// are primarily used as determinism hints; operation variants (e.g.
/// [`EffectHint::HttpRead`]) are primarily used as effect hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EffectHint {
    /// `resource::http` — HTTP family (determinism hint).
    Http,
    /// `resource::http::read` — outbound HTTP reads.
    HttpRead,
    /// `resource::http::write` — outbound HTTP writes.
    HttpWrite,
    /// `resource::clock` — wall-clock access.
    Clock,
    /// `resource::rng` — randomness.
    Rng,
    /// `resource::db` — legacy relational DB family (determinism hint).
    Db,
    /// `resource::db::read` — legacy relational DB reads.
    DbRead,
    /// `resource::db::write` — legacy relational DB writes.
    DbWrite,
    /// `resource::sql` — SQL family (determinism hint).
    Sql,
    /// `resource::sql::read` — SQL reads.
    SqlRead,
    /// `resource::sql::write` — SQL writes.
    SqlWrite,
    /// `resource::sql::admin` — SQL schema/admin operations.
    SqlAdmin,
    /// `resource::kv` — key-value family (determinism hint).
    Kv,
    /// `resource::kv::read` — KV reads.
    KvRead,
    /// `resource::kv::write` — KV writes.
    KvWrite,
    /// `resource::blob` — blob storage family (determinism hint).
    Blob,
    /// `resource::blob::read` — blob reads.
    BlobRead,
    /// `resource::blob::write` — blob writes.
    BlobWrite,
    /// `resource::queue` — queue family (determinism hint).
    Queue,
    /// `resource::queue::publish` — queue publishes.
    QueuePublish,
    /// `resource::queue::consume` — queue consumption.
    QueueConsume,
    /// `resource::dedupe` — dedupe-store family (determinism hint).
    Dedupe,
    /// `resource::dedupe::write` — dedupe-store reservations/writes.
    DedupeWrite,
    /// `resource::workspace` — run-scoped workspace family (determinism hint).
    Workspace,
    /// `resource::workspace::read` — workspace reads.
    WorkspaceRead,
    /// `resource::workspace::write` — workspace writes.
    WorkspaceWrite,
}

/// Error produced when a hint string does not name a canonical hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownEffectHint {
    /// The offending hint string.
    pub value: String,
}

impl std::fmt::Display for UnknownEffectHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unknown resource hint `{}`; hints must be canonical `resource::*` identifiers \
             defined by dag_core::EffectHint (e.g. `{}`)",
            self.value,
            EffectHint::HttpRead.as_str()
        )
    }
}

impl std::error::Error for UnknownEffectHint {}

impl EffectHint {
    /// Every canonical hint, in stable order. Constraint derivation, docs,
    /// and tooling iterate this instead of consulting a runtime registry.
    pub const ALL: [EffectHint; 26] = [
        EffectHint::Http,
        EffectHint::HttpRead,
        EffectHint::HttpWrite,
        EffectHint::Clock,
        EffectHint::Rng,
        EffectHint::Db,
        EffectHint::DbRead,
        EffectHint::DbWrite,
        EffectHint::Sql,
        EffectHint::SqlRead,
        EffectHint::SqlWrite,
        EffectHint::SqlAdmin,
        EffectHint::Kv,
        EffectHint::KvRead,
        EffectHint::KvWrite,
        EffectHint::Blob,
        EffectHint::BlobRead,
        EffectHint::BlobWrite,
        EffectHint::Queue,
        EffectHint::QueuePublish,
        EffectHint::QueueConsume,
        EffectHint::Dedupe,
        EffectHint::DedupeWrite,
        EffectHint::Workspace,
        EffectHint::WorkspaceRead,
        EffectHint::WorkspaceWrite,
    ];

    /// Canonical string form (`resource::*`). This is the serialisation used
    /// in Flow IR JSON and is byte-identical to the historical literals.
    pub const fn as_str(self) -> &'static str {
        match self {
            EffectHint::Http => "resource::http",
            EffectHint::HttpRead => "resource::http::read",
            EffectHint::HttpWrite => "resource::http::write",
            EffectHint::Clock => "resource::clock",
            EffectHint::Rng => "resource::rng",
            EffectHint::Db => "resource::db",
            EffectHint::DbRead => "resource::db::read",
            EffectHint::DbWrite => "resource::db::write",
            EffectHint::Sql => "resource::sql",
            EffectHint::SqlRead => "resource::sql::read",
            EffectHint::SqlWrite => "resource::sql::write",
            EffectHint::SqlAdmin => "resource::sql::admin",
            EffectHint::Kv => "resource::kv",
            EffectHint::KvRead => "resource::kv::read",
            EffectHint::KvWrite => "resource::kv::write",
            EffectHint::Blob => "resource::blob",
            EffectHint::BlobRead => "resource::blob::read",
            EffectHint::BlobWrite => "resource::blob::write",
            EffectHint::Queue => "resource::queue",
            EffectHint::QueuePublish => "resource::queue::publish",
            EffectHint::QueueConsume => "resource::queue::consume",
            EffectHint::Dedupe => "resource::dedupe",
            EffectHint::DedupeWrite => "resource::dedupe::write",
            EffectHint::Workspace => "resource::workspace",
            EffectHint::WorkspaceRead => "resource::workspace::read",
            EffectHint::WorkspaceWrite => "resource::workspace::write",
        }
    }

    /// Parse a canonical hint string. Fails closed on anything that is not
    /// an exact canonical spelling — both prefix typos (`resorce::http::read`)
    /// and suffix typos (`resource::http_raed`) are rejected.
    pub fn parse(value: &str) -> Result<EffectHint, UnknownEffectHint> {
        for hint in EffectHint::ALL {
            if hint.as_str() == value {
                return Ok(hint);
            }
        }
        Err(UnknownEffectHint {
            value: value.to_string(),
        })
    }

    /// The bare family variant for this hint (`HttpRead -> Http`, etc).
    pub const fn family(self) -> EffectHint {
        match self {
            EffectHint::Http | EffectHint::HttpRead | EffectHint::HttpWrite => EffectHint::Http,
            EffectHint::Clock => EffectHint::Clock,
            EffectHint::Rng => EffectHint::Rng,
            EffectHint::Db | EffectHint::DbRead | EffectHint::DbWrite => EffectHint::Db,
            EffectHint::Sql
            | EffectHint::SqlRead
            | EffectHint::SqlWrite
            | EffectHint::SqlAdmin => EffectHint::Sql,
            EffectHint::Kv | EffectHint::KvRead | EffectHint::KvWrite => EffectHint::Kv,
            EffectHint::Blob | EffectHint::BlobRead | EffectHint::BlobWrite => EffectHint::Blob,
            EffectHint::Queue | EffectHint::QueuePublish | EffectHint::QueueConsume => {
                EffectHint::Queue
            }
            EffectHint::Dedupe | EffectHint::DedupeWrite => EffectHint::Dedupe,
            EffectHint::Workspace | EffectHint::WorkspaceRead | EffectHint::WorkspaceWrite => {
                EffectHint::Workspace
            }
        }
    }

    /// Minimum effects level implied by this hint, with remediation guidance.
    ///
    /// Derived exhaustively here (not registered at runtime), so EFFECT201
    /// checking no longer depends on which `ensure_registered()` calls have
    /// happened to run in the current process.
    pub const fn effect_constraint(self) -> Option<(Effects, &'static str)> {
        match self {
            EffectHint::HttpRead => Some((
                Effects::ReadOnly,
                "HTTP reads reach external systems; declare effects = ReadOnly or Effectful.",
            )),
            EffectHint::HttpWrite => Some((
                Effects::Effectful,
                "HTTP writes are effectful; declare effects = Effectful and provide idempotency keys.",
            )),
            EffectHint::DbRead => Some((
                Effects::ReadOnly,
                "Database reads reach external state; declare effects = ReadOnly or Effectful.",
            )),
            EffectHint::DbWrite => Some((
                Effects::Effectful,
                "Database writes mutate external systems; declare effects = Effectful and supply idempotency.",
            )),
            EffectHint::SqlRead => Some((
                Effects::ReadOnly,
                "SQL reads access external relational state; declare effects = ReadOnly or Effectful.",
            )),
            EffectHint::SqlWrite => Some((
                Effects::Effectful,
                "SQL writes mutate external relational state; declare effects = Effectful and ensure idempotency.",
            )),
            EffectHint::SqlAdmin => Some((
                Effects::Effectful,
                "SQL admin access can alter schema or provider state; declare effects = Effectful and gate binding policy.",
            )),
            EffectHint::KvRead => Some((
                Effects::ReadOnly,
                "KV reads access external state; declare effects = ReadOnly or stronger.",
            )),
            EffectHint::KvWrite => Some((
                Effects::Effectful,
                "KV writes are effectful; declare effects = Effectful and ensure dedupe/idempotency.",
            )),
            EffectHint::BlobRead => Some((
                Effects::ReadOnly,
                "Blob reads access external storage; declare effects = ReadOnly or stronger.",
            )),
            EffectHint::BlobWrite => Some((
                Effects::Effectful,
                "Blob writes mutate external storage; declare effects = Effectful and supply idempotency.",
            )),
            EffectHint::QueuePublish => Some((
                Effects::Effectful,
                "Queue publishes are effectful; ensure effects = Effectful with dedupe keys.",
            )),
            EffectHint::QueueConsume => Some((
                Effects::ReadOnly,
                "Queue consumption acknowledges messages; treat as at least ReadOnly.",
            )),
            EffectHint::DedupeWrite => Some((
                Effects::Effectful,
                "Dedupe stores persist state; declare effects = Effectful when binding.",
            )),
            EffectHint::WorkspaceRead => Some((
                Effects::ReadOnly,
                "Workspace reads access run-scoped state; declare effects = ReadOnly or stronger.",
            )),
            EffectHint::WorkspaceWrite => Some((
                Effects::Effectful,
                "Workspace writes mutate run-scoped state; declare effects = Effectful.",
            )),
            // Bare family hints carry no effects floor on their own.
            EffectHint::Http
            | EffectHint::Clock
            | EffectHint::Rng
            | EffectHint::Db
            | EffectHint::Sql
            | EffectHint::Kv
            | EffectHint::Blob
            | EffectHint::Queue
            | EffectHint::Dedupe
            | EffectHint::Workspace => None,
        }
    }

    /// Minimum (i.e. most permissive acceptable) determinism level implied by
    /// this hint, with remediation guidance. Family-wide: `HttpRead` inherits
    /// the `Http` constraint, matching the historical prefix matching.
    pub const fn determinism_constraint(self) -> Option<(Determinism, &'static str)> {
        let guidance = match self.family() {
            EffectHint::Http => {
                "HTTP calls vary across retries; downgrade determinism or pin responses via caching."
            }
            EffectHint::Clock => {
                "Clock access is nondeterministic; declare determinism = BestEffort or lower."
            }
            EffectHint::Rng => {
                "Randomness is nondeterministic; downgrade determinism or inject fixed seeds."
            }
            EffectHint::Db => {
                "Database results can vary across retries; downgrade determinism or pin revisions."
            }
            EffectHint::Sql => {
                "SQL results can vary across retries; downgrade determinism or pin state through a higher-level protocol."
            }
            EffectHint::Kv => {
                "KV values may change between executions; downgrade determinism or pin versions."
            }
            EffectHint::Blob => {
                "Blob storage responses can change over time; downgrade determinism or pin versions."
            }
            EffectHint::Queue => {
                "Queue ordering and visibility vary; downgrade determinism or add sequence checks."
            }
            EffectHint::Dedupe => {
                "Dedupe lookups depend on external state; downgrade determinism or provide proofs."
            }
            EffectHint::Workspace => {
                "Workspace contents can differ across retries unless persisted; downgrade determinism or pin inputs."
            }
            // `family()` only returns bare family variants; the operation
            // variants are unreachable here but the match must be exhaustive.
            _ => "Resource access is nondeterministic; downgrade determinism.",
        };
        Some((Determinism::BestEffort, guidance))
    }
}

impl std::fmt::Display for EffectHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for EffectHint {
    type Err = UnknownEffectHint;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        EffectHint::parse(value)
    }
}

impl Serialize for EffectHint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for EffectHint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        EffectHint::parse(&value).map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for EffectHint {
    fn schema_name() -> String {
        "EffectHint".to_string()
    }

    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            enum_values: Some(
                EffectHint::ALL
                    .iter()
                    .map(|hint| serde_json::Value::String(hint.as_str().to_string()))
                    .collect(),
            ),
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "Canonical typed resource hint (dag_core::EffectHint). Unknown strings fail \
                     closed at validation time (EFFECT202)."
                        .to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_canonical_string() {
        for hint in EffectHint::ALL {
            assert_eq!(EffectHint::parse(hint.as_str()), Ok(hint));
            let json = serde_json::to_string(&hint).expect("serialize");
            assert_eq!(json, format!("\"{}\"", hint.as_str()));
            let back: EffectHint = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, hint);
        }
    }

    #[test]
    fn rejects_suffix_and_prefix_typos() {
        // suffix typo (correct prefix, bogus operation)
        let suffix_typo = ["resource", "::http_raed"].concat();
        assert!(EffectHint::parse(&suffix_typo).is_err());
        // prefix typo (misspelled `resource`)
        assert!(EffectHint::parse("resorce::http::read").is_err());
        // unknown family with correct shape
        let unknown_family = ["resource", "::mystery::read"].concat();
        assert!(EffectHint::parse(&unknown_family).is_err());
        assert!(EffectHint::parse("").is_err());
    }

    #[test]
    fn every_hint_has_a_determinism_constraint() {
        for hint in EffectHint::ALL {
            let (minimum, guidance) = hint
                .determinism_constraint()
                .expect("determinism constraint");
            assert_eq!(minimum, Determinism::BestEffort);
            assert!(!guidance.is_empty());
        }
    }

    #[test]
    fn operation_hints_carry_effects_floors() {
        assert_eq!(
            EffectHint::HttpRead.effect_constraint().map(|(min, _)| min),
            Some(Effects::ReadOnly)
        );
        assert_eq!(
            EffectHint::HttpWrite
                .effect_constraint()
                .map(|(min, _)| min),
            Some(Effects::Effectful)
        );
        assert_eq!(EffectHint::Http.effect_constraint(), None);
        assert_eq!(EffectHint::Clock.effect_constraint(), None);
    }
}
