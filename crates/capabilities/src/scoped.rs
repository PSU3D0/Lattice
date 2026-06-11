//! Scoped per-node resource views (packet A2).
//!
//! `ScopedResources` is the enforcement half of the capability declaration
//! contract. A node's declarations (`NodeIR.effect_hints`, plus any
//! connector-resolved hints the host grants) become a *grant set* of
//! [`dag_core::EffectHint`]s; every capability accessor on the wrapped
//! [`ResourceAccess`] is gated on that grant set. Undeclared access fails
//! closed: the accessor returns `None`, a structured `CAP110` denial is
//! recorded on the view (and emitted as a `tracing` warning) so the executor
//! can attribute the resulting node failure to the missing declaration
//! instead of a bare "capability missing" message.
//!
//! Grant semantics mirror the host preflight satisfaction rules
//! (`host-inproc::is_hint_satisfied_by_resources`), inverted:
//!
//! - A bare family hint (e.g. `resource::http`) grants every accessor of the
//!   family (`http_read()` and `http_write()`), because preflight accepts any
//!   accessor of the family as satisfying a bare hint.
//! - An operation hint grants exactly its accessor (`resource::http::read`
//!   grants `http_read()` only). KV/Blob/Queue/Dedupe/Workspace expose one
//!   accessor per family, so any hint of those families grants that accessor.
//! - `resource::db::*` and `resource::rng` have NO `ResourceAccess`
//!   accessors; granting them grants nothing (they remain unsatisfiable, as
//!   in preflight).
//!
//! Deliberately NOT gated (pass-through to the inner view):
//!
//! - `cache()` — has no `resource::*` hint vocabulary; it is process-local
//!   infrastructure, not a declared capability (see capabilities-and-binding
//!   spec, "Capability Inventory").
//! - The four durability accessors (`checkpoint_store()`, etc.) — durability
//!   is a host-internal service selected by durability *policy*, explicitly
//!   not bound via `resource::*` hints.
//! - `connector_runtime()` / `connector_scope()` — connector access is
//!   declared via `NodeIR.connector_ops` and constrained by the per-node
//!   `ConnectorBindingScope`, a separate declaration surface.
//! - `max_durability_mode()` — policy metadata, not a capability.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use dag_core::EffectHint;

use crate::{ResourceAccess, connector, durability, workspace};

/// Stable diagnostic code for an undeclared capability access denial.
/// Registered in `dag_core::DIAGNOSTIC_CODES` and `impl-docs/error-codes.md`.
pub const CAPABILITY_DENIED_CODE: &str = "CAP110";

/// One recorded denial: a node asked for a capability accessor its grant set
/// does not cover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityDenial {
    /// Alias of the node whose execution context performed the access.
    pub node_alias: String,
    /// The `ResourceAccess` accessor that was denied (e.g. `http_read`).
    pub capability: &'static str,
    /// The effect hints that would have granted this accessor.
    pub granting_hints: &'static [EffectHint],
}

impl CapabilityDenial {
    /// Human-facing denial message carrying the CAP110 code and the exact
    /// declaration the author needs to add.
    pub fn message(&self) -> String {
        let hints = self
            .granting_hints
            .iter()
            .map(|hint| format!("`{}`", hint.as_str()))
            .collect::<Vec<_>>()
            .join(" or ");
        format!(
            "{code}: node `{node}` accessed capability `{cap}()` which is not declared in its \
             effect hints. Declare the capability on the node — add a `resources(...)` binding \
             for it (or the equivalent effect hint {hints}) — or remove the access. \
             See impl-docs/error-codes.md ({code}).",
            code = CAPABILITY_DENIED_CODE,
            node = self.node_alias,
            cap = self.capability,
            hints = hints,
        )
    }
}

impl std::fmt::Display for CapabilityDenial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

/// Per-node capability view enforcing the node's declared grant set.
///
/// Constructed by the executor for every node from its declarations; wraps
/// the full host bag (or any inner `ResourceAccess`). Undeclared accessors
/// return `None` and record a [`CapabilityDenial`].
pub struct ScopedResources {
    inner: Arc<dyn ResourceAccess>,
    grants: BTreeSet<EffectHint>,
    node_alias: String,
    denials: Mutex<Vec<CapabilityDenial>>,
}

// Accessor -> granting hints tables (inverse of preflight satisfaction).
const GRANTS_HTTP_READ: &[EffectHint] = &[EffectHint::Http, EffectHint::HttpRead];
const GRANTS_HTTP_WRITE: &[EffectHint] = &[EffectHint::Http, EffectHint::HttpWrite];
const GRANTS_CLOCK: &[EffectHint] = &[EffectHint::Clock];
const GRANTS_KV: &[EffectHint] = &[EffectHint::Kv, EffectHint::KvRead, EffectHint::KvWrite];
const GRANTS_SQL_READ: &[EffectHint] = &[EffectHint::Sql, EffectHint::SqlRead];
const GRANTS_SQL_WRITE: &[EffectHint] = &[EffectHint::Sql, EffectHint::SqlWrite];
const GRANTS_SQL_ADMIN: &[EffectHint] = &[EffectHint::Sql, EffectHint::SqlAdmin];
const GRANTS_BLOB: &[EffectHint] = &[
    EffectHint::Blob,
    EffectHint::BlobRead,
    EffectHint::BlobWrite,
];
const GRANTS_QUEUE: &[EffectHint] = &[
    EffectHint::Queue,
    EffectHint::QueuePublish,
    EffectHint::QueueConsume,
];
const GRANTS_DEDUPE: &[EffectHint] = &[EffectHint::Dedupe, EffectHint::DedupeWrite];
const GRANTS_WORKSPACE: &[EffectHint] = &[
    EffectHint::Workspace,
    EffectHint::WorkspaceRead,
    EffectHint::WorkspaceWrite,
];

impl ScopedResources {
    /// Build a scoped view for `node_alias` over `inner`, granting exactly
    /// the supplied hints. An empty iterator yields an empty view: Pure
    /// nodes get no clock, no rng, nothing.
    pub fn new(
        node_alias: impl Into<String>,
        inner: Arc<dyn ResourceAccess>,
        grants: impl IntoIterator<Item = EffectHint>,
    ) -> Self {
        Self {
            inner,
            grants: grants.into_iter().collect(),
            node_alias: node_alias.into(),
            denials: Mutex::new(Vec::new()),
        }
    }

    /// The grant set backing this view.
    pub fn grants(&self) -> &BTreeSet<EffectHint> {
        &self.grants
    }

    /// Alias of the node this view is scoped to.
    pub fn node_alias(&self) -> &str {
        &self.node_alias
    }

    /// Drain and return every denial recorded since the last call. The
    /// executor calls this after each handler invocation to attribute
    /// failures to undeclared access.
    pub fn take_denials(&self) -> Vec<CapabilityDenial> {
        std::mem::take(&mut *self.denials.lock().expect("denials mutex poisoned"))
    }

    fn allows(&self, capability: &'static str, granting_hints: &'static [EffectHint]) -> bool {
        if granting_hints
            .iter()
            .any(|hint| self.grants.contains(hint))
        {
            return true;
        }
        let denial = CapabilityDenial {
            node_alias: self.node_alias.clone(),
            capability,
            granting_hints,
        };
        tracing::warn!(
            code = CAPABILITY_DENIED_CODE,
            node = %self.node_alias,
            capability = capability,
            "{}",
            denial.message(),
        );
        self.denials
            .lock()
            .expect("denials mutex poisoned")
            .push(denial);
        false
    }
}

impl ResourceAccess for ScopedResources {
    fn http_read(&self) -> Option<&dyn crate::http::HttpRead> {
        if !self.allows("http_read", GRANTS_HTTP_READ) {
            return None;
        }
        self.inner.http_read()
    }

    fn http_write(&self) -> Option<&dyn crate::http::HttpWrite> {
        if !self.allows("http_write", GRANTS_HTTP_WRITE) {
            return None;
        }
        self.inner.http_write()
    }

    fn clock(&self) -> Option<&dyn crate::clock::Clock> {
        if !self.allows("clock", GRANTS_CLOCK) {
            return None;
        }
        self.inner.clock()
    }

    // Cache has no `resource::*` hint vocabulary; pass through (see module docs).
    fn cache(&self) -> Option<&dyn crate::cache::Cache> {
        self.inner.cache()
    }

    fn kv(&self) -> Option<&dyn crate::kv::KeyValue> {
        if !self.allows("kv", GRANTS_KV) {
            return None;
        }
        self.inner.kv()
    }

    fn sql_read(&self) -> Option<&dyn crate::sql::SqlRead> {
        if !self.allows("sql_read", GRANTS_SQL_READ) {
            return None;
        }
        self.inner.sql_read()
    }

    fn sql_write(&self) -> Option<&dyn crate::sql::SqlWrite> {
        if !self.allows("sql_write", GRANTS_SQL_WRITE) {
            return None;
        }
        self.inner.sql_write()
    }

    fn sql_admin(&self) -> Option<&dyn crate::sql::SqlAdmin> {
        if !self.allows("sql_admin", GRANTS_SQL_ADMIN) {
            return None;
        }
        self.inner.sql_admin()
    }

    fn blob(&self) -> Option<&dyn crate::blob::BlobStore> {
        if !self.allows("blob", GRANTS_BLOB) {
            return None;
        }
        self.inner.blob()
    }

    fn queue(&self) -> Option<&dyn crate::queue::Queue> {
        if !self.allows("queue", GRANTS_QUEUE) {
            return None;
        }
        self.inner.queue()
    }

    fn dedupe_store(&self) -> Option<&dyn crate::dedupe::DedupeStore> {
        if !self.allows("dedupe_store", GRANTS_DEDUPE) {
            return None;
        }
        self.inner.dedupe_store()
    }

    // Durability services are host-internal, selected by durability policy,
    // not bound via resource hints; pass through (see module docs).
    fn checkpoint_store(&self) -> Option<&dyn durability::CheckpointStore> {
        self.inner.checkpoint_store()
    }

    fn resume_scheduler(&self) -> Option<&dyn durability::ResumeScheduler> {
        self.inner.resume_scheduler()
    }

    fn resume_signal_source(&self) -> Option<&dyn durability::ResumeSignalSource> {
        self.inner.resume_signal_source()
    }

    fn checkpoint_blob_store(&self) -> Option<&dyn durability::CheckpointBlobStore> {
        self.inner.checkpoint_blob_store()
    }

    fn workspace(&self) -> Option<&dyn workspace::Workspace> {
        if !self.allows("workspace", GRANTS_WORKSPACE) {
            return None;
        }
        self.inner.workspace()
    }

    // Connector access is declared via NodeIR.connector_ops and constrained
    // by ConnectorBindingScope; pass through (see module docs).
    fn connector_runtime(&self) -> Option<Arc<dyn connector::ConnectorRuntime>> {
        self.inner.connector_runtime()
    }

    fn connector_scope(&self) -> Option<connector::ConnectorBindingScope> {
        self.inner.connector_scope()
    }

    // Binding metadata (lock-recorded hints), not a capability; pass through.
    fn connector_resolved_effect_hints(&self) -> Option<&connector::ConnectorResolvedEffectHints> {
        self.inner.connector_resolved_effect_hints()
    }

    fn max_durability_mode(&self) -> dag_core::DurabilityMode {
        self.inner.max_durability_mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceBag;
    use std::time::SystemTime;

    struct TestClock;

    impl crate::Capability for TestClock {
        fn name(&self) -> &'static str {
            "clock.test.scoped"
        }
    }

    impl crate::clock::Clock for TestClock {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }
    }

    fn full_bag() -> Arc<dyn ResourceAccess> {
        Arc::new(
            ResourceBag::new()
                .with_clock(Arc::new(TestClock))
                .with_kv(Arc::new(crate::kv::MemoryKv::new()))
                .with_cache(Arc::new(crate::cache::MemoryCache::new())),
        )
    }

    #[test]
    fn empty_grant_set_denies_everything_gated() {
        let scoped = ScopedResources::new("pure_node", full_bag(), []);
        assert!(scoped.clock().is_none());
        assert!(scoped.kv().is_none());
        assert!(scoped.http_read().is_none());
        let denials = scoped.take_denials();
        assert_eq!(denials.len(), 3);
        assert!(denials.iter().all(|d| d.node_alias == "pure_node"));
        // Draining resets the record.
        assert!(scoped.take_denials().is_empty());
    }

    #[test]
    fn operation_hint_grants_exactly_its_accessor() {
        let scoped =
            ScopedResources::new("kv_node", full_bag(), [EffectHint::KvRead]);
        assert!(scoped.kv().is_some());
        assert!(scoped.clock().is_none());
        let denials = scoped.take_denials();
        assert_eq!(denials.len(), 1);
        assert_eq!(denials[0].capability, "clock");
        let message = denials[0].message();
        assert!(message.contains(CAPABILITY_DENIED_CODE));
        assert!(message.contains("kv_node"));
        assert!(message.contains("clock()"));
        assert!(message.contains("resources("));
        assert!(message.contains(EffectHint::Clock.as_str()));
    }

    #[test]
    fn bare_family_hint_grants_family_accessors() {
        let bag = full_bag();
        let scoped = ScopedResources::new("clocky", bag, [EffectHint::Clock]);
        assert!(scoped.clock().is_some());
        assert!(scoped.take_denials().is_empty());
    }

    #[test]
    fn ungated_surfaces_pass_through() {
        let scoped = ScopedResources::new("pure_node", full_bag(), []);
        // cache has no hint vocabulary; durability/connector surfaces are
        // host-internal declaration surfaces.
        assert!(scoped.cache().is_some());
        assert!(scoped.checkpoint_store().is_none()); // bag has none; no denial either way
        assert!(scoped.connector_runtime().is_none());
        assert!(scoped.take_denials().is_empty());
    }

    #[test]
    fn denied_view_still_denies_via_dyn_resource_access() {
        let scoped: Arc<dyn ResourceAccess> =
            Arc::new(ScopedResources::new("dyn_node", full_bag(), []));
        assert!(scoped.clock().is_none());
    }
}
