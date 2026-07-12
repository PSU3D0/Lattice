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
// H5b (F1/F2): the workspace grant is split so `read` does NOT confer
// `write`/`delete`, mirroring HTTP's `Http`/`HttpRead`/`HttpWrite`. The
// per-opcode byte surface (`workspace_read()`/`workspace_write()` +
// `_raw`) uses the specific split grants below.
//
// H5c-enforcement (F2 native fix): the deprecated bare `workspace()` accessor
// is re-gated to `[EffectHint::Workspace]` ONLY — it no longer accepts a
// `WorkspaceRead`/`WorkspaceWrite` hint. Previously it was gated on the
// *combined* set, so a `workspace::read`-only node could obtain the full raw
// write/delete trait through bare `workspace()`, bypassing the split on native
// hosts. Gating on bare `Workspace` alone closes that (a read-only node holds
// only `WorkspaceRead`, which no longer reaches this accessor). Preflight is
// unaffected — it runs against the unscoped bag, not `ScopedResources`.
const GRANTS_WORKSPACE_BARE: &[EffectHint] = &[EffectHint::Workspace];
// Read is the lesser privilege: bare `Workspace`, `WorkspaceRead`, AND
// `WorkspaceWrite` all confer it (write implies read, matching the
// `WorkspaceWrite` view which exposes `read(handle)`). Write is conferred only
// by bare `Workspace` or `WorkspaceWrite` — a `WorkspaceRead`-only node is
// denied write/delete (the F2 fix).
const GRANTS_WORKSPACE_READ: &[EffectHint] = &[
    EffectHint::Workspace,
    EffectHint::WorkspaceRead,
    EffectHint::WorkspaceWrite,
];
const GRANTS_WORKSPACE_WRITE: &[EffectHint] = &[EffectHint::Workspace, EffectHint::WorkspaceWrite];

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
        if granting_hints.iter().any(|hint| self.grants.contains(hint)) {
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
        // H5c-enforcement (F2 native fix): bare `workspace()` is gated on
        // `[EffectHint::Workspace]` ONLY, so a read-only node cannot climb to
        // the raw write/delete trait through it.
        if !self.allows("workspace", GRANTS_WORKSPACE_BARE) {
            return None;
        }
        self.inner.workspace()
    }

    // H5c-enforcement: the split accessors now delegate to the INNER VIEW
    // accessors (not `inner.workspace()`), so the per-run root key arrives from
    // the base layer (`InvocationResources`/`ResourceBag`) and gates 2+3 become
    // load-bearing. A `workspace::read` grant reaches only `workspace_read()`;
    // `write`/`delete` go through `workspace_write()`. The `_raw` variants keep
    // the split grants but return the arbitrary-path trait for path-by-contract
    // consumers (stdlib path nodes, the wasm workspace opcodes).
    fn workspace_read(&self) -> Option<crate::WorkspaceRead> {
        if !self.allows("workspace_read", GRANTS_WORKSPACE_READ) {
            return None;
        }
        self.inner.workspace_read()
    }

    fn workspace_write(&self) -> Option<crate::WorkspaceWrite> {
        if !self.allows("workspace_write", GRANTS_WORKSPACE_WRITE) {
            return None;
        }
        self.inner.workspace_write()
    }

    fn workspace_read_raw(&self) -> Option<&dyn workspace::Workspace> {
        if !self.allows("workspace_read_raw", GRANTS_WORKSPACE_READ) {
            return None;
        }
        self.inner.workspace_read_raw()
    }

    fn workspace_write_raw(&self) -> Option<&dyn workspace::Workspace> {
        if !self.allows("workspace_write_raw", GRANTS_WORKSPACE_WRITE) {
            return None;
        }
        self.inner.workspace_write_raw()
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
        let scoped = ScopedResources::new("kv_node", full_bag(), [EffectHint::KvRead]);
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

    // ---- H5b: workspace read/write grant split (§16.4, F1/F2) ----

    struct NoopWorkspace;
    impl crate::Capability for NoopWorkspace {
        fn name(&self) -> &'static str {
            "workspace.noop.scoped"
        }
    }
    #[async_trait::async_trait]
    impl workspace::Workspace for NoopWorkspace {
        async fn read_normalized(
            &self,
            _p: &str,
        ) -> Result<Option<workspace::WorkspaceReadResult>, workspace::WorkspaceError> {
            Ok(None)
        }
        async fn write_normalized(
            &self,
            p: &str,
            data: &[u8],
            _o: workspace::WorkspaceWriteOptions,
        ) -> Result<workspace::WorkspaceWriteResult, workspace::WorkspaceError> {
            Ok(workspace::WorkspaceWriteResult {
                path: p.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: 0,
            })
        }
        async fn list_normalized(
            &self,
            _o: workspace::WorkspaceListOptions,
        ) -> Result<Vec<workspace::WorkspaceEntry>, workspace::WorkspaceError> {
            Ok(Vec::new())
        }
        async fn delete_normalized(
            &self,
            _p: &str,
        ) -> Result<workspace::WorkspaceDeleteResult, workspace::WorkspaceError> {
            Ok(workspace::WorkspaceDeleteResult { deleted: false })
        }
    }

    fn ws_bag() -> Arc<dyn ResourceAccess> {
        Arc::new(ResourceBag::new().with_workspace(Arc::new(NoopWorkspace)))
    }

    #[test]
    fn empty_grant_denies_both_workspace_accessors() {
        let scoped = ScopedResources::new("pure_node", ws_bag(), []);
        assert!(scoped.workspace_read().is_none());
        assert!(scoped.workspace_write().is_none());
        let denials = scoped.take_denials();
        assert_eq!(denials.len(), 2);
        assert!(denials.iter().any(|d| d.capability == "workspace_read"));
        assert!(denials.iter().any(|d| d.capability == "workspace_write"));
    }

    #[test]
    fn read_only_node_gets_read_but_not_write() {
        let scoped = ScopedResources::new("reader", ws_bag(), [EffectHint::WorkspaceRead]);
        assert!(scoped.workspace_read().is_some());
        assert!(scoped.workspace_write().is_none());
        let denials = scoped.take_denials();
        assert_eq!(denials.len(), 1);
        assert_eq!(denials[0].capability, "workspace_write");
        assert_eq!(denials[0].granting_hints, GRANTS_WORKSPACE_WRITE);
    }

    #[test]
    fn write_grant_confers_write_and_read_but_bare_read_hint_does_not_confer_write() {
        // Bare Workspace hint grants both (back-compat, like bare Http).
        let both = ScopedResources::new("bare", ws_bag(), [EffectHint::Workspace]);
        assert!(both.workspace_read().is_some());
        assert!(both.workspace_write().is_some());
        assert!(both.take_denials().is_empty());

        // Write hint grants write AND read (write implies read).
        let writer = ScopedResources::new("writer", ws_bag(), [EffectHint::WorkspaceWrite]);
        assert!(writer.workspace_write().is_some());
        assert!(writer.workspace_read().is_some());
        assert!(writer.take_denials().is_empty());
    }

    // ---- H5c-enforcement: F2 native fix — read cannot reach write, all three
    // surfaces (view, raw, AND bare workspace()) ----

    #[test]
    fn read_only_node_cannot_reach_any_write_surface() {
        let scoped = ScopedResources::new("reader", ws_bag(), [EffectHint::WorkspaceRead]);
        // The read surfaces are granted.
        assert!(scoped.workspace_read().is_some());
        assert!(scoped.workspace_read_raw().is_some());
        // None of the write surfaces — including the previously-bypassable bare
        // `workspace()` accessor (the F2-native fix) — are reachable.
        assert!(scoped.workspace_write().is_none());
        assert!(scoped.workspace_write_raw().is_none());
        assert!(scoped.workspace().is_none());
        let denials = scoped.take_denials();
        assert!(denials.iter().any(|d| d.capability == "workspace_write"));
        assert!(
            denials
                .iter()
                .any(|d| d.capability == "workspace_write_raw")
        );
        assert!(denials.iter().any(|d| d.capability == "workspace"));
    }

    #[test]
    fn bare_workspace_requires_the_bare_workspace_hint() {
        // Only `EffectHint::Workspace` confers the deprecated raw bare
        // `workspace()`; a split write hint alone does NOT (it grants the split
        // write surfaces instead). This is the re-gating that closes F2.
        let bare = ScopedResources::new("bare", ws_bag(), [EffectHint::Workspace]);
        assert!(bare.workspace().is_some());
        assert!(bare.take_denials().is_empty());

        let writer = ScopedResources::new("writer", ws_bag(), [EffectHint::WorkspaceWrite]);
        assert!(writer.workspace().is_none());
        assert_eq!(writer.take_denials()[0].capability, "workspace");
    }

    #[test]
    fn denied_view_still_denies_via_dyn_resource_access() {
        let scoped: Arc<dyn ResourceAccess> =
            Arc::new(ScopedResources::new("dyn_node", full_bag(), []));
        assert!(scoped.clock().is_none());
    }
}
