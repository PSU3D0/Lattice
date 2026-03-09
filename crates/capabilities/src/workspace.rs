use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

use crate::Capability;

pub const ERR_WORKSPACE_INVALID_PATH: &str = "CAP-WS-001";
pub const ERR_WORKSPACE_PATH_TRAVERSAL: &str = "CAP-WS-002";
pub const ERR_WORKSPACE_NOT_FOUND: &str = "CAP-WS-003";
pub const ERR_WORKSPACE_UNSUPPORTED: &str = "CAP-WS-004";
pub const ERR_WORKSPACE_BACKEND: &str = "CAP-WS-005";

pub const HINT_WORKSPACE: &str = "resource::workspace";
pub const HINT_WORKSPACE_READ: &str = "resource::workspace::read";
pub const HINT_WORKSPACE_WRITE: &str = "resource::workspace::write";

static REGISTRATION: OnceLock<()> = OnceLock::new();

pub fn ensure_registered() {
    REGISTRATION.get_or_init(|| {
        dag_core::effects_registry::register_effect_constraint(
            dag_core::effects_registry::EffectConstraint::new(
                HINT_WORKSPACE_READ,
                dag_core::Effects::ReadOnly,
                "Workspace reads access run-scoped state; declare effects = ReadOnly or stronger.",
            ),
        );
        dag_core::effects_registry::register_effect_constraint(
            dag_core::effects_registry::EffectConstraint::new(
                HINT_WORKSPACE_WRITE,
                dag_core::Effects::Effectful,
                "Workspace writes mutate run-scoped state; declare effects = Effectful.",
            ),
        );
        dag_core::determinism::register_determinism_constraint(
            dag_core::determinism::DeterminismConstraint::new(
                HINT_WORKSPACE,
                dag_core::Determinism::BestEffort,
                "Workspace contents can differ across retries unless persisted; downgrade determinism or pin inputs.",
            ),
        );
    });
}

/// Write-time options for workspace entries.
///
/// Retention is intentionally not flow-controlled in the capability core.
/// Hosts own cleanup and retention policy for run-scoped workspaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceWriteOptions {}

/// Options for listing workspace entries.
///
/// `prefix` is currently the only portable filter. Pattern/glob semantics are
/// intentionally left out of the 0.1 capability core until they can be made
/// host-portable.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceListOptions {
    pub prefix: Option<String>,
}

impl WorkspaceListOptions {
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn normalized_prefix(&self) -> Result<Option<String>, WorkspaceError> {
        match self.prefix.as_deref() {
            Some(prefix) if prefix.is_empty() || prefix == "." => Ok(None),
            Some(prefix) => normalize_path(prefix).map(Some),
            None => Ok(None),
        }
    }

    pub fn normalized(&self) -> Result<Self, WorkspaceError> {
        Ok(Self {
            prefix: self.normalized_prefix()?,
        })
    }
}

/// Host-owned policy applied to a run-scoped workspace provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePolicy {
    pub max_total_bytes: Option<u64>,
    pub max_file_count: Option<u64>,
    pub max_single_file_bytes: Option<u64>,
    pub retain_completed_for: Option<std::time::Duration>,
}

/// Stable run scope used to bind a workspace provider across halt/resume.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkspaceRunScope {
    pub flow_id: String,
    pub run_id: String,
}

impl WorkspaceRunScope {
    pub fn new(flow_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            flow_id: flow_id.into(),
            run_id: run_id.into(),
        }
    }
}

/// Terminal run disposition passed to host-managed workspace cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceCompletionDisposition {
    Succeeded,
    Failed,
}

/// Host-side factory for binding run-scoped workspace providers.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait WorkspaceFactory: Send + Sync + 'static {
    async fn open(&self, scope: WorkspaceRunScope)
    -> anyhow::Result<std::sync::Arc<dyn Workspace>>;

    async fn complete(
        &self,
        scope: WorkspaceRunScope,
        disposition: WorkspaceCompletionDisposition,
    ) -> anyhow::Result<()>;
}

/// Metadata returned for workspace entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceEntry {
    pub path: String,
    pub size_bytes: u64,
    pub updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

/// Read payload for a workspace entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceReadResult {
    Bytes(Vec<u8>),
    BlobRef(String),
}

/// Result metadata for a write operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWriteResult {
    pub path: String,
    pub size_bytes: u64,
    pub updated_at_ms: u64,
}

/// Result metadata for a delete operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceDeleteResult {
    pub deleted: bool,
}

/// Errors raised by workspace capability implementations.
///
/// Missing reads should normally return `Ok(None)` and missing deletes should
/// return `WorkspaceDeleteResult { deleted: false }`. `NotFound` is reserved for
/// backends that need to surface a strict missing-object failure in a more
/// specific operation.
#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    #[error("invalid workspace path: {0}")]
    InvalidPath(String),
    #[error("workspace path traversal rejected: {0}")]
    PathTraversal(String),
    #[error("workspace entry not found: {0}")]
    NotFound(String),
    #[error("workspace operation unsupported: {0}")]
    Unsupported(String),
    #[error("workspace backend error: {0}")]
    Backend(String),
}

impl WorkspaceError {
    pub const fn code(&self) -> &'static str {
        match self {
            WorkspaceError::InvalidPath(_) => ERR_WORKSPACE_INVALID_PATH,
            WorkspaceError::PathTraversal(_) => ERR_WORKSPACE_PATH_TRAVERSAL,
            WorkspaceError::NotFound(_) => ERR_WORKSPACE_NOT_FOUND,
            WorkspaceError::Unsupported(_) => ERR_WORKSPACE_UNSUPPORTED,
            WorkspaceError::Backend(_) => ERR_WORKSPACE_BACKEND,
        }
    }
}

/// Normalize a user-provided workspace path.
///
/// Rules:
/// - convert `\\` separators into `/`
/// - remove `.` components
/// - reject absolute paths and `..` traversal
/// - reject non-ASCII paths by default
/// - reject paths resolving to workspace root
pub fn normalize_path(path: &str) -> Result<String, WorkspaceError> {
    if path.is_empty() {
        return Err(WorkspaceError::InvalidPath("path is empty".to_string()));
    }

    if path.contains('\0') {
        return Err(WorkspaceError::InvalidPath(
            "path contains NUL bytes".to_string(),
        ));
    }

    if !path.is_ascii() {
        return Err(WorkspaceError::InvalidPath(
            "path must be ASCII-safe by default".to_string(),
        ));
    }

    let canonical = path.replace('\\', "/");

    if canonical.starts_with('/') {
        return Err(WorkspaceError::InvalidPath(
            "absolute paths are not allowed".to_string(),
        ));
    }

    let bytes = canonical.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return Err(WorkspaceError::InvalidPath(
            "drive-prefixed paths are not allowed".to_string(),
        ));
    }

    let mut segments = Vec::new();
    for segment in canonical.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }

        if segment == ".." {
            return Err(WorkspaceError::PathTraversal(path.to_string()));
        }

        segments.push(segment);
    }

    if segments.is_empty() {
        return Err(WorkspaceError::InvalidPath(
            "path resolves to workspace root".to_string(),
        ));
    }

    Ok(segments.join("/"))
}

/// Run-scoped, sandboxed workspace filesystem surface.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait Workspace: Capability {
    async fn read_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, WorkspaceError>;

    async fn write_normalized(
        &self,
        normalized_path: &str,
        data: &[u8],
        options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, WorkspaceError>;

    async fn list_normalized(
        &self,
        options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError>;

    async fn delete_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, WorkspaceError>;

    async fn read(&self, path: &str) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
        let normalized = normalize_path(path)?;
        self.read_normalized(&normalized).await
    }

    async fn write(
        &self,
        path: &str,
        data: &[u8],
        options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, WorkspaceError> {
        let normalized = normalize_path(path)?;
        self.write_normalized(&normalized, data, options).await
    }

    async fn list(
        &self,
        options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
        let normalized = options.normalized()?;
        self.list_normalized(normalized).await
    }

    async fn delete(&self, path: &str) -> Result<WorkspaceDeleteResult, WorkspaceError> {
        let normalized = normalize_path(path)?;
        self.delete_normalized(&normalized).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingWorkspace {
        last_path: Mutex<Option<String>>,
        last_list_prefix: Mutex<Option<Option<String>>>,
    }

    impl Capability for RecordingWorkspace {
        fn name(&self) -> &'static str {
            "workspace.recording"
        }
    }

    #[async_trait]
    impl Workspace for RecordingWorkspace {
        async fn read_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            let mut last_path = self.last_path.lock().expect("lock poisoned");
            *last_path = Some(normalized_path.to_string());
            Ok(None)
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, WorkspaceError> {
            let mut last_path = self.last_path.lock().expect("lock poisoned");
            *last_path = Some(normalized_path.to_string());
            Ok(WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: 0,
            })
        }

        async fn list_normalized(
            &self,
            options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
            let mut last_prefix = self.last_list_prefix.lock().expect("lock poisoned");
            *last_prefix = Some(options.prefix);
            Ok(Vec::new())
        }

        async fn delete_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
            let mut last_path = self.last_path.lock().expect("lock poisoned");
            *last_path = Some(normalized_path.to_string());
            Ok(WorkspaceDeleteResult { deleted: false })
        }
    }

    #[test]
    fn registers_workspace_constraints_once() {
        ensure_registered();
        ensure_registered();

        let read = dag_core::effects_registry::constraint_for_hint(HINT_WORKSPACE_READ)
            .expect("workspace read constraint");
        assert_eq!(read.minimum, dag_core::Effects::ReadOnly);

        let write = dag_core::effects_registry::constraint_for_hint(HINT_WORKSPACE_WRITE)
            .expect("workspace write constraint");
        assert_eq!(write.minimum, dag_core::Effects::Effectful);

        let det = dag_core::determinism::constraint_for_hint(HINT_WORKSPACE)
            .expect("workspace determinism constraint");
        assert_eq!(det.minimum, dag_core::Determinism::BestEffort);
    }

    #[test]
    fn normalize_path_removes_current_dir_and_duplicate_slashes() {
        let normalized = normalize_path("inbox//./events/data.json").expect("normalize");
        assert_eq!(normalized, "inbox/events/data.json");
    }

    #[test]
    fn normalize_path_rejects_traversal() {
        let err = normalize_path("../../etc/passwd").expect_err("path traversal must fail");
        assert_eq!(err.code(), ERR_WORKSPACE_PATH_TRAVERSAL);

        let err =
            normalize_path("..\\..\\etc\\passwd").expect_err("windows traversal path must fail");
        assert_eq!(err.code(), ERR_WORKSPACE_PATH_TRAVERSAL);
    }

    #[test]
    fn normalize_path_rejects_absolute_paths() {
        let err = normalize_path("/tmp/out.txt").expect_err("absolute path must fail");
        assert_eq!(err.code(), ERR_WORKSPACE_INVALID_PATH);
    }

    #[test]
    fn normalize_path_rejects_drive_prefixed_paths() {
        let err = normalize_path("C:/temp/out.txt").expect_err("drive path must fail");
        assert_eq!(err.code(), ERR_WORKSPACE_INVALID_PATH);
    }

    #[test]
    fn normalize_path_rejects_non_ascii_paths_by_default() {
        let err = normalize_path("inbox/naive-cafe\u{00e9}.txt").expect_err("non-ascii path");
        assert_eq!(err.code(), ERR_WORKSPACE_INVALID_PATH);
    }

    #[test]
    fn list_options_normalize_prefix() {
        let options = WorkspaceListOptions::default().with_prefix("./inbox//events");
        assert_eq!(
            options.normalized_prefix().expect("normalized prefix"),
            Some("inbox/events".to_string())
        );

        let normalized = options.normalized().expect("normalized list options");
        assert_eq!(normalized.prefix.as_deref(), Some("inbox/events"));
    }

    #[test]
    fn list_options_reject_non_ascii_prefix_by_default() {
        let options = WorkspaceListOptions::default().with_prefix("inbox/r\u{00e9}ports");
        let err = options.normalized().expect_err("non-ascii prefix rejected");
        assert_eq!(err.code(), ERR_WORKSPACE_INVALID_PATH);
    }

    #[tokio::test]
    async fn workspace_trait_read_normalizes_before_dispatch() {
        let workspace = RecordingWorkspace::default();
        workspace
            .read("./inbox//items.json")
            .await
            .expect("read succeeds");

        let last_path = workspace.last_path.lock().expect("lock poisoned").clone();
        assert_eq!(last_path.as_deref(), Some("inbox/items.json"));
    }

    #[tokio::test]
    async fn workspace_trait_read_rejects_traversal_before_dispatch() {
        let workspace = RecordingWorkspace::default();
        let err = workspace
            .read("../secret.txt")
            .await
            .expect_err("must fail");
        assert_eq!(err.code(), ERR_WORKSPACE_PATH_TRAVERSAL);

        let last_path = workspace.last_path.lock().expect("lock poisoned").clone();
        assert!(last_path.is_none());
    }

    #[tokio::test]
    async fn workspace_trait_list_normalizes_prefix_before_dispatch() {
        let workspace = RecordingWorkspace::default();
        workspace
            .list(WorkspaceListOptions::default().with_prefix("./inbox//events"))
            .await
            .expect("list succeeds");

        let last_prefix = workspace
            .last_list_prefix
            .lock()
            .expect("lock poisoned")
            .clone();
        assert_eq!(last_prefix, Some(Some("inbox/events".to_string())));
    }

    #[tokio::test]
    async fn workspace_trait_list_rejects_traversal_before_dispatch() {
        let workspace = RecordingWorkspace::default();
        let err = workspace
            .list(WorkspaceListOptions::default().with_prefix("../secrets"))
            .await
            .expect_err("list traversal must fail");
        assert_eq!(err.code(), ERR_WORKSPACE_PATH_TRAVERSAL);

        let last_prefix = workspace
            .last_list_prefix
            .lock()
            .expect("lock poisoned")
            .clone();
        assert!(last_prefix.is_none());
    }

    #[test]
    fn write_options_default_to_empty_host_policy_marker() {
        let _options = WorkspaceWriteOptions::default();
    }
}
