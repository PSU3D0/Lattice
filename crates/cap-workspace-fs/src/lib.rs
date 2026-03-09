use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use capabilities::Capability;
use capabilities::workspace::{
    Workspace, WorkspaceCompletionDisposition, WorkspaceDeleteResult, WorkspaceEntry,
    WorkspaceError, WorkspaceFactory, WorkspaceListOptions, WorkspacePolicy, WorkspaceReadResult,
    WorkspaceRunScope, WorkspaceWriteOptions, WorkspaceWriteResult,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct FsWorkspaceConfig {
    pub root: PathBuf,
    pub policy: WorkspacePolicy,
}

#[derive(Debug, Clone)]
pub struct FsWorkspaceFactory {
    config: FsWorkspaceConfig,
}

#[derive(Debug)]
pub struct FsWorkspace {
    run_root: PathBuf,
    policy: WorkspacePolicy,
    usage: Mutex<WorkspaceUsage>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WorkspaceUsage {
    total_bytes: u64,
    file_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetentionMarker {
    flow_id: String,
    run_id: String,
    disposition: String,
    completed_at_ms: u64,
    retain_until_ms: u64,
}

#[derive(Debug, Error)]
pub enum FsWorkspaceError {
    #[error("invalid workspace path `{path}`")]
    InvalidPath { path: String },
    #[error("workspace policy violated: {0}")]
    Policy(String),
    #[error("workspace io failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("workspace metadata failure: {0}")]
    Metadata(#[from] serde_json::Error),
}

impl FsWorkspaceFactory {
    pub fn new(config: FsWorkspaceConfig) -> Self {
        Self { config }
    }

    pub fn run_root_path(&self, scope: &WorkspaceRunScope) -> PathBuf {
        self.config
            .root
            .join("runs")
            .join(flow_key(&scope.flow_id))
            .join(path_key(&scope.run_id))
    }

    pub fn retention_marker_path(&self, scope: &WorkspaceRunScope) -> PathBuf {
        self.config
            .root
            .join("retained")
            .join(flow_key(&scope.flow_id))
            .join(format!("{}.json", path_key(&scope.run_id)))
    }

    pub fn reap_expired(&self) -> Result<usize, FsWorkspaceError> {
        self.reap_expired_at(now_ms())
    }

    pub fn reap_expired_at(&self, now_ms: u64) -> Result<usize, FsWorkspaceError> {
        let retained_root = self.config.root.join("retained");
        if !retained_root.exists() {
            return Ok(0);
        }

        let mut removed = 0usize;
        for entry in WalkDir::new(&retained_root)
            .min_depth(1)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
        {
            let marker: RetentionMarker = serde_json::from_slice(&fs::read(entry.path())?)?;
            if marker.retain_until_ms > now_ms {
                continue;
            }
            let scope = WorkspaceRunScope::new(marker.flow_id, marker.run_id);
            let run_root = self.run_root_path(&scope);
            if run_root.exists() {
                fs::remove_dir_all(&run_root)?;
            }
            remove_empty_ancestors(&run_root, &self.config.root.join("runs"))?;
            fs::remove_file(entry.path())?;
            remove_empty_ancestors(entry.path(), &retained_root)?;
            removed += 1;
        }

        Ok(removed)
    }
}

#[async_trait]
impl WorkspaceFactory for FsWorkspaceFactory {
    async fn open(&self, scope: WorkspaceRunScope) -> anyhow::Result<Arc<dyn Workspace>> {
        let run_root = self.run_root_path(&scope);
        fs::create_dir_all(&run_root)?;
        let usage = scan_usage(&run_root)?;
        Ok(Arc::new(FsWorkspace {
            run_root,
            policy: self.config.policy.clone(),
            usage: Mutex::new(usage),
        }))
    }

    async fn complete(
        &self,
        scope: WorkspaceRunScope,
        disposition: WorkspaceCompletionDisposition,
    ) -> anyhow::Result<()> {
        let run_root = self.run_root_path(&scope);
        let marker_path = self.retention_marker_path(&scope);

        if let Some(retain_for) = self.config.policy.retain_completed_for {
            let completed_at_ms = now_ms();
            let retain_until_ms = completed_at_ms.saturating_add(duration_to_millis(retain_for));
            if let Some(parent) = marker_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let marker = RetentionMarker {
                flow_id: scope.flow_id,
                run_id: scope.run_id,
                disposition: match disposition {
                    WorkspaceCompletionDisposition::Succeeded => "succeeded".to_string(),
                    WorkspaceCompletionDisposition::Failed => "failed".to_string(),
                },
                completed_at_ms,
                retain_until_ms,
            };
            fs::write(&marker_path, serde_json::to_vec_pretty(&marker)?)?;
            return Ok(());
        }

        if run_root.exists() {
            fs::remove_dir_all(&run_root)?;
        }
        remove_empty_ancestors(&run_root, &self.config.root.join("runs"))?;
        if marker_path.exists() {
            fs::remove_file(&marker_path)?;
            remove_empty_ancestors(&marker_path, &self.config.root.join("retained"))?;
        }
        Ok(())
    }
}

impl Capability for FsWorkspace {
    fn name(&self) -> &'static str {
        "workspace-fs"
    }
}

#[async_trait]
impl Workspace for FsWorkspace {
    async fn read_normalized(
        &self,
        path: &str,
    ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
        let full_path = self.resolve(path)?;
        if !full_path.exists() {
            return Ok(None);
        }
        let metadata = fs::metadata(&full_path).map_err(workspace_io_error)?;
        if !metadata.is_file() {
            return Err(WorkspaceError::Backend(format!(
                "workspace path `{path}` does not refer to a file"
            )));
        }
        let bytes = fs::read(&full_path).map_err(workspace_io_error)?;
        Ok(Some(WorkspaceReadResult::Bytes(bytes)))
    }

    async fn write_normalized(
        &self,
        path: &str,
        data: &[u8],
        _options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, WorkspaceError> {
        let full_path = self.resolve(path)?;
        let new_size = data.len() as u64;
        enforce_single_file_limit(&self.policy, path, new_size)?;

        let existing_metadata = fs::metadata(&full_path).ok();
        if let Some(metadata) = existing_metadata.as_ref() {
            if !metadata.is_file() {
                return Err(WorkspaceError::Backend(format!(
                    "workspace path `{path}` does not refer to a file"
                )));
            }
        }

        {
            let mut usage = self.usage.lock().map_err(|_| {
                WorkspaceError::Backend("workspace usage lock poisoned".to_string())
            })?;
            let existing_size = existing_metadata
                .as_ref()
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            let file_count_delta = if existing_metadata.is_some() { 0 } else { 1 };
            let next_total = usage
                .total_bytes
                .saturating_sub(existing_size)
                .saturating_add(new_size);
            let next_file_count = usage.file_count + file_count_delta;
            enforce_policy(&self.policy, path, next_total, next_file_count)?;

            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent).map_err(workspace_io_error)?;
            }
            fs::write(&full_path, data).map_err(workspace_io_error)?;
            usage.total_bytes = next_total;
            usage.file_count = next_file_count;
        }

        Ok(WorkspaceWriteResult {
            path: path.to_string(),
            size_bytes: new_size,
            updated_at_ms: now_ms(),
        })
    }

    async fn list_normalized(
        &self,
        options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
        if !self.run_root.exists() {
            return Ok(Vec::new());
        }

        let prefix = options.prefix;
        let mut entries = Vec::new();
        for entry in WalkDir::new(&self.run_root)
            .min_depth(1)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
        {
            let Some(relative_path) = relative_workspace_path(entry.path(), &self.run_root) else {
                continue;
            };
            if let Some(prefix) = prefix.as_deref() {
                if !relative_path.starts_with(prefix) {
                    continue;
                }
            }
            let metadata = entry
                .metadata()
                .map_err(|err| WorkspaceError::Backend(err.to_string()))?;
            entries.push(WorkspaceEntry {
                path: relative_path,
                size_bytes: metadata.len(),
                updated_at_ms: metadata_modified_ms(&metadata),
                content_hash: None,
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    async fn delete_normalized(&self, path: &str) -> Result<WorkspaceDeleteResult, WorkspaceError> {
        let full_path = self.resolve(path)?;
        let metadata = match fs::metadata(&full_path) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(WorkspaceDeleteResult { deleted: false });
            }
            Err(err) => return Err(workspace_io_error(err)),
        };

        if !metadata.is_file() {
            return Err(WorkspaceError::Backend(format!(
                "workspace path `{path}` does not refer to a file"
            )));
        }

        fs::remove_file(&full_path).map_err(workspace_io_error)?;
        remove_empty_ancestors(&full_path, &self.run_root).map_err(workspace_io_error)?;

        let mut usage = self
            .usage
            .lock()
            .map_err(|_| WorkspaceError::Backend("workspace usage lock poisoned".to_string()))?;
        usage.total_bytes = usage.total_bytes.saturating_sub(metadata.len());
        usage.file_count = usage.file_count.saturating_sub(1);

        Ok(WorkspaceDeleteResult { deleted: true })
    }
}

impl FsWorkspace {
    fn resolve(&self, path: &str) -> Result<PathBuf, WorkspaceError> {
        resolve_relative_path(&self.run_root, path).map_err(|err| match err {
            FsWorkspaceError::InvalidPath { path } => WorkspaceError::InvalidPath(path),
            other => WorkspaceError::Backend(other.to_string()),
        })
    }
}

fn workspace_io_error(err: std::io::Error) -> WorkspaceError {
    WorkspaceError::Backend(err.to_string())
}

fn enforce_single_file_limit(
    policy: &WorkspacePolicy,
    path: &str,
    new_size: u64,
) -> Result<(), WorkspaceError> {
    if let Some(limit) = policy.max_single_file_bytes {
        if new_size > limit {
            return Err(WorkspaceError::Backend(format!(
                "workspace write for `{path}` exceeds max_single_file_bytes ({new_size} > {limit})"
            )));
        }
    }
    Ok(())
}

fn enforce_policy(
    policy: &WorkspacePolicy,
    path: &str,
    next_total_bytes: u64,
    next_file_count: u64,
) -> Result<(), WorkspaceError> {
    if let Some(limit) = policy.max_total_bytes {
        if next_total_bytes > limit {
            return Err(WorkspaceError::Backend(format!(
                "workspace write for `{path}` exceeds max_total_bytes ({next_total_bytes} > {limit})"
            )));
        }
    }
    if let Some(limit) = policy.max_file_count {
        if next_file_count > limit {
            return Err(WorkspaceError::Backend(format!(
                "workspace write for `{path}` exceeds max_file_count ({next_file_count} > {limit})"
            )));
        }
    }
    Ok(())
}

fn resolve_relative_path(root: &Path, path: &str) -> Result<PathBuf, FsWorkspaceError> {
    let mut resolved = root.to_path_buf();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(segment) => resolved.push(segment),
            _ => {
                return Err(FsWorkspaceError::InvalidPath {
                    path: path.to_string(),
                });
            }
        }
    }
    Ok(resolved)
}

fn scan_usage(run_root: &Path) -> Result<WorkspaceUsage, FsWorkspaceError> {
    if !run_root.exists() {
        return Ok(WorkspaceUsage::default());
    }

    let mut usage = WorkspaceUsage::default();
    for entry in WalkDir::new(run_root)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        let metadata = entry
            .metadata()
            .map_err(|err| FsWorkspaceError::Io(std::io::Error::other(err.to_string())))?;
        usage.file_count += 1;
        usage.total_bytes = usage.total_bytes.saturating_add(metadata.len());
    }
    Ok(usage)
}

fn relative_workspace_path(path: &Path, run_root: &Path) -> Option<String> {
    let relative = path.strip_prefix(run_root).ok()?;
    let normalized = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn metadata_modified_ms(metadata: &fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

fn flow_key(flow_id: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, flow_id.as_bytes()).to_string()
}

fn path_key(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        let ch = byte as char;
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            encoded.push(ch);
        } else {
            encoded.push('_');
            encoded.push_str(&format!("{byte:02x}"));
        }
    }
    if encoded.is_empty() {
        "_".to_string()
    } else {
        encoded
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn remove_empty_ancestors(path: &Path, stop_at: &Path) -> std::io::Result<()> {
    let mut current = path.parent();
    while let Some(dir) = current {
        if dir == stop_at || !dir.starts_with(stop_at) {
            break;
        }
        match fs::remove_dir(dir) {
            Ok(()) => current = dir.parent(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => current = dir.parent(),
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_scope(run_id: &str) -> WorkspaceRunScope {
        WorkspaceRunScope::new("flow:test", run_id)
    }

    fn test_factory(policy: WorkspacePolicy) -> (tempfile::TempDir, FsWorkspaceFactory) {
        let dir = tempdir().expect("tempdir");
        let factory = FsWorkspaceFactory::new(FsWorkspaceConfig {
            root: dir.path().to_path_buf(),
            policy,
        });
        (dir, factory)
    }

    #[tokio::test]
    async fn isolates_runs_and_preserves_resume_state() {
        let (_dir, factory) = test_factory(WorkspacePolicy::default());
        let scope_a = test_scope("run-a");
        let scope_b = test_scope("run-b");

        let workspace_a = factory.open(scope_a.clone()).await.expect("open run a");
        workspace_a
            .write(
                "nested/output.txt",
                b"alpha",
                WorkspaceWriteOptions::default(),
            )
            .await
            .expect("write run a");

        let workspace_a_resume = factory.open(scope_a.clone()).await.expect("reopen run a");
        let read_back = workspace_a_resume
            .read("nested/output.txt")
            .await
            .expect("read run a")
            .expect("file exists");
        assert_eq!(read_back, WorkspaceReadResult::Bytes(b"alpha".to_vec()));

        let workspace_b = factory.open(scope_b.clone()).await.expect("open run b");
        assert_eq!(
            workspace_b
                .read("nested/output.txt")
                .await
                .expect("read run b"),
            None
        );
    }

    #[tokio::test]
    async fn rejects_traversal_paths() {
        let (_dir, factory) = test_factory(WorkspacePolicy::default());
        let workspace = factory
            .open(test_scope("run-a"))
            .await
            .expect("open workspace");

        let err = workspace
            .write("../escape.txt", b"bad", WorkspaceWriteOptions::default())
            .await
            .expect_err("traversal rejected");
        assert!(matches!(
            err,
            WorkspaceError::InvalidPath(_) | WorkspaceError::PathTraversal(_)
        ));
    }

    #[tokio::test]
    async fn enforces_quota_on_overwrite_and_file_count() {
        let (_dir, factory) = test_factory(WorkspacePolicy {
            max_total_bytes: Some(5),
            max_file_count: Some(1),
            max_single_file_bytes: Some(5),
            retain_completed_for: None,
        });
        let workspace = factory
            .open(test_scope("run-a"))
            .await
            .expect("open workspace");

        workspace
            .write("a.txt", b"abc", WorkspaceWriteOptions::default())
            .await
            .expect("initial write");
        workspace
            .write("a.txt", b"abcd", WorkspaceWriteOptions::default())
            .await
            .expect("overwrite within quota");

        let count_err = workspace
            .write("b.txt", b"z", WorkspaceWriteOptions::default())
            .await
            .expect_err("file count exceeded");
        assert!(
            matches!(count_err, WorkspaceError::Backend(message) if message.contains("max_file_count"))
        );

        let size_err = workspace
            .write("a.txt", b"abcdef", WorkspaceWriteOptions::default())
            .await
            .expect_err("single file limit exceeded");
        assert!(
            matches!(size_err, WorkspaceError::Backend(message) if message.contains("max_single_file_bytes"))
        );
    }

    #[tokio::test]
    async fn complete_removes_run_root_when_retention_is_unset() {
        let (_dir, factory) = test_factory(WorkspacePolicy::default());
        let scope = test_scope("run-a");
        let workspace = factory.open(scope.clone()).await.expect("open workspace");
        workspace
            .write("a.txt", b"abc", WorkspaceWriteOptions::default())
            .await
            .expect("write file");

        let run_root = factory.run_root_path(&scope);
        assert!(run_root.exists());

        factory
            .complete(scope.clone(), WorkspaceCompletionDisposition::Succeeded)
            .await
            .expect("complete workspace");

        assert!(!run_root.exists());
    }

    #[tokio::test]
    async fn retained_completion_is_reaped_after_deadline() {
        let (_dir, factory) = test_factory(WorkspacePolicy {
            max_total_bytes: None,
            max_file_count: None,
            max_single_file_bytes: None,
            retain_completed_for: Some(Duration::from_millis(50)),
        });
        let scope = test_scope("run-a");
        let workspace = factory.open(scope.clone()).await.expect("open workspace");
        workspace
            .write("a.txt", b"abc", WorkspaceWriteOptions::default())
            .await
            .expect("write file");

        let run_root = factory.run_root_path(&scope);
        let marker_path = factory.retention_marker_path(&scope);

        factory
            .complete(scope.clone(), WorkspaceCompletionDisposition::Succeeded)
            .await
            .expect("complete workspace");

        assert!(run_root.exists());
        assert!(marker_path.exists());

        let marker: RetentionMarker =
            serde_json::from_slice(&fs::read(&marker_path).expect("read marker"))
                .expect("decode marker");
        let removed = factory
            .reap_expired_at(marker.retain_until_ms.saturating_sub(1))
            .expect("reap before deadline");
        assert_eq!(removed, 0);
        assert!(run_root.exists());

        let removed = factory
            .reap_expired_at(marker.retain_until_ms)
            .expect("reap after deadline");
        assert_eq!(removed, 1);
        assert!(!run_root.exists());
        assert!(!marker_path.exists());
    }
}
