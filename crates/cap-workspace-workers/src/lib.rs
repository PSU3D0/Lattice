#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use capabilities::Capability;
#[cfg(not(target_arch = "wasm32"))]
use capabilities::workspace::{
    Workspace, WorkspaceCompletionDisposition, WorkspaceDeleteResult, WorkspaceError,
    WorkspaceFactory, WorkspaceListOptions, WorkspaceReadResult, WorkspaceWriteOptions,
    WorkspaceWriteResult,
};
use capabilities::workspace::{WorkspaceEntry, WorkspacePolicy, WorkspaceRunScope};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_WORKSPACE_BUCKET_BINDING: &str = "WORKSPACE_BUCKET";
pub const DEFAULT_WORKSPACE_DO_BINDING: &str = "WORKSPACE_DO";
pub const DEFAULT_WORKSPACE_OBJECT_PREFIX: &str = "workspace";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkersWorkspaceConfig {
    pub bucket_binding: String,
    pub index_binding: String,
    pub object_prefix: String,
    pub policy: WorkspacePolicy,
    pub blocked_prefixes: Vec<String>,
    pub max_path_depth: Option<u32>,
    pub max_path_length: Option<u32>,
}

impl Default for WorkersWorkspaceConfig {
    fn default() -> Self {
        Self {
            bucket_binding: DEFAULT_WORKSPACE_BUCKET_BINDING.to_string(),
            index_binding: DEFAULT_WORKSPACE_DO_BINDING.to_string(),
            object_prefix: DEFAULT_WORKSPACE_OBJECT_PREFIX.to_string(),
            policy: WorkspacePolicy::default(),
            blocked_prefixes: Vec::new(),
            max_path_depth: None,
            max_path_length: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceEntryMeta {
    pub path: String,
    pub object_key: String,
    pub size_bytes: u64,
    pub updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

impl WorkspaceEntryMeta {
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    fn to_workspace_entry(&self) -> WorkspaceEntry {
        WorkspaceEntry {
            path: self.path.clone(),
            size_bytes: self.size_bytes,
            updated_at_ms: self.updated_at_ms,
            content_hash: self.content_hash.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWriteReservation {
    pub entry: WorkspaceEntryMeta,
}

#[derive(Debug, Error)]
pub enum WorkersWorkspaceError {
    #[error("cap-workspace-workers requires wasm32")]
    Unsupported,
    #[error("workers workspace backend error: {0}")]
    Backend(String),
    #[error("workers workspace protocol error: {0}")]
    Protocol(String),
    #[error("workers workspace serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct WorkersWorkspaceFactory {
    config: WorkersWorkspaceConfig,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WorkersWorkspace {
    scope: WorkspaceRunScope,
    policy: WorkspacePolicy,
}

#[cfg(not(target_arch = "wasm32"))]
impl WorkersWorkspaceFactory {
    pub fn new(config: WorkersWorkspaceConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &WorkersWorkspaceConfig {
        &self.config
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl WorkspaceFactory for WorkersWorkspaceFactory {
    async fn open(&self, _scope: WorkspaceRunScope) -> anyhow::Result<Arc<dyn Workspace>> {
        Err(anyhow::anyhow!(WorkersWorkspaceError::Unsupported))
    }

    async fn complete(
        &self,
        _scope: WorkspaceRunScope,
        _disposition: WorkspaceCompletionDisposition,
    ) -> anyhow::Result<()> {
        Err(anyhow::anyhow!(WorkersWorkspaceError::Unsupported))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Capability for WorkersWorkspace {
    fn name(&self) -> &'static str {
        "workspace.workers"
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl Workspace for WorkersWorkspace {
    async fn read_normalized(
        &self,
        _normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }

    async fn write_normalized(
        &self,
        _normalized_path: &str,
        _data: &[u8],
        _options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }

    async fn list_normalized(
        &self,
        _options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }

    async fn delete_normalized(
        &self,
        _normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
        Err(WorkspaceError::Unsupported(
            WorkersWorkspaceError::Unsupported.to_string(),
        ))
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(target_arch = "wasm32")]
pub use wasm::{
    WorkersWorkspace, WorkersWorkspaceFactory, WorkspaceDurableObject, WorkspaceIndexClient,
};

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn duration_to_millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn now_ms() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        let millis = js_sys::Date::now();
        if millis.is_finite() && millis >= 0.0 {
            millis as u64
        } else {
            0
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration_to_millis(duration))
            .unwrap_or(0)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn workspace_object_key(
    object_prefix: &str,
    flow_id: &str,
    run_id: &str,
    path: &str,
) -> String {
    format!(
        "{}/{}/{}/{}",
        trim_slashes(object_prefix),
        flow_key(flow_id),
        path_key(run_id),
        path
    )
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn workspace_scope_name(scope: &WorkspaceRunScope) -> String {
    format!(
        "workspace/{}/{}",
        flow_key(&scope.flow_id),
        path_key(&scope.run_id)
    )
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn flow_key(flow_id: &str) -> String {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, flow_id.as_bytes()).to_string()
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn path_key(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            encoded.push(ch);
        } else {
            encoded.push('_');
        }
    }
    if encoded.is_empty() {
        "_".to_string()
    } else {
        encoded
    }
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn trim_slashes(value: &str) -> &str {
    value.trim_matches('/')
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = u32::from(last).checked_add(1).and_then(char::from_u32);
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn normalize_blocked_prefix(prefix: &str) -> Option<String> {
    let trimmed = trim_slashes(prefix);
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn is_blocked_path(path: &str, blocked_prefixes: &[String]) -> bool {
    blocked_prefixes.iter().any(|prefix| {
        path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) fn path_depth(path: &str) -> u32 {
    if path.is_empty() {
        0
    } else {
        path.split('/').count() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::{is_blocked_path, normalize_blocked_prefix, path_depth, prefix_upper_bound};

    #[test]
    fn prefix_upper_bound_advances_ascii_suffix() {
        assert_eq!(prefix_upper_bound("abc"), Some("abd".to_string()));
        assert_eq!(prefix_upper_bound("tmp/"), Some("tmp0".to_string()));
    }

    #[test]
    fn prefix_upper_bound_empty_is_none() {
        assert_eq!(prefix_upper_bound(""), None);
    }

    #[test]
    fn blocked_prefixes_match_exact_or_nested_paths() {
        let blocked = vec!["node_modules".to_string(), "target/debug".to_string()];
        assert!(is_blocked_path("node_modules", &blocked));
        assert!(is_blocked_path("node_modules/pkg/index.js", &blocked));
        assert!(is_blocked_path("target/debug/build.txt", &blocked));
        assert!(!is_blocked_path("node_modules-cache/pkg", &blocked));
        assert!(!is_blocked_path("target/release/app", &blocked));
    }

    #[test]
    fn normalize_blocked_prefix_trims_slashes() {
        assert_eq!(
            normalize_blocked_prefix("/node_modules/"),
            Some("node_modules".to_string())
        );
        assert_eq!(normalize_blocked_prefix("///"), None);
    }

    #[test]
    fn path_depth_counts_segments() {
        assert_eq!(path_depth(""), 0);
        assert_eq!(path_depth("one"), 1);
        assert_eq!(path_depth("one/two/three"), 3);
    }
}
