use capabilities::context;
use capabilities::workspace::{
    WorkspaceEntry, WorkspaceError, WorkspaceListOptions, WorkspaceReadResult,
    WorkspaceWriteOptions, normalize_path,
};
use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;
#[cfg(feature = "host-bundle")]
use kernel_exec::{NodeRegistry, RegistryError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceWriteInput {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWriteOutput {
    pub path: String,
    pub size_bytes: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceReadInput {
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceReadOutput {
    pub path: String,
    pub found: bool,
    pub value: Option<WorkspaceReadValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceReadValue {
    Bytes { bytes: Vec<u8> },
    BlobRef { blob_ref: String },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WorkspaceListInput {
    pub prefix: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceListOutput {
    pub entries: Vec<WorkspaceListEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceListEntry {
    pub path: String,
    pub size_bytes: u64,
    pub updated_at_ms: u64,
    pub content_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceDeleteInput {
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceDeleteOutput {
    pub path: String,
    pub deleted: bool,
}

#[def_node(
    name = "WorkspaceWrite",
    summary = "Write bytes into the run-scoped workspace",
    identifier = "std.workspace.write",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_write(capabilities::workspace::Workspace)),
    idempotency(key = "workspace.write.path", scope = "Node")
)]
pub async fn workspace_write(input: WorkspaceWriteInput) -> NodeResult<WorkspaceWriteOutput> {
    let normalized_path =
        normalize_path(&input.path).map_err(|err| workspace_operation_error("write", err))?;

    context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| missing_workspace_error("write"))?;
        let result = workspace
            .write_normalized(
                &normalized_path,
                &input.bytes,
                WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(|err| workspace_operation_error("write", err))?;
        Ok(WorkspaceWriteOutput {
            path: result.path,
            size_bytes: result.size_bytes,
            updated_at_ms: result.updated_at_ms,
        })
    })
    .await
    .ok_or_else(|| missing_resource_context_error("write"))?
}

#[def_node(
    name = "WorkspaceRead",
    summary = "Read bytes or blob references from the run-scoped workspace",
    identifier = "std.workspace.read",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(workspace_read(capabilities::workspace::Workspace))
)]
pub async fn workspace_read(input: WorkspaceReadInput) -> NodeResult<WorkspaceReadOutput> {
    let normalized_path =
        normalize_path(&input.path).map_err(|err| workspace_operation_error("read", err))?;

    context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| missing_workspace_error("read"))?;
        let result = workspace
            .read_normalized(&normalized_path)
            .await
            .map_err(|err| workspace_operation_error("read", err))?;
        Ok(match result {
            Some(value) => WorkspaceReadOutput {
                path: normalized_path,
                found: true,
                value: Some(map_read_value(value)),
            },
            None => WorkspaceReadOutput {
                path: normalized_path,
                found: false,
                value: None,
            },
        })
    })
    .await
    .ok_or_else(|| missing_resource_context_error("read"))?
}

#[def_node(
    name = "WorkspaceList",
    summary = "List workspace entries by optional prefix",
    identifier = "std.workspace.list",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(workspace_read(capabilities::workspace::Workspace))
)]
pub async fn workspace_list(input: WorkspaceListInput) -> NodeResult<WorkspaceListOutput> {
    let options = WorkspaceListOptions {
        prefix: input.prefix,
    }
    .normalized()
    .map_err(|err| workspace_operation_error("list", err))?;

    context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| missing_workspace_error("list"))?;
        let entries = workspace
            .list_normalized(options)
            .await
            .map_err(|err| workspace_operation_error("list", err))?;
        Ok(WorkspaceListOutput {
            entries: entries.into_iter().map(map_list_entry).collect(),
        })
    })
    .await
    .ok_or_else(|| missing_resource_context_error("list"))?
}

#[def_node(
    name = "WorkspaceDelete",
    summary = "Delete a workspace entry by path",
    identifier = "std.workspace.delete",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_write(capabilities::workspace::Workspace)),
    idempotency(key = "workspace.delete.path", scope = "Node")
)]
pub async fn workspace_delete(input: WorkspaceDeleteInput) -> NodeResult<WorkspaceDeleteOutput> {
    let normalized_path =
        normalize_path(&input.path).map_err(|err| workspace_operation_error("delete", err))?;

    context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| missing_workspace_error("delete"))?;
        let result = workspace
            .delete_normalized(&normalized_path)
            .await
            .map_err(|err| workspace_operation_error("delete", err))?;
        Ok(WorkspaceDeleteOutput {
            path: normalized_path,
            deleted: result.deleted,
        })
    })
    .await
    .ok_or_else(|| missing_resource_context_error("delete"))?
}

fn missing_resource_context_error(operation: &str) -> NodeError {
    NodeError::new(format!(
        "std.workspace.{operation} missing ResourceAccess context"
    ))
}

fn missing_workspace_error(operation: &str) -> NodeError {
    NodeError::new(format!("std.workspace.{operation} requires Workspace"))
}

fn workspace_operation_error(operation: &str, err: WorkspaceError) -> NodeError {
    NodeError::new(format!(
        "std.workspace.{operation} failed [{}]: {err}",
        err.code()
    ))
}

fn map_read_value(value: WorkspaceReadResult) -> WorkspaceReadValue {
    match value {
        WorkspaceReadResult::Bytes(bytes) => WorkspaceReadValue::Bytes { bytes },
        WorkspaceReadResult::BlobRef(blob_ref) => WorkspaceReadValue::BlobRef { blob_ref },
    }
}

fn map_list_entry(entry: WorkspaceEntry) -> WorkspaceListEntry {
    WorkspaceListEntry {
        path: entry.path,
        size_bytes: entry.size_bytes,
        updated_at_ms: entry.updated_at_ms,
        content_hash: entry.content_hash,
    }
}

#[cfg(feature = "host-bundle")]
pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    workspace_write_register(registry)?;
    workspace_read_register(registry)?;
    workspace_list_register(registry)?;
    workspace_delete_register(registry)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use capabilities::workspace::{Workspace, WorkspaceDeleteResult, WorkspaceWriteResult};
    use capabilities::{Capability, ResourceBag, context};

    #[derive(Default)]
    struct MemoryWorkspace {
        files: Mutex<BTreeMap<String, StoredEntry>>,
        clock_ms: Mutex<u64>,
    }

    #[derive(Clone, Debug)]
    struct StoredEntry {
        value: WorkspaceReadResult,
        updated_at_ms: u64,
        content_hash: Option<String>,
    }

    impl Capability for MemoryWorkspace {
        fn name(&self) -> &'static str {
            "workspace.memory"
        }
    }

    #[async_trait]
    impl Workspace for MemoryWorkspace {
        async fn read_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            Ok(self
                .files
                .lock()
                .expect("files lock")
                .get(normalized_path)
                .map(|entry| entry.value.clone()))
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, WorkspaceError> {
            let mut clock = self.clock_ms.lock().expect("clock lock");
            *clock += 1;
            let updated_at_ms = *clock;
            self.files.lock().expect("files lock").insert(
                normalized_path.to_string(),
                StoredEntry {
                    value: WorkspaceReadResult::Bytes(data.to_vec()),
                    updated_at_ms,
                    content_hash: Some(format!("sha256:{}", data.len())),
                },
            );
            Ok(WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms,
            })
        }

        async fn list_normalized(
            &self,
            options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
            let prefix = options.prefix;
            let files = self.files.lock().expect("files lock");
            let mut entries = files
                .iter()
                .filter(|(path, _)| {
                    prefix
                        .as_deref()
                        .map(|prefix| path.starts_with(prefix))
                        .unwrap_or(true)
                })
                .map(|(path, entry)| WorkspaceEntry {
                    path: path.clone(),
                    size_bytes: match &entry.value {
                        WorkspaceReadResult::Bytes(bytes) => bytes.len() as u64,
                        WorkspaceReadResult::BlobRef(blob_ref) => blob_ref.len() as u64,
                    },
                    updated_at_ms: entry.updated_at_ms,
                    content_hash: entry.content_hash.clone(),
                })
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            Ok(entries)
        }

        async fn delete_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
            let deleted = self
                .files
                .lock()
                .expect("files lock")
                .remove(normalized_path)
                .is_some();
            Ok(WorkspaceDeleteResult { deleted })
        }
    }

    #[test]
    fn node_specs_expose_workspace_hints() {
        let write = workspace_write_node_spec();
        assert_eq!(write.identifier, "std.workspace.write");
        assert_eq!(write.effects, dag_core::Effects::Effectful);
        assert_eq!(write.determinism, dag_core::Determinism::BestEffort);
        assert_eq!(
            write.effect_hints,
            [capabilities::workspace::HINT_WORKSPACE_WRITE]
        );

        let read = workspace_read_node_spec();
        assert_eq!(read.identifier, "std.workspace.read");
        assert_eq!(read.effects, dag_core::Effects::ReadOnly);
        assert_eq!(
            read.effect_hints,
            [capabilities::workspace::HINT_WORKSPACE_READ]
        );

        let list = workspace_list_node_spec();
        assert_eq!(list.identifier, "std.workspace.list");
        assert_eq!(
            list.effect_hints,
            [capabilities::workspace::HINT_WORKSPACE_READ]
        );

        let delete = workspace_delete_node_spec();
        assert_eq!(delete.identifier, "std.workspace.delete");
        assert_eq!(delete.effects, dag_core::Effects::Effectful);
        assert_eq!(
            delete.effect_hints,
            [capabilities::workspace::HINT_WORKSPACE_WRITE]
        );
    }

    #[tokio::test]
    async fn write_returns_metadata_and_normalized_path() {
        let bag = ResourceBag::new().with_workspace(Arc::new(MemoryWorkspace::default()));
        let output = context::with_resources(Arc::new(bag), async {
            workspace_write(WorkspaceWriteInput {
                path: "./artifacts//report.txt".to_string(),
                bytes: b"hello".to_vec(),
            })
            .await
        })
        .await
        .expect("workspace write succeeds");

        assert_eq!(
            output,
            WorkspaceWriteOutput {
                path: "artifacts/report.txt".to_string(),
                size_bytes: 5,
                updated_at_ms: 1,
            }
        );
    }

    #[tokio::test]
    async fn read_returns_found_false_for_missing_entries() {
        let bag = ResourceBag::new().with_workspace(Arc::new(MemoryWorkspace::default()));
        let output = context::with_resources(Arc::new(bag), async {
            workspace_read(WorkspaceReadInput {
                path: "missing.txt".to_string(),
            })
            .await
        })
        .await
        .expect("workspace read succeeds");

        assert_eq!(
            output,
            WorkspaceReadOutput {
                path: "missing.txt".to_string(),
                found: false,
                value: None,
            }
        );
    }

    #[tokio::test]
    async fn read_maps_blob_ref_payloads() {
        let workspace = Arc::new(MemoryWorkspace::default());
        workspace.files.lock().expect("files lock").insert(
            "artifacts/blob.bin".to_string(),
            StoredEntry {
                value: WorkspaceReadResult::BlobRef("blob://artifact".to_string()),
                updated_at_ms: 7,
                content_hash: None,
            },
        );

        let bag = ResourceBag::new().with_workspace(workspace);
        let output = context::with_resources(Arc::new(bag), async {
            workspace_read(WorkspaceReadInput {
                path: "artifacts/blob.bin".to_string(),
            })
            .await
        })
        .await
        .expect("workspace read succeeds");

        assert_eq!(
            output,
            WorkspaceReadOutput {
                path: "artifacts/blob.bin".to_string(),
                found: true,
                value: Some(WorkspaceReadValue::BlobRef {
                    blob_ref: "blob://artifact".to_string(),
                }),
            }
        );
    }

    #[tokio::test]
    async fn list_maps_workspace_entries() {
        let workspace = Arc::new(MemoryWorkspace::default());
        workspace
            .write("artifacts/b.txt", b"bbb", WorkspaceWriteOptions::default())
            .await
            .expect("seed b");
        workspace
            .write("artifacts/a.txt", b"a", WorkspaceWriteOptions::default())
            .await
            .expect("seed a");

        let bag = ResourceBag::new().with_workspace(workspace);
        let output = context::with_resources(Arc::new(bag), async {
            workspace_list(WorkspaceListInput {
                prefix: Some("artifacts".to_string()),
            })
            .await
        })
        .await
        .expect("workspace list succeeds");

        assert_eq!(output.entries.len(), 2);
        assert_eq!(output.entries[0].path, "artifacts/a.txt");
        assert_eq!(output.entries[1].path, "artifacts/b.txt");
    }

    #[tokio::test]
    async fn delete_preserves_soft_miss_behavior() {
        let bag = ResourceBag::new().with_workspace(Arc::new(MemoryWorkspace::default()));
        let output = context::with_resources(Arc::new(bag), async {
            workspace_delete(WorkspaceDeleteInput {
                path: "missing.txt".to_string(),
            })
            .await
        })
        .await
        .expect("workspace delete succeeds");

        assert_eq!(
            output,
            WorkspaceDeleteOutput {
                path: "missing.txt".to_string(),
                deleted: false,
            }
        );
    }

    #[tokio::test]
    async fn invalid_path_errors_include_workspace_code() {
        let bag = ResourceBag::new().with_workspace(Arc::new(MemoryWorkspace::default()));
        let err = context::with_resources(Arc::new(bag), async {
            workspace_read(WorkspaceReadInput {
                path: "../secret.txt".to_string(),
            })
            .await
        })
        .await
        .expect_err("invalid path should fail");

        let message = err.to_string();
        assert!(message.contains("std.workspace.read failed"));
        assert!(message.contains(capabilities::workspace::ERR_WORKSPACE_PATH_TRAVERSAL));
    }

    #[tokio::test]
    async fn backend_failures_propagate_as_node_errors() {
        struct FailingWorkspace;

        impl Capability for FailingWorkspace {
            fn name(&self) -> &'static str {
                "workspace.failing"
            }
        }

        #[async_trait]
        impl Workspace for FailingWorkspace {
            async fn read_normalized(
                &self,
                _normalized_path: &str,
            ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
                Err(WorkspaceError::Backend("backend exploded".to_string()))
            }

            async fn write_normalized(
                &self,
                _normalized_path: &str,
                _data: &[u8],
                _options: WorkspaceWriteOptions,
            ) -> Result<WorkspaceWriteResult, WorkspaceError> {
                Err(WorkspaceError::Backend("backend exploded".to_string()))
            }

            async fn list_normalized(
                &self,
                _options: WorkspaceListOptions,
            ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
                Err(WorkspaceError::Backend("backend exploded".to_string()))
            }

            async fn delete_normalized(
                &self,
                _normalized_path: &str,
            ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
                Err(WorkspaceError::Backend("backend exploded".to_string()))
            }
        }

        let bag = ResourceBag::new().with_workspace(Arc::new(FailingWorkspace));
        let err = context::with_resources(Arc::new(bag), async {
            workspace_write(WorkspaceWriteInput {
                path: "artifacts/out.txt".to_string(),
                bytes: b"boom".to_vec(),
            })
            .await
        })
        .await
        .expect_err("backend failure should surface");

        let message = err.to_string();
        assert!(message.contains("std.workspace.write failed"));
        assert!(message.contains(capabilities::workspace::ERR_WORKSPACE_BACKEND));
        assert!(message.contains("backend exploded"));
    }

    #[tokio::test]
    async fn missing_workspace_resource_is_a_node_error() {
        let err = context::with_resources(Arc::new(ResourceBag::new()), async {
            workspace_read(WorkspaceReadInput {
                path: "missing.txt".to_string(),
            })
            .await
        })
        .await
        .expect_err("missing workspace should fail");

        assert!(err.to_string().contains("requires Workspace"));
    }

    #[tokio::test]
    async fn missing_resource_context_is_a_node_error() {
        let err = workspace_read(WorkspaceReadInput {
            path: "missing.txt".to_string(),
        })
        .await
        .expect_err("missing context should fail");

        assert!(
            err.to_string()
                .contains("std.workspace.read missing ResourceAccess context")
        );
    }
}
