use std::sync::Arc;

use async_trait::async_trait;
use capabilities::ResourceBag;
use capabilities::blob::{BlobStore, MemoryBlobStore};
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
    OutboundAuthProfileDescriptor, ResolvedConnectorConnection, ResolvedEndpointProfile,
};
use capabilities::context;
use capabilities::workspace::{
    Workspace, WorkspaceDeleteResult, WorkspaceEntry, WorkspaceListOptions, WorkspaceReadResult,
    WorkspaceWriteOptions, WorkspaceWriteResult,
};
use connector_formualizer_sheetport::actions::sheetport_evaluate;
use connector_formualizer_sheetport::errors::SheetPortConnectorError;
use connector_formualizer_sheetport::ops::SheetPortEvaluate;
use connector_formualizer_sheetport::types::{
    ManifestSourceRef, SheetPortEvaluateInput, SheetPortInputPayload, SheetPortModelSelector,
    SheetPortValue, WorkbookSourceRef,
};
use formualizer_workbook::{
    JsonAdapter, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Clone)]
struct MockRuntime {
    connection: Option<ResolvedConnectorConnection>,
}

#[derive(Default)]
struct MemoryWorkspace {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    clock: Mutex<u64>,
}

impl capabilities::Capability for MemoryWorkspace {
    fn name(&self) -> &'static str {
        "workspace.memory"
    }
}

#[async_trait]
impl Workspace for MemoryWorkspace {
    async fn read_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, capabilities::workspace::WorkspaceError> {
        Ok(self
            .files
            .lock()
            .expect("workspace lock")
            .get(normalized_path)
            .cloned()
            .map(WorkspaceReadResult::Bytes))
    }

    async fn write_normalized(
        &self,
        normalized_path: &str,
        data: &[u8],
        _options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, capabilities::workspace::WorkspaceError> {
        self.files
            .lock()
            .expect("workspace lock")
            .insert(normalized_path.to_string(), data.to_vec());
        let mut clock = self.clock.lock().expect("clock lock");
        *clock += 1;
        Ok(WorkspaceWriteResult {
            path: normalized_path.to_string(),
            size_bytes: data.len() as u64,
            updated_at_ms: *clock,
        })
    }

    async fn list_normalized(
        &self,
        options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, capabilities::workspace::WorkspaceError> {
        let prefix = options.prefix;
        let files = self.files.lock().expect("workspace lock");
        Ok(files
            .iter()
            .filter(|(path, _)| {
                prefix
                    .as_deref()
                    .map(|prefix| path.starts_with(prefix))
                    .unwrap_or(true)
            })
            .enumerate()
            .map(|(index, (path, bytes))| WorkspaceEntry {
                path: path.clone(),
                size_bytes: bytes.len() as u64,
                updated_at_ms: index as u64,
                content_hash: Some(format!("sha256:{}", bytes.len())),
            })
            .collect())
    }

    async fn delete_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, capabilities::workspace::WorkspaceError> {
        let deleted = self
            .files
            .lock()
            .expect("workspace lock")
            .remove(normalized_path)
            .is_some();
        Ok(WorkspaceDeleteResult { deleted })
    }
}

#[async_trait]
impl ConnectorRuntime for MockRuntime {
    async fn apply_outbound_auth(
        &self,
        _scope: &ConnectorBindingScope,
        _profile: &OutboundAuthProfileDescriptor,
        _request: &mut capabilities::http::HttpRequest,
    ) -> Result<(), ConnectorRuntimeError> {
        unreachable!("auth is not used in this test")
    }

    async fn resolve_endpoint_profile(
        &self,
        _scope: &ConnectorBindingScope,
        _profile: &EndpointProfileDescriptor,
    ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
        unreachable!("endpoint profiles are not used in this test")
    }

    async fn resolve_connection(
        &self,
        _scope: &ConnectorBindingScope,
    ) -> Result<Option<ResolvedConnectorConnection>, ConnectorRuntimeError> {
        Ok(self.connection.clone())
    }
}

fn connector_scope() -> ConnectorBindingScope {
    ConnectorBindingScope::new(
        "flow.sheetport.quote",
        "quote_node",
        "tests::quote_node",
        connector_formualizer_sheetport::CONNECTOR_FAMILY,
    )
}

fn quote_manifest_yaml() -> String {
    r#"spec: fio
spec_version: "0.3.0"
manifest:
  id: quote-model
  name: Quote Model
ports:
  - id: base_price
    dir: in
    shape: scalar
    location: { a1: "Quote!A1" }
    schema: { type: number }
  - id: quantity
    dir: in
    shape: scalar
    location: { a1: "Quote!A2" }
    schema: { type: integer }
  - id: discount
    dir: in
    shape: scalar
    location: { a1: "Quote!A3" }
    schema: { type: number }
  - id: total
    dir: out
    shape: scalar
    location: { a1: "Quote!A4" }
    schema: { type: number }
"#
    .to_string()
}

fn quote_workbook_bytes() -> Vec<u8> {
    let mut book = umya_spreadsheet::new_file();
    let sheet = book
        .get_sheet_by_name_mut("Sheet1")
        .expect("default sheet exists");
    sheet.set_name("Quote");
    sheet.get_cell_mut((1, 4)).set_formula("A1*A2*(1-A3)");

    let mut out = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&book, &mut out).expect("xlsx bytes");
    out
}

async fn blob_store_with_quote_model() -> Arc<MemoryBlobStore> {
    let blob = Arc::new(MemoryBlobStore::default());
    blob.put("models/quote.xlsx", &quote_workbook_bytes())
        .await
        .expect("store workbook bytes");
    blob
}

async fn blob_store_with_materialized_quote_model() -> Arc<MemoryBlobStore> {
    let blob = Arc::new(MemoryBlobStore::default());
    let portable = connector_formualizer_sheetport::runtime::materialize_xlsx_workbook_bytes_to_workbook_json_v1(
        &quote_workbook_bytes(),
    )
    .expect("materialize workbook to portable json");
    blob.put("models/quote.materialized.json", &portable)
        .await
        .expect("store materialized workbook bytes");
    blob
}

#[test]
fn workbook_json_materializer_emits_json_adapter_representation() {
    let bytes = connector_formualizer_sheetport::runtime::materialize_xlsx_workbook_bytes_to_workbook_json_v1(
        &quote_workbook_bytes(),
    )
    .expect("materialize workbook json");
    let mut adapter = JsonAdapter::open_bytes(bytes).expect("open materialized json workbook");
    let sheet = adapter.read_sheet("Quote").expect("read quote sheet");
    assert_eq!(sheet.dimensions, Some((4, 1)));
    assert_eq!(
        sheet
            .cells
            .get(&(4, 1))
            .and_then(|cell| cell.formula.as_deref()),
        Some("=A1*A2*(1-A3)")
    );
}

fn quote_input() -> SheetPortEvaluateInput {
    quote_input_with_export(false)
}

fn quote_input_with_export(emit_debug_artifacts: bool) -> SheetPortEvaluateInput {
    SheetPortEvaluateInput {
        model: None,
        inputs: SheetPortInputPayload {
            ports: [
                (
                    "base_price".to_string(),
                    SheetPortValue::Scalar {
                        value: json!(100.0),
                    },
                ),
                (
                    "quantity".to_string(),
                    SheetPortValue::Scalar { value: json!(2) },
                ),
                (
                    "discount".to_string(),
                    SheetPortValue::Scalar { value: json!(0.1) },
                ),
            ]
            .into_iter()
            .collect(),
        },
        eval: None,
        emit_debug_artifacts,
    }
}

fn quote_total(output: &connector_formualizer_sheetport::types::SheetPortEvaluateOutput) -> f64 {
    let value = match output.outputs.ports.get("total") {
        Some(SheetPortValue::Scalar { value }) => value,
        other => panic!("expected scalar total output, got {other:?}"),
    };
    as_f64(value).expect("numeric total")
}

fn as_f64(value: &JsonValue) -> Option<f64> {
    match value {
        JsonValue::Number(value) => value.as_f64(),
        _ => None,
    }
}

#[tokio::test]
async fn bound_mode_executes_quote_model_and_matches_canonical_node() {
    let blob = blob_store_with_quote_model().await;
    let runtime = MockRuntime {
        connection: Some(ResolvedConnectorConnection {
            connection_name: Some("pricing_model_v7".to_string()),
            connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
            config: json!({
                "workbook_source": {
                    "kind": "blob",
                    "key": "models/quote.xlsx"
                },
                "manifest_source": {
                    "kind": "inline_yaml",
                    "value": quote_manifest_yaml()
                },
                "eval_defaults": {
                    "freeze_volatile": true,
                    "rng_seed": 7
                },
                "artifact_policy": {
                    "allow_workspace_export": false
                }
            }),
        }),
    };

    let bag = ResourceBag::new()
        .with_blob(blob)
        .with_connector_runtime(Arc::new(runtime))
        .with_connector_scope(connector_scope());

    context::with_resources(Arc::new(bag), async {
        let internal = SheetPortEvaluate::invoke(&quote_input())
            .await
            .expect("internal op succeeds");
        let canonical = sheetport_evaluate(quote_input())
            .await
            .expect("canonical node succeeds");

        assert_eq!(internal.manifest_id, "quote-model");
        assert_eq!(
            internal.connection_name.as_deref(),
            Some("pricing_model_v7")
        );
        assert!((quote_total(&internal) - 180.0).abs() < 0.0001);
        assert_eq!(internal, canonical);
    })
    .await;
}

#[tokio::test]
async fn materialized_blob_mode_executes_quote_model() {
    let blob = blob_store_with_materialized_quote_model().await;
    let input = SheetPortEvaluateInput {
        model: Some(SheetPortModelSelector::LateBoundSources {
            workbook_source: WorkbookSourceRef::MaterializedBlob {
                key: "models/quote.materialized.json".to_string(),
                format:
                    connector_formualizer_sheetport::types::MaterializedWorkbookFormat::WorkbookJsonV1,
            },
            manifest_source: ManifestSourceRef::InlineYaml {
                value: quote_manifest_yaml(),
            },
            eval_defaults: None,
        }),
        ..quote_input()
    };
    let bag = ResourceBag::new().with_blob(blob);

    let output = context::with_resources(Arc::new(bag), async {
        SheetPortEvaluate::invoke(&input)
            .await
            .expect("materialized blob mode should execute")
    })
    .await;

    assert_eq!(quote_total(&output), 180.0);
    assert_eq!(output.manifest_id, "quote-model");
    assert_eq!(output.connection_name, None);
}

#[tokio::test]
async fn late_bound_mode_executes_without_connector_runtime() {
    let blob = blob_store_with_quote_model().await;
    let bag = ResourceBag::new().with_blob(blob);

    let input = SheetPortEvaluateInput {
        model: Some(SheetPortModelSelector::LateBoundSources {
            workbook_source: WorkbookSourceRef::Blob {
                key: "models/quote.xlsx".to_string(),
            },
            manifest_source: ManifestSourceRef::InlineYaml {
                value: quote_manifest_yaml(),
            },
            eval_defaults: None,
        }),
        ..quote_input()
    };

    let output = context::with_resources(Arc::new(bag), async {
        SheetPortEvaluate::invoke(&input)
            .await
            .expect("late-bound execution succeeds")
    })
    .await;

    assert_eq!(output.connection_name, None);
    assert!((quote_total(&output) - 180.0).abs() < 0.0001);
}

#[tokio::test]
async fn late_bound_blob_source_requires_blob_capability() {
    let input = SheetPortEvaluateInput {
        model: Some(SheetPortModelSelector::LateBoundSources {
            workbook_source: WorkbookSourceRef::Blob {
                key: "models/quote.xlsx".to_string(),
            },
            manifest_source: ManifestSourceRef::InlineYaml {
                value: quote_manifest_yaml(),
            },
            eval_defaults: None,
        }),
        ..quote_input()
    };

    let err = context::with_resources(Arc::new(ResourceBag::new()), async {
        SheetPortEvaluate::invoke(&input)
            .await
            .expect_err("missing blob capability should fail")
    })
    .await;

    match err {
        SheetPortConnectorError::MissingBlobCapability => {}
        other => panic!("expected MissingBlobCapability, got {other:?}"),
    }
}

#[tokio::test]
async fn bound_mode_workspace_export_writes_evaluated_workbook_snapshot() {
    let blob = blob_store_with_quote_model().await;
    let workspace = Arc::new(MemoryWorkspace::default());
    let runtime = MockRuntime {
        connection: Some(ResolvedConnectorConnection {
            connection_name: Some("pricing_model_v7".to_string()),
            connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
            config: json!({
                "workbook_source": {
                    "kind": "blob",
                    "key": "models/quote.xlsx"
                },
                "manifest_source": {
                    "kind": "inline_yaml",
                    "value": quote_manifest_yaml()
                },
                "artifact_policy": {
                    "allow_workspace_export": true
                }
            }),
        }),
    };

    let bag = ResourceBag::new()
        .with_blob(blob)
        .with_workspace(Arc::clone(&workspace))
        .with_connector_runtime(Arc::new(runtime))
        .with_connector_scope(connector_scope());

    let output = context::with_resources(Arc::new(bag), async {
        SheetPortEvaluate::invoke(&quote_input_with_export(true))
            .await
            .expect("workspace export succeeds")
    })
    .await;

    let path = output
        .debug_artifacts
        .as_ref()
        .and_then(|artifacts| artifacts.evaluated_workbook_workspace_path.as_deref())
        .expect("workspace artifact path");
    assert_eq!(
        path,
        "artifacts/sheetport/quote-model/pricing_model_v7/evaluated.xlsx"
    );

    let bytes = workspace
        .files
        .lock()
        .expect("workspace lock")
        .get(path)
        .cloned()
        .expect("exported workbook bytes present");
    let adapter = UmyaAdapter::open_bytes(bytes).expect("reopen exported workbook");
    let mut workbook = Workbook::from_reader(
        adapter,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load exported workbook");
    workbook
        .evaluate_all()
        .expect("re-evaluate exported workbook");
    let total = workbook
        .get_value("Quote", 4, 1)
        .expect("total cell value present");
    match total {
        formualizer_common::LiteralValue::Number(value) => {
            assert!((value - 180.0).abs() < 0.0001);
        }
        formualizer_common::LiteralValue::Text(value) => {
            assert_eq!(value, "180");
        }
        other => panic!("expected numeric-ish total in exported workbook, got {other:?}"),
    }
}

#[tokio::test]
async fn bound_mode_workspace_export_requires_workspace_when_requested() {
    let blob = blob_store_with_quote_model().await;
    let runtime = MockRuntime {
        connection: Some(ResolvedConnectorConnection {
            connection_name: Some("pricing_model_v7".to_string()),
            connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
            config: json!({
                "workbook_source": {
                    "kind": "blob",
                    "key": "models/quote.xlsx"
                },
                "manifest_source": {
                    "kind": "inline_yaml",
                    "value": quote_manifest_yaml()
                },
                "artifact_policy": {
                    "allow_workspace_export": true
                }
            }),
        }),
    };
    let bag = ResourceBag::new()
        .with_blob(blob)
        .with_connector_runtime(Arc::new(runtime))
        .with_connector_scope(connector_scope());

    let err = context::with_resources(Arc::new(bag), async {
        SheetPortEvaluate::invoke(&quote_input_with_export(true))
            .await
            .expect_err("missing workspace should fail")
    })
    .await;

    match err {
        SheetPortConnectorError::MissingWorkspaceCapability => {}
        other => panic!("expected MissingWorkspaceCapability, got {other:?}"),
    }
}

#[tokio::test]
async fn bound_mode_workspace_export_respects_connection_policy() {
    let blob = blob_store_with_quote_model().await;
    let workspace = Arc::new(MemoryWorkspace::default());
    let runtime = MockRuntime {
        connection: Some(ResolvedConnectorConnection {
            connection_name: Some("pricing_model_v7".to_string()),
            connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
            config: json!({
                "workbook_source": {
                    "kind": "blob",
                    "key": "models/quote.xlsx"
                },
                "manifest_source": {
                    "kind": "inline_yaml",
                    "value": quote_manifest_yaml()
                },
                "artifact_policy": {
                    "allow_workspace_export": false
                }
            }),
        }),
    };

    let bag = ResourceBag::new()
        .with_blob(blob)
        .with_workspace(workspace)
        .with_connector_runtime(Arc::new(runtime))
        .with_connector_scope(connector_scope());

    let err = context::with_resources(Arc::new(bag), async {
        SheetPortEvaluate::invoke(&quote_input_with_export(true))
            .await
            .expect_err("disallowed export should fail")
    })
    .await;

    match err {
        SheetPortConnectorError::ArtifactExportNotAllowed => {}
        other => panic!("expected ArtifactExportNotAllowed, got {other:?}"),
    }
}

#[tokio::test]
async fn bound_mode_reports_missing_blob_object() {
    let runtime = MockRuntime {
        connection: Some(ResolvedConnectorConnection {
            connection_name: Some("missing_model".to_string()),
            connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
            config: json!({
                "workbook_source": {
                    "kind": "blob",
                    "key": "models/missing.xlsx"
                },
                "manifest_source": {
                    "kind": "inline_yaml",
                    "value": quote_manifest_yaml()
                }
            }),
        }),
    };
    let bag = ResourceBag::new()
        .with_blob(Arc::new(MemoryBlobStore::default()))
        .with_connector_runtime(Arc::new(runtime))
        .with_connector_scope(connector_scope());

    let err = context::with_resources(Arc::new(bag), async {
        SheetPortEvaluate::invoke(&quote_input())
            .await
            .expect_err("missing workbook blob should fail")
    })
    .await;

    match err {
        SheetPortConnectorError::SourceObjectMissing { key } => {
            assert_eq!(key, "models/missing.xlsx");
        }
        other => panic!("expected SourceObjectMissing, got {other:?}"),
    }
}
