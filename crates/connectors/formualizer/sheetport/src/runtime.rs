use crate::errors::SheetPortConnectorError;
use crate::types::{
    ManifestSourceRef, MaterializedWorkbookFormat, SheetPortArtifactRefs,
    SheetPortConnectionConfig, SheetPortEvalDefaults, SheetPortEvalOverride,
    SheetPortModelSelector, WorkbookSourceRef,
};
use capabilities::context;
#[cfg(feature = "native-xlsx")]
use capabilities::workspace::WorkspaceWriteOptions;
#[cfg(feature = "native-xlsx")]
use formualizer_sheetport::{BoundPort, FieldLocation, InputSnapshot, PortBinding, ScalarLocation};
use formualizer_sheetport::{EvalOptions, SheetPortSession};
use formualizer_workbook::Workbook;
#[cfg(feature = "native-xlsx")]
use formualizer_workbook::{
    CellData, FormulaCacheUpdate, JsonAdapter, LoadStrategy, SpreadsheetReader, SpreadsheetWriter,
    UmyaAdapter, WorkbookConfig,
};
#[cfg(not(feature = "native-xlsx"))]
use formualizer_workbook::{JsonAdapter, LoadStrategy, SpreadsheetReader, WorkbookConfig};
#[cfg(feature = "native-xlsx")]
use sheetport_spec::Direction;
use sheetport_spec::Manifest;

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedSheetPortConnection {
    pub connection_name: Option<String>,
    pub config: SheetPortConnectionConfig,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ResolvedSheetPortEvalPolicy {
    pub freeze_volatile: Option<bool>,
    pub rng_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EffectiveSheetPortModel {
    BoundConnection(ResolvedSheetPortConnection),
    LateBound {
        workbook_source: WorkbookSourceRef,
        manifest_source: ManifestSourceRef,
        eval_defaults: Option<SheetPortEvalDefaults>,
    },
}

pub struct LoadedSheetPortModel {
    pub connection_name: Option<String>,
    pub workbook: Workbook,
    pub manifest: Manifest,
    pub source_workbook_bytes: Vec<u8>,
}

pub fn requires_blob(config: &SheetPortConnectionConfig) -> bool {
    matches!(
        config.workbook_source,
        WorkbookSourceRef::Blob { .. } | WorkbookSourceRef::MaterializedBlob { .. }
    ) || matches!(config.manifest_source, ManifestSourceRef::Blob { .. })
}

pub fn requires_blob_for_model(model: &EffectiveSheetPortModel) -> bool {
    match model {
        EffectiveSheetPortModel::BoundConnection(connection) => requires_blob(&connection.config),
        EffectiveSheetPortModel::LateBound {
            workbook_source,
            manifest_source,
            ..
        } => {
            matches!(
                workbook_source,
                WorkbookSourceRef::Blob { .. } | WorkbookSourceRef::MaterializedBlob { .. }
            ) || matches!(manifest_source, ManifestSourceRef::Blob { .. })
        }
    }
}

pub fn workspace_export_enabled(config: &SheetPortConnectionConfig, requested: bool) -> bool {
    requested
        && config
            .artifact_policy
            .as_ref()
            .map(|policy| policy.allow_workspace_export)
            .unwrap_or(false)
}

pub fn workspace_export_enabled_for_model(
    model: &EffectiveSheetPortModel,
    requested: bool,
) -> bool {
    match model {
        EffectiveSheetPortModel::BoundConnection(connection) => {
            workspace_export_enabled(&connection.config, requested)
        }
        EffectiveSheetPortModel::LateBound { .. } => false,
    }
}

pub fn merge_eval_policy(
    defaults: Option<&SheetPortEvalDefaults>,
    override_: Option<&SheetPortEvalOverride>,
) -> ResolvedSheetPortEvalPolicy {
    ResolvedSheetPortEvalPolicy {
        freeze_volatile: override_
            .and_then(|value| value.freeze_volatile)
            .or_else(|| defaults.and_then(|value| value.freeze_volatile)),
        rng_seed: override_
            .and_then(|value| value.rng_seed)
            .or_else(|| defaults.and_then(|value| value.rng_seed)),
    }
}

pub async fn resolve_current_connection(
    action: &'static str,
) -> Result<ResolvedSheetPortConnection, SheetPortConnectorError> {
    context::with_current_async(|resources| async move {
        let runtime = resources
            .connector_runtime()
            .ok_or(SheetPortConnectorError::MissingConnectorRuntime { action })?;
        let scope = resources
            .connector_scope()
            .ok_or(SheetPortConnectorError::MissingConnectorScope { action })?;
        let resolved = runtime
            .resolve_connection(&scope)
            .await
            .map_err(|err| SheetPortConnectorError::InvalidConnectionConfig {
                reason: err.to_string(),
            })?
            .ok_or(SheetPortConnectorError::MissingConnectionBinding { action })?;

        if resolved.connector_id != scope.connector_id {
            return Err(SheetPortConnectorError::ConnectorMismatch {
                action,
                actual: resolved.connector_id,
            });
        }

        let config: SheetPortConnectionConfig =
            serde_json::from_value(resolved.config).map_err(|err| {
                SheetPortConnectorError::InvalidConnectionConfig {
                    reason: err.to_string(),
                }
            })?;

        Ok(ResolvedSheetPortConnection {
            connection_name: resolved.connection_name,
            config,
        })
    })
    .await
    .ok_or(SheetPortConnectorError::MissingResourceContext)?
}

pub async fn resolve_effective_model(
    action: &'static str,
    selector: Option<&SheetPortModelSelector>,
) -> Result<EffectiveSheetPortModel, SheetPortConnectorError> {
    match selector {
        Some(SheetPortModelSelector::LateBoundSources {
            workbook_source,
            manifest_source,
            eval_defaults,
        }) => Ok(EffectiveSheetPortModel::LateBound {
            workbook_source: workbook_source.clone(),
            manifest_source: manifest_source.clone(),
            eval_defaults: eval_defaults.clone(),
        }),
        Some(SheetPortModelSelector::BoundConnection) | None => resolve_current_connection(action)
            .await
            .map(EffectiveSheetPortModel::BoundConnection),
    }
}

pub async fn load_model(
    model: &EffectiveSheetPortModel,
) -> Result<LoadedSheetPortModel, SheetPortConnectorError> {
    let (connection_name, workbook_source, manifest_source) = match model {
        EffectiveSheetPortModel::BoundConnection(connection) => (
            connection.connection_name.clone(),
            &connection.config.workbook_source,
            &connection.config.manifest_source,
        ),
        EffectiveSheetPortModel::LateBound {
            workbook_source,
            manifest_source,
            ..
        } => (None, workbook_source, manifest_source),
    };

    let workbook_bytes = load_workbook_bytes(workbook_source).await?;
    let manifest_text = load_manifest_text(manifest_source).await?;
    let manifest = Manifest::from_yaml_str(&manifest_text).map_err(|err| {
        SheetPortConnectorError::ManifestInvalid {
            reason: err.to_string(),
        }
    })?;
    manifest
        .validate()
        .map_err(|err| SheetPortConnectorError::ManifestInvalid {
            reason: err.to_string(),
        })?;

    let workbook = load_workbook_from_source(workbook_source, &workbook_bytes)?;

    Ok(LoadedSheetPortModel {
        connection_name,
        workbook,
        manifest,
        source_workbook_bytes: workbook_bytes,
    })
}

pub fn to_eval_options(policy: &ResolvedSheetPortEvalPolicy) -> EvalOptions {
    EvalOptions {
        freeze_volatile: policy.freeze_volatile.unwrap_or(false),
        rng_seed: policy.rng_seed,
        ..EvalOptions::default()
    }
}

fn load_workbook_from_source(
    source: &WorkbookSourceRef,
    workbook_bytes: &[u8],
) -> Result<Workbook, SheetPortConnectorError> {
    match source {
        WorkbookSourceRef::MaterializedBlob { format, .. } => {
            load_materialized_workbook(format, workbook_bytes)
        }
        WorkbookSourceRef::Blob { .. } | WorkbookSourceRef::FilePath { .. } => {
            load_xlsx_workbook(workbook_bytes)
        }
    }
}

fn load_materialized_workbook(
    format: &MaterializedWorkbookFormat,
    workbook_bytes: &[u8],
) -> Result<Workbook, SheetPortConnectorError> {
    match format {
        MaterializedWorkbookFormat::WorkbookJsonV1 => {
            let adapter = JsonAdapter::open_bytes(workbook_bytes.to_vec()).map_err(|err| {
                SheetPortConnectorError::WorkbookLoadFailed {
                    reason: format!(
                        "failed to parse materialized workbook payload as workbook_json_v1: {err}"
                    ),
                }
            })?;
            Workbook::from_reader(
                adapter,
                LoadStrategy::EagerAll,
                WorkbookConfig::interactive(),
            )
            .map_err(|err| SheetPortConnectorError::WorkbookLoadFailed {
                reason: err.to_string(),
            })
        }
    }
}

fn load_xlsx_workbook(workbook_bytes: &[u8]) -> Result<Workbook, SheetPortConnectorError> {
    #[cfg(feature = "native-xlsx")]
    {
        let adapter = UmyaAdapter::open_bytes(workbook_bytes.to_vec()).map_err(|err| {
            SheetPortConnectorError::WorkbookLoadFailed {
                reason: err.to_string(),
            }
        })?;
        Workbook::from_reader(
            adapter,
            LoadStrategy::EagerAll,
            WorkbookConfig::interactive(),
        )
        .map_err(|err| SheetPortConnectorError::WorkbookLoadFailed {
            reason: err.to_string(),
        })
    }
    #[cfg(not(feature = "native-xlsx"))]
    {
        let _ = workbook_bytes;
        Err(SheetPortConnectorError::WorkbookLoadFailed {
            reason: "xlsx workbook loading requires the `native-xlsx` feature; portable guest execution should use a host-materialized workbook path instead".to_string(),
        })
    }
}

/// Materialize native XLSX workbook bytes into the existing
/// `formualizer-workbook` JSON backend representation (`workbook_json_v1`).
#[cfg(feature = "native-xlsx")]
pub fn materialize_xlsx_workbook_bytes_to_workbook_json_v1(
    source_workbook_bytes: &[u8],
) -> Result<Vec<u8>, SheetPortConnectorError> {
    let mut xlsx = UmyaAdapter::open_bytes(source_workbook_bytes.to_vec()).map_err(|err| {
        SheetPortConnectorError::WorkbookLoadFailed {
            reason: err.to_string(),
        }
    })?;
    serialize_xlsx_reader_to_workbook_json_v1(&mut xlsx)
}

#[cfg(feature = "native-xlsx")]
fn serialize_xlsx_reader_to_workbook_json_v1(
    reader: &mut UmyaAdapter,
) -> Result<Vec<u8>, SheetPortConnectorError> {
    let mut adapter = JsonAdapter::new();
    let sheet_names =
        reader
            .sheet_names()
            .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                reason: format!(
                    "failed to enumerate workbook sheets for json materialization: {err}"
                ),
            })?;
    let defined_names =
        reader
            .defined_names()
            .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                reason: format!(
                    "failed to read workbook defined names for json materialization: {err}"
                ),
            })?;

    for sheet_name in sheet_names {
        adapter.create_sheet(&sheet_name).map_err(|err| {
            SheetPortConnectorError::ArtifactExportFailed {
                reason: format!("failed to create json sheet `{sheet_name}`: {err}"),
            }
        })?;

        let sheet = reader.read_sheet(&sheet_name).map_err(|err| {
            SheetPortConnectorError::ArtifactExportFailed {
                reason: format!(
                    "failed to read workbook sheet `{sheet_name}` for json materialization: {err}"
                ),
            }
        })?;

        adapter.set_dimensions(&sheet_name, sheet.dimensions);
        adapter.set_date_system_1904(&sheet_name, sheet.date_system_1904);
        adapter.set_merged_cells(&sheet_name, sheet.merged_cells);
        adapter.set_tables(&sheet_name, sheet.tables);
        adapter.set_named_ranges(&sheet_name, sheet.named_ranges);
        adapter.set_hidden(&sheet_name, sheet.hidden);
        adapter.set_row_hidden_manual(&sheet_name, sheet.row_hidden_manual);
        adapter.set_row_hidden_filter(&sheet_name, sheet.row_hidden_filter);
        adapter
            .write_range(&sheet_name, sheet.cells)
            .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                reason: format!(
                    "failed to write json materialized cells for `{sheet_name}`: {err}"
                ),
            })?;
    }

    adapter.set_defined_names(defined_names);
    adapter
        .save_to_bytes()
        .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
            reason: format!("failed to serialize workbook json backend representation: {err}"),
        })
}

async fn load_workbook_bytes(
    source: &WorkbookSourceRef,
) -> Result<Vec<u8>, SheetPortConnectorError> {
    match source {
        WorkbookSourceRef::Blob { key } | WorkbookSourceRef::MaterializedBlob { key, .. } => {
            context::with_current_async(|resources| async move {
                let blob = resources
                    .blob()
                    .ok_or(SheetPortConnectorError::MissingBlobCapability)?;
                blob.get(key)
                    .await
                    .map_err(|err| SheetPortConnectorError::WorkbookLoadFailed {
                        reason: err.to_string(),
                    })?
                    .ok_or_else(|| SheetPortConnectorError::SourceObjectMissing {
                        key: key.clone(),
                    })
            })
            .await
            .ok_or(SheetPortConnectorError::MissingResourceContext)?
        }
        WorkbookSourceRef::FilePath { path } => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                std::fs::read(path).map_err(|err| SheetPortConnectorError::WorkbookLoadFailed {
                    reason: format!("failed to read workbook file `{path}`: {err}"),
                })
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = path;
                Err(SheetPortConnectorError::UnsupportedSourceKind {
                    kind: "file_path".to_string(),
                })
            }
        }
    }
}

pub async fn export_evaluated_workbook(
    source_workbook_bytes: &[u8],
    session: &mut SheetPortSession,
    manifest_id: &str,
    connection_name: Option<&str>,
) -> Result<SheetPortArtifactRefs, SheetPortConnectorError> {
    #[cfg(feature = "native-xlsx")]
    {
        let path = export_workspace_path(manifest_id, connection_name);
        let mut adapter =
            UmyaAdapter::open_bytes(source_workbook_bytes.to_vec()).map_err(|err| {
                SheetPortConnectorError::ArtifactExportFailed {
                    reason: format!("failed to reopen source workbook bytes: {err}"),
                }
            })?;

        let inputs =
            session
                .read_inputs()
                .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                    reason: format!("failed to snapshot current sheetport inputs: {err}"),
                })?;
        apply_inputs_to_adapter(&mut adapter, session.bindings(), &inputs)?;
        write_formula_caches_from_workbook(&mut adapter, session.workbook())?;
        let bytes = adapter.save_to_bytes().map_err(|err| {
            SheetPortConnectorError::ArtifactExportFailed {
                reason: format!("failed to serialize evaluated workbook: {err}"),
            }
        })?;

        let result = context::with_current_async(|resources| async move {
            let workspace = resources
                .workspace()
                .ok_or(SheetPortConnectorError::MissingWorkspaceCapability)?;
            workspace
                .write(&path, &bytes, WorkspaceWriteOptions::default())
                .await
                .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                    reason: err.to_string(),
                })
        })
        .await
        .ok_or(SheetPortConnectorError::MissingResourceContext)??;

        Ok(SheetPortArtifactRefs {
            evaluated_workbook_workspace_path: Some(result.path),
        })
    }
    #[cfg(not(feature = "native-xlsx"))]
    {
        let _ = (source_workbook_bytes, session, manifest_id, connection_name);
        Err(SheetPortConnectorError::ArtifactExportUnsupported {
            reason: "evaluated workbook XLSX export requires the `native-xlsx` feature and is intended for host-native execution".to_string(),
        })
    }
}

#[cfg(feature = "native-xlsx")]
fn export_workspace_path(manifest_id: &str, connection_name: Option<&str>) -> String {
    let scope_suffix = context::with_current(|resources| {
        resources
            .connector_scope()
            .map(|scope| scope.node_alias)
            .unwrap_or_else(|| "node".to_string())
    })
    .unwrap_or_else(|| "node".to_string());
    let connection = connection_name.unwrap_or(&scope_suffix);
    format!(
        "artifacts/sheetport/{}/{}/evaluated.xlsx",
        sanitize_segment(manifest_id),
        sanitize_segment(connection)
    )
}

#[cfg(feature = "native-xlsx")]
fn sanitize_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_string()
}

#[cfg(feature = "native-xlsx")]
fn apply_inputs_to_adapter(
    adapter: &mut UmyaAdapter,
    bindings: &[PortBinding],
    inputs: &InputSnapshot,
) -> Result<(), SheetPortConnectorError> {
    for binding in bindings {
        if binding.direction != Direction::In {
            continue;
        }
        let Some(value) = inputs.get(&binding.id) else {
            continue;
        };
        match (&binding.kind, value) {
            (BoundPort::Scalar(scalar), formualizer_sheetport::PortValue::Scalar(value)) => {
                match &scalar.location {
                    ScalarLocation::Cell(addr) => {
                        adapter
                            .write_cell(
                                &addr.sheet,
                                addr.start_row,
                                addr.start_col,
                                CellData::from_value(value.clone()),
                            )
                            .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                                reason: format!(
                                    "failed writing scalar input `{}`: {err}",
                                    binding.id
                                ),
                            })?;
                    }
                    other => {
                        return Err(SheetPortConnectorError::ArtifactExportUnsupported {
                            reason: format!(
                                "input export currently supports only scalar cell bindings; `{}` uses `{other:?}`",
                                binding.id
                            ),
                        });
                    }
                }
            }
            (BoundPort::Record(record), formualizer_sheetport::PortValue::Record(values)) => {
                for (field_name, field_binding) in &record.fields {
                    let Some(value) = values.get(field_name) else {
                        continue;
                    };
                    match &field_binding.location {
                        FieldLocation::Cell(addr) => {
                            adapter
                                .write_cell(
                                    &addr.sheet,
                                    addr.start_row,
                                    addr.start_col,
                                    CellData::from_value(value.clone()),
                                )
                                .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                                    reason: format!(
                                        "failed writing record field `{}.{field_name}`: {err}",
                                        binding.id
                                    ),
                                })?;
                        }
                        other => {
                            return Err(SheetPortConnectorError::ArtifactExportUnsupported {
                                reason: format!(
                                    "record export currently supports only cell field bindings; `{}.{field_name}` uses `{other:?}`",
                                    binding.id
                                ),
                            });
                        }
                    }
                }
            }
            (BoundPort::Range(range), formualizer_sheetport::PortValue::Range(rows)) => {
                match &range.location {
                    formualizer_sheetport::AreaLocation::Range(addr) => {
                        for (row_offset, row) in rows.iter().enumerate() {
                            for (col_offset, value) in row.iter().enumerate() {
                                adapter
                                    .write_cell(
                                        &addr.sheet,
                                        addr.start_row + row_offset as u32,
                                        addr.start_col + col_offset as u32,
                                        CellData::from_value(value.clone()),
                                    )
                                    .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
                                        reason: format!(
                                            "failed writing range input `{}` at offset ({row_offset},{col_offset}): {err}",
                                            binding.id
                                        ),
                                    })?;
                            }
                        }
                    }
                    other => {
                        return Err(SheetPortConnectorError::ArtifactExportUnsupported {
                            reason: format!(
                                "range export currently supports only explicit range bindings; `{}` uses `{other:?}`",
                                binding.id
                            ),
                        });
                    }
                }
            }
            (BoundPort::Table(table), formualizer_sheetport::PortValue::Table(_rows)) => {
                return Err(SheetPortConnectorError::ArtifactExportUnsupported {
                    reason: format!(
                        "table export is not implemented yet for `{}` with location `{:?}`",
                        binding.id, table.location
                    ),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(feature = "native-xlsx")]
fn write_formula_caches_from_workbook(
    adapter: &mut UmyaAdapter,
    workbook: &Workbook,
) -> Result<(), SheetPortConnectorError> {
    let date_system = workbook.eval_config().date_system;
    let updates: Vec<FormulaCacheUpdate> = adapter
        .formula_cells()
        .into_iter()
        .filter_map(|(sheet, row, col)| {
            workbook
                .get_value(&sheet, row, col)
                .map(|value| FormulaCacheUpdate {
                    sheet,
                    row,
                    col,
                    value,
                })
        })
        .collect();

    adapter
        .write_formula_caches_batch(&updates, date_system)
        .map_err(|err| SheetPortConnectorError::ArtifactExportFailed {
            reason: format!("failed writing formula caches: {err}"),
        })?;
    Ok(())
}

async fn load_manifest_text(source: &ManifestSourceRef) -> Result<String, SheetPortConnectorError> {
    match source {
        ManifestSourceRef::InlineYaml { value } => Ok(value.clone()),
        ManifestSourceRef::Blob { key } => {
            let bytes = context::with_current_async(|resources| async move {
                let blob = resources
                    .blob()
                    .ok_or(SheetPortConnectorError::MissingBlobCapability)?;
                blob.get(key)
                    .await
                    .map_err(|err| SheetPortConnectorError::ManifestInvalid {
                        reason: err.to_string(),
                    })?
                    .ok_or_else(|| SheetPortConnectorError::SourceObjectMissing {
                        key: key.clone(),
                    })
            })
            .await
            .ok_or(SheetPortConnectorError::MissingResourceContext)??;
            String::from_utf8(bytes).map_err(|err| SheetPortConnectorError::ManifestInvalid {
                reason: format!("manifest blob `{key}` is not valid UTF-8: {err}"),
            })
        }
        ManifestSourceRef::FilePath { path } => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                std::fs::read_to_string(path).map_err(|err| {
                    SheetPortConnectorError::ManifestInvalid {
                        reason: format!("failed to read manifest file `{path}`: {err}"),
                    }
                })
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = path;
                Err(SheetPortConnectorError::UnsupportedSourceKind {
                    kind: "file_path".to_string(),
                })
            }
        }
    }
}
