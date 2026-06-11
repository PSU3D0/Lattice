use std::collections::BTreeMap;

use crate::errors::SheetPortConnectorError;
use crate::runtime::{
    EffectiveSheetPortModel, export_evaluated_workbook, load_model, merge_eval_policy,
    requires_blob_for_model, resolve_effective_model, to_eval_options,
    workspace_export_enabled_for_model,
};
use crate::types::{
    SheetPortEvaluateInput, SheetPortEvaluateOutput, SheetPortInputPayload, SheetPortOutputPayload,
    SheetPortValue,
};
use formualizer_common::LiteralValue;
use formualizer_sheetport::{
    InputUpdate, OutputSnapshot, PortValue, SheetPortSession, TableRow, TableValue,
};
use serde_json::{Number, Value as JsonValue};

pub struct SheetPortEvaluate;

impl SheetPortEvaluate {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: crate::SHEETPORT_EVALUATE_IDENTIFIER,
        connector_id: crate::CONNECTOR_FAMILY,
        summary: "Evaluate a SheetPort workbook as a typed semantic function",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[],
        // Evaluate materializes the workbook from blob storage before running
        // the semantic function, so the op intrinsically reads blobs.
        effect_hints: &[::dag_core::EffectHint::BlobRead.as_str()],
        roles: &[],
        resolution: ::dag_core::ConnectorResolutionContract {
            supported_modes: &[
                ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
                ::dag_core::ConnectorResolutionModeDecl::LateBoundRefs,
                ::dag_core::ConnectorResolutionModeDecl::InlinePayload,
            ],
            default_mode: ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };

    pub async fn invoke(
        input: &SheetPortEvaluateInput,
    ) -> Result<SheetPortEvaluateOutput, SheetPortConnectorError> {
        let model = resolve_effective_model(Self::META.operation_id, input.model.as_ref()).await?;
        let defaults = match &model {
            EffectiveSheetPortModel::BoundConnection(connection) => {
                connection.config.eval_defaults.as_ref()
            }
            EffectiveSheetPortModel::LateBound { eval_defaults, .. } => eval_defaults.as_ref(),
        };
        let effective_eval = merge_eval_policy(defaults, input.eval.as_ref());
        let _requires_blob = requires_blob_for_model(&model);
        let wants_workspace_export =
            workspace_export_enabled_for_model(&model, input.emit_debug_artifacts);

        if input.emit_debug_artifacts && !wants_workspace_export {
            return Err(SheetPortConnectorError::ArtifactExportNotAllowed);
        }

        let loaded = load_model(&model).await?;
        let manifest_id = loaded.manifest.manifest.id.clone();
        let connection_name = loaded.connection_name.clone();
        let source_workbook_bytes = loaded.source_workbook_bytes.clone();
        let mut session =
            SheetPortSession::new(loaded.workbook, loaded.manifest).map_err(|err| {
                SheetPortConnectorError::WorkbookLoadFailed {
                    reason: err.to_string(),
                }
            })?;
        let update = input_payload_to_update(&input.inputs)?;
        if !update.is_empty() {
            session
                .write_inputs(update)
                .map_err(map_sheetport_error_for_input)?;
        }
        let outputs = session
            .evaluate_once(to_eval_options(&effective_eval))
            .map_err(map_sheetport_error_for_eval)?;
        let debug_artifacts = if wants_workspace_export {
            Some(
                export_evaluated_workbook(
                    &source_workbook_bytes,
                    &mut session,
                    &manifest_id,
                    connection_name.as_deref(),
                )
                .await?,
            )
        } else {
            None
        };

        Ok(SheetPortEvaluateOutput {
            outputs: output_snapshot_to_payload(outputs),
            manifest_id,
            connection_name,
            debug_artifacts,
        })
    }

    pub fn placeholder_output() -> SheetPortEvaluateOutput {
        SheetPortEvaluateOutput {
            outputs: SheetPortOutputPayload::default(),
            manifest_id: "sheetport-skeleton".to_string(),
            connection_name: None,
            debug_artifacts: None,
        }
    }
}

fn input_payload_to_update(
    payload: &SheetPortInputPayload,
) -> Result<InputUpdate, SheetPortConnectorError> {
    let mut update = InputUpdate::new();
    for (port_id, value) in &payload.ports {
        update.insert(port_id.clone(), sheetport_value_to_runtime(value)?);
    }
    Ok(update)
}

fn output_snapshot_to_payload(snapshot: OutputSnapshot) -> SheetPortOutputPayload {
    let mut ports = BTreeMap::new();
    for (port_id, value) in snapshot.into_inner() {
        ports.insert(port_id, runtime_port_value_to_sheetport_value(value));
    }
    SheetPortOutputPayload { ports }
}

fn sheetport_value_to_runtime(
    value: &SheetPortValue,
) -> Result<PortValue, SheetPortConnectorError> {
    Ok(match value {
        SheetPortValue::Scalar { value } => PortValue::Scalar(json_to_literal(value)?),
        SheetPortValue::Record { fields } => PortValue::Record(
            fields
                .iter()
                .map(|(name, value)| Ok((name.clone(), json_to_literal(value)?)))
                .collect::<Result<_, SheetPortConnectorError>>()?,
        ),
        SheetPortValue::Range { rows } => PortValue::Range(
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(json_to_literal)
                        .collect::<Result<Vec<_>, SheetPortConnectorError>>()
                })
                .collect::<Result<Vec<_>, SheetPortConnectorError>>()?,
        ),
        SheetPortValue::Table { rows } => PortValue::Table(TableValue::new(
            rows.iter()
                .map(|row| {
                    let values = row
                        .iter()
                        .map(|(name, value)| Ok((name.clone(), json_to_literal(value)?)))
                        .collect::<Result<BTreeMap<_, _>, SheetPortConnectorError>>()?;
                    Ok(TableRow::new(values))
                })
                .collect::<Result<Vec<_>, SheetPortConnectorError>>()?,
        )),
    })
}

fn runtime_port_value_to_sheetport_value(value: PortValue) -> SheetPortValue {
    match value {
        PortValue::Scalar(value) => SheetPortValue::Scalar {
            value: literal_to_json(&value),
        },
        PortValue::Record(fields) => SheetPortValue::Record {
            fields: fields
                .into_iter()
                .map(|(name, value)| (name, literal_to_json(&value)))
                .collect(),
        },
        PortValue::Range(rows) => SheetPortValue::Range {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| literal_to_json(&value))
                        .collect()
                })
                .collect(),
        },
        PortValue::Table(table) => SheetPortValue::Table {
            rows: table
                .rows
                .into_iter()
                .map(|row| {
                    row.values
                        .into_iter()
                        .map(|(name, value)| (name, literal_to_json(&value)))
                        .collect()
                })
                .collect(),
        },
    }
}

fn json_to_literal(value: &JsonValue) -> Result<LiteralValue, SheetPortConnectorError> {
    match value {
        JsonValue::Null => Ok(LiteralValue::Empty),
        JsonValue::Bool(value) => Ok(LiteralValue::Boolean(*value)),
        JsonValue::String(value) => Ok(LiteralValue::Text(value.clone())),
        JsonValue::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Ok(LiteralValue::Int(integer))
            } else if let Some(number) = value.as_f64() {
                Ok(LiteralValue::Number(number))
            } else {
                Err(SheetPortConnectorError::InvalidConnectionConfig {
                    reason: format!("unsupported JSON number `{value}`"),
                })
            }
        }
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Err(SheetPortConnectorError::InvalidConnectionConfig {
                reason: format!("expected scalar JSON value, got `{value}`"),
            })
        }
    }
}

fn literal_to_json(value: &LiteralValue) -> JsonValue {
    match value {
        LiteralValue::Empty => JsonValue::Null,
        LiteralValue::Boolean(value) => JsonValue::Bool(*value),
        LiteralValue::Int(value) => JsonValue::Number(Number::from(*value)),
        LiteralValue::Number(value) => Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        LiteralValue::Text(value) => JsonValue::String(value.clone()),
        LiteralValue::Date(value) => JsonValue::String(value.to_string()),
        LiteralValue::DateTime(value) => JsonValue::String(value.to_string()),
        LiteralValue::Time(value) => JsonValue::String(value.to_string()),
        LiteralValue::Duration(value) => JsonValue::String(value.to_string()),
        LiteralValue::Pending => JsonValue::String("Pending".to_string()),
        LiteralValue::Error(value) => JsonValue::String(value.to_string()),
        LiteralValue::Array(rows) => JsonValue::Array(
            rows.iter()
                .map(|row| JsonValue::Array(row.iter().map(literal_to_json).collect()))
                .collect(),
        ),
    }
}

fn map_sheetport_error_for_input(
    err: formualizer_sheetport::SheetPortError,
) -> SheetPortConnectorError {
    match err {
        formualizer_sheetport::SheetPortError::ConstraintViolation { violations } => {
            SheetPortConnectorError::InputConstraintViolation {
                reason: format!("{} violation(s): {violations:?}", violations.len()),
            }
        }
        other => SheetPortConnectorError::EvaluationFailed {
            reason: other.to_string(),
        },
    }
}

fn map_sheetport_error_for_eval(
    err: formualizer_sheetport::SheetPortError,
) -> SheetPortConnectorError {
    match err {
        formualizer_sheetport::SheetPortError::ConstraintViolation { violations } => {
            SheetPortConnectorError::InputConstraintViolation {
                reason: format!("{} violation(s): {violations:?}", violations.len()),
            }
        }
        other => SheetPortConnectorError::EvaluationFailed {
            reason: other.to_string(),
        },
    }
}
