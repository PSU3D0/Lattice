use thiserror::Error;

#[derive(Debug, Error)]
pub enum SheetPortConnectorError {
    #[error("missing resource context for sheetport connector execution")]
    MissingResourceContext,
    #[error("missing connector runtime for sheetport action `{action}`")]
    MissingConnectorRuntime { action: &'static str },
    #[error("missing connector scope for sheetport action `{action}`")]
    MissingConnectorScope { action: &'static str },
    #[error("missing sheetport connection binding for action `{action}`")]
    MissingConnectionBinding { action: &'static str },
    #[error("sheetport connector returned unexpected connector `{actual}` for action `{action}`")]
    ConnectorMismatch {
        action: &'static str,
        actual: String,
    },
    #[error("sheetport connector skeleton: operation `{operation}` not yet implemented")]
    OperationNotImplemented { operation: &'static str },
    #[error("invalid sheetport connection config: {reason}")]
    InvalidConnectionConfig { reason: String },
    #[error("unsupported sheetport source kind `{kind}`")]
    UnsupportedSourceKind { kind: String },
    #[error("missing blob capability for sheetport source resolution")]
    MissingBlobCapability,
    #[error("blob object `{key}` not found")]
    SourceObjectMissing { key: String },
    #[error("sheetport manifest is invalid: {reason}")]
    ManifestInvalid { reason: String },
    #[error("sheetport workbook load failed: {reason}")]
    WorkbookLoadFailed { reason: String },
    #[error("sheetport input constraint violation: {reason}")]
    InputConstraintViolation { reason: String },
    #[error("sheetport evaluation failed: {reason}")]
    EvaluationFailed { reason: String },
    #[error("sheetport artifact export is not allowed by the current connection policy")]
    ArtifactExportNotAllowed,
    #[error("sheetport artifact export is not supported: {reason}")]
    ArtifactExportUnsupported { reason: String },
    #[error("sheetport artifact export failed: {reason}")]
    ArtifactExportFailed { reason: String },
    #[error("workspace export requested but workspace capability is unavailable")]
    MissingWorkspaceCapability,
}
