use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SheetPortConnectionConfig {
    pub workbook_source: WorkbookSourceRef,
    pub manifest_source: ManifestSourceRef,
    #[serde(default)]
    pub eval_defaults: Option<SheetPortEvalDefaults>,
    #[serde(default)]
    pub artifact_policy: Option<SheetPortArtifactPolicy>,
}

/// Portable workbook materialization formats.
///
/// `WorkbookJsonV1` refers to the existing `formualizer-workbook` JSON backend
/// representation (`JsonAdapter` schema), used here as a host↔guest relay for
/// evaluation-oriented workbook state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializedWorkbookFormat {
    WorkbookJsonV1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkbookSourceRef {
    Blob {
        key: String,
    },
    MaterializedBlob {
        key: String,
        format: MaterializedWorkbookFormat,
    },
    FilePath {
        path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManifestSourceRef {
    InlineYaml { value: String },
    Blob { key: String },
    FilePath { path: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SheetPortEvalDefaults {
    #[serde(default)]
    pub freeze_volatile: Option<bool>,
    #[serde(default)]
    pub rng_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SheetPortArtifactPolicy {
    #[serde(default)]
    pub allow_workspace_export: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "shape", rename_all = "snake_case")]
pub enum SheetPortValue {
    Scalar {
        value: JsonValue,
    },
    Record {
        fields: BTreeMap<String, JsonValue>,
    },
    Range {
        rows: Vec<Vec<JsonValue>>,
    },
    Table {
        rows: Vec<BTreeMap<String, JsonValue>>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SheetPortInputPayload {
    #[serde(default)]
    pub ports: BTreeMap<String, SheetPortValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SheetPortOutputPayload {
    #[serde(default)]
    pub ports: BTreeMap<String, SheetPortValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SheetPortEvalOverride {
    #[serde(default)]
    pub freeze_volatile: Option<bool>,
    #[serde(default)]
    pub rng_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SheetPortArtifactRefs {
    #[serde(default)]
    pub evaluated_workbook_workspace_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SheetPortModelSelector {
    BoundConnection,
    LateBoundSources {
        workbook_source: WorkbookSourceRef,
        manifest_source: ManifestSourceRef,
        #[serde(default)]
        eval_defaults: Option<SheetPortEvalDefaults>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SheetPortEvaluateInput {
    #[serde(default)]
    pub model: Option<SheetPortModelSelector>,
    pub inputs: SheetPortInputPayload,
    #[serde(default)]
    pub eval: Option<SheetPortEvalOverride>,
    #[serde(default)]
    pub emit_debug_artifacts: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SheetPortEvaluateOutput {
    pub outputs: SheetPortOutputPayload,
    pub manifest_id: String,
    #[serde(default)]
    pub connection_name: Option<String>,
    #[serde(default)]
    pub debug_artifacts: Option<SheetPortArtifactRefs>,
}
