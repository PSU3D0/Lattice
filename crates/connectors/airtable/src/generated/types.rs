use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AirtableCreateRecordInput {
    pub base_id: String,
    pub table: String,
    pub fields: JsonValue,
    pub typecast: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AirtableCreateRecordOutput {
    pub id: String,
    pub created_time: Option<String>,
}
