use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NotionCreatePageInput {
    pub database_id: String,
    pub properties: JsonValue,
    pub icon: Option<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NotionCreatePageOutput {
    pub id: String,
    pub url: Option<String>,
    pub object: Option<String>,
}
