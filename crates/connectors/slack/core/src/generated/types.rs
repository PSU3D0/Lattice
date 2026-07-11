use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SlackPostMessageInput {
    pub channel: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocks: Option<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SlackPostMessageOutput {
    pub channel: String,
    pub ts: String,
}
