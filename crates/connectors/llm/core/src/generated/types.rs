use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Wire dialect spoken to the lock-selected endpoint.
///
/// The bindings lock selects the endpoint (`llm_default`) and the credential
/// (`llm_api_key`); this enum selects which `llm-provider-*` crate composes
/// the request against that endpoint. It must agree with the base URL the
/// lock resolves (an OpenAI-compatible URL for `openai_compat`, an Anthropic
/// Messages URL for `anthropic`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmProvider {
    #[default]
    OpenaiCompat,
    Anthropic,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmCompleteInput {
    /// Wire dialect; defaults to `openai_compat`.
    #[serde(default)]
    pub provider: LlmProvider,
    /// Provider model identifier (no baked-in default: the caller owns it).
    pub model: String,
    /// The user prompt (single-turn; chat history is a declared gap).
    pub prompt: String,
    /// Optional system instructions.
    #[serde(default)]
    pub system: Option<String>,
    /// Optional sampling temperature.
    #[serde(default)]
    pub temperature: Option<f64>,
    /// Optional output token cap. Recommended for `anthropic`, which requires
    /// `max_tokens` (the provider crate derives a default from known model
    /// names; unknown models without `max_tokens` fail at request build).
    #[serde(default)]
    pub max_tokens: Option<u64>,
    /// Optional JSON Schema for native structured output. When set, the
    /// completion text must parse as JSON and is echoed in `structured`.
    #[serde(default)]
    pub output_schema: Option<JsonValue>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LlmCompleteOutput {
    /// Concatenated assistant text content.
    pub text: String,
    /// Parsed JSON of `text`, present iff `output_schema` was supplied.
    #[serde(default)]
    pub structured: Option<JsonValue>,
    /// The model the completion was requested against (input echo).
    pub model: String,
    pub usage: LlmUsage,
}
