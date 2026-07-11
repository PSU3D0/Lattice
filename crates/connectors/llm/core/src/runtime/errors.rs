pub use connectors_std::errors::ConnectorRuntimeError;

/// Error surface for `connector.llm` ops.
///
/// Connector-context failures (missing runtime/scope, endpoint/auth
/// resolution) keep the shared [`ConnectorRuntimeError`] shape; provider-side
/// failures surface the `llm-types` completion error verbatim, which includes
/// the fail-closed capability denial ("POST requests require a HTTP write
/// capability") when a node lied about `http_write`.
#[derive(Debug, thiserror::Error)]
pub enum LlmConnectorError {
    #[error(transparent)]
    Runtime(#[from] ConnectorRuntimeError),
    #[error("llm completion failed: {0}")]
    Completion(#[from] llm_types::completion::CompletionError),
    #[error("llm client construction failed: {0}")]
    Client(#[from] llm_types::http_client::Error),
    #[error(
        "connector runtime resolved auth role `llm_api_key` without a bearer authorization header"
    )]
    MissingBearerSecret,
    #[error("invalid `output_schema`: {0}")]
    InvalidOutputSchema(String),
    #[error("invalid provider response: {0}")]
    InvalidResponse(String),
}

impl LlmConnectorError {
    pub fn invalid_response(message: impl Into<String>) -> Self {
        Self::InvalidResponse(message.into())
    }
}
