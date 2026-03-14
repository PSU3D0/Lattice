use capabilities::http::HttpError;

#[derive(Debug, thiserror::Error)]
pub enum ConnectorRuntimeError {
    #[error("connector action requires ResourceAccess context")]
    MissingResourceContext,
    #[error("connector action `{action}` requires HttpRead")]
    MissingHttpRead { action: &'static str },
    #[error("connector action `{action}` requires HttpWrite")]
    MissingHttpWrite { action: &'static str },
    #[error("connector action input must serialize to a JSON object")]
    InvalidInputObject,
    #[error("connector action references missing input field `{field}`")]
    MissingInputField { field: String },
    #[error("connector action field `{field}` must be scalar-compatible for {usage}")]
    InvalidScalarField { field: String, usage: &'static str },
    #[error("connector action path template error: {0}")]
    InvalidPathTemplate(String),
    #[error("connector action returned HTTP {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("connector action response was invalid: {0}")]
    InvalidResponse(String),
    #[error(
        "connector auth profile `{profile}` requires local env override `{env_var}` in Phase B"
    )]
    MissingAuthOverride {
        profile: &'static str,
        env_var: &'static str,
    },
    #[error("connector auth profile `{profile}` uses unsupported Phase-B auth kind `{kind}`")]
    UnsupportedAuthKind {
        profile: &'static str,
        kind: &'static str,
    },
    #[error(transparent)]
    Http(#[from] HttpError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl ConnectorRuntimeError {
    pub fn invalid_response(message: impl Into<String>) -> Self {
        Self::InvalidResponse(message.into())
    }
}
