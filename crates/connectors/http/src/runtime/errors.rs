//! connector.http runtime errors, each tagged with its spec §12 diagnostic
//! code so downstream error routing and tests can classify by `[HTTPxxx]`.

pub use connectors_std::errors::ConnectorRuntimeError;

#[derive(Debug, thiserror::Error)]
pub enum HttpConnectorError {
    /// HTTP001 — input `path` malformed (missing `/`, `//`, `..`, backslash,
    /// control chars). Checked pre-send; fail closed.
    #[error("[HTTP001] connector.http path invalid: {0}")]
    PathInvalid(String),
    /// HTTP002 — Tier-1 `target` names a role not bound on the connection.
    #[error("[HTTP002] connector.http target `{0}` names no bound endpoint-profile role")]
    UnknownTarget(String),
    /// HTTP101 — non-2xx response in error-on-status mode.
    #[error("[HTTP101] connector.http non-2xx response: status {status}: {excerpt}")]
    NonSuccessStatus { status: u16, excerpt: String },
    /// HTTP102 — 2xx body not valid JSON in a JSON mode.
    #[error("[HTTP102] connector.http 2xx body was not valid JSON: {0}")]
    BodyNotJson(String),
    /// HTTP103 — 2xx JSON did not match the typed output `T` (serde path).
    #[error("[HTTP103] connector.http 2xx JSON did not match typed output: {0}")]
    TypedMismatch(String),
    /// HTTP104 — 2xx body not valid UTF-8 in text mode.
    #[error("[HTTP104] connector.http 2xx body was not valid UTF-8 in text mode")]
    BodyNotUtf8,
    /// HTTP105 — composed URL origin ≠ granted profile origin (fatal).
    #[error(
        "[HTTP105] connector.http composed URL origin `{composed}` != granted origin `{granted}`"
    )]
    OriginMismatch { composed: String, granted: String },
    /// HTTP106 — forbidden/malformed header name or CR/LF in a header value.
    #[error("[HTTP106] connector.http forbidden or malformed header: {0}")]
    ForbiddenHeader(String),
    /// Tier-2 SSRF guard rejection (https-only + hostname/IP denylist, §10).
    #[error("connector.http SSRF guard rejected any-origin URL: {0}")]
    Ssrf(String),
    #[error(transparent)]
    Runtime(#[from] ConnectorRuntimeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl HttpConnectorError {
    /// The spec §12 diagnostic code this error maps to, when it has one.
    pub fn code(&self) -> Option<&'static str> {
        Some(match self {
            HttpConnectorError::PathInvalid(_) => "HTTP001",
            HttpConnectorError::UnknownTarget(_) => "HTTP002",
            HttpConnectorError::NonSuccessStatus { .. } => "HTTP101",
            HttpConnectorError::BodyNotJson(_) => "HTTP102",
            HttpConnectorError::TypedMismatch(_) => "HTTP103",
            HttpConnectorError::BodyNotUtf8 => "HTTP104",
            HttpConnectorError::OriginMismatch { .. } => "HTTP105",
            HttpConnectorError::ForbiddenHeader(_) => "HTTP106",
            _ => return None,
        })
    }
}
