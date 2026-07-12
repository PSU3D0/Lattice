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
    /// `get_binary` staging denied: the node holds no `resource::workspace::write`
    /// grant, so the `workspace_write()` view is unreachable (§16.4 gate 1 —
    /// the byte-plane analogue of `MissingHttpWrite`).
    #[error(
        "[HTTP110] connector.http.get_binary requires the workspace::write grant (staging denied)"
    )]
    MissingWorkspaceWrite,
    /// Multipart artifact-part deref denied: the node holds no
    /// `resource::workspace::read` grant (§16.4 gate 1).
    #[error(
        "[HTTP111] connector.http multipart requires the workspace::read grant (artifact deref denied)"
    )]
    MissingWorkspaceRead,
    /// `get_binary` 2xx body exceeds the size ceiling (§16.9 Q2). No partial
    /// artifact is staged.
    #[error(
        "[HTTP112] connector.http.get_binary body of {actual} bytes exceeds cap of {cap} bytes"
    )]
    ArtifactTooLarge { cap: u64, actual: u64 },
    /// The byte op ran with no scoped `ResourceAccess` in the task context.
    #[error("connector.http byte op missing ResourceAccess context")]
    MissingResourceContext,
    /// A workspace view (stage/deref) failed one of the deref gates or the
    /// backend rejected the operation (§16.4 gates 2/3).
    #[error("[HTTP113] connector.http workspace access failed: {0}")]
    Workspace(#[from] capabilities::ByteAccessError),
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
            HttpConnectorError::MissingWorkspaceWrite => "HTTP110",
            HttpConnectorError::MissingWorkspaceRead => "HTTP111",
            HttpConnectorError::ArtifactTooLarge { .. } => "HTTP112",
            HttpConnectorError::Workspace(_) => "HTTP113",
            _ => return None,
        })
    }
}
