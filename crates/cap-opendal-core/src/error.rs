use opendal::ErrorKind;

/// Common result type returned by OpenDAL helpers.
pub type Result<T> = std::result::Result<T, OpendalError>;

/// Condensed error type used by downstream capability crates.
#[derive(Debug, thiserror::Error)]
pub enum OpendalError {
    /// Resource was not present in the backing storage.
    #[error("resource not found")]
    NotFound,
    /// Resource already exists in the backing storage.
    #[error("resource already exists")]
    AlreadyExists,
    /// Caller lacks permission to perform the operation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    /// Operation is not supported by the selected backend.
    #[error("operation unsupported: {0}")]
    Unsupported(String),
    /// Backend rejected configuration (typically during operator construction).
    #[error("configuration invalid: {0}")]
    ConfigInvalid(String),
    /// Backend requested the caller to slow down or retry later.
    #[error("backend rate limited: {0}")]
    RateLimited(String),
    /// Conditional request failed (for example, ETag or precondition mismatch).
    #[error("conditional request failed: {0}")]
    ConditionNotMet(String),
    /// Any other backend error surfaced by OpenDAL.
    #[error("{kind:?} backend error: {message}")]
    Backend {
        /// Error kind reported by OpenDAL.
        kind: ErrorKind,
        /// Human-readable message captured from OpenDAL.
        message: String,
    },
}

impl OpendalError {
    /// Convert an `opendal::Error` into an [`OpendalError`].
    pub fn from_opendal(error: opendal::Error) -> Self {
        let message = error.to_string();
        match error.kind() {
            ErrorKind::NotFound => Self::NotFound,
            ErrorKind::AlreadyExists => Self::AlreadyExists,
            ErrorKind::PermissionDenied => Self::PermissionDenied(message),
            ErrorKind::Unsupported => Self::Unsupported(message),
            ErrorKind::ConfigInvalid => Self::ConfigInvalid(message),
            ErrorKind::RateLimited => Self::RateLimited(message),
            ErrorKind::ConditionNotMatch => Self::ConditionNotMet(message),
            kind => Self::Backend { kind, message },
        }
    }
}

impl From<opendal::Error> for OpendalError {
    fn from(value: opendal::Error) -> Self {
        Self::from_opendal(value)
    }
}
