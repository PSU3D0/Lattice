pub mod auth;
pub mod transcripts;

use capabilities::http::HttpRequest;

pub const ZOOM_CONNECTOR_ID: &str = "connector.zoom";
pub const DEFAULT_ZOOM_API_BASE_URL: &str = "https://api.zoom.us/v2";
pub const DEFAULT_ZOOM_OAUTH_TOKEN_URL: &str = "https://zoom.us/oauth/token";
pub const DEFAULT_ZOOM_HTTP_TIMEOUT_MS: u64 = 10_000;
pub const ZOOM_MEETING_TRANSCRIPT_READ_SCOPE: &str = "cloud_recording:read:meeting_transcript";
pub const ZOOM_MEETING_TRANSCRIPT_READ_ADMIN_SCOPE: &str =
    "cloud_recording:read:meeting_transcript:admin";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZoomRequestConfig {
    pub api_base_url: String,
    pub oauth_token_url: String,
    pub timeout_ms: Option<u64>,
}

impl Default for ZoomRequestConfig {
    fn default() -> Self {
        Self {
            api_base_url: DEFAULT_ZOOM_API_BASE_URL.to_string(),
            oauth_token_url: DEFAULT_ZOOM_OAUTH_TOKEN_URL.to_string(),
            timeout_ms: Some(DEFAULT_ZOOM_HTTP_TIMEOUT_MS),
        }
    }
}

impl ZoomRequestConfig {
    pub fn with_api_base_url(mut self, api_base_url: impl Into<String>) -> Self {
        self.api_base_url = api_base_url.into();
        self
    }

    pub fn with_oauth_token_url(mut self, oauth_token_url: impl Into<String>) -> Self {
        self.oauth_token_url = oauth_token_url.into();
        self
    }

    pub fn with_timeout(mut self, timeout_ms: Option<u64>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

pub(crate) fn apply_timeout(request: &mut HttpRequest, timeout_ms: Option<u64>) {
    request.timeout_ms = timeout_ms;
}

pub(crate) fn join_base_url(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}
