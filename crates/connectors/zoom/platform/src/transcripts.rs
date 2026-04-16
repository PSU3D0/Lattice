use capabilities::http::{HttpMethod, HttpRequest};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::apply_bearer_authorization;
use crate::{
    DEFAULT_ZOOM_API_BASE_URL, DEFAULT_ZOOM_HTTP_TIMEOUT_MS, ZoomRequestConfig, apply_timeout,
    join_base_url,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoomMeetingTranscript {
    pub meeting_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meeting_topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_created_time: Option<String>,
    pub can_download: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_delete: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_delete_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_restriction_reason: Option<ZoomTranscriptDownloadRestrictionReason>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ZoomTranscriptDownloadRestrictionReason {
    DeletedOrTrashed,
    Unsupported,
    NoTranscriptData,
    NotReady,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZoomTranscriptAvailability {
    Ready { download_url: String },
    NotReady,
    Unavailable(ZoomTranscriptUnavailableReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZoomTranscriptUnavailableReason {
    DeletedOrTrashed,
    Unsupported,
    NoTranscriptData,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ZoomTranscriptStateError {
    #[error("Zoom transcript response marked downloadable but omitted download_url")]
    MissingDownloadUrl,
    #[error("Zoom transcript response marked unavailable but omitted download_restriction_reason")]
    MissingDownloadRestrictionReason,
    #[error(
        "Zoom transcript response marked downloadable but included download_restriction_reason"
    )]
    UnexpectedDownloadRestrictionReason,
    #[error("Zoom transcript response marked unavailable but included download_url")]
    UnexpectedDownloadUrl,
}

impl ZoomMeetingTranscript {
    pub fn availability(&self) -> Result<ZoomTranscriptAvailability, ZoomTranscriptStateError> {
        if self.can_download {
            if self.download_restriction_reason.is_some() {
                return Err(ZoomTranscriptStateError::UnexpectedDownloadRestrictionReason);
            }
            let download_url = self
                .download_url
                .clone()
                .ok_or(ZoomTranscriptStateError::MissingDownloadUrl)?;
            return Ok(ZoomTranscriptAvailability::Ready { download_url });
        }

        if self.download_url.is_some() {
            return Err(ZoomTranscriptStateError::UnexpectedDownloadUrl);
        }

        let reason = self
            .download_restriction_reason
            .clone()
            .ok_or(ZoomTranscriptStateError::MissingDownloadRestrictionReason)?;

        match reason {
            ZoomTranscriptDownloadRestrictionReason::NotReady => {
                Ok(ZoomTranscriptAvailability::NotReady)
            }
            ZoomTranscriptDownloadRestrictionReason::DeletedOrTrashed => {
                Ok(ZoomTranscriptAvailability::Unavailable(
                    ZoomTranscriptUnavailableReason::DeletedOrTrashed,
                ))
            }
            ZoomTranscriptDownloadRestrictionReason::Unsupported => {
                Ok(ZoomTranscriptAvailability::Unavailable(
                    ZoomTranscriptUnavailableReason::Unsupported,
                ))
            }
            ZoomTranscriptDownloadRestrictionReason::NoTranscriptData => {
                Ok(ZoomTranscriptAvailability::Unavailable(
                    ZoomTranscriptUnavailableReason::NoTranscriptData,
                ))
            }
        }
    }
}

pub fn encode_meeting_identifier(meeting_id_or_uuid: &str) -> String {
    let encoded = utf8_percent_encode(meeting_id_or_uuid, NON_ALPHANUMERIC).to_string();
    if meeting_id_or_uuid.starts_with('/') || meeting_id_or_uuid.contains("//") {
        utf8_percent_encode(&encoded, NON_ALPHANUMERIC).to_string()
    } else {
        encoded
    }
}

pub fn meeting_transcript_path(meeting_id_or_uuid: &str) -> String {
    format!(
        "/meetings/{}/transcript",
        encode_meeting_identifier(meeting_id_or_uuid)
    )
}

pub fn meeting_transcript_request(meeting_id_or_uuid: &str, access_token: &str) -> HttpRequest {
    meeting_transcript_request_with_parts(
        meeting_id_or_uuid,
        access_token,
        DEFAULT_ZOOM_API_BASE_URL,
        Some(DEFAULT_ZOOM_HTTP_TIMEOUT_MS),
    )
}

pub fn meeting_transcript_request_with_config(
    meeting_id_or_uuid: &str,
    access_token: &str,
    config: &ZoomRequestConfig,
) -> HttpRequest {
    meeting_transcript_request_with_parts(
        meeting_id_or_uuid,
        access_token,
        &config.api_base_url,
        config.timeout_ms,
    )
}

fn meeting_transcript_request_with_parts(
    meeting_id_or_uuid: &str,
    access_token: &str,
    api_base_url: &str,
    timeout_ms: Option<u64>,
) -> HttpRequest {
    let mut request = HttpRequest::new(
        HttpMethod::Get,
        join_base_url(api_base_url, &meeting_transcript_path(meeting_id_or_uuid)),
    );
    apply_timeout(&mut request, timeout_ms);
    request.headers.insert("Accept", "application/json");
    apply_bearer_authorization(&mut request, access_token);
    request
}

pub fn transcript_download_request(download_url: &str, access_token: &str) -> HttpRequest {
    transcript_download_request_with_parts(
        download_url,
        access_token,
        Some(DEFAULT_ZOOM_HTTP_TIMEOUT_MS),
    )
}

pub fn transcript_download_request_with_config(
    download_url: &str,
    access_token: &str,
    config: &ZoomRequestConfig,
) -> HttpRequest {
    transcript_download_request_with_parts(download_url, access_token, config.timeout_ms)
}

fn transcript_download_request_with_parts(
    download_url: &str,
    access_token: &str,
    timeout_ms: Option<u64>,
) -> HttpRequest {
    let mut request = HttpRequest::new(HttpMethod::Get, download_url);
    apply_timeout(&mut request, timeout_ms);
    apply_bearer_authorization(&mut request, access_token);
    request
}

#[cfg(test)]
mod tests {
    use serde_json::from_str;

    use super::*;

    #[test]
    fn test_encode_meeting_identifier_when_zoom_uuid_contains_slashes_double_encodes() {
        assert_eq!(encode_meeting_identifier("123456789"), "123456789");
        assert_eq!(
            encode_meeting_identifier("/ajXp112QmuoKj4854875=="),
            "%252FajXp112QmuoKj4854875%253D%253D"
        );
        assert_eq!(encode_meeting_identifier("abc//def"), "abc%252F%252Fdef");
    }

    #[test]
    fn test_meeting_transcript_request_with_config_when_overridden_uses_custom_base_url_and_timeout()
     {
        let config = ZoomRequestConfig::default()
            .with_api_base_url("https://zoom.example.test/internal/v2/")
            .with_timeout(None);

        let request =
            meeting_transcript_request_with_config("/ajXp112QmuoKj4854875==", "token-123", &config);

        assert_eq!(request.method, HttpMethod::Get);
        assert_eq!(
            request.url,
            "https://zoom.example.test/internal/v2/meetings/%252FajXp112QmuoKj4854875%253D%253D/transcript"
        );
        assert_eq!(request.timeout_ms, None);
        assert_eq!(
            request.headers.get("Accept"),
            Some(&"application/json".to_string())
        );
        assert_eq!(
            request.headers.get("Authorization"),
            Some(&"Bearer token-123".to_string())
        );
    }

    #[test]
    fn test_zoom_transcript_serde_when_ready_payload_matches_zoom_shape_maps_to_ready() {
        let transcript: ZoomMeetingTranscript = from_str(
            r#"{
                "meeting_id": "uaFkQyFCSwya8iNYtkAw3A==",
                "account_id": "Cx3wERazSgup7ZWRHQM8-w",
                "meeting_topic": "My Personal Meeting",
                "host_id": "_0ctZtY0REqWalTmwvrdIw",
                "transcript_created_time": "2025-06-27T13:48:24Z",
                "can_download": true,
                "auto_delete": true,
                "auto_delete_date": "2052-11-07",
                "download_url": "https://example.com/rec/meeting/transcript/download/YDztop0PYLrAQat616a1q1H86RM4jf1Bf3p42a4Ap1jV3bWAJAE.jjixtQU52SEwrsuJ",
                "download_restriction_reason": null
            }"#,
        )
        .expect("Zoom transcript payload should deserialize");

        assert_eq!(
            transcript.availability(),
            Ok(ZoomTranscriptAvailability::Ready {
                download_url: "https://example.com/rec/meeting/transcript/download/YDztop0PYLrAQat616a1q1H86RM4jf1Bf3p42a4Ap1jV3bWAJAE.jjixtQU52SEwrsuJ".to_string(),
            })
        );
    }

    #[test]
    fn test_zoom_transcript_serde_when_not_ready_payload_matches_zoom_shape_maps_to_not_ready() {
        let transcript: ZoomMeetingTranscript = from_str(
            r#"{
                "meeting_id": "uaFkQyFCSwya8iNYtkAw3A==",
                "account_id": "Cx3wERazSgup7ZWRHQM8-w",
                "meeting_topic": "My Personal Meeting",
                "host_id": "_0ctZtY0REqWalTmwvrdIw",
                "transcript_created_time": "2025-06-27T13:48:24Z",
                "can_download": false,
                "auto_delete": true,
                "auto_delete_date": "2052-11-07",
                "download_url": null,
                "download_restriction_reason": "NOT_READY"
            }"#,
        )
        .expect("Zoom transcript payload should deserialize");

        assert_eq!(
            transcript.availability(),
            Ok(ZoomTranscriptAvailability::NotReady)
        );
    }

    #[test]
    fn test_zoom_transcript_availability_when_payload_is_inconsistent_returns_error() {
        let inconsistent = ZoomMeetingTranscript {
            meeting_id: "meeting-1".to_string(),
            account_id: None,
            meeting_topic: None,
            host_id: None,
            transcript_created_time: None,
            can_download: true,
            auto_delete: None,
            auto_delete_date: None,
            download_url: None,
            download_restriction_reason: None,
        };

        assert_eq!(
            inconsistent.availability(),
            Err(ZoomTranscriptStateError::MissingDownloadUrl)
        );
    }
}
