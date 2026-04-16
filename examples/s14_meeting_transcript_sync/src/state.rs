use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::domain::{CompletedMeeting, ConferenceKind, ConferenceLocator, TranscriptSourceRef};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Discovered,
    WaitingForRetry,
    Uploaded,
    NeedsManualReview,
    PermanentFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct MeetingJob {
    pub meeting_key: String,
    pub meeting: CompletedMeeting,
    pub org_scope: String,
    pub conference_kind: ConferenceKind,
    pub locator: ConferenceLocator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<TranscriptSourceRef>,
    pub status: JobStatus,
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uploaded_destination_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
}

impl MeetingJob {
    pub fn new_discovered(org_scope: impl Into<String>, meeting: CompletedMeeting) -> Self {
        Self {
            meeting_key: meeting.meeting_key.clone(),
            meeting,
            org_scope: org_scope.into(),
            conference_kind: ConferenceKind::Unknown,
            locator: ConferenceLocator::default(),
            source_ref: None,
            status: JobStatus::Discovered,
            attempt_count: 0,
            next_retry_at: None,
            uploaded_destination_uri: None,
            source_checksum: None,
            last_error_code: None,
            last_error_message: None,
        }
    }
}
