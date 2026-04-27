use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub fn meeting_key_from_event(calendar_event_id: &str, scheduled_end_at: &str) -> String {
    format!("{calendar_event_id}:{scheduled_end_at}")
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TranscriptSyncRequest {
    pub org_scope: String,
    pub window_start: String,
    pub window_end: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CompletedMeeting {
    pub meeting_key: String,
    pub calendar_event_id: String,
    pub calendar_id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_start_at: Option<String>,
    pub scheduled_end_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organizer_email: Option<String>,
    #[serde(default)]
    pub attendees: Vec<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl CompletedMeeting {
    pub fn new(
        calendar_id: impl Into<String>,
        calendar_event_id: impl Into<String>,
        title: impl Into<String>,
        scheduled_end_at: impl Into<String>,
    ) -> Self {
        let calendar_event_id = calendar_event_id.into();
        let scheduled_end_at = scheduled_end_at.into();
        Self {
            meeting_key: meeting_key_from_event(&calendar_event_id, &scheduled_end_at),
            calendar_event_id,
            calendar_id: calendar_id.into(),
            title: title.into(),
            scheduled_start_at: None,
            scheduled_end_at,
            join_url: None,
            description: None,
            location: None,
            organizer_email: None,
            attendees: Vec::new(),
            metadata: serde_json::Value::Null,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConferenceKind {
    Zoom,
    GoogleMeet,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ConferenceLocator {
    pub kind: ConferenceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zoom_meeting_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub google_meet_code: Option<String>,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl ConferenceLocator {
    pub fn zoom(
        join_url: Option<String>,
        zoom_meeting_id: Option<String>,
        notes: Vec<String>,
    ) -> Self {
        Self {
            kind: ConferenceKind::Zoom,
            join_url,
            zoom_meeting_id,
            google_meet_code: None,
            notes,
        }
    }

    pub fn google_meet(
        join_url: Option<String>,
        google_meet_code: Option<String>,
        notes: Vec<String>,
    ) -> Self {
        Self {
            kind: ConferenceKind::GoogleMeet,
            join_url,
            zoom_meeting_id: None,
            google_meet_code,
            notes,
        }
    }

    pub fn unknown(notes: Vec<String>) -> Self {
        Self {
            kind: ConferenceKind::Unknown,
            join_url: None,
            zoom_meeting_id: None,
            google_meet_code: None,
            notes,
        }
    }
}

impl Default for ConferenceLocator {
    fn default() -> Self {
        Self::unknown(Vec::new())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptSourceKind {
    ZoomTranscript,
    GoogleDocTranscript,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct TranscriptSourceRef {
    pub kind: TranscriptSourceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl TranscriptSourceRef {
    pub fn zoom_transcript(source_id: impl Into<String>, source_uri: impl Into<String>) -> Self {
        Self {
            kind: TranscriptSourceKind::ZoomTranscript,
            source_id: Some(source_id.into()),
            source_uri: Some(source_uri.into()),
            metadata: json!({
                "connector_id": connector_zoom_platform::ZOOM_CONNECTOR_ID,
                "scope_hint": connector_zoom_platform::ZOOM_MEETING_TRANSCRIPT_READ_SCOPE,
            }),
        }
    }

    pub fn google_doc(source_id: impl Into<String>, source_uri: impl Into<String>) -> Self {
        Self {
            kind: TranscriptSourceKind::GoogleDocTranscript,
            source_id: Some(source_id.into()),
            source_uri: Some(source_uri.into()),
            metadata: json!({
                "mime_type": connector_google_platform::drive::GOOGLE_DOCS_DOCUMENT_MIME_TYPE,
                "export_mime_type": connector_google_platform::drive::GOOGLE_DRIVE_EXPORT_TEXT_PLAIN_MIME_TYPE,
            }),
        }
    }

    pub fn cache_key(&self) -> String {
        self.source_id
            .clone()
            .or_else(|| self.source_uri.clone())
            .unwrap_or_else(|| format!("{:?}", self.kind))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct TranscriptArtifact {
    pub text: String,
    #[serde(default)]
    pub normalized: serde_json::Value,
    pub source_ref: TranscriptSourceRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct UploadedTranscript {
    pub destination_uri: String,
    pub checksum: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TranscriptSyncSummary {
    pub discovered: usize,
    pub selected_due_jobs: usize,
    pub processed: usize,
    pub waiting: usize,
    pub uploaded: usize,
    pub manual_review: usize,
    pub permanent_failures: usize,
    pub retryable_errors: usize,
    pub skipped_idempotent: usize,
}
