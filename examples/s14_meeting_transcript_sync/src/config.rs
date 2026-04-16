use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TranscriptSyncConfig {
    pub org_scope: String,
    pub calendar_ids: Vec<String>,
    pub sync_lookback_minutes: u32,
    pub sync_batch_limit: u32,
    pub transcript_ready_retry_minutes: u32,
    pub max_transcript_attempts: u32,
    pub destination_prefix: String,
    pub gmeet_doc_title_patterns: Vec<String>,
}

impl Default for TranscriptSyncConfig {
    fn default() -> Self {
        Self {
            org_scope: "studio".to_string(),
            calendar_ids: vec!["primary".to_string()],
            sync_lookback_minutes: 30,
            sync_batch_limit: 20,
            transcript_ready_retry_minutes: 5,
            max_transcript_attempts: 6,
            destination_prefix: "r2://meeting-transcripts/transcripts".to_string(),
            gmeet_doc_title_patterns: vec![
                "Transcript".to_string(),
                "Meeting notes".to_string(),
                "Meet transcript".to_string(),
            ],
        }
    }
}
