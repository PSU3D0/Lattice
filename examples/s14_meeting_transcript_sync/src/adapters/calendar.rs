use async_trait::async_trait;

use crate::config::TranscriptSyncConfig;
use crate::domain::{CompletedMeeting, TranscriptSyncRequest};

#[async_trait]
pub trait MeetingSource: Send + Sync {
    async fn fetch_recent_completed_meetings(
        &self,
        request: &TranscriptSyncRequest,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<Vec<CompletedMeeting>>;
}
