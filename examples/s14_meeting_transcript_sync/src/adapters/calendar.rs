use async_trait::async_trait;

use crate::config::TranscriptSyncConfig;
use crate::domain::{CompletedMeeting, TranscriptSyncRequest};

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait MeetingSource: Send + Sync {
    async fn fetch_recent_completed_meetings(
        &self,
        request: &TranscriptSyncRequest,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<Vec<CompletedMeeting>>;
}
