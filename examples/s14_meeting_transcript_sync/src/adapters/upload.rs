use async_trait::async_trait;

use crate::config::TranscriptSyncConfig;
use crate::domain::{CompletedMeeting, TranscriptArtifact, UploadedTranscript};

#[async_trait]
pub trait TranscriptUploader: Send + Sync {
    async fn upload(
        &self,
        meeting: &CompletedMeeting,
        artifact: &TranscriptArtifact,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<UploadedTranscript>;
}
