use async_trait::async_trait;

use crate::config::TranscriptSyncConfig;
use crate::domain::{CompletedMeeting, TranscriptArtifact, UploadedTranscript};

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TranscriptUploader: Send + Sync {
    async fn upload(
        &self,
        meeting: &CompletedMeeting,
        artifact: &TranscriptArtifact,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<UploadedTranscript>;
}
