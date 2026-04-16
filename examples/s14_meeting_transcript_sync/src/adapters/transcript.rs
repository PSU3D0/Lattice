use async_trait::async_trait;

use crate::config::TranscriptSyncConfig;
use crate::domain::{CompletedMeeting, ConferenceLocator, TranscriptArtifact, TranscriptSourceRef};

#[derive(Clone, Debug, PartialEq)]
pub enum SourceResolution {
    Resolved(TranscriptSourceRef),
    NotFoundYet,
    Ambiguous {
        candidates: Vec<TranscriptSourceRef>,
    },
    PermanentFailure {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum FetchOutcome {
    Ready(TranscriptArtifact),
    NotReady,
    PermanentFailure { code: String, message: String },
}

#[async_trait]
pub trait TranscriptSourceResolver: Send + Sync {
    async fn resolve(
        &self,
        meeting: &CompletedMeeting,
        locator: &ConferenceLocator,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<SourceResolution>;
}

#[async_trait]
pub trait TranscriptFetcher: Send + Sync {
    async fn fetch(
        &self,
        source: &TranscriptSourceRef,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<FetchOutcome>;
}
