pub mod calendar;
pub mod fake;
pub mod transcript;
pub mod upload;

use async_trait::async_trait;

use crate::domain::CompletedMeeting;
use crate::state::MeetingJob;

pub use calendar::MeetingSource;
pub use transcript::{FetchOutcome, SourceResolution, TranscriptFetcher, TranscriptSourceResolver};
pub use upload::TranscriptUploader;

#[async_trait]
pub trait TranscriptJobStore: Send + Sync {
    async fn upsert_discovered(
        &self,
        org_scope: &str,
        meetings: &[CompletedMeeting],
    ) -> anyhow::Result<()>;

    async fn due_jobs(&self, now_iso: &str, limit: u32) -> anyhow::Result<Vec<MeetingJob>>;

    async fn save_job(&self, job: &MeetingJob) -> anyhow::Result<()>;
}
