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

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait TranscriptJobStore: Send + Sync {
    async fn upsert_discovered(
        &self,
        org_scope: &str,
        meetings: &[CompletedMeeting],
    ) -> anyhow::Result<()>;

    async fn due_jobs(
        &self,
        org_scope: &str,
        now_iso: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<MeetingJob>>;

    async fn save_job(&self, job: &MeetingJob) -> anyhow::Result<()>;
}
