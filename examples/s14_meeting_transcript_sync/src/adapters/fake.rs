use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::adapters::{
    FetchOutcome, MeetingSource, SourceResolution, TranscriptFetcher, TranscriptJobStore,
    TranscriptSourceResolver, TranscriptUploader,
};
use crate::config::TranscriptSyncConfig;
use crate::domain::{
    CompletedMeeting, ConferenceLocator, TranscriptArtifact, TranscriptSourceRef,
    TranscriptSyncRequest, UploadedTranscript,
};
use crate::state::{JobStatus, MeetingJob};

#[derive(Clone, Default)]
pub struct FakeMeetingSource {
    inner: Arc<Mutex<Vec<CompletedMeeting>>>,
}

impl FakeMeetingSource {
    pub fn new(meetings: Vec<CompletedMeeting>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(meetings)),
        }
    }

    pub fn set_meetings(&self, meetings: Vec<CompletedMeeting>) {
        *self.inner.lock().expect("fake meeting source lock") = meetings;
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MeetingSource for FakeMeetingSource {
    async fn fetch_recent_completed_meetings(
        &self,
        _request: &TranscriptSyncRequest,
        _config: &TranscriptSyncConfig,
    ) -> anyhow::Result<Vec<CompletedMeeting>> {
        Ok(self.inner.lock().expect("fake meeting source lock").clone())
    }
}

#[derive(Clone, Default)]
pub struct FakeTranscriptSourceResolver {
    outcomes: Arc<Mutex<BTreeMap<String, SourceResolution>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeTranscriptSourceResolver {
    pub fn with_outcome(self, meeting_key: impl Into<String>, outcome: SourceResolution) -> Self {
        self.set_outcome(meeting_key, outcome);
        self
    }

    pub fn set_outcome(&self, meeting_key: impl Into<String>, outcome: SourceResolution) {
        self.outcomes
            .lock()
            .expect("fake resolver outcomes lock")
            .insert(meeting_key.into(), outcome);
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("fake resolver calls lock").clone()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptSourceResolver for FakeTranscriptSourceResolver {
    async fn resolve(
        &self,
        meeting: &CompletedMeeting,
        _locator: &ConferenceLocator,
        _config: &TranscriptSyncConfig,
    ) -> anyhow::Result<SourceResolution> {
        self.calls
            .lock()
            .expect("fake resolver calls lock")
            .push(meeting.meeting_key.clone());
        Ok(self
            .outcomes
            .lock()
            .expect("fake resolver outcomes lock")
            .get(&meeting.meeting_key)
            .cloned()
            .unwrap_or(SourceResolution::NotFoundYet))
    }
}

#[derive(Clone, Default)]
pub struct FakeTranscriptFetcher {
    outcomes: Arc<Mutex<BTreeMap<String, FetchOutcome>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeTranscriptFetcher {
    pub fn with_outcome(self, source_key: impl Into<String>, outcome: FetchOutcome) -> Self {
        self.set_outcome(source_key, outcome);
        self
    }

    pub fn set_outcome(&self, source_key: impl Into<String>, outcome: FetchOutcome) {
        self.outcomes
            .lock()
            .expect("fake fetcher outcomes lock")
            .insert(source_key.into(), outcome);
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("fake fetcher calls lock").clone()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptFetcher for FakeTranscriptFetcher {
    async fn fetch(
        &self,
        source: &TranscriptSourceRef,
        _config: &TranscriptSyncConfig,
    ) -> anyhow::Result<FetchOutcome> {
        let key = source.cache_key();
        self.calls
            .lock()
            .expect("fake fetcher calls lock")
            .push(key.clone());
        Ok(self
            .outcomes
            .lock()
            .expect("fake fetcher outcomes lock")
            .get(&key)
            .cloned()
            .unwrap_or(FetchOutcome::NotReady))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UploadBehavior {
    Success(UploadedTranscript),
    Fail { message: String },
}

#[derive(Clone, Default)]
pub struct FakeTranscriptUploader {
    outcomes: Arc<Mutex<BTreeMap<String, UploadBehavior>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeTranscriptUploader {
    pub fn with_success(
        self,
        meeting_key: impl Into<String>,
        uploaded: UploadedTranscript,
    ) -> Self {
        self.set_success(meeting_key, uploaded);
        self
    }

    pub fn with_failure(self, meeting_key: impl Into<String>, message: impl Into<String>) -> Self {
        self.set_failure(meeting_key, message);
        self
    }

    pub fn set_success(&self, meeting_key: impl Into<String>, uploaded: UploadedTranscript) {
        self.outcomes
            .lock()
            .expect("fake uploader outcomes lock")
            .insert(meeting_key.into(), UploadBehavior::Success(uploaded));
    }

    pub fn set_failure(&self, meeting_key: impl Into<String>, message: impl Into<String>) {
        self.outcomes
            .lock()
            .expect("fake uploader outcomes lock")
            .insert(
                meeting_key.into(),
                UploadBehavior::Fail {
                    message: message.into(),
                },
            );
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("fake uploader calls lock").clone()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptUploader for FakeTranscriptUploader {
    async fn upload(
        &self,
        meeting: &CompletedMeeting,
        artifact: &TranscriptArtifact,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<UploadedTranscript> {
        self.calls
            .lock()
            .expect("fake uploader calls lock")
            .push(meeting.meeting_key.clone());

        if let Some(outcome) = self
            .outcomes
            .lock()
            .expect("fake uploader outcomes lock")
            .get(&meeting.meeting_key)
            .cloned()
        {
            return match outcome {
                UploadBehavior::Success(uploaded) => Ok(uploaded),
                UploadBehavior::Fail { message } => Err(anyhow!(message)),
            };
        }

        let prefix = config.destination_prefix.trim_end_matches('/');
        Ok(UploadedTranscript {
            destination_uri: format!("{prefix}/{}/transcript.txt", meeting.meeting_key),
            checksum: format!("len:{}", artifact.text.len()),
            size_bytes: artifact.text.len() as u64,
        })
    }
}

#[derive(Clone, Default)]
pub struct InMemoryTranscriptJobStore {
    jobs: Arc<Mutex<BTreeMap<String, MeetingJob>>>,
}

impl InMemoryTranscriptJobStore {
    pub fn seed(&self, job: MeetingJob) {
        self.jobs
            .lock()
            .expect("in-memory job store lock")
            .insert(job.meeting_key.clone(), job);
    }

    pub fn get(&self, meeting_key: &str) -> Option<MeetingJob> {
        self.jobs
            .lock()
            .expect("in-memory job store lock")
            .get(meeting_key)
            .cloned()
    }

    pub fn all_jobs(&self) -> Vec<MeetingJob> {
        self.jobs
            .lock()
            .expect("in-memory job store lock")
            .values()
            .cloned()
            .collect()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptJobStore for InMemoryTranscriptJobStore {
    async fn upsert_discovered(
        &self,
        org_scope: &str,
        meetings: &[CompletedMeeting],
    ) -> anyhow::Result<()> {
        let mut jobs = self.jobs.lock().expect("in-memory job store lock");
        for meeting in meetings {
            if let Some(existing) = jobs.get_mut(&meeting.meeting_key) {
                existing.meeting = meeting.clone();
                existing.meeting_key = meeting.meeting_key.clone();
                existing.org_scope = org_scope.to_string();
                continue;
            }
            jobs.insert(
                meeting.meeting_key.clone(),
                MeetingJob::new_discovered(org_scope.to_string(), meeting.clone()),
            );
        }
        Ok(())
    }

    async fn due_jobs(
        &self,
        org_scope: &str,
        now_iso: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<MeetingJob>> {
        let now = parse_rfc3339("now_iso", None, now_iso)?;
        let jobs = self
            .jobs
            .lock()
            .expect("in-memory job store lock")
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let mut due = Vec::new();
        for job in jobs {
            if job.org_scope != org_scope {
                continue;
            }
            if is_due(&job, now)? {
                let sort_key = due_sort_key(&job)?;
                due.push((sort_key.due_at, sort_key.scheduled_end_at, job));
            }
        }

        due.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then(left.1.cmp(&right.1))
                .then(left.2.meeting_key.cmp(&right.2.meeting_key))
        });
        due.truncate(limit as usize);
        Ok(due.into_iter().map(|(_, _, job)| job).collect())
    }

    async fn save_job(&self, job: &MeetingJob) -> anyhow::Result<()> {
        self.jobs
            .lock()
            .expect("in-memory job store lock")
            .insert(job.meeting_key.clone(), job.clone());
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct DueSortKey {
    due_at: OffsetDateTime,
    scheduled_end_at: OffsetDateTime,
}

fn is_due(job: &MeetingJob, now: OffsetDateTime) -> anyhow::Result<bool> {
    match job.status {
        JobStatus::Discovered => Ok(true),
        JobStatus::WaitingForRetry => job
            .next_retry_at
            .as_deref()
            .map(|retry_at| {
                parse_rfc3339("next_retry_at", Some(job.meeting_key.as_str()), retry_at)
            })
            .transpose()
            .map(|retry_at| retry_at.map(|retry_at| retry_at <= now).unwrap_or(true)),
        JobStatus::Uploaded | JobStatus::NeedsManualReview | JobStatus::PermanentFailure => {
            Ok(false)
        }
    }
}

fn due_sort_key(job: &MeetingJob) -> anyhow::Result<DueSortKey> {
    let scheduled_end_at = parse_rfc3339(
        "meeting.scheduled_end_at",
        Some(job.meeting_key.as_str()),
        &job.meeting.scheduled_end_at,
    )?;
    let due_at = job
        .next_retry_at
        .as_deref()
        .map(|retry_at| parse_rfc3339("next_retry_at", Some(job.meeting_key.as_str()), retry_at))
        .transpose()?
        .unwrap_or(scheduled_end_at);
    Ok(DueSortKey {
        due_at,
        scheduled_end_at,
    })
}

fn parse_rfc3339(
    field: &str,
    meeting_key: Option<&str>,
    value: &str,
) -> anyhow::Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).with_context(|| match meeting_key {
        Some(meeting_key) => {
            format!("parse RFC3339 {field} for meeting job `{meeting_key}`: `{value}`")
        }
        None => format!("parse RFC3339 {field}: `{value}`"),
    })
}
