#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use capabilities::sql::{
    SqlAdmin, SqlBatch, SqlBatchAtomicity, SqlRead, SqlRow, SqlStatement, SqlStatementKind,
    SqlStatementOptions, SqlValue, SqlWrite,
};

use crate::adapters::TranscriptJobStore;
use crate::domain::{CompletedMeeting, ConferenceKind, ConferenceLocator, TranscriptSourceRef};
use crate::state::{JobStatus, MeetingJob};

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS meeting_jobs (
  meeting_key TEXT PRIMARY KEY,
  calendar_event_id TEXT NOT NULL,
  calendar_id TEXT NOT NULL,
  org_scope TEXT NOT NULL,
  meeting_json TEXT NOT NULL,
  scheduled_start_at TEXT,
  scheduled_end_at TEXT NOT NULL,
  conference_kind TEXT NOT NULL,
  conference_locator_json TEXT NOT NULL,
  source_lookup_status TEXT NOT NULL,
  source_ref_json TEXT,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_retry_at TEXT,
  last_attempt_at TEXT,
  uploaded_at TEXT,
  destination_uri TEXT,
  source_artifact_uri TEXT,
  source_checksum TEXT,
  last_error_code TEXT,
  last_error_message TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_meeting_jobs_org_status_retry
  ON meeting_jobs (org_scope, status, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_meeting_jobs_end_time
  ON meeting_jobs (scheduled_end_at);
"#;

const UPSERT_DISCOVERED_SQL: &str = r#"
INSERT INTO meeting_jobs (
  meeting_key,
  calendar_event_id,
  calendar_id,
  org_scope,
  meeting_json,
  scheduled_start_at,
  scheduled_end_at,
  conference_kind,
  conference_locator_json,
  source_lookup_status,
  source_ref_json,
  status,
  attempt_count,
  next_retry_at,
  last_attempt_at,
  uploaded_at,
  destination_uri,
  source_artifact_uri,
  source_checksum,
  last_error_code,
  last_error_message,
  created_at,
  updated_at
) VALUES (
  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
  strftime('%Y-%m-%dT%H:%M:%fZ','now')
)
ON CONFLICT(meeting_key) DO UPDATE SET
  calendar_event_id = excluded.calendar_event_id,
  calendar_id = excluded.calendar_id,
  org_scope = excluded.org_scope,
  meeting_json = excluded.meeting_json,
  scheduled_start_at = excluded.scheduled_start_at,
  scheduled_end_at = excluded.scheduled_end_at,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
"#;

const SAVE_JOB_SQL: &str = r#"
INSERT INTO meeting_jobs (
  meeting_key,
  calendar_event_id,
  calendar_id,
  org_scope,
  meeting_json,
  scheduled_start_at,
  scheduled_end_at,
  conference_kind,
  conference_locator_json,
  source_lookup_status,
  source_ref_json,
  status,
  attempt_count,
  next_retry_at,
  last_attempt_at,
  uploaded_at,
  destination_uri,
  source_artifact_uri,
  source_checksum,
  last_error_code,
  last_error_message,
  created_at,
  updated_at
) VALUES (
  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
  CASE WHEN ? = 'uploaded' THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE NULL END,
  ?, ?, ?, ?, ?,
  strftime('%Y-%m-%dT%H:%M:%fZ','now'),
  strftime('%Y-%m-%dT%H:%M:%fZ','now')
)
ON CONFLICT(meeting_key) DO UPDATE SET
  calendar_event_id = excluded.calendar_event_id,
  calendar_id = excluded.calendar_id,
  org_scope = excluded.org_scope,
  meeting_json = excluded.meeting_json,
  scheduled_start_at = excluded.scheduled_start_at,
  scheduled_end_at = excluded.scheduled_end_at,
  conference_kind = excluded.conference_kind,
  conference_locator_json = excluded.conference_locator_json,
  source_lookup_status = excluded.source_lookup_status,
  source_ref_json = excluded.source_ref_json,
  status = excluded.status,
  attempt_count = excluded.attempt_count,
  next_retry_at = excluded.next_retry_at,
  last_attempt_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
  uploaded_at = CASE
    WHEN excluded.status = 'uploaded' THEN strftime('%Y-%m-%dT%H:%M:%fZ','now')
    ELSE meeting_jobs.uploaded_at
  END,
  destination_uri = excluded.destination_uri,
  source_artifact_uri = excluded.source_artifact_uri,
  source_checksum = excluded.source_checksum,
  last_error_code = excluded.last_error_code,
  last_error_message = excluded.last_error_message,
  updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
"#;

const SELECT_DUE_JOBS_SQL: &str = r#"
SELECT
  meeting_json,
  org_scope,
  conference_kind,
  conference_locator_json,
  source_ref_json,
  status,
  attempt_count,
  next_retry_at,
  destination_uri,
  source_checksum,
  last_error_code,
  last_error_message
FROM meeting_jobs
WHERE org_scope = ?1
  AND (
    status = ?2
    OR (status = ?3 AND (next_retry_at IS NULL OR unixepoch(next_retry_at) <= unixepoch(?4)))
  )
ORDER BY
  COALESCE(unixepoch(next_retry_at), unixepoch(scheduled_end_at)) ASC,
  unixepoch(scheduled_end_at) ASC,
  meeting_key ASC
LIMIT ?5
"#;

#[cfg(test)]
const SELECT_JOB_BY_KEY_SQL: &str = r#"
SELECT
  meeting_json,
  org_scope,
  conference_kind,
  conference_locator_json,
  source_ref_json,
  status,
  attempt_count,
  next_retry_at,
  destination_uri,
  source_checksum,
  last_error_code,
  last_error_message
FROM meeting_jobs
WHERE meeting_key = ?1
"#;

pub struct SqlTranscriptJobStore {
    read: Arc<dyn SqlRead>,
    write: Arc<dyn SqlWrite>,
}

impl SqlTranscriptJobStore {
    pub fn new(read: Arc<dyn SqlRead>, write: Arc<dyn SqlWrite>) -> Self {
        Self { read, write }
    }

    pub async fn new_with_setup(
        read: Arc<dyn SqlRead>,
        write: Arc<dyn SqlWrite>,
        admin: &dyn SqlAdmin,
    ) -> anyhow::Result<Self> {
        Self::setup(admin).await?;
        Ok(Self::new(read, write))
    }

    pub async fn setup(admin: &dyn SqlAdmin) -> anyhow::Result<()> {
        for ddl in SCHEMA_SQL
            .split(';')
            .map(str::trim)
            .filter(|ddl| !ddl.is_empty())
        {
            admin
                .execute_ddl(statement(ddl, Vec::new(), SqlStatementKind::Ddl))
                .await
                .with_context(|| format!("initialize SQL meeting_jobs schema with `{ddl}`"))?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn job(&self, meeting_key: &str) -> anyhow::Result<Option<MeetingJob>> {
        let result = self
            .read
            .query(statement(
                SELECT_JOB_BY_KEY_SQL,
                vec![SqlValue::Text(meeting_key.to_string())],
                SqlStatementKind::Read,
            ))
            .await
            .context("query SQL job by key")?;

        result
            .rows
            .into_iter()
            .next()
            .map(|row| StoredJobRecord::from_sql_row(&row).and_then(StoredJobRecord::into_job))
            .transpose()
    }
}

#[async_trait]
impl TranscriptJobStore for SqlTranscriptJobStore {
    async fn upsert_discovered(
        &self,
        org_scope: &str,
        meetings: &[CompletedMeeting],
    ) -> anyhow::Result<()> {
        let mut statements = Vec::with_capacity(meetings.len());
        for meeting in meetings {
            statements.push(statement(
                UPSERT_DISCOVERED_SQL,
                vec![
                    SqlValue::Text(meeting.meeting_key.clone()),
                    SqlValue::Text(meeting.calendar_event_id.clone()),
                    SqlValue::Text(meeting.calendar_id.clone()),
                    SqlValue::Text(org_scope.to_string()),
                    SqlValue::Text(
                        serde_json::to_string(meeting)
                            .context("serialize completed meeting for SQL")?,
                    ),
                    optional_text(meeting.scheduled_start_at.clone()),
                    SqlValue::Text(meeting.scheduled_end_at.clone()),
                    SqlValue::Text(conference_kind_sql(ConferenceKind::Unknown).to_string()),
                    SqlValue::Text(
                        serde_json::to_string(&ConferenceLocator::default())
                            .context("serialize default conference locator for SQL")?,
                    ),
                    SqlValue::Text(source_lookup_status_for_discovered().to_string()),
                    SqlValue::Null,
                    SqlValue::Text(job_status_sql(JobStatus::Discovered).to_string()),
                ],
                SqlStatementKind::Write,
            ));
        }

        self.write
            .batch(SqlBatch {
                statements,
                atomicity: SqlBatchAtomicity::RequireAtomic,
            })
            .await
            .context("execute SQL upsert_discovered batch")?;
        Ok(())
    }

    async fn due_jobs(
        &self,
        org_scope: &str,
        now_iso: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<MeetingJob>> {
        let result = self
            .read
            .query(statement(
                SELECT_DUE_JOBS_SQL,
                vec![
                    SqlValue::Text(org_scope.to_string()),
                    SqlValue::Text(job_status_sql(JobStatus::Discovered).to_string()),
                    SqlValue::Text(job_status_sql(JobStatus::WaitingForRetry).to_string()),
                    SqlValue::Text(now_iso.to_string()),
                    SqlValue::I64(i64::from(limit)),
                ],
                SqlStatementKind::Read,
            ))
            .await
            .context("query SQL due_jobs")?;

        result
            .rows
            .into_iter()
            .map(|row| StoredJobRecord::from_sql_row(&row).and_then(StoredJobRecord::into_job))
            .collect()
    }

    async fn save_job(&self, job: &MeetingJob) -> anyhow::Result<()> {
        let persistence = PersistedJob::from_job(job)?;
        self.write
            .execute(statement(
                SAVE_JOB_SQL,
                vec![
                    SqlValue::Text(persistence.meeting_key),
                    SqlValue::Text(persistence.calendar_event_id),
                    SqlValue::Text(persistence.calendar_id),
                    SqlValue::Text(persistence.org_scope),
                    SqlValue::Text(persistence.meeting_json),
                    optional_text(persistence.scheduled_start_at),
                    SqlValue::Text(persistence.scheduled_end_at),
                    SqlValue::Text(persistence.conference_kind),
                    SqlValue::Text(persistence.conference_locator_json),
                    SqlValue::Text(persistence.source_lookup_status),
                    optional_text(persistence.source_ref_json),
                    SqlValue::Text(persistence.status.clone()),
                    SqlValue::I64(persistence.attempt_count),
                    optional_text(persistence.next_retry_at),
                    SqlValue::Text(persistence.status),
                    optional_text(persistence.destination_uri),
                    optional_text(persistence.source_artifact_uri),
                    optional_text(persistence.source_checksum),
                    optional_text(persistence.last_error_code),
                    optional_text(persistence.last_error_message),
                ],
                SqlStatementKind::Write,
            ))
            .await
            .context("execute SQL save_job upsert")?;
        Ok(())
    }
}

#[derive(Debug)]
struct PersistedJob {
    meeting_key: String,
    calendar_event_id: String,
    calendar_id: String,
    org_scope: String,
    meeting_json: String,
    scheduled_start_at: Option<String>,
    scheduled_end_at: String,
    conference_kind: String,
    conference_locator_json: String,
    source_lookup_status: String,
    source_ref_json: Option<String>,
    status: String,
    attempt_count: i64,
    next_retry_at: Option<String>,
    destination_uri: Option<String>,
    source_artifact_uri: Option<String>,
    source_checksum: Option<String>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

impl PersistedJob {
    fn from_job(job: &MeetingJob) -> anyhow::Result<Self> {
        let source_ref_json = job
            .source_ref
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .context("serialize transcript source ref")?;
        Ok(Self {
            meeting_key: job.meeting_key.clone(),
            calendar_event_id: job.meeting.calendar_event_id.clone(),
            calendar_id: job.meeting.calendar_id.clone(),
            org_scope: job.org_scope.clone(),
            meeting_json: serde_json::to_string(&job.meeting)
                .context("serialize meeting job meeting payload")?,
            scheduled_start_at: job.meeting.scheduled_start_at.clone(),
            scheduled_end_at: job.meeting.scheduled_end_at.clone(),
            conference_kind: conference_kind_sql(job.conference_kind).to_string(),
            conference_locator_json: serde_json::to_string(&job.locator)
                .context("serialize meeting job conference locator")?,
            source_lookup_status: source_lookup_status_for_job(job).to_string(),
            source_ref_json,
            status: job_status_sql(job.status).to_string(),
            attempt_count: i64::from(job.attempt_count),
            next_retry_at: job.next_retry_at.clone(),
            destination_uri: job.uploaded_destination_uri.clone(),
            source_artifact_uri: job
                .source_ref
                .as_ref()
                .and_then(|source_ref| source_ref.source_uri.clone()),
            source_checksum: job.source_checksum.clone(),
            last_error_code: job.last_error_code.clone(),
            last_error_message: job.last_error_message.clone(),
        })
    }
}

#[derive(Debug)]
struct StoredJobRecord {
    meeting_json: String,
    org_scope: String,
    conference_kind: String,
    conference_locator_json: String,
    source_ref_json: Option<String>,
    status: String,
    attempt_count: i64,
    next_retry_at: Option<String>,
    destination_uri: Option<String>,
    source_checksum: Option<String>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

impl StoredJobRecord {
    fn from_sql_row(row: &SqlRow) -> anyhow::Result<Self> {
        Ok(Self {
            meeting_json: text_at(row, 0, "meeting_json")?,
            org_scope: text_at(row, 1, "org_scope")?,
            conference_kind: text_at(row, 2, "conference_kind")?,
            conference_locator_json: text_at(row, 3, "conference_locator_json")?,
            source_ref_json: optional_text_at(row, 4, "source_ref_json")?,
            status: text_at(row, 5, "status")?,
            attempt_count: i64_at(row, 6, "attempt_count")?,
            next_retry_at: optional_text_at(row, 7, "next_retry_at")?,
            destination_uri: optional_text_at(row, 8, "destination_uri")?,
            source_checksum: optional_text_at(row, 9, "source_checksum")?,
            last_error_code: optional_text_at(row, 10, "last_error_code")?,
            last_error_message: optional_text_at(row, 11, "last_error_message")?,
        })
    }

    fn into_job(self) -> anyhow::Result<MeetingJob> {
        let meeting: CompletedMeeting = serde_json::from_str(&self.meeting_json)
            .context("decode stored meeting_json payload")?;
        let locator: ConferenceLocator = serde_json::from_str(&self.conference_locator_json)
            .context("decode stored conference locator")?;
        let source_ref = self
            .source_ref_json
            .as_deref()
            .map(serde_json::from_str::<TranscriptSourceRef>)
            .transpose()
            .context("decode stored transcript source ref")?;
        Ok(MeetingJob {
            meeting_key: meeting.meeting_key.clone(),
            meeting,
            org_scope: self.org_scope,
            conference_kind: conference_kind_from_sql(&self.conference_kind)?,
            locator,
            source_ref,
            status: job_status_from_sql(&self.status)?,
            attempt_count: u32::try_from(self.attempt_count)
                .map_err(|_| anyhow!("invalid negative attempt_count {}", self.attempt_count))?,
            next_retry_at: self.next_retry_at,
            uploaded_destination_uri: self.destination_uri,
            source_checksum: self.source_checksum,
            last_error_code: self.last_error_code,
            last_error_message: self.last_error_message,
        })
    }
}

fn statement(
    sql: impl Into<String>,
    params: Vec<SqlValue>,
    kind: SqlStatementKind,
) -> SqlStatement {
    SqlStatement::new(sql)
        .with_params(params)
        .with_options(SqlStatementOptions {
            statement_kind: Some(kind),
            ..SqlStatementOptions::default()
        })
}

fn optional_text(value: Option<String>) -> SqlValue {
    value.map(SqlValue::Text).unwrap_or(SqlValue::Null)
}

fn text_at(row: &SqlRow, index: usize, name: &str) -> anyhow::Result<String> {
    match value_at(row, index, name)? {
        SqlValue::Text(value) => Ok(value.clone()),
        other => Err(anyhow!("expected text for {name}, got {other:?}")),
    }
}

fn optional_text_at(row: &SqlRow, index: usize, name: &str) -> anyhow::Result<Option<String>> {
    match value_at(row, index, name)? {
        SqlValue::Null => Ok(None),
        SqlValue::Text(value) => Ok(Some(value.clone())),
        other => Err(anyhow!("expected optional text for {name}, got {other:?}")),
    }
}

fn i64_at(row: &SqlRow, index: usize, name: &str) -> anyhow::Result<i64> {
    match value_at(row, index, name)? {
        SqlValue::I64(value) => Ok(*value),
        SqlValue::Text(value) => value
            .parse()
            .with_context(|| format!("parse integer text for {name}")),
        other => Err(anyhow!("expected integer for {name}, got {other:?}")),
    }
}

fn value_at<'a>(row: &'a SqlRow, index: usize, name: &str) -> anyhow::Result<&'a SqlValue> {
    row.values
        .get(index)
        .ok_or_else(|| anyhow!("missing SQL column {name} at index {index}"))
}

fn conference_kind_sql(kind: ConferenceKind) -> &'static str {
    match kind {
        ConferenceKind::Zoom => "zoom",
        ConferenceKind::GoogleMeet => "google_meet",
        ConferenceKind::Unknown => "unknown",
    }
}

fn conference_kind_from_sql(value: &str) -> anyhow::Result<ConferenceKind> {
    match value {
        "zoom" => Ok(ConferenceKind::Zoom),
        "google_meet" => Ok(ConferenceKind::GoogleMeet),
        "unknown" => Ok(ConferenceKind::Unknown),
        _ => Err(anyhow!(
            "unsupported conference kind `{value}` in SQL store"
        )),
    }
}

fn job_status_sql(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Discovered => "discovered",
        JobStatus::WaitingForRetry => "waiting_for_retry",
        JobStatus::Uploaded => "uploaded",
        JobStatus::NeedsManualReview => "needs_manual_review",
        JobStatus::PermanentFailure => "permanent_failure",
    }
}

fn job_status_from_sql(value: &str) -> anyhow::Result<JobStatus> {
    match value {
        "discovered" => Ok(JobStatus::Discovered),
        "waiting_for_retry" => Ok(JobStatus::WaitingForRetry),
        "uploaded" => Ok(JobStatus::Uploaded),
        "needs_manual_review" => Ok(JobStatus::NeedsManualReview),
        "permanent_failure" => Ok(JobStatus::PermanentFailure),
        _ => Err(anyhow!("unsupported job status `{value}` in SQL store")),
    }
}

fn source_lookup_status_for_discovered() -> &'static str {
    "unresolved"
}

fn source_lookup_status_for_job(job: &MeetingJob) -> &'static str {
    if job.source_ref.is_some() {
        return "resolved";
    }

    match job.status {
        JobStatus::Discovered => "unresolved",
        JobStatus::WaitingForRetry => "waiting",
        JobStatus::Uploaded => "resolved",
        JobStatus::NeedsManualReview => "manual_review",
        JobStatus::PermanentFailure => "permanent_failure",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{TranscriptSourceRef, meeting_key_from_event};
    use cap_sql_sqlx_sqlite::SqlxSqlite;

    async fn store() -> SqlTranscriptJobStore {
        let provider = Arc::new(SqlxSqlite::in_memory().expect("sqlite provider"));
        SqlTranscriptJobStore::new_with_setup(provider.clone(), provider.clone(), provider.as_ref())
            .await
            .expect("set up SQL store")
    }

    fn zoom_meeting(event_id: &str, end_at: &str) -> CompletedMeeting {
        let mut meeting = CompletedMeeting::new("primary", event_id, "Design review", end_at);
        meeting.scheduled_start_at = Some("2026-04-16T09:00:00Z".to_string());
        meeting.join_url = Some("https://zoom.us/j/123456789?pwd=test".to_string());
        meeting
    }

    fn sample_job(event_id: &str, end_at: &str) -> MeetingJob {
        let meeting = zoom_meeting(event_id, end_at);
        let mut job = MeetingJob::new_discovered("studio".to_string(), meeting.clone());
        job.conference_kind = ConferenceKind::Zoom;
        job.locator = ConferenceLocator::zoom(
            meeting.join_url.clone(),
            Some("123456789".to_string()),
            vec!["classified from Zoom join surface".to_string()],
        );
        job
    }

    #[tokio::test]
    async fn sql_store_upsert_discovered_is_idempotent_and_selects_due_jobs() {
        let store = store().await;
        let due_meeting = zoom_meeting("evt-due", "2026-04-16T10:00:00Z");
        let future_meeting = zoom_meeting("evt-future", "2026-04-16T10:05:00Z");

        store
            .upsert_discovered("studio", &[due_meeting.clone(), future_meeting.clone()])
            .await
            .expect("upsert meetings");
        store
            .upsert_discovered("studio", &[due_meeting.clone(), future_meeting.clone()])
            .await
            .expect("upsert meetings again");

        let mut future_job = store
            .job(&future_meeting.meeting_key)
            .await
            .expect("load future job")
            .expect("future job exists");
        future_job.status = JobStatus::WaitingForRetry;
        future_job.next_retry_at = Some("2026-04-16T10:30:00Z".to_string());
        store.save_job(&future_job).await.expect("save future job");

        let due_jobs = store
            .due_jobs("studio", "2026-04-16T10:00:00Z", 10)
            .await
            .expect("select due jobs");

        assert_eq!(due_jobs.len(), 1);
        assert_eq!(due_jobs[0].meeting_key, due_meeting.meeting_key);
    }

    #[tokio::test]
    async fn sql_store_save_job_round_trips_state_and_source_ref() {
        let store = store().await;
        let meeting_key = meeting_key_from_event("evt-saved", "2026-04-16T10:00:00Z");
        let mut job = sample_job("evt-saved", "2026-04-16T10:00:00Z");
        job.status = JobStatus::Uploaded;
        job.attempt_count = 2;
        job.source_ref = Some(TranscriptSourceRef::zoom_transcript(
            "zoom-transcript-1",
            "https://zoom.example.invalid/transcript-1",
        ));
        job.uploaded_destination_uri = Some(
            "r2://meeting-transcripts/transcripts/studio/2026/04/evt-saved/transcript.txt"
                .to_string(),
        );
        job.source_checksum = Some("sha256:deadbeef".to_string());

        store.save_job(&job).await.expect("save uploaded job");
        store.save_job(&job).await.expect("save uploaded job again");
        let loaded = store
            .job(&meeting_key)
            .await
            .expect("load uploaded job")
            .expect("uploaded job exists");

        assert_eq!(loaded.status, JobStatus::Uploaded);
        assert_eq!(loaded.attempt_count, 2);
        assert_eq!(loaded.source_checksum.as_deref(), Some("sha256:deadbeef"));
        assert_eq!(loaded.source_ref, job.source_ref);
        assert_eq!(
            loaded.uploaded_destination_uri,
            job.uploaded_destination_uri
        );
    }
}
