#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow};
use async_trait::async_trait;

#[cfg(all(not(target_arch = "wasm32"), test))]
use rusqlite::OptionalExtension;
#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{Connection, params};
#[cfg(target_arch = "wasm32")]
use worker::d1::D1Database;
#[cfg(target_arch = "wasm32")]
use worker::wasm_bindgen::JsValue;

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

pub struct D1TranscriptJobStore {
    #[cfg(not(target_arch = "wasm32"))]
    connection: Arc<Mutex<Connection>>,
    #[cfg(target_arch = "wasm32")]
    database: DatabaseHandle,
}

#[cfg(target_arch = "wasm32")]
struct DatabaseHandle(D1Database);

#[cfg(target_arch = "wasm32")]
// SAFETY: Cloudflare Workers runs this wasm code on a single-threaded event loop.
unsafe impl Send for DatabaseHandle {}
#[cfg(target_arch = "wasm32")]
// SAFETY: Cloudflare Workers runs this wasm code on a single-threaded event loop.
unsafe impl Sync for DatabaseHandle {}

impl D1TranscriptJobStore {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_in_memory() -> anyhow::Result<Self> {
        let connection = Connection::open_in_memory().context("open in-memory D1-shaped sqlite")?;
        Self::from_connection(connection)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_path(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let connection = Connection::open(path.as_ref())
            .with_context(|| format!("open sqlite job store at {}", path.as_ref().display()))?;
        Self::from_connection(connection)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_connection(connection: Connection) -> anyhow::Result<Self> {
        connection
            .execute_batch(SCHEMA_SQL)
            .context("initialize D1-shaped meeting_jobs schema")?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn from_database(database: D1Database) -> Result<Self, worker::Error> {
        database.exec(SCHEMA_SQL).await?;
        Ok(Self {
            database: DatabaseHandle(database),
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn from_env(env: &worker::Env, binding: &str) -> Result<Self, worker::Error> {
        let database = env.d1(binding)?;
        Self::from_database(database).await
    }

    #[cfg(test)]
    async fn job(&self, meeting_key: &str) -> anyhow::Result<Option<MeetingJob>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            return self.job_native(meeting_key);
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.job_workers(meeting_key).await
        }
    }

    #[cfg(all(not(target_arch = "wasm32"), test))]
    fn job_native(&self, meeting_key: &str) -> anyhow::Result<Option<MeetingJob>> {
        let connection = self.connection.lock().expect("sqlite D1 store lock");
        let mut statement = connection
            .prepare(SELECT_JOB_BY_KEY_SQL)
            .context("prepare sqlite select job by key")?;
        statement
            .query_row([meeting_key], StoredJobRecord::from_sql_row)
            .optional()
            .context("query sqlite job by key")?
            .map(StoredJobRecord::into_job)
            .transpose()
    }

    #[cfg(all(target_arch = "wasm32", test))]
    async fn job_workers(&self, meeting_key: &str) -> anyhow::Result<Option<MeetingJob>> {
        let results = self
            .database
            .0
            .prepare(SELECT_JOB_BY_KEY_SQL)
            .bind(&[JsValue::from_str(meeting_key)])
            .map_err(|error| anyhow!("bind D1 job lookup: {error}"))?
            .all()
            .await
            .map_err(|error| anyhow!("query D1 job by key: {error}"))?;
        results
            .results::<StoredJobRecord>()
            .map_err(|error| anyhow!("decode D1 job by key: {error}"))?
            .into_iter()
            .next()
            .map(StoredJobRecord::into_job)
            .transpose()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn upsert_discovered_native(
        &self,
        org_scope: &str,
        meetings: &[CompletedMeeting],
    ) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("sqlite D1 store lock");
        let transaction = connection
            .transaction()
            .context("begin sqlite upsert_discovered transaction")?;
        {
            let mut statement = transaction
                .prepare(UPSERT_DISCOVERED_SQL)
                .context("prepare sqlite upsert_discovered statement")?;
            for meeting in meetings {
                statement
                    .execute(params![
                        meeting.meeting_key,
                        meeting.calendar_event_id,
                        meeting.calendar_id,
                        org_scope,
                        serde_json::to_string(meeting)
                            .context("serialize completed meeting for sqlite")?,
                        meeting.scheduled_start_at,
                        meeting.scheduled_end_at,
                        conference_kind_sql(ConferenceKind::Unknown),
                        serde_json::to_string(&ConferenceLocator::default())
                            .context("serialize default conference locator for sqlite")?,
                        source_lookup_status_for_discovered(),
                        Option::<String>::None,
                        job_status_sql(JobStatus::Discovered),
                    ])
                    .context("execute sqlite upsert_discovered row")?;
            }
        }
        transaction
            .commit()
            .context("commit sqlite upsert_discovered transaction")
    }

    #[cfg(target_arch = "wasm32")]
    async fn upsert_discovered_workers(
        &self,
        org_scope: &str,
        meetings: &[CompletedMeeting],
    ) -> anyhow::Result<()> {
        for meeting in meetings {
            let values = vec![
                JsValue::from_str(&meeting.meeting_key),
                JsValue::from_str(&meeting.calendar_event_id),
                JsValue::from_str(&meeting.calendar_id),
                JsValue::from_str(org_scope),
                JsValue::from_str(
                    &serde_json::to_string(meeting)
                        .context("serialize completed meeting for D1")?,
                ),
                option_str_js(meeting.scheduled_start_at.as_deref()),
                JsValue::from_str(&meeting.scheduled_end_at),
                JsValue::from_str(conference_kind_sql(ConferenceKind::Unknown)),
                JsValue::from_str(
                    &serde_json::to_string(&ConferenceLocator::default())
                        .context("serialize default conference locator for D1")?,
                ),
                JsValue::from_str(source_lookup_status_for_discovered()),
                JsValue::NULL,
                JsValue::from_str(job_status_sql(JobStatus::Discovered)),
            ];
            self.database
                .0
                .prepare(UPSERT_DISCOVERED_SQL)
                .bind(&values)
                .map_err(|error| anyhow!("bind D1 upsert_discovered row: {error}"))?
                .run()
                .await
                .map_err(|error| anyhow!("execute D1 upsert_discovered row: {error}"))?;
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn due_jobs_native(
        &self,
        org_scope: &str,
        now_iso: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<MeetingJob>> {
        let connection = self.connection.lock().expect("sqlite D1 store lock");
        let mut statement = connection
            .prepare(SELECT_DUE_JOBS_SQL)
            .context("prepare sqlite due_jobs statement")?;
        let mut rows = statement
            .query(params![
                org_scope,
                job_status_sql(JobStatus::Discovered),
                job_status_sql(JobStatus::WaitingForRetry),
                now_iso,
                i64::from(limit),
            ])
            .context("execute sqlite due_jobs query")?;

        let mut jobs = Vec::new();
        while let Some(row) = rows.next().context("iterate sqlite due_jobs rows")? {
            jobs.push(StoredJobRecord::from_sql_row(row)?.into_job()?);
        }
        Ok(jobs)
    }

    #[cfg(target_arch = "wasm32")]
    async fn due_jobs_workers(
        &self,
        org_scope: &str,
        now_iso: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<MeetingJob>> {
        let results = self
            .database
            .0
            .prepare(SELECT_DUE_JOBS_SQL)
            .bind(&[
                JsValue::from_str(org_scope),
                JsValue::from_str(job_status_sql(JobStatus::Discovered)),
                JsValue::from_str(job_status_sql(JobStatus::WaitingForRetry)),
                JsValue::from_str(now_iso),
                JsValue::from_f64(f64::from(limit)),
            ])
            .map_err(|error| anyhow!("bind D1 due_jobs query: {error}"))?
            .all()
            .await
            .map_err(|error| anyhow!("execute D1 due_jobs query: {error}"))?;

        results
            .results::<StoredJobRecord>()
            .map_err(|error| anyhow!("decode D1 due_jobs rows: {error}"))?
            .into_iter()
            .map(StoredJobRecord::into_job)
            .collect()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn save_job_native(&self, job: &MeetingJob) -> anyhow::Result<()> {
        let persistence = PersistedJob::from_job(job)?;
        let connection = self.connection.lock().expect("sqlite D1 store lock");
        connection
            .execute(
                SAVE_JOB_SQL,
                params![
                    persistence.meeting_key,
                    persistence.calendar_event_id,
                    persistence.calendar_id,
                    persistence.org_scope,
                    persistence.meeting_json,
                    persistence.scheduled_start_at,
                    persistence.scheduled_end_at,
                    persistence.conference_kind,
                    persistence.conference_locator_json,
                    persistence.source_lookup_status,
                    persistence.source_ref_json,
                    persistence.status,
                    persistence.attempt_count,
                    persistence.next_retry_at,
                    persistence.status,
                    persistence.destination_uri,
                    persistence.source_artifact_uri,
                    persistence.source_checksum,
                    persistence.last_error_code,
                    persistence.last_error_message,
                ],
            )
            .context("execute sqlite save_job upsert")?;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    async fn save_job_workers(&self, job: &MeetingJob) -> anyhow::Result<()> {
        let persistence = PersistedJob::from_job(job)?;
        let values = vec![
            JsValue::from_str(&persistence.meeting_key),
            JsValue::from_str(&persistence.calendar_event_id),
            JsValue::from_str(&persistence.calendar_id),
            JsValue::from_str(&persistence.org_scope),
            JsValue::from_str(&persistence.meeting_json),
            option_str_js(persistence.scheduled_start_at.as_deref()),
            JsValue::from_str(&persistence.scheduled_end_at),
            JsValue::from_str(&persistence.conference_kind),
            JsValue::from_str(&persistence.conference_locator_json),
            JsValue::from_str(&persistence.source_lookup_status),
            option_str_js(persistence.source_ref_json.as_deref()),
            JsValue::from_str(&persistence.status),
            JsValue::from_f64(persistence.attempt_count as f64),
            option_str_js(persistence.next_retry_at.as_deref()),
            JsValue::from_str(&persistence.status),
            option_str_js(persistence.destination_uri.as_deref()),
            option_str_js(persistence.source_artifact_uri.as_deref()),
            option_str_js(persistence.source_checksum.as_deref()),
            option_str_js(persistence.last_error_code.as_deref()),
            option_str_js(persistence.last_error_message.as_deref()),
        ];
        self.database
            .0
            .prepare(SAVE_JOB_SQL)
            .bind(&values)
            .map_err(|error| anyhow!("bind D1 save_job upsert: {error}"))?
            .run()
            .await
            .map_err(|error| anyhow!("execute D1 save_job upsert: {error}"))?;
        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptJobStore for D1TranscriptJobStore {
    async fn upsert_discovered(
        &self,
        org_scope: &str,
        meetings: &[CompletedMeeting],
    ) -> anyhow::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            return self.upsert_discovered_native(org_scope, meetings);
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.upsert_discovered_workers(org_scope, meetings).await
        }
    }

    async fn due_jobs(
        &self,
        org_scope: &str,
        now_iso: &str,
        limit: u32,
    ) -> anyhow::Result<Vec<MeetingJob>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            return self.due_jobs_native(org_scope, now_iso, limit);
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.due_jobs_workers(org_scope, now_iso, limit).await
        }
    }

    async fn save_job(&self, job: &MeetingJob) -> anyhow::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            return self.save_job_native(job);
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.save_job_workers(job).await
        }
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

#[derive(Debug, serde::Deserialize)]
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
    #[cfg(not(target_arch = "wasm32"))]
    fn from_sql_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            meeting_json: row.get("meeting_json")?,
            org_scope: row.get("org_scope")?,
            conference_kind: row.get("conference_kind")?,
            conference_locator_json: row.get("conference_locator_json")?,
            source_ref_json: row.get("source_ref_json")?,
            status: row.get("status")?,
            attempt_count: row.get("attempt_count")?,
            next_retry_at: row.get("next_retry_at")?,
            destination_uri: row.get("destination_uri")?,
            source_checksum: row.get("source_checksum")?,
            last_error_code: row.get("last_error_code")?,
            last_error_message: row.get("last_error_message")?,
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
        _ => Err(anyhow!("unsupported conference kind `{value}` in D1 store")),
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
        _ => Err(anyhow!("unsupported job status `{value}` in D1 store")),
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

#[cfg(target_arch = "wasm32")]
fn option_str_js(value: Option<&str>) -> JsValue {
    value.map(JsValue::from_str).unwrap_or(JsValue::NULL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{TranscriptSourceRef, meeting_key_from_event};

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
    async fn d1_store_round_trips_upsert_and_due_selection() {
        let store = D1TranscriptJobStore::open_in_memory().expect("open D1-shaped store");
        let due_meeting = zoom_meeting("evt-due", "2026-04-16T10:00:00Z");
        let future_meeting = zoom_meeting("evt-future", "2026-04-16T10:05:00Z");

        store
            .upsert_discovered("studio", &[due_meeting.clone(), future_meeting.clone()])
            .await
            .expect("upsert meetings");

        let mut future_job = store
            .job(&future_meeting.meeting_key)
            .await
            .expect("load future job")
            .expect("future job exists");
        future_job.status = JobStatus::WaitingForRetry;
        future_job.next_retry_at = Some("2026-04-16T10:30:00Z".to_string());
        future_job.source_ref = Some(TranscriptSourceRef::zoom_transcript(
            "zoom-future",
            "https://zoom.example.invalid/future",
        ));
        store.save_job(&future_job).await.expect("save future job");

        let due_jobs = store
            .due_jobs("studio", "2026-04-16T10:00:00Z", 10)
            .await
            .expect("select due jobs");

        assert_eq!(due_jobs.len(), 1);
        assert_eq!(due_jobs[0].meeting_key, due_meeting.meeting_key);
    }

    #[tokio::test]
    async fn d1_store_due_selection_filters_by_org_scope() {
        let store = D1TranscriptJobStore::open_in_memory().expect("open D1-shaped store");
        let studio_meeting = zoom_meeting("evt-studio", "2026-04-16T10:00:00Z");
        let other_meeting = zoom_meeting("evt-other", "2026-04-16T10:01:00Z");

        store
            .upsert_discovered("studio", std::slice::from_ref(&studio_meeting))
            .await
            .expect("upsert studio meeting");
        store
            .upsert_discovered("ops", std::slice::from_ref(&other_meeting))
            .await
            .expect("upsert ops meeting");

        let due_jobs = store
            .due_jobs("studio", "2026-04-16T10:10:00Z", 10)
            .await
            .expect("select scoped jobs");

        assert_eq!(due_jobs.len(), 1);
        assert_eq!(due_jobs[0].org_scope, "studio");
        assert_eq!(due_jobs[0].meeting_key, studio_meeting.meeting_key);
    }

    #[tokio::test]
    async fn d1_store_persists_saved_state_and_source_ref() {
        let store = D1TranscriptJobStore::open_in_memory().expect("open D1-shaped store");
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
