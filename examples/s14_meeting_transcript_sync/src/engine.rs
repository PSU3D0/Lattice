use std::sync::Arc;

use anyhow::{Context, anyhow};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

use crate::adapters::{
    MeetingSource, SourceResolution, TranscriptFetcher, TranscriptJobStore,
    TranscriptSourceResolver, TranscriptUploader,
};
use crate::config::TranscriptSyncConfig;
use crate::domain::{
    CompletedMeeting, ConferenceKind, ConferenceLocator, TranscriptSyncRequest,
    TranscriptSyncSummary,
};
use crate::state::{JobStatus, MeetingJob};

#[derive(Clone)]
pub struct TranscriptSyncServices {
    pub meeting_source: Arc<dyn MeetingSource>,
    pub resolver: Arc<dyn TranscriptSourceResolver>,
    pub fetcher: Arc<dyn TranscriptFetcher>,
    pub uploader: Arc<dyn TranscriptUploader>,
    pub store: Arc<dyn TranscriptJobStore>,
}

impl TranscriptSyncServices {
    pub fn new(
        meeting_source: Arc<dyn MeetingSource>,
        resolver: Arc<dyn TranscriptSourceResolver>,
        fetcher: Arc<dyn TranscriptFetcher>,
        uploader: Arc<dyn TranscriptUploader>,
        store: Arc<dyn TranscriptJobStore>,
    ) -> Self {
        Self {
            meeting_source,
            resolver,
            fetcher,
            uploader,
            store,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub(crate) struct DiscoveredMeetingsBatch {
    pub request: TranscriptSyncRequest,
    pub meetings: Vec<CompletedMeeting>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub(crate) struct UpsertedMeetingsBatch {
    pub request: TranscriptSyncRequest,
    pub discovered_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub(crate) struct DueJobsBatch {
    pub request: TranscriptSyncRequest,
    pub discovered_count: usize,
    pub due_jobs: Vec<MeetingJob>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessDisposition {
    Uploaded,
    Waiting,
    ManualReview,
    PermanentFailure,
    RetryableError,
    SkippedIdempotent,
}

#[derive(Clone, Debug, PartialEq)]
struct ProcessedJob {
    job: MeetingJob,
    disposition: ProcessDisposition,
}

pub(crate) fn bundle_execution_error(node_name: &str) -> anyhow::Error {
    anyhow!(
        "s14 node `{node_name}` is not wired to a production host runtime; use TranscriptSyncExecutor::execute(...) or TranscriptSyncExecutor::execute_scheduled_tick(...) instead"
    )
}

pub(crate) async fn fetch_recent_completed_meetings(
    services: &TranscriptSyncServices,
    config: &TranscriptSyncConfig,
    request: &TranscriptSyncRequest,
) -> anyhow::Result<DiscoveredMeetingsBatch> {
    let meetings = services
        .meeting_source
        .fetch_recent_completed_meetings(request, config)
        .await?;
    Ok(DiscoveredMeetingsBatch {
        request: request.clone(),
        meetings,
    })
}

pub(crate) async fn upsert_meeting_jobs(
    services: &TranscriptSyncServices,
    batch: DiscoveredMeetingsBatch,
) -> anyhow::Result<UpsertedMeetingsBatch> {
    services
        .store
        .upsert_discovered(&batch.request.org_scope, &batch.meetings)
        .await?;
    Ok(UpsertedMeetingsBatch {
        request: batch.request,
        discovered_count: batch.meetings.len(),
    })
}

pub(crate) async fn select_due_jobs(
    services: &TranscriptSyncServices,
    config: &TranscriptSyncConfig,
    batch: UpsertedMeetingsBatch,
) -> anyhow::Result<DueJobsBatch> {
    let due_jobs = services
        .store
        .due_jobs(
            &batch.request.org_scope,
            &batch.request.window_end,
            config.sync_batch_limit,
        )
        .await?;
    Ok(DueJobsBatch {
        request: batch.request,
        discovered_count: batch.discovered_count,
        due_jobs,
    })
}

pub(crate) async fn reconcile_due_jobs(
    services: &TranscriptSyncServices,
    config: &TranscriptSyncConfig,
    batch: DueJobsBatch,
) -> anyhow::Result<TranscriptSyncSummary> {
    let mut summary = TranscriptSyncSummary {
        discovered: batch.discovered_count,
        selected_due_jobs: batch.due_jobs.len(),
        ..TranscriptSyncSummary::default()
    };

    for job in batch.due_jobs {
        let processed = process_job(services, config, &batch.request.window_end, job).await?;
        services.store.save_job(&processed.job).await?;
        summary.processed += 1;
        match processed.disposition {
            ProcessDisposition::Uploaded => summary.uploaded += 1,
            ProcessDisposition::Waiting => summary.waiting += 1,
            ProcessDisposition::ManualReview => summary.manual_review += 1,
            ProcessDisposition::PermanentFailure => summary.permanent_failures += 1,
            ProcessDisposition::RetryableError => summary.retryable_errors += 1,
            ProcessDisposition::SkippedIdempotent => summary.skipped_idempotent += 1,
        }
    }

    Ok(summary)
}

#[allow(dead_code)]
pub(crate) async fn run_reconcile(
    services: &TranscriptSyncServices,
    config: &TranscriptSyncConfig,
    request: &TranscriptSyncRequest,
) -> anyhow::Result<TranscriptSyncSummary> {
    let discovered = fetch_recent_completed_meetings(services, config, request).await?;
    let upserted = upsert_meeting_jobs(services, discovered).await?;
    let due = select_due_jobs(services, config, upserted).await?;
    reconcile_due_jobs(services, config, due).await
}

async fn process_job(
    services: &TranscriptSyncServices,
    config: &TranscriptSyncConfig,
    now_iso: &str,
    mut job: MeetingJob,
) -> anyhow::Result<ProcessedJob> {
    if job.status == JobStatus::Uploaded {
        return Ok(ProcessedJob {
            job,
            disposition: ProcessDisposition::SkippedIdempotent,
        });
    }

    job.attempt_count += 1;
    job.next_retry_at = None;

    let locator = classify_conference(&job.meeting);
    job.conference_kind = locator.kind;
    job.locator = locator.clone();

    match locator.kind {
        ConferenceKind::Unknown => {
            job.status = JobStatus::NeedsManualReview;
            job.last_error_code = Some("unknown_conference_kind".to_string());
            job.last_error_message = Some(
                "meeting transcript sync will not guess an unknown conference kind".to_string(),
            );
            return Ok(ProcessedJob {
                job,
                disposition: ProcessDisposition::ManualReview,
            });
        }
        ConferenceKind::Zoom | ConferenceKind::GoogleMeet => {}
    }

    let source_ref = if let Some(source_ref) = job.source_ref.clone() {
        source_ref
    } else {
        match services
            .resolver
            .resolve(&job.meeting, &locator, config)
            .await
            .with_context(|| format!("resolve transcript source for {}", job.meeting_key))?
        {
            SourceResolution::Resolved(source_ref) => {
                job.source_ref = Some(source_ref.clone());
                source_ref
            }
            SourceResolution::NotFoundYet => {
                let disposition = mark_retry_or_terminal(
                    &mut job,
                    now_iso,
                    config,
                    "transcript_source_not_ready",
                    "transcript source is not ready yet",
                    ProcessDisposition::Waiting,
                )?;
                return Ok(ProcessedJob { job, disposition });
            }
            SourceResolution::Ambiguous { .. } => {
                job.status = JobStatus::NeedsManualReview;
                job.last_error_code = Some(
                    match locator.kind {
                        ConferenceKind::GoogleMeet => "gmeet_doc_ambiguous",
                        ConferenceKind::Zoom => "zoom_transcript_ambiguous",
                        ConferenceKind::Unknown => "transcript_source_ambiguous",
                    }
                    .to_string(),
                );
                job.last_error_message = Some(
                    "multiple transcript source candidates were found; manual review required"
                        .to_string(),
                );
                return Ok(ProcessedJob {
                    job,
                    disposition: ProcessDisposition::ManualReview,
                });
            }
            SourceResolution::PermanentFailure { code, message } => {
                job.status = JobStatus::PermanentFailure;
                job.last_error_code = Some(code);
                job.last_error_message = Some(message);
                return Ok(ProcessedJob {
                    job,
                    disposition: ProcessDisposition::PermanentFailure,
                });
            }
        }
    };

    match services
        .fetcher
        .fetch(&source_ref, config)
        .await
        .with_context(|| format!("fetch transcript artifact for {}", job.meeting_key))?
    {
        crate::adapters::FetchOutcome::Ready(artifact) => {
            match services
                .uploader
                .upload(&job.meeting, &artifact, config)
                .await
                .with_context(|| format!("upload transcript for {}", job.meeting_key))
            {
                Ok(uploaded) => {
                    job.status = JobStatus::Uploaded;
                    job.uploaded_destination_uri = Some(uploaded.destination_uri);
                    job.source_checksum = Some(uploaded.checksum);
                    job.last_error_code = None;
                    job.last_error_message = None;
                    Ok(ProcessedJob {
                        job,
                        disposition: ProcessDisposition::Uploaded,
                    })
                }
                Err(error) => {
                    let disposition = mark_retry_or_terminal(
                        &mut job,
                        now_iso,
                        config,
                        "upload_retryable_error",
                        &format!("{error:#}"),
                        ProcessDisposition::RetryableError,
                    )?;
                    Ok(ProcessedJob { job, disposition })
                }
            }
        }
        crate::adapters::FetchOutcome::NotReady => {
            let disposition = mark_retry_or_terminal(
                &mut job,
                now_iso,
                config,
                "transcript_not_ready",
                "transcript artifact is not ready yet",
                ProcessDisposition::Waiting,
            )?;
            Ok(ProcessedJob { job, disposition })
        }
        crate::adapters::FetchOutcome::PermanentFailure { code, message } => {
            job.status = JobStatus::PermanentFailure;
            job.last_error_code = Some(code);
            job.last_error_message = Some(message);
            Ok(ProcessedJob {
                job,
                disposition: ProcessDisposition::PermanentFailure,
            })
        }
    }
}

fn mark_retry_or_terminal(
    job: &mut MeetingJob,
    now_iso: &str,
    config: &TranscriptSyncConfig,
    code: &str,
    message: &str,
    retry_disposition: ProcessDisposition,
) -> anyhow::Result<ProcessDisposition> {
    if job.attempt_count >= config.max_transcript_attempts {
        job.status = JobStatus::PermanentFailure;
        job.next_retry_at = None;
        job.last_error_code = Some("retry_budget_exhausted".to_string());
        job.last_error_message = Some(format!(
            "retry budget exhausted after last error `{code}`: {message}"
        ));
        return Ok(ProcessDisposition::PermanentFailure);
    }

    job.status = JobStatus::WaitingForRetry;
    job.next_retry_at = Some(add_minutes(now_iso, config.transcript_ready_retry_minutes)?);
    job.last_error_code = Some(code.to_string());
    job.last_error_message = Some(message.to_string());
    Ok(retry_disposition)
}

pub(crate) fn classify_conference(meeting: &CompletedMeeting) -> ConferenceLocator {
    let join_url = meeting.join_url.clone();
    let fields = [
        meeting.join_url.as_deref(),
        meeting.description.as_deref(),
        meeting.location.as_deref(),
    ];

    let zoom_hit = fields
        .iter()
        .flatten()
        .find(|value| contains_zoom_signal(value));
    let gmeet_hit = fields
        .iter()
        .flatten()
        .find(|value| contains_google_meet_signal(value));

    match (zoom_hit, gmeet_hit) {
        (Some(_), Some(_)) => ConferenceLocator::unknown(vec![
            "meeting metadata contains both Zoom and Google Meet signals".to_string(),
        ]),
        (Some(source), None) => ConferenceLocator::zoom(
            join_url,
            extract_zoom_meeting_id(source),
            vec!["classified from Zoom join surface".to_string()],
        ),
        (None, Some(source)) => ConferenceLocator::google_meet(
            meeting
                .join_url
                .clone()
                .filter(|value| contains_google_meet_signal(value)),
            extract_google_meet_code(source),
            vec!["classified from Google Meet join surface".to_string()],
        ),
        (None, None) => ConferenceLocator::unknown(vec![
            "no supported conference locator found in join_url, description, or location"
                .to_string(),
        ]),
    }
}

fn contains_zoom_signal(value: &str) -> bool {
    value.contains("zoom.us/") || value.contains("zoom.com/")
}

fn contains_google_meet_signal(value: &str) -> bool {
    value.contains("meet.google.com/")
}

fn extract_zoom_meeting_id(value: &str) -> Option<String> {
    let marker = "/j/";
    let start = value.find(marker)? + marker.len();
    let digits = value[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        Some(digits)
    }
}

fn extract_google_meet_code(value: &str) -> Option<String> {
    let marker = "meet.google.com/";
    let start = value.find(marker)? + marker.len();
    let code = value[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
        .collect::<String>();
    if code.is_empty() { None } else { Some(code) }
}

fn add_minutes(now_iso: &str, minutes: u32) -> anyhow::Result<String> {
    let parsed = OffsetDateTime::parse(now_iso, &Rfc3339)
        .with_context(|| format!("parse RFC3339 timestamp `{now_iso}`"))?;
    let shifted = parsed + Duration::minutes(i64::from(minutes));
    shifted
        .format(&Rfc3339)
        .map_err(|error| anyhow!("format RFC3339 timestamp: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::meeting_key_from_event;

    #[test]
    fn classify_conference_prefers_unknown_when_signals_conflict() {
        let meeting = CompletedMeeting {
            meeting_key: meeting_key_from_event("evt-1", "2026-04-16T10:00:00Z"),
            calendar_event_id: "evt-1".to_string(),
            calendar_id: "primary".to_string(),
            title: "Conflicted".to_string(),
            scheduled_start_at: None,
            scheduled_end_at: "2026-04-16T10:00:00Z".to_string(),
            join_url: Some("https://zoom.us/j/123456789".to_string()),
            description: Some("fallback https://meet.google.com/abc-defg-hij".to_string()),
            location: None,
            organizer_email: None,
            attendees: Vec::new(),
            metadata: serde_json::Value::Null,
        };

        let locator = classify_conference(&meeting);
        assert_eq!(locator.kind, ConferenceKind::Unknown);
        assert_eq!(locator.notes.len(), 1);
    }

    #[test]
    fn add_minutes_preserves_rfc3339_shape() {
        assert_eq!(
            add_minutes("2026-04-16T10:00:00Z", 5).expect("add minutes"),
            "2026-04-16T10:05:00Z"
        );
    }
}
