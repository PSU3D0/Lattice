#![cfg(not(target_arch = "wasm32"))]

use std::sync::{Arc, Mutex};

use capabilities::ResourceBag;
use host_inproc::HostExecutionResult;

use crate::adapters::fake::{
    FakeMeetingSource, FakeTranscriptFetcher, FakeTranscriptSourceResolver, FakeTranscriptUploader,
    InMemoryTranscriptJobStore,
};
use crate::adapters::{FetchOutcome, SourceResolution};
use crate::config::TranscriptSyncConfig;
use crate::domain::{
    CompletedMeeting, TranscriptArtifact, TranscriptSourceRef, TranscriptSyncRequest,
    TranscriptSyncSummary, UploadedTranscript,
};
use crate::engine::{InstalledRuntime, TranscriptSyncServices, install_runtime};
use crate::state::{JobStatus, MeetingJob};
use crate::{bundle, flow, meeting_key_from_event};

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone)]
struct TestHarness {
    config: TranscriptSyncConfig,
    meeting_source: FakeMeetingSource,
    resolver: FakeTranscriptSourceResolver,
    fetcher: FakeTranscriptFetcher,
    uploader: FakeTranscriptUploader,
    store: InMemoryTranscriptJobStore,
}

impl Default for TestHarness {
    fn default() -> Self {
        Self {
            config: sample_config(),
            meeting_source: FakeMeetingSource::default(),
            resolver: FakeTranscriptSourceResolver::default(),
            fetcher: FakeTranscriptFetcher::default(),
            uploader: FakeTranscriptUploader::default(),
            store: InMemoryTranscriptJobStore::default(),
        }
    }
}

impl TestHarness {
    async fn execute(&self, request: TranscriptSyncRequest) -> TranscriptSyncSummary {
        let runtime = Arc::new(InstalledRuntime::new(self.config.clone(), self.services()));
        let _guard = install_runtime(runtime);
        let payload = serde_json::to_value(request).expect("serialize request");
        let result = bundle()
            .executor()
            .with_resource_bag(ResourceBag::default())
            .run_once(&bundle().validated_ir, "trigger", payload, "capture", None)
            .await
            .expect("flow execution succeeds");

        match result {
            HostExecutionResult::Value(value) => {
                serde_json::from_value(value).expect("decode summary")
            }
            HostExecutionResult::Stream(_) => panic!("expected a value result"),
            HostExecutionResult::Halt { alias, .. } => {
                panic!("unexpected halt at {alias}")
            }
        }
    }

    fn services(&self) -> TranscriptSyncServices {
        TranscriptSyncServices::new(
            Arc::new(self.meeting_source.clone()),
            Arc::new(self.resolver.clone()),
            Arc::new(self.fetcher.clone()),
            Arc::new(self.uploader.clone()),
            Arc::new(self.store.clone()),
        )
    }
}

fn sample_config() -> TranscriptSyncConfig {
    TranscriptSyncConfig {
        org_scope: "studio".to_string(),
        calendar_ids: vec!["primary".to_string()],
        sync_lookback_minutes: 30,
        sync_batch_limit: 20,
        transcript_ready_retry_minutes: 5,
        max_transcript_attempts: 3,
        destination_prefix: "r2://meeting-transcripts/transcripts".to_string(),
        gmeet_doc_title_patterns: vec!["Transcript".to_string(), "Meeting notes".to_string()],
    }
}

fn sample_request() -> TranscriptSyncRequest {
    request_ending_at("2026-04-16T10:00:00Z")
}

fn request_ending_at(window_end: &str) -> TranscriptSyncRequest {
    TranscriptSyncRequest {
        org_scope: "studio".to_string(),
        window_start: "2026-04-16T09:30:00Z".to_string(),
        window_end: window_end.to_string(),
        source: "cron".to_string(),
        backfill_reason: None,
    }
}

fn zoom_meeting(event_id: &str, end_at: &str) -> CompletedMeeting {
    let mut meeting = CompletedMeeting::new("primary", event_id, "Design review", end_at);
    meeting.scheduled_start_at = Some("2026-04-16T09:00:00Z".to_string());
    meeting.join_url = Some("https://zoom.us/j/123456789?pwd=test".to_string());
    meeting.organizer_email = Some("host@example.com".to_string());
    meeting.attendees = vec!["guest@example.com".to_string()];
    meeting
}

fn gmeet_meeting(event_id: &str, end_at: &str) -> CompletedMeeting {
    let mut meeting = CompletedMeeting::new("primary", event_id, "Weekly sync", end_at);
    meeting.scheduled_start_at = Some("2026-04-16T09:00:00Z".to_string());
    meeting.join_url = Some("https://meet.google.com/abc-defg-hij".to_string());
    meeting.description = Some("Weekly sync transcript should land in Docs".to_string());
    meeting
}

fn unknown_meeting(event_id: &str, end_at: &str) -> CompletedMeeting {
    let mut meeting = CompletedMeeting::new("primary", event_id, "Partner call", end_at);
    meeting.scheduled_start_at = Some("2026-04-16T09:00:00Z".to_string());
    meeting.join_url = Some("https://teams.microsoft.com/l/meetup-join/123".to_string());
    meeting.location = Some("Teams".to_string());
    meeting
}

fn zoom_source_ref(source_id: &str) -> TranscriptSourceRef {
    TranscriptSourceRef::zoom_transcript(
        source_id,
        format!("https://zoom.example.invalid/{source_id}"),
    )
}

fn google_doc_source_ref(source_id: &str) -> TranscriptSourceRef {
    TranscriptSourceRef::google_doc(
        source_id,
        format!("https://docs.google.com/document/d/{source_id}/edit"),
    )
}

fn artifact(source_ref: TranscriptSourceRef, text: &str) -> TranscriptArtifact {
    TranscriptArtifact {
        text: text.to_string(),
        normalized: serde_json::json!({ "paragraphs": 1 }),
        source_ref,
    }
}

fn explicit_upload(meeting_key: &str) -> UploadedTranscript {
    UploadedTranscript {
        destination_uri: format!(
            "r2://meeting-transcripts/transcripts/{meeting_key}/transcript.txt"
        ),
        checksum: format!("sha256:{meeting_key}"),
        size_bytes: 128,
    }
}

#[test]
fn flow_contains_explicit_reconcile_nodes() {
    let ir = flow();
    let aliases = ir
        .nodes
        .iter()
        .map(|node| node.alias.as_str())
        .collect::<Vec<_>>();
    assert!(aliases.contains(&"fetch"));
    assert!(aliases.contains(&"upsert"));
    assert!(aliases.contains(&"select_due"));
    assert!(aliases.contains(&"reconcile"));
}

#[tokio::test]
async fn bundle_without_installed_runtime_fails_with_explicit_message() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let payload = serde_json::to_value(sample_request()).expect("serialize request");

    let result = bundle()
        .executor()
        .with_resource_bag(ResourceBag::default())
        .run_once(&bundle().validated_ir, "trigger", payload, "capture", None)
        .await;

    let error = match result {
        Ok(_) => panic!("bundle execution should fail without installed runtime"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("meeting transcript sync runtime is not installed")
    );
}

#[tokio::test]
async fn zoom_ready_transcript_uploads_successfully() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = zoom_meeting("evt-zoom-ready", "2026-04-16T10:00:00Z");
    let source_ref = zoom_source_ref("zoom-transcript-1");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Resolved(source_ref.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default().with_outcome(
            source_ref.cache_key(),
            FetchOutcome::Ready(artifact(source_ref.clone(), "Zoom transcript ready")),
        ),
        uploader: FakeTranscriptUploader::default().with_success(
            meeting.meeting_key.clone(),
            explicit_upload(&meeting.meeting_key),
        ),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(summary.uploaded, 1);
    assert_eq!(summary.processed, 1);
    assert_eq!(job.status, JobStatus::Uploaded);
    assert_eq!(job.attempt_count, 1);
    assert!(job.uploaded_destination_uri.is_some());
    assert_eq!(harness.uploader.calls(), vec![meeting.meeting_key.clone()]);
}

#[tokio::test]
async fn zoom_not_ready_moves_to_waiting_state() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = zoom_meeting("evt-zoom-wait", "2026-04-16T10:00:00Z");
    let source_ref = zoom_source_ref("zoom-transcript-2");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Resolved(source_ref.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default()
            .with_outcome(source_ref.cache_key(), FetchOutcome::NotReady),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(summary.waiting, 1);
    assert_eq!(job.status, JobStatus::WaitingForRetry);
    assert_eq!(job.next_retry_at.as_deref(), Some("2026-04-16T10:05:00Z"));
    assert_eq!(job.last_error_code.as_deref(), Some("transcript_not_ready"));
    assert!(harness.uploader.calls().is_empty());
}

#[tokio::test]
async fn gmeet_doc_resolves_and_uploads_successfully() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = gmeet_meeting("evt-gmeet-ready", "2026-04-16T10:00:00Z");
    let source_ref = google_doc_source_ref("doc-1");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Resolved(source_ref.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default().with_outcome(
            source_ref.cache_key(),
            FetchOutcome::Ready(artifact(source_ref.clone(), "GMeet transcript text")),
        ),
        uploader: FakeTranscriptUploader::default().with_success(
            meeting.meeting_key.clone(),
            explicit_upload(&meeting.meeting_key),
        ),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(summary.uploaded, 1);
    assert_eq!(job.status, JobStatus::Uploaded);
    assert_eq!(job.conference_kind, crate::ConferenceKind::GoogleMeet);
    assert_eq!(job.source_ref, Some(source_ref));
}

#[tokio::test]
async fn gmeet_doc_ambiguous_becomes_explicit_non_success_state() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = gmeet_meeting("evt-gmeet-ambiguous", "2026-04-16T10:00:00Z");
    let source_a = google_doc_source_ref("doc-a");
    let source_b = google_doc_source_ref("doc-b");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Ambiguous {
                candidates: vec![source_a, source_b],
            },
        ),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(summary.manual_review, 1);
    assert_eq!(job.status, JobStatus::NeedsManualReview);
    assert_eq!(job.last_error_code.as_deref(), Some("gmeet_doc_ambiguous"));
    assert!(harness.fetcher.calls().is_empty());
}

#[tokio::test]
async fn unknown_conference_kind_is_not_blindly_guessed() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = unknown_meeting("evt-unknown", "2026-04-16T10:00:00Z");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(summary.manual_review, 1);
    assert_eq!(job.status, JobStatus::NeedsManualReview);
    assert_eq!(
        job.last_error_code.as_deref(),
        Some("unknown_conference_kind")
    );
    assert!(harness.resolver.calls().is_empty());
}

#[tokio::test]
async fn already_uploaded_job_is_skipped_idempotently() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = zoom_meeting("evt-uploaded", "2026-04-16T10:00:00Z");
    let source_ref = zoom_source_ref("zoom-uploaded-source");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Resolved(source_ref.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default().with_outcome(
            source_ref.cache_key(),
            FetchOutcome::Ready(artifact(source_ref.clone(), "Uploaded once")),
        ),
        uploader: FakeTranscriptUploader::default().with_success(
            meeting.meeting_key.clone(),
            explicit_upload(&meeting.meeting_key),
        ),
        ..TestHarness::default()
    };

    let first = harness.execute(sample_request()).await;
    let second = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(first.uploaded, 1);
    assert_eq!(second.selected_due_jobs, 0);
    assert_eq!(second.processed, 0);
    assert_eq!(job.status, JobStatus::Uploaded);
    assert_eq!(harness.uploader.calls().len(), 1);
}

#[tokio::test]
async fn upsert_discovered_is_idempotent_for_same_meeting_key() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = zoom_meeting("evt-idempotent-upsert", "2026-04-16T10:00:00Z");
    let source_ref = zoom_source_ref("zoom-upsert-source");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Resolved(source_ref.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default()
            .with_outcome(source_ref.cache_key(), FetchOutcome::NotReady),
        ..TestHarness::default()
    };

    harness.execute(sample_request()).await;
    harness.execute(sample_request()).await;

    assert_eq!(harness.store.all_jobs().len(), 1);
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");
    assert_eq!(job.status, JobStatus::WaitingForRetry);
}

#[tokio::test]
async fn only_due_jobs_are_selected_for_processing() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let due_meeting = zoom_meeting("evt-due", "2026-04-16T09:55:00Z");
    let future_meeting = zoom_meeting("evt-future", "2026-04-16T09:56:00Z");
    let due_source = zoom_source_ref("zoom-due-source");
    let future_source = zoom_source_ref("zoom-future-source");

    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(Vec::new()),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            due_meeting.meeting_key.clone(),
            SourceResolution::Resolved(due_source.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default().with_outcome(
            due_source.cache_key(),
            FetchOutcome::Ready(artifact(due_source.clone(), "Due transcript")),
        ),
        uploader: FakeTranscriptUploader::default().with_success(
            due_meeting.meeting_key.clone(),
            explicit_upload(&due_meeting.meeting_key),
        ),
        ..TestHarness::default()
    };

    harness.store.seed(MeetingJob::new_discovered(
        "studio".to_string(),
        due_meeting.clone(),
    ));

    let mut future_job = MeetingJob::new_discovered("studio".to_string(), future_meeting.clone());
    future_job.status = JobStatus::WaitingForRetry;
    future_job.next_retry_at = Some("2026-04-16T10:30:00Z".to_string());
    future_job.source_ref = Some(future_source);
    harness.store.seed(future_job);

    let summary = harness.execute(sample_request()).await;
    let future_job = harness
        .store
        .get(&future_meeting.meeting_key)
        .expect("future job exists");

    assert_eq!(summary.selected_due_jobs, 1);
    assert_eq!(summary.processed, 1);
    assert_eq!(summary.uploaded, 1);
    assert_eq!(future_job.status, JobStatus::WaitingForRetry);
    assert_eq!(
        harness.uploader.calls(),
        vec![due_meeting.meeting_key.clone()]
    );
}

#[tokio::test]
async fn offset_rfc3339_due_selection_uses_real_instants() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let utc_meeting = zoom_meeting("evt-offset-utc", "2026-04-16T12:00:00Z");
    let offset_due_meeting = zoom_meeting("evt-offset-due", "2026-04-16T12:05:00Z");
    let offset_future_meeting = zoom_meeting("evt-offset-future", "2026-04-16T12:10:00Z");

    let utc_source = zoom_source_ref("offset-utc-source");
    let offset_due_source = zoom_source_ref("offset-due-source");
    let offset_future_source = zoom_source_ref("offset-future-source");

    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(Vec::new()),
        fetcher: FakeTranscriptFetcher::default()
            .with_outcome(
                utc_source.cache_key(),
                FetchOutcome::Ready(artifact(utc_source.clone(), "UTC due first")),
            )
            .with_outcome(
                offset_due_source.cache_key(),
                FetchOutcome::Ready(artifact(offset_due_source.clone(), "Offset due second")),
            )
            .with_outcome(
                offset_future_source.cache_key(),
                FetchOutcome::Ready(artifact(offset_future_source.clone(), "Not due yet")),
            ),
        uploader: FakeTranscriptUploader::default()
            .with_success(
                utc_meeting.meeting_key.clone(),
                explicit_upload(&utc_meeting.meeting_key),
            )
            .with_success(
                offset_due_meeting.meeting_key.clone(),
                explicit_upload(&offset_due_meeting.meeting_key),
            )
            .with_success(
                offset_future_meeting.meeting_key.clone(),
                explicit_upload(&offset_future_meeting.meeting_key),
            ),
        ..TestHarness::default()
    };

    let mut utc_job = MeetingJob::new_discovered("studio".to_string(), utc_meeting.clone());
    utc_job.status = JobStatus::WaitingForRetry;
    utc_job.next_retry_at = Some("2026-04-16T13:00:00Z".to_string());
    utc_job.source_ref = Some(utc_source);
    harness.store.seed(utc_job);

    let mut offset_due_job =
        MeetingJob::new_discovered("studio".to_string(), offset_due_meeting.clone());
    offset_due_job.status = JobStatus::WaitingForRetry;
    offset_due_job.next_retry_at = Some("2026-04-16T09:15:00-04:00".to_string());
    offset_due_job.source_ref = Some(offset_due_source);
    harness.store.seed(offset_due_job);

    let mut offset_future_job =
        MeetingJob::new_discovered("studio".to_string(), offset_future_meeting.clone());
    offset_future_job.status = JobStatus::WaitingForRetry;
    offset_future_job.next_retry_at = Some("2026-04-16T09:45:00-04:00".to_string());
    offset_future_job.source_ref = Some(offset_future_source);
    harness.store.seed(offset_future_job);

    let summary = harness
        .execute(request_ending_at("2026-04-16T13:30:00Z"))
        .await;
    let future_job = harness
        .store
        .get(&offset_future_meeting.meeting_key)
        .expect("future job exists");

    assert_eq!(summary.selected_due_jobs, 2);
    assert_eq!(summary.processed, 2);
    assert_eq!(summary.uploaded, 2);
    assert_eq!(future_job.status, JobStatus::WaitingForRetry);
    assert_eq!(
        harness.uploader.calls(),
        vec![
            utc_meeting.meeting_key.clone(),
            offset_due_meeting.meeting_key.clone(),
        ]
    );
}

#[tokio::test]
async fn batch_limit_caps_work_per_tick() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting_a = zoom_meeting("evt-batch-a", "2026-04-16T09:50:00Z");
    let meeting_b = zoom_meeting("evt-batch-b", "2026-04-16T09:51:00Z");
    let meeting_c = zoom_meeting("evt-batch-c", "2026-04-16T09:52:00Z");

    let source_a = zoom_source_ref("batch-a");
    let source_b = zoom_source_ref("batch-b");
    let source_c = zoom_source_ref("batch-c");

    let harness = TestHarness {
        config: TranscriptSyncConfig {
            sync_batch_limit: 2,
            ..sample_config()
        },
        meeting_source: FakeMeetingSource::new(Vec::new()),
        resolver: FakeTranscriptSourceResolver::default()
            .with_outcome(
                meeting_a.meeting_key.clone(),
                SourceResolution::Resolved(source_a.clone()),
            )
            .with_outcome(
                meeting_b.meeting_key.clone(),
                SourceResolution::Resolved(source_b.clone()),
            )
            .with_outcome(
                meeting_c.meeting_key.clone(),
                SourceResolution::Resolved(source_c.clone()),
            ),
        fetcher: FakeTranscriptFetcher::default()
            .with_outcome(
                source_a.cache_key(),
                FetchOutcome::Ready(artifact(source_a.clone(), "A")),
            )
            .with_outcome(
                source_b.cache_key(),
                FetchOutcome::Ready(artifact(source_b.clone(), "B")),
            )
            .with_outcome(
                source_c.cache_key(),
                FetchOutcome::Ready(artifact(source_c.clone(), "C")),
            ),
        uploader: FakeTranscriptUploader::default()
            .with_success(
                meeting_a.meeting_key.clone(),
                explicit_upload(&meeting_a.meeting_key),
            )
            .with_success(
                meeting_b.meeting_key.clone(),
                explicit_upload(&meeting_b.meeting_key),
            )
            .with_success(
                meeting_c.meeting_key.clone(),
                explicit_upload(&meeting_c.meeting_key),
            ),
        ..TestHarness::default()
    };

    harness.store.seed(MeetingJob::new_discovered(
        "studio".to_string(),
        meeting_a.clone(),
    ));
    harness.store.seed(MeetingJob::new_discovered(
        "studio".to_string(),
        meeting_b.clone(),
    ));
    harness.store.seed(MeetingJob::new_discovered(
        "studio".to_string(),
        meeting_c.clone(),
    ));

    let summary = harness.execute(sample_request()).await;

    assert_eq!(summary.selected_due_jobs, 2);
    assert_eq!(summary.processed, 2);
    assert_eq!(summary.uploaded, 2);
    assert_eq!(harness.uploader.calls().len(), 2);
    let remaining = harness
        .store
        .get(&meeting_c.meeting_key)
        .expect("remaining job exists");
    assert_eq!(remaining.status, JobStatus::Discovered);
}

#[tokio::test]
async fn upload_failure_records_retryable_error() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = zoom_meeting("evt-upload-failure", "2026-04-16T10:00:00Z");
    let source_ref = zoom_source_ref("upload-failure-source");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Resolved(source_ref.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default().with_outcome(
            source_ref.cache_key(),
            FetchOutcome::Ready(artifact(source_ref.clone(), "Retryable upload failure")),
        ),
        uploader: FakeTranscriptUploader::default()
            .with_failure(meeting.meeting_key.clone(), "temporary R2 outage"),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(summary.retryable_errors, 1);
    assert_eq!(job.status, JobStatus::WaitingForRetry);
    assert_eq!(
        job.last_error_code.as_deref(),
        Some("upload_retryable_error")
    );
    assert_eq!(job.next_retry_at.as_deref(), Some("2026-04-16T10:05:00Z"));
}

#[tokio::test]
async fn permanent_source_failure_stops_retry_loop() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let meeting = gmeet_meeting("evt-permanent-failure", "2026-04-16T10:00:00Z");
    let source_ref = google_doc_source_ref("permanent-doc");
    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![meeting.clone()]),
        resolver: FakeTranscriptSourceResolver::default().with_outcome(
            meeting.meeting_key.clone(),
            SourceResolution::Resolved(source_ref.clone()),
        ),
        fetcher: FakeTranscriptFetcher::default().with_outcome(
            source_ref.cache_key(),
            FetchOutcome::PermanentFailure {
                code: "google_doc_export_failed".to_string(),
                message: "document export permanently denied".to_string(),
            },
        ),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;
    let job = harness.store.get(&meeting.meeting_key).expect("job exists");

    assert_eq!(summary.permanent_failures, 1);
    assert_eq!(job.status, JobStatus::PermanentFailure);
    assert_eq!(
        job.last_error_code.as_deref(),
        Some("google_doc_export_failed")
    );
    assert!(job.next_retry_at.is_none());
}

#[tokio::test]
async fn summary_counts_match_processed_results() {
    let _lock = TEST_LOCK.lock().expect("test lock");
    let uploaded_meeting = zoom_meeting("evt-summary-uploaded", "2026-04-16T09:50:00Z");
    let waiting_meeting = zoom_meeting("evt-summary-waiting", "2026-04-16T09:51:00Z");
    let permanent_meeting = gmeet_meeting("evt-summary-permanent", "2026-04-16T09:52:00Z");
    let unknown_meeting = unknown_meeting("evt-summary-unknown", "2026-04-16T09:53:00Z");

    let uploaded_source = zoom_source_ref("summary-uploaded");
    let waiting_source = zoom_source_ref("summary-waiting");
    let permanent_source = google_doc_source_ref("summary-permanent");

    let harness = TestHarness {
        meeting_source: FakeMeetingSource::new(vec![
            uploaded_meeting.clone(),
            waiting_meeting.clone(),
            permanent_meeting.clone(),
            unknown_meeting.clone(),
        ]),
        resolver: FakeTranscriptSourceResolver::default()
            .with_outcome(
                uploaded_meeting.meeting_key.clone(),
                SourceResolution::Resolved(uploaded_source.clone()),
            )
            .with_outcome(
                waiting_meeting.meeting_key.clone(),
                SourceResolution::Resolved(waiting_source.clone()),
            )
            .with_outcome(
                permanent_meeting.meeting_key.clone(),
                SourceResolution::Resolved(permanent_source.clone()),
            ),
        fetcher: FakeTranscriptFetcher::default()
            .with_outcome(
                uploaded_source.cache_key(),
                FetchOutcome::Ready(artifact(uploaded_source.clone(), "uploaded")),
            )
            .with_outcome(waiting_source.cache_key(), FetchOutcome::NotReady)
            .with_outcome(
                permanent_source.cache_key(),
                FetchOutcome::PermanentFailure {
                    code: "google_doc_missing".to_string(),
                    message: "document disappeared".to_string(),
                },
            ),
        uploader: FakeTranscriptUploader::default().with_success(
            uploaded_meeting.meeting_key.clone(),
            explicit_upload(&uploaded_meeting.meeting_key),
        ),
        ..TestHarness::default()
    };

    let summary = harness.execute(sample_request()).await;

    assert_eq!(summary.discovered, 4);
    assert_eq!(summary.selected_due_jobs, 4);
    assert_eq!(summary.processed, 4);
    assert_eq!(summary.uploaded, 1);
    assert_eq!(summary.waiting, 1);
    assert_eq!(summary.permanent_failures, 1);
    assert_eq!(summary.manual_review, 1);
}

#[test]
fn meeting_key_helper_stays_deterministic() {
    assert_eq!(
        meeting_key_from_event("evt-1", "2026-04-16T10:00:00Z"),
        "evt-1:2026-04-16T10:00:00Z"
    );
}
