#[cfg(feature = "host-bundle")]
use std::sync::Arc;

use dag_core::{FlowIR, NodeError, NodeResult};
use dag_macros::def_node;
use kernel_plan::{ValidatedIR, validate};

mod adapters;
pub mod cloudflare;
pub mod config;
pub mod domain;
mod engine;
pub mod state;

pub use adapters::fake;
pub use config::TranscriptSyncConfig;
pub use domain::{
    CompletedMeeting, ConferenceKind, ConferenceLocator, TranscriptArtifact, TranscriptSourceKind,
    TranscriptSourceRef, TranscriptSyncRequest, TranscriptSyncSummary, UploadedTranscript,
    meeting_key_from_event,
};
pub use state::{JobStatus, MeetingJob};

#[def_node(
    trigger,
    name = "TranscriptSyncTrigger",
    summary = "Ingress trigger for a manual or scheduled transcript sync request",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn transcript_sync_trigger(
    input: TranscriptSyncRequest,
) -> NodeResult<TranscriptSyncRequest> {
    Ok(input)
}

#[def_node(
    name = "FetchRecentCompletedMeetings",
    summary = "Fetch recently completed meetings from the configured meeting source",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn fetch_recent_completed_meetings(
    request: TranscriptSyncRequest,
) -> NodeResult<engine::DiscoveredMeetingsBatch> {
    engine::with_installed_runtime(|runtime| async move {
        engine::fetch_recent_completed_meetings(&runtime.services, &runtime.config, &request).await
    })
    .await
    .map_err(node_error)
}

#[def_node(
    name = "UpsertMeetingJobs",
    summary = "Upsert discovered meetings into the flow-local job ledger",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn upsert_meeting_jobs(
    batch: engine::DiscoveredMeetingsBatch,
) -> NodeResult<engine::UpsertedMeetingsBatch> {
    engine::with_installed_runtime(|runtime| async move {
        engine::upsert_meeting_jobs(&runtime.services, batch).await
    })
    .await
    .map_err(node_error)
}

#[def_node(
    name = "SelectDueJobs",
    summary = "Select only due jobs for the current reconciliation tick",
    effects = "ReadOnly",
    determinism = "BestEffort"
)]
async fn select_due_jobs(batch: engine::UpsertedMeetingsBatch) -> NodeResult<engine::DueJobsBatch> {
    engine::with_installed_runtime(|runtime| async move {
        engine::select_due_jobs(&runtime.services, &runtime.config, batch).await
    })
    .await
    .map_err(node_error)
}

#[def_node(
    name = "ReconcileDueJobs",
    summary = "Process due jobs with flow-local transcript source, retry, and upload policy",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn reconcile_due_jobs(batch: engine::DueJobsBatch) -> NodeResult<TranscriptSyncSummary> {
    engine::with_installed_runtime(|runtime| async move {
        engine::reconcile_due_jobs(&runtime.services, &runtime.config, batch).await
    })
    .await
    .map_err(node_error)
}

#[def_node(
    name = "CaptureSummary",
    summary = "Capture the current tick summary for operator inspection",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture_summary(summary: TranscriptSyncSummary) -> NodeResult<TranscriptSyncSummary> {
    Ok(summary)
}

mod bundle_def {
    #[cfg(feature = "host-bundle")]
    use super::{
        capture_summary_register, fetch_recent_completed_meetings_register,
        reconcile_due_jobs_register, select_due_jobs_register, transcript_sync_trigger_register,
        upsert_meeting_jobs_register,
    };
    use dag_macros::node;

    dag_macros::flow! {
        name: s14_meeting_transcript_sync_flow,
        version: "0.1.0",
        profile: Dev,
        summary: "Meeting transcript sync workflow with explicit reconcile stages and flow-local retry/source policy";

        let trigger = node!(transcript_sync_trigger);
        let fetch = node!(fetch_recent_completed_meetings);
        let upsert = node!(upsert_meeting_jobs);
        let select_due = node!(select_due_jobs);
        let reconcile = node!(reconcile_due_jobs);
        let capture = node!(capture_summary);

        connect!(trigger -> fetch);
        connect!(fetch -> upsert);
        connect!(upsert -> select_due);
        connect!(select_due -> reconcile);
        connect!(reconcile -> capture);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route_aliases: ["/meeting-transcript-sync/run"],
            method: "POST",
            deadline_ms: 30_000,
        });
    }
}

pub fn flow() -> FlowIR {
    bundle_def::flow()
}

pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("s14 meeting transcript sync flow should validate")
}

#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
pub fn bundle() -> host_inproc::FlowBundle {
    use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
    use kernel_exec::{NodeRegistry, RegistryResolver};
    use std::time::Duration;

    let validated_ir = validated_ir();
    let mut registry = NodeRegistry::new();
    transcript_sync_trigger_register(&mut registry).expect("register transcript_sync_trigger");
    fetch_recent_completed_meetings_register(&mut registry)
        .expect("register fetch_recent_completed_meetings");
    upsert_meeting_jobs_register(&mut registry).expect("register upsert_meeting_jobs");
    select_due_jobs_register(&mut registry).expect("register select_due_jobs");
    reconcile_due_jobs_register(&mut registry).expect("register reconcile_due_jobs");
    capture_summary_register(&mut registry).expect("register capture_summary");

    let node_contracts = flow()
        .nodes
        .iter()
        .map(|node| NodeContract {
            identifier: node.identifier.clone(),
            contract_hash: None,
            source: NodeSource::Local,
        })
        .collect();

    FlowBundle {
        validated_ir,
        entrypoints: vec![FlowEntrypoint {
            trigger_alias: "trigger".to_string(),
            capture_alias: "capture".to_string(),
            route_path: Some("/meeting-transcript-sync/run".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(30_000)),
            route_aliases: vec!["/meeting-transcript-sync/run".to_string()],
        }],
        resolver: Arc::new(RegistryResolver::new(Arc::new(registry))),
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

#[cfg(test)]
mod tests;
