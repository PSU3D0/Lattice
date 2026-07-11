use super::*;
use std::sync::{Arc, Mutex};

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use capabilities::kv::{KeyValue, MemoryKv};
use capabilities::scoped::ScopedResources;
use capabilities::{Capability, ResourceAccess, ResourceBag, context};
use connector_google_drive::GoogleDrivePermissionSummary;
use connector_google_drive::runtime::transport::EnvConnectorRuntime;
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

/// 2026-07-02T06:00:00Z — one daily fire.
const FIRE_MS: u64 = 1_782_972_000_000;

// ---- In-memory checkpoint store (same shape as s15's test double) --------

#[derive(Default)]
struct MemoryCheckpointStore {
    records: Mutex<BTreeMap<String, CheckpointRecord>>,
}

impl Capability for MemoryCheckpointStore {
    fn name(&self) -> &'static str {
        "checkpoint_store.memory"
    }
}

#[async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
        let handle = CheckpointHandle {
            checkpoint_id: record.checkpoint_id.clone(),
            flow_id: record.flow_id.clone(),
            run_id: record.run_id.clone(),
        };
        self.records
            .lock()
            .expect("records")
            .insert(record.checkpoint_id.clone(), record);
        Ok(handle)
    }

    async fn get(&self, handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError> {
        self.records
            .lock()
            .expect("records")
            .get(&handle.checkpoint_id)
            .cloned()
            .ok_or(CheckpointError::NotFound)
    }

    async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
        self.records
            .lock()
            .expect("records")
            .remove(&handle.checkpoint_id);
        Ok(())
    }

    async fn lease(
        &self,
        handle: &CheckpointHandle,
        ttl: Duration,
    ) -> Result<Lease, CheckpointError> {
        Ok(Lease {
            lease_id: format!("lease:{}", handle.checkpoint_id),
            expires_at_ms: ttl.as_millis().try_into().unwrap_or(u64::MAX),
        })
    }

    async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
        Ok(())
    }

    async fn list(
        &self,
        _filter: CheckpointFilter,
    ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
        Ok(Vec::new())
    }
}

// ---- Env plumbing ---------------------------------------------------------

const SHEETS_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
const DRIVE_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_DRIVE_DEFAULT_BASE_URL";
const GMAIL_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

// ---- Fixtures --------------------------------------------------------------

fn permission(
    id: &str,
    grantee_type: &str,
    role: &str,
    email: Option<&str>,
) -> GoogleDrivePermissionSummary {
    GoogleDrivePermissionSummary {
        permission_id: Some(id.to_string()),
        grantee_type: Some(grantee_type.to_string()),
        role: Some(role.to_string()),
        email_address: email.map(str::to_string),
    }
}

fn file(
    id: &str,
    name: &str,
    shared: bool,
    permissions: Vec<GoogleDrivePermissionSummary>,
) -> GoogleDriveFileHit {
    GoogleDriveFileHit {
        id: id.to_string(),
        name: Some(name.to_string()),
        mime_type: Some("application/vnd.google-apps.document".to_string()),
        shared: Some(shared),
        web_view_link: None,
        permissions,
    }
}

fn owner_grant() -> GoogleDrivePermissionSummary {
    permission("perm-owner", "user", "owner", Some("me@lattice-pilot.test"))
}

fn scheduled_event(scheduled_time_ms: u64) -> serde_json::Value {
    serde_json::to_value(ScheduledEvent {
        scheduled_time_ms,
        cron: SCHEDULE_CRON.to_string(),
    })
    .expect("serialize scheduled event")
}

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn utc_date_handles_epoch_and_leap_years() {
    assert_eq!(utc_date(0), "1970-01-01");
    assert_eq!(utc_date(86_400_000), "1970-01-02");
    // 2024-02-29 (leap day): 2024-03-01T00:00:00Z minus one day.
    assert_eq!(utc_date(1_709_164_800_000), "2024-02-29");
    assert_eq!(utc_date(FIRE_MS), "2026-07-02");
}

#[test]
fn utc_rfc3339_formats_time_of_day() {
    assert_eq!(utc_rfc3339(0), "1970-01-01T00:00:00Z");
    assert_eq!(utc_rfc3339(FIRE_MS), "2026-07-02T06:00:00Z");
}

#[tokio::test]
async fn plan_audit_is_a_pure_function_of_the_fire() {
    let plan = plan_audit(ScheduledEvent {
        scheduled_time_ms: FIRE_MS,
        cron: SCHEDULE_CRON.to_string(),
    })
    .await
    .expect("plan");

    assert_eq!(plan.date, "2026-07-02");
    assert_eq!(plan.sheet_title, "audit-20260702");
    assert_eq!(
        plan.drive_query,
        "modifiedTime > '2026-07-01T06:00:00Z' and trashed = false"
    );
    assert_eq!(
        audit_key(FIRE_MS),
        format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{FIRE_MS}")
    );
}

#[test]
fn risk_rule_flags_public_and_external_but_not_internal() {
    let public = file(
        "doc-public",
        "roadmap",
        true,
        vec![owner_grant(), permission("p1", "anyone", "reader", None)],
    );
    let external = file(
        "doc-external",
        "budget",
        true,
        vec![
            owner_grant(),
            permission("p2", "user", "writer", Some("partner@othercorp.test")),
        ],
    );
    let internal = file(
        "doc-internal",
        "notes",
        true,
        vec![
            owner_grant(),
            permission("p3", "user", "writer", Some("teammate@lattice-pilot.test")),
        ],
    );

    assert!(file_is_flagged(&public));
    assert!(file_is_flagged(&external));
    assert!(!file_is_flagged(&internal));

    // Owner grants never become audit rows.
    let rows = rows_for_file(&external);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].user, "partner@othercorp.test");
    assert_eq!(rows[0].role, "writer");
    assert_eq!(rows[0].share_type, "user");
}

#[test]
fn report_text_lists_public_and_external_sections() {
    let plan = AuditPlan {
        scheduled_time_ms: FIRE_MS,
        cron: SCHEDULE_CRON.to_string(),
        date: "2026-07-02".to_string(),
        sheet_title: "audit-20260702".to_string(),
        drive_query: String::new(),
    };
    let sheet = AuditSheet {
        plan,
        spreadsheet_id: AUDIT_SPREADSHEET_ID.to_string(),
        sheet_id: 88,
    };
    let findings = AuditFindings {
        sheet,
        files_flagged: 2,
        rows: vec![
            AuditRow {
                file_id: "doc-public".into(),
                file_name: "roadmap".into(),
                share_type: "anyone".into(),
                user_id: "p1".into(),
                user: "unknown".into(),
                role: "reader".into(),
            },
            AuditRow {
                file_id: "doc-external".into(),
                file_name: "budget".into(),
                share_type: "user".into(),
                user_id: "p2".into(),
                user: "partner@othercorp.test".into(),
                role: "writer".into(),
            },
        ],
    };

    let text = report_text(&findings);
    assert!(text.contains("2026-07-02"));
    assert!(text.contains("Open to anyone with the link:"));
    assert!(text.contains("roadmap (doc-public)"));
    assert!(text.contains("Shared with external accounts:"));
    assert!(text.contains("budget (doc-external) -> partner@othercorp.test"));
    assert!(text.contains("audit-20260702"));
}

// ---- Flow validation + requirements ----------------------------------------

#[test]
fn flow_validates_and_derives_schedule_requirement() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    let trigger = requirements
        .triggers
        .iter()
        .find(|t| t.alias == TRIGGER_ALIAS)
        .expect("audit_trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Schedule);
    assert_eq!(trigger.crons, vec![SCHEDULE_CRON.to_string()]);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("schedule entrypoint requirement present");
    assert_eq!(entrypoint.schedule.as_deref(), Some(SCHEDULE_CRON));
    assert_eq!(entrypoint.capture_alias, "capture");
}

#[test]
fn flow_shape_declares_honest_effects_per_node() {
    let ir = flow();

    let hints = |alias: &str| -> Vec<String> {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .unwrap_or_else(|| panic!("node `{alias}` present"))
            .effect_hints
            .clone()
    };

    // Read-only Drive search: http_read only.
    assert!(hints("fetch_recent_files").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(!hints("fetch_recent_files").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Sheet writes: append also reads the header row (http_read + http_write).
    assert!(hints("append_audit_rows").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(hints("append_audit_rows").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(hints("create_audit_sheet").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Gmail send: http_write only.
    assert!(hints("send_audit_report").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("send_audit_report").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Terminal KV upsert.
    assert!(hints("record_audit").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its op identifier (connector-prefixed so the
    // runtime can infer the connection scope).
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert_eq!(
        identifier("create_audit_sheet"),
        "connector.google.sheets.create_audit_sheet"
    );
    assert_eq!(
        identifier("fetch_recent_files"),
        "connector.google.drive.fetch_recent_files"
    );
    assert_eq!(
        identifier("append_audit_rows"),
        "connector.google.sheets.append_audit_rows"
    );
    assert_eq!(
        identifier("send_audit_report"),
        "connector.google.gmail.send_audit_report"
    );
}

// ---- record_audit honesty + idempotency (s15 pattern) -----------------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_report(scheduled_time_ms: u64) -> ReportedAudit {
    ReportedAudit {
        scheduled_time_ms,
        date: utc_date(scheduled_time_ms),
        files_flagged: 2,
        rows_appended: 2,
        message_id: "msg-1".to_string(),
    }
}

#[tokio::test]
async fn record_audit_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_audit",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_audit(sample_report(FIRE_MS))
            .await
            .expect("write succeeds under declared hints")
    })
    .await;

    assert!(record.stored);
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn record_audit_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_audit", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_audit(sample_report(FIRE_MS))
            .await
            .expect_err("write must fail when kv is not granted")
    })
    .await;
    assert!(
        err.to_string().contains("KV capability"),
        "expected a missing-kv error, got: {err}"
    );

    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|d| d.capability == "kv"),
        "expected a CAP110 kv denial, got: {denials:?}"
    );
}

#[tokio::test]
async fn record_audit_is_idempotent_on_scheduled_time() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_audit",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_audit(sample_report(FIRE_MS)).await.expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_audit(sample_report(FIRE_MS))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_audit(sample_report(FIRE_MS + 86_400_000))
            .await
            .expect("next day's fire")
    })
    .await;
    assert!(other.stored, "a distinct fire writes its own record");
    assert_ne!(first.key, other.key);
}

// ---- Compose: fire -> sheet tab -> drive scan -> rows -> email -> record ----

// The env lock is held across awaits deliberately: the process-global
// endpoint/auth env vars must not be mutated by a parallel test mid-fire.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn scheduled_fire_audits_drive_and_records_idempotent_summary() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _sheets = EnvGuard::set(SHEETS_ENDPOINT_ENV, &server.base_url());
    let _drive = EnvGuard::set(DRIVE_ENDPOINT_ENV, &server.base_url());
    let _gmail = EnvGuard::set(GMAIL_ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "s16-test-token");

    // 1. create_sheet -> spreadsheets batchUpdate (addSheet reply).
    let create_sheet = server.mock(|when, then| {
        when.method(POST).path_contains("batchUpdate");
        then.status(200).json_body_obj(&serde_json::json!({
            "replies": [
                { "addSheet": { "properties": { "sheetId": 88, "title": "audit-20260702" } } }
            ]
        }));
    });

    // 2. drive search: one public file, one externally shared, one internal.
    let drive_search = server.mock(|when, then| {
        when.method(GET)
            .path("/drive/v3/files")
            .header("authorization", "Bearer s16-test-token")
            .query_param(
                "q",
                "modifiedTime > '2026-07-01T06:00:00Z' and trashed = false",
            );
        then.status(200).json_body_obj(&serde_json::json!({
            "files": [
                {
                    "id": "doc-public", "name": "roadmap", "shared": true,
                    "mimeType": "application/vnd.google-apps.document",
                    "permissions": [
                        { "id": "perm-owner", "type": "user", "role": "owner",
                          "emailAddress": "me@lattice-pilot.test" },
                        { "id": "p1", "type": "anyone", "role": "reader" }
                    ]
                },
                {
                    "id": "doc-external", "name": "budget", "shared": true,
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "permissions": [
                        { "id": "perm-owner", "type": "user", "role": "owner",
                          "emailAddress": "me@lattice-pilot.test" },
                        { "id": "p2", "type": "user", "role": "writer",
                          "emailAddress": "partner@othercorp.test" }
                    ]
                },
                {
                    "id": "doc-internal", "name": "notes", "shared": true,
                    "mimeType": "application/vnd.google-apps.document",
                    "permissions": [
                        { "id": "perm-owner", "type": "user", "role": "owner",
                          "emailAddress": "me@lattice-pilot.test" },
                        { "id": "p3", "type": "user", "role": "writer",
                          "emailAddress": "teammate@lattice-pilot.test" }
                    ]
                }
            ]
        }));
    });

    // 3. append_row reads the tab's header row before each write...
    let values_read = server.mock(|when, then| {
        when.method(GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [["file_id", "file_name", "share_type", "user_id", "user", "role"]]
        }));
    });
    // ...then appends the ordered row.
    let values_append = server.mock(|when, then| {
        when.method(POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'audit-20260702'!A2:F2" }
        }));
    });

    // 4. gmail send.
    let gmail_send = server.mock(|when, then| {
        when.method(POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s16-test-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "msg-audit-1", "threadId": "thread-audit-1", "labelIds": ["SENT"]
        }));
    });

    let kv = Arc::new(MemoryKv::new());
    let bag = ResourceBag::default()
        .with_http_read(Arc::new(ReqwestHttpClient::default()))
        .with_http_write(Arc::new(ReqwestHttpClient::default()))
        .with_connector_runtime(Arc::new(EnvConnectorRuntime))
        .with_checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
        .with_kv(kv.clone());

    let bundle = bundle();
    let runtime =
        HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir)).with_resource_bag(bag);

    let fire = || Invocation::new(TRIGGER_ALIAS, "capture", scheduled_event(FIRE_MS));

    // First delivery of the fire.
    let first = runtime.execute(fire()).await.expect("first fire runs");
    let first = match first {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<AuditRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the first fire"),
    };
    assert!(first.stored);
    assert_eq!(first.files_flagged, 2);
    assert_eq!(first.rows_appended, 2);
    assert_eq!(first.message_id, "msg-audit-1");
    assert_eq!(first.key, audit_key(FIRE_MS));

    create_sheet.assert_hits(1);
    drive_search.assert_hits(1);
    values_read.assert_hits(2);
    values_append.assert_hits(2);
    gmail_send.assert_hits(1);

    // Redelivery of the SAME fire: payloads are byte-identical (pure functions
    // of scheduled_time_ms) and the terminal KV record dedupes.
    let second = runtime.execute(fire()).await.expect("redelivery runs");
    let second = match second {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<AuditRecord>(value).expect("decode record")
        }
        _ => panic!("expected a value result from the redelivery"),
    };
    assert!(
        !second.stored,
        "redelivery must not write a duplicate record"
    );
    assert_eq!(second.key, first.key);

    // Exactly one durable record for the fire.
    let raw = kv
        .get(&audit_key(FIRE_MS))
        .await
        .expect("kv get")
        .expect("record present");
    let row: AuditRecord = serde_json::from_slice(&raw).expect("decode row");
    assert_eq!(row.files_flagged, 2);
}
