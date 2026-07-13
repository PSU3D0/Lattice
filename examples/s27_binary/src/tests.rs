//! In-crate acceptance tests for the S27 byte-plane flow (packet H5d), driven
//! through the real `host-inproc` runtime. The separate CLI golden proves the
//! report/egress path with CLI-provisioned workspace; these tests own the mirror
//! ingress path and the negative workspace-grant proof. Each runtime test wires:
//!
//! - an `FsWorkspaceFactory` over a temp dir (the run-scoped workspace the byte
//!   plane stages into) plus a stable `with_workspace_root_key` (macaroon master
//!   key — §16.2);
//! - the `connector.http` runtime (`EnvConnectorRuntime`) + an `endpoint.profile`
//!   pointed at an `httpmock` server via `LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL`;
//! - the http read/write capability (`ReqwestHttpClient`).
//!
//! and asserts the compositional byte plane end-to-end: the CSV is staged and
//! the multipart POST reaches the mock with the expected body; a download is
//! staged, transformed, and re-uploaded; and — the load-bearing negative —
//! a read-only-granted staging node is denied `MissingWorkspaceWrite` through
//! the real runtime (the §16.4 read/write split, proven compositionally).

use super::*;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cap_http_reqwest::ReqwestHttpClient;
use cap_workspace_fs::{FsWorkspaceConfig, FsWorkspaceFactory};
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use capabilities::workspace::WorkspacePolicy;
use capabilities::{Capability, ResourceBag};
use connector_http::runtime::transport::EnvConnectorRuntime;
use dag_core::requirements::TriggerKind;
use dag_core::{EffectHint, Effects};
use host_inproc::{HostExecutionError, HostExecutionResult, HostRuntime, Invocation};
use httpmock::Method::{GET, POST};
use httpmock::MockServer;
use kernel_plan::derive_requirements;

// ---- In-memory checkpoint store (s26 test double) --------------------------

#[derive(Default)]
struct MemoryCheckpointStore {
    records: Mutex<BTreeMap<String, CheckpointRecord>>,
}

impl Capability for MemoryCheckpointStore {
    fn name(&self) -> &'static str {
        "checkpoint_store.memory"
    }
}

#[async_trait::async_trait]
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

// ---- Env plumbing ----------------------------------------------------------

const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL";
const TEST_MASTER_KEY: &[u8] = b"s27-binary-test-master-key-0000000000000000";

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

// ---- Runtime wiring --------------------------------------------------------

fn base_bag() -> ResourceBag {
    let client = Arc::new(ReqwestHttpClient::default());
    ResourceBag::default()
        .with_http_read(Arc::clone(&client))
        .with_http_write(client)
        .with_connector_runtime(Arc::new(EnvConnectorRuntime))
        .with_checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
}

/// Build a `HostRuntime` for a bundle, wiring the FS workspace factory (temp
/// dir) + a stable macaroon master key. Returns the runtime and the tempdir
/// (kept alive for the workspace's lifetime).
fn runtime_for(bundle: host_inproc::FlowBundle) -> (HostRuntime, tempfile::TempDir) {
    let temp = tempfile::tempdir().expect("tempdir");
    let factory = FsWorkspaceFactory::new(FsWorkspaceConfig {
        root: temp.path().to_path_buf(),
        policy: WorkspacePolicy::default(),
    });
    let runtime = HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir))
        .with_resource_bag(base_bag())
        .with_workspace_factory(Arc::new(factory))
        .with_workspace_root_key(TEST_MASTER_KEY.to_vec());
    (runtime, temp)
}

// ---- Flow validation + shape -----------------------------------------------

fn node<'a>(ir: &'a dag_core::FlowIR, alias: &str) -> &'a dag_core::NodeIR {
    ir.nodes
        .iter()
        .find(|n| n.alias == alias)
        .unwrap_or_else(|| panic!("node `{alias}` present"))
}

fn has_hint(node: &dag_core::NodeIR, hint: EffectHint) -> bool {
    node.effect_hints.contains(&hint.as_str().to_string())
}

#[test]
fn render_csv_is_a_pure_serialization() {
    let csv = render_csv(&[
        Row {
            label: "alpha".to_string(),
            value: 1,
        },
        Row {
            label: "beta".to_string(),
            value: 2,
        },
    ]);
    assert_eq!(csv, "label,value\nalpha,1\nbeta,2\n");
}

#[test]
fn flow_validates_and_derives_both_http_entrypoints() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    for alias in [REPORT_TRIGGER, MIRROR_TRIGGER] {
        let trigger = requirements
            .triggers
            .iter()
            .find(|t| t.alias == alias)
            .unwrap_or_else(|| panic!("trigger `{alias}` present"));
        assert_eq!(trigger.kind, TriggerKind::Http);
    }
}

#[test]
fn byte_nodes_carry_the_honest_static_effect_floors() {
    let ir = flow();

    // Egress build: workspace::write, Effectful (spec §16.3).
    let build = node(&ir, "build_report");
    assert_eq!(build.effects, Effects::Effectful);
    assert!(has_hint(build, EffectHint::WorkspaceWrite));
    assert!(!has_hint(build, EffectHint::WorkspaceRead));

    // Egress upload (post_multipart): http::write + workspace::read, Effectful.
    let upload = node(&ir, "upload_report");
    assert_eq!(upload.effects, Effects::Effectful);
    assert_eq!(
        upload.connector_ops.first().unwrap().operation_id,
        "connector.http.post_multipart"
    );
    assert!(has_hint(upload, EffectHint::HttpWrite));
    assert!(has_hint(upload, EffectHint::WorkspaceRead));

    // Ingress download (get_binary): http::read + workspace::write, Effectful
    // (run-local-idempotent — no exactly-once edge required, spec §16.3/F5).
    let download = node(&ir, "download");
    assert_eq!(download.effects, Effects::Effectful);
    assert_eq!(
        download.connector_ops.first().unwrap().operation_id,
        "connector.http.get_binary"
    );
    assert!(has_hint(download, EffectHint::HttpRead));
    assert!(has_hint(download, EffectHint::WorkspaceWrite));
    // A binary GET does NOT masquerade as an http write.
    assert!(!has_hint(download, EffectHint::HttpWrite));

    // Ingress transform: reads and re-stages — both workspace grants.
    let transform = node(&ir, "transform");
    assert_eq!(transform.effects, Effects::Effectful);
    assert!(has_hint(transform, EffectHint::WorkspaceRead));
    assert!(has_hint(transform, EffectHint::WorkspaceWrite));

    // Connector nodes carry the load-bearing `connector.http.*` identifier that
    // resolves their bound-connection scope at runtime (s15/s26 finding).
    assert_eq!(download.identifier, "connector.http.download");
    assert_eq!(upload.identifier, "connector.http.upload_report");
}

// ---- Egress: CSV build → stage → multipart POST ----------------------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn egress_builds_csv_stages_and_multipart_posts_it() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    // The mock asserts the multipart body carries the artifact part (text/csv)
    // with the CSV content built in-Rust, plus the inline "kind" part.
    let upload = server.mock(|when, then| {
        when.method(POST)
            .path("/upload")
            .header_exists("content-type")
            .body_contains("name=\"file\"")
            .body_contains("Content-Type: text/csv")
            .body_contains("label,value")
            .body_contains("alpha,1")
            .body_contains("beta,2")
            .body_contains("name=\"kind\"")
            .body_contains("daily");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": true }));
    });

    let (runtime, _temp) = runtime_for(bundle());

    let payload = serde_json::json!({
        "rows": [
            { "label": "alpha", "value": 1 },
            { "label": "beta", "value": 2 }
        ]
    });
    let receipt = match runtime
        .execute(Invocation::new(REPORT_TRIGGER, "upload_report", payload))
        .await
        .expect("egress runs")
    {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<UploadReceipt>(value).expect("decode receipt")
        }
        _ => panic!("expected a value result"),
    };

    upload.assert();
    assert!(receipt.ok, "the mock accepted the upload");
    assert_eq!(receipt.artifact_name, REPORT_STAGE_NAME);
    assert_eq!(receipt.content_type, "text/csv");
    assert_eq!(
        receipt.len,
        render_csv(&[
            Row {
                label: "alpha".to_string(),
                value: 1
            },
            Row {
                label: "beta".to_string(),
                value: 2
            },
        ])
        .len() as u64
    );
}

// ---- Ingress: download → transform → re-upload -----------------------------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn ingress_downloads_transforms_and_reuploads_the_artifact() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let source = server.mock(|when, then| {
        when.method(GET).path("/source.csv");
        then.status(200)
            .header("content-type", "text/csv")
            .body("a,b\n1,2\n");
    });
    // The transform upper-cases the bytes; the mirror upload must carry the
    // TRANSFORMED content, proving the download was staged, dereffed, processed,
    // and re-staged through the workspace views.
    let mirror = server.mock(|when, then| {
        when.method(POST)
            .path("/mirror-upload")
            .body_contains("name=\"file\"")
            .body_contains("A,B")
            .body_contains("1,2");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": true }));
    });

    let (runtime, _temp) = runtime_for(bundle());

    let payload = serde_json::json!({ "source_path": "/source.csv" });
    let receipt = match runtime
        .execute(Invocation::new(MIRROR_TRIGGER, "reupload", payload))
        .await
        .expect("ingress runs")
    {
        HostExecutionResult::Value(value) => {
            serde_json::from_value::<UploadReceipt>(value).expect("decode receipt")
        }
        _ => panic!("expected a value result"),
    };

    source.assert();
    mirror.assert();
    assert!(receipt.ok);
    assert_eq!(receipt.artifact_name, PROCESSED_STAGE_NAME);
    assert_eq!(receipt.content_type, "text/csv");
    assert_eq!(receipt.len, "a,b\n1,2\n".len() as u64);
}

// ---- Negative: the read/write split holds through the real runtime ---------

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn denied_staging_node_is_refused_missing_workspace_write() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    // This upload must NEVER be reached: the staging node is denied before the
    // artifact ever exists, so the flow fails upstream of any egress.
    let upload = server.mock(|when, then| {
        when.method(POST).path("/upload");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": true }));
    });

    let (runtime, _temp) = runtime_for(denied_bundle());

    let payload = serde_json::json!({ "rows": [ { "label": "alpha", "value": 1 } ] });
    let err = match runtime
        .execute(Invocation::new(REPORT_TRIGGER, "upload_report", payload))
        .await
    {
        Err(err) => err,
        Ok(_) => panic!("a read-only-granted node must be denied when it tries to stage"),
    };

    // The workspace IS present (factory-bound) and the node CLAIMS Effectful, but
    // its `resource::workspace::read`-only grant cannot reach the write view, so
    // `workspace_write()` returns None → MissingWorkspaceWrite, surfaced as a
    // node failure at `build_report_ungranted`.
    match &err {
        HostExecutionError::NodeFailed { alias, .. } => {
            assert_eq!(alias, "build_report_ungranted");
        }
        other => panic!("expected a NodeFailed denial, got {other:?}"),
    }
    assert!(
        format!("{err}").contains("MissingWorkspaceWrite"),
        "denial must name the withheld workspace::write grant, got: {err}"
    );
    assert_eq!(upload.hits(), 0, "denial must precede any egress");
}
