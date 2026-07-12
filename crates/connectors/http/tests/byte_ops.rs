//! Runtime + capability-honesty tests for the connector.http byte-plane ops
//! (spec §16.5, packet H5c-ops native):
//!
//! - `get_binary` downloads from an httpmock server and returns an `Artifact`
//!   whose staged bytes round-trip (read back through a `workspace_read()`
//!   VIEW), with the response `Content-Type` and a content hash carried;
//! - `post_multipart` assembles one inline part + one artifact part into a
//!   well-formed `multipart/form-data` body the mock receives;
//! - non-2xx on `get_binary` → NodeError (HTTP101), unchanged from §5;
//! - **honesty (load-bearing):** a `get_binary` denied the `workspace::write`
//!   grant fails at the staging step (`MissingWorkspaceWrite` + a recorded
//!   CAP110 `workspace_write` denial); a `post_multipart` denied
//!   `workspace::read` fails dereferencing its artifact part
//!   (`MissingWorkspaceRead` + a recorded `workspace_read` denial), before any
//!   request is sent — the byte-plane analogue of the §4 MissingHttp* tests.
#![allow(clippy::await_holding_lock)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::scoped::ScopedResources;
use capabilities::workspace::{
    Workspace, WorkspaceDeleteResult, WorkspaceEntry, WorkspaceError, WorkspaceListOptions,
    WorkspaceReadResult, WorkspaceWriteOptions, WorkspaceWriteResult,
};
use capabilities::{Capability, ResourceAccess, ResourceBag, context};
use connector_http::form;
use connector_http::ops::{HttpGetBinary, HttpPostMultipart};
use connector_http::runtime::errors::HttpConnectorError;
use connector_http::runtime::transport::EnvConnectorRuntime;
use connector_http::{HttpGetBinaryInput, HttpMultipartInput};
use dag_core::EffectHint;
use httpmock::Method::{GET, POST};
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL";

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(previous) => unsafe {
                std::env::set_var(self.key, previous);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

// ---- In-memory workspace ---------------------------------------------------

#[derive(Default)]
struct MemoryWorkspace {
    files: Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl Capability for MemoryWorkspace {
    fn name(&self) -> &'static str {
        "workspace.byte-ops-test"
    }
}

#[async_trait]
impl Workspace for MemoryWorkspace {
    async fn read_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
        Ok(self
            .files
            .lock()
            .unwrap()
            .get(normalized_path)
            .cloned()
            .map(WorkspaceReadResult::Bytes))
    }

    async fn write_normalized(
        &self,
        normalized_path: &str,
        data: &[u8],
        _options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, WorkspaceError> {
        self.files
            .lock()
            .unwrap()
            .insert(normalized_path.to_string(), data.to_vec());
        Ok(WorkspaceWriteResult {
            path: normalized_path.to_string(),
            size_bytes: data.len() as u64,
            updated_at_ms: 0,
        })
    }

    async fn list_normalized(
        &self,
        _options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
        Ok(Vec::new())
    }

    async fn delete_normalized(
        &self,
        _normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
        Ok(WorkspaceDeleteResult { deleted: false })
    }
}

fn full_bag() -> Arc<ResourceBag> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_workspace(Arc::new(MemoryWorkspace::default()))
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://tests",
                "byte_ops_test",
                "connector.http.test",
                "connector.http",
            )),
    )
}

fn scoped(op_meta: &dag_core::ConnectorOpMetadata, grants: &[EffectHint]) -> Arc<ScopedResources> {
    Arc::new(ScopedResources::new(
        op_meta.operation_id,
        full_bag(),
        grants.iter().copied(),
    ))
}

// ---- get_binary: download → Artifact → round-trip --------------------------

#[tokio::test]
async fn get_binary_stages_body_and_round_trips_through_the_read_view() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let payload = b"%PDF-1.7 binary bytes \x00\x01\x02".to_vec();
    let mock = server.mock(|when, then| {
        when.method(GET).path("/report.pdf");
        then.status(200)
            .header("content-type", "application/pdf")
            .body(&payload);
    });

    let bag = full_bag();
    let view: Arc<dyn ResourceAccess> = bag.clone();
    let artifact = context::with_resources(view, async {
        HttpGetBinary::invoke(&HttpGetBinaryInput {
            path: "/report.pdf".to_string(),
            ..Default::default()
        })
        .await
        .expect("get_binary stages an artifact")
    })
    .await;

    mock.assert();
    assert_eq!(artifact.content_type, "application/pdf");
    assert_eq!(artifact.len, payload.len() as u64);
    assert_eq!(
        artifact.content_hash.as_deref(),
        Some(capabilities::artifact::sha256_hex(&payload).as_str())
    );

    // Read the staged bytes back through the workspace_read() VIEW (gates 2+3),
    // proving the returned handle actually dereferences to the downloaded body.
    let reader = bag.workspace_read().expect("read view available");
    let bytes = reader.read(&artifact.handle).await.expect("deref handle");
    assert_eq!(bytes, payload);
}

#[tokio::test]
async fn get_binary_honors_explicit_stage_name() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(GET).path("/data");
        then.status(200).body("csv,data\n");
    });

    let bag = full_bag();
    let view: Arc<dyn ResourceAccess> = bag.clone();
    let artifact = context::with_resources(view, async {
        HttpGetBinary::invoke(&HttpGetBinaryInput {
            path: "/data".to_string(),
            stage_name: Some("out/named.csv".to_string()),
            ..Default::default()
        })
        .await
        .expect("get_binary succeeds")
    })
    .await;

    mock.assert();
    match artifact.handle.scope() {
        capabilities::HandleScope::Exact(path) => assert_eq!(path, "out/named.csv"),
        other => panic!("expected an exact handle scope, got {other:?}"),
    }
}

#[tokio::test]
async fn get_binary_non_2xx_maps_to_http101() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(GET).path("/missing.bin");
        then.status(503).body("upstream unavailable");
    });

    let bag = full_bag();
    let view: Arc<dyn ResourceAccess> = bag.clone();
    let err = context::with_resources(view, async {
        HttpGetBinary::invoke(&HttpGetBinaryInput {
            path: "/missing.bin".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("non-2xx must fail with no staged artifact")
    })
    .await;

    mock.assert();
    assert_eq!(err.code(), Some("HTTP101"));
}

// ---- post_multipart: inline + artifact parts -> well-formed body -----------

#[tokio::test]
async fn post_multipart_sends_inline_and_artifact_parts() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/upload")
            .header_exists("content-type")
            .body_contains("name=\"file\"")
            .body_contains("Content-Type: text/csv")
            .body_contains("a,b\n1,2\n")
            .body_contains("name=\"kind\"")
            .body_contains("daily");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": true }));
    });

    let bag = full_bag();

    // Stage a real artifact through the write view, then reference it as a part.
    let artifact = bag
        .workspace_write()
        .expect("write view")
        .stage_artifact("uploads/report.csv", b"a,b\n1,2\n", "text/csv")
        .await
        .expect("stage artifact");

    let view: Arc<dyn ResourceAccess> = bag.clone();
    let output = context::with_resources(view, async {
        HttpPostMultipart::invoke(&HttpMultipartInput {
            path: "/upload".to_string(),
            query: Vec::new(),
            headers: BTreeMap::new(),
            parts: form! { "file" => artifact, "kind" => "daily" },
            target: None,
        })
        .await
        .expect("post_multipart succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.body, serde_json::json!({ "ok": true }));
}

// ---- Honesty: workspace grants are load-bearing ----------------------------

#[tokio::test]
async fn get_binary_denied_workspace_write_fails_at_staging() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(GET).path("/report.pdf");
        then.status(200).body("bytes");
    });

    // Grant only http_read: the GET succeeds, but staging is denied because the
    // node lacks workspace::write. (An empty grant set would be denied earlier,
    // at http_read; granting it isolates the workspace-stage gate — the
    // load-bearing byte-plane denial.)
    let scoped = scoped(&HttpGetBinary::META, &[EffectHint::HttpRead]);
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    let err = context::with_resources(view, async {
        HttpGetBinary::invoke(&HttpGetBinaryInput {
            path: "/report.pdf".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("staging must be denied without workspace::write")
    })
    .await;

    assert_eq!(mock.hits(), 1, "the GET runs; only staging is denied");
    assert!(
        matches!(err, HttpConnectorError::MissingWorkspaceWrite),
        "expected MissingWorkspaceWrite, got {err:?}"
    );
    assert_eq!(err.code(), Some("HTTP110"));

    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|d| d.capability == "workspace_write"),
        "expected a workspace_write CAP110 denial, got: {denials:?}"
    );
}

#[tokio::test]
async fn post_multipart_denied_workspace_read_fails_dereferencing_artifact_part() {
    let _lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

    let mock = server.mock(|when, then| {
        when.method(POST).path("/upload");
        then.status(200).body("{}");
    });

    let bag = full_bag();
    // Stage a real artifact (under the unscoped bag) to reference as a part.
    let artifact = bag
        .workspace_write()
        .expect("write view")
        .stage_artifact("uploads/report.csv", b"a,b\n1,2\n", "text/csv")
        .await
        .expect("stage artifact");

    // Grant http_write but NOT workspace_read: dereferencing the artifact part
    // must be denied before any request is sent.
    let scoped = Arc::new(ScopedResources::new(
        HttpPostMultipart::META.operation_id,
        bag,
        [EffectHint::HttpWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();
    let err = context::with_resources(view, async {
        HttpPostMultipart::invoke(&HttpMultipartInput {
            path: "/upload".to_string(),
            query: Vec::new(),
            headers: BTreeMap::new(),
            parts: form! { "file" => artifact },
            target: None,
        })
        .await
        .expect_err("artifact deref must be denied without workspace::read")
    })
    .await;

    assert_eq!(mock.hits(), 0, "deref denial must precede any request");
    assert!(
        matches!(err, HttpConnectorError::MissingWorkspaceRead),
        "expected MissingWorkspaceRead, got {err:?}"
    );
    assert_eq!(err.code(), Some("HTTP111"));

    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|d| d.capability == "workspace_read"),
        "expected a workspace_read CAP110 denial, got: {denials:?}"
    );
}
