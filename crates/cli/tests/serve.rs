use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use assert_cmd::prelude::*;
use async_trait::async_trait;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use capabilities::{Capability, ResourceBag};
use connector_google_sheets::runtime::transport::EnvConnectorRuntime;
use dag_core::{DurabilityMode, FlowId};
use example_connector_google_sheets_local_flow as google_sheets_local;
use example_s1_echo as s1_echo;
use example_s2_site as s2_site;
use flow_bundle::ExecPolicy;
use host_wasmtime::load_flow_bundle;
use host_web_axum::{HostHandle, RouteConfig};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::json;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[derive(Default)]
struct TestCheckpointStore {
    records: Mutex<HashMap<(FlowId, String, String), CheckpointRecord>>,
}

impl Capability for TestCheckpointStore {
    fn name(&self) -> &'static str {
        "test_checkpoint_store"
    }
}

#[async_trait]
impl CheckpointStore for TestCheckpointStore {
    async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
        let handle = CheckpointHandle {
            checkpoint_id: record.checkpoint_id.clone(),
            flow_id: record.flow_id.clone(),
            run_id: record.run_id.clone(),
        };
        let key = (
            record.flow_id.clone(),
            record.run_id.clone(),
            record.checkpoint_id.clone(),
        );
        self.records.lock().unwrap().insert(key, record);
        Ok(handle)
    }

    async fn get(&self, handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError> {
        let key = (
            handle.flow_id.clone(),
            handle.run_id.clone(),
            handle.checkpoint_id.clone(),
        );
        self.records
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or(CheckpointError::NotFound)
    }

    async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
        let key = (
            handle.flow_id.clone(),
            handle.run_id.clone(),
            handle.checkpoint_id.clone(),
        );
        self.records.lock().unwrap().remove(&key);
        Ok(())
    }

    async fn lease(
        &self,
        handle: &CheckpointHandle,
        _ttl: Duration,
    ) -> Result<Lease, CheckpointError> {
        Ok(Lease {
            lease_id: format!(
                "{}:{}:{}",
                handle.flow_id.as_str(),
                handle.run_id.as_str(),
                handle.checkpoint_id.as_str(),
            ),
            expires_at_ms: 0,
        })
    }

    async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
        Ok(())
    }

    async fn list(
        &self,
        filter: CheckpointFilter,
    ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
        let mut handles = Vec::new();
        for record in self.records.lock().unwrap().values() {
            if let Some(flow_id) = &filter.flow_id {
                if &record.flow_id != flow_id {
                    continue;
                }
            }
            if let Some(run_id) = &filter.run_id {
                if &record.run_id != run_id {
                    continue;
                }
            }
            handles.push(CheckpointHandle {
                checkpoint_id: record.checkpoint_id.clone(),
                flow_id: record.flow_id.clone(),
                run_id: record.run_id.clone(),
            });
        }
        Ok(handles)
    }
}

fn test_resources() -> ResourceBag {
    ResourceBag::default()
        .with_checkpoint_store(Arc::new(TestCheckpointStore::default()))
        .with_max_durability_mode(DurabilityMode::Partial)
}

fn google_sheets_test_resources() -> ResourceBag {
    let client = Arc::new(ReqwestHttpClient::default());
    ResourceBag::default()
        .with_http_read(Arc::clone(&client))
        .with_http_write(client)
        .with_connector_runtime(Arc::new(EnvConnectorRuntime))
        .with_checkpoint_store(Arc::new(TestCheckpointStore::default()))
        .with_max_durability_mode(DurabilityMode::Partial)
}

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

fn google_values_path(spreadsheet_id: &str, range: &str) -> String {
    format!(
        "/v4/spreadsheets/{}/values/{}",
        utf8_percent_encode(spreadsheet_id, NON_ALPHANUMERIC),
        utf8_percent_encode(range, NON_ALPHANUMERIC)
    )
}

#[tokio::test]
async fn serve_echo_route_round_trips_json() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = s1_echo::bundle();
    let entrypoint = bundle.entrypoints.first().expect("bundle entrypoint");
    let executor = bundle.executor();
    let ir = Arc::new(bundle.validated_ir);
    let route_path = entrypoint.route_path.as_deref().unwrap_or("/");
    let mut config = RouteConfig::new(route_path)
        .with_trigger_alias(entrypoint.trigger_alias.clone())
        .with_capture_alias(entrypoint.capture_alias.clone());
    if let Some(deadline) = entrypoint.deadline {
        config = config.with_deadline(deadline);
    }
    for plugin in s1_echo::environment_plugins() {
        config = config.with_environment_plugin(plugin);
    }
    config = config.with_resources(test_resources());

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let host = HostHandle::new(executor, ir, config);
    let server = tokio::spawn(async move {
        axum::serve(listener, host.into_service())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}{route_path}");
    let response = timeout(
        Duration::from_secs(5),
        client
            .post(url)
            .header(
                "x-auth-user",
                r#"{"sub":"user-42","email":"user42@example.com"}"#,
            )
            .json(&s1_echo::EchoRequest {
                value: "World".into(),
            })
            .send(),
    )
    .await??;
    assert!(
        response.status().is_success(),
        "expected success, got {}",
        response.status()
    );

    let body: s1_echo::EchoResponse = response.json().await?;
    assert_eq!(body.value, "world");
    let user = body.user.expect("user field should be present");
    assert_eq!(user.sub, "user-42");
    assert_eq!(user.email.as_deref(), Some("user42@example.com"));

    let _ = shutdown_tx.send(());
    let server_result = timeout(Duration::from_secs(2), server).await??;
    server_result?;

    Ok(())
}

#[tokio::test]
async fn serve_streaming_route_emits_sse() -> Result<(), Box<dyn std::error::Error>> {
    let bundle = s2_site::bundle();
    let entrypoint = bundle.entrypoints.first().expect("bundle entrypoint");
    let executor = bundle.executor();
    let ir = Arc::new(bundle.validated_ir);
    let route_path = entrypoint.route_path.as_deref().unwrap_or("/");
    let method_str = entrypoint.method.as_deref().unwrap_or("POST");
    let method = method_str.parse::<axum::http::Method>()?;
    let mut config = RouteConfig::new(route_path)
        .with_method(method)
        .with_trigger_alias(entrypoint.trigger_alias.clone())
        .with_capture_alias(entrypoint.capture_alias.clone());
    if let Some(deadline) = entrypoint.deadline {
        config = config.with_deadline(deadline);
    }
    config = config.with_resources(test_resources());

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let host = HostHandle::new(executor, ir, config);
    let server = tokio::spawn(async move {
        axum::serve(listener, host.into_service())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}{route_path}");
    let response = timeout(
        Duration::from_secs(5),
        client.post(url).json(&json!({ "site": "alpha" })).send(),
    )
    .await??;

    assert_eq!(response.status(), 200);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    assert_eq!(content_type, "text/event-stream");

    let body = timeout(Duration::from_secs(5), response.text()).await??;
    assert!(
        body.contains("snapshot"),
        "body should include snapshot event: {body}"
    );
    assert!(
        body.contains("update_"),
        "body should include update events: {body}"
    );

    let _ = shutdown_tx.send(());
    let server_result = timeout(Duration::from_secs(2), server).await??;
    server_result?;

    Ok(())
}

#[tokio::test]
async fn load_wasm_bundle_preserves_route_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-s6-spill",
            "--wasm",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        output.status.success(),
        "expected wasm bundle build to succeed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let resources = ResourceBag::default()
        .with_blob(Arc::new(capabilities::blob::MemoryBlobStore::new()))
        .with_checkpoint_store(Arc::new(TestCheckpointStore::default()))
        .with_max_durability_mode(DurabilityMode::Partial);

    let bundle = load_flow_bundle(&out_dir, ExecPolicy::Wasm, None, Arc::new(resources))?;
    let entrypoint = bundle.entrypoints.first().expect("bundle entrypoint");
    assert_eq!(entrypoint.route_path.as_deref(), Some("/spill"));
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(entrypoint.trigger_alias, "trigger");
    assert_eq!(entrypoint.capture_alias, "capture");

    Ok(())
}

#[tokio::test]
async fn serve_google_sheets_route_round_trips_connector_flow()
-> Result<(), Box<dyn std::error::Error>> {
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = httpmock::MockServer::start();
    let _endpoint = EnvGuard::set(
        connector_google_sheets::GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV,
        &server.base_url(),
    );
    let _auth = EnvGuard::set(
        connector_google_sheets::GOOGLE_WORKSPACE_AUTH_ENV,
        "google-local-demo-token",
    );

    let sheet = "Leads";
    let spreadsheet_id = "demo-spreadsheet";
    let read_range = connector_google_platform::sheets::wide_read_range(sheet, 1);
    let append_range = connector_google_platform::sheets::append_table_range(sheet, 1, 3);

    let metadata_mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path(format!("/v4/spreadsheets/{}", utf8_percent_encode(spreadsheet_id, NON_ALPHANUMERIC)))
            .query_param(
                "fields",
                "spreadsheetId,spreadsheetUrl,sheets.properties(sheetId,title,index,gridProperties(rowCount,columnCount))",
            )
            .header("authorization", "Bearer google-local-demo-token")
            .header("accept", "application/json");
        then.status(200).json_body_obj(&json!({
            "spreadsheetId": spreadsheet_id,
            "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/demo-spreadsheet/edit",
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": sheet,
                        "index": 0,
                        "gridProperties": {
                            "rowCount": 1000,
                            "columnCount": 26
                        }
                    }
                }
            ]
        }));
    });

    let read_mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path(google_values_path(spreadsheet_id, &read_range))
            .header("authorization", "Bearer google-local-demo-token")
            .header("accept", "application/json");
        then.status(200).json_body_obj(&json!({
            "range": read_range,
            "values": [["email", "name", "summary"]]
        }));
    });

    let append_mock = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path(format!(
                "{}:append",
                google_values_path(spreadsheet_id, &append_range)
            ))
            .header("authorization", "Bearer google-local-demo-token")
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .query_param("insertDataOption", "INSERT_ROWS")
            .query_param("valueInputOption", "RAW")
            .json_body_obj(&json!({
                "majorDimension": "ROWS",
                "values": [["ada@example.test", "Ada Lovelace", "served via axum"]]
            }));
        then.status(200).json_body_obj(&json!({
            "updates": {
                "updatedRange": "'Leads'!A2:C2"
            }
        }));
    });

    let bundle = google_sheets_local::bundle();
    let entrypoint = bundle.entrypoints.first().expect("bundle entrypoint");
    let executor = bundle.executor();
    let ir = Arc::new(bundle.validated_ir);
    let route_path = entrypoint.route_path.as_deref().unwrap_or("/");
    let mut config = RouteConfig::new(route_path)
        .with_trigger_alias(entrypoint.trigger_alias.clone())
        .with_capture_alias(entrypoint.capture_alias.clone())
        .with_resources(google_sheets_test_resources());
    if let Some(deadline) = entrypoint.deadline {
        config = config.with_deadline(deadline);
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let host = HostHandle::new(executor, ir, config);
    let server_task = tokio::spawn(async move {
        axum::serve(listener, host.into_service())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}{route_path}");
    let response = timeout(
        Duration::from_secs(5),
        client
            .post(url)
            .json(&json!({
                "spreadsheet_id": spreadsheet_id,
                "sheet": sheet,
                "email": "ada@example.test",
                "name": "Ada Lovelace",
                "summary": "served via axum"
            }))
            .send(),
    )
    .await??;

    let status = response.status();
    let response_text = response.text().await?;
    assert_eq!(status, 200, "unexpected body: {response_text}");
    let body: serde_json::Value = serde_json::from_str(&response_text)?;
    metadata_mock.assert();
    read_mock.assert_hits(2);
    append_mock.assert();
    assert_eq!(body["action"], json!("inserted"));
    assert_eq!(body["row_index"], json!(2));
    assert_eq!(body["updated_range"], json!("'Leads'!A2:C2"));
    assert_eq!(
        body["spreadsheet_url"],
        json!("https://docs.google.com/spreadsheets/d/demo-spreadsheet/edit")
    );

    let _ = shutdown_tx.send(());
    let server_result = timeout(Duration::from_secs(2), server_task).await??;
    server_result?;

    Ok(())
}

#[tokio::test]
async fn load_google_sheets_wasm_bundle_preserves_route_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-connector-google-sheets-local-flow",
            "--wasm",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        output.status.success(),
        "expected google sheets wasm bundle to succeed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let resources = ResourceBag::default()
        .with_checkpoint_store(Arc::new(TestCheckpointStore::default()))
        .with_max_durability_mode(DurabilityMode::Partial);

    let bundle = load_flow_bundle(&out_dir, ExecPolicy::Wasm, None, Arc::new(resources))?;
    let entrypoint = bundle.entrypoints.first().expect("bundle entrypoint");
    assert_eq!(
        entrypoint.route_path.as_deref(),
        Some("/google/sheets/local")
    );
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(entrypoint.trigger_alias, "trigger");
    assert_eq!(entrypoint.capture_alias, "upsert");

    Ok(())
}
