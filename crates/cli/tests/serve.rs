use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use capabilities::{Capability, ResourceBag};
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use dag_core::{DurabilityMode, FlowId};
use example_s1_echo as s1_echo;
use example_s2_site as s2_site;
use host_web_axum::{HostHandle, RouteConfig};
use serde_json::json;
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

    async fn lease(&self, handle: &CheckpointHandle, _ttl: Duration) -> Result<Lease, CheckpointError> {
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

    async fn list(&self, filter: CheckpointFilter) -> Result<Vec<CheckpointHandle>, CheckpointError> {
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
