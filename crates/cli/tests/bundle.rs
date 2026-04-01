use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use assert_cmd::prelude::*;
use async_trait::async_trait;
use base64::Engine;
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
    OutboundAuthKind, OutboundAuthProfileDescriptor, ResolvedEndpointProfile,
};
use capabilities::http::HttpRequest;
use capabilities::workspace::{
    Workspace, WorkspaceDeleteResult, WorkspaceEntry, WorkspaceListOptions, WorkspaceReadResult,
    WorkspaceWriteOptions, WorkspaceWriteResult,
};
use capabilities::{Capability, ResourceBag};
use flow_bundle::ExecPolicy;
use host_inproc::HostExecutionResult;
use host_wasmtime::load_flow_bundle;
use httpmock::Method::POST;
use httpmock::MockServer;
use serde_json::json;
use tempfile::tempdir;

fn temp_bundle_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.bundle.it.{label}.{pid}.{counter}"));
    path
}

#[derive(Default)]
struct MemoryWorkspace {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    clock: Mutex<u64>,
}

impl Capability for MemoryWorkspace {
    fn name(&self) -> &'static str {
        "workspace.memory"
    }
}

#[async_trait]
impl Workspace for MemoryWorkspace {
    async fn read_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, capabilities::workspace::WorkspaceError> {
        Ok(self
            .files
            .lock()
            .expect("workspace lock")
            .get(normalized_path)
            .cloned()
            .map(WorkspaceReadResult::Bytes))
    }

    async fn write_normalized(
        &self,
        normalized_path: &str,
        data: &[u8],
        _options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, capabilities::workspace::WorkspaceError> {
        self.files
            .lock()
            .expect("workspace lock")
            .insert(normalized_path.to_string(), data.to_vec());
        let mut clock = self.clock.lock().expect("clock lock");
        *clock += 1;
        Ok(WorkspaceWriteResult {
            path: normalized_path.to_string(),
            size_bytes: data.len() as u64,
            updated_at_ms: *clock,
        })
    }

    async fn list_normalized(
        &self,
        options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, capabilities::workspace::WorkspaceError> {
        let prefix = options.prefix;
        let files = self.files.lock().expect("workspace lock");
        Ok(files
            .iter()
            .filter(|(path, _)| {
                prefix
                    .as_deref()
                    .map(|prefix| path.starts_with(prefix))
                    .unwrap_or(true)
            })
            .enumerate()
            .map(|(index, (path, bytes))| WorkspaceEntry {
                path: path.clone(),
                size_bytes: bytes.len() as u64,
                updated_at_ms: index as u64,
                content_hash: Some(format!("sha256:{}", bytes.len())),
            })
            .collect())
    }

    async fn delete_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, capabilities::workspace::WorkspaceError> {
        let deleted = self
            .files
            .lock()
            .expect("workspace lock")
            .remove(normalized_path)
            .is_some();
        Ok(WorkspaceDeleteResult { deleted })
    }
}

#[derive(Clone)]
struct MockOpenAiConnectorRuntime {
    base_url: String,
    api_key: String,
}

#[async_trait]
impl ConnectorRuntime for MockOpenAiConnectorRuntime {
    async fn apply_outbound_auth(
        &self,
        _scope: &ConnectorBindingScope,
        profile: &OutboundAuthProfileDescriptor,
        request: &mut HttpRequest,
    ) -> Result<(), ConnectorRuntimeError> {
        match profile.kind {
            OutboundAuthKind::Bearer { .. } => {
                request
                    .headers
                    .insert("authorization", format!("Bearer {}", self.api_key));
                Ok(())
            }
            _ => Err(ConnectorRuntimeError::UnsupportedAuthKind {
                role_name: profile.name,
                kind: profile.kind.kind_name(),
            }),
        }
    }

    async fn resolve_endpoint_profile(
        &self,
        _scope: &ConnectorBindingScope,
        _profile: &EndpointProfileDescriptor,
    ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
        Ok(ResolvedEndpointProfile {
            base_url: self.base_url.clone(),
            default_headers: Vec::new(),
        })
    }
}

#[test]
fn bundle_rejects_invalid_flow_ir_path() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let manifest_path = temp.path().join("manifest.json");
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flows": [
            {
                "id": "flow://demo",
                "version": "v0.1.0",
                "profile": "wasm",
                "flow_ir": {
                    "artifact": "../flow_ir.json",
                    "hash": "sha256:2222222222222222222222222222222222222222222222222222222222222222"
                }
            }
        ]
    });

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest_json)?)?;

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "host-workers",
            "--manifest",
            manifest_path.to_str().expect("manifest path"),
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
            "--dev",
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        !output.status.success(),
        "expected bundle to fail for invalid manifest: {output:?}"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("flow_ir artifact path must not traverse"),
        "stderr missing path validation detail: {stderr}"
    );

    Ok(())
}

#[test]
fn bundle_generates_manifest_when_missing() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-s6-spill",
            "--native",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        output.status.success(),
        "expected bundle to succeed without --manifest: {output:?}"
    );
    assert!(out_dir.join("manifest.json").exists());
    assert!(out_dir.join("flows/s6_spill_flow/flow_ir.json").exists());

    Ok(())
}

#[test]
fn bundle_generates_wasm_manifest_for_connector_google_sheets_example()
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
        "expected connector example wasm bundle to succeed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(out_dir.join("manifest.json").exists());
    assert!(
        out_dir
            .join("flows/connector_google_sheets_local_flow/flow_ir.json")
            .exists()
    );

    let manifest_raw = fs::read_to_string(out_dir.join("manifest.json"))?;
    let manifest_json: serde_json::Value = serde_json::from_str(&manifest_raw)?;
    assert_eq!(
        manifest_json["flows"][0]["entrypoints"][0]["method"],
        json!("POST")
    );

    Ok(())
}

#[test]
fn bundle_generates_wasm_manifest_for_s11_lead_intake_example()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-s11-lead-intake",
            "--wasm",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        output.status.success(),
        "expected s11 lead intake wasm bundle to succeed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(out_dir.join("manifest.json").exists());
    assert!(out_dir.join("module.wasm").exists());
    assert!(
        out_dir
            .join("flows/s11_lead_intake_flow/flow_ir.json")
            .exists()
    );

    let manifest_raw = fs::read_to_string(out_dir.join("manifest.json"))?;
    let manifest_json: serde_json::Value = serde_json::from_str(&manifest_raw)?;
    assert_eq!(manifest_json["flows"][0]["profile"], json!("web"));
    assert_eq!(
        manifest_json["flows"][0]["entrypoints"][0]["method"],
        json!("POST")
    );
    assert_eq!(
        manifest_json["flows"][0]["entrypoints"][0]["route_aliases"],
        json!(["/leads"])
    );
    assert_eq!(
        manifest_json["flows"][0]["entrypoints"][0]["trigger"],
        json!("trigger")
    );
    assert_eq!(
        manifest_json["flows"][0]["entrypoints"][0]["capture"],
        json!("capture")
    );

    Ok(())
}

#[tokio::test]
async fn run_bundle_s11_lead_intake_wasmtime_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = temp_bundle_dir("s11-lead-intake-wasmtime");
    let target_dir = out_dir.join("target");
    fs::create_dir_all(&target_dir)?;

    let build = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-s11-lead-intake",
            "--wasm",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("out dir"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;
    assert!(
        build.status.success(),
        "s11 lead intake wasm bundle failed: status={:?}, stderr={}",
        build.status,
        String::from_utf8_lossy(&build.stderr)
    );

    let server = MockServer::start();
    let lead = json!({
        "name": "Morgan Lee",
        "email": "morgan.lee@example.com",
        "priority": "high",
        "product_interest": "workflow automation",
        "seat_count": 140,
        "timeline": "before July rollout",
        "summary": "Needs production-ready workflow automation and enterprise pricing this quarter."
    });
    let draft = json!({
        "subject": "Fast follow-up on workflow automation + enterprise pricing",
        "body": "Hi Morgan,\n\nThanks for the detail — it sounds like you’re on an active evaluation timeline. We can help with rollout guidance, enterprise pricing, and a practical plan for a July launch.\n\nWould you be open to a short call this week to review fit and next steps?\n\nBest,\n[Your Name]",
        "tone": "warm"
    });
    let image_response = json!({
        "created": 1,
        "data": [{
            "b64_json": base64::engine::general_purpose::STANDARD.encode(b"wasmtime-image-bytes")
        }]
    });

    server.mock(|when, then| {
        when.method(POST)
            .path("/v1/chat/completions")
            .header("authorization", "Bearer test-key")
            .body_contains("LeadInfo");
        then.status(200).json_body(json!({
            "id": "chatcmpl-extract",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5.4-mini",
            "system_fingerprint": null,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": serde_json::to_string(&lead).unwrap(), "tool_calls": []},
                "logprobs": null,
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "prompt_tokens_details": {"cached_tokens": 0}}
        }));
    });
    server.mock(|when, then| {
        when.method(POST)
            .path("/v1/chat/completions")
            .header("authorization", "Bearer test-key")
            .body_contains("OutreachDraft");
        then.status(200).json_body(json!({
            "id": "chatcmpl-draft",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5.4-mini",
            "system_fingerprint": null,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": serde_json::to_string(&draft).unwrap(), "tool_calls": []},
                "logprobs": null,
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 14, "completion_tokens": 7, "total_tokens": 21, "prompt_tokens_details": {"cached_tokens": 0}}
        }));
    });
    server.mock(|when, then| {
        when.method(POST)
            .path("/v1/images/generations")
            .header("authorization", "Bearer test-key")
            .body_contains("gpt-image-1.5");
        then.status(200).json_body(image_response.clone());
    });

    let workspace = Arc::new(MemoryWorkspace::default());
    let http = Arc::new(cap_http_reqwest::ReqwestHttpClient::default());
    let resources = ResourceBag::new()
        .with_http_read(Arc::clone(&http))
        .with_http_write(http)
        .with_workspace(Arc::clone(&workspace))
        .with_connector_runtime(Arc::new(MockOpenAiConnectorRuntime {
            base_url: format!("{}/v1", server.base_url()),
            api_key: "test-key".to_string(),
        }));

    let bundle = load_flow_bundle(
        &out_dir,
        ExecPolicy::Wasm,
        None,
        Arc::new(resources.clone()),
    )?;
    let payload = json!({
        "name": "Morgan Lee",
        "email": "morgan.lee@example.com",
        "message": "We need production-ready workflow automation and enterprise pricing before a July rollout for roughly 140 seats."
    });
    let output = bundle
        .executor()
        .with_resource_bag(resources)
        .run_once(&bundle.validated_ir, "trigger", payload, "capture", None)
        .await?;

    let value = match output {
        HostExecutionResult::Value(value) => value,
        HostExecutionResult::Stream(_) => panic!("expected value output"),
        HostExecutionResult::Halt { alias, .. } => panic!("unexpected halt at {alias}"),
    };

    assert_eq!(value["priority"], json!("high"));
    assert_eq!(value["to"], json!("morgan.lee@example.com"));
    assert_eq!(
        value["image_artifact_path"],
        json!("artifacts/lead-intake/morgan-lee-example-com/hero.png")
    );

    let stored = workspace
        .files
        .lock()
        .expect("workspace lock")
        .get("artifacts/lead-intake/morgan-lee-example-com/hero.png")
        .cloned()
        .expect("image stored in workspace");
    assert_eq!(stored, b"wasmtime-image-bytes");

    Ok(())
}

/// Proves that the s6_spill example (which uses blob capability) can be built
/// as a wasm bundle and executed end-to-end through the wasmtime host.
/// The host provides a MemoryBlobStore; the guest talks to it through the
/// `RemoteBlobStore` → `lf_cap_call(OP_BLOB_*)` → `host_block_on` bridge.
#[test]
fn run_bundle_s6_spill_blob_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = temp_bundle_dir("s6-spill-blob");
    let target_dir = out_dir.join("target");
    fs::create_dir_all(&target_dir)?;

    let build = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-s6-spill",
            "--wasm",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("out dir"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;
    assert!(
        build.status.success(),
        "s6_spill wasm bundle failed: status={:?}, stderr={}",
        build.status,
        String::from_utf8_lossy(&build.stderr)
    );

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "bundle",
            "--bundle",
            out_dir.to_str().expect("out dir"),
            "--bind",
            "resource::blob=memory",
            "--payload",
            r#"{"batch_id":"wasm-test","items":["alpha","beta"]}"#,
        ])
        .output()?;
    assert!(
        output.status.success(),
        "s6_spill bundle run failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: serde_json::Value = serde_json::from_str(stdout.trim())?;
    let acks = payload.as_array().expect("output should be array of acks");
    assert_eq!(acks.len(), 2);
    assert_eq!(acks[0]["batch_id"], "wasm-test");
    assert_eq!(acks[0]["stored"], true);
    assert_eq!(acks[1]["batch_id"], "wasm-test");
    assert_eq!(acks[1]["stored"], true);

    Ok(())
}
