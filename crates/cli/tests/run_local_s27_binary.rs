//! CLI acceptance for S27's report/egress path. The in-crate S27 tests retain
//! ingress and negative workspace-grant coverage.

use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::Value;

fn multipart_content_type() -> String {
    use sha2::{Digest, Sha256};

    let mut boundary_input = Vec::new();
    for (field, bytes) in [
        ("file", b"label,value\nalpha,1\nbeta,2\n".as_slice()),
        ("kind", b"daily".as_slice()),
    ] {
        boundary_input.extend_from_slice(field.as_bytes());
        boundary_input.push(0);
        boundary_input.extend_from_slice(bytes);
        boundary_input.push(0);
    }
    let digest = Sha256::digest(&boundary_input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(
        "multipart/form-data; boundary=----LatticeBoundary{}",
        &digest[..16]
    )
}

fn files_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if !root.exists() {
        return files;
    }
    for entry in std::fs::read_dir(root).expect("read workspace directory") {
        let path = entry.expect("workspace entry").path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.push(path);
        }
    }
    files
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("json scalar")
        }
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|key| format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("json key"),
                        canonical_json(map.get(key).expect("key present"))
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

fn stamp_hash(lock: &mut Value) {
    use sha2::{Digest, Sha256};

    let mut unhashed = lock.clone();
    unhashed
        .as_object_mut()
        .expect("lock object")
        .remove("content_hash");
    let hash = Sha256::digest(canonical_json(&unhashed).as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
}

fn s27_lock(flow_id: &str, base_url: &str) -> Value {
    let http_hint = dag_core::EffectHint::Http.as_str();
    let mut lock = serde_json::json!({
        "version": 1,
        "generated_at": "2026-07-13T00:00:00Z",
        "content_hash": "",
        "instances": {
            "http1": {
                "provider_kind": "http.reqwest",
                "provides": [http_hint],
                "connect": {}, "config": {}, "isolation": []
            }
        },
        "flows": { flow_id: { "use": { (http_hint): "http1" } } },
        "connector_handles": {
            "endpoint.local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": { "base_url": base_url },
                "grants": {}
            }
        },
        "connector_connections": {
            "local": {
                "connector_id": "connector.http",
                "roles": { "endpoint_profile.http_target": "endpoint.local" }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": { "connector.http": "local" },
                "nodes": {},
                "resolved_effect_hints": {
                    "download": [], "reupload": [], "upload_report": []
                }
            }
        }
    });
    stamp_hash(&mut lock);
    lock
}

#[test]
fn run_local_posts_staged_csv_as_multipart_and_returns_receipt() {
    let server = httpmock::MockServer::start();
    let expected_content_type = multipart_content_type();
    let upload = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/upload")
            .header("content-type", &expected_content_type)
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

    let temp = tempfile::tempdir().expect("tempdir");
    let flow_id = example_s27_binary::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();
    let lock_path = temp.path().join("bindings.lock.json");
    std::fs::write(
        &lock_path,
        serde_json::to_vec_pretty(&s27_lock(&flow_id, &server.base_url())).expect("lock json"),
    )
    .expect("write lock");
    let workspace_dir = temp.path().join("workspaces");
    let payload = serde_json::json!({
        "rows": [
            { "label": "alpha", "value": 1 },
            { "label": "beta", "value": 2 }
        ]
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args(["run", "local", "--example", "s27_binary", "--payload"])
        .arg(payload.to_string())
        .args(["--bindings-lock"])
        .arg(&lock_path)
        .args(["--checkpoint-store", "memory", "--workspace-dir"])
        .arg(&workspace_dir)
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s27 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    upload.assert_hits(1);
    assert!(
        workspace_dir.exists(),
        "explicit workspace root was provisioned"
    );
    assert!(
        !workspace_dir.join("retained").exists(),
        "WorkspacePolicy::default must not retain a completed run"
    );
    assert!(
        files_under(&workspace_dir).is_empty(),
        "terminal workspace cleanup must remove all run files: {:?}",
        files_under(&workspace_dir)
    );

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let receipt: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not an UploadReceipt ({err}): {stdout}"));
    assert_eq!(receipt["ok"], serde_json::json!(true));
    assert_eq!(receipt["artifact_name"], serde_json::json!("report.csv"));
    assert_eq!(receipt["content_type"], serde_json::json!("text/csv"));
    assert_eq!(receipt["len"], serde_json::json!(27));
}

#[test]
fn run_local_burst_fails_closed_for_workspace_flow() {
    let temp = tempfile::tempdir().expect("tempdir");
    let workspace_dir = temp.path().join("workspaces");
    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s27_binary",
            "--burst",
            "2",
            "--payload",
            r#"{"rows":[{"label":"alpha","value":1}]}"#,
            "--checkpoint-store",
            "memory",
            "--workspace-dir",
        ])
        .arg(&workspace_dir)
        .output()
        .expect("run flows");

    assert!(!output.status.success(), "workspace burst must fail closed");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--burst > 1 is not supported"), "{stderr}");
    assert!(
        stderr.contains(dag_core::EffectHint::WorkspaceRead.as_str()),
        "{stderr}"
    );
    assert!(
        stderr.contains(dag_core::EffectHint::WorkspaceWrite.as_str()),
        "{stderr}"
    );
    assert!(
        stderr.contains("bypasses HostRuntime workspace binding"),
        "{stderr}"
    );
    assert!(
        !workspace_dir.exists(),
        "failure must precede workspace creation"
    );
}
