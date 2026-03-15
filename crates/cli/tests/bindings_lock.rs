use std::path::PathBuf;
use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::Value;

fn temp_lock_path() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.bindings.lock.it.{pid}.{counter}.json"));
    path
}

fn generate_lock(
    example: &str,
    extra_binds: &[&str],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = temp_lock_path();
    let mut cmd = Command::cargo_bin("flows")?;
    cmd.args([
        "bindings",
        "lock",
        "generate",
        "--example",
        example,
        "--out",
        path.to_str().expect("path"),
    ]);

    for bind in extra_binds {
        cmd.args(["--bind", bind]);
    }

    let output = cmd.output()?;
    assert!(
        output.status.success(),
        "lock generate failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(path)
}

#[test]
fn run_local_succeeds_with_bindings_lock() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock(
        "s4_preflight",
        &["resource::kv=memory", "durability::checkpoint_store=memory"],
    )?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "flows run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload, serde_json::json!({}));

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_rejects_bind_and_bindings_lock_together() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock(
        "s4_preflight",
        &["resource::kv=memory", "durability::checkpoint_store=memory"],
    )?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
            "--bind",
            "resource::kv=memory",
        ])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--bindings-lock cannot be combined with --bind"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_rejects_hash_mismatch_in_lock() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock(
        "s4_preflight",
        &["resource::kv=memory", "durability::checkpoint_store=memory"],
    )?;

    let raw = std::fs::read_to_string(&lock_path)?;
    let mut json: Value = serde_json::from_str(&raw)?;
    json["content_hash"] = serde_json::json!("deadbeef");
    std::fs::write(&lock_path, serde_json::to_vec(&json)?)?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
        ])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("content_hash mismatch"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_rejects_lock_missing_flow_id() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock("s1_echo", &[])?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
        ])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("does not define bindings for flow_id"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_connector_example_succeeds_with_connector_bindings_lock(
) -> Result<(), Box<dyn std::error::Error>> {
    let server = httpmock::MockServer::start();
    let flow_id = example_connector_github_issues_local_flow::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let mut lock = serde_json::json!({
        "version": 1,
        "generated_at": "2025-12-15T00:00:00Z",
        "content_hash": "",
        "instances": {
            "http1": {
                "provider_kind": "http.reqwest",
                "provides": ["resource::http"],
                "connect": {},
                "config": {},
                "isolation": []
            }
        },
        "flows": {
            flow_id.clone(): {
                "use": {
                    "resource::http": "http1"
                }
            }
        },
        "connector_handles": {
            "endpoint.github_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": server.base_url(),
                    "default_headers": {
                        "Accept": "application/json",
                        "X-GitHub-Api-Version": "2022-11-28"
                    }
                },
                "grants": {}
            }
        },
        "connector_connections": {
            "github_local": {
                "connector_id": "connector.github.issues",
                "roles": {
                    "endpoint_profile.github_default": "endpoint.github_local"
                }
            }
        },
        "connector_bindings": {
            flow_id.clone(): {
                "defaults": {
                    "connector.github.issues": "github_local"
                },
                "nodes": {}
            }
        }
    });

    let path = temp_lock_path();
    let json_for_hash = lock.clone();

    let mut hasher = sha2::Sha256::new();
    let canonical = canonical_json_without_hash_for_test(&json_for_hash);
    use sha2::Digest;
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    let hash = digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
    std::fs::write(&path, serde_json::to_vec_pretty(&lock)?)?;

    let mock = server.mock(|_when, then| {
        then.status(200).json_body_obj(&serde_json::json!([
            {
                "number": 501,
                "title": "from cli bindings lock",
                "state": "open",
                "html_url": "https://example.test/issues/501"
            }
        ]));
    });

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "connector_github_issues_local_flow",
            "--bindings-lock",
            path.to_str().expect("path"),
            "--payload",
            r#"{"owner":"rust-lang","repo":"cargo"}"#,
        ])
        .output()?;

    assert!(
        output.status.success(),
        "connector run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload["items"][0]["number"], 501);
    assert_eq!(payload["items"][0]["title"], "from cli bindings lock");
    mock.assert();

    std::fs::remove_file(&path).ok();
    Ok(())
}

fn canonical_json_without_hash_for_test(value: &Value) -> String {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("content_hash");
    }
    canonical_json_for_test(&value)
}

fn canonical_json_for_test(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("json")
        }
        Value::Array(values) => {
            let mut out = String::from("[");
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json_for_test(item));
            }
            out.push(']');
            out
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = String::from("{");
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("json key"));
                out.push(':');
                out.push_str(&canonical_json_for_test(map.get(key).expect("key present")));
            }
            out.push('}');
            out
        }
    }
}
