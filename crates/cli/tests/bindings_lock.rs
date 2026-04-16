use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use assert_cmd::prelude::*;
use example_s12_sheetport_quote as s12_sheetport_quote;
use serde_json::Value;

const BUILD_JOBS_LIMIT: &str = "4";

fn build_heavy_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn shared_target_dir() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/flows-cli-it");
    std::fs::create_dir_all(&path).expect("create shared target dir");
    path
}

fn temp_lock_path() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.bindings.lock.it.{pid}.{counter}.json"));
    path
}

fn temp_bundle_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.bundle.it.{label}.{pid}.{counter}"));
    path
}

// Deterministic test-only RSA fixture used for service-account JWT coverage.
// This path is allowlisted in `.gitleaks.toml`.
const TEST_RSA_PRIVATE_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDIA/LEAPFqnUft
gmeGPFVtcWpJSkDfOtqucdzB7lhvV3qKjHgAijwySNPWYbwq+PqjULtMmD5ishZj
vy86n2oV4dT9wZllpywjgyiwClgLNTmNefZaV7MorK68/rLXlKRZ5w8krokQCDYK
lKU7PF4u2o5FC/iUT1jDXm7pq7YkldfVmq0QVGlABMNqnEmKHvmE4M0ZMv6g17+w
w0KPbbI5J1CCiZF0Dvx8775X3yLn4qEW7Euj4lx0Hb2Xc0plGY3LGG6qeApocjNH
RMg2c15Xb3bSo2JNOoR0CQ58c1ZIh+Eo9kf6foAftrrW6cDoWFdkgGX/3LsNRTz+
nAH4JFnhAgMBAAECggEAF1riq4FiryzLW8/oz7NW1E80dnddqNNB+rGf8eMnX2Tr
EaeCUanSipqXZcaGxsvI1G4WWMTEMBkUZTRLSwCXThPPH4xOIaEKFeF4TEoA6tod
rMfrfLQV3u9+/eGNt3+LS1YgHgvlREJ5MPYXbxnG85igmS5jKco0Fqf9snpS6+WA
W7J7RHLcGIO8FqGZ9Hn6F76zrnV5E2zu4V9Q+eU3KWvQatPjiDEQq0rArTPFgV/v
2WxnMJxwbZ/VPbZR2Mx2HQkOaw74kwZeQKxKWzP2ndw7bv1GUFA9FfhuDWYHRsQ2
mxV2Zgf7JqOTcRcsd0L2KE7ArAjaO5lx3YgEnQKc2wKBgQDwZwqKDoZehUVP/l4s
GpQMGV+rJvD/imUczCJsZnymWlawr2PMLLvG+pNWg4LALISYyhOhW/b4x8W/eHFr
Cmd3Z8LIJIheDNTREMhpaHqutQgORJEtSZjD2Wehhpgm3l9jifFuDh8KbRPUwRyH
TPis/Vz17RWwyvOxei6EoFgOcwKBgQDU/hlYjRUpaQLFlpI63zHbLjmlT7Q0SBNA
UDCwpuLcPsrxVB2lbbWQ25VCnUx9DftSTZt5wbfNpowvHJi+e42TiUgNp8LTLiuv
FjO+HNwjzmZcjfsDkUmOe/hi4UeiZmDYxV7kE5nHeE2fwtnNMAIiJ/LQecMZswQi
uegKUD8tWwKBgHD/rieIfkZtlE/ue6t1bsNlJd/YNQ2YqsBnf4K+hbbX3cm9F0bA
fB8iZyESPeJAyq7axXFiPetgU6YVYhJzWID6x8a1zVeP5nTC08EgOBJoy3mRZ0AH
SQQ964U0M86JVgL+svoNLzACZ4DoqJU8a+M8UHbUUw6/xt5UVQtIJzvbAoGABsrb
sBE/vYRVzEtS+oGnq1+8AuOZ0ZkC1Cg6hUetMGzoN+4AzAfFpIr8JZWynMJXY3aK
IMXmwK4xBkeZL2ntR+k23Qiek/GC/yBsIgH1m0a3yPfWK3T0rZCSiUS57hnpuMAC
mK9vVgcmIpQqMfr39nLjsXZQnH8zAJCBL+MDQMUCgYBC+zi/0AQU1Vo3DDyEoYb6
zvp9oL/loKZA0xJlSz2ZO8ychRWnuJtf5SpAm30O4VJQcOvbV2hwd7sJsh4sAnY1
qIbhEamp5tjBbAdxLON1Q3Qpyt/uemzi+TSKJbcZ3OQHe2bylylyYYh+4zGCSGMY
RGKOKF9RKKgFGiXk5I97qQ==
-----END PRIVATE KEY-----"#;

fn s12_asset_path(relative: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/s12_sheetport_quote")
        .join(relative)
        .canonicalize()?;
    Ok(path)
}

fn write_s12_file_path_lock() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let flow_id = s12_sheetport_quote::validated_bound_ir()
        .flow()
        .id
        .as_str()
        .to_string();
    let workbook_path = s12_asset_path("assets/quote_model.xlsx")?;
    let manifest_path = s12_asset_path("assets/quote_model.fio.yaml")?;

    let mut lock = serde_json::json!({
        "version": 1,
        "generated_at": "2026-04-01T00:00:00Z",
        "content_hash": "",
        "instances": {},
        "flows": {
            flow_id.clone(): {
                "use": {}
            }
        },
        "connector_handles": {},
        "connector_connections": {
            "sheetport_quote_local": {
                "connector_id": "connector.formualizer.sheetport",
                "roles": {},
                "config": {
                    "workbook_source": {
                        "kind": "file_path",
                        "path": workbook_path
                    },
                    "manifest_source": {
                        "kind": "file_path",
                        "path": manifest_path
                    }
                }
            }
        },
        "connector_bindings": {
            flow_id.clone(): {
                "defaults": {
                    "connector.formualizer.sheetport": "sheetport_quote_local"
                },
                "nodes": {}
            }
        }
    });

    let path = temp_lock_path();
    let hash = {
        let json_for_hash = lock.clone();
        let mut hasher = sha2::Sha256::new();
        let canonical = canonical_json_without_hash_for_test(&json_for_hash);
        use sha2::Digest;
        hasher.update(canonical.as_bytes());
        let digest = hasher.finalize();
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    lock["content_hash"] = serde_json::json!(hash);
    std::fs::write(&path, serde_json::to_vec_pretty(&lock)?)?;
    Ok(path)
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

fn generate_lock_for_package(
    package: &str,
    flow: Option<&str>,
    extra_binds: &[&str],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = temp_lock_path();
    let mut cmd = Command::cargo_bin("flows")?;
    cmd.args([
        "bindings",
        "lock",
        "generate",
        "--package",
        package,
        "--out",
        path.to_str().expect("path"),
    ]);

    if let Some(flow) = flow {
        cmd.args(["--flow", flow]);
    }

    for bind in extra_binds {
        cmd.args(["--bind", bind]);
    }

    let output = cmd.output()?;
    assert!(
        output.status.success(),
        "package lock generate failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(path)
}

fn build_bundle(package: &str, label: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let _guard = build_heavy_lock().lock().expect("build lock");
    let dir = temp_bundle_dir(label);
    let output = Command::cargo_bin("flows")?
        .env("CARGO_TARGET_DIR", shared_target_dir())
        .env("CARGO_BUILD_JOBS", BUILD_JOBS_LIMIT)
        .args([
            "bundle",
            "-p",
            package,
            "--wasm",
            "--dev",
            "--out-dir",
            dir.to_str().expect("bundle dir"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "bundle failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(dir)
}

fn generate_lock_for_bundle(
    bundle: &PathBuf,
    flow: Option<&str>,
    extra_binds: &[&str],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = temp_lock_path();
    let mut cmd = Command::cargo_bin("flows")?;
    cmd.args([
        "bindings",
        "lock",
        "generate",
        "--bundle",
        bundle.to_str().expect("bundle path"),
        "--out",
        path.to_str().expect("path"),
    ]);

    if let Some(flow) = flow {
        cmd.args(["--flow", flow]);
    }

    for bind in extra_binds {
        cmd.args(["--bind", bind]);
    }

    let output = cmd.output()?;
    assert!(
        output.status.success(),
        "bundle lock generate failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(path)
}

#[test]
fn generate_bindings_lock_for_builtin_s11_example_fails_honestly_on_workspace_provider_gap()
-> Result<(), Box<dyn std::error::Error>> {
    let path = temp_lock_path();
    let output = Command::cargo_bin("flows")?
        .args([
            "bindings",
            "lock",
            "generate",
            "--example",
            "s11_lead_intake",
            "--bind",
            "resource::http::write=reqwest",
            "--out",
            path.to_str().expect("path"),
        ])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("resource::workspace::write"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !stderr.contains("unknown example `s11_lead_intake`"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_file(&path).ok();
    Ok(())
}

#[test]
fn generate_bindings_lock_for_builtin_s13_example_exports_resource_hints()
-> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock(
        "s13_github_issue_investigator",
        &[
            "resource::http::write=reqwest",
            "durability::checkpoint_store=memory",
        ],
    )?;

    let raw = std::fs::read_to_string(&lock_path)?;
    let lock: Value = serde_json::from_str(&raw)?;
    let flows = lock["flows"].as_object().expect("flows object");
    assert_eq!(flows.len(), 1);
    let flow = flows.values().next().expect("single flow");
    let use_map = flow["use"].as_object().expect("use map");
    assert_eq!(
        use_map.get("resource::http::write").and_then(Value::as_str),
        Some("http_reqwest")
    );
    assert_eq!(
        use_map
            .get("durability::checkpoint_store")
            .and_then(Value::as_str),
        Some("checkpoint_store_memory")
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn generate_bindings_lock_for_package_flow_exports_resource_hints()
-> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock_for_package(
        "example-s13-github-issue-investigator",
        None,
        &[
            "resource::http::write=reqwest",
            "durability::checkpoint_store=memory",
        ],
    )?;

    let raw = std::fs::read_to_string(&lock_path)?;
    let lock: Value = serde_json::from_str(&raw)?;
    let flows = lock["flows"].as_object().expect("flows object");
    assert_eq!(flows.len(), 1);
    let flow = flows.values().next().expect("single flow");
    let use_map = flow["use"].as_object().expect("use map");
    assert_eq!(
        use_map.get("resource::http::write").and_then(Value::as_str),
        Some("http_reqwest")
    );
    assert_eq!(
        use_map
            .get("durability::checkpoint_store")
            .and_then(Value::as_str),
        Some("checkpoint_store_memory")
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn generate_bindings_lock_for_bundle_flow_exports_resource_hints()
-> Result<(), Box<dyn std::error::Error>> {
    let bundle_dir = build_bundle("example-s13-github-issue-investigator", "s13-lock-generate")?;
    let lock_path = generate_lock_for_bundle(
        &bundle_dir,
        None,
        &[
            "resource::http::write=reqwest",
            "durability::checkpoint_store=memory",
        ],
    )?;

    let raw = std::fs::read_to_string(&lock_path)?;
    let lock: Value = serde_json::from_str(&raw)?;
    let flows = lock["flows"].as_object().expect("flows object");
    assert_eq!(flows.len(), 1);
    let flow = flows.values().next().expect("single flow");
    let use_map = flow["use"].as_object().expect("use map");
    assert_eq!(
        use_map.get("resource::http::write").and_then(Value::as_str),
        Some("http_reqwest")
    );
    assert_eq!(
        use_map
            .get("durability::checkpoint_store")
            .and_then(Value::as_str),
        Some("checkpoint_store_memory")
    );

    std::fs::remove_file(&lock_path).ok();
    std::fs::remove_dir_all(&bundle_dir).ok();
    Ok(())
}

#[test]
fn generate_bindings_lock_for_package_selected_flow_restricts_output()
-> Result<(), Box<dyn std::error::Error>> {
    let flow_id = s12_sheetport_quote::validated_bound_ir()
        .flow()
        .id
        .as_str()
        .to_string();
    let lock_path = generate_lock_for_package(
        "example-s12-sheetport-quote",
        Some(&flow_id),
        &["resource::blob=memory"],
    )?;

    let raw = std::fs::read_to_string(&lock_path)?;
    let lock: Value = serde_json::from_str(&raw)?;
    let flows = lock["flows"].as_object().expect("flows object");
    assert_eq!(flows.len(), 1);
    assert!(flows.contains_key(&flow_id));

    std::fs::remove_file(&lock_path).ok();
    Ok(())
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
fn run_local_s12_sheetport_quote_succeeds_with_file_path_bindings_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let lock_path = write_s12_file_path_lock()?;
    let payload_path = s12_asset_path("payloads/sample.json")?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s12_sheetport_quote",
            "--bindings-lock",
            lock_path.to_str().expect("lock path"),
            "--payload-file",
            payload_path.to_str().expect("payload path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "s12 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload["manifest_id"], "quote-model");
    assert_eq!(payload["connection_name"], "sheetport_quote_local");
    assert_eq!(payload["mode"], "bound");
    assert_eq!(payload["total"], serde_json::json!(180.0));

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_connector_example_succeeds_with_connector_bindings_lock()
-> Result<(), Box<dyn std::error::Error>> {
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
    let hash = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
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

#[test]
fn run_bundle_connector_example_succeeds_with_connector_bindings_lock()
-> Result<(), Box<dyn std::error::Error>> {
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
    let hash = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
    std::fs::write(&path, serde_json::to_vec_pretty(&lock)?)?;

    let mock = server.mock(|_when, then| {
        then.status(200).json_body_obj(&serde_json::json!([
            {
                "number": 502,
                "title": "from bundle bindings lock",
                "state": "open",
                "html_url": "https://example.test/issues/502"
            }
        ]));
    });

    let out_dir = temp_bundle_dir("github-issues");
    let bundle_output = {
        let _build_guard = build_heavy_lock().lock().expect("build lock");
        Command::cargo_bin("flows")?
            .args([
                "bundle",
                "-p",
                "example-connector-github-issues-local-flow",
                "--wasm",
                "--dev",
                "--out-dir",
                out_dir.to_str().expect("out dir"),
            ])
            .env("CARGO_TARGET_DIR", shared_target_dir())
            .env("CARGO_BUILD_JOBS", BUILD_JOBS_LIMIT)
            .output()?
    };

    assert!(
        bundle_output.status.success(),
        "connector bundle build failed: status={:?}, stderr={}",
        bundle_output.status,
        String::from_utf8_lossy(&bundle_output.stderr)
    );

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "bundle",
            "--bundle",
            out_dir.to_str().expect("out dir"),
            "--bindings-lock",
            path.to_str().expect("path"),
            "--payload",
            r#"{"owner":"rust-lang","repo":"cargo"}"#,
        ])
        .output()?;

    assert!(
        output.status.success(),
        "connector run bundle failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload["items"][0]["number"], 502);
    assert_eq!(payload["items"][0]["title"], "from bundle bindings lock");
    mock.assert();

    std::fs::remove_file(&path).ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn run_local_google_sheets_example_succeeds_with_service_account_bindings_lock()
-> Result<(), Box<dyn std::error::Error>> {
    use axum::Json;
    use axum::body::Body;
    use axum::extract::{Path, Request};
    use axum::http::{Method, Response, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::{Router, serve};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    async fn token_handler() -> impl IntoResponse {
        Json(serde_json::json!({
            "access_token": "google-service-account-token",
            "token_type": "Bearer",
            "expires_in": 3600
        }))
    }

    async fn metadata_handler(Path(spreadsheet_id): Path<String>) -> impl IntoResponse {
        Json(serde_json::json!({
            "spreadsheetId": spreadsheet_id,
            "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/demo-spreadsheet/edit",
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": "Leads",
                        "index": 0,
                        "gridProperties": {
                            "rowCount": 1000,
                            "columnCount": 26
                        }
                    }
                }
            ]
        }))
    }

    async fn values_get_handler(
        Path((_spreadsheet_id, _tail)): Path<(String, String)>,
    ) -> impl IntoResponse {
        Json(serde_json::json!({
            "range": "'Leads'!A1:ZZZ",
            "values": [["email", "name", "summary"]]
        }))
    }

    async fn values_post_handler(
        Path((_spreadsheet_id, tail)): Path<(String, String)>,
        request: Request,
    ) -> Response<Body> {
        if request.method() == Method::POST && tail.ends_with(":append") {
            Json(serde_json::json!({
                "updates": {
                    "updatedRange": "'Leads'!A2:C2"
                }
            }))
            .into_response()
        } else {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("not found"))
                .expect("response")
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let app = Router::new()
        .route("/oauth/token", post(token_handler))
        .route("/v4/spreadsheets/:spreadsheet_id", get(metadata_handler))
        .route(
            "/v4/spreadsheets/:spreadsheet_id/values/*tail",
            get(values_get_handler).post(values_post_handler),
        );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let flow_id = example_connector_google_sheets_local_flow::validated_ir()
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
            "auth.google_sheets_sa": {
                "provider_kind": "auth.service_account_jwt",
                "handle_kind": "http.bearer",
                "connect": {
                    "service_account_email_ref": "google_sheets_sa_email",
                    "private_key_ref": "google_sheets_sa_private_key"
                },
                "config": {
                    "token_url": format!("{}/oauth/token", base_url),
                    "scopes": [
                        "https://www.googleapis.com/auth/spreadsheets"
                    ]
                },
                "grants": {}
            },
            "endpoint.google_sheets_default": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": {
                        "Accept": "application/json"
                    }
                },
                "grants": {}
            }
        },
        "connector_connections": {
            "google_sheets_local": {
                "connector_id": "connector.google.sheets",
                "roles": {
                    "outbound_auth.google_workspace_auth": "auth.google_sheets_sa",
                    "endpoint_profile.google_sheets_default": "endpoint.google_sheets_default"
                }
            }
        },
        "connector_bindings": {
            flow_id.clone(): {
                "defaults": {
                    "connector.google.sheets": "google_sheets_local"
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
    let hash = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
    std::fs::write(&path, serde_json::to_vec_pretty(&lock)?)?;

    let path_for_cmd = path.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("flows")
            .expect("flows binary")
            .args([
                "run",
                "local",
                "--example",
                "connector_google_sheets_local_flow",
                "--bindings-lock",
                path_for_cmd.to_str().expect("path"),
                "--payload",
                r#"{"spreadsheet_id":"demo-spreadsheet","sheet":"Leads","email":"ada@example.test","name":"Ada Lovelace","summary":"from bindings lock"}"#,
            ])
            .env("google_sheets_sa_email", "svc@example.test")
            .env("google_sheets_sa_private_key", TEST_RSA_PRIVATE_KEY_PEM)
            .output()
            .expect("flows run local output")
    })
    .await?;

    assert!(
        output.status.success(),
        "connector run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload["action"], "inserted");
    assert_eq!(payload["row_index"], 2);
    assert_eq!(payload["updated_range"], "'Leads'!A2:C2");
    assert_eq!(
        payload["spreadsheet_url"],
        "https://docs.google.com/spreadsheets/d/demo-spreadsheet/edit"
    );

    std::fs::remove_file(&path).ok();
    let _ = shutdown_tx.send(());
    server_task.await??;
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
