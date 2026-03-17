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

fn values_path_for_test(spreadsheet_id: &str, range: &str) -> String {
    format!(
        "/v4/spreadsheets/{}/values/{}",
        percent_encoding::utf8_percent_encode(spreadsheet_id, percent_encoding::NON_ALPHANUMERIC),
        percent_encoding::utf8_percent_encode(range, percent_encoding::NON_ALPHANUMERIC)
    )
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
fn run_local_google_sheets_example_succeeds_with_service_account_bindings_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let token_server = httpmock::MockServer::start();
    let api_server = httpmock::MockServer::start();
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
                    "token_url": format!("{}/oauth/token", token_server.base_url()),
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
                    "base_url": api_server.base_url(),
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

    let spreadsheet_id = "demo-spreadsheet";
    let read_range = "'Leads'!A1:ZZZ";
    let append_range = "'Leads'!A1:C";

    let token_mock = token_server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/oauth/token")
            .body_contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer")
            .body_contains("assertion=");
        then.status(200).json_body_obj(&serde_json::json!({
            "access_token": "google-service-account-token",
            "token_type": "Bearer",
            "expires_in": 3600
        }));
    });

    let read_mock = api_server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path(values_path_for_test(spreadsheet_id, read_range))
            .header("authorization", "Bearer google-service-account-token")
            .header("accept", "application/json");
        then.status(200).json_body_obj(&serde_json::json!({
            "range": read_range,
            "values": [["email", "name", "summary"]]
        }));
    });

    let append_mock = api_server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path(format!(
                "{}:append",
                values_path_for_test(spreadsheet_id, append_range)
            ))
            .header("authorization", "Bearer google-service-account-token")
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .query_param("insertDataOption", "INSERT_ROWS")
            .query_param("valueInputOption", "RAW")
            .json_body_obj(&serde_json::json!({
                "majorDimension": "ROWS",
                "values": [["ada@example.test", "Ada Lovelace", "from bindings lock"]]
            }));
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": {
                "updatedRange": "'Leads'!A2:C2"
            }
        }));
    });

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "connector_google_sheets_local_flow",
            "--bindings-lock",
            path.to_str().expect("path"),
            "--payload",
            r#"{"spreadsheet_id":"demo-spreadsheet","sheet":"Leads","email":"ada@example.test","name":"Ada Lovelace","summary":"from bindings lock"}"#,
        ])
        .env("google_sheets_sa_email", "svc@example.test")
        .env("google_sheets_sa_private_key", TEST_RSA_PRIVATE_KEY_PEM)
        .output()?;

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

    token_mock.assert();
    read_mock.assert();
    append_mock.assert();

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
