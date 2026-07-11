//! Golden end-to-end for the s20 clone (packet N4-T10 of the phase-1
//! clone-engine plan): `flows run local --example s20_form_signup_notify`
//! fires the webhook entrypoint once and drives TWO connector families
//! (google/sheets append + upsert, slack/core post) entirely against a mock
//! server provisioned through a hand-authored bindings.lock.
//!
//! This is the webhook-trigger analogue of the s16 schedule golden: distinct
//! `auth.static_bearer` handles serve the two connections (Google vs Slack use
//! different bearer roles), proving per-connection auth resolution.

use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::Value;

fn canonical_json_without_hash(value: &Value) -> String {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("content_hash");
    }
    canonical_json(&value)
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("json scalar")
        }
        Value::Array(items) => {
            let mut out = String::from("[");
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json(item));
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
                out.push_str(&canonical_json(map.get(key).expect("key present")));
            }
            out.push('}');
            out
        }
    }
}

fn temp_lock_path() -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.s20.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s20 bindings.lock against a mock base URL. Two connections
/// (sheets + slack) each bind their own static bearer handle.
fn s20_lock(flow_id: &str, base_url: &str) -> Value {
    let mut lock = serde_json::json!({
        "version": 1,
        "generated_at": "2026-07-11T00:00:00Z",
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
            flow_id: {
                "use": {
                    "resource::http": "http1"
                }
            }
        },
        "connector_handles": {
            "auth.google_workspace": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S20_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.slack": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S20_SLACK_BEARER" },
                "config": {},
                "grants": {}
            },
            "endpoint.google_sheets_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": { "Accept": "application/json" }
                },
                "grants": {}
            },
            "endpoint.slack_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": { "Accept": "application/json" }
                },
                "grants": {}
            }
        },
        "connector_connections": {
            "google_sheets_local": {
                "connector_id": "connector.google.sheets",
                "roles": {
                    "endpoint_profile.google_sheets_default": "endpoint.google_sheets_local",
                    "outbound_auth.google_workspace_auth": "auth.google_workspace"
                }
            },
            "slack_local": {
                "connector_id": "connector.slack.core",
                "roles": {
                    "endpoint_profile.slack_default": "endpoint.slack_local",
                    "outbound_auth.slack_auth": "auth.slack"
                }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.google.sheets": "google_sheets_local",
                    "connector.slack.core": "slack_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "record_signup": [],
                    "notify_signup": [],
                    "enrich_signup": []
                }
            }
        }
    });

    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical_json_without_hash(&lock).as_bytes());
    let hash = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
    lock
}

#[test]
fn run_local_runs_s20_clone_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s20_form_signup_notify::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s20_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    // The sheet already contains the signup row, so the upsert takes the update
    // (PUT) path. Mocks match on stable path fragments.
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [
                ["email", "first_name", "last_name", "job_level", "product_goals"],
                ["signup@example.test", "Ada", "Lovelace", "", ""]
            ]
        }));
    });
    let values_append = server.mock(|when, then| {
        when.method(httpmock::Method::POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Sheet1'!A2:E2" }
        }));
    });
    let slack_post = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/chat.postMessage")
            .header("authorization", "Bearer s20-slack-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "channel": "C-newsletter", "ts": "1700000000.000200"
        }));
    });
    let values_update = server.mock(|when, then| {
        when.method(httpmock::Method::PUT)
            .path_contains("/values/")
            .header("authorization", "Bearer s20-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "updatedRange": "'Sheet1'!A2:E2"
        }));
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s20_form_signup_notify",
            "--payload",
            r#"{"email":"signup@example.test","first_name":"Ada","last_name":"Lovelace","job_level":"Director","product_goals":"automation"}"#,
            "--bindings-lock",
            path.to_str().expect("lock path"),
        ])
        .env("S20_GOOGLE_BEARER", "s20-google-token")
        .env("S20_SLACK_BEARER", "s20-slack-token")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s20 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // Both connector families really ran against the mock: append reads the
    // header row + upsert reads it again = two GET /values/.
    values_read.assert_hits(2);
    values_append.assert_hits(1);
    slack_post.assert_hits(1);
    values_update.assert_hits(1);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a SignupRecord JSON ({err}): {stdout}"));
    assert_eq!(record["email"], serde_json::json!("signup@example.test"));
    assert_eq!(record["slack_channel"], serde_json::json!("C-newsletter"));
    assert_eq!(record["slack_ts"], serde_json::json!("1700000000.000200"));
    assert_eq!(
        record["appended_range"],
        serde_json::json!("'Sheet1'!A2:E2")
    );
    assert_eq!(record["updated_range"], serde_json::json!("'Sheet1'!A2:E2"));

    std::fs::remove_file(&path).ok();
}
