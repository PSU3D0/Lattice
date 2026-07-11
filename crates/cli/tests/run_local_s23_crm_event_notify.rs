//! Golden end-to-end for the s23 clone (packet N4-T4 of the phase-1
//! clone-engine plan): `flows run local --example s23_crm_event_notify
//! --payload <crm_event>` fires the HTTP webhook entrypoint and drives the
//! webhook CRM-event router — Sheets read+append (always-on log), then a switch
//! that routes `delete` events to Gmail and all other events to Slack — plus
//! the enforced terminal KV write, entirely against a mock server provisioned
//! through a hand-authored bindings.lock.
//!
//! Two invocations exercise both switch branches: a `person.delete` payload
//! takes the Gmail (email) branch and a `company.created` payload takes the
//! Slack (message) branch. Sheets + Gmail share one `auth.static_bearer`
//! (`google_workspace_auth`); Slack has its own, proving per-connection auth
//! resolution — the webhook-trigger analog of the s16/s18 goldens.

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
    path.push(format!("lattice.s23.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s23 bindings.lock against a mock base URL. Sheets + Gmail share the
/// Google bearer; Slack has its own. A `kv.memory` instance backs the terminal
/// record write.
fn s23_lock(flow_id: &str, base_url: &str) -> Value {
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
            },
            "kv1": {
                "provider_kind": "kv.memory",
                "provides": ["resource::kv"],
                "connect": {},
                "config": {},
                "isolation": []
            }
        },
        "flows": {
            flow_id: {
                "use": {
                    "resource::http": "http1",
                    "resource::kv": "kv1"
                }
            }
        },
        "connector_handles": {
            "auth.google_workspace": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S23_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.slack": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S23_SLACK_BEARER" },
                "config": {},
                "grants": {}
            },
            "endpoint.sheets_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": { "Accept": "application/json" }
                },
                "grants": {}
            },
            "endpoint.gmail_local": {
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
            "sheets_local": {
                "connector_id": "connector.google.sheets",
                "roles": {
                    "endpoint_profile.google_sheets_default": "endpoint.sheets_local",
                    "outbound_auth.google_workspace_auth": "auth.google_workspace"
                }
            },
            "gmail_local": {
                "connector_id": "connector.google.gmail",
                "roles": {
                    "endpoint_profile.google_gmail_default": "endpoint.gmail_local",
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
                    "connector.google.sheets": "sheets_local",
                    "connector.google.gmail": "gmail_local",
                    "connector.slack.core": "slack_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "log_event": [],
                    "notify_email": [],
                    "notify_slack": []
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

fn run_once(lock_path: &std::path::Path, payload: &str) -> std::process::Output {
    Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s23_crm_event_notify",
            "--payload",
            payload,
            "--bindings-lock",
            lock_path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .env("S23_GOOGLE_BEARER", "s23-google-token")
        .env("S23_SLACK_BEARER", "s23-slack-token")
        .output()
        .expect("run flows")
}

#[test]
fn run_local_runs_s23_clone_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s23_crm_event_notify::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s23_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    // Always-on Google Sheets event log: append reads the header row first.
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [[
                "event_name", "action", "object_id", "object_name",
                "record_id", "record_type", "channel"
            ]]
        }));
    });
    let values_append = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path_contains("append")
            .header("authorization", "Bearer s23-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Events'!A2:G2" }
        }));
    });
    // Email branch: Gmail send (shares the Google bearer).
    let gmail_send = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path_contains("messages/send")
            .header("authorization", "Bearer s23-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "gmail-golden-1",
            "threadId": "gmail-thread-1",
            "labelIds": ["SENT"]
        }));
    });
    // Message branch: Slack post (its own bearer).
    let slack_post = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/chat.postMessage")
            .header("authorization", "Bearer s23-slack-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "channel": "C-crm-events", "ts": "1700000000.000900"
        }));
    });

    // 1) A `person.delete` event routes to the Gmail (email) branch.
    let delete_out = run_once(
        &path,
        r#"{"event_name":"person.delete","object_metadata":{"id":"evt-del-9","name_singular":"person"},"record":{"id":"rec-7","type_name":"Person"}}"#,
    );
    assert!(
        delete_out.status.success(),
        "s23 delete run failed: status={:?}, stderr={}",
        delete_out.status,
        String::from_utf8_lossy(&delete_out.stderr)
    );
    let delete_stdout = String::from_utf8(delete_out.stdout).expect("utf8 stdout");
    let delete_record: Value = serde_json::from_str(delete_stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not JSON ({err}): {delete_stdout}"));
    assert_eq!(delete_record["stored"], serde_json::json!(true));
    assert_eq!(delete_record["channel"], serde_json::json!("email"));
    assert_eq!(
        delete_record["delivery_ref"],
        serde_json::json!("gmail-golden-1")
    );
    assert_eq!(
        delete_record["key"],
        serde_json::json!("s23_crm_event_notify_flow:crm_event_trigger:evt-del-9")
    );

    // 2) A `company.created` event routes to the Slack (message) branch.
    let create_out = run_once(
        &path,
        r#"{"event_name":"company.created","object_metadata":{"id":"evt-new-3","name_singular":"company"},"record":{"id":"rec-3","type_name":"Company"}}"#,
    );
    assert!(
        create_out.status.success(),
        "s23 create run failed: status={:?}, stderr={}",
        create_out.status,
        String::from_utf8_lossy(&create_out.stderr)
    );
    let create_stdout = String::from_utf8(create_out.stdout).expect("utf8 stdout");
    let create_record: Value = serde_json::from_str(create_stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not JSON ({err}): {create_stdout}"));
    assert_eq!(create_record["stored"], serde_json::json!(true));
    assert_eq!(create_record["channel"], serde_json::json!("message"));
    assert_eq!(
        create_record["delivery_ref"],
        serde_json::json!("1700000000.000900")
    );

    // Both events logged; each branch's channel write fired exactly once.
    values_read.assert_hits(2);
    values_append.assert_hits(2);
    gmail_send.assert_hits(1);
    slack_post.assert_hits(1);

    std::fs::remove_file(&path).ok();
}
