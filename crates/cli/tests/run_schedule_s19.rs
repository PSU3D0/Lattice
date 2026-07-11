//! Golden end-to-end for the s19 clone (packet N4-T9 of the phase-1
//! clone-engine plan): `flows run schedule --example s19_scheduled_email_dispatch
//! --once` fires the daily schedule entrypoint one time and drives TWO
//! connector families (sheets find_rows + upsert_row, gmail send) plus the
//! enforced terminal KV write, entirely against a mock server provisioned
//! through a hand-authored bindings.lock.
//!
//! Like the s16 golden, one `auth.static_bearer` handle serves both families
//! because they share the `google_workspace_auth` role name. The fixture rows
//! carry a far-past (`2020-01-01`) and a far-future (`2999-01-01`) `send_date`
//! so the due-date cutoff is deterministic regardless of the scheduler's
//! synthesized fire time: exactly one message is due.

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
    path.push(format!("lattice.s19.schedule.lock.{pid}.{counter}.json"));
    path
}

/// Build the s19 bindings.lock against a mock base URL. Exercises the shared
/// google_workspace_auth role: ONE bearer handle bound into two connections.
fn s19_lock(flow_id: &str, base_url: &str) -> Value {
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
                "connect": { "secret_ref": "S19_GOOGLE_BEARER" },
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
            "endpoint.google_gmail_local": {
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
            "google_gmail_local": {
                "connector_id": "connector.google.gmail",
                "roles": {
                    "endpoint_profile.google_gmail_default": "endpoint.google_gmail_local",
                    "outbound_auth.google_workspace_auth": "auth.google_workspace"
                }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.google.sheets": "google_sheets_local",
                    "connector.google.gmail": "google_gmail_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "load_queue": [],
                    "dispatch_messages": [],
                    "mark_messages_sent": []
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
fn schedule_once_runs_s19_clone_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s19_scheduled_email_dispatch::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s19_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    // find_rows + upsert_row both read the queue table via GET /values/. The
    // far-past m1 is always due; the far-future m2 never is, so the dev
    // scheduler's synthesized fire time does not change the outcome.
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [
                ["id", "email", "name", "subject", "body", "send_date", "status"],
                ["m1", "alice@lattice-pilot.test", "Alice", "Welcome",
                 "Your onboarding is ready.", "2020-01-01", "queued"],
                ["m2", "bob@lattice-pilot.test", "Bob", "Reminder",
                 "Your renewal is coming up.", "2999-01-01", "queued"]
            ]
        }));
    });
    let gmail_send = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s19-golden-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "msg-golden-1", "threadId": "t-1", "labelIds": ["SENT"]
        }));
    });
    let values_update = server.mock(|when, then| {
        when.method(httpmock::Method::PUT).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "updatedRange": "'Queue'!A2:G2"
        }));
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "schedule",
            "--example",
            "s19_scheduled_email_dispatch",
            "--once",
            "--bindings-lock",
            path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .env("S19_GOOGLE_BEARER", "s19-golden-token")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s19 schedule --once failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // Both connector families really ran against the mock.
    values_read.assert_hits(2);
    gmail_send.assert_hits(1);
    values_update.assert_hits(1);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a DispatchRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["sent_count"], serde_json::json!(1));
    assert_eq!(record["marked_count"], serde_json::json!(1));
    assert_eq!(record["message_ids"], serde_json::json!(["msg-golden-1"]));
    assert!(
        record["key"]
            .as_str()
            .expect("key field")
            .starts_with("s19_scheduled_email_dispatch_flow:dispatch_trigger:"),
        "dispatch key must be scheduled-time keyed: {record}"
    );

    std::fs::remove_file(&path).ok();
}
