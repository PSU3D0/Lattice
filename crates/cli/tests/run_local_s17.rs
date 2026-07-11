//! Golden end-to-end for the s17 clone (packet N4-T6 of the phase-1
//! clone-engine plan): `flows run local --example s17_telegram_broadcast
//! --payload <request>` fires the MANUAL (HTTP) entrypoint once and drives TWO
//! connector families (sheets `find_rows` read + Telegram `send_message` per
//! recipient) plus the enforced terminal KV write, entirely against a mock
//! server provisioned through a hand-authored bindings.lock.
//!
//! Unlike the schedule golden (run_schedule_s16), the manual trigger takes its
//! input from `--payload`, so the roster/message/broadcast_id are stable and
//! the mocks can match exact bodies.

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
    path.push(format!("lattice.s17.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s17 bindings.lock against a mock base URL. Two connector families,
/// each with its own auth role (`google_workspace_auth` vs `telegram_bot_auth`).
fn s17_lock(flow_id: &str, base_url: &str) -> Value {
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
                "connect": { "secret_ref": "S17_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.telegram_bot": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S17_TELEGRAM_TOKEN" },
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
            "endpoint.telegram_local": {
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
            "telegram_local": {
                "connector_id": "connector.telegram",
                "roles": {
                    "endpoint_profile.telegram_bot_default": "endpoint.telegram_local",
                    "outbound_auth.telegram_bot_auth": "auth.telegram_bot"
                }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.google.sheets": "google_sheets_local",
                    "connector.telegram": "telegram_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "load_roster": [],
                    "broadcast_messages": []
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
fn run_local_runs_s17_telegram_broadcast_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s17_telegram_broadcast::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s17_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    // sheets find_rows reads the roster tab: header row + two chat ids.
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [["chat_id"], ["1001"], ["2002"]]
        }));
    });
    // Telegram send: the bot token is spliced into the path (/bot<token>/...).
    let telegram_send = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path_contains("/sendMessage");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "result": { "message_id": 900, "date": 1_700_000_000 }
        }));
    });

    let payload = serde_json::json!({
        "broadcast_id": "bcast-golden",
        "spreadsheet_id": "roster-doc",
        "sheet": "contacts",
        "message": "Release 4.2 ships Friday."
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s17_telegram_broadcast",
            "--payload",
            &payload.to_string(),
            "--bindings-lock",
            path.to_str().expect("lock path"),
        ])
        .env("S17_GOOGLE_BEARER", "s17-google-token")
        .env("S17_TELEGRAM_TOKEN", "123456:S17GOLDEN")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s17 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    values_read.assert_hits(1);
    telegram_send.assert_hits(2);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a BroadcastRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["recipients"], serde_json::json!(2));
    assert_eq!(record["delivered"], serde_json::json!(2));
    assert_eq!(
        record["key"],
        serde_json::json!("s17_telegram_broadcast_flow:broadcast_trigger:bcast-golden")
    );

    std::fs::remove_file(&path).ok();
}
