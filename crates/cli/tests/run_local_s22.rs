//! Golden end-to-end for the s22 clone (packet N4-T3 of the phase-1
//! clone-engine plan): `flows run local --example s22_hydration_reminder
//! --payload <reminder_request>` fires the HTTP webhook entrypoint one time and
//! drives THREE connector families (Google Sheets find_rows, LLM completion,
//! Slack post) plus the enforced terminal KV write, entirely against a mock
//! server provisioned through a hand-authored bindings.lock.
//!
//! This is the webhook-triggered analog of the s18 golden: each connector
//! family has its own `auth.static_bearer` handle (Google, the LLM provider,
//! and Slack secrets are distinct providers), bound into three connections.

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
    path.push(format!("lattice.s22.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s22 bindings.lock against a mock base URL. Three connector
/// families, each with its own bearer handle bound into one connection.
fn s22_lock(flow_id: &str, base_url: &str) -> Value {
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
                "connect": { "secret_ref": "S22_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.llm": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S22_LLM_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.slack": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S22_SLACK_BEARER" },
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
            "endpoint.llm_local": {
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
            "llm_local": {
                "connector_id": "connector.llm",
                "roles": {
                    "endpoint_profile.llm_default": "endpoint.llm_local",
                    "outbound_auth.llm_api_key": "auth.llm"
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
                    "connector.llm": "llm_local",
                    "connector.slack.core": "slack_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "read_water_log": [],
                    "generate_reminder": [],
                    "post_reminder": []
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

fn reminder_payload() -> Value {
    serde_json::json!({
        "reminder_id": "rem-golden-001",
        "user": "darrell",
        "date": "2026-07-11",
        "target_ml": 2000.0,
        "model": "gpt-4o-mini"
    })
}

fn sheet_values_body() -> Value {
    serde_json::json!({
        "values": [
            ["date", "time", "value"],
            ["2026-07-11", "08:15:00", "250"],
            ["2026-07-11", "10:30:00", "300"]
        ]
    })
}

fn completion_body(message: &str) -> Value {
    let content = serde_json::json!({ "message": message }).to_string();
    serde_json::json!({
        "id": "chatcmpl-s22-golden",
        "object": "chat.completion",
        "created": 1,
        "model": "gpt-4o-mini",
        "system_fingerprint": null,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content, "tool_calls": [] },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 42,
            "completion_tokens": 30,
            "total_tokens": 72,
            "prompt_tokens_details": { "cached_tokens": 0 }
        }
    })
}

#[test]
fn run_local_runs_s22_clone_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s22_hydration_reminder::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s22_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path_contains("/values/")
            .header("authorization", "Bearer s22-google-token");
        then.status(200).json_body_obj(&sheet_values_body());
    });
    let llm_complete = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/chat/completions")
            .header("authorization", "Bearer s22-llm-key");
        then.status(200)
            .json_body(completion_body("Drink up — your body will thank you!"));
    });
    let slack_post = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/chat.postMessage")
            .header("authorization", "Bearer s22-slack-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true,
            "channel": "C-hydration-golden",
            "ts": "1700000000.000900"
        }));
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s22_hydration_reminder",
            "--payload",
            &reminder_payload().to_string(),
            "--bindings-lock",
            path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .env("S22_GOOGLE_BEARER", "s22-google-token")
        .env("S22_LLM_BEARER", "s22-llm-key")
        .env("S22_SLACK_BEARER", "s22-slack-token")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s22 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // Every connector family really ran against the mock.
    values_read.assert_hits(1);
    llm_complete.assert_hits(1);
    slack_post.assert_hits(1);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a ReminderRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["reminder_id"], serde_json::json!("rem-golden-001"));
    assert_eq!(
        record["slack_channel"],
        serde_json::json!("C-hydration-golden")
    );
    assert_eq!(record["slack_ts"], serde_json::json!("1700000000.000900"));
    assert_eq!(
        record["message"],
        serde_json::json!("Drink up — your body will thank you!")
    );
    assert_eq!(
        record["key"],
        serde_json::json!("s22_hydration_reminder_flow:reminder_trigger:rem-golden-001")
    );

    std::fs::remove_file(&path).ok();
}
