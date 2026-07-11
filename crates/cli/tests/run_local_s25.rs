//! Golden end-to-end for the s25 clone (packet N4-T5 of the phase-1
//! clone-engine plan): `flows run local --example s25_form_feedback_summary
//! --payload <request>` fires the MANUAL (HTTP) entrypoint once and drives THREE
//! connector families (Google Sheets `find_rows` read + `connector.llm.complete`
//! summary + Gmail `send_message`) plus the enforced terminal KV write, entirely
//! against a mock server provisioned through a hand-authored bindings.lock.
//!
//! Like run_local_s17, the manual trigger takes its input from `--payload`, so
//! the request is stable and the mocks can match exact paths. The LLM completion
//! is mocked, so the otherwise-nondeterministic summary is fixed for the golden.

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
    path.push(format!("lattice.s25.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s25 bindings.lock against a mock base URL. Three connector
/// families: sheets + gmail share the `google_workspace_auth` bearer; the LLM
/// connector carries its own `llm_api_key` bearer.
fn s25_lock(flow_id: &str, base_url: &str) -> Value {
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
                "connect": { "secret_ref": "S25_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.llm_api_key": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S25_LLM_KEY" },
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
            },
            "llm_local": {
                "connector_id": "connector.llm",
                "roles": {
                    "endpoint_profile.llm_default": "endpoint.llm_local",
                    "outbound_auth.llm_api_key": "auth.llm_api_key"
                }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.google.sheets": "google_sheets_local",
                    "connector.google.gmail": "google_gmail_local",
                    "connector.llm": "llm_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "load_feedback": [],
                    "summarize_feedback": [],
                    "send_report": []
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
fn run_local_runs_s25_form_feedback_summary_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s25_form_feedback_summary::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s25_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    // sheets find_rows reads the form responses.
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [
                ["What went great?", "How can we improve?"],
                ["Great speakers", "More breaks"],
                ["Good food", "Bigger venue"]
            ]
        }));
    });
    // llm.complete: openai_compat POSTs /chat/completions.
    let llm_complete = server.mock(|when, then| {
        when.method(httpmock::Method::POST).path("/chat/completions");
        then.status(200).json_body(serde_json::json!({
            "id": "chatcmpl-s25-golden",
            "object": "chat.completion",
            "created": 1,
            "model": "mock-model-1",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "## Summary\nOverall positive.", "tool_calls": [] },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 40, "completion_tokens": 20, "total_tokens": 60 }
        }));
    });
    // gmail send for the report email.
    let gmail_send = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/gmail/v1/users/me/messages/send");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "msg-report-golden", "threadId": "thread-1", "labelIds": ["SENT"]
        }));
    });

    let payload = serde_json::json!({
        "run_id": "run-golden",
        "spreadsheet_id": "feedback-doc",
        "sheet": "Form Responses",
        "model": "mock-model-1",
        "questions": ["What went great?", "How can we improve?"],
        "recipient": "organizer@lattice-pilot.test",
        "subject": "Event feedback summary"
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s25_form_feedback_summary",
            "--payload",
            &payload.to_string(),
            "--bindings-lock",
            path.to_str().expect("lock path"),
        ])
        .env("S25_GOOGLE_BEARER", "s25-google-token")
        .env("S25_LLM_KEY", "s25-llm-key")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s25 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    values_read.assert_hits(1);
    llm_complete.assert_hits(1);
    gmail_send.assert_hits(1);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a SummaryRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["response_count"], serde_json::json!(2));
    assert_eq!(record["total_tokens"], serde_json::json!(60));
    assert_eq!(record["message_id"], serde_json::json!("msg-report-golden"));
    assert_eq!(
        record["key"],
        serde_json::json!("s25_form_feedback_summary_flow:feedback_trigger:run-golden")
    );

    std::fs::remove_file(&path).ok();
}
