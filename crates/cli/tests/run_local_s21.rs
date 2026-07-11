//! Golden end-to-end for the s21 clone (packet N4-T2 of the phase-1
//! clone-engine plan): `flows run local --example s21_ai_cv_screening
//! --payload <application>` fires the HTTP webhook entrypoint one time and
//! drives THREE connector families (LLM completion, Sheets read+append, Gmail
//! send x2) plus the enforced terminal KV write, entirely against a mock server
//! provisioned through a hand-authored bindings.lock.
//!
//! This is the webhook-triggered analog of the s18/s20 goldens and the first
//! golden to exercise `connector.llm.complete` inside a flow. Sheets and Gmail
//! share the one `google_workspace_auth` bearer; the LLM connection binds its
//! own `llm_api_key` bearer — three connections, two secrets.

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
    path.push(format!("lattice.s21.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s21 bindings.lock against a mock base URL. Three connections
/// (llm + sheets + gmail); sheets and gmail share the google bearer, the LLM
/// connection binds its own.
fn s21_lock(flow_id: &str, base_url: &str) -> Value {
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
                "connect": { "secret_ref": "S21_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.llm": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S21_LLM_BEARER" },
                "config": {},
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
            }
        },
        "connector_connections": {
            "llm_local": {
                "connector_id": "connector.llm",
                "roles": {
                    "endpoint_profile.llm_default": "endpoint.llm_local",
                    "outbound_auth.llm_api_key": "auth.llm"
                }
            },
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
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.llm": "llm_local",
                    "connector.google.sheets": "sheets_local",
                    "connector.google.gmail": "gmail_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "rate_candidate": [],
                    "record_candidate": [],
                    "confirm_candidate": [],
                    "notify_hr": []
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

fn application_payload() -> Value {
    serde_json::json!({
        "full_name": "Ada Lovelace",
        "email": "ada@applicant.test",
        "expectation": "5000-6000",
        "linkedin": "https://linkedin.test/in/ada",
        "cv_filename": "ada_lovelace_cv.pdf",
        "resume_text": "Ten years building analytical engines and Rust services."
    })
}

fn openai_completion_body(content: &str) -> Value {
    serde_json::json!({
        "id": "chatcmpl-golden",
        "object": "chat.completion",
        "created": 1,
        "model": "gemini-1.5-flash",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content, "tool_calls": [] },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 40,
            "completion_tokens": 20,
            "total_tokens": 60,
            "prompt_tokens_details": { "cached_tokens": 0 }
        }
    })
}

#[test]
fn run_local_runs_s21_clone_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s21_ai_cv_screening::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s21_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    let rating = "Rating: 9/10. Strong match. Recommend an interview.";

    let llm_complete = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path_contains("/chat/completions")
            .header("authorization", "Bearer s21-llm-token");
        then.status(200).json_body(openai_completion_body(rating));
    });
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [[
                "full_name", "email", "expectation", "linkedin", "cv_filename", "ai_rating"
            ]]
        }));
    });
    let values_append = server.mock(|when, then| {
        when.method(httpmock::Method::POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Candidates'!A2:F2" }
        }));
    });
    let gmail_send = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s21-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "msg-golden", "threadId": "thread-golden", "labelIds": ["SENT"]
        }));
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s21_ai_cv_screening",
            "--payload",
            &application_payload().to_string(),
            "--bindings-lock",
            path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .env("S21_GOOGLE_BEARER", "s21-google-token")
        .env("S21_LLM_BEARER", "s21-llm-token")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s21 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // Every connector family really ran against the mock; both emails sent.
    llm_complete.assert_hits(1);
    values_read.assert_hits(1);
    values_append.assert_hits(1);
    gmail_send.assert_hits(2);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a ScreeningRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["email"], serde_json::json!("ada@applicant.test"));
    assert_eq!(record["ai_rating"], serde_json::json!(rating));
    assert_eq!(
        record["appended_range"],
        serde_json::json!("'Candidates'!A2:F2")
    );
    assert_eq!(
        record["candidate_message_id"],
        serde_json::json!("msg-golden")
    );
    assert_eq!(record["hr_message_id"], serde_json::json!("msg-golden"));
    assert_eq!(
        record["key"],
        serde_json::json!("s21_ai_cv_screening_flow:screening_trigger:ada@applicant.test")
    );

    std::fs::remove_file(&path).ok();
}
