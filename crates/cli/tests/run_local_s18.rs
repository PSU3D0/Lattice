//! Golden end-to-end for the s18 clone (packet N4-T7 of the phase-1
//! clone-engine plan): `flows run local --example s18_retell_transcript_sink
//! --payload <call_analyzed>` fires the HTTP webhook entrypoint one time and
//! drives THREE connector families (Airtable create, Sheets read+append, Notion
//! create) plus the enforced terminal KV write, entirely against a mock server
//! provisioned through a hand-authored bindings.lock.
//!
//! This is the webhook-triggered analog of the s16 schedule golden: each
//! connector family has its own `auth.static_bearer` handle (Airtable, Google,
//! and Notion secrets are distinct providers), bound into three connections.

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
    path.push(format!("lattice.s18.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s18 bindings.lock against a mock base URL. Three connector
/// families, each with its own bearer handle bound into one connection.
fn s18_lock(flow_id: &str, base_url: &str) -> Value {
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
            "auth.airtable": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S18_AIRTABLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.google_workspace": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S18_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.notion": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S18_NOTION_BEARER" },
                "config": {},
                "grants": {}
            },
            "endpoint.airtable_local": {
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
            "endpoint.notion_local": {
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
            "airtable_local": {
                "connector_id": "connector.airtable",
                "roles": {
                    "endpoint_profile.airtable_default": "endpoint.airtable_local",
                    "outbound_auth.airtable_token_auth": "auth.airtable"
                }
            },
            "sheets_local": {
                "connector_id": "connector.google.sheets",
                "roles": {
                    "endpoint_profile.google_sheets_default": "endpoint.sheets_local",
                    "outbound_auth.google_workspace_auth": "auth.google_workspace"
                }
            },
            "notion_local": {
                "connector_id": "connector.notion",
                "roles": {
                    "endpoint_profile.notion_default": "endpoint.notion_local",
                    "outbound_auth.notion_api_auth": "auth.notion"
                }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.airtable": "airtable_local",
                    "connector.google.sheets": "sheets_local",
                    "connector.notion": "notion_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "store_in_airtable": [],
                    "store_in_sheets": [],
                    "store_in_notion": []
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

fn call_analyzed_payload() -> Value {
    serde_json::json!({
        "event": "call_analyzed",
        "call": {
            "call_id": "call-golden-001",
            "direction": "outbound",
            "from_number": "+15550000000",
            "to_number": "+15551234567",
            "start_timestamp_ms": 1_782_972_000_000u64,
            "end_timestamp_ms": 1_782_972_083_000u64,
            "duration_seconds": 83.0,
            "transcript": "Agent: hello. User: hi.",
            "summary": "Caller booked a standard room.",
            "sentiment": "positive",
            "combined_cost_cents": 1250.0
        }
    })
}

#[test]
fn run_local_runs_s18_clone_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s18_retell_transcript_sink::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s18_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    let airtable_create = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/v0/appLatticePilot01/Transcripts")
            .header("authorization", "Bearer s18-airtable-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "rec-golden-1",
            "createdTime": "2026-07-02T06:02:00.000Z"
        }));
    });
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [[
                "Call ID", "Start Datetime", "End Datetime", "Duration in seconds",
                "Phone Number", "Transcript", "Call Summary", "User Sentiment",
                "Total Cost in Dollars"
            ]]
        }));
    });
    let values_append = server.mock(|when, then| {
        when.method(httpmock::Method::POST).path_contains("append");
        then.status(200).json_body_obj(&serde_json::json!({
            "updates": { "updatedRange": "'Transcripts'!A2:I2" }
        }));
    });
    let notion_create = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/v1/pages")
            .header("authorization", "Bearer s18-notion-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "object": "page",
            "id": "page-golden-1",
            "url": "https://www.notion.so/page-golden-1"
        }));
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s18_retell_transcript_sink",
            "--payload",
            &call_analyzed_payload().to_string(),
            "--bindings-lock",
            path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .env("S18_AIRTABLE_BEARER", "s18-airtable-token")
        .env("S18_GOOGLE_BEARER", "s18-google-token")
        .env("S18_NOTION_BEARER", "s18-notion-token")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s18 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // Every connector family really ran against the mock.
    airtable_create.assert_hits(1);
    values_read.assert_hits(1);
    values_append.assert_hits(1);
    notion_create.assert_hits(1);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a SinkRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["call_id"], serde_json::json!("call-golden-001"));
    assert_eq!(record["airtable_record_id"], serde_json::json!("rec-golden-1"));
    assert_eq!(record["notion_page_id"], serde_json::json!("page-golden-1"));
    assert_eq!(
        record["key"],
        serde_json::json!("s18_retell_transcript_sink_flow:intake:call-golden-001")
    );

    std::fs::remove_file(&path).ok();
}
