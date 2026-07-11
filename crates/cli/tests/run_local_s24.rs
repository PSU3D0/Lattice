//! Golden end-to-end for the s24 clone (packet N4-T8 of the phase-1
//! clone-engine plan): `flows run local --example s24_lead_intake_verify
//! --payload <lead>` fires the HTTP webhook entrypoint one time and drives FOUR
//! connector families (Hunter email-verify, Sheets read+upsert, Gmail send,
//! Discord webhook post) plus the enforced terminal KV write, entirely against a
//! mock server provisioned through a hand-authored bindings.lock.
//!
//! This is the webhook-triggered analog of the s18 golden. Sheets and Gmail
//! share ONE `google_workspace_auth` bearer handle (two connections); Hunter and
//! Discord are distinct providers with their own bearer handles (Hunter's key
//! is a query parameter, Discord's secret is the webhook id/token spliced into
//! the path).

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
    path.push(format!("lattice.s24.local.lock.{pid}.{counter}.json"));
    path
}

/// Build the s24 bindings.lock against a mock base URL. Four connector families;
/// Sheets + Gmail share one bearer, Hunter and Discord have their own.
fn s24_lock(flow_id: &str, base_url: &str) -> Value {
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
            "auth.hunter": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S24_HUNTER_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.google_workspace": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S24_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.discord": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S24_DISCORD_BEARER" },
                "config": {},
                "grants": {}
            },
            "endpoint.hunter_local": {
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
            },
            "endpoint.discord_local": {
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
            "hunter_local": {
                "connector_id": "connector.hunter",
                "roles": {
                    "endpoint_profile.hunter_default": "endpoint.hunter_local",
                    "outbound_auth.hunter_api_key_auth": "auth.hunter"
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
            },
            "discord_local": {
                "connector_id": "connector.discord",
                "roles": {
                    "endpoint_profile.discord_webhook_default": "endpoint.discord_local",
                    "outbound_auth.discord_webhook_auth": "auth.discord"
                }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.hunter": "hunter_local",
                    "connector.google.sheets": "sheets_local",
                    "connector.google.gmail": "gmail_local",
                    "connector.discord": "discord_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "verify_lead": [],
                    "record_lead_in_sheet": [],
                    "notify_lead_by_email": [],
                    "announce_lead_on_discord": []
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

fn lead_payload() -> Value {
    serde_json::json!({
        "name": "Ada Lovelace",
        "email": "ada@leads.test",
        "query": "Do you support cron triggers?",
        "submitted_at": "2026-07-11T09:00:00Z"
    })
}

#[test]
fn run_local_runs_s24_clone_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s24_lead_intake_verify::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    let lock = s24_lock(&flow_id, &server.base_url());
    let path = temp_lock_path();
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    // Hunter verifies the lead as deliverable.
    let hunter_verify = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/v2/email-verifier")
            .query_param("email", "ada@leads.test")
            .query_param("api_key", "s24-hunter-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "data": { "status": "valid", "result": "deliverable", "score": 92, "email": "ada@leads.test" }
        }));
    });
    // Sheets upsert: read the header row (+ existing row) then update it.
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "values": [
                ["Name", "Email", "Query", "Submitted On"],
                ["Ada Lovelace", "ada@leads.test", "", ""]
            ]
        }));
    });
    let values_update = server.mock(|when, then| {
        when.method(httpmock::Method::PUT).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({
            "updatedRange": "'Leads'!A2:D2"
        }));
    });
    // Gmail send.
    let gmail_send = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s24-google-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "id": "gmail-golden-1", "threadId": "thread-1", "labelIds": ["SENT"]
        }));
    });
    // Discord webhook post (204 No Content).
    let discord_post = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/api/webhooks/111/golden-webhook-token");
        then.status(204);
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s24_lead_intake_verify",
            "--payload",
            &lead_payload().to_string(),
            "--bindings-lock",
            path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .env("S24_HUNTER_BEARER", "s24-hunter-token")
        .env("S24_GOOGLE_BEARER", "s24-google-token")
        .env("S24_DISCORD_BEARER", "111/golden-webhook-token")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s24 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // Every connector family really ran against the mock.
    hunter_verify.assert_hits(1);
    values_read.assert_hits(1);
    values_update.assert_hits(1);
    gmail_send.assert_hits(1);
    discord_post.assert_hits(1);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a LeadRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["email"], serde_json::json!("ada@leads.test"));
    assert_eq!(record["updated_range"], serde_json::json!("'Leads'!A2:D2"));
    assert_eq!(
        record["gmail_message_id"],
        serde_json::json!("gmail-golden-1")
    );
    assert_eq!(record["discord_delivered"], serde_json::json!(true));
    assert_eq!(
        record["key"],
        serde_json::json!("s24_lead_intake_verify_flow:lead_trigger:ada@leads.test")
    );

    std::fs::remove_file(&path).ok();
}
