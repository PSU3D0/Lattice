//! Golden end-to-end for the s26 generic-HTTP acceptance flow (packet H4 of
//! `impl-docs/spec/http-request-node.md`): `flows run local --example
//! s26_http_longtail` drives the `/ingest` entrypoint one time against a mock
//! server provisioned through a hand-authored bindings.lock, exercising the
//! whole `connector.http` path — a typed Tier-0 GET, a pure shape, a
//! dedupe-guarded Effectful POST, and an Effectful webhook GET (spec §4). A
//! second assertion renders the flow (`flows deploy render`) and checks the
//! origin-audit NOTES + the Tier-2 warning line.
//!
//! Redelivery-no-double-fire is proven in the example's in-process unit tests
//! (a single CLI process gets a fresh in-memory KV, so cross-process dedupe is
//! not observable here — the same posture as the s18 golden).

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

fn stamp_hash(lock: &mut Value) {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical_json_without_hash(lock).as_bytes());
    let hash = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
}

fn temp_path(tag: &str) -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.s26.{tag}.{pid}.{counter}.json"));
    path
}

/// Build the s26 bindings.lock against a mock base URL: one Tier-0 authed
/// connection (GET + POST + webhook GET) and one Tier-2 any-origin connection
/// bound to `probe_public` via the flat `nodes` override map (spec §8, F5).
fn s26_lock(flow_id: &str, base_url: &str) -> Value {
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
                "use": { "resource::http": "http1", "resource::kv": "kv1" }
            }
        },
        "connector_handles": {
            "auth.http_token": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S26_HTTP_BEARER" },
                "config": {},
                "grants": {}
            },
            "endpoint.http_api": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": { "Accept": "application/json" }
                },
                "grants": {}
            },
            "endpoint.anywhere": {
                "provider_kind": "endpoint.any_origin",
                "handle_kind": "endpoint.any_origin",
                "connect": {},
                "config": {},
                "grants": {}
            }
        },
        "connector_connections": {
            "http_api": {
                "connector_id": "connector.http",
                "roles": {
                    "endpoint_profile.http_target": "endpoint.http_api",
                    "outbound_auth.http_target_auth": "auth.http_token"
                }
            },
            "anywhere": {
                "connector_id": "connector.http",
                "roles": { "endpoint_profile.http_anywhere": "endpoint.anywhere" }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": { "connector.http": "http_api" },
                "nodes": { "probe_public": "anywhere" },
                "resolved_effect_hints": {
                    "fetch_product": [],
                    "push_stats": [],
                    "webhook_ping": [],
                    "probe_public": []
                }
            }
        }
    });
    stamp_hash(&mut lock);
    lock
}

fn write_lock(tag: &str, lock: &Value) -> std::path::PathBuf {
    let path = temp_path(tag);
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(lock).expect("serialize lock"),
    )
    .expect("write lock");
    path
}

fn ingest_payload() -> Value {
    serde_json::json!({ "sku": "SKU-42", "event_id": "evt-golden-001" })
}

#[test]
fn run_local_drives_s26_connector_http_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s26_http_longtail::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();
    let lock_path = write_lock("run", &s26_lock(&flow_id, &server.base_url()));

    // Tier-0 typed GET (with the bound bearer credential attached).
    let product = server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/v2/products/SKU-42")
            .header("authorization", "Bearer s26-golden-token");
        then.status(200).json_body_obj(&serde_json::json!({
            "sku": "SKU-42",
            "name": "Widget",
            "price_cents": 1999,
            "in_stock": true
        }));
    });
    // Tier-0 Effectful POST (the §6 dedupe-guarded write).
    let stats = server.mock(|when, then| {
        when.method(httpmock::Method::POST).path("/v2/stats");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "id": "stat-golden-1", "ok": true }));
    });
    // §4 Effectful webhook GET.
    let notify = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/hooks/notify");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "ok": true }));
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "local",
            "--example",
            "s26_http_longtail",
            "--payload",
            &ingest_payload().to_string(),
            "--bindings-lock",
            lock_path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .env("S26_HTTP_BEARER", "s26-golden-token")
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s26 run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    product.assert_hits(1);
    stats.assert_hits(1);
    notify.assert_hits(1);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let receipt: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not an IngestReceipt JSON ({err}): {stdout}"));
    assert_eq!(receipt["push"]["applied"], serde_json::json!(true));
    assert_eq!(
        receipt["push"]["remote_id"],
        serde_json::json!("stat-golden-1")
    );
    assert_eq!(receipt["ping"]["fired"], serde_json::json!(true));
    assert_eq!(receipt["sku"], serde_json::json!("SKU-42"));

    std::fs::remove_file(&lock_path).ok();
}

#[test]
fn render_emits_origin_audit_and_tier2_warning() {
    let flow_id = example_s26_http_longtail::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();
    // Render does not execute the flow; a placeholder origin is fine.
    let lock_path = write_lock("render", &s26_lock(&flow_id, "https://api.acme.example"));
    let out_dir = temp_path("render_out");

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "deploy",
            "render",
            "--example",
            "s26_http_longtail",
            "--bindings-lock",
            lock_path.to_str().expect("lock path"),
            "--out",
            out_dir.to_str().expect("out path"),
        ])
        .output()
        .expect("run flows deploy render");

    assert!(
        output.status.success(),
        "s26 render failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let notes = String::from_utf8_lossy(&output.stderr);
    // Tier-0 nodes name their fixed origin; the Tier-2 node renders ANY-ORIGIN.
    for alias in ["fetch_product", "push_stats", "webhook_ping"] {
        assert!(
            notes.contains(&format!(
                "connector.http origin-audit: node `{alias}` (connection `http_api`) -> origin https://api.acme.example"
            )),
            "missing origin audit for {alias}:\n{notes}"
        );
    }
    assert!(
        notes.contains(
            "connector.http origin-audit: node `probe_public` (connection `anywhere`) -> ANY-ORIGIN (Tier 2, unauthenticated)"
        ),
        "missing Tier-2 origin audit:\n{notes}"
    );
    assert!(
        notes.contains("connector.http Tier-2 (any-origin) grant present — node(s) probe_public"),
        "missing Tier-2 warning:\n{notes}"
    );

    std::fs::remove_file(&lock_path).ok();
    std::fs::remove_dir_all(&out_dir).ok();
}
