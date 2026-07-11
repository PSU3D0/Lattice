//! CLI wiring tests for `flows run schedule` (dev scheduler, packet T2).
//!
//! The end-to-end firing path (synthetic ScheduledEvent → HostRuntime → trigger)
//! is characterized in `host-inproc`'s `schedule_fire` integration test, which
//! drives the exact core (`fire_invocation` + `HostRuntime::execute`) this
//! command calls; none of the bundled examples declares a schedule entrypoint
//! yet (that arrives with packet T4). These tests pin the CLI surface: the
//! subcommand parses, and its user-facing error paths are clear.

use std::process::Command;

use assert_cmd::prelude::*;

#[test]
fn schedule_errors_clearly_when_example_has_no_schedule_entrypoint() {
    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args(["run", "schedule", "--example", "s1_echo", "--once"])
        .output()
        .expect("run flows");

    assert!(
        !output.status.success(),
        "expected non-zero exit for a non-schedule example: {output:?}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no schedule entrypoints"),
        "stderr should explain the flow has no schedule entrypoints: {stderr}"
    );
}

#[test]
fn schedule_rejects_unknown_example() {
    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args(["run", "schedule", "--example", "does_not_exist", "--once"])
        .output()
        .expect("run flows");

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown example"),
        "stderr should report the unknown example: {stderr}"
    );
}

#[test]
fn schedule_rejects_malformed_at_timestamp() {
    // `--at` is validated before example loading, so the RFC3339 error surfaces
    // regardless of which example is named.
    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "schedule",
            "--example",
            "s1_echo",
            "--at",
            "not-a-timestamp",
        ])
        .output()
        .expect("run flows");

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("RFC3339"),
        "stderr should explain the timestamp is not RFC3339: {stderr}"
    );
}

#[test]
fn schedule_rejects_conflicting_binding_sources() {
    // Built via concat so the hint-gate's literal check stays authoritative;
    // the value itself is irrelevant here — the test asserts the flag conflict.
    let kv_bind = ["resource", "::kv=memory"].concat();
    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "schedule",
            "--example",
            "s1_echo",
            "--once",
            "--bind",
            &kv_bind,
            "--bindings-lock",
            "/tmp/does-not-matter.json",
        ])
        .output()
        .expect("run flows");

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--bindings-lock cannot be combined with --bind"),
        "stderr should reject conflicting binding sources: {stderr}"
    );
}

#[test]
fn schedule_help_lists_once_and_at_flags() {
    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args(["run", "schedule", "--help"])
        .output()
        .expect("run flows");

    assert!(output.status.success(), "help should succeed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--once"), "help missing --once: {stdout}");
    assert!(stdout.contains("--at"), "help missing --at: {stdout}");
    assert!(
        stdout.contains("--trigger"),
        "help missing --trigger: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Golden end-to-end: the s15 cron canary (packet T4).
//
// `flows run schedule --example s15_scheduled_poll --once` fires the schedule
// entrypoint one time, runs the flow through the real GitHub connector op
// (pointed at a mock server via the bindings.lock endpoint handle) and the
// enforced KV write, and prints the captured `SummaryRecord`. This is the
// golden T2's report recommended: it proves trigger + connector + enforcement
// compose under the dev scheduler using only lock-provisioned memory/mock
// bindings.
// ---------------------------------------------------------------------------

use serde_json::Value;

/// Canonical JSON (sorted keys, no whitespace) with `content_hash` removed —
/// mirrors the bindings.lock hashing the CLI verifies. Kept local so this test
/// stays self-contained.
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
    path.push(format!("lattice.s15.schedule.lock.{pid}.{counter}.json"));
    path
}

#[test]
fn schedule_once_runs_s15_cron_canary_end_to_end_with_bindings_lock() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s15_scheduled_poll::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();

    // The lock provisions everything the dev scheduler cannot supply via
    // `--bind`: the GitHub connector runtime (endpoint -> mock server) AND the
    // in-memory KV the enforced summary write targets. `--bind` and
    // `--bindings-lock` are mutually exclusive, so a single lock is the only
    // way to hand the flow both a connector runtime and a write capability.
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
            flow_id.clone(): {
                "use": {
                    "resource::http": "http1",
                    "resource::kv": "kv1"
                }
            }
        },
        "connector_handles": {
            "endpoint.github_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": server.base_url(),
                    "default_headers": {
                        "Accept": "application/json",
                        "X-GitHub-Api-Version": "2022-11-28"
                    }
                },
                "grants": {}
            }
        },
        "connector_connections": {
            "github_local": {
                "connector_id": "connector.github.issues",
                "roles": {
                    "endpoint_profile.github_default": "endpoint.github_local"
                }
            }
        },
        "connector_bindings": {
            flow_id.clone(): {
                "defaults": {
                    "connector.github.issues": "github_local"
                },
                "nodes": {},
                // C2 lock-recorded resolution for the bound poll node: the
                // GitHub connection adds no requirements beyond the op's static
                // http_read hint, but the alias must be present or preflight
                // fails closed asking for regeneration.
                "resolved_effect_hints": { "poll_open_issues": [] }
            }
        }
    });

    let path = temp_lock_path();
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical_json_without_hash(&lock).as_bytes());
    let hash = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");

    let issues = server.mock(|when, then| {
        when.method(httpmock::Method::GET);
        then.status(200).json_body_obj(&serde_json::json!([
            { "number": 101, "title": "flaky test", "state": "open",
              "html_url": "https://example.test/issues/101" },
            { "number": 102, "title": "docs typo", "state": "open",
              "html_url": "https://example.test/issues/102" }
        ]));
    });

    let output = Command::cargo_bin("flows")
        .expect("flows binary")
        .args([
            "run",
            "schedule",
            "--example",
            "s15_scheduled_poll",
            "--once",
            "--bindings-lock",
            path.to_str().expect("lock path"),
            "--checkpoint-store",
            "memory",
        ])
        .output()
        .expect("run flows");

    assert!(
        output.status.success(),
        "s15 schedule --once failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // The poll hit the mock GitHub server and the enforced KV write stored the
    // fire's summary row.
    issues.assert();
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let record: Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|err| panic!("stdout is not a SummaryRecord JSON ({err}): {stdout}"));
    assert_eq!(record["stored"], serde_json::json!(true));
    assert_eq!(record["open_count"], serde_json::json!(2));
    assert!(
        record["key"]
            .as_str()
            .expect("key field")
            .starts_with("s15_scheduled_poll_flow:poll_trigger:"),
        "summary key must be scheduled-time keyed: {record}"
    );

    std::fs::remove_file(&path).ok();
}
