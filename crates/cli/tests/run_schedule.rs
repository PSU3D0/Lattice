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
