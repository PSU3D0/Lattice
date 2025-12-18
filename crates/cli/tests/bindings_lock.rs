use std::path::PathBuf;
use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::Value;

fn temp_lock_path() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.bindings.lock.it.{pid}.{counter}.json"));
    path
}

fn generate_lock(
    example: &str,
    extra_binds: &[&str],
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = temp_lock_path();
    let mut cmd = Command::cargo_bin("flows")?;
    cmd.args([
        "bindings",
        "lock",
        "generate",
        "--example",
        example,
        "--out",
        path.to_str().expect("path"),
    ]);

    for bind in extra_binds {
        cmd.args(["--bind", bind]);
    }

    let output = cmd.output()?;
    assert!(
        output.status.success(),
        "lock generate failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(path)
}

#[test]
fn run_local_succeeds_with_bindings_lock() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock("s4_preflight", &["resource::kv=memory"])?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
        ])
        .output()?;

    assert!(
        output.status.success(),
        "flows run local failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload, serde_json::json!({}));

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_rejects_bind_and_bindings_lock_together() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock("s4_preflight", &["resource::kv=memory"])?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
            "--bind",
            "resource::kv=memory",
        ])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--bindings-lock cannot be combined with --bind"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_rejects_hash_mismatch_in_lock() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock("s4_preflight", &["resource::kv=memory"])?;

    let raw = std::fs::read_to_string(&lock_path)?;
    let mut json: Value = serde_json::from_str(&raw)?;
    json["content_hash"] = serde_json::json!("deadbeef");
    std::fs::write(&lock_path, serde_json::to_vec(&json)?)?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
        ])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("content_hash mismatch"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}

#[test]
fn run_local_rejects_lock_missing_flow_id() -> Result<(), Box<dyn std::error::Error>> {
    let lock_path = generate_lock("s1_echo", &[])?;

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bindings-lock",
            lock_path.to_str().expect("path"),
        ])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("does not define bindings for flow_id"),
        "unexpected stderr: {stderr}"
    );

    std::fs::remove_file(&lock_path).ok();
    Ok(())
}
