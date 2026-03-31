use std::fs;
use std::path::PathBuf;
use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::json;
use tempfile::tempdir;

fn temp_bundle_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.push(format!("lattice.bundle.it.{label}.{pid}.{counter}"));
    path
}

#[test]
fn bundle_rejects_invalid_flow_ir_path() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let manifest_path = temp.path().join("manifest.json");
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flows": [
            {
                "id": "flow://demo",
                "version": "v0.1.0",
                "profile": "wasm",
                "flow_ir": {
                    "artifact": "../flow_ir.json",
                    "hash": "sha256:2222222222222222222222222222222222222222222222222222222222222222"
                }
            }
        ]
    });

    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest_json)?)?;

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "host-workers",
            "--manifest",
            manifest_path.to_str().expect("manifest path"),
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
            "--dev",
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        !output.status.success(),
        "expected bundle to fail for invalid manifest: {output:?}"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("flow_ir artifact path must not traverse"),
        "stderr missing path validation detail: {stderr}"
    );

    Ok(())
}

#[test]
fn bundle_generates_manifest_when_missing() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-s6-spill",
            "--native",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        output.status.success(),
        "expected bundle to succeed without --manifest: {output:?}"
    );
    assert!(out_dir.join("manifest.json").exists());
    assert!(out_dir.join("flows/s6_spill_flow/flow_ir.json").exists());

    Ok(())
}

#[test]
fn bundle_generates_wasm_manifest_for_connector_google_sheets_example()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let out_dir = temp.path().join("flow.bundle");
    let target_dir = temp.path().join("target");

    let output = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-connector-google-sheets-local-flow",
            "--wasm",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("bundle output path"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        output.status.success(),
        "expected connector example wasm bundle to succeed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(out_dir.join("manifest.json").exists());
    assert!(
        out_dir
            .join("flows/connector_google_sheets_local_flow/flow_ir.json")
            .exists()
    );

    let manifest_raw = fs::read_to_string(out_dir.join("manifest.json"))?;
    let manifest_json: serde_json::Value = serde_json::from_str(&manifest_raw)?;
    assert_eq!(
        manifest_json["flows"][0]["entrypoints"][0]["method"],
        json!("POST")
    );

    Ok(())
}

/// Proves that the s6_spill example (which uses blob capability) can be built
/// as a wasm bundle and executed end-to-end through the wasmtime host.
/// The host provides a MemoryBlobStore; the guest talks to it through the
/// `RemoteBlobStore` → `lf_cap_call(OP_BLOB_*)` → `host_block_on` bridge.
#[test]
fn run_bundle_s6_spill_blob_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = temp_bundle_dir("s6-spill-blob");
    let target_dir = out_dir.join("target");
    fs::create_dir_all(&target_dir)?;

    let build = Command::cargo_bin("flows")?
        .args([
            "bundle",
            "-p",
            "example-s6-spill",
            "--wasm",
            "--dev",
            "--out-dir",
            out_dir.to_str().expect("out dir"),
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;
    assert!(
        build.status.success(),
        "s6_spill wasm bundle failed: status={:?}, stderr={}",
        build.status,
        String::from_utf8_lossy(&build.stderr)
    );

    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "bundle",
            "--bundle",
            out_dir.to_str().expect("out dir"),
            "--bind",
            "resource::blob=memory",
            "--payload",
            r#"{"batch_id":"wasm-test","items":["alpha","beta"]}"#,
        ])
        .output()?;
    assert!(
        output.status.success(),
        "s6_spill bundle run failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: serde_json::Value = serde_json::from_str(stdout.trim())?;
    let acks = payload.as_array().expect("output should be array of acks");
    assert_eq!(acks.len(), 2);
    assert_eq!(acks[0]["batch_id"], "wasm-test");
    assert_eq!(acks[0]["stored"], true);
    assert_eq!(acks[1]["batch_id"], "wasm-test");
    assert_eq!(acks[1]["stored"], true);

    Ok(())
}
