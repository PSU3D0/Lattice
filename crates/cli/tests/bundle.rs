use std::fs;
use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::json;
use tempfile::tempdir;

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
