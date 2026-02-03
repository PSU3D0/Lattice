use std::fs;
use std::process::Command;

use assert_cmd::prelude::*;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn bundle_rejects_uppercase_artifact_hash_in_manifest() -> Result<(), Box<dyn std::error::Error>> {
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
        "artifacts": [
            {
                "target": "x86_64-unknown-linux-gnu",
                "file": "flow",
                "hash": "sha256:222222222222222222222222222222222222222222222222222222222222222A"
            }
        ],
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        }
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
        ])
        .env("CARGO_TARGET_DIR", &target_dir)
        .output()?;

    assert!(
        !output.status.success(),
        "expected bundle to fail for invalid manifest: {output:?}"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("manifest validation"),
        "stderr missing validation context: {stderr}"
    );
    assert!(
        stderr.contains("artifacts[].hash"),
        "stderr missing hash detail: {stderr}"
    );

    Ok(())
}
