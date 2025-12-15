use std::process::Command;

use assert_cmd::prelude::*;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde_json::Value;
use serde_json::json;

#[test]
fn run_local_echoes_normalised_payload() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s1_echo",
            "--payload",
            r#"{"value": "HeLLo  "}"#,
        ])
        .output()?;

    assert!(
        output.status.success(),
        "flows run local exited unsuccessfully: {:?}",
        output
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(
        payload.get("value").and_then(Value::as_str),
        Some("hello"),
        "normalised value should be lowercase without surrounding whitespace"
    );

    Ok(())
}

fn invoke_run_local(payload: &Value) -> Result<Value, String> {
    let payload_str = payload.to_string();
    let mut cmd = Command::cargo_bin("flows").map_err(|err| err.to_string())?;
    cmd.args(["run", "local", "--example", "s1_echo", "--payload"])
        .arg(payload_str);
    let output = cmd.output().map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "flows run local failed: status={:?}, stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let stdout = String::from_utf8(output.stdout).map_err(|err| err.to_string())?;
    serde_json::from_str(stdout.trim()).map_err(|err| err.to_string())
}

#[test]
fn run_local_handles_property_inputs() {
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 16,
        ..ProptestConfig::default()
    });
    let strategy = proptest::collection::vec(proptest::char::any(), 0..=12)
        .prop_map(|chars| chars.into_iter().collect::<String>());

    runner
        .run(&strategy, |input| {
            let payload = json!({ "value": input.clone() });
            let expected = input.trim().to_lowercase();
            match invoke_run_local(&payload) {
                Ok(result) => {
                    let actual = result
                        .get("value")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    prop_assert_eq!(
                        actual,
                        expected,
                        "normalised output mismatch for input {:?}",
                        input
                    );
                    Ok(())
                }
                Err(err) => Err(TestCaseError::fail(err)),
            }
        })
        .unwrap();
}

#[test]
fn run_local_preflight_fails_without_required_bindings() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::cargo_bin("flows")?
        .args(["run", "local", "--example", "s4_preflight"])
        .output()?;

    assert!(!output.status.success(), "expected failure: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CAP101"), "stderr missing CAP101: {stderr}");
    assert!(
        stderr.contains("resource::kv::read"),
        "stderr missing required hint: {stderr}"
    );

    Ok(())
}

#[test]
fn run_local_preflight_succeeds_with_kv_binding() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s4_preflight",
            "--bind",
            "resource::kv=memory",
        ])
        .output()?;

    assert!(output.status.success(), "expected success: {output:?}");

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload, json!({}));

    Ok(())
}

#[test]
fn run_local_streams_events() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s2_site",
            "--payload",
            r#"{"site": "alpha"}"#,
            "--stream",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "flows run local exited with failure: {output:?}"
    );

    let stdout = String::from_utf8(output.stdout)?;
    let events: Vec<_> = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    assert!(
        events.len() >= 2,
        "expected multiple streaming events, got {events:?}"
    );

    let first: Value = serde_json::from_str(events[0])?;
    assert_eq!(
        first.get("stage"),
        Some(&Value::String("snapshot".to_string()))
    );

    let second: Value = serde_json::from_str(events[1])?;
    assert!(
        second
            .get("stage")
            .and_then(Value::as_str)
            .map(|stage| stage.starts_with("update_"))
            .unwrap_or(false)
    );

    Ok(())
}

#[test]
fn run_local_emits_json_summary() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s1_echo",
            "--payload",
            r#"{"value": "Ping"}"#,
            "--json",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "flows run local --json exited unsuccessfully: {:?}",
        output
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;

    assert_eq!(
        payload.get("example").and_then(Value::as_str),
        Some("s1_echo"),
        "example name should be included"
    );

    assert_eq!(
        payload
            .get("result")
            .and_then(|value| value.get("value"))
            .and_then(Value::as_str),
        Some("ping"),
        "result should include normalised payload"
    );

    let summary = payload
        .get("summary")
        .cloned()
        .expect("summary missing in JSON output");
    assert!(
        summary.get("duration_ms").and_then(Value::as_f64).is_some(),
        "duration_ms missing in summary"
    );

    let nodes = summary
        .get("nodes")
        .and_then(Value::as_array)
        .expect("nodes array missing in summary");
    assert!(!nodes.is_empty(), "expected node metrics in summary");

    Ok(())
}

#[test]
fn run_local_branching_routes_then_branch() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::cargo_bin("flows")?
        .args([
            "run",
            "local",
            "--example",
            "s3_branching",
            "--payload",
            r#"{"flag": true, "mode": "a"}"#,
        ])
        .output()?;

    assert!(
        output.status.success(),
        "flows run local s3_branching failed: {:?}",
        output
    );

    let stdout = String::from_utf8(output.stdout)?;
    let payload: Value = serde_json::from_str(stdout.trim())?;
    assert_eq!(payload.get("branch").and_then(Value::as_str), Some("then"));
    assert_eq!(
        payload.get("mode_selected").and_then(Value::as_str),
        Some("a")
    );

    Ok(())
}
