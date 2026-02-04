use std::fs::{self, File};
use std::path::Path;

use assert_cmd::Command;
use capabilities::durability::{
    CheckpointRecord, FlowFrontier, FrontierEntry, IdempotencyState, SerializedState,
};
use dag_core::FlowId;
use serde_json::json;

fn write_checkpoint(root: &Path, record: &CheckpointRecord) {
    let path = root
        .join(record.flow_id.as_str())
        .join(&record.run_id)
        .join(format!("{}.json", record.checkpoint_id));
    fs::create_dir_all(path.parent().expect("checkpoint parent")).unwrap();
    let file = File::create(path).unwrap();
    serde_json::to_writer(file, record).unwrap();
}

fn sample_checkpoint_record(flow_id: &str, run_id: &str, checkpoint_id: &str) -> CheckpointRecord {
    CheckpointRecord {
        checkpoint_id: checkpoint_id.to_string(),
        flow_id: FlowId(flow_id.to_string()),
        flow_version: "0.1.0".to_string(),
        run_id: run_id.to_string(),
        parent_run_id: None,
        frontier: FlowFrontier {
            completed: vec![FrontierEntry {
                node_alias: "node_a".to_string(),
                output_port: "out".to_string(),
                cursor: None,
            }],
            pending: vec!["node_b".to_string()],
        },
        state: SerializedState {
            data: json!({"value": 42}),
            blobs: Vec::new(),
        },
        idempotency: IdempotencyState::default(),
        created_at_ms: 0,
        resume_after_ms: Some(0),
        ttl_ms: None,
        version: 1,
    }
}

fn sample_checkpoint_record_with_ttl(
    flow_id: &str,
    run_id: &str,
    checkpoint_id: &str,
    created_at_ms: u64,
    ttl_ms: u64,
) -> CheckpointRecord {
    let mut record = sample_checkpoint_record(flow_id, run_id, checkpoint_id);
    record.created_at_ms = created_at_ms;
    record.ttl_ms = Some(ttl_ms);
    record
}

#[test]
fn resume_list_works() {
    let dir = tempfile::tempdir().unwrap();
    let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_1");
    write_checkpoint(dir.path(), &record);

    let mut cmd = Command::cargo_bin("flows").unwrap();
    cmd.args([
        "resume",
        "list",
        "--checkpoint-dir",
        dir.path().to_str().unwrap(),
        "--flow",
        "flow_a",
    ]);
    let output = cmd.assert().success().get_output().stdout.clone();
    let listed: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["checkpoint_id"], "ckpt_1");
}

#[test]
fn resume_show_works() {
    let dir = tempfile::tempdir().unwrap();
    let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_2");
    write_checkpoint(dir.path(), &record);

    let mut cmd = Command::cargo_bin("flows").unwrap();
    cmd.args([
        "resume",
        "show",
        "ckpt_2",
        "--checkpoint-dir",
        dir.path().to_str().unwrap(),
    ]);
    let output = cmd.assert().success().get_output().stdout.clone();
    let shown: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let expected = serde_json::to_value(&record).unwrap();
    assert_eq!(shown, expected);
}

#[test]
fn resume_list_due_defaults_to_active_status() {
    let dir = tempfile::tempdir().unwrap();
    let active = sample_checkpoint_record("flow_a", "run_1", "ckpt_active");
    let mut due_active = active.clone();
    due_active.resume_after_ms = Some(0);
    write_checkpoint(dir.path(), &due_active);

    let expired = sample_checkpoint_record_with_ttl("flow_a", "run_1", "ckpt_expired", 0, 1);
    let mut due_expired = expired.clone();
    due_expired.resume_after_ms = Some(0);
    write_checkpoint(dir.path(), &due_expired);

    let mut cmd = Command::cargo_bin("flows").unwrap();
    cmd.args([
        "resume",
        "list",
        "--checkpoint-dir",
        dir.path().to_str().unwrap(),
        "--due",
    ]);
    let output = cmd.assert().success().get_output().stdout.clone();
    let listed: Vec<serde_json::Value> = serde_json::from_slice(&output).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["checkpoint_id"], "ckpt_active");
}

#[test]
fn resume_show_requires_flow_or_run_when_checkpoint_id_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let record_a = sample_checkpoint_record("flow_a", "run_1", "ckpt_dup");
    let record_b = sample_checkpoint_record("flow_b", "run_2", "ckpt_dup");
    write_checkpoint(dir.path(), &record_a);
    write_checkpoint(dir.path(), &record_b);

    let mut cmd = Command::cargo_bin("flows").unwrap();
    cmd.args([
        "resume",
        "show",
        "ckpt_dup",
        "--checkpoint-dir",
        dir.path().to_str().unwrap(),
    ]);
    cmd.assert()
        .failure()
        .stderr(predicates::str::contains("matches multiple records"))
        .stderr(predicates::str::contains("--flow"))
        .stderr(predicates::str::contains("--run"));
}

#[test]
fn resume_run_fails_when_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let record = sample_checkpoint_record("flow_a", "run_1", "ckpt_unsupported");
    write_checkpoint(dir.path(), &record);

    let mut cmd = Command::cargo_bin("flows").unwrap();
    cmd.args([
        "resume",
        "run",
        "ckpt_unsupported",
        "--checkpoint-dir",
        dir.path().to_str().unwrap(),
    ]);
    cmd.assert().failure().stderr(predicates::str::contains(
        "resume execution not yet supported",
    ));
}
