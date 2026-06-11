//! Tests for `flows bundle requirements` (packet C3).
//!
//! The command emits the static FlowRequirements manifest (packet C1) as JSON
//! — the seed artifact for the future infra-from-code planner. Two source
//! modes emit two precise forms of the same shape:
//!
//! - `--example <name>` emits the **bare-IR form** (no `flow_ir_hash`, no
//!   entrypoint `deadline_ms`), byte-equivalent to the C1 golden fixtures
//!   under `crates/kernel-plan/tests/fixtures/`. These golden tests pin that
//!   equivalence: the command must not drift from the handwritten fixtures.
//! - `--bundle <path>` prints the **enriched form** an assembled bundle
//!   carried (with both enrichments). Built in-test from the flow registry via
//!   `exporters::bundle` (no wasm build required), mirroring the bundle
//!   assembly path that C1's `requirements_carry` test exercises.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use assert_cmd::prelude::*;
use dag_core::FlowRequirements;
use flow_bundle::{
    AbiRef, Capabilities, CodeDescriptor, FlowEntry, Manifest, compute_bundle_id, sha256_prefixed,
};
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel-plan/tests/fixtures")
        .join(name);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read fixture {}: {err}", path.display()));
    serde_json::from_str(&text).expect("fixture is valid JSON")
}

fn run_requirements(args: &[&str]) -> std::process::Output {
    Command::cargo_bin("flows")
        .expect("flows binary")
        .arg("bundle")
        .arg("requirements")
        .args(args)
        .output()
        .expect("run flows bundle requirements")
}

fn stdout_json(output: &std::process::Output) -> Value {
    assert!(
        output.status.success(),
        "command failed: status={:?}, stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "stdout is not valid JSON ({err}): {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

#[test]
fn example_s1_echo_matches_bare_golden() {
    let output = run_requirements(&["--example", "s1_echo"]);
    let actual = stdout_json(&output);
    assert_eq!(
        actual,
        fixture("s1_echo.requirements.json"),
        "--example s1_echo drifted from the bare-IR golden fixture"
    );
    // The bare form carries neither bundle-assembly enrichment.
    assert!(actual.get("flow_ir_hash").is_none());
    assert!(actual["entrypoints"][0].get("deadline_ms").is_none());
}

#[test]
fn example_s12_bound_matches_bare_golden() {
    let output = run_requirements(&["--example", "s12_sheetport_quote"]);
    let actual = stdout_json(&output);
    assert_eq!(
        actual,
        fixture("s12_sheetport_quote_bound.requirements.json"),
        "--example s12_sheetport_quote drifted from the bound bare-IR golden fixture"
    );
    // s12 bound: a connector op selected in bound_connection mode.
    assert_eq!(actual["host"]["requires_connector_runtime"], Value::Bool(true));
    assert_eq!(
        actual["connectors"][0]["operations"][0]["requires_bound_connection"],
        Value::Bool(true)
    );
}

#[test]
fn out_flag_writes_file_and_prints_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let out = temp.path().join("reqs.json");
    let output = run_requirements(&[
        "--example",
        "s1_echo",
        "--out",
        out.to_str().expect("out path"),
    ]);
    assert!(output.status.success(), "command failed: {output:?}");
    let written: Value = serde_json::from_slice(&fs::read(&out).expect("read out file"))
        .expect("written file is valid JSON");
    assert_eq!(written, fixture("s1_echo.requirements.json"));
    let printed = String::from_utf8_lossy(&output.stdout);
    assert!(
        printed.trim().ends_with("reqs.json"),
        "expected the output path to be printed: {printed}"
    );
}

#[test]
fn schema_flag_matches_checked_in_schema_byte_for_byte() {
    let output = run_requirements(&["--schema"]);
    assert!(output.status.success(), "command failed: {output:?}");

    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schemas/flow_requirements.schema.json");
    let on_disk = fs::read(&schema_path).expect("read schema file");
    assert_eq!(
        output.stdout, on_disk,
        "--schema output is not byte-for-byte identical to {}",
        schema_path.display()
    );

    // And it must parse as JSON (valid schema document).
    let parsed: Value = serde_json::from_slice(&output.stdout).expect("schema output is valid JSON");
    assert_eq!(
        parsed["$id"],
        Value::String("https://lattice.dev/schemas/flow_requirements.schema.json".to_string())
    );
}

/// Enriched requirements for a flow: the bare derivation plus the two
/// bundle-assembly enrichments (`flow_ir_hash`, entrypoint `deadline_ms`),
/// matching what `exporters::bundle` carries. Hand-assembled here so the
/// `--bundle` reader is tested without a wasm build (the enrichment path
/// itself is proven by `flow-bundle`'s `requirements_carry` test).
fn enriched_requirements(flow: &dag_core::FlowIR, deadline_ms: u64) -> (FlowRequirements, String) {
    let flow_ir_bytes = serde_json::to_vec_pretty(flow).expect("serialize flow ir");
    let flow_ir_hash = sha256_prefixed(&flow_ir_bytes);
    let mut requirements = FlowRequirements::derive(flow)
        .expect("derive requirements")
        .with_flow_ir_hash(flow_ir_hash.clone());
    for entrypoint in &mut requirements.entrypoints {
        entrypoint.deadline_ms = Some(deadline_ms);
    }
    (requirements, flow_ir_hash)
}

fn write_single_flow_bundle(
    dir: &Path,
    flow: &dag_core::FlowIR,
    requirements: FlowRequirements,
    flow_ir_hash: String,
) {
    fs::create_dir_all(dir).expect("create bundle dir");
    let artifact = "flows/flow_ir.json";
    let flow_ir_bytes = serde_json::to_vec_pretty(flow).expect("serialize flow ir");
    let ir_path = dir.join(artifact);
    fs::create_dir_all(ir_path.parent().unwrap()).expect("create ir dir");
    fs::write(&ir_path, &flow_ir_bytes).expect("write flow ir");

    let mut manifest = Manifest {
        bundle_version: "0.1".to_string(),
        abi: AbiRef {
            name: "latticeflow.wit".to_string(),
            version: "0.1".to_string(),
        },
        bundle_id: String::new(),
        code: CodeDescriptor {
            target: "wasm32-unknown-unknown".to_string(),
            file: "module.wasm".to_string(),
            hash: sha256_prefixed(&[]),
            size_bytes: 0,
        },
        artifacts: Vec::new(),
        flows: vec![FlowEntry {
            id: flow.id.as_str().to_string(),
            version: flow.version.to_string(),
            profile: "web".to_string(),
            flow_ir: Some(flow_bundle::FlowIrRef {
                artifact: artifact.to_string(),
                hash: flow_ir_hash,
            }),
            flow_ir_expanded: None,
            entrypoints: Vec::new(),
            nodes: BTreeMap::new(),
            capabilities: Capabilities::default(),
            subflows: Vec::new(),
            wasm_guest_exports: None,
            requirements: Some(requirements),
        }],
        subflows: Vec::new(),
        default_flow: Some(flow.id.as_str().to_string()),
        signing: None,
    };
    manifest.bundle_id = compute_bundle_id(&manifest).expect("bundle id");
    fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
    )
    .expect("write manifest");
}

#[test]
fn bundle_flag_prints_enriched_carried_requirements() {
    let flow = example_s1_echo::flow();
    let (requirements, flow_ir_hash) = enriched_requirements(&flow, 250);

    let temp = tempfile::tempdir().expect("tempdir");
    let bundle_dir = temp.path().join("flow.bundle");
    write_single_flow_bundle(&bundle_dir, &flow, requirements, flow_ir_hash);

    let output = run_requirements(&["--bundle", bundle_dir.to_str().expect("bundle path")]);
    let actual = stdout_json(&output);

    // The enriched form carries both bundle-assembly values the bare
    // --example form omits.
    let hash = actual
        .get("flow_ir_hash")
        .and_then(Value::as_str)
        .expect("enriched form carries flow_ir_hash");
    assert!(
        hash.starts_with("sha256:"),
        "flow_ir_hash should be sha256-prefixed: {hash}"
    );
    assert_eq!(
        actual["entrypoints"][0]["deadline_ms"],
        Value::from(250u64),
        "the carried entrypoint deadline should be printed"
    );

    // Stripping the two enrichments must leave exactly the bare golden.
    let mut stripped = actual.clone();
    stripped.as_object_mut().unwrap().remove("flow_ir_hash");
    stripped["entrypoints"][0]
        .as_object_mut()
        .unwrap()
        .remove("deadline_ms");
    assert_eq!(
        stripped,
        fixture("s1_echo.requirements.json"),
        "enriched bundle requirements minus enrichments must equal the bare golden"
    );
}

#[test]
fn bundle_flag_selects_flow_by_id() {
    let flow = example_s1_echo::flow();
    let flow_id = flow.id.as_str().to_string();
    let (requirements, flow_ir_hash) = enriched_requirements(&flow, 250);

    let temp = tempfile::tempdir().expect("tempdir");
    let bundle_dir = temp.path().join("flow.bundle");
    write_single_flow_bundle(&bundle_dir, &flow, requirements, flow_ir_hash);

    // Selecting the carried flow by id succeeds.
    let selected = run_requirements(&[
        "--bundle",
        bundle_dir.to_str().expect("bundle path"),
        "--flow",
        &flow_id,
    ]);
    let actual = stdout_json(&selected);
    assert_eq!(actual["flow"]["id"], Value::String(flow_id));

    // Selecting an unknown flow id fails closed.
    let missing = run_requirements(&[
        "--bundle",
        bundle_dir.to_str().expect("bundle path"),
        "--flow",
        "does-not-exist",
    ]);
    assert!(
        !missing.status.success(),
        "expected failure for unknown --flow id"
    );
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("does not define flow"),
        "stderr should explain the missing flow: {}",
        String::from_utf8_lossy(&missing.stderr)
    );
}
