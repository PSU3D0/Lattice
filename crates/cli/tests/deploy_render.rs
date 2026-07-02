//! Tests for `flows deploy render` (packet W1 of
//! ops/phase1-clone-engine-plan-2026-06-12.md): FlowRequirements (+ optional
//! bindings.lock) → wrangler.toml.
//!
//! Coverage:
//! - golden files: `s1_echo` (http-only, near-empty bindings) and a synthetic
//!   kv+blob manifest (no built-in example declares both today — s4 is
//!   kv-only, s6 is blob-only, s12 is blob-only; the synthetic fixture also
//!   exercises the pending-T3 schedule stub via an `unspecified` trigger);
//! - every rendered file must PARSE as TOML (asserted with the `toml` crate,
//!   already in the dependency tree via trybuild);
//! - fail-closed: a hand-built manifest with `resource::db` + `resource::rng`
//!   hints must abort with per-node attribution, a reason, and a fix — that
//!   error text is planner v0;
//! - reproduction: a synthetic manifest matching what
//!   `crates/host-workers/workerd-tests/wrangler.toml` provisions renders a
//!   config whose binding sections match the hand-written one (modulo
//!   names/ids) — the packet's acceptance bar;
//! - size budget: >1MiB compressed fails on the free tier, passes (with a
//!   warning) under `--paid`;
//! - bindings.lock: a lock instance carrying a KV namespace id replaces the
//!   placeholder.
//!
//! Regenerate goldens: `LATTICE_BLESS_GOLDEN=1 cargo test -p flows-cli --test deploy_render`

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use assert_cmd::prelude::*;
use toml::Table;

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/deploy_golden")
}

fn run_render(args: &[&str]) -> std::process::Output {
    Command::cargo_bin("flows")
        .expect("flows binary")
        .arg("deploy")
        .arg("render")
        .args(args)
        .output()
        .expect("run flows deploy render")
}

/// Render into a temp dir and return the wrangler.toml contents.
fn render_ok(args: &[&str]) -> (String, String) {
    let temp = tempfile::tempdir().expect("tempdir");
    let out_dir = temp.path().join("deploy");
    let mut full: Vec<&str> = args.to_vec();
    let out_str = out_dir.to_str().expect("out path").to_string();
    full.push("--out");
    full.push(&out_str);
    let output = run_render(&full);
    assert!(
        output.status.success(),
        "render failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let toml_text =
        fs::read_to_string(out_dir.join("wrangler.toml")).expect("read rendered wrangler.toml");
    let notes = String::from_utf8_lossy(&output.stderr).to_string();
    (toml_text, notes)
}

fn parse_toml(text: &str, context: &str) -> Table {
    text.parse::<Table>()
        .unwrap_or_else(|err| panic!("{context} is not valid TOML ({err}):\n{text}"))
}

fn assert_matches_golden(rendered: &str, golden_name: &str) {
    let golden_path = fixture_root().join(golden_name);
    if std::env::var_os("LATTICE_BLESS_GOLDEN").is_some() {
        fs::write(&golden_path, rendered).expect("bless golden");
        return;
    }
    let expected = fs::read_to_string(&golden_path).unwrap_or_else(|err| {
        panic!(
            "missing golden {} ({err}); run with LATTICE_BLESS_GOLDEN=1 to create it",
            golden_path.display()
        )
    });
    assert_eq!(
        rendered,
        expected,
        "rendered wrangler.toml drifted from golden {}",
        golden_path.display()
    );
}

fn fixture_arg(name: &str) -> String {
    fixture_root().join(name).to_str().expect("fixture path").to_string()
}

// ---------------------------------------------------------------------------
// Golden: s1_echo — http-only flow, near-empty bindings (just the durability
// checkpoint store Durable Object; outbound HTTP is ambient on Workers).
// ---------------------------------------------------------------------------
#[test]
fn s1_echo_renders_golden_and_parses() {
    let (rendered, notes) = render_ok(&["--example", "s1_echo"]);
    assert_matches_golden(&rendered, "s1_echo.wrangler.toml.golden");

    let parsed = parse_toml(&rendered, "s1_echo render");
    assert_eq!(parsed["name"].as_str(), Some("s1-echo-flow"));
    assert_eq!(parsed["main"].as_str(), Some("build/worker/shim.mjs"));
    // http-only: no kv/d1/r2 sections.
    assert!(parsed.get("kv_namespaces").is_none());
    assert!(parsed.get("d1_databases").is_none());
    assert!(parsed.get("r2_buckets").is_none());
    // durability checkpoint store -> FLOW_DO (host-workers idiom).
    let bindings = parsed["durable_objects"]["bindings"].as_array().expect("do bindings");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["name"].as_str(), Some("FLOW_DO"));
    assert_eq!(bindings[0]["class_name"].as_str(), Some("FlowDurableObject"));
    // Budget was not silently skipped.
    assert!(notes.contains("module size budget NOT checked"));
    assert!(rendered.contains("compressed module size was NOT checked"));
}

// ---------------------------------------------------------------------------
// Golden: synthetic kv+blob manifest (also exercises the pending-T3 schedule
// stub via its `unspecified` trigger).
// ---------------------------------------------------------------------------
#[test]
fn kv_blob_renders_golden_with_placeholders_and_schedule_stub() {
    let requirements = fixture_arg("kv_blob.requirements.json");
    let (rendered, notes) = render_ok(&["--requirements", &requirements]);
    assert_matches_golden(&rendered, "kv_blob.wrangler.toml.golden");

    let parsed = parse_toml(&rendered, "kv_blob render");
    let kv = parsed["kv_namespaces"].as_array().expect("kv namespaces");
    assert_eq!(kv.len(), 1);
    assert_eq!(kv[0]["binding"].as_str(), Some("FLOW_KV"));
    assert_eq!(kv[0]["id"].as_str(), Some("REPLACE_WITH_KV_NAMESPACE_ID"));
    let r2 = parsed["r2_buckets"].as_array().expect("r2 buckets");
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0]["binding"].as_str(), Some("BLOB_BUCKET"));
    assert_eq!(r2[0]["bucket_name"].as_str(), Some("kv-blob-demo-flow-blob"));

    // A fresh user can follow the file: each placeholder carries the exact
    // creation command.
    assert!(rendered.contains("wrangler kv namespace create kv-blob-demo-flow-kv"));
    assert!(rendered.contains("wrangler r2 bucket create kv-blob-demo-flow-blob"));
    // Per-node attribution is visible next to each binding.
    assert!(rendered.contains("required by node(s): cache_read, cache_write"));
    assert!(rendered.contains("required by node(s): store"));

    // The schedule/cron arm is a clearly marked stub until T3 lands.
    assert!(rendered.contains("STUB(T3)"));
    assert!(rendered.contains("impl-docs/spec/schedule-trigger.md"));
    assert!(notes.contains("pending packet T3"));
}

// ---------------------------------------------------------------------------
// Fail-closed: unmappable requirements produce the planner-v0 error.
// ---------------------------------------------------------------------------
#[test]
fn unmappable_requirements_fail_closed_with_attribution() {
    let requirements = fixture_arg("db_native.requirements.json");
    let temp = tempfile::tempdir().expect("tempdir");
    let out = temp.path().join("deploy");
    let output = run_render(&[
        "--requirements",
        &requirements,
        "--out",
        out.to_str().expect("out"),
    ]);
    assert!(!output.status.success(), "db+rng manifest must fail closed");
    assert!(
        !out.join("wrangler.toml").exists(),
        "no partial config may be written on failure"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    // Header names the flow and counts the gaps.
    assert!(
        stderr.contains(
            "flow `db_native_demo_flow` cannot be rendered for Cloudflare Workers: \
             2 requirement families have no Workers mapping"
        ),
        "missing error header: {stderr}"
    );
    // Exactly which nodes need what... (hint strings via EffectHint::as_str(),
    // per the hint-literal grep gate)
    assert!(stderr.contains(&format!(
        "{} — required by node(s): fetch_legacy",
        dag_core::EffectHint::Db.as_str()
    )));
    assert!(stderr.contains(&format!(
        "{} — required by node(s): shuffle",
        dag_core::EffectHint::Rng.as_str()
    )));
    // ...why it cannot run on Workers...
    assert!(stderr.contains("no Workers capability provider"));
    // ...and what to do about it.
    assert!(stderr.contains("migrate the node to `resource::sql` (D1 via cap-sql-workers-d1)"));
    assert!(stderr.contains("This flow can still run on a native host"));
}

// ---------------------------------------------------------------------------
// Reproduction: the renderer must reproduce the binding shapes of the
// hand-written crates/host-workers/workerd-tests/wrangler.toml (modulo
// names/ids). Idioms the mapping CANNOT express yet, asserted absent so this
// test starts failing the day they become expressible and must be re-audited:
// - migration `tag`: hand-written config is at "v2" (it evolved in place);
//   the renderer always emits a fresh "v1". Tags are deploy-history state,
//   not derivable from requirements.
// - `workers_dev` is emitted unconditionally; the hand-written file sets it
//   explicitly too, so shapes agree today.
// - per-environment `preview_id` (s7's kv idiom) and `[limits] cpu_ms`
//   (cap-do-workers' idiom): resource sizing/dev-vs-prod splits are not in
//   FlowRequirements (spec non-goal), so they are not rendered.
// - app-specific `[vars]` like s7's OTEL endpoint: not derivable from
//   requirements; only durability-service vars are rendered.
// ---------------------------------------------------------------------------
#[test]
fn reproduces_host_workers_workerd_test_binding_shapes() {
    let requirements = fixture_arg("host_workers_repro.requirements.json");
    let (rendered, _notes) = render_ok(&["--requirements", &requirements]);
    let ours = parse_toml(&rendered, "host_workers_repro render");

    let hand_written_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../host-workers/workerd-tests/wrangler.toml");
    let hand_written_text =
        fs::read_to_string(&hand_written_path).expect("read hand-written wrangler.toml");
    let theirs = parse_toml(&hand_written_text, "hand-written host-workers config");

    // Entry + build pipeline are byte-identical idioms.
    assert_eq!(ours["main"].as_str(), theirs["main"].as_str());
    assert_eq!(
        ours["build"]["command"].as_str(),
        theirs["build"]["command"].as_str()
    );
    assert_eq!(
        ours["compatibility_date"].as_str(),
        theirs["compatibility_date"].as_str()
    );

    // Durable Object bindings: same binding names AND classes (these names
    // are meaningful — they are cap-workspace-workers/host-workers defaults).
    let mut ours_do: Vec<(String, String)> = ours["durable_objects"]["bindings"]
        .as_array()
        .expect("our do bindings")
        .iter()
        .map(|binding| {
            (
                binding["name"].as_str().unwrap().to_string(),
                binding["class_name"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let mut theirs_do: Vec<(String, String)> = theirs["durable_objects"]["bindings"]
        .as_array()
        .expect("their do bindings")
        .iter()
        .map(|binding| {
            (
                binding["name"].as_str().unwrap().to_string(),
                binding["class_name"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    ours_do.sort();
    theirs_do.sort();
    assert_eq!(ours_do, theirs_do, "durable object binding shapes must match");

    // R2: one workspace bucket with the cap-workspace-workers default binding
    // name (bucket_name is instance-specific — modulo ids).
    let ours_r2 = ours["r2_buckets"].as_array().expect("our r2");
    let theirs_r2 = theirs["r2_buckets"].as_array().expect("their r2");
    assert_eq!(ours_r2.len(), theirs_r2.len());
    assert_eq!(
        ours_r2[0]["binding"].as_str(),
        theirs_r2[0]["binding"].as_str()
    );

    // Migrations: same sqlite class set (tags are deploy-history, see header).
    let class_set = |value: &Table| -> Vec<String> {
        let mut classes: Vec<String> = value["migrations"]
            .as_array()
            .expect("migrations")
            .iter()
            .flat_map(|migration| {
                migration["new_sqlite_classes"]
                    .as_array()
                    .expect("classes")
                    .iter()
                    .map(|class| class.as_str().unwrap().to_string())
            })
            .collect();
        classes.sort();
        classes
    };
    assert_eq!(class_set(&ours), class_set(&theirs));

    // Alarm->resume dispatch: self service binding + routing var.
    let ours_services = ours["services"].as_array().expect("our services");
    let theirs_services = theirs["services"].as_array().expect("their services");
    assert_eq!(
        ours_services[0]["binding"].as_str(),
        theirs_services[0]["binding"].as_str()
    );
    // The service target is the worker's own name in both (self-binding).
    assert_eq!(
        ours_services[0]["service"].as_str(),
        ours["name"].as_str(),
        "rendered service binding must be a self-binding"
    );
    assert_eq!(
        theirs_services[0]["service"].as_str(),
        theirs["name"].as_str(),
        "hand-written service binding is a self-binding"
    );
    assert_eq!(
        ours["vars"]["LATTICE_RESUME_SERVICE_BINDING"].as_str(),
        theirs["vars"]["LATTICE_RESUME_SERVICE_BINDING"].as_str()
    );

    // Inexpressible-idiom watchdog (see test header): the renderer must not
    // silently start emitting these without a decision.
    assert!(ours.get("limits").is_none());
    assert!(!rendered.contains("preview_id"));
}

// ---------------------------------------------------------------------------
// Size budget.
// ---------------------------------------------------------------------------

/// Deterministic pseudo-random bytes (xorshift64*): effectively
/// incompressible, so the gzip size tracks the raw size closely.
fn incompressible_bytes(len: usize) -> Vec<u8> {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let value = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        out.extend_from_slice(&value.to_le_bytes());
    }
    out.truncate(len);
    out
}

#[test]
fn size_budget_fails_free_tier_and_warns_paid() {
    let temp = tempfile::tempdir().expect("tempdir");
    let artifact = temp.path().join("module.wasm");
    // ~4MiB incompressible: > 1MiB free-tier limit, > 3MiB paid warn
    // threshold, < 10MiB paid limit.
    fs::write(&artifact, incompressible_bytes(4 * 1024 * 1024)).expect("write artifact");
    let artifact_arg = artifact.to_str().expect("artifact path");

    // Free tier: hard error, pointing at --paid.
    let out = temp.path().join("free");
    let output = run_render(&[
        "--example",
        "s1_echo",
        "--wasm-artifact",
        artifact_arg,
        "--out",
        out.to_str().expect("out"),
    ]);
    assert!(!output.status.success(), "over-budget module must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("over the free tier limit of 1 MiB"), "{stderr}");
    assert!(stderr.contains("--paid"), "{stderr}");

    // Paid tier: renders, but the warning is in NOTES and in the file.
    let (rendered, notes) = render_ok(&[
        "--example",
        "s1_echo",
        "--wasm-artifact",
        artifact_arg,
        "--paid",
    ]);
    assert!(notes.contains("above the 3 MiB warning threshold"), "{notes}");
    assert!(rendered.contains("WARNING:"), "budget warning must be in the file");
    assert!(rendered.contains("paid tier"), "budget comment must name the tier");
    parse_toml(&rendered, "paid render");
}

// ---------------------------------------------------------------------------
// bindings.lock supplies real instance ids.
// ---------------------------------------------------------------------------
#[test]
fn bindings_lock_instance_id_replaces_placeholder() {
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("bindings.lock.json");
    fs::write(
        &lock_path,
        serde_json::json!({
            "version": 1,
            "generated_at": "1970-01-01T00:00:00Z",
            "content_hash": "0000",
            "instances": {
                "kv_default": {
                    "provider_kind": "workers-kv",
                    "provides": [
                        dag_core::EffectHint::KvRead.as_str(),
                        dag_core::EffectHint::KvWrite.as_str()
                    ],
                    "connect": { "namespace_id": "6f2ab1c3d4e5f60718293a4b5c6d7e8f" }
                }
            }
        })
        .to_string(),
    )
    .expect("write lock");

    let requirements = fixture_arg("kv_blob.requirements.json");
    let (rendered, _notes) = render_ok(&[
        "--requirements",
        &requirements,
        "--bindings-lock",
        lock_path.to_str().expect("lock path"),
    ]);
    let parsed = parse_toml(&rendered, "lock-backed render");
    let kv = parsed["kv_namespaces"].as_array().expect("kv namespaces");
    assert_eq!(
        kv[0]["id"].as_str(),
        Some("6f2ab1c3d4e5f60718293a4b5c6d7e8f"),
        "lock-supplied namespace id must replace the placeholder"
    );
    assert!(rendered.contains("bindings.lock instance `kv_default` (provider_kind = workers-kv)"));
    // Blob had no lock instance: placeholder flow remains intact.
    assert!(rendered.contains("wrangler r2 bucket create kv-blob-demo-flow-blob"));
}

// ---------------------------------------------------------------------------
// Custom worker name flows through bindings and self service references.
// ---------------------------------------------------------------------------
#[test]
fn custom_name_overrides_derived_worker_name() {
    let requirements = fixture_arg("host_workers_repro.requirements.json");
    let (rendered, _notes) = render_ok(&["--requirements", &requirements, "--name", "repro-w1"]);
    let parsed = parse_toml(&rendered, "named render");
    assert_eq!(parsed["name"].as_str(), Some("repro-w1"));
    let services = parsed["services"].as_array().expect("services");
    assert_eq!(services[0]["service"].as_str(), Some("repro-w1"));
    assert!(rendered.contains("wrangler r2 bucket create repro-w1-workspace"));
}
