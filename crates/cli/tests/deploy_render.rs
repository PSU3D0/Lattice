//! Tests for `flows deploy render` (packet W1 of
//! ops/phase1-clone-engine-plan-2026-06-12.md): FlowRequirements (+ optional
//! bindings.lock) → wrangler.toml.
//!
//! Coverage:
//! - golden files: `s1_echo` (http-only, near-empty bindings), a synthetic
//!   kv+blob manifest (no built-in example declares both today — s4 is
//!   kv-only, s6 is blob-only, s12 is blob-only; the synthetic fixture also
//!   exercises the `unspecified`-trigger warning), and a synthetic schedule
//!   manifest (packet T3: `[triggers].crons` union, byte-verbatim, deduped
//!   across colliding entrypoints — schedule-trigger.md §7c);
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
    fixture_root()
        .join(name)
        .to_str()
        .expect("fixture path")
        .to_string()
}

// ---------------------------------------------------------------------------
// bindings.lock content_hash helpers. The lock's `content_hash` is the sha256
// of the canonical JSON of the lock with `content_hash` removed (matches
// `compute_lock_content_hash` in main.rs). Tests build a lock, stamp the real
// hash, then write it to a temp dir — so the render path's hash verification
// (folded in with H3) accepts a well-formed lock and rejects a tampered one.
// ---------------------------------------------------------------------------

fn sha256_hex(payload: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn canonical_json_for_test(value: &serde_json::Value) -> String {
    use serde_json::Value;
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("json")
        }
        Value::Array(values) => {
            let mut out = String::from("[");
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json_for_test(item));
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
                out.push_str(&canonical_json_for_test(map.get(key).expect("present")));
            }
            out.push('}');
            out
        }
    }
}

/// Stamp the correct `content_hash` into a lock value (hash computed over the
/// lock WITHOUT the `content_hash` field).
fn stamp_content_hash(lock: &mut serde_json::Value) {
    let mut without_hash = lock.clone();
    if let Some(obj) = without_hash.as_object_mut() {
        obj.remove("content_hash");
    }
    let hash = sha256_hex(&canonical_json_for_test(&without_hash));
    lock["content_hash"] = serde_json::json!(hash);
}

/// Write a lock value (stamped with a valid `content_hash`) to a temp dir and
/// return its path (plus the owning tempdir, which must stay alive).
fn write_valid_lock(lock: &serde_json::Value) -> (tempfile::TempDir, PathBuf) {
    let mut lock = lock.clone();
    stamp_content_hash(&mut lock);
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("bindings.lock.json");
    fs::write(
        &path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write lock");
    (temp, path)
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
    let bindings = parsed["durable_objects"]["bindings"]
        .as_array()
        .expect("do bindings");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["name"].as_str(), Some("FLOW_DO"));
    assert_eq!(
        bindings[0]["class_name"].as_str(),
        Some("FlowDurableObject")
    );
    // Budget was not silently skipped.
    assert!(notes.contains("module size budget NOT checked"));
    assert!(rendered.contains("compressed module size was NOT checked"));
}

// ---------------------------------------------------------------------------
// Golden: synthetic kv+blob manifest (also exercises the unspecified-trigger
// warning: an alias wired to no entrypoint cannot fire).
// ---------------------------------------------------------------------------
#[test]
fn kv_blob_renders_golden_with_placeholders_and_unspecified_warning() {
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
    assert_eq!(
        r2[0]["bucket_name"].as_str(),
        Some("kv-blob-demo-flow-blob")
    );

    // A fresh user can follow the file: each placeholder carries the exact
    // creation command.
    assert!(rendered.contains("wrangler kv namespace create kv-blob-demo-flow-kv"));
    assert!(rendered.contains("wrangler r2 bucket create kv-blob-demo-flow-blob"));
    // Per-node attribution is visible next to each binding.
    assert!(rendered.contains("required by node(s): cache_read, cache_write"));
    assert!(rendered.contains("required by node(s): store"));

    // The unspecified `poller` trigger cannot fire: warned, not silently
    // dropped — and no [triggers] block is invented for it.
    assert!(
        notes.contains("trigger `poller` is wired to neither an HTTP nor a schedule entrypoint")
    );
    assert!(!rendered.contains("[triggers]"));
    assert!(!rendered.contains("STUB(T3)"));
}

// ---------------------------------------------------------------------------
// Golden: synthetic schedule manifest (packet T3) — `[triggers].crons` is the
// sorted, deduplicated, byte-verbatim union of entrypoints[].schedule; two
// entrypoints colliding on one cron (the defined fan-out) collapse to a
// single crons entry; schedule entrypoints are NOT listed as HTTP routes.
// ---------------------------------------------------------------------------
#[test]
fn schedule_renders_golden_with_crons_union() {
    let requirements = fixture_arg("schedule.requirements.json");
    let (rendered, notes) = render_ok(&["--requirements", &requirements]);
    assert_matches_golden(&rendered, "schedule.wrangler.toml.golden");

    let parsed = parse_toml(&rendered, "schedule render");
    let crons: Vec<&str> = parsed["triggers"]["crons"]
        .as_array()
        .expect("crons array")
        .iter()
        .map(|value| value.as_str().expect("cron string"))
        .collect();
    // Sorted + deduped ("*/5 * * * *" appears on two entrypoints), strings
    // byte-verbatim from the manifest.
    assert_eq!(crons, vec!["*/5 * * * *", "0 2 * * *"]);

    // Attribution comments map each cron to its entrypoints (fan-out visible).
    assert!(rendered.contains("(trigger `tick` -> capture `record`, deadline 30000ms)"));
    assert!(rendered.contains("(trigger `tick_shadow` -> capture `record_shadow`)"));
    assert!(rendered.contains("(trigger `nightly` -> capture `report`)"));

    // Schedule entrypoints are not HTTP routes: no fetch-handler listing.
    assert!(!rendered.contains("HTTP entrypoints"));

    // kv requirement still renders its binding alongside the triggers.
    let kv = parsed["kv_namespaces"].as_array().expect("kv namespaces");
    assert_eq!(kv[0]["binding"].as_str(), Some("FLOW_KV"));

    // 2 crons is under the account cap: no cap warning.
    assert!(!notes.contains("account-wide cap"));
}

// ---------------------------------------------------------------------------
// Cron cap: a union over the Cloudflare free-plan account-wide cap (5) warns
// in NOTES (deploy will reject it; the render still succeeds so the file can
// be inspected/edited).
// ---------------------------------------------------------------------------
#[test]
fn schedule_over_free_cron_cap_warns() {
    let base = fs::read_to_string(fixture_root().join("schedule.requirements.json"))
        .expect("read schedule fixture");
    let mut manifest: serde_json::Value = serde_json::from_str(&base).expect("parse fixture");
    // Six distinct crons > free cap of 5.
    let crons = [
        "*/1 * * * *",
        "*/2 * * * *",
        "*/3 * * * *",
        "*/4 * * * *",
        "*/6 * * * *",
        "*/7 * * * *",
    ];
    let entrypoints: Vec<serde_json::Value> = crons
        .iter()
        .enumerate()
        .map(|(index, cron)| {
            serde_json::json!({
                "trigger_alias": format!("tick_{index}"),
                "capture_alias": format!("record_{index}"),
                "schedule": cron,
            })
        })
        .collect();
    manifest["entrypoints"] = serde_json::Value::Array(entrypoints);
    manifest["triggers"] = serde_json::json!([]);

    let temp = tempfile::tempdir().expect("tempdir");
    let fixture = temp.path().join("over_cap.requirements.json");
    fs::write(&fixture, manifest.to_string()).expect("write fixture");

    let (rendered, notes) = render_ok(&["--requirements", fixture.to_str().expect("path")]);
    assert!(
        notes.contains("6 cron triggers, over the Cloudflare free plan account-wide cap of 5"),
        "missing cron cap warning: {notes}"
    );
    let parsed = parse_toml(&rendered, "over-cap render");
    assert_eq!(
        parsed["triggers"]["crons"].as_array().expect("crons").len(),
        6,
        "the union still renders; the deploy is where the cap is enforced"
    );
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

    let hand_written_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../host-workers/workerd-tests/wrangler.toml");
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
    assert_eq!(
        ours_do, theirs_do,
        "durable object binding shapes must match"
    );

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
    assert!(
        stderr.contains("over the free tier limit of 1 MiB"),
        "{stderr}"
    );
    assert!(stderr.contains("--paid"), "{stderr}");

    // Paid tier: renders, but the warning is in NOTES and in the file.
    let (rendered, notes) = render_ok(&[
        "--example",
        "s1_echo",
        "--wasm-artifact",
        artifact_arg,
        "--paid",
    ]);
    assert!(
        notes.contains("above the 3 MiB warning threshold"),
        "{notes}"
    );
    assert!(
        rendered.contains("WARNING:"),
        "budget warning must be in the file"
    );
    assert!(
        rendered.contains("paid tier"),
        "budget comment must name the tier"
    );
    parse_toml(&rendered, "paid render");
}

// ---------------------------------------------------------------------------
// bindings.lock supplies real instance ids.
// ---------------------------------------------------------------------------
#[test]
fn bindings_lock_instance_id_replaces_placeholder() {
    let (_lock_dir, lock_path) = write_valid_lock(&serde_json::json!({
        "version": 1,
        "generated_at": "1970-01-01T00:00:00Z",
        "content_hash": "",
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
    }));

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

// ---------------------------------------------------------------------------
// Typed-dependency schema floor: an older manifest must be regenerated rather
// than defaulting implementation dependencies empty.
// ---------------------------------------------------------------------------
#[test]
fn pre_implementation_dependency_schema_fails_workers_render_closed() {
    let mut requirements =
        dag_core::FlowRequirements::derive(&example_s1_echo::flow()).expect("derive requirements");
    requirements.schema_version = "0.1".to_string();
    let temp = tempfile::tempdir().expect("tempdir");
    let requirements_path = temp.path().join("schema-0.1.requirements.json");
    fs::write(
        &requirements_path,
        serde_json::to_vec_pretty(&requirements).expect("serialize requirements"),
    )
    .expect("write requirements");
    let out_dir = temp.path().join("deploy");
    let output = run_render(&[
        "--requirements",
        requirements_path.to_str().expect("requirements path"),
        "--out",
        out_dir.to_str().expect("out path"),
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("predates the typed implementation-dependency placement surface"),
        "{stderr}"
    );
    assert!(stderr.contains("regenerate requirements"), "{stderr}");
}

// ---------------------------------------------------------------------------
// P4a positive control: workspace reads alone remain Workers-mappable. The
// placement gate keys off the statically-derived implementation identifier,
// not the extractor's workspace-read effect floor.
// ---------------------------------------------------------------------------
#[test]
fn benign_workspace_read_only_flow_still_renders() {
    let mut flow = example_s1_echo::flow();
    let reader = flow
        .nodes
        .iter_mut()
        .find(|node| node.alias == "normalize")
        .expect("normalize node");
    reader.identifier = "std.workspace.read".to_string();
    reader.effects = dag_core::Effects::ReadOnly;
    reader.determinism = dag_core::Determinism::BestEffort;
    reader.effect_hints = vec![dag_core::EffectHint::WorkspaceRead.as_str().to_string()];

    let requirements = dag_core::FlowRequirements::derive(&flow).expect("derive requirements");
    assert!(requirements.implementation_dependencies.is_empty());
    let temp = tempfile::tempdir().expect("tempdir");
    let requirements_path = temp.path().join("workspace-read.requirements.json");
    fs::write(
        &requirements_path,
        serde_json::to_vec_pretty(&requirements).expect("serialize requirements"),
    )
    .expect("write requirements");

    let (rendered, notes) = render_ok(&[
        "--requirements",
        requirements_path.to_str().expect("requirements path"),
    ]);
    let parsed = parse_toml(&rendered, "benign workspace-read render");
    assert!(parsed.get("r2_buckets").is_some());
    assert!(!notes.contains("LATTICE_EXTRACT_PDF"));
}

// ---------------------------------------------------------------------------
// S21: its application-local extraction node declares a typed sandboxed
// transform contract. Until W4 explicitly configures that Workers backend,
// rendering must fail closed from metadata rather than handler identity.
// ---------------------------------------------------------------------------
#[test]
fn s21_pdf_extraction_fails_workers_render_from_typed_dependency() {
    let flow = example_s21_ai_cv_screening::validated_ir().flow().clone();
    let requirements = dag_core::FlowRequirements::derive(&flow).expect("derive S21 requirements");
    assert_eq!(requirements.implementation_dependencies.len(), 1);
    assert_eq!(
        requirements.implementation_dependencies[0].kind,
        dag_core::ImplementationDependencyKind::SandboxedTransform
    );
    assert_eq!(
        requirements.implementation_dependencies[0].key,
        example_s21_ai_cv_screening::pdf_extraction::PDF_EXTRACT_TRANSFORM_ID
    );
    assert_eq!(
        requirements.implementation_dependencies[0].nodes,
        vec!["extract_cv_text".to_string()]
    );
    let temp = tempfile::tempdir().expect("tempdir");
    let requirements_path = temp.path().join("s21-pdf.requirements.json");
    fs::write(
        &requirements_path,
        serde_json::to_vec_pretty(&requirements).expect("serialize requirements"),
    )
    .expect("write requirements");
    let out_dir = temp.path().join("deploy");
    let output = run_render(&[
        "--requirements",
        requirements_path.to_str().expect("requirements path"),
        "--out",
        out_dir.to_str().expect("out path"),
    ]);

    assert!(
        !output.status.success(),
        "S21 PDF extraction must fail Workers render"
    );
    assert!(!out_dir.join("wrangler.toml").exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("lattice.pdf.extract_text.v1"), "{stderr}");
    assert!(stderr.contains("extract_cv_text"), "{stderr}");
    assert!(stderr.contains("no configured Workers backend"), "{stderr}");
    assert!(!stderr.contains("LATTICE_EXTRACT_PDF"), "{stderr}");
    assert!(!stderr.contains("S21 PDF"), "{stderr}");
}

// ---------------------------------------------------------------------------
// S27/H5d: its byte operations are native-only until H5c-ops-wasm exists.
// Static generic requirements derivation still identifies the exact ops and
// nodes, so Workers render must fail closed before writing a config.
// ---------------------------------------------------------------------------
#[test]
fn s27_binary_fails_workers_render_with_native_byte_operations() {
    let temp = tempfile::tempdir().expect("tempdir");
    let out_dir = temp.path().join("deploy");
    let output = run_render(&[
        "--example",
        "s27_binary",
        "--out",
        out_dir.to_str().expect("out path"),
    ]);

    assert!(
        !output.status.success(),
        "S27 Workers render must fail closed"
    );
    assert!(!out_dir.join("wrangler.toml").exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("H5c-ops-wasm is absent"), "{stderr}");
    assert!(
        stderr.contains("`connector.http.get_binary` — required by node(s): download"),
        "{stderr}"
    );
    assert!(
        stderr.contains(
            "`connector.http.post_multipart` — required by node(s): reupload, upload_report"
        ),
        "{stderr}"
    );
    assert!(stderr.contains("native host"), "{stderr}");
    assert!(stderr.contains("flows run local"), "{stderr}");
}

// ---------------------------------------------------------------------------
// connector.http origin-audit (packet H3, spec §8): a connector.http flow with
// one Tier-0 authed connection (GET + POST) and one Tier-2 unauthenticated
// any-origin GET. `resource::http` is Ambient on Workers (no binding emitted),
// but the renderer must emit one origin-audit NOTE per bound connector.http
// node — naming the endpoint origin, or ANY-ORIGIN for the Tier-2 grant — plus
// a Tier-2 warning line naming every any-origin node. The bindings.lock encodes
// the verified flat `nodes: {alias -> connection}` override map (F5).
// ---------------------------------------------------------------------------
#[test]
fn http_connector_renders_origin_audit_and_tier2_warning() {
    let requirements = fixture_arg("http_connector.requirements.json");
    let lock_fixture: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fixture_root().join("http_connector.bindings.lock.json"))
            .expect("read http lock fixture"),
    )
    .expect("parse http lock fixture");
    let (_lock_dir, lock_path) = write_valid_lock(&lock_fixture);

    let (rendered, notes) = render_ok(&[
        "--requirements",
        &requirements,
        "--bindings-lock",
        lock_path.to_str().expect("lock path"),
    ]);
    assert_matches_golden(&rendered, "http_connector.wrangler.toml.golden");

    // `resource::http` is Ambient: no kv/d1/r2/DO binding emitted for it.
    let parsed = parse_toml(&rendered, "http_connector render");
    assert!(parsed.get("kv_namespaces").is_none());
    assert!(parsed.get("d1_databases").is_none());
    assert!(parsed.get("r2_buckets").is_none());
    assert!(parsed.get("durable_objects").is_none());

    // Origin-audit NOTES: Tier-0 nodes name the fixed origin; the Tier-2 node
    // renders ANY-ORIGIN. Node aliases are emitted in sorted order.
    assert!(
        rendered.contains(
            "connector.http origin-audit: node `fetch_crm` (connection `crm_api`) -> origin https://api.crm.example"
        ),
        "missing fetch_crm origin audit:\n{rendered}"
    );
    assert!(
        rendered.contains(
            "connector.http origin-audit: node `push_stats` (connection `crm_api`) -> origin https://api.crm.example"
        ),
        "missing push_stats origin audit:\n{rendered}"
    );
    assert!(
        rendered.contains(
            "connector.http origin-audit: node `fetch_public` (connection `anywhere`) -> ANY-ORIGIN (Tier 2, unauthenticated)"
        ),
        "missing fetch_public any-origin audit:\n{rendered}"
    );

    // Tier-2 warning names exactly the any-origin node.
    assert!(
        rendered
            .contains("connector.http Tier-2 (any-origin) grant present — node(s) fetch_public"),
        "missing Tier-2 warning:\n{rendered}"
    );
    // The same notes reach stderr (the operator-facing channel).
    assert!(
        notes.contains("origin-audit: node `fetch_public`"),
        "{notes}"
    );
    assert!(
        notes.contains("Tier-2 (any-origin) grant present"),
        "{notes}"
    );
}

// ---------------------------------------------------------------------------
// content_hash on render (packet H3 follow-up): the run path verifies the
// lock's content_hash; render now does too, so a tampered lock fails render the
// same way it fails run.
// ---------------------------------------------------------------------------
#[test]
fn render_rejects_tampered_bindings_lock_content_hash() {
    let requirements = fixture_arg("http_connector.requirements.json");
    let mut lock: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fixture_root().join("http_connector.bindings.lock.json"))
            .expect("read http lock fixture"),
    )
    .expect("parse http lock fixture");

    // Stamp the correct hash, THEN tamper an origin: the recorded hash no
    // longer matches the (now-rewritten) content.
    stamp_content_hash(&mut lock);
    lock["connector_handles"]["endpoint.crm_api"]["config"]["base_url"] =
        serde_json::json!("https://evil.example");

    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("bindings.lock.json");
    fs::write(
        &lock_path,
        serde_json::to_vec_pretty(&lock).expect("serialize lock"),
    )
    .expect("write tampered lock");

    let out = temp.path().join("deploy");
    let output = run_render(&[
        "--requirements",
        &requirements,
        "--bindings-lock",
        lock_path.to_str().expect("lock path"),
        "--out",
        out.to_str().expect("out"),
    ]);
    assert!(
        !output.status.success(),
        "a tampered lock must fail render, not render silently"
    );
    assert!(
        !out.join("wrangler.toml").exists(),
        "no wrangler.toml may be written when the lock is rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("content_hash mismatch"),
        "render must reject the tampered lock with a content_hash mismatch: {stderr}"
    );
}
