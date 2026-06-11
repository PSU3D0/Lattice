//! `flows bundle requirements` — emit the static FlowRequirements manifest
//! (packet C1's `FlowRequirements`) as JSON for any example or built bundle.
//!
//! This is the seed artifact for the future infra-from-code planner
//! (`manifest → {cf-workers-free | cf-paid | native}`): every flow requirement
//! is answerable from the bundle without executing anything (the static
//! derivability rule of `impl-docs/spec/flow-requirements.md`).
//!
//! Two source modes, emitting two precise forms of the same shape:
//!
//! - `--example <name>` derives directly from the example's validated Flow IR
//!   via `kernel_plan::derive_requirements`. This is the **bare-IR form**: no
//!   `flow_ir_hash` and no entrypoint `deadline_ms`, because those two values
//!   are known only to the bundle assembler. This form is byte-equivalent to
//!   the C1 golden fixtures under
//!   `crates/kernel-plan/tests/fixtures/*.requirements.json` (the spec calls
//!   them "the IR-derived golden ... identical minus the two bundle-assembly
//!   enrichments"). The spec's derivation table names this command explicitly:
//!   "When derived directly from IR (e.g. future `flows bundle requirements`
//!   on a bare IR), `deadline_ms` is `null`."
//!
//! - `--bundle <path>` reads an already-built bundle directory and prints the
//!   **enriched form** that `exporters::bundle` carried into each
//!   `FlowEntry.requirements` (with `flow_ir_hash` and entrypoint
//!   `deadline_ms`). This mode never re-derives; it prints the manifest the
//!   bundle already committed to, so drift between a stale manifest and the
//!   serialized IR is observable rather than papered over.
//!
//! `--schema` prints the generated `flow_requirements.schema.json` (byte-for-
//! byte identical to the checked-in `schemas/flow_requirements.schema.json`).
//!
//! Output is deterministic: `FlowRequirements` sorts every vector by canonical
//! string and keys `per_node` by a `BTreeMap`, and serde emits struct fields in
//! declaration order, so `serde_json::to_string_pretty` is stable across runs.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use dag_core::FlowRequirements;

use crate::{load_example, load_manifest_from_dir};

#[derive(clap::Args, Debug)]
pub struct RequirementsArgs {
    /// Built-in example to derive requirements for (e.g. `s1_echo`).
    ///
    /// Emits the bare-IR manifest: no `flow_ir_hash`, entrypoint
    /// `deadline_ms` is `null` (those are bundle-assembly enrichments). This
    /// is identical to the C1 golden fixtures.
    #[arg(long, conflicts_with_all = ["bundle", "schema"])]
    pub example: Option<String>,
    /// Path to an already-built FlowBundle directory containing manifest.json.
    ///
    /// Prints the enriched requirements the bundle assembler carried into
    /// each flow entry (`flow_ir_hash` + entrypoint `deadline_ms`).
    #[arg(long, conflicts_with_all = ["example", "schema"])]
    pub bundle: Option<PathBuf>,
    /// Flow id to select when a bundle carries multiple flows.
    #[arg(long, requires = "bundle")]
    pub flow: Option<String>,
    /// Print the flow_requirements JSON schema instead of a manifest.
    #[arg(long, conflicts_with_all = ["example", "bundle", "flow", "out"])]
    pub schema: bool,
    /// Write JSON to this path instead of stdout.
    #[arg(long)]
    pub out: Option<PathBuf>,
}

pub fn run_requirements(args: RequirementsArgs) -> Result<()> {
    let json = if args.schema {
        schema_json()?
    } else {
        let requirements = resolve_requirements(&args)?;
        let mut text = serde_json::to_string_pretty(&requirements)
            .context("failed to serialize flow requirements")?;
        text.push('\n');
        text
    };

    match args.out.as_deref() {
        Some(path) => {
            fs::write(path, json.as_bytes())
                .with_context(|| format!("failed to write {}", path.display()))?;
            println!("{}", path.display());
        }
        None => {
            print!("{json}");
        }
    }

    Ok(())
}

/// Emit the canonical `flow_requirements.schema.json`. Byte-for-byte identical
/// to the checked-in schema file (both come from `to_string_pretty` of
/// `schema_json_for_file` plus a trailing newline), so a planner can validate
/// against either source.
fn schema_json() -> Result<String> {
    let schema = dag_core::schema::schema_json_for_file("flow_requirements.schema.json")
        .ok_or_else(|| anyhow!("flow_requirements schema is not registered with the emitter"))?;
    let mut text =
        serde_json::to_string_pretty(&schema).context("failed to serialize requirements schema")?;
    text.push('\n');
    Ok(text)
}

fn resolve_requirements(args: &RequirementsArgs) -> Result<FlowRequirements> {
    match (args.example.as_deref(), args.bundle.as_deref()) {
        (Some(example), None) => requirements_from_example(example),
        (None, Some(bundle_dir)) => requirements_from_bundle(bundle_dir, args.flow.as_deref()),
        _ => Err(anyhow!(
            "exactly one of --example, --bundle, or --schema must be provided"
        )),
    }
}

/// Derive the bare-IR requirements manifest for a built-in example. The form
/// matches the C1 golden fixtures: `kernel_plan::derive_requirements` is the
/// pure derivation from validated IR, carrying no bundle-assembly enrichments.
fn requirements_from_example(example: &str) -> Result<FlowRequirements> {
    let handle = load_example(example)?;
    Ok(kernel_plan::derive_requirements(&handle.ir))
}

/// Read the enriched requirements an already-built bundle carried for the
/// selected flow. Nothing is re-derived: this prints the manifest the bundle
/// assembler committed to (so a stale manifest is observable, not hidden).
fn requirements_from_bundle(bundle_dir: &Path, selected_flow: Option<&str>) -> Result<FlowRequirements> {
    let manifest = load_manifest_from_dir(bundle_dir)?;

    let mut matches = manifest.flows.iter().filter(|entry| {
        selected_flow
            .map(|selected| entry.id == selected)
            .unwrap_or(true)
    });

    let entry = match matches.next() {
        Some(entry) => entry,
        None => {
            return match selected_flow {
                Some(selected) => Err(anyhow!(
                    "bundle does not define flow `{selected}`"
                )),
                None => Err(anyhow!("bundle defines no flows")),
            };
        }
    };

    if selected_flow.is_none() && matches.next().is_some() {
        let ids: Vec<&str> = manifest.flows.iter().map(|entry| entry.id.as_str()).collect();
        return Err(anyhow!(
            "bundle carries multiple flows ({}); pass --flow <id> to select one",
            ids.join(", ")
        ));
    }

    entry.requirements.clone().ok_or_else(|| {
        anyhow!(
            "bundle flow `{}` carries no requirements manifest; rebuild with a toolchain that emits FlowRequirements (packet C1)",
            entry.id
        )
    })
}
