//! `flows deploy render` — FlowRequirements (+ optional bindings.lock) →
//! wrangler.toml. Infra-from-code v0: the seed of the planner (workstream W of
//! ops/phase1-clone-engine-plan-2026-06-12.md, packet W1).
//!
//! Ground truth: the hand-written wrangler.toml files under
//! `crates/host-workers/`, `crates/host-workers/workerd-tests/`,
//! `crates/cap-{http,kv,do}-workers/workerd-tests/`,
//! `crates/cap-sql-workers-d1/workerd-tests/`, and
//! `examples/s7_cloudflare_idem/`. The renderer must be able to reproduce the
//! binding *shapes* of those configs (modulo instance names/ids) — that is the
//! acceptance bar, pinned by `tests/deploy_render.rs`'s reproduction test.
//!
//! Design decisions:
//!
//! - The EffectHint-family → Workers-binding mapping is a **data table**
//!   ([`FAMILY_MAPPINGS`]), not an if-chain: it will grow, and its
//!   exhaustiveness over `EffectHint::ALL` families is asserted by a test.
//! - **Fail closed**: any requirement family with no Workers mapping aborts
//!   the render with an error listing exactly which nodes need what, why it
//!   cannot run on Workers, and what to do instead. That error text is
//!   planner v0.
//! - **Deterministic output**: sections and bindings are emitted in a fixed
//!   order, so the goldens are stable and diffs are meaningful.
//! - **Honest placeholders**: when no bindings.lock supplies a real instance
//!   id, the rendered file carries a `REPLACE_WITH_*` placeholder plus a
//!   comment with the exact `wrangler ... create` command — a fresh user must
//!   be able to follow the file to a working deploy (the s7 example config is
//!   the idiom source).
//! - **Size budget**: if a wasm artifact is provided (`--wasm-artifact`) or
//!   derivable (`--bundle` code descriptor on disk), its gzip-compressed size
//!   is checked against the Cloudflare per-script budget (free: warn >900KiB,
//!   error >1MiB; `--paid`: warn >3MiB, error >10MiB). With no artifact the
//!   budget is emitted as comments + a NOTES warning — never silently
//!   skipped.
//! - **Schedule/cron triggers** (packet T3): schedule entrypoints render a
//!   `[triggers]` / `crons = [...]` block — the sorted, deduplicated union of
//!   `entrypoints[].schedule`, byte-verbatim (the host-workers `scheduled()`
//!   handler routes fires by byte equality; see
//!   `impl-docs/spec/schedule-trigger.md` §7). The union is checked against
//!   the Cloudflare account-wide cron cap (warn, not abort). Triggers with
//!   `kind = unspecified` still render a NOTES warning: the worker cannot
//!   fire them.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use dag_core::requirements::TriggerKind;
use dag_core::{EffectHint, FlowRequirements};

use crate::BindingsLock;

/// Compatibility date pinned to the value every hand-written workerd-tests
/// config in this repo uses. Bump deliberately, with a workerd proof.
const COMPATIBILITY_DATE: &str = "2024-09-23";

/// Worker entry produced by `worker-build` (see the workerd-tests configs and
/// scripts/ensure-worker-build.sh).
const WORKER_MAIN: &str = "build/worker/shim.mjs";

/// Build command shared by every hand-written config in the repo.
const BUILD_COMMAND: &str =
    "bash \\\"$(git rev-parse --show-toplevel)/scripts/ensure-worker-build.sh\\\" --release";

/// Cloudflare free-tier per-script compressed size budget.
const FREE_WARN_BYTES: u64 = 900 * 1024;
const FREE_MAX_BYTES: u64 = 1024 * 1024;
/// Paid-tier budget (`--paid`).
const PAID_WARN_BYTES: u64 = 3 * 1024 * 1024;
const PAID_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// Cloudflare Cron Trigger caps, verified 2026-07-11 against
/// <https://developers.cloudflare.com/workers/platform/limits/>: 5 Cron
/// Triggers per **account** on the free plan, 250 per account on paid. The
/// cap is account-wide (not per worker), so a single rendered worker whose
/// crons union exceeds it definitively cannot deploy on that plan; smaller
/// unions can still collide with other workers on the account, which only
/// the deploy can detect. Exceeding warns (NOTES + file comment), it does
/// not abort the render.
const FREE_MAX_CRONS: usize = 5;
const PAID_MAX_CRONS: usize = 250;

/// Native-only connector.http byte operations. These require the deferred
/// H5c-ops-wasm host composites and cannot execute in a Workers guest yet.
const NATIVE_ONLY_HTTP_BYTE_OPERATIONS: &[&str] = &[
    "connector.http.get_binary",
    "connector.http.post_multipart",
    "connector.http.put_multipart",
];

#[derive(clap::Subcommand, Debug)]
pub enum DeployCommand {
    /// Render a wrangler.toml from a flow's static requirements manifest.
    Render(RenderArgs),
}

#[derive(clap::Args, Debug)]
pub struct RenderArgs {
    /// Built-in example to render a deploy config for (e.g. `s1_echo`).
    #[arg(long, conflicts_with_all = ["bundle", "requirements"])]
    pub example: Option<String>,
    /// Path to an already-built FlowBundle directory containing manifest.json.
    #[arg(long, conflicts_with_all = ["example", "requirements"])]
    pub bundle: Option<PathBuf>,
    /// Path to a bare FlowRequirements JSON manifest (as emitted by
    /// `flows bundle requirements`).
    #[arg(long, conflicts_with_all = ["example", "bundle"])]
    pub requirements: Option<PathBuf>,
    /// Flow id to select when a bundle carries multiple flows.
    #[arg(long, requires = "bundle")]
    pub flow: Option<String>,
    /// Output directory for the rendered wrangler.toml.
    #[arg(long)]
    pub out: PathBuf,
    /// Worker name (default: the flow name, sanitized for Cloudflare).
    #[arg(long)]
    pub name: Option<String>,
    /// bindings.lock.json supplying instance names/ids for rendered bindings
    /// and lock-resolved connector effect hints.
    #[arg(long)]
    pub bindings_lock: Option<PathBuf>,
    /// wasm module to size-check against the per-script budget (compressed).
    #[arg(long)]
    pub wasm_artifact: Option<PathBuf>,
    /// Use the paid-tier size budget (warn >3MiB, error >10MiB compressed)
    /// instead of the free tier (warn >900KiB, error >1MiB).
    #[arg(long)]
    pub paid: bool,
}

pub fn run_deploy(command: DeployCommand) -> Result<()> {
    match command {
        DeployCommand::Render(args) => run_render(args),
    }
}

fn run_render(args: RenderArgs) -> Result<()> {
    let requirements = resolve_requirements(&args)?;
    let lock = args
        .bindings_lock
        .as_deref()
        .map(load_bindings_lock)
        .transpose()?;

    let artifact = resolve_artifact(&args)?;
    let options = RenderOptions {
        worker_name: args
            .name
            .clone()
            .unwrap_or_else(|| sanitize_worker_name(&requirements.flow.name)),
        paid: args.paid,
        artifact,
        lock,
    };

    let rendered = render_wrangler(&requirements, &options)?;

    fs::create_dir_all(&args.out)
        .with_context(|| format!("failed to create {}", args.out.display()))?;
    let toml_path = args.out.join("wrangler.toml");
    fs::write(&toml_path, rendered.wrangler_toml.as_bytes())
        .with_context(|| format!("failed to write {}", toml_path.display()))?;

    for note in &rendered.notes {
        eprintln!("NOTE: {note}");
    }
    println!("{}", toml_path.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Requirements resolution (same source-selection idiom as requirements.rs)
// ---------------------------------------------------------------------------

fn resolve_requirements(args: &RenderArgs) -> Result<FlowRequirements> {
    match (
        args.example.as_deref(),
        args.bundle.as_deref(),
        args.requirements.as_deref(),
    ) {
        (Some(example), None, None) => crate::requirements::requirements_from_example(example),
        (None, Some(bundle_dir), None) => {
            crate::requirements::requirements_from_bundle(bundle_dir, args.flow.as_deref())
        }
        (None, None, Some(path)) => requirements_from_file(path),
        _ => Err(anyhow!(
            "exactly one of --example, --bundle, or --requirements must be provided"
        )),
    }
}

fn requirements_from_file(path: &Path) -> Result<FlowRequirements> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let requirements: FlowRequirements = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "{} is not a valid FlowRequirements manifest",
            path.display()
        )
    })?;
    // Versioning policy (impl-docs/spec/flow-requirements.md): consumers MUST
    // reject unknown major shapes.
    if !requirements.schema_version.starts_with("0.") {
        bail!(
            "unsupported FlowRequirements schema_version `{}` in {} (this toolchain understands 0.x)",
            requirements.schema_version,
            path.display()
        );
    }
    Ok(requirements)
}

fn load_bindings_lock(path: &Path) -> Result<BindingsLock> {
    // Verify `content_hash` (+ version + generated_at) exactly as the run path
    // does — `flows deploy render` previously only deserialized the lock, so a
    // tampered lock rendered without complaint even though `flows run` rejects
    // it (noted across Phase 1: s16/s20/s25). Fold the check into render so a
    // tampered lock fails render the same way it fails run. This matters more
    // now that origin grants (§8) are security-relevant.
    crate::load_bindings_lock(path)
}

/// Locate the wasm artifact to size-check: explicit `--wasm-artifact` wins;
/// a `--bundle` source falls back to the code descriptor on disk.
fn resolve_artifact(args: &RenderArgs) -> Result<Option<ArtifactInfo>> {
    if let Some(path) = args.wasm_artifact.as_deref() {
        let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        return Ok(Some(ArtifactInfo {
            source: path.display().to_string(),
            compressed_bytes: gzip_len(&bytes)?,
            raw_bytes: bytes.len() as u64,
        }));
    }
    if let Some(bundle_dir) = args.bundle.as_deref() {
        let manifest = crate::load_manifest_from_dir(bundle_dir)?;
        let module_path = bundle_dir.join(&manifest.code.file);
        if module_path.is_file() {
            let bytes = fs::read(&module_path)
                .with_context(|| format!("failed to read {}", module_path.display()))?;
            return Ok(Some(ArtifactInfo {
                source: module_path.display().to_string(),
                compressed_bytes: gzip_len(&bytes)?,
                raw_bytes: bytes.len() as u64,
            }));
        }
    }
    Ok(None)
}

fn gzip_len(bytes: &[u8]) -> Result<u64> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(bytes)
        .context("failed to gzip wasm artifact")?;
    let compressed = encoder.finish().context("failed to finish gzip")?;
    Ok(compressed.len() as u64)
}

// ---------------------------------------------------------------------------
// The mapping table: EffectHint families → Workers bindings
// ---------------------------------------------------------------------------

/// What a capability family maps to on Cloudflare Workers.
///
/// This is DATA, not control flow: rows are looked up from
/// [`FAMILY_MAPPINGS`], and a test asserts every `EffectHint::ALL` family has
/// exactly one row, so adding a hint family without deciding its Workers
/// story is a compile-adjacent failure, not a silent gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkersMapping {
    /// `[[kv_namespaces]]` (ground truth: cap-kv-workers/workerd-tests,
    /// examples/s7_cloudflare_idem).
    KvNamespace,
    /// `[[d1_databases]]` (ground truth: cap-sql-workers-d1/workerd-tests).
    D1Database,
    /// `[[r2_buckets]]` (planned blob mapping; see NOTES emitted at render).
    R2Bucket,
    /// cap-workspace-workers shape: R2 bucket + Durable Object index
    /// (ground truth: host-workers/workerd-tests — `WORKSPACE_BUCKET` +
    /// `WORKSPACE_DO`/`WorkspaceDurableObject` + sqlite migration).
    Workspace,
    /// Durable-Object-backed dedupe store (ground truth:
    /// examples/s7_cloudflare_idem — `DEDUP_DO`/`FlowDurableObject`).
    DedupeDurableObject,
    /// Nothing to render: the platform provides this ambiently.
    Ambient(&'static str),
    /// No Workers mapping exists. Rendering fails closed with this reason
    /// and remediation; the error text is planner v0.
    Unsupported {
        reason: &'static str,
        fix: &'static str,
    },
}

/// The family → Workers mapping table, in `EffectHint::ALL` family order.
pub(crate) const FAMILY_MAPPINGS: &[(EffectHint, WorkersMapping)] = &[
    (
        EffectHint::Http,
        WorkersMapping::Ambient("outbound fetch is ambient on Workers; no binding required"),
    ),
    (
        EffectHint::Clock,
        WorkersMapping::Ambient(
            "wall clock is ambient on Workers (Date.now advances only across I/O)",
        ),
    ),
    (
        EffectHint::Rng,
        WorkersMapping::Unsupported {
            reason: "no Workers capability provider exists for `resource::rng` (crypto.getRandomValues \
                     is available on the platform, but Lattice has no cap-rng-workers crate wiring it \
                     into the resource bag)",
            fix: "run this flow on a native host (host-inproc / host-web-axum), or build a \
                  cap-rng-workers provider and add its mapping row",
        },
    ),
    (
        EffectHint::Db,
        WorkersMapping::Unsupported {
            reason: "the legacy `resource::db` relational family has no Workers capability provider",
            fix: "migrate the node to `resource::sql` (D1 via cap-sql-workers-d1), or run this flow \
                  on a native host",
        },
    ),
    (EffectHint::Sql, WorkersMapping::D1Database),
    (EffectHint::Kv, WorkersMapping::KvNamespace),
    (EffectHint::Blob, WorkersMapping::R2Bucket),
    (
        EffectHint::Queue,
        WorkersMapping::Unsupported {
            reason: "`resource::queue` is provided by bridge-queue-redis (native only); Cloudflare \
                     Queues exist but Lattice has no Workers queue provider crate yet",
            fix: "run this flow on a native host with Redis, or build a cap-queue-workers provider \
                  (Cloudflare Queues) and add its mapping row",
        },
    ),
    (EffectHint::Dedupe, WorkersMapping::DedupeDurableObject),
    (EffectHint::Workspace, WorkersMapping::Workspace),
];

pub(crate) fn mapping_for_family(family: EffectHint) -> Option<WorkersMapping> {
    FAMILY_MAPPINGS
        .iter()
        .find(|(entry, _)| *entry == family)
        .map(|(_, mapping)| *mapping)
}

// ---------------------------------------------------------------------------
// Render plan
// ---------------------------------------------------------------------------

pub(crate) struct RenderOptions {
    pub(crate) worker_name: String,
    pub(crate) paid: bool,
    pub(crate) artifact: Option<ArtifactInfo>,
    pub(crate) lock: Option<BindingsLock>,
}

pub(crate) struct ArtifactInfo {
    pub(crate) source: String,
    pub(crate) compressed_bytes: u64,
    pub(crate) raw_bytes: u64,
}

pub(crate) struct Rendered {
    pub(crate) wrangler_toml: String,
    pub(crate) notes: Vec<String>,
}

struct KvBinding {
    comment: Vec<String>,
    binding: String,
    id: String,
}

struct D1Binding {
    comment: Vec<String>,
    binding: String,
    database_name: String,
    database_id: String,
}

struct R2Binding {
    comment: Vec<String>,
    binding: String,
    bucket_name: String,
}

struct DoBinding {
    comment: Vec<String>,
    name: String,
    class_name: String,
}

struct UnmappableFamily {
    family: EffectHint,
    reason: &'static str,
    fix: &'static str,
    nodes: Vec<String>,
}

/// Sanitize a flow name into a Cloudflare worker name: lowercase
/// alphanumerics and hyphens.
pub(crate) fn sanitize_worker_name(flow_name: &str) -> String {
    let mut out = String::with_capacity(flow_name.len());
    let mut last_hyphen = true; // trim leading hyphens
    for ch in flow_name.chars() {
        let mapped = match ch {
            'a'..='z' | '0'..='9' => Some(ch),
            'A'..='Z' => Some(ch.to_ascii_lowercase()),
            '_' | '-' | ' ' | '.' => None,
            _ => None,
        };
        match mapped {
            Some(ch) => {
                out.push(ch);
                last_hyphen = false;
            }
            None => {
                if !last_hyphen {
                    out.push('-');
                    last_hyphen = true;
                }
            }
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "lattice-flow".to_string()
    } else {
        out
    }
}

/// Node aliases (from the per-node attribution) that declare a hint of the
/// given family. Lock-resolved hints contribute with a `(lock)` marker.
fn nodes_requiring_family(
    requirements: &FlowRequirements,
    lock_hints: &BTreeMap<String, BTreeSet<EffectHint>>,
    family: EffectHint,
) -> Vec<String> {
    let mut nodes = BTreeSet::new();
    for (alias, hints) in &requirements.effects.per_node {
        if hints.iter().any(|hint| hint.family() == family) {
            nodes.insert(alias.clone());
        }
    }
    for (alias, hints) in lock_hints {
        if hints.iter().any(|hint| hint.family() == family) {
            nodes.insert(format!("{alias} (lock-resolved)"));
        }
    }
    nodes.into_iter().collect()
}

/// Connection-dependent effect hints recorded in the lock for this flow
/// (`connector_bindings.<flow>.resolved_effect_hints`, packet C2). Keyed by
/// flow id with a flow-name fallback (locks are keyed by whatever the
/// `--connectors-from` sections used).
fn lock_resolved_hints(
    requirements: &FlowRequirements,
    lock: Option<&BindingsLock>,
) -> Result<BTreeMap<String, BTreeSet<EffectHint>>> {
    let mut resolved: BTreeMap<String, BTreeSet<EffectHint>> = BTreeMap::new();
    let Some(lock) = lock else {
        return Ok(resolved);
    };
    let flow_id = requirements.flow.id.as_str();
    let bindings = lock
        .connector_bindings
        .get(flow_id)
        .or_else(|| lock.connector_bindings.get(&requirements.flow.name));
    let Some(bindings) = bindings else {
        return Ok(resolved);
    };
    for (alias, hints) in &bindings.resolved_effect_hints {
        let mut parsed_set = BTreeSet::new();
        for hint in hints {
            let parsed = EffectHint::parse(hint).map_err(|err| {
                anyhow!(
                    "bindings.lock records invalid resolved effect hint `{hint}` for node \
                     `{alias}`: {err} (EFFECT202; see impl-docs/error-codes.md)"
                )
            })?;
            parsed_set.insert(parsed);
        }
        resolved.insert(alias.clone(), parsed_set);
    }
    Ok(resolved)
}

/// connector.http origin-audit (spec §8): the planner-v0 reachable-origin
/// line. For a flow bound to `connector.http` connections, emit one NOTES line
/// per bound node stating which endpoint origin it can reach (from the lock's
/// `endpoint.profile` handle `config.base_url`), or `ANY-ORIGIN (Tier 2,
/// unauthenticated)` for a Tier-2 `endpoint.any_origin` grant. The per-node
/// connection is resolved through the verified flat `nodes: {alias ->
/// connection}` override map, falling back to the `connector.http` default
/// (main.rs `resolve_connection_name`). Output is deterministic (node aliases
/// sorted). Returns `(audit_lines, tier2_node_aliases)`.
fn connector_http_origin_audit(
    requirements: &FlowRequirements,
    lock: Option<&BindingsLock>,
) -> (Vec<String>, Vec<String>) {
    let mut audit: Vec<String> = Vec::new();
    let mut tier2_nodes: Vec<String> = Vec::new();
    let Some(lock) = lock else {
        return (audit, tier2_nodes);
    };
    let flow_id = requirements.flow.id.as_str();
    let Some(bindings) = lock
        .connector_bindings
        .get(flow_id)
        .or_else(|| lock.connector_bindings.get(&requirements.flow.name))
    else {
        return (audit, tier2_nodes);
    };

    // Every bound node alias: per-node overrides plus the recorded
    // resolved-hint aliases (the full set of bound-connection nodes, packet
    // C2). BTreeSet keys keep the emission order deterministic.
    let mut aliases: BTreeSet<&String> = BTreeSet::new();
    aliases.extend(bindings.nodes.keys());
    aliases.extend(bindings.resolved_effect_hints.keys());

    for alias in aliases {
        // Per-node override wins, else the connector.http default.
        let Some(connection_name) = bindings
            .nodes
            .get(alias)
            .or_else(|| bindings.defaults.get("connector.http"))
        else {
            continue;
        };
        let Some(connection) = lock.connector_connections.get(connection_name) else {
            continue;
        };
        if connection.connector_id != "connector.http" {
            continue;
        }

        // Classify the endpoint-profile role's handle: a Tier-2
        // `endpoint.any_origin` grant, or a Tier-0/1 fixed origin.
        let mut any_origin = false;
        let mut origin: Option<String> = None;
        for (role_key, handle_name) in &connection.roles {
            if !role_key.starts_with("endpoint_profile.") {
                continue;
            }
            let Some(handle) = lock.connector_handles.get(handle_name) else {
                continue;
            };
            if handle.handle_kind == "endpoint.any_origin" {
                any_origin = true;
            } else if handle.handle_kind == "endpoint.profile" {
                origin = handle
                    .config
                    .get("base_url")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
            }
        }

        if any_origin {
            audit.push(format!(
                "connector.http origin-audit: node `{alias}` (connection `{connection_name}`) \
                 -> ANY-ORIGIN (Tier 2, unauthenticated)"
            ));
            tier2_nodes.push(alias.clone());
        } else {
            let origin = origin.unwrap_or_else(|| "<unresolved origin>".to_string());
            audit.push(format!(
                "connector.http origin-audit: node `{alias}` (connection `{connection_name}`) \
                 -> origin {origin}"
            ));
        }
    }

    (audit, tier2_nodes)
}

/// Look up a string the lock may carry for an instance providing hints of
/// `family`: searches `instances[*].connect`/`config` for the first of `keys`
/// on an instance whose `provides` covers the family.
fn lock_instance_value(
    lock: Option<&BindingsLock>,
    family: EffectHint,
    keys: &[&str],
) -> Option<(String, String, String)> {
    let lock = lock?;
    for (instance_name, instance) in &lock.instances {
        let provides_family = instance.provides.iter().any(|hint| {
            EffectHint::parse(hint)
                .map(|parsed| parsed.family() == family)
                .unwrap_or(false)
        });
        if !provides_family {
            continue;
        }
        for source in [&instance.connect, &instance.config] {
            if let Some(map) = source.as_object() {
                for key in keys {
                    if let Some(value) = map.get(*key).and_then(|value| value.as_str()) {
                        return Some((
                            instance_name.clone(),
                            instance.provider_kind.clone(),
                            value.to_string(),
                        ));
                    }
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// The renderer
// ---------------------------------------------------------------------------

pub(crate) fn render_wrangler(
    requirements: &FlowRequirements,
    options: &RenderOptions,
) -> Result<Rendered> {
    reject_native_only_http_byte_operations(requirements)?;

    let worker = &options.worker_name;
    let mut notes: Vec<String> = Vec::new();

    // Effective families = manifest families ∪ families of lock-resolved
    // connector hints (the lock-time half of the requirements story).
    let lock_hints = lock_resolved_hints(requirements, options.lock.as_ref())?;
    let mut families: BTreeSet<EffectHint> =
        requirements.effects.families.iter().copied().collect();
    for hints in lock_hints.values() {
        families.extend(hints.iter().map(|hint| hint.family()));
    }

    // Fail-closed pass: every family must have a mapping row, and the row
    // must not be Unsupported.
    let mut unmappable: Vec<UnmappableFamily> = Vec::new();
    for family in &families {
        match mapping_for_family(*family) {
            Some(WorkersMapping::Unsupported { reason, fix }) => {
                unmappable.push(UnmappableFamily {
                    family: *family,
                    reason,
                    fix,
                    nodes: nodes_requiring_family(requirements, &lock_hints, *family),
                });
            }
            Some(_) => {}
            None => {
                // A hint family with no table row: fail closed with the same
                // shape (this happens when EffectHint grows a family before
                // its Workers story is decided).
                unmappable.push(UnmappableFamily {
                    family: *family,
                    reason: "this capability family has no row in the Workers mapping table yet",
                    fix: "decide its Workers mapping and add a FAMILY_MAPPINGS row \
                          (crates/cli/src/deploy.rs), or run this flow on a native host",
                    nodes: nodes_requiring_family(requirements, &lock_hints, *family),
                });
            }
        }
    }
    if !unmappable.is_empty() {
        return Err(anyhow!(render_unmappable_error(requirements, &unmappable)));
    }

    // Collect bindings in deterministic (family table) order.
    let mut kv_namespaces: Vec<KvBinding> = Vec::new();
    let mut d1_databases: Vec<D1Binding> = Vec::new();
    let mut r2_buckets: Vec<R2Binding> = Vec::new();
    let mut durable_objects: Vec<DoBinding> = Vec::new();
    let mut vars: BTreeMap<String, String> = BTreeMap::new();
    let mut services: Vec<(Vec<String>, String, String)> = Vec::new();

    for (family, mapping) in FAMILY_MAPPINGS {
        if !families.contains(family) {
            continue;
        }
        let nodes = nodes_requiring_family(requirements, &lock_hints, *family);
        let nodes_list = nodes.join(", ");
        match mapping {
            WorkersMapping::KvNamespace => {
                let (id, provenance) = match lock_instance_value(
                    options.lock.as_ref(),
                    EffectHint::Kv,
                    &["namespace_id", "id"],
                ) {
                    Some((instance, kind, id)) => (
                        id,
                        format!(
                            "id from bindings.lock instance `{instance}` (provider_kind = {kind})"
                        ),
                    ),
                    None => (
                        "REPLACE_WITH_KV_NAMESPACE_ID".to_string(),
                        format!("create with: wrangler kv namespace create {worker}-kv"),
                    ),
                };
                kv_namespaces.push(KvBinding {
                    comment: vec![
                        format!("{} — required by node(s): {nodes_list}", family.as_str()),
                        provenance,
                    ],
                    binding: "FLOW_KV".to_string(),
                    id,
                });
            }
            WorkersMapping::D1Database => {
                let (database_id, provenance) = match lock_instance_value(
                    options.lock.as_ref(),
                    EffectHint::Sql,
                    &["database_id", "id"],
                ) {
                    Some((instance, kind, id)) => (
                        id,
                        format!(
                            "id from bindings.lock instance `{instance}` (provider_kind = {kind})"
                        ),
                    ),
                    None => (
                        "REPLACE_WITH_D1_DATABASE_ID".to_string(),
                        format!("create with: wrangler d1 create {worker}-db"),
                    ),
                };
                let database_name =
                    lock_instance_value(options.lock.as_ref(), EffectHint::Sql, &["database_name"])
                        .map(|(_, _, name)| name)
                        .unwrap_or_else(|| format!("{worker}-db"));
                d1_databases.push(D1Binding {
                    comment: vec![
                        format!("{} — required by node(s): {nodes_list}", family.as_str()),
                        provenance,
                    ],
                    binding: "DB".to_string(),
                    database_name,
                    database_id,
                });
            }
            WorkersMapping::R2Bucket => {
                let (bucket_name, provenance) = match lock_instance_value(
                    options.lock.as_ref(),
                    EffectHint::Blob,
                    &["bucket_name", "bucket"],
                ) {
                    Some((instance, kind, name)) => (
                        name,
                        format!(
                            "bucket from bindings.lock instance `{instance}` (provider_kind = {kind})"
                        ),
                    ),
                    None => (
                        format!("{worker}-blob"),
                        format!("create with: wrangler r2 bucket create {worker}-blob"),
                    ),
                };
                r2_buckets.push(R2Binding {
                    comment: vec![
                        format!("{} — required by node(s): {nodes_list}", family.as_str()),
                        provenance,
                    ],
                    binding: "BLOB_BUCKET".to_string(),
                    bucket_name,
                });
                notes.push(format!(
                    "{} maps to R2 per the W1 mapping table, but no cap-blob-workers \
                     provider crate exists yet; the binding is rendered for the planner, runtime \
                     wiring is a follow-up",
                    family.as_str()
                ));
            }
            WorkersMapping::Workspace => {
                // cap-workspace-workers shape (host-workers/workerd-tests):
                // R2 bucket + Durable Object index + sqlite migration.
                let (bucket_name, provenance) = match lock_instance_value(
                    options.lock.as_ref(),
                    EffectHint::Workspace,
                    &["bucket_name", "bucket"],
                ) {
                    Some((instance, kind, name)) => (
                        name,
                        format!(
                            "bucket from bindings.lock instance `{instance}` (provider_kind = {kind})"
                        ),
                    ),
                    None => (
                        format!("{worker}-workspace"),
                        format!("create with: wrangler r2 bucket create {worker}-workspace"),
                    ),
                };
                r2_buckets.push(R2Binding {
                    comment: vec![
                        format!(
                            "{} (cap-workspace-workers) — required by node(s): {nodes_list}",
                            family.as_str()
                        ),
                        provenance,
                    ],
                    binding: "WORKSPACE_BUCKET".to_string(),
                    bucket_name,
                });
                durable_objects.push(DoBinding {
                    comment: vec![format!("{} index (cap-workspace-workers)", family.as_str())],
                    name: "WORKSPACE_DO".to_string(),
                    class_name: "WorkspaceDurableObject".to_string(),
                });
            }
            WorkersMapping::DedupeDurableObject => {
                durable_objects.push(DoBinding {
                    comment: vec![format!(
                        "{} (Durable-Object-backed, s7 idiom) — required by node(s): {nodes_list}",
                        family.as_str()
                    )],
                    name: "DEDUP_DO".to_string(),
                    class_name: "FlowDurableObject".to_string(),
                });
            }
            WorkersMapping::Ambient(_) | WorkersMapping::Unsupported { .. } => {}
        }
    }

    // Durability services (mirrors host preflight demands).
    let durability = &requirements.durability;
    if durability.needs_checkpoint_store {
        durable_objects.push(DoBinding {
            comment: vec![format!(
                "durability: checkpoint store (durability.mode = \"{}\") — host-workers \
                 Durable Object idiom",
                durability_mode_str(durability.mode)
            )],
            name: "FLOW_DO".to_string(),
            class_name: "FlowDurableObject".to_string(),
        });
    }
    if durability.needs_resume_scheduler {
        // host-workers/workerd-tests idiom: alarm->resume dispatch via a
        // self service binding, configured through vars + a wrangler secret.
        vars.insert(
            "LATTICE_RESUME_SERVICE_BINDING".to_string(),
            "LATTICE_RESUME_SERVICE".to_string(),
        );
        services.push((
            vec![
                "durability: resume scheduler (halting timer nodes) — alarm->resume dispatch \
                 stays on the account edge via a self service binding"
                    .to_string(),
            ],
            "LATTICE_RESUME_SERVICE".to_string(),
            worker.clone(),
        ));
        notes.push(
            "resume scheduler required: set LATTICE_INTERNAL_RESUME_TOKEN as a Wrangler secret \
             (`wrangler secret put LATTICE_INTERNAL_RESUME_TOKEN`); do not commit secrets"
                .to_string(),
        );
    }
    if durability.needs_resume_signal_source {
        notes.push(
            "resume signal source required (callback/approval nodes): external resume calls \
             must present LATTICE_INTERNAL_RESUME_TOKEN (`wrangler secret put ...`)"
                .to_string(),
        );
    }
    if durability.needs_checkpoint_blob_store {
        notes.push(
            "checkpoint blob spill configured (blob_threshold_bytes): checkpoint payloads above \
             the threshold spill to the blob capability; ensure the R2 binding above is provisioned"
                .to_string(),
        );
    }

    // Deduplicate durable object bindings (workspace + durability may share
    // classes but never binding names) and keep deterministic order by name.
    durable_objects.sort_by(|a, b| a.name.cmp(&b.name));
    durable_objects.dedup_by(|a, b| a.name == b.name && a.class_name == b.class_name);
    let migration_classes: BTreeSet<String> = durable_objects
        .iter()
        .map(|binding| binding.class_name.clone())
        .collect();

    // Triggers: http triggers are served by the fetch handler; schedule
    // triggers render the `[triggers].crons` union below (schedule-trigger.md
    // §7c); an unspecified trigger has no wiring at all and cannot fire.
    for trigger in &requirements.triggers {
        match trigger.kind {
            TriggerKind::Http | TriggerKind::Schedule => {}
            TriggerKind::Unspecified => {
                notes.push(format!(
                    "trigger `{}` is wired to neither an HTTP nor a schedule entrypoint \
                     (kind = unspecified) — the worker will NOT fire it",
                    trigger.alias
                ));
            }
        }
    }

    // Schedule (cron) entrypoints -> [triggers].crons: sorted, deduplicated
    // union of entrypoints[].schedule, byte-verbatim (never normalized or
    // rewritten — the host-workers scheduled() handler routes fires by byte
    // equality against these strings). Duplicates across entrypoints are the
    // defined fan-out case and collapse to one crons entry (CF rejects
    // duplicate crons).
    let crons: BTreeSet<&str> = requirements
        .entrypoints
        .iter()
        .filter_map(|entrypoint| entrypoint.schedule.as_deref())
        .collect();
    let (cron_cap, cron_tier) = if options.paid {
        (PAID_MAX_CRONS, "paid plan")
    } else {
        (FREE_MAX_CRONS, "free plan")
    };
    if crons.len() > cron_cap {
        notes.push(format!(
            "this worker declares {} cron triggers, over the Cloudflare {cron_tier} \
             account-wide cap of {cron_cap} — the deploy will be rejected; drop schedules \
             or upgrade the plan",
            crons.len()
        ));
    }

    // Connector contracts → deploy notes (instance binding is lock-time; the
    // renderer surfaces what the deploy must satisfy).
    for connector in &requirements.connectors {
        for op in &connector.operations {
            if !op.requires_bound_connection {
                continue;
            }
            let roles = if op.roles.is_empty() {
                "no declared roles".to_string()
            } else {
                op.roles
                    .iter()
                    .map(|role| {
                        format!(
                            "{} `{}` expects {}",
                            role_kind_str(role.kind),
                            role.name,
                            role.expected_handle_kind
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            };
            notes.push(format!(
                "connector op `{}` requires a bound connection ({roles}); bind instances in \
                 bindings.lock and provide secrets via `wrangler secret put` before activation",
                op.operation_id
            ));
        }
    }

    // connector.http reachable-origin audit (spec §8): `resource::http` is
    // Ambient on Workers (no binding), but the renderer states which origin
    // each bound connector.http node can reach — planner v0 for HTTP — and
    // warns loudly when any Tier-2 any-origin (unauthenticated) grant is
    // present.
    let (http_audit, tier2_http_nodes) =
        connector_http_origin_audit(requirements, options.lock.as_ref());
    notes.extend(http_audit);
    if !tier2_http_nodes.is_empty() {
        notes.push(format!(
            "connector.http Tier-2 (any-origin) grant present — node(s) {} can reach ANY origin \
             with NO platform-managed credential (spec §8/§10); review before `wrangler deploy`",
            tier2_http_nodes.join(", ")
        ));
    }

    // Size budget.
    let (warn_bytes, max_bytes, tier) = if options.paid {
        (PAID_WARN_BYTES, PAID_MAX_BYTES, "paid tier")
    } else {
        (FREE_WARN_BYTES, FREE_MAX_BYTES, "free tier")
    };
    let mut budget_comment: Vec<String> = vec![format!(
        "Size budget (Cloudflare Workers {tier}): warn > {} compressed, error > {}.",
        format_bytes(warn_bytes),
        format_bytes(max_bytes)
    )];
    match &options.artifact {
        Some(artifact) => {
            if artifact.compressed_bytes > max_bytes {
                let paid_hint = if options.paid {
                    "the module must shrink (split flows across scripts, or trim dependencies)"
                } else {
                    "re-render with --paid if the account is on the paid plan, or shrink the module"
                };
                return Err(anyhow!(
                    "wasm module {} is {} compressed ({} raw), over the {tier} limit of {}: {paid_hint}",
                    artifact.source,
                    format_bytes(artifact.compressed_bytes),
                    format_bytes(artifact.raw_bytes),
                    format_bytes(max_bytes),
                ));
            }
            budget_comment.push(format!(
                "Measured: {} -> {} compressed ({} raw).",
                artifact.source,
                format_bytes(artifact.compressed_bytes),
                format_bytes(artifact.raw_bytes)
            ));
            if artifact.compressed_bytes > warn_bytes {
                let warning = format!(
                    "wasm module is {} compressed — within the {tier} limit of {} but above the \
                     {} warning threshold; expect deploy friction as the flow grows",
                    format_bytes(artifact.compressed_bytes),
                    format_bytes(max_bytes),
                    format_bytes(warn_bytes)
                );
                budget_comment.push(format!("WARNING: {warning}"));
                notes.push(warning);
            }
        }
        None => {
            budget_comment.push(
                "No wasm artifact was available at render time, so the compressed module size \
                 was NOT checked."
                    .to_string(),
            );
            budget_comment.push(
                "After building, re-render with: --wasm-artifact \
                 target/wasm32-unknown-unknown/release/<package>.wasm"
                    .to_string(),
            );
            notes.push(
                "no wasm artifact available at render time; module size budget NOT checked \
                 (see the size budget comment in wrangler.toml)"
                    .to_string(),
            );
        }
    }

    notes.push(
        "worker entry crate is not scaffolded by this command yet; follow the \
         crates/host-workers worker-build entry pattern (see its wrangler.toml and \
         scripts/ensure-worker-build.sh)"
            .to_string(),
    );

    // ------------------------------------------------------------------
    // Emit, in fixed section order.
    // ------------------------------------------------------------------
    let mut out = String::new();
    let push_line = |out: &mut String, line: &str| {
        out.push_str(line);
        out.push('\n');
    };

    push_line(
        &mut out,
        "# wrangler.toml — rendered by `flows deploy render` (infra-from-code v0).",
    );
    push_line(&mut out, "#");
    push_line(
        &mut out,
        &format!(
            "# flow: {} v{} (id {})",
            requirements.flow.name,
            requirements.flow.version,
            requirements.flow.id.as_str()
        ),
    );
    if let Some(hash) = &requirements.flow_ir_hash {
        push_line(&mut out, &format!("# flow_ir_hash: {hash}"));
    }
    push_line(
        &mut out,
        &format!("# requirements schema: {}", requirements.schema_version),
    );
    push_line(&mut out, "#");
    push_line(
        &mut out,
        "# Review every REPLACE_WITH_* placeholder before `wrangler deploy`; each carries",
    );
    push_line(
        &mut out,
        "# a comment with the exact `wrangler` command that creates the resource.",
    );
    push_line(&mut out, "");
    push_line(&mut out, &format!("name = \"{worker}\""));
    push_line(&mut out, &format!("main = \"{WORKER_MAIN}\""));
    push_line(&mut out, "workers_dev = true");
    push_line(
        &mut out,
        &format!("compatibility_date = \"{COMPATIBILITY_DATE}\""),
    );
    push_line(&mut out, "");
    push_line(&mut out, "[build]");
    push_line(
        &mut out,
        "# worker-build install handled once by the guard script (see README \"Workers build tooling\").",
    );
    push_line(&mut out, &format!("command = \"{BUILD_COMMAND}\""));

    if !vars.is_empty() {
        push_line(&mut out, "");
        push_line(&mut out, "[vars]");
        for (key, value) in &vars {
            push_line(&mut out, &format!("{key} = \"{value}\""));
        }
    }

    for kv in &kv_namespaces {
        push_line(&mut out, "");
        for line in &kv.comment {
            push_line(&mut out, &format!("# {line}"));
        }
        push_line(&mut out, "[[kv_namespaces]]");
        push_line(&mut out, &format!("binding = \"{}\"", kv.binding));
        push_line(&mut out, &format!("id = \"{}\"", kv.id));
    }

    for d1 in &d1_databases {
        push_line(&mut out, "");
        for line in &d1.comment {
            push_line(&mut out, &format!("# {line}"));
        }
        push_line(&mut out, "[[d1_databases]]");
        push_line(&mut out, &format!("binding = \"{}\"", d1.binding));
        push_line(
            &mut out,
            &format!("database_name = \"{}\"", d1.database_name),
        );
        push_line(&mut out, &format!("database_id = \"{}\"", d1.database_id));
    }

    for r2 in &r2_buckets {
        push_line(&mut out, "");
        for line in &r2.comment {
            push_line(&mut out, &format!("# {line}"));
        }
        push_line(&mut out, "[[r2_buckets]]");
        push_line(&mut out, &format!("binding = \"{}\"", r2.binding));
        push_line(&mut out, &format!("bucket_name = \"{}\"", r2.bucket_name));
    }

    for durable in &durable_objects {
        push_line(&mut out, "");
        for line in &durable.comment {
            push_line(&mut out, &format!("# {line}"));
        }
        push_line(&mut out, "[[durable_objects.bindings]]");
        push_line(&mut out, &format!("name = \"{}\"", durable.name));
        push_line(
            &mut out,
            &format!("class_name = \"{}\"", durable.class_name),
        );
    }

    if !migration_classes.is_empty() {
        push_line(&mut out, "");
        push_line(&mut out, "[[migrations]]");
        push_line(&mut out, "tag = \"v1\"");
        let classes = migration_classes
            .iter()
            .map(|class| format!("\"{class}\""))
            .collect::<Vec<_>>()
            .join(", ");
        push_line(&mut out, &format!("new_sqlite_classes = [{classes}]"));
    }

    for (comment, binding, service) in &services {
        push_line(&mut out, "");
        for line in comment {
            push_line(&mut out, &format!("# {line}"));
        }
        push_line(&mut out, "[[services]]");
        push_line(&mut out, &format!("binding = \"{binding}\""));
        push_line(&mut out, &format!("service = \"{service}\""));
    }

    if !crons.is_empty() {
        push_line(&mut out, "");
        push_line(
            &mut out,
            "# Cron schedules (fired into the worker scheduled() handler, which routes by",
        );
        push_line(
            &mut out,
            "# byte equality against these strings — never edit them out of sync with the",
        );
        push_line(
            &mut out,
            "# bundle; see impl-docs/spec/schedule-trigger.md):",
        );
        for cron in &crons {
            for entrypoint in &requirements.entrypoints {
                if entrypoint.schedule.as_deref() != Some(*cron) {
                    continue;
                }
                let deadline = entrypoint
                    .deadline_ms
                    .map(|ms| format!(", deadline {ms}ms"))
                    .unwrap_or_default();
                push_line(
                    &mut out,
                    &format!(
                        "#   \"{cron}\"  (trigger `{}` -> capture `{}`{deadline})",
                        entrypoint.trigger_alias, entrypoint.capture_alias
                    ),
                );
            }
        }
        push_line(&mut out, "[triggers]");
        let cron_list = crons
            .iter()
            .map(|cron| format!("\"{cron}\""))
            .collect::<Vec<_>>()
            .join(", ");
        push_line(&mut out, &format!("crons = [{cron_list}]"));
    }

    let http_entrypoints: Vec<_> = requirements
        .entrypoints
        .iter()
        .filter(|entrypoint| entrypoint.schedule.is_none())
        .collect();
    if !http_entrypoints.is_empty() {
        push_line(&mut out, "");
        push_line(
            &mut out,
            "# HTTP entrypoints (served by the worker fetch handler, not wrangler config;",
        );
        push_line(
            &mut out,
            "# workers_dev = true exposes the <name>.workers.dev route):",
        );
        for entrypoint in http_entrypoints {
            let method = entrypoint.method.as_deref().unwrap_or("ANY");
            let route = entrypoint.route_path.as_deref().unwrap_or("<unrouted>");
            let deadline = entrypoint
                .deadline_ms
                .map(|ms| format!(", deadline {ms}ms"))
                .unwrap_or_default();
            push_line(
                &mut out,
                &format!(
                    "#   {method} {route}  (trigger `{}` -> capture `{}`{deadline})",
                    entrypoint.trigger_alias, entrypoint.capture_alias
                ),
            );
        }
    }

    push_line(&mut out, "");
    for line in &budget_comment {
        push_line(&mut out, &format!("# {line}"));
    }

    if !notes.is_empty() {
        push_line(&mut out, "");
        push_line(&mut out, "# NOTES:");
        for note in &notes {
            push_line(&mut out, &format!("# - {note}"));
        }
    }

    Ok(Rendered {
        wrangler_toml: out,
        notes,
    })
}

fn reject_native_only_http_byte_operations(requirements: &FlowRequirements) -> Result<()> {
    let unsupported: Vec<_> = requirements
        .connectors
        .iter()
        .flat_map(|connector| connector.operations.iter())
        .filter(|operation| {
            NATIVE_ONLY_HTTP_BYTE_OPERATIONS.contains(&operation.operation_id.as_str())
        })
        .collect();
    if unsupported.is_empty() {
        return Ok(());
    }

    let mut message = format!(
        "flow `{}` cannot be rendered for Cloudflare Workers: H5c-ops-wasm is absent, so these connector.http byte operations are native-only:\n",
        requirements.flow.name
    );
    for operation in unsupported {
        let nodes = if operation.nodes.is_empty() {
            "<no per-node attribution recorded>".to_string()
        } else {
            operation.nodes.join(", ")
        };
        message.push_str(&format!(
            "\n  `{}` — required by node(s): {nodes}",
            operation.operation_id
        ));
    }
    message.push_str(
        "\n\nRun this flow on a native host (`flows run local --example <name>` / host-web-axum). Workers deployment requires implementing H5c-ops-wasm first.",
    );
    Err(anyhow!(message))
}

/// Planner v0: the fail-closed error for requirements Workers cannot satisfy.
fn render_unmappable_error(
    requirements: &FlowRequirements,
    unmappable: &[UnmappableFamily],
) -> String {
    let mut out = format!(
        "flow `{}` cannot be rendered for Cloudflare Workers: {} requirement famil{} have no \
         Workers mapping\n",
        requirements.flow.name,
        unmappable.len(),
        if unmappable.len() == 1 { "y" } else { "ies" },
    );
    for family in unmappable {
        let nodes = if family.nodes.is_empty() {
            "<no per-node attribution recorded>".to_string()
        } else {
            family.nodes.join(", ")
        };
        out.push_str(&format!(
            "\n  {} — required by node(s): {nodes}\n      why: {}\n      fix: {}\n",
            family.family.as_str(),
            family.reason,
            family.fix
        ));
    }
    out.push_str(
        "\nThis flow can still run on a native host (`flows run local --example <name>` / \
         host-web-axum). Re-render after removing or remapping the unsupported requirements.",
    );
    out
}

fn durability_mode_str(mode: dag_core::DurabilityMode) -> &'static str {
    match mode {
        dag_core::DurabilityMode::Off => "off",
        dag_core::DurabilityMode::Partial => "partial",
        dag_core::DurabilityMode::Strong => "strong",
    }
}

fn role_kind_str(kind: dag_core::ConnectorRoleKindDecl) -> &'static str {
    match kind {
        dag_core::ConnectorRoleKindDecl::OutboundAuth => "outbound_auth",
        dag_core::ConnectorRoleKindDecl::ProvisioningAuth => "provisioning_auth",
        dag_core::ConnectorRoleKindDecl::InboundVerifier => "inbound_verifier",
        dag_core::ConnectorRoleKindDecl::EndpointProfile => "endpoint_profile",
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes % MIB == 0 {
        format!("{} MiB", bytes / MIB)
    } else if bytes % KIB == 0 {
        format!("{} KiB", bytes / KIB)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every EffectHint family must have exactly one mapping-table row: a new
    /// hint family cannot land without deciding its Workers story.
    #[test]
    fn mapping_table_is_exhaustive_over_hint_families() {
        let families: BTreeSet<EffectHint> =
            EffectHint::ALL.iter().map(|hint| hint.family()).collect();
        for family in &families {
            let rows = FAMILY_MAPPINGS
                .iter()
                .filter(|(entry, _)| entry == family)
                .count();
            assert_eq!(
                rows,
                1,
                "family {} must have exactly one FAMILY_MAPPINGS row (found {rows})",
                family.as_str()
            );
        }
        assert_eq!(
            FAMILY_MAPPINGS.len(),
            families.len(),
            "FAMILY_MAPPINGS must not carry rows for non-family hints"
        );
    }

    #[test]
    fn worker_name_sanitization() {
        assert_eq!(sanitize_worker_name("s1_echo_flow"), "s1-echo-flow");
        assert_eq!(sanitize_worker_name("My Flow.v2"), "my-flow-v2");
        assert_eq!(sanitize_worker_name("__x__"), "x");
        assert_eq!(sanitize_worker_name("!!!"), "lattice-flow");
    }

    #[test]
    fn byte_formatting_used_in_budget_comments() {
        assert_eq!(format_bytes(FREE_WARN_BYTES), "900 KiB");
        assert_eq!(format_bytes(FREE_MAX_BYTES), "1 MiB");
        assert_eq!(format_bytes(PAID_WARN_BYTES), "3 MiB");
        assert_eq!(format_bytes(PAID_MAX_BYTES), "10 MiB");
        assert_eq!(format_bytes(1000), "1000 B");
    }
}
