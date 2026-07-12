//! S27 — the connector.http byte-plane acceptance flow (packet H5d of
//! `impl-docs/spec/http-request-node.md` §16). This example exercises the byte
//! plane **compositionally**, through the real host-inproc runtime, and is the
//! canonical clone-recipe reference for byte-handling flows.
//!
//! **The decision it proves (spec §16.0):** bytes never enter the JSON data
//! plane. A node stages bytes into the run-scoped workspace and hands
//! downstream a minted, attenuable `Artifact` handle
//! (`{ handle, content_type, len, content_hash }`); a consumer dereferences it
//! under a `workspace::read` grant. The `Artifact` is the only thing that
//! travels node-to-node as JSON — never base64 bytes.
//!
//! Two round-trips (two entrypoints on one flow):
//!
//! **Egress — CSV build → stage → multipart POST (`POST /report`):**
//! 1. `report_trigger` receives structured rows.
//! 2. `build_report` (`resource::workspace::write`, Effectful) builds a small
//!    CSV in plain Rust, stages it via the handle-only `workspace_write()` VIEW
//!    (`context::with_current_async` — spec §16.4/§16.5; there is NO `ws:`
//!    def_node parameter injection, the macro sugar does not exist), and returns
//!    the staged `Artifact`.
//! 3. `upload_report` (`connector.http.post_multipart`:
//!    `resource::http::write + resource::workspace::read`, Effectful) uploads it
//!    with `form!{ "file" => artifact, "kind" => "daily" }`; the artifact part
//!    is dereffed through the `workspace_read()` VIEW at send time.
//!
//! **Ingress — download → process → re-upload (`POST /mirror`):**
//! 1. `mirror_trigger` receives a source path.
//! 2. `download` (`connector.http.get_binary`:
//!    `resource::http::read + resource::workspace::write`, Effectful) downloads
//!    the body straight into a workspace `Artifact` (host-side stage — the bytes
//!    never round-trip through the JSON plane).
//! 3. `transform` (`workspace::read + workspace::write`, Effectful) reads the
//!    bytes back through the `workspace_read()` VIEW, upper-cases them, and
//!    re-stages them via `workspace_write()`.
//! 4. `reupload` (`connector.http.post_multipart`) uploads the processed
//!    artifact.
//!
//! Every workspace access uses the context-fetch style
//! (`capabilities::context::with_current_async(|r| r.workspace_write()/…)`),
//! never a raw `Workspace` trait and never a def_node `ws:` parameter.
//!
//! The [`denied_bundle`] variant swaps the honest `build_report` for a
//! `build_report_ungranted` node that declares `resource::workspace::read` only
//! but attempts to `stage` — the compositional analogue of the connector
//! honesty tests. Run through the real runtime it is denied
//! (`MissingWorkspaceWrite`), proving the §16.4 read/write split is
//! load-bearing at the node boundary, not decorative.

use std::collections::BTreeMap;
use std::time::Duration;

use capabilities::{Artifact, context};
use connector_http::form;
use connector_http::ops::{HttpGetBinary, HttpPostMultipart};
use connector_http::{HttpGetBinaryInput, HttpJsonOutput, HttpMultipartInput};
use dag_core::{FlowIR, NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};

pub const FLOW_NAME: &str = "s27_binary_flow";
pub const REPORT_TRIGGER: &str = "report_trigger";
pub const MIRROR_TRIGGER: &str = "mirror_trigger";

pub const REPORT_UPLOAD_PATH: &str = "/upload";
pub const MIRROR_UPLOAD_PATH: &str = "/mirror-upload";

/// The staged CSV artifact name (egress).
pub const REPORT_STAGE_NAME: &str = "report.csv";
/// The staged download artifact name (ingress).
pub const DOWNLOAD_STAGE_NAME: &str = "downloads/source.csv";
/// The re-staged, transformed artifact name (ingress).
pub const PROCESSED_STAGE_NAME: &str = "processed.csv";

// ---------------------------------------------------------------------------
// Boundary types (JSON-plane data). The byte plane is separate — only the
// `Artifact` handle crosses node boundaries, never the bytes.
// ---------------------------------------------------------------------------

/// One structured row the egress CSV is built from.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Row {
    pub label: String,
    pub value: i64,
}

/// Egress input: the rows to serialize into a CSV and upload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReportRequest {
    pub rows: Vec<Row>,
}

/// Output of `build_report`: the staged `Artifact` handle plus a small bit of
/// self-description. The bytes live in the workspace; only this crosses the
/// wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StagedReport {
    pub artifact: Artifact,
    pub row_count: u64,
}

/// Ingress input: the source path to download and mirror.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MirrorRequest {
    pub source_path: String,
}

/// Output of `download`: the artifact the 2xx body was staged into.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Downloaded {
    pub artifact: Artifact,
}

/// Output of `transform`: the re-staged, processed artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Processed {
    pub artifact: Artifact,
    pub original_len: u64,
}

/// Terminal receipt for an upload leg.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UploadReceipt {
    pub ok: bool,
    pub artifact_name: String,
    pub content_type: String,
    pub len: u64,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

/// Serialize rows into a small CSV in plain Rust (spec §16.5: serialization is
/// a code node, not a Lattice primitive). No third-party crate needed for this
/// shape; the point is that the CSV is built in-process and staged as bytes.
pub fn render_csv(rows: &[Row]) -> String {
    let mut out = String::from("label,value\n");
    for row in rows {
        // Values are integers and labels are simple tokens in this example, so
        // no quoting is required; a real clone would use the `csv` crate.
        out.push_str(&row.label);
        out.push(',');
        out.push_str(&row.value.to_string());
        out.push('\n');
    }
    out
}

fn upload_receipt(
    out: &HttpJsonOutput,
    artifact_name: &str,
    content_type: &str,
    len: u64,
) -> UploadReceipt {
    UploadReceipt {
        ok: out
            .body
            .get("ok")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        artifact_name: artifact_name.to_string(),
        content_type: content_type.to_string(),
        len,
    }
}

// ---------------------------------------------------------------------------
// Nodes — egress (`POST /report`)
// ---------------------------------------------------------------------------

/// Egress ingress: passes the structured rows through.
#[def_node(
    trigger,
    name = "ReportTrigger",
    summary = "HTTP webhook ingress; receives the rows to build a CSV report from",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn report_trigger(req: ReportRequest) -> NodeResult<ReportRequest> {
    Ok(req)
}

/// Build a CSV in plain Rust and stage it into the run workspace, returning the
/// minted `Artifact` handle. Floor: `resource::workspace::write`, Effectful.
/// Workspace access is the handle-only `workspace_write()` VIEW reached via
/// `context::with_current_async` (spec §16.4/§16.5) — never a raw `Workspace`
/// trait, never a `ws:` def_node parameter.
#[def_node(
    name = "BuildReport",
    summary = "Build a CSV from rows and stage it as a workspace Artifact (workspace::write)",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_write(capabilities::workspace::Workspace))
)]
async fn build_report(req: ReportRequest) -> NodeResult<StagedReport> {
    let csv = render_csv(&req.rows);
    let row_count = req.rows.len() as u64;

    let artifact = context::with_current_async(move |resources| async move {
        let writer = resources.workspace_write().ok_or_else(|| {
            node_error("staging denied: this node lacks resource::workspace::write (MissingWorkspaceWrite)")
        })?;
        writer
            .stage_artifact(REPORT_STAGE_NAME, csv.as_bytes(), "text/csv")
            .await
            .map_err(|err| node_error(format!("stage {REPORT_STAGE_NAME} failed: {err}")))
    })
    .await
    .ok_or_else(|| node_error("missing ResourceAccess context"))??;

    Ok(StagedReport {
        artifact,
        row_count,
    })
}

/// Dishonest variant of `build_report` for the negative/denial test: it
/// declares `resource::workspace::read` ONLY, but its body still attempts to
/// `stage` (a write). Run through the real runtime the `workspace_write()` VIEW
/// is unreachable (the read grant does not confer write — spec §16.4), so the
/// node is denied `MissingWorkspaceWrite`. This is the compositional analogue
/// of the connector honesty tests: the effect floor holds at the node boundary,
/// not by assertion.
#[def_node(
    name = "BuildReportUngranted",
    summary = "Read-only-granted node that attempts to stage — proves the workspace read/write split (denial)",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_read(capabilities::workspace::Workspace))
)]
async fn build_report_ungranted(req: ReportRequest) -> NodeResult<StagedReport> {
    let csv = render_csv(&req.rows);
    let row_count = req.rows.len() as u64;

    let artifact = context::with_current_async(move |resources| async move {
        let writer = resources.workspace_write().ok_or_else(|| {
            node_error("staging denied: this node lacks resource::workspace::write (MissingWorkspaceWrite)")
        })?;
        writer
            .stage_artifact(REPORT_STAGE_NAME, csv.as_bytes(), "text/csv")
            .await
            .map_err(|err| node_error(format!("stage {REPORT_STAGE_NAME} failed: {err}")))
    })
    .await
    .ok_or_else(|| node_error("missing ResourceAccess context"))??;

    Ok(StagedReport {
        artifact,
        row_count,
    })
}

/// Multipart POST of the staged report. Floor (via `connector_ops`):
/// `resource::http::write + resource::workspace::read`, Effectful. The artifact
/// part is dereferenced through the `workspace_read()` VIEW inside the op at
/// send time (`form!`), before any request leaves the process. The
/// `connector.http.*` identifier is load-bearing for runtime connection scope
/// resolution (s15 finding, s26/H4).
#[def_node(
    name = "UploadReport",
    identifier = "connector.http.upload_report",
    summary = "POST the staged CSV as multipart/form-data via connector.http.post_multipart",
    effects = "Effectful",
    determinism = "BestEffort",
    connector_ops(connector_http::ops::HttpPostMultipart)
)]
async fn upload_report(staged: StagedReport) -> NodeResult<UploadReceipt> {
    let content_type = staged.artifact.content_type.clone();
    let len = staged.artifact.len;
    let out: HttpJsonOutput = HttpPostMultipart::invoke(&HttpMultipartInput {
        path: REPORT_UPLOAD_PATH.to_string(),
        query: Vec::new(),
        headers: BTreeMap::new(),
        parts: form! { "file" => staged.artifact, "kind" => "daily" },
        target: None,
    })
    .await
    .map_err(|err| {
        node_error(format!(
            "connector.http.post_multipart (report) failed: {err}"
        ))
    })?;

    Ok(upload_receipt(&out, REPORT_STAGE_NAME, &content_type, len))
}

// ---------------------------------------------------------------------------
// Nodes — ingress (`POST /mirror`)
// ---------------------------------------------------------------------------

/// Ingress ingress: passes the source path through.
#[def_node(
    trigger,
    name = "MirrorTrigger",
    summary = "HTTP webhook ingress; receives the source path to download and mirror",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn mirror_trigger(req: MirrorRequest) -> NodeResult<MirrorRequest> {
    Ok(req)
}

/// Download the source into a workspace `Artifact`. Floor (via `connector_ops`):
/// `resource::http::read + resource::workspace::write`, Effectful and
/// run-local-idempotent (spec §16.3, F5 — no dedupe edge needed: a replay
/// re-downloads and re-stages to the same path). `get_binary` is a host-side
/// composite; the bytes never enter the JSON plane.
#[def_node(
    name = "Download",
    identifier = "connector.http.download",
    summary = "GET the source body straight into a workspace Artifact via connector.http.get_binary",
    effects = "Effectful",
    determinism = "BestEffort",
    connector_ops(connector_http::ops::HttpGetBinary)
)]
async fn download(req: MirrorRequest) -> NodeResult<Downloaded> {
    let artifact = HttpGetBinary::invoke(&HttpGetBinaryInput {
        path: req.source_path.clone(),
        stage_name: Some(DOWNLOAD_STAGE_NAME.to_string()),
        ..Default::default()
    })
    .await
    .map_err(|err| node_error(format!("connector.http.get_binary failed: {err}")))?;

    Ok(Downloaded { artifact })
}

/// Read the downloaded bytes back through the `workspace_read()` VIEW,
/// transform them (upper-case), and re-stage through the `workspace_write()`
/// VIEW. Floor: `resource::workspace::read + resource::workspace::write`,
/// Effectful. Both accesses use the context-fetch style (spec §16.4).
#[def_node(
    name = "Transform",
    summary = "Deref the downloaded Artifact, upper-case it, and re-stage it (workspace::read + workspace::write)",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        workspace_read(capabilities::workspace::Workspace),
        workspace_write(capabilities::workspace::Workspace)
    )
)]
async fn transform(down: Downloaded) -> NodeResult<Processed> {
    let artifact = down.artifact;

    let (processed, original_len) = context::with_current_async(move |resources| async move {
        let reader = resources.workspace_read().ok_or_else(|| {
            node_error("deref denied: this node lacks resource::workspace::read (MissingWorkspaceRead)")
        })?;
        let bytes = reader
            .read(&artifact.handle)
            .await
            .map_err(|err| node_error(format!("deref downloaded artifact failed: {err}")))?;
        let original_len = bytes.len() as u64;

        // The transform: upper-case the bytes (a stand-in for "process the file").
        let upper: Vec<u8> = bytes.iter().map(|byte| byte.to_ascii_uppercase()).collect();

        let writer = resources.workspace_write().ok_or_else(|| {
            node_error("re-stage denied: this node lacks resource::workspace::write (MissingWorkspaceWrite)")
        })?;
        let staged = writer
            .stage_artifact(PROCESSED_STAGE_NAME, &upper, "text/csv")
            .await
            .map_err(|err| node_error(format!("re-stage {PROCESSED_STAGE_NAME} failed: {err}")))?;

        Ok::<(Artifact, u64), NodeError>((staged, original_len))
    })
    .await
    .ok_or_else(|| node_error("missing ResourceAccess context"))??;

    Ok(Processed {
        artifact: processed,
        original_len,
    })
}

/// Multipart POST of the processed artifact. Floor (via `connector_ops`):
/// `resource::http::write + resource::workspace::read`, Effectful.
#[def_node(
    name = "Reupload",
    identifier = "connector.http.reupload",
    summary = "POST the processed artifact as multipart/form-data via connector.http.post_multipart",
    effects = "Effectful",
    determinism = "BestEffort",
    connector_ops(connector_http::ops::HttpPostMultipart)
)]
async fn reupload(processed: Processed) -> NodeResult<UploadReceipt> {
    let content_type = processed.artifact.content_type.clone();
    let len = processed.artifact.len;
    let out: HttpJsonOutput = HttpPostMultipart::invoke(&HttpMultipartInput {
        path: MIRROR_UPLOAD_PATH.to_string(),
        query: Vec::new(),
        headers: BTreeMap::new(),
        parts: form! { "file" => processed.artifact },
        target: None,
    })
    .await
    .map_err(|err| {
        node_error(format!(
            "connector.http.post_multipart (mirror) failed: {err}"
        ))
    })?;

    Ok(upload_receipt(
        &out,
        PROCESSED_STAGE_NAME,
        &content_type,
        len,
    ))
}

// ---------------------------------------------------------------------------
// Flow — the honest, full byte-plane graph (both round-trips)
// ---------------------------------------------------------------------------

// Two HTTP triggers (the `/report` egress and `/mirror` ingress), so the flow
// opts into `allow_multiple_triggers` after macro expansion — the s7/s26
// pattern.
mod bundle_def {
    use dag_macros::node;

    dag_macros::flow! {
        name: s27_binary_flow,
        version: "1.0.0",
        profile: Web,
        summary: "H5d acceptance: connector.http byte plane — CSV build/stage/multipart-POST egress and get_binary/transform/re-upload ingress, all through minted Artifact handles";

        // Egress: POST /report.
        let report_trigger = node!(report_trigger);
        let build_report = node!(build_report);
        let upload_report = node!(upload_report);

        connect!(report_trigger -> build_report);
        connect!(build_report -> upload_report);

        // Ingress: POST /mirror.
        let mirror_trigger = node!(mirror_trigger);
        let download = node!(download);
        let transform = node!(transform);
        let reupload = node!(reupload);

        connect!(mirror_trigger -> download);
        connect!(download -> transform);
        connect!(transform -> reupload);

        entrypoint!({
            trigger: "report_trigger",
            capture: "upload_report",
            route_aliases: ["/report"],
            method: "POST",
            deadline_ms: 15_000,
        });
        entrypoint!({
            trigger: "mirror_trigger",
            capture: "reupload",
            route_aliases: ["/mirror"],
            method: "POST",
            deadline_ms: 15_000,
        });
    }
}

/// The flow IR, with the multi-trigger lint opt-in applied.
pub fn flow() -> FlowIR {
    let mut flow = bundle_def::flow();
    flow.policies.lint.allow_multiple_triggers = Some(true);
    flow
}

/// The validated IR.
pub fn validated_ir() -> kernel_plan::ValidatedIR {
    kernel_plan::validate(&flow()).expect("s27 flow should validate")
}

/// The host-inproc bundle (both round-trips intact).
#[cfg(feature = "host-bundle")]
pub fn bundle() -> host_inproc::FlowBundle {
    build_bundle(validated_ir(), false)
}

// ---------------------------------------------------------------------------
// Denied flow — the negative/honesty variant (egress only, ungranted staging)
// ---------------------------------------------------------------------------

mod denied_def {
    use dag_macros::node;

    dag_macros::flow! {
        name: s27_binary_denied_flow,
        version: "1.0.0",
        profile: Web,
        summary: "H5d honesty negative: the egress round-trip with a read-only-granted staging node — denied MissingWorkspaceWrite through the real runtime";

        let report_trigger = node!(report_trigger);
        let build_report_ungranted = node!(build_report_ungranted);
        let upload_report = node!(upload_report);

        connect!(report_trigger -> build_report_ungranted);
        connect!(build_report_ungranted -> upload_report);

        entrypoint!({
            trigger: "report_trigger",
            capture: "upload_report",
            route_aliases: ["/report"],
            method: "POST",
            deadline_ms: 15_000,
        });
    }
}

/// The denied flow IR (single egress entrypoint).
pub fn denied_flow() -> FlowIR {
    denied_def::flow()
}

/// Validated denied IR.
pub fn denied_validated_ir() -> kernel_plan::ValidatedIR {
    kernel_plan::validate(&denied_flow()).expect("s27 denied flow should validate")
}

/// The host-inproc bundle for the denied (read-only-granted staging) variant.
#[cfg(feature = "host-bundle")]
pub fn denied_bundle() -> host_inproc::FlowBundle {
    build_bundle(denied_validated_ir(), true)
}

// ---------------------------------------------------------------------------
// Bundle construction shared by both flows.
// ---------------------------------------------------------------------------

#[cfg(feature = "host-bundle")]
fn build_bundle(validated_ir: kernel_plan::ValidatedIR, denied: bool) -> host_inproc::FlowBundle {
    use std::sync::Arc;

    use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
    use kernel_exec::NodeRegistry;

    let mut registry = NodeRegistry::new();
    report_trigger_register(&mut registry).expect("register report_trigger");
    upload_report_register(&mut registry).expect("register upload_report");
    if denied {
        build_report_ungranted_register(&mut registry).expect("register build_report_ungranted");
    } else {
        build_report_register(&mut registry).expect("register build_report");
        mirror_trigger_register(&mut registry).expect("register mirror_trigger");
        download_register(&mut registry).expect("register download");
        transform_register(&mut registry).expect("register transform");
        reupload_register(&mut registry).expect("register reupload");
    }

    let registry = Arc::new(registry);
    let resolver: Arc<dyn kernel_exec::NodeResolver> =
        Arc::new(kernel_exec::RegistryResolver::new(registry.clone()));

    let mut entrypoints = vec![FlowEntrypoint {
        trigger_alias: "report_trigger".to_string(),
        capture_alias: "upload_report".to_string(),
        route_path: Some("/report".to_string()),
        method: Some("POST".to_string()),
        deadline: Some(Duration::from_millis(15_000)),
        route_aliases: vec!["/report".to_string()],
        schedule: None,
    }];

    let mut node_contracts = if denied {
        vec![
            node!(report_trigger),
            node!(build_report_ungranted),
            node!(upload_report),
        ]
    } else {
        entrypoints.push(FlowEntrypoint {
            trigger_alias: "mirror_trigger".to_string(),
            capture_alias: "reupload".to_string(),
            route_path: Some("/mirror".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(15_000)),
            route_aliases: vec!["/mirror".to_string()],
            schedule: None,
        });
        vec![
            node!(report_trigger),
            node!(build_report),
            node!(upload_report),
            node!(mirror_trigger),
            node!(download),
            node!(transform),
            node!(reupload),
        ]
    };

    let node_contracts = node_contracts
        .drain(..)
        .map(|spec| NodeContract {
            identifier: spec.identifier.to_string(),
            contract_hash: None,
            source: NodeSource::Local,
        })
        .collect();

    FlowBundle {
        validated_ir,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

#[cfg(test)]
mod tests;
