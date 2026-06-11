use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use axum::http::Method;
use cargo_metadata::MetadataCommand;
use clap::{Args, Parser, Subcommand, ValueEnum};
use dag_core::{Diagnostic, DurabilityMode, FlowIR, Severity};
use exporters::{harness::HarnessConfig, to_dot, to_json_value};
#[cfg(feature = "host-wasmtime")]
use flow_bundle::ExecPolicy;
use flow_bundle::Manifest;
use futures::StreamExt;
#[cfg(feature = "host-wasmtime")]
use host_wasmtime::load_flow_bundle;
use host_web_axum::{HostHandle, RouteConfig};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kernel_exec::{ExecutionResult, FlowExecutor};
use kernel_plan::{ValidatedIR, validate};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::signal;

use capabilities::Capability;
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRoleKind, ConnectorRuntime, ConnectorRuntimeError,
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
    ResolvedConnectorConnection, ResolvedEndpointProfile,
};
use capabilities::durability::{
    CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, Lease,
};
use capabilities::{ResourceAccess, ResourceBag};
use host_inproc::{EnvironmentPlugin, HostRuntime, Invocation};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

#[cfg(feature = "example-github-issues")]
use example_connector_github_issues_local_flow;
#[cfg(feature = "example-google-sheets")]
use example_connector_google_sheets_local_flow;
#[cfg(feature = "example-s1")]
use example_s1_echo as s1_echo;
#[cfg(feature = "example-s2")]
use example_s2_site as s2_site;
#[cfg(feature = "example-s3")]
use example_s3_branching as s3_branching;
#[cfg(feature = "example-s4")]
use example_s4_preflight as s4_preflight;
#[cfg(feature = "example-s5")]
use example_s5_unsupported_surface as s5_unsupported_surface;
#[cfg(feature = "example-s6")]
use example_s6_spill_host as s6_spill;
#[cfg(feature = "example-s11")]
use example_s11_lead_intake as s11_lead_intake;
#[cfg(feature = "example-s12")]
use example_s12_sheetport_quote as s12_sheetport_quote;
#[cfg(feature = "example-s13")]
use example_s13_github_issue_investigator as s13_github_issue_investigator;

mod bundle;
mod local_durability;
mod resume;
mod scaffold;

#[derive(Parser, Debug)]
#[command(
    name = "flows",
    version,
    author,
    about = "Lattice command-line interface"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Graph inspection and validation commands.
    #[command(subcommand)]
    Graph(GraphCommand),
    /// Entrypoint/trigger wiring validation.
    #[command(subcommand)]
    Entrypoints(EntrypointsCommand),
    /// Execute or serve workflows locally.
    #[command(subcommand)]
    Run(RunCommand),
    /// Resource bindings tooling.
    #[command(subcommand)]
    Bindings(BindingsCommand),
    /// Build a FlowBundle for wasm targets.
    Bundle(bundle::BundleArgs),
    /// Resume checkpoints from local durability store.
    #[command(subcommand)]
    Resume(resume::ResumeCommand),
    /// Scaffold a new example crate (modeled on examples/s1_echo).
    New(scaffold::NewArgs),
}

#[derive(Subcommand, Debug)]
enum GraphCommand {
    /// Validate a Flow IR document and optionally emit artifacts.
    Check(GraphCheckArgs),
}

#[derive(Subcommand, Debug)]
enum EntrypointsCommand {
    /// Validate trigger/capture wiring for an entrypoint.
    Check(EntrypointsCheckArgs),
}

#[derive(Subcommand, Debug)]
enum RunCommand {
    /// Execute a workflow example in-process and print the result.
    Local(LocalArgs),
    /// Serve a workflow example over HTTP using the Axum host.
    Serve(ServeArgs),
    /// Execute a FlowBundle from a bundle directory.
    Bundle(BundleArgs),
}

#[derive(Subcommand, Debug)]
enum BindingsCommand {
    /// Work with machine-generated bindings lockfiles.
    #[command(subcommand)]
    Lock(LockCommand),
}

#[derive(Subcommand, Debug)]
enum LockCommand {
    /// Generate a bindings.lock.json for a built-in example, package, or bundle.
    Generate(LockGenerateArgs),
}

#[derive(Args, Debug)]
struct LockGenerateArgs {
    /// Built-in example name (e.g. `s6_spill`).
    #[arg(long, conflicts_with_all = ["package", "bundle"])]
    example: Option<String>,
    /// Cargo package name to inspect using the flow-registry exporter harness.
    #[arg(long, conflicts_with_all = ["example", "bundle"])]
    package: Option<String>,
    /// Path to a FlowBundle directory containing manifest.json and artifacts.
    #[arg(long, conflicts_with_all = ["example", "package"])]
    bundle: Option<PathBuf>,
    /// Optional flow id to target when --package or --bundle contains multiple flows.
    #[arg(long)]
    flow: Option<String>,
    /// Bind capability providers for required `resource::*` domains.
    ///
    /// Examples:
    /// - `--bind resource::kv=memory`
    /// - `--bind kv=memory` (sugar)
    /// - `--bind resource::http::write=reqwest`
    #[arg(long = "bind")]
    bindings: Vec<String>,
    /// RFC3339 timestamp for `generated_at` (default is stable).
    #[arg(long, default_value = "1970-01-01T00:00:00Z")]
    generated_at: String,
    /// Output path for the generated bindings.lock.json.
    #[arg(long)]
    out: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
struct PackageMetadata {
    #[serde(default)]
    latticeflow: Option<LatticeflowMetadata>,
}

#[derive(Debug, Default, Deserialize)]
struct LatticeflowMetadata {
    #[serde(default)]
    flows: Option<Vec<String>>,
    #[serde(default)]
    default_flow: Option<String>,
}

#[derive(Args, Debug)]
struct LocalArgs {
    /// Built-in example to execute (e.g. `s6_spill`).
    #[arg(long, default_value = "s1_echo")]
    example: String,
    /// Bind capability providers for required `resource::*` domains.
    ///
    /// Examples:
    /// - `--bind resource::kv=memory`
    /// - `--bind kv=memory` (sugar)
    /// - `--bind resource::http::write=reqwest`
    #[arg(long = "bind")]
    bindings: Vec<String>,
    /// Path to a machine-generated `bindings.lock.json` file.
    #[arg(long)]
    bindings_lock: Option<PathBuf>,
    /// Inline JSON payload to feed the trigger input.
    #[arg(long)]
    payload: Option<String>,
    /// Path to a JSON file used as trigger payload (mutually exclusive with --payload).
    #[arg(long)]
    payload_file: Option<PathBuf>,
    /// Stream incremental results to stdout when supported by the workflow.
    #[arg(long)]
    stream: bool,
    /// Emit structured JSON containing the result and metrics summary.
    #[arg(long)]
    json: bool,
    /// Invoke the trigger multiple times against a single instance.
    #[arg(long, default_value_t = 1)]
    burst: usize,
    /// Checkpoint store implementation (fs or memory).
    #[arg(long, value_enum)]
    checkpoint_store: Option<CheckpointStoreKind>,
    /// Root directory for filesystem checkpoints (used with --checkpoint-store fs).
    #[arg(long)]
    checkpoint_dir: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum CheckpointStoreKind {
    Fs,
    Memory,
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Built-in example to serve (defaults to `s1_echo` when --bundle is omitted).
    #[arg(long, conflicts_with = "bundle")]
    example: Option<String>,
    /// Path to a FlowBundle directory to serve through the Axum host.
    #[arg(long, conflicts_with = "example")]
    bundle: Option<PathBuf>,
    /// Flow id to serve when --bundle contains multiple flows.
    #[arg(long, requires = "bundle")]
    flow: Option<String>,
    /// Trigger alias to serve from a bundle (defaults to first entrypoint).
    #[arg(long, requires = "bundle")]
    trigger_alias: Option<String>,
    /// Capture alias to serve from a bundle (defaults to selected entrypoint capture).
    #[arg(long, requires = "bundle")]
    capture_alias: Option<String>,
    /// Bind capability providers for required `resource::*` domains.
    #[arg(long = "bind")]
    bindings: Vec<String>,
    /// Path to a machine-generated `bindings.lock.json` file.
    #[arg(long)]
    bindings_lock: Option<PathBuf>,
    /// Address to bind (host:port).
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: SocketAddr,
}

#[derive(Args, Debug)]
struct BundleArgs {
    /// Path to a bundle directory containing manifest.json and artifacts.
    #[arg(long)]
    bundle: PathBuf,
    /// Flow id to execute when the bundle contains multiple flows.
    #[arg(long)]
    flow: Option<String>,
    /// Trigger alias to execute (defaults to first entrypoint).
    #[arg(long)]
    trigger_alias: Option<String>,
    /// Capture alias to execute (defaults to entrypoint capture).
    #[arg(long)]
    capture_alias: Option<String>,
    /// Bind capability providers for required `resource::*` domains.
    #[arg(long = "bind")]
    bindings: Vec<String>,
    /// Path to a machine-generated `bindings.lock.json` file.
    #[arg(long)]
    bindings_lock: Option<PathBuf>,
    /// Inline JSON payload to feed the trigger input.
    #[arg(long)]
    payload: Option<String>,
    /// Path to a JSON file used as trigger payload (mutually exclusive with --payload).
    #[arg(long)]
    payload_file: Option<PathBuf>,
    /// Stream incremental results to stdout when supported by the workflow.
    #[arg(long)]
    stream: bool,
    /// Emit structured JSON containing the result and metrics summary.
    #[arg(long)]
    json: bool,
    /// Checkpoint store implementation (fs or memory).
    #[arg(long, value_enum)]
    checkpoint_store: Option<CheckpointStoreKind>,
    /// Root directory for filesystem checkpoints (used with --checkpoint-store fs).
    #[arg(long)]
    checkpoint_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct GraphCheckArgs {
    /// Path to a Flow IR JSON document. Reads stdin when omitted.
    #[arg(long)]
    input: Option<PathBuf>,
    /// Write DOT graph to the provided path.
    #[arg(long)]
    dot: Option<PathBuf>,
    /// Print DOT graph to stdout.
    #[arg(long)]
    emit_dot: bool,
    /// Pretty-print Flow IR JSON after validation.
    #[arg(long)]
    pretty_json: bool,
    /// Emit structured JSON instead of human-readable text.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct EntrypointsCheckArgs {
    /// Path to a Flow IR JSON document. Reads stdin when omitted.
    #[arg(long)]
    flow: PathBuf,
    /// Trigger alias used to start execution.
    #[arg(long)]
    trigger_alias: String,
    /// Node alias whose output is captured as the result.
    #[arg(long)]
    capture_alias: String,
}

fn cli_metrics_snapshotter() -> &'static Snapshotter {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::set_global_recorder(recorder)
            .unwrap_or_else(|_| panic!("metrics recorder already installed"));
        snapshotter
    })
}

fn main() -> Result<()> {
    cli_metrics_snapshotter();
    let cli = Cli::parse();
    match cli.command {
        Command::Graph(GraphCommand::Check(args)) => run_graph_check(args),
        Command::Entrypoints(EntrypointsCommand::Check(args)) => run_entrypoints_check(args),
        Command::Run(RunCommand::Local(args)) => run_local(args),
        Command::Run(RunCommand::Serve(args)) => run_serve(args),
        Command::Run(RunCommand::Bundle(args)) => run_bundle(args),
        Command::Bindings(BindingsCommand::Lock(LockCommand::Generate(args))) => {
            run_bindings_lock_generate(args)
        }
        Command::Bundle(args) => bundle::run_bundle(args),
        Command::Resume(command) => resume::run_resume(command),
        Command::New(args) => scaffold::run_new(args),
    }
}

fn run_entrypoints_check(args: EntrypointsCheckArgs) -> Result<()> {
    let payload =
        fs::read(&args.flow).with_context(|| format!("failed to read {}", args.flow.display()))?;
    let flow: dag_core::FlowIR =
        serde_json::from_slice(&payload).context("input is not valid Flow IR JSON")?;

    let validated = match kernel_plan::validate(&flow) {
        Ok(validated) => validated,
        Err(diags) => {
            eprintln!(
                "✗ graph validation failed with {} diagnostic(s):",
                diags.len()
            );
            for diag in &diags {
                eprintln!("{}", format_text_diagnostic(diag));
            }
            return Err(anyhow!("graph validation failed"));
        }
    };

    let trigger = validated
        .flow()
        .node(args.trigger_alias.as_str())
        .ok_or_else(|| anyhow!("unknown trigger_alias `{}`", args.trigger_alias))?;

    if trigger.kind != dag_core::NodeKind::Trigger {
        return Err(anyhow!(
            "trigger_alias `{}` refers to non-trigger node kind {:?}",
            args.trigger_alias,
            trigger.kind
        ));
    }

    validated
        .flow()
        .node(args.capture_alias.as_str())
        .ok_or_else(|| anyhow!("unknown capture_alias `{}`", args.capture_alias))?;

    println!("OK");
    Ok(())
}

fn run_graph_check(args: GraphCheckArgs) -> Result<()> {
    if args.json && (args.emit_dot || args.dot.is_some() || args.pretty_json) {
        return Err(anyhow!(
            "--json cannot be combined with --emit-dot, --dot, or --pretty-json"
        ));
    }

    let payload = match args.input {
        Some(path) => {
            fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?
        }
        None => {
            let mut buf = Vec::new();
            io::stdin()
                .read_to_end(&mut buf)
                .context("failed to read Flow IR from stdin")?;
            buf
        }
    };

    let flow: dag_core::FlowIR =
        serde_json::from_slice(&payload).context("input is not valid Flow IR JSON")?;

    let node_count = flow.nodes.len();
    let edge_count = flow.edges.len();

    match validate(&flow) {
        Ok(_) => {
            if args.json {
                let response = GraphCheckResponse {
                    status: GraphStatus::Ok,
                    node_count,
                    edge_count,
                    diagnostics: Vec::new(),
                };
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("✓ graph is valid ({node_count} nodes, {edge_count} edges)");
            }
        }
        Err(diags) => {
            if args.json {
                let response = GraphCheckResponse {
                    status: GraphStatus::Error,
                    node_count,
                    edge_count,
                    diagnostics: diags.iter().map(DiagnosticPayload::from).collect(),
                };
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                eprintln!(
                    "✗ graph validation failed with {} diagnostic(s):",
                    diags.len()
                );
                for diag in &diags {
                    eprintln!("{}", format_text_diagnostic(diag));
                }
            }
            return Err(anyhow!("graph validation failed"));
        }
    }

    if args.json {
        return Ok(());
    }

    if args.pretty_json {
        let json = to_json_value(&flow);
        println!("{}", serde_json::to_string_pretty(&json)?);
    }

    if args.emit_dot {
        println!("{}", to_dot(&flow));
    }

    if let Some(path) = args.dot {
        fs::write(&path, to_dot(&flow))
            .with_context(|| format!("failed to write DOT to {}", path.display()))?;
        println!("DOT graph written to {}", path.display());
    }

    Ok(())
}

fn format_text_diagnostic(diag: &Diagnostic) -> String {
    let severity = format_severity(diag.code.default_severity);
    let mut output = format!(
        "  [{}] {}({}): {}",
        diag.code.code, severity, diag.code.subsystem, diag.message
    );
    output.push('\n');
    output.push_str(&format!("      summary: {}", diag.code.summary));
    if let Some(location) = &diag.location {
        output.push('\n');
        output.push_str(&format!("      location: {location}"));
    }
    output
}

fn format_severity(severity: Severity) -> &'static str {
    match severity {
        Severity::Error => "error",
        Severity::Warn => "warn",
        Severity::Info => "info",
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum GraphStatus {
    Ok,
    Error,
}

#[derive(Debug, Serialize)]
struct GraphCheckResponse {
    status: GraphStatus,
    node_count: usize,
    edge_count: usize,
    diagnostics: Vec<DiagnosticPayload>,
}

#[derive(Debug, Serialize)]
struct DiagnosticPayload {
    code: String,
    severity: Severity,
    subsystem: String,
    summary: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
}

impl From<&Diagnostic> for DiagnosticPayload {
    fn from(diag: &Diagnostic) -> Self {
        Self {
            code: diag.code.code.to_string(),
            severity: diag.code.default_severity,
            subsystem: diag.code.subsystem.to_string(),
            summary: diag.code.summary.to_string(),
            message: diag.message.clone(),
            location: diag.location.clone(),
        }
    }
}

struct ExampleHandle {
    executor: FlowExecutor,
    ir: Arc<ValidatedIR>,
    trigger_alias: String,
    capture_alias: String,
    deadline: Option<Duration>,
    route_path: String,
    method: Method,
    is_streaming: bool,
    environment_plugins: Vec<Arc<dyn EnvironmentPlugin>>,
}

impl ExampleHandle {}

struct RunOutcome {
    result: Option<JsonValue>,
    stream_events: Vec<JsonValue>,
    stream_count: usize,
}

#[derive(Serialize)]
struct RunSummary {
    duration_ms: f64,
    nodes: Vec<NodeSummary>,
    errors: Vec<NodeErrorSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_events: Option<usize>,
}

#[derive(Serialize)]
struct NodeSummary {
    alias: String,
    invocations: usize,
    avg_ms: f64,
}

#[derive(Serialize)]
struct NodeErrorSummary {
    alias: String,
    error_kind: String,
    count: u64,
}

#[derive(Serialize)]
struct LocalJsonOutput {
    example: String,
    result: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_events: Option<Vec<JsonValue>>,
    summary: RunSummary,
}

fn run_local(args: LocalArgs) -> Result<()> {
    let example_name = args.example.clone();
    let stream_mode = args.stream;
    let json_mode = args.json;
    let burst = args.burst.max(1);
    let payload = parse_payload(&args)?;
    if args.bindings_lock.is_some() && !args.bindings.is_empty() {
        return Err(anyhow!("--bindings-lock cannot be combined with --bind"));
    }
    let handle = load_example(&args.example)?;

    if handle.is_streaming && !stream_mode {
        return Err(anyhow!(
            "example `{}` produces streaming output; re-run with --stream to consume events",
            example_name
        ));
    }

    let ExampleHandle {
        executor,
        ir,
        trigger_alias,
        capture_alias,
        deadline,
        environment_plugins,
        ..
    } = handle;

    let executor = if burst > 1 {
        executor.with_capture_capacity(burst)
    } else {
        executor
    };

    let mut resources = if let Some(lock_path) = &args.bindings_lock {
        resource_bag_from_bindings_lock(lock_path.as_path(), ir.flow().id.as_str())?
    } else {
        resource_bag_from_bindings(&args.bindings)?
    };

    let checkpoint_override = args.checkpoint_store.is_some() || args.checkpoint_dir.is_some();
    let has_checkpoint_store = resources.checkpoint_store().is_some();
    if checkpoint_override {
        let store_kind = args.checkpoint_store.unwrap_or(CheckpointStoreKind::Fs);
        if store_kind != CheckpointStoreKind::Fs && args.checkpoint_dir.is_some() {
            return Err(anyhow!(
                "--checkpoint-dir can only be used with --checkpoint-store fs"
            ));
        }
        resources = attach_checkpoint_store(resources, store_kind, args.checkpoint_dir.as_deref());
    } else if !has_checkpoint_store {
        resources = attach_checkpoint_store(resources, CheckpointStoreKind::Fs, None);
    } else if resources.max_durability_mode() == DurabilityMode::Off {
        resources = resources.with_max_durability_mode(DurabilityMode::Partial);
    }

    let flow_name = ir.flow().name.clone();
    let capture_alias_str = capture_alias.to_string();

    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialise Tokio runtime")?;

    let snapshotter = cli_metrics_snapshotter();
    let _ = snapshotter.snapshot();
    let start = Instant::now();

    let outcome: RunOutcome = runtime.block_on(async move {
        let host_runtime = HostRuntime::with_plugins(executor, ir.clone(), environment_plugins)
            .with_resource_bag(resources);

        if burst == 1 {
            let invocation =
                Invocation::new(trigger_alias.as_str(), capture_alias.as_str(), payload)
                    .with_deadline(deadline);

            let execution = host_runtime
                .execute(invocation)
                .await
                .map_err(|err| match &err {
                    kernel_exec::ExecutionError::MissingCapabilities { hints } => {
                        anyhow!("[CAP101] missing required capabilities: {hints:?}")
                    }
                    _ => anyhow::Error::new(err),
                })?;

            let result: Result<RunOutcome> = match execution {
                ExecutionResult::Value(value) => Ok(RunOutcome {
                    result: Some(value),
                    stream_events: Vec::new(),
                    stream_count: 0,
                }),
                ExecutionResult::Halt { alias, payload } => Ok(RunOutcome {
                    result: Some(json!({
                        "halted": true,
                        "node": alias,
                        "payload": payload,
                    })),
                    stream_events: Vec::new(),
                    stream_count: 0,
                }),
                ExecutionResult::Stream(mut stream) => {
                    let mut events = Vec::new();
                    let mut count = 0usize;
                    while let Some(event) = stream.next().await {
                        let payload = event.map_err(anyhow::Error::from)?;
                        if json_mode {
                            events.push(payload.clone());
                        } else {
                            println!("{}", serde_json::to_string(&payload)?);
                        }
                        count += 1;
                    }
                    Ok(RunOutcome {
                        result: None,
                        stream_events: events,
                        stream_count: count,
                    })
                }
            };
            return result;
        }

        if stream_mode {
            return Err(anyhow!("--burst is not supported with streaming examples"));
        }

        let mut instance = host_runtime
            .executor()
            .instantiate(ir.as_ref(), capture_alias.as_str())
            .map_err(anyhow::Error::new)?;

        for idx in 0..burst {
            let mut burst_payload = payload.clone();
            if let JsonValue::Object(map) = &mut burst_payload {
                map.insert("lf_burst_index".to_string(), JsonValue::from(idx as u64));
            }
            instance
                .send(trigger_alias.as_str(), burst_payload)
                .await
                .map_err(anyhow::Error::new)?;
        }

        let mut results = Vec::with_capacity(burst);
        for _ in 0..burst {
            match instance.next().await {
                Some(Ok(kernel_exec::CaptureResult::Value(value))) => {
                    results.push(value);
                }
                Some(Ok(kernel_exec::CaptureResult::Halt { .. })) => {
                    return Err(anyhow!("halted execution not supported in burst mode"));
                }
                Some(Ok(kernel_exec::CaptureResult::Stream(_))) => {
                    return Err(anyhow!("streaming capture not supported in burst mode"));
                }
                Some(Err(err)) => return Err(anyhow::Error::new(err)),
                None => return Err(anyhow!("capture channel closed before completion")),
            }
        }

        instance.shutdown().await.map_err(anyhow::Error::new)?;

        Ok(RunOutcome {
            result: Some(JsonValue::Array(results)),
            stream_events: Vec::new(),
            stream_count: 0,
        })
    })?;

    let duration = start.elapsed();
    let snapshot = snapshotter.snapshot();
    let summary = build_run_summary(duration, snapshot, outcome.stream_count);

    record_cli_metrics(&flow_name, &example_name, &capture_alias_str, &summary);

    if json_mode {
        let output = LocalJsonOutput {
            example: example_name,
            result: outcome.result,
            stream_events: if outcome.stream_events.is_empty() {
                None
            } else {
                Some(outcome.stream_events)
            },
            summary,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        if let Some(result) = outcome.result {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        print_text_summary(&summary);
    }

    Ok(())
}

#[cfg(not(feature = "host-wasmtime"))]
fn run_bundle(_args: BundleArgs) -> Result<()> {
    Err(anyhow!(
        "`flows run bundle` requires the wasmtime host; rebuild flows-cli with the `host-wasmtime` feature"
    ))
}

#[cfg(feature = "host-wasmtime")]
fn run_bundle(args: BundleArgs) -> Result<()> {
    if args.bindings_lock.is_some() && !args.bindings.is_empty() {
        return Err(anyhow!("--bindings-lock cannot be combined with --bind"));
    }

    let payload = parse_payload_sources(args.payload.as_deref(), args.payload_file.as_deref())?;
    let flow_id_for_lock = if args.bindings_lock.is_some() {
        Some(resolve_bundle_flow_id(&args.bundle, args.flow.as_deref())?)
    } else {
        None
    };

    let mut resources = if let Some(lock_path) = &args.bindings_lock {
        let flow_id = flow_id_for_lock
            .as_deref()
            .context("missing flow id for bindings lock")?;
        resource_bag_from_bindings_lock(lock_path.as_path(), flow_id)?
    } else {
        resource_bag_from_bindings(&args.bindings)?
    };

    let checkpoint_override = args.checkpoint_store.is_some() || args.checkpoint_dir.is_some();
    let has_checkpoint_store = resources.checkpoint_store().is_some();
    if checkpoint_override {
        let store_kind = args.checkpoint_store.unwrap_or(CheckpointStoreKind::Fs);
        if store_kind != CheckpointStoreKind::Fs && args.checkpoint_dir.is_some() {
            return Err(anyhow!(
                "--checkpoint-dir can only be used with --checkpoint-store fs"
            ));
        }
        resources = attach_checkpoint_store(resources, store_kind, args.checkpoint_dir.as_deref());
    } else if !has_checkpoint_store {
        resources = attach_checkpoint_store(resources, CheckpointStoreKind::Fs, None);
    } else if resources.max_durability_mode() == DurabilityMode::Off {
        resources = resources.with_max_durability_mode(DurabilityMode::Partial);
    }

    let bundle = load_flow_bundle(
        &args.bundle,
        ExecPolicy::Wasm,
        args.flow.as_deref(),
        Arc::new(resources.clone()),
    )?;
    let entrypoint = select_bundle_entrypoint(
        &bundle,
        args.trigger_alias.as_deref(),
        args.capture_alias.as_deref(),
    )?;
    let trigger_alias = entrypoint.trigger_alias.clone();
    let capture_alias = entrypoint.capture_alias.clone();
    let deadline = entrypoint.deadline;

    let flow_name = bundle.validated_ir.flow().name.clone();
    let capture_alias_str = capture_alias.clone();
    let flow_label = bundle.validated_ir.flow().id.as_str().to_string();
    let stream_mode = args.stream;
    let json_mode = args.json;

    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialise Tokio runtime")?;

    let snapshotter = cli_metrics_snapshotter();
    let _ = snapshotter.snapshot();
    let start = Instant::now();

    let outcome: RunOutcome = runtime.block_on(async move {
        let host_runtime = HostRuntime::with_plugins(
            bundle.executor(),
            Arc::new(bundle.validated_ir.clone()),
            bundle.environment_plugins,
        )
        .with_resource_bag(resources);

        let invocation = Invocation::new(trigger_alias.as_str(), capture_alias.as_str(), payload)
            .with_deadline(deadline);

        let execution = host_runtime
            .execute(invocation)
            .await
            .map_err(|err| match &err {
                kernel_exec::ExecutionError::MissingCapabilities { hints } => {
                    anyhow!("[CAP101] missing required capabilities: {hints:?}")
                }
                _ => anyhow::Error::new(err),
            })?;

        match execution {
            ExecutionResult::Value(value) => Ok(RunOutcome {
                result: Some(value),
                stream_events: Vec::new(),
                stream_count: 0,
            }),
            ExecutionResult::Halt { alias, payload } => Ok(RunOutcome {
                result: Some(json!({
                    "halted": true,
                    "node": alias,
                    "payload": payload,
                })),
                stream_events: Vec::new(),
                stream_count: 0,
            }),
            ExecutionResult::Stream(mut stream) => {
                if !stream_mode {
                    return Err(anyhow!(
                        "bundle execution returned a stream; re-run with --stream"
                    ));
                }
                let mut events = Vec::new();
                let mut count = 0usize;
                while let Some(event) = stream.next().await {
                    let payload = event.map_err(anyhow::Error::from)?;
                    if json_mode {
                        events.push(payload.clone());
                    } else {
                        println!("{}", serde_json::to_string(&payload)?);
                    }
                    count += 1;
                }
                Ok(RunOutcome {
                    result: None,
                    stream_events: events,
                    stream_count: count,
                })
            }
        }
    })?;

    let duration = start.elapsed();
    let snapshot = snapshotter.snapshot();
    let summary = build_run_summary(duration, snapshot, outcome.stream_count);

    record_cli_metrics(&flow_name, &flow_label, &capture_alias_str, &summary);

    if json_mode {
        let output = LocalJsonOutput {
            example: flow_label,
            result: outcome.result,
            stream_events: if outcome.stream_events.is_empty() {
                None
            } else {
                Some(outcome.stream_events)
            },
            summary,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        if let Some(result) = outcome.result {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        print_text_summary(&summary);
    }

    Ok(())
}

fn resolve_bundle_flow_id(bundle_dir: &Path, flow_id: Option<&str>) -> Result<String> {
    let manifest_path = bundle_dir.join("manifest.json");
    let bytes = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest: flow_bundle::Manifest = serde_json::from_slice(&bytes)
        .context("manifest.json is not valid bundle manifest JSON")?;
    flow_bundle::validate_manifest(&manifest)?;

    if let Some(id) = flow_id {
        if manifest.flows.iter().any(|flow| flow.id == id) {
            return Ok(id.to_string());
        }
        return Err(anyhow!("bundle missing flow id {id}"));
    }

    if let Some(default_flow) = manifest.default_flow.as_ref() {
        if manifest.flows.iter().any(|flow| flow.id == *default_flow) {
            return Ok(default_flow.clone());
        }
    }

    manifest
        .flows
        .first()
        .map(|flow| flow.id.clone())
        .context("bundle has no flow entries")
}

fn select_bundle_entrypoint<'a>(
    bundle: &'a host_inproc::FlowBundle,
    trigger_alias: Option<&str>,
    capture_alias: Option<&str>,
) -> Result<&'a host_inproc::FlowEntrypoint> {
    if let Some(trigger_alias) = trigger_alias {
        if let Some(capture_alias) = capture_alias {
            return bundle
                .entrypoints
                .iter()
                .find(|entry| {
                    entry.trigger_alias == trigger_alias && entry.capture_alias == capture_alias
                })
                .with_context(|| {
                    format!(
                        "no entrypoint matching trigger {trigger_alias} and capture {capture_alias}"
                    )
                });
        }
        return bundle
            .entrypoints
            .iter()
            .find(|entry| entry.trigger_alias == trigger_alias)
            .with_context(|| format!("no entrypoint matching trigger {trigger_alias}"));
    }

    if let Some(capture_alias) = capture_alias {
        return bundle
            .entrypoints
            .iter()
            .find(|entry| entry.capture_alias == capture_alias)
            .with_context(|| format!("no entrypoint matching capture {capture_alias}"));
    }

    bundle
        .entrypoints
        .first()
        .context("bundle has no entrypoints")
}

fn run_serve(args: ServeArgs) -> Result<()> {
    if args.bindings_lock.is_some() && !args.bindings.is_empty() {
        return Err(anyhow!("--bindings-lock cannot be combined with --bind"));
    }

    let mut resources = if let Some(lock_path) = args.bindings_lock.as_ref() {
        if let Some(bundle_dir) = args.bundle.as_ref() {
            let flow_id = resolve_bundle_flow_id(bundle_dir, args.flow.as_deref())?;
            resource_bag_from_bindings_lock(lock_path.as_path(), &flow_id)?
        } else {
            let example_name = args.example.as_deref().unwrap_or("s1_echo");
            let example = load_example(example_name)?;
            resource_bag_from_bindings_lock(lock_path.as_path(), example.ir.flow().id.as_str())?
        }
    } else {
        resource_bag_from_bindings(&args.bindings)?
    };

    if resources.checkpoint_store().is_none() {
        resources = attach_checkpoint_store(resources, CheckpointStoreKind::Fs, None);
    } else if resources.max_durability_mode() == DurabilityMode::Off {
        resources = resources.with_max_durability_mode(DurabilityMode::Partial);
    }

    let serve_label: String;
    let handle = if let Some(bundle_dir) = args.bundle.as_ref() {
        #[cfg(not(feature = "host-wasmtime"))]
        {
            let _ = bundle_dir;
            return Err(anyhow!(
                "`flows run serve --bundle` requires the wasmtime host; rebuild flows-cli with the `host-wasmtime` feature"
            ));
        }
        #[cfg(feature = "host-wasmtime")]
        {
            let bundle = load_flow_bundle(
                bundle_dir,
                ExecPolicy::Wasm,
                args.flow.as_deref(),
                Arc::new(resources.clone()),
            )?;
            let handle = example_from_bundle_entrypoint(
                bundle,
                false,
                args.trigger_alias.as_deref(),
                args.capture_alias.as_deref(),
            )?;
            serve_label = format!("bundle `{}`", bundle_dir.display());
            handle
        }
    } else {
        let example_name = args.example.as_deref().unwrap_or("s1_echo");
        serve_label = format!("example `{example_name}`");
        load_example(example_name)?
    };

    let ExampleHandle {
        executor,
        ir,
        trigger_alias,
        capture_alias,
        deadline,
        route_path,
        method,
        environment_plugins,
        ..
    } = handle;

    let addr = args.addr;
    let runtime = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to initialise Tokio runtime")?;

    runtime.block_on(async move {
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("failed to bind {addr}"))?;

        let mut config = RouteConfig::new(route_path.as_str())
            .with_trigger_alias(trigger_alias.as_str())
            .with_capture_alias(capture_alias.as_str())
            .with_resources(resources);
        config = config.with_method(method);
        if let Some(deadline) = deadline {
            config = config.with_deadline(deadline);
        }
        for plugin in environment_plugins {
            config = config.with_environment_plugin(plugin);
        }

        let host = HostHandle::try_new(executor, ir, config).map_err(anyhow::Error::new)?;
        let local_addr = listener
            .local_addr()
            .context("failed to determine bound address")?;
        println!("Serving {serve_label} on http://{local_addr}{route_path} (Ctrl+C to stop)");

        let shutdown = async {
            let _ = signal::ctrl_c().await;
            println!("signal received, shutting down server…");
        };

        axum::serve(listener, host.into_service())
            .with_graceful_shutdown(shutdown)
            .await
            .context("Axum server terminated unexpectedly")?;
        println!("Server stopped cleanly.");
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

#[derive(Default)]
struct NodeStats {
    invocations: usize,
    total_ms: f64,
}

fn build_run_summary(
    duration: Duration,
    snapshot: metrics_util::debugging::Snapshot,
    stream_count: usize,
) -> RunSummary {
    let mut nodes: HashMap<String, NodeStats> = HashMap::new();
    let mut errors: HashMap<(String, String), u64> = HashMap::new();

    for (key, _unit, _desc, value) in snapshot.into_vec() {
        let metric = key.key();
        let name = metric.name();
        match (name, value) {
            ("lattice.executor.node_latency_ms", DebugValue::Histogram(values)) => {
                let mut alias: Option<String> = None;
                for label in metric.labels() {
                    if label.key() == "node" {
                        alias = Some(label.value().to_string());
                        break;
                    }
                }
                if let Some(alias) = alias {
                    let entry = nodes.entry(alias).or_default();
                    entry.invocations += values.len();
                    entry.total_ms += values.into_iter().map(|val| val.into_inner()).sum::<f64>();
                }
            }
            ("lattice.executor.node_errors_total", DebugValue::Counter(count)) => {
                let mut alias: Option<String> = None;
                let mut error_kind: Option<String> = None;
                for label in metric.labels() {
                    match label.key() {
                        "node" => alias = Some(label.value().to_string()),
                        "error_kind" => error_kind = Some(label.value().to_string()),
                        _ => {}
                    }
                }
                if let (Some(alias), Some(kind)) = (alias, error_kind) {
                    *errors.entry((alias, kind)).or_insert(0) += count;
                }
            }
            _ => {}
        }
    }

    let mut nodes_summary: Vec<NodeSummary> = nodes
        .into_iter()
        .map(|(alias, stats)| {
            let avg = if stats.invocations == 0 {
                0.0
            } else {
                stats.total_ms / stats.invocations as f64
            };
            NodeSummary {
                alias,
                invocations: stats.invocations,
                avg_ms: avg,
            }
        })
        .collect();
    nodes_summary.sort_by(|a, b| a.alias.cmp(&b.alias));

    let mut errors_summary: Vec<NodeErrorSummary> = errors
        .into_iter()
        .map(|((alias, kind), count)| NodeErrorSummary {
            alias,
            error_kind: kind,
            count,
        })
        .collect();
    errors_summary.sort_by(|a, b| a.alias.cmp(&b.alias).then(a.error_kind.cmp(&b.error_kind)));

    RunSummary {
        duration_ms: duration.as_secs_f64() * 1_000.0,
        nodes: nodes_summary,
        errors: errors_summary,
        stream_events: if stream_count > 0 {
            Some(stream_count)
        } else {
            None
        },
    }
}

fn record_cli_metrics(
    flow_name: &str,
    example_name: &str,
    capture_alias: &str,
    summary: &RunSummary,
) {
    let flow_label = flow_name.to_string();
    let example_label = example_name.to_string();
    metrics::histogram!(
        "lattice.cli.run_duration_ms",
        "flow" => flow_label.clone(),
        "example" => example_label
    )
    .record(summary.duration_ms);

    for node in &summary.nodes {
        metrics::counter!(
            "lattice.cli.nodes_succeeded_total",
            "flow" => flow_label.clone(),
            "node" => node.alias.clone()
        )
        .increment(node.invocations as u64);
    }

    for error in &summary.errors {
        metrics::counter!(
            "lattice.cli.nodes_failed_total",
            "flow" => flow_label.clone(),
            "node" => error.alias.clone(),
            "error_kind" => error.error_kind.clone()
        )
        .increment(error.count);
    }

    if let Some(events) = summary.stream_events {
        metrics::counter!(
            "lattice.cli.captures_emitted_total",
            "flow" => flow_label,
            "node" => capture_alias.to_string(),
            "capture" => capture_alias.to_string()
        )
        .increment(events as u64);
    }
}

fn print_text_summary(summary: &RunSummary) {
    eprintln!("--- Run Summary ---");
    eprintln!("  duration_ms: {:.2}", summary.duration_ms);
    if let Some(events) = summary.stream_events {
        eprintln!("  stream_events: {}", events);
    }
    if summary.nodes.is_empty() {
        eprintln!("  nodes: (no execution data)");
    } else {
        eprintln!("  nodes:");
        for node in &summary.nodes {
            eprintln!(
                "    {}: {} call(s), avg {:.2} ms",
                node.alias, node.invocations, node.avg_ms
            );
        }
    }
    if !summary.errors.is_empty() {
        eprintln!("  errors:");
        for error in &summary.errors {
            eprintln!(
                "    {} [{}]: {} occurrence(s)",
                error.alias, error.error_kind, error.count
            );
        }
    }
}

fn normalize_binding_key(raw: &str) -> String {
    if raw.starts_with("resource::") {
        return raw.to_string();
    }

    match raw {
        "http" => "resource::http",
        "http_read" => "resource::http::read",
        "http_write" => "resource::http::write",
        "kv" => "resource::kv",
        "kv_read" => "resource::kv::read",
        "kv_write" => "resource::kv::write",
        "blob" => "resource::blob",
        "blob_read" => "resource::blob::read",
        "blob_write" => "resource::blob::write",
        "queue" => "resource::queue",
        "queue_publish" => "resource::queue::publish",
        "queue_consume" => "resource::queue::consume",
        "dedupe" => "resource::dedupe",
        "dedupe_write" => "resource::dedupe::write",
        "db" => "resource::db",
        "db_read" => "resource::db::read",
        "db_write" => "resource::db::write",
        other => other,
    }
    .to_string()
}

#[derive(Default)]
struct MemoryCheckpointStore {
    records: Mutex<HashMap<String, CheckpointRecord>>,
}

impl Capability for MemoryCheckpointStore {
    fn name(&self) -> &'static str {
        "checkpoint_store.memory"
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl CheckpointStore for MemoryCheckpointStore {
    async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
        let handle = CheckpointHandle {
            checkpoint_id: record.checkpoint_id.clone(),
            flow_id: record.flow_id.clone(),
            run_id: record.run_id.clone(),
        };
        let mut records = self.records.lock().expect("checkpoint store lock");
        records.insert(record.checkpoint_id.clone(), record);
        Ok(handle)
    }

    async fn get(&self, handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError> {
        let records = self.records.lock().expect("checkpoint store lock");
        records
            .get(&handle.checkpoint_id)
            .cloned()
            .ok_or(CheckpointError::NotFound)
    }

    async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
        let mut records = self.records.lock().expect("checkpoint store lock");
        records.remove(&handle.checkpoint_id);
        Ok(())
    }

    async fn lease(
        &self,
        handle: &CheckpointHandle,
        ttl: Duration,
    ) -> Result<Lease, CheckpointError> {
        let records = self.records.lock().expect("checkpoint store lock");
        if !records.contains_key(&handle.checkpoint_id) {
            return Err(CheckpointError::NotFound);
        }
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let expires_at_ms = now_ms.saturating_add(ttl.as_millis() as u64);
        Ok(Lease {
            lease_id: format!("lease-{}-{expires_at_ms}", handle.checkpoint_id),
            expires_at_ms,
        })
    }

    async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
        Ok(())
    }

    async fn list(
        &self,
        filter: CheckpointFilter,
    ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
        let records = self.records.lock().expect("checkpoint store lock");
        let mut handles = Vec::new();
        for record in records.values() {
            if let Some(flow_id) = &filter.flow_id {
                if flow_id != &record.flow_id {
                    continue;
                }
            }
            if let Some(run_id) = &filter.run_id {
                if run_id != &record.run_id {
                    continue;
                }
            }
            handles.push(CheckpointHandle {
                checkpoint_id: record.checkpoint_id.clone(),
                flow_id: record.flow_id.clone(),
                run_id: record.run_id.clone(),
            });
        }
        Ok(handles)
    }
}

fn resource_bag_from_bindings(bindings: &[String]) -> Result<ResourceBag> {
    let mut bag = ResourceBag::new();

    for binding in bindings {
        let (raw_key, raw_value) = binding.split_once('=').ok_or_else(|| {
            anyhow!("invalid --bind `{binding}`; expected `<resource::hint>=<provider>`")
        })?;
        let key = normalize_binding_key(raw_key.trim());
        let value = raw_value.trim();

        match (key.as_str(), value) {
            ("resource::kv" | "resource::kv::read" | "resource::kv::write", "memory") => {
                bag = bag.with_kv(Arc::new(capabilities::kv::MemoryKv::new()));
            }
            ("resource::blob" | "resource::blob::read" | "resource::blob::write", "memory") => {
                bag = bag.with_blob(Arc::new(capabilities::blob::MemoryBlobStore::new()));
            }
            ("durability::checkpoint_store", "memory") => {
                bag = attach_checkpoint_store(bag, CheckpointStoreKind::Memory, None);
            }
            ("resource::http", "reqwest") => {
                let client = Arc::new(cap_http_reqwest::ReqwestHttpClient::default());
                bag = bag.with_http_read(Arc::clone(&client));
                bag = bag.with_http_write(client);
            }
            ("resource::http::read", "reqwest") => {
                bag = bag.with_http_read(Arc::new(cap_http_reqwest::ReqwestHttpClient::default()));
            }
            ("resource::http::write", "reqwest") => {
                bag = bag.with_http_write(Arc::new(cap_http_reqwest::ReqwestHttpClient::default()));
            }
            _ => {
                return Err(anyhow!(
                    "unsupported binding `{binding}`; supported: resource::kv=memory, resource::blob=memory, resource::http::read=reqwest, resource::http::write=reqwest, durability::checkpoint_store=memory"
                ));
            }
        }
    }

    Ok(bag)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BindingsLock {
    version: u32,
    generated_at: String,
    content_hash: String,
    #[serde(default)]
    instances: BTreeMap<String, LockInstance>,
    #[serde(default)]
    flows: BTreeMap<String, LockFlow>,
    #[serde(default)]
    connector_handles: BTreeMap<String, ConnectorHandleInstance>,
    #[serde(default)]
    connector_connections: BTreeMap<String, ConnectorConnectionInstance>,
    #[serde(default)]
    connector_bindings: BTreeMap<String, ConnectorFlowBindings>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LockInstance {
    provider_kind: String,
    #[serde(default)]
    provides: Vec<String>,
    #[serde(default)]
    connect: JsonValue,
    #[serde(default)]
    config: JsonValue,
    #[serde(default)]
    isolation: Vec<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LockFlow {
    #[serde(rename = "use", default)]
    use_map: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorHandleInstance {
    provider_kind: String,
    handle_kind: String,
    #[serde(default)]
    connect: JsonValue,
    #[serde(default)]
    config: JsonValue,
    #[serde(default)]
    grants: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConnectorConnectionInstance {
    connector_id: String,
    #[serde(default)]
    roles: BTreeMap<String, String>,
    #[serde(default = "default_json_object")]
    config: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ConnectorFlowBindings {
    #[serde(default)]
    defaults: BTreeMap<String, String>,
    #[serde(default)]
    nodes: BTreeMap<String, String>,
}

fn default_json_object() -> JsonValue {
    JsonValue::Object(serde_json::Map::new())
}

fn canonical_json(value: &JsonValue) -> String {
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            serde_json::to_string(value).expect("json")
        }
        JsonValue::Array(values) => {
            let mut out = String::from("[");
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json(item));
            }
            out.push(']');
            out
        }
        JsonValue::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();

            let mut out = String::from("{");
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("json key"));
                out.push(':');
                out.push_str(&canonical_json(map.get(key).expect("key present")));
            }
            out.push('}');
            out
        }
    }
}

fn sha256_hex(payload: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    let digest = hasher.finalize();
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn compute_lock_content_hash(lock_json: &JsonValue) -> Result<String> {
    let mut copy = lock_json.clone();
    let Some(obj) = copy.as_object_mut() else {
        return Err(anyhow!("bindings.lock must be a JSON object"));
    };
    obj.remove("content_hash");

    Ok(sha256_hex(&canonical_json(&copy)))
}

fn required_resource_hints(flow: &dag_core::FlowIR) -> BTreeSet<String> {
    let mut required = BTreeSet::new();
    for node in &flow.nodes {
        for hint in &node.effect_hints {
            if hint.starts_with("resource::") {
                required.insert(hint.clone());
            }
        }
    }
    required
}

fn load_manifest_from_dir(dir: &Path) -> Result<Manifest> {
    let path = dir.join("manifest.json");
    let bytes = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("{} is not valid manifest JSON", path.display()))
}

fn load_flow_irs_from_manifest(
    manifest: &Manifest,
    artifact_root: &Path,
    selected_flow: Option<&str>,
) -> Result<BTreeMap<String, FlowIR>> {
    let mut flows = BTreeMap::new();

    for entry in &manifest.flows {
        if let Some(selected) = selected_flow {
            if entry.id != selected {
                continue;
            }
        }

        let flow_ir = entry.flow_ir.as_ref().ok_or_else(|| {
            anyhow!(
                "manifest flow `{}` is missing flow_ir; cannot generate bindings.lock",
                entry.id
            )
        })?;
        let ir_path = artifact_root.join(&flow_ir.artifact);
        let bytes =
            fs::read(&ir_path).with_context(|| format!("failed to read {}", ir_path.display()))?;
        let flow: FlowIR = serde_json::from_slice(&bytes)
            .with_context(|| format!("{} is not valid Flow IR JSON", ir_path.display()))?;
        flows.insert(flow.id.as_str().to_string(), flow);
    }

    if let Some(selected) = selected_flow {
        if flows.is_empty() {
            return Err(anyhow!(
                "manifest does not define flow `{selected}` for bindings lock generation"
            ));
        }
    }

    if flows.is_empty() {
        return Err(anyhow!(
            "manifest does not define any flows for bindings lock generation"
        ));
    }

    Ok(flows)
}

fn resolve_package_export_config(package_name: &str) -> Result<(PathBuf, HarnessConfig)> {
    let metadata = MetadataCommand::new()
        .no_deps()
        .exec()
        .context("failed to load cargo metadata")?;
    let package = metadata
        .packages
        .iter()
        .find(|candidate| candidate.name == package_name)
        .ok_or_else(|| anyhow!("package not found in workspace: {package_name}"))?;
    let manifest_dir = package
        .manifest_path
        .parent()
        .ok_or_else(|| anyhow!("missing manifest path for {package_name}"))?;

    let metadata: PackageMetadata =
        serde_json::from_value(package.metadata.clone()).unwrap_or_default();
    let latticeflow = metadata.latticeflow.unwrap_or_default();
    let config = HarnessConfig {
        default_flow: latticeflow.default_flow,
        flows: latticeflow.flows,
    };

    Ok((manifest_dir.to_path_buf().into(), config))
}

fn load_flow_irs_from_package(
    package_name: &str,
    selected_flow: Option<&str>,
) -> Result<BTreeMap<String, FlowIR>> {
    let (package_dir, config) = resolve_package_export_config(package_name)?;
    let export_temp = tempdir().context("failed to create exporter temp dir")?;
    let export_crate_dir = export_temp.path().join("exporter");
    let export_out_dir = export_temp.path().join("bundle");
    let export_manifest_path = exporters::harness::write_exporter_crate(
        &export_crate_dir,
        &package_dir,
        package_name,
        &config,
    )?;

    let status = std::process::Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg(&export_manifest_path)
        .arg("--")
        .arg("--out-dir")
        .arg(&export_out_dir)
        .status()
        .context("failed to run exporter harness")?;
    if !status.success() {
        return Err(anyhow!("exporter harness failed with status {}", status));
    }

    let manifest = load_manifest_from_dir(&export_out_dir)?;
    load_flow_irs_from_manifest(&manifest, &export_out_dir, selected_flow)
}

fn resolve_lock_generate_flows(args: &LockGenerateArgs) -> Result<BTreeMap<String, FlowIR>> {
    match (
        args.example.as_deref(),
        args.package.as_deref(),
        args.bundle.as_deref(),
    ) {
        (Some(example), None, None) => {
            let handle = load_example(example)?;
            let flow = handle.ir.flow().clone();
            if let Some(selected) = args.flow.as_deref() {
                if flow.id.as_str() != selected {
                    return Err(anyhow!(
                        "example `{example}` does not define flow `{selected}`"
                    ));
                }
            }
            let mut flows = BTreeMap::new();
            flows.insert(flow.id.as_str().to_string(), flow);
            Ok(flows)
        }
        (None, Some(package), None) => load_flow_irs_from_package(package, args.flow.as_deref()),
        (None, None, Some(bundle_dir)) => {
            let manifest = load_manifest_from_dir(bundle_dir)?;
            load_flow_irs_from_manifest(&manifest, bundle_dir, args.flow.as_deref())
        }
        _ => Err(anyhow!(
            "exactly one of --example, --package, or --bundle must be provided"
        )),
    }
}

fn provider_kind_from_binding(key: &str, token: &str) -> Option<&'static str> {
    match token {
        "memory" => {
            if key.starts_with("resource::kv") {
                Some("kv.memory")
            } else if key.starts_with("resource::blob") {
                Some("blob.memory")
            } else if key == "durability::checkpoint_store" {
                Some("checkpoint_store.memory")
            } else {
                None
            }
        }
        "kv.memory" => Some("kv.memory"),
        "blob.memory" => Some("blob.memory"),
        "checkpoint_store.memory" => Some("checkpoint_store.memory"),
        "reqwest" | "http.reqwest" => Some("http.reqwest"),
        _ => None,
    }
}

fn default_provider_kind_for_required_hint(required: &str) -> Option<&'static str> {
    if required.starts_with("resource::kv") {
        return Some("kv.memory");
    }

    if required.starts_with("resource::blob") {
        return Some("blob.memory");
    }

    if required.starts_with("resource::http") {
        return Some("http.reqwest");
    }

    None
}

fn binding_key_covers_required(binding_key: &str, required: &str) -> bool {
    required == binding_key || required.starts_with(&format!("{binding_key}::"))
}

fn parse_bindings_for_lock(bindings: &[String]) -> Result<Vec<(String, String)>> {
    let mut parsed = Vec::new();

    for binding in bindings {
        let (raw_key, raw_value) = binding.split_once('=').ok_or_else(|| {
            anyhow!("invalid --bind `{binding}`; expected `<resource::hint>=<provider>`")
        })?;
        let key = normalize_binding_key(raw_key.trim());
        if !key.starts_with("resource::") && !key.starts_with("durability::") {
            return Err(anyhow!(
                "invalid --bind `{binding}`; expected `resource::*` or `durability::*` key after normalization"
            ));
        }

        let token = raw_value.trim();
        let provider_kind = provider_kind_from_binding(&key, token).ok_or_else(|| {
            anyhow!(
                "unsupported provider `{token}` in --bind `{binding}`; supported: memory, kv.memory, blob.memory, checkpoint_store.memory, reqwest"
            )
        })?;

        parsed.push((key, provider_kind.to_string()));
    }

    Ok(parsed)
}

fn lock_instance_for_provider_kind(provider_kind: &str) -> Result<LockInstance> {
    let provides = match provider_kind {
        "kv.memory" => vec!["resource::kv".to_string()],
        "blob.memory" => vec!["resource::blob".to_string()],
        "checkpoint_store.memory" => vec!["durability::checkpoint_store".to_string()],
        "http.reqwest" => vec!["resource::http".to_string()],
        other => {
            return Err(anyhow!(
                "unsupported provider_kind `{other}` in bindings.lock generator"
            ));
        }
    };

    Ok(LockInstance {
        provider_kind: provider_kind.to_string(),
        provides,
        connect: json!({}),
        config: json!({}),
        isolation: Vec::new(),
    })
}

fn instance_name_for_provider_kind(provider_kind: &str) -> String {
    provider_kind.replace('.', "_")
}

fn select_provider_kind_for_required_hint(
    overrides: &[(String, String)],
    required: &str,
) -> Result<String> {
    let mut best: Option<&(String, String)> = None;

    for entry in overrides {
        let (key, _kind) = entry;
        if binding_key_covers_required(key, required) {
            match best {
                Some((best_key, _)) if best_key.len() >= key.len() => {}
                _ => best = Some(entry),
            }
        }
    }

    let provider_kind = if let Some((_, kind)) = best {
        kind.clone()
    } else if let Some(default) = default_provider_kind_for_required_hint(required) {
        default.to_string()
    } else {
        return Err(anyhow!(
            "no provider selected for required `{required}`; pass `--bind <resource::...>=<provider>`"
        ));
    };

    let instance = lock_instance_for_provider_kind(&provider_kind)?;
    if !instance_provides(&instance, required) {
        return Err(anyhow!(
            "provider_kind `{provider_kind}` does not provide `{required}`"
        ));
    }

    Ok(provider_kind)
}

fn run_bindings_lock_generate(args: LockGenerateArgs) -> Result<()> {
    if args.generated_at.trim().is_empty() {
        return Err(anyhow!("--generated-at cannot be empty"));
    }

    let flows_for_lock = resolve_lock_generate_flows(&args)?;
    let overrides = parse_bindings_for_lock(&args.bindings)?;

    let mut flows = BTreeMap::new();
    let mut instances: BTreeMap<String, LockInstance> = BTreeMap::new();

    for (flow_id, flow) in flows_for_lock {
        let mut required = required_resource_hints(&flow);
        for (key, _) in &overrides {
            required.insert(key.clone());
        }

        let mut use_map: BTreeMap<String, String> = BTreeMap::new();
        for hint in required {
            let provider_kind = select_provider_kind_for_required_hint(&overrides, &hint)?;
            let instance_name = instance_name_for_provider_kind(&provider_kind);
            use_map.insert(hint, instance_name.clone());

            match instances.entry(instance_name) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(lock_instance_for_provider_kind(&provider_kind)?);
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }

        flows.insert(flow_id, LockFlow { use_map });
    }

    let mut lock = BindingsLock {
        version: 1,
        generated_at: args.generated_at,
        content_hash: String::new(),
        instances,
        flows,
        connector_handles: BTreeMap::new(),
        connector_connections: BTreeMap::new(),
        connector_bindings: BTreeMap::new(),
    };

    let json = serde_json::to_value(&lock).context("failed to serialize bindings.lock")?;
    lock.content_hash = compute_lock_content_hash(&json)?;

    let payload = serde_json::to_vec_pretty(&lock).context("failed to serialize bindings.lock")?;
    fs::write(&args.out, payload)
        .with_context(|| format!("failed to write {}", args.out.display()))?;
    println!("{}", args.out.display());
    Ok(())
}

fn load_bindings_lock(path: &Path) -> Result<BindingsLock> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let value: JsonValue = serde_json::from_slice(&bytes)
        .with_context(|| format!("{} is not valid JSON", path.display()))?;

    let expected = value
        .get("content_hash")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            anyhow!(
                "{} is missing required field `content_hash`",
                path.display()
            )
        })?
        .to_string();

    let actual = compute_lock_content_hash(&value)?;
    if expected != actual {
        return Err(anyhow!(
            "{} content_hash mismatch (expected {expected}, computed {actual})",
            path.display()
        ));
    }

    let lock: BindingsLock = serde_json::from_value(value)
        .with_context(|| format!("{} is not a valid bindings.lock document", path.display()))?;

    if lock.version != 1 {
        return Err(anyhow!(
            "{} has unsupported bindings.lock version {}; expected 1",
            path.display(),
            lock.version
        ));
    }

    if lock.generated_at.trim().is_empty() {
        return Err(anyhow!(
            "{} is missing required field `generated_at`",
            path.display()
        ));
    }

    if lock.content_hash != expected {
        return Err(anyhow!(
            "{} content_hash mismatch after parsing (expected {expected}, parsed {})",
            path.display(),
            lock.content_hash
        ));
    }

    Ok(lock)
}

fn instance_provides(instance: &LockInstance, required: &str) -> bool {
    instance
        .provides
        .iter()
        .any(|provided| required == provided || required.starts_with(&format!("{provided}::")))
}

fn validate_lock_instance_well_formed(name: &str, instance: &LockInstance) -> Result<()> {
    for provided in &instance.provides {
        if !provided.starts_with("resource::") && !provided.starts_with("durability::") {
            return Err(anyhow!(
                "bindings.lock instance `{name}` has unsupported provides entry `{provided}`"
            ));
        }
    }

    if !instance.connect.is_object() {
        return Err(anyhow!(
            "bindings.lock instance `{name}` has invalid `connect` (expected object)"
        ));
    }

    if !instance.config.is_object() {
        return Err(anyhow!(
            "bindings.lock instance `{name}` has invalid `config` (expected object)"
        ));
    }

    if !instance.isolation.is_empty() {
        return Err(anyhow!(
            "bindings.lock instance `{name}` uses isolation wrappers; wrappers not supported"
        ));
    }

    Ok(())
}

fn validate_connector_handle_well_formed(
    name: &str,
    handle: &ConnectorHandleInstance,
) -> Result<()> {
    if handle.provider_kind.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has empty `provider_kind`"
        ));
    }

    if handle.handle_kind.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has empty `handle_kind`"
        ));
    }

    if !handle.connect.is_object() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has invalid `connect` (expected object)"
        ));
    }

    if !handle.config.is_object() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has invalid `config` (expected object)"
        ));
    }

    if !handle.grants.is_object() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has invalid `grants` (expected object)"
        ));
    }

    validate_connector_handle_provider_config(name, handle)
}

fn connector_role_kind_label(kind: ConnectorRoleKind) -> &'static str {
    match kind {
        ConnectorRoleKind::OutboundAuth => "outbound_auth",
        ConnectorRoleKind::ProvisioningAuth => "provisioning_auth",
        ConnectorRoleKind::InboundVerifier => "inbound_verifier",
        ConnectorRoleKind::EndpointProfile => "endpoint_profile",
    }
}

fn parse_connector_role_key(role_key: &str) -> Result<(ConnectorRoleKind, &str)> {
    let (kind, name) = role_key.split_once('.').ok_or_else(|| {
        anyhow!(
            "bindings.lock connector role `{role_key}` is invalid; expected `<role_kind>.<role_name>`"
        )
    })?;

    if name.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector role `{role_key}` is invalid; role name cannot be empty"
        ));
    }

    let kind = match kind {
        "outbound_auth" => ConnectorRoleKind::OutboundAuth,
        "provisioning_auth" => ConnectorRoleKind::ProvisioningAuth,
        "inbound_verifier" => ConnectorRoleKind::InboundVerifier,
        "endpoint_profile" => ConnectorRoleKind::EndpointProfile,
        other => {
            return Err(anyhow!(
                "bindings.lock connector role `{role_key}` uses unsupported role kind `{other}`"
            ));
        }
    };

    Ok((kind, name))
}

fn validate_connector_handle_provider_config(
    name: &str,
    handle: &ConnectorHandleInstance,
) -> Result<()> {
    match handle.provider_kind.as_str() {
        "auth.static_bearer" => {
            if handle.handle_kind != "http.bearer" {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` uses provider_kind `auth.static_bearer` but has handle_kind `{}`; expected `http.bearer`",
                    handle.handle_kind,
                ));
            }
            validate_handle_secret_ref(name, handle)?;
        }
        "auth.static_secret" => {
            if handle.handle_kind != "raw.secret" {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` uses provider_kind `auth.static_secret` but has handle_kind `{}`; expected `raw.secret`",
                    handle.handle_kind,
                ));
            }
            validate_handle_secret_ref(name, handle)?;
        }
        "auth.oauth2.refresh" => {
            if handle.handle_kind != "http.bearer" {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` uses provider_kind `auth.oauth2.refresh` but has handle_kind `{}`; expected `http.bearer`",
                    handle.handle_kind,
                ));
            }
            validate_named_connect_secret_ref(name, handle, "client_id_ref")?;
            validate_named_connect_secret_ref(name, handle, "client_secret_ref")?;
            validate_named_connect_secret_ref(name, handle, "refresh_token_ref")?;
            validate_oauth2_refresh_config(name, handle)?;
        }
        "auth.service_account_jwt" => {
            if handle.handle_kind != "http.bearer" {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` uses provider_kind `auth.service_account_jwt` but has handle_kind `{}`; expected `http.bearer`",
                    handle.handle_kind,
                ));
            }
            validate_named_connect_secret_ref(name, handle, "service_account_email_ref")?;
            validate_named_connect_secret_ref(name, handle, "private_key_ref")?;
            validate_service_account_jwt_config(name, handle)?;
        }
        "endpoint.profile.static" => {
            if handle.handle_kind != "endpoint.profile" {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` uses provider_kind `endpoint.profile.static` but has handle_kind `{}`; expected `endpoint.profile`",
                    handle.handle_kind,
                ));
            }
            validate_static_endpoint_profile_handle(name, handle)?;
        }
        _ => {}
    }

    Ok(())
}

fn validate_handle_secret_ref(name: &str, handle: &ConnectorHandleInstance) -> Result<()> {
    validate_named_connect_secret_ref(name, handle, "secret_ref")
}

fn validate_named_connect_secret_ref(
    name: &str,
    handle: &ConnectorHandleInstance,
    field: &str,
) -> Result<()> {
    let secret_ref = handle
        .connect
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` is missing required `connect.{field}` string"
            )
        })?;

    if secret_ref.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has empty `connect.{field}`"
        ));
    }

    Ok(())
}

fn validate_oauth2_refresh_config(name: &str, handle: &ConnectorHandleInstance) -> Result<()> {
    let token_url = handle
        .config
        .get("token_url")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` is missing required `config.token_url` string"
            )
        })?;
    if token_url.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has empty `config.token_url`"
        ));
    }

    if let Some(scopes) = handle.config.get("scopes") {
        let scopes = scopes.as_array().ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.scopes` (expected array)"
            )
        })?;
        for (index, value) in scopes.iter().enumerate() {
            if value.as_str().is_none() {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` has non-string `config.scopes[{index}]`"
                ));
            }
        }
    }

    if let Some(extra_form_fields) = handle.config.get("extra_form_fields") {
        let fields = extra_form_fields.as_object().ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.extra_form_fields` (expected object)"
            )
        })?;
        for (field_name, field_value) in fields {
            if !field_value.is_string() {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` has non-string `config.extra_form_fields.{field_name}`"
                ));
            }
        }
    }

    Ok(())
}

fn validate_service_account_jwt_config(name: &str, handle: &ConnectorHandleInstance) -> Result<()> {
    let token_url = handle
        .config
        .get("token_url")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` is missing required `config.token_url` string"
            )
        })?;
    if token_url.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has empty `config.token_url`"
        ));
    }

    let scopes = handle
        .config
        .get("scopes")
        .ok_or_else(|| anyhow!(
            "bindings.lock connector handle `{name}` is missing required `config.scopes` array"
        ))?
        .as_array()
        .ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.scopes` (expected array)"
            )
        })?;
    if scopes.is_empty() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` requires non-empty `config.scopes`"
        ));
    }
    for (index, value) in scopes.iter().enumerate() {
        if value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .is_none()
        {
            return Err(anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.scopes[{index}]`"
            ));
        }
    }

    if let Some(subject) = handle.config.get("subject") {
        if subject
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .is_none()
        {
            return Err(anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.subject` (expected non-empty string)"
            ));
        }
    }

    if let Some(token_lifetime_seconds) = handle.config.get("token_lifetime_seconds") {
        let Some(seconds) = token_lifetime_seconds.as_u64() else {
            return Err(anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.token_lifetime_seconds` (expected u64)"
            ));
        };
        if seconds == 0 || seconds > 3600 {
            return Err(anyhow!(
                "bindings.lock connector handle `{name}` requires `config.token_lifetime_seconds` in 1..=3600"
            ));
        }
    }

    if let Some(extra_form_fields) = handle.config.get("extra_form_fields") {
        let fields = extra_form_fields.as_object().ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.extra_form_fields` (expected object)"
            )
        })?;
        for (field_name, field_value) in fields {
            if !field_value.is_string() {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` has non-string `config.extra_form_fields.{field_name}`"
                ));
            }
        }
    }

    Ok(())
}

fn validate_static_endpoint_profile_handle(
    name: &str,
    handle: &ConnectorHandleInstance,
) -> Result<()> {
    let base_url = handle
        .config
        .get("base_url")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` is missing required `config.base_url` string"
            )
        })?;
    if base_url.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector handle `{name}` has empty `config.base_url`"
        ));
    }

    if let Some(default_headers) = handle.config.get("default_headers") {
        let headers = default_headers.as_object().ok_or_else(|| {
            anyhow!(
                "bindings.lock connector handle `{name}` has invalid `config.default_headers` (expected object)"
            )
        })?;
        for (header_name, header_value) in headers {
            if !header_value.is_string() {
                return Err(anyhow!(
                    "bindings.lock connector handle `{name}` has non-string `config.default_headers.{header_name}`"
                ));
            }
        }
    }

    Ok(())
}

fn validate_connector_role_provider_compatibility(
    connection_name: &str,
    role_key: &str,
    handle_name: &str,
    handle: &ConnectorHandleInstance,
) -> Result<()> {
    let (role_kind, _) = parse_connector_role_key(role_key)?;
    let provider_kind = handle.provider_kind.as_str();

    let family_matches = match role_kind {
        ConnectorRoleKind::OutboundAuth | ConnectorRoleKind::ProvisioningAuth => {
            provider_kind.starts_with("auth.")
        }
        ConnectorRoleKind::InboundVerifier => provider_kind.starts_with("verifier."),
        ConnectorRoleKind::EndpointProfile => provider_kind.starts_with("endpoint.profile."),
    };

    if !family_matches {
        return Err(anyhow!(
            "bindings.lock connector connection `{connection_name}` binds role `{role_key}` to handle `{handle_name}` with provider_kind `{provider_kind}`, but role kind `{}` expects the matching provider family",
            connector_role_kind_label(role_kind),
        ));
    }

    Ok(())
}

fn validate_connector_connection_well_formed(
    name: &str,
    connection: &ConnectorConnectionInstance,
    handles: &BTreeMap<String, ConnectorHandleInstance>,
) -> Result<()> {
    if connection.connector_id.trim().is_empty() {
        return Err(anyhow!(
            "bindings.lock connector connection `{name}` has empty `connector_id`"
        ));
    }

    if !connection.config.is_object() {
        return Err(anyhow!(
            "bindings.lock connector connection `{name}` has invalid `config` (expected object)"
        ));
    }

    for (role_key, handle_name) in &connection.roles {
        let handle = handles.get(handle_name).ok_or_else(|| {
            anyhow!(
                "bindings.lock connector connection `{name}` references unknown handle `{handle_name}` for role `{role_key}`"
            )
        })?;
        validate_connector_role_provider_compatibility(name, role_key, handle_name, handle)?;
    }

    validate_connector_connection_config(name, connection)?;

    Ok(())
}

fn validate_connector_connection_config(
    name: &str,
    connection: &ConnectorConnectionInstance,
) -> Result<()> {
    match connection.connector_id.as_str() {
        "connector.formualizer.sheetport" => validate_sheetport_connection_config(name, connection),
        _ => Ok(()),
    }
}

fn validate_sheetport_connection_config(
    name: &str,
    connection: &ConnectorConnectionInstance,
) -> Result<()> {
    let config = connection.config.as_object().ok_or_else(|| {
        anyhow!(
            "bindings.lock connector connection `{name}` has invalid `config` (expected object)"
        )
    })?;

    let workbook_source = config.get("workbook_source").ok_or_else(|| {
        anyhow!(
            "bindings.lock connector connection `{name}` for `connector.formualizer.sheetport` is missing required `config.workbook_source` object"
        )
    })?;
    let workbook_source = workbook_source.as_object().ok_or_else(|| {
        anyhow!(
            "bindings.lock connector connection `{name}` has invalid `config.workbook_source` (expected object)"
        )
    })?;
    let workbook_kind = required_nonempty_string_field(
        workbook_source,
        &format!("bindings.lock connector connection `{name}` config.workbook_source"),
        "kind",
    )?;
    match workbook_kind {
        "blob" => {
            required_nonempty_string_field(
                workbook_source,
                &format!("bindings.lock connector connection `{name}` config.workbook_source"),
                "key",
            )?;
        }
        "materialized_blob" => {
            required_nonempty_string_field(
                workbook_source,
                &format!("bindings.lock connector connection `{name}` config.workbook_source"),
                "key",
            )?;
            let format = required_nonempty_string_field(
                workbook_source,
                &format!("bindings.lock connector connection `{name}` config.workbook_source"),
                "format",
            )?;
            match format {
                "workbook_json_v1" => {}
                other => {
                    return Err(anyhow!(
                        "bindings.lock connector connection `{name}` has unsupported `config.workbook_source.format` `{other}`; expected `workbook_json_v1`"
                    ));
                }
            }
        }
        "file_path" => {
            required_nonempty_string_field(
                workbook_source,
                &format!("bindings.lock connector connection `{name}` config.workbook_source"),
                "path",
            )?;
        }
        other => {
            return Err(anyhow!(
                "bindings.lock connector connection `{name}` has unsupported `config.workbook_source.kind` `{other}`; expected one of: blob, materialized_blob, file_path"
            ));
        }
    }

    let manifest_source = config.get("manifest_source").ok_or_else(|| {
        anyhow!(
            "bindings.lock connector connection `{name}` for `connector.formualizer.sheetport` is missing required `config.manifest_source` object"
        )
    })?;
    let manifest_source = manifest_source.as_object().ok_or_else(|| {
        anyhow!(
            "bindings.lock connector connection `{name}` has invalid `config.manifest_source` (expected object)"
        )
    })?;
    let manifest_kind = required_nonempty_string_field(
        manifest_source,
        &format!("bindings.lock connector connection `{name}` config.manifest_source"),
        "kind",
    )?;
    match manifest_kind {
        "inline_yaml" => {
            required_nonempty_string_field(
                manifest_source,
                &format!("bindings.lock connector connection `{name}` config.manifest_source"),
                "value",
            )?;
        }
        "blob" => {
            required_nonempty_string_field(
                manifest_source,
                &format!("bindings.lock connector connection `{name}` config.manifest_source"),
                "key",
            )?;
        }
        "file_path" => {
            required_nonempty_string_field(
                manifest_source,
                &format!("bindings.lock connector connection `{name}` config.manifest_source"),
                "path",
            )?;
        }
        other => {
            return Err(anyhow!(
                "bindings.lock connector connection `{name}` has unsupported `config.manifest_source.kind` `{other}`; expected one of: inline_yaml, blob, file_path"
            ));
        }
    }

    if let Some(eval_defaults) = config.get("eval_defaults")
        && !eval_defaults.is_object()
    {
        return Err(anyhow!(
            "bindings.lock connector connection `{name}` has invalid `config.eval_defaults` (expected object)"
        ));
    }

    if let Some(artifact_policy) = config.get("artifact_policy")
        && !artifact_policy.is_object()
    {
        return Err(anyhow!(
            "bindings.lock connector connection `{name}` has invalid `config.artifact_policy` (expected object)"
        ));
    }

    Ok(())
}

fn required_nonempty_string_field<'a>(
    object: &'a serde_json::Map<String, JsonValue>,
    context: &str,
    field: &str,
) -> Result<&'a str> {
    let value = object
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("{context} is missing required `{field}` string"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{context} has empty `{field}`"));
    }
    Ok(trimmed)
}

#[derive(Clone)]
struct BindingsLockConnectorRuntime {
    flow_id: String,
    handles: BTreeMap<String, ConnectorHandleInstance>,
    connections: BTreeMap<String, ConnectorConnectionInstance>,
    bindings: ConnectorFlowBindings,
    auth_http_client: reqwest::Client,
    access_token_cache: Arc<Mutex<HashMap<String, CachedAccessToken>>>,
}

#[derive(Debug, Clone)]
struct CachedAccessToken {
    access_token: String,
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct OAuth2RefreshProviderConfig {
    token_url: String,
    scopes: Vec<String>,
    extra_form_fields: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct ServiceAccountJwtProviderConfig {
    token_url: String,
    scopes: Vec<String>,
    subject: Option<String>,
    token_lifetime_seconds: u64,
    extra_form_fields: Vec<(String, String)>,
}

#[derive(Debug, Serialize)]
struct ServiceAccountJwtClaims {
    iss: String,
    aud: String,
    scope: String,
    iat: u64,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub: Option<String>,
}

impl BindingsLockConnectorRuntime {
    fn new(lock: &BindingsLock, flow_id: &str) -> Result<Self> {
        let bindings = lock
            .connector_bindings
            .get(flow_id)
            .cloned()
            .unwrap_or_default();

        for (name, handle) in &lock.connector_handles {
            validate_connector_handle_well_formed(name, handle)?;
        }
        for (name, connection) in &lock.connector_connections {
            validate_connector_connection_well_formed(name, connection, &lock.connector_handles)?;
        }
        for (node_alias, connection_name) in &bindings.nodes {
            if !lock.connector_connections.contains_key(connection_name) {
                return Err(anyhow!(
                    "bindings.lock connector binding for node `{node_alias}` references unknown connection `{connection_name}`"
                ));
            }
        }
        for (connector_id, connection_name) in &bindings.defaults {
            let connection = lock.connector_connections.get(connection_name).ok_or_else(|| {
                anyhow!(
                    "bindings.lock connector default for `{connector_id}` references unknown connection `{connection_name}`"
                )
            })?;
            if connection.connector_id != *connector_id {
                return Err(anyhow!(
                    "bindings.lock connector default for `{connector_id}` references connection `{connection_name}` targeting `{}`",
                    connection.connector_id,
                ));
            }
        }

        Ok(Self {
            flow_id: flow_id.to_string(),
            handles: lock.connector_handles.clone(),
            connections: lock.connector_connections.clone(),
            bindings,
            auth_http_client: reqwest::Client::new(),
            access_token_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn resolve_connection_name(&self, scope: &ConnectorBindingScope) -> Result<&str> {
        if scope.flow_id != self.flow_id {
            return Err(anyhow!(
                "connector runtime loaded for flow `{}` but invoked from flow `{}`",
                self.flow_id,
                scope.flow_id
            ));
        }

        if let Some(connection_name) = self.bindings.nodes.get(&scope.node_alias) {
            return Ok(connection_name.as_str());
        }

        if let Some(connection_name) = self.bindings.defaults.get(&scope.connector_id) {
            return Ok(connection_name.as_str());
        }

        Err(anyhow!(
            "no connector binding resolved for connector `{}` at node alias `{}`",
            scope.connector_id,
            scope.node_alias
        ))
    }

    fn resolve_bound_connection<'a>(
        &'a self,
        scope: &ConnectorBindingScope,
    ) -> Result<(&'a str, &'a ConnectorConnectionInstance)> {
        let connection_name = self.resolve_connection_name(scope)?;
        let connection = self.connections.get(connection_name).ok_or_else(|| {
            anyhow!("bindings.lock references unknown connector connection `{connection_name}`")
        })?;

        if connection.connector_id != scope.connector_id {
            return Err(anyhow!(
                "connector connection `{connection_name}` targets `{}` but runtime requested `{}`",
                connection.connector_id,
                scope.connector_id
            ));
        }

        Ok((connection_name, connection))
    }

    fn resolve_role_handle<'a>(
        &'a self,
        scope: &ConnectorBindingScope,
        role_key: &str,
    ) -> Result<(&'a str, &'a ConnectorHandleInstance)> {
        let (connection_name, connection) = self.resolve_bound_connection(scope)?;
        let handle_name = connection.roles.get(role_key).ok_or_else(|| {
            anyhow!(
                "connector connection `{connection_name}` does not bind required role `{role_key}`"
            )
        })?;
        let handle = self.handles.get(handle_name).ok_or_else(|| {
            anyhow!(
                "connector connection `{connection_name}` references unknown handle `{handle_name}`"
            )
        })?;
        Ok((handle_name.as_str(), handle))
    }

    fn cached_access_token(&self, handle_name: &str) -> Option<String> {
        let cache = self.access_token_cache.lock().expect("access token cache");
        let entry = cache.get(handle_name)?;
        if let Some(expires_at) = entry.expires_at {
            if Instant::now() + Duration::from_secs(30) >= expires_at {
                return None;
            }
        }
        Some(entry.access_token.clone())
    }

    fn store_access_token(
        &self,
        handle_name: &str,
        access_token: String,
        expires_in_seconds: Option<u64>,
    ) {
        let expires_at =
            expires_in_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds));
        let mut cache = self.access_token_cache.lock().expect("access token cache");
        cache.insert(
            handle_name.to_string(),
            CachedAccessToken {
                access_token,
                expires_at,
            },
        );
    }

    async fn exchange_access_token(
        &self,
        handle_name: &str,
        provider_label: &str,
        token_url: String,
        form_fields: Vec<(String, String)>,
    ) -> Result<String, ConnectorRuntimeError> {
        let response = self
            .auth_http_client
            .post(token_url)
            .form(&form_fields)
            .send()
            .await
            .map_err(|err| {
                ConnectorRuntimeError::Provider(anyhow!(err).context(format!(
                    "connector handle `{handle_name}` failed to exchange {provider_label} token"
                )))
            })?;

        let status = response.status();
        let body: JsonValue = response.json().await.map_err(|err| {
            ConnectorRuntimeError::Provider(anyhow!(err).context(format!(
                "connector handle `{handle_name}` returned invalid {provider_label} token JSON"
            )))
        })?;

        if !status.is_success() {
            return Err(ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` {provider_label} token request failed with status {status}: {body}"
            )));
        }

        let token_type = body
            .get("token_type")
            .and_then(JsonValue::as_str)
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_else(|| "bearer".to_string());
        if token_type != "bearer" {
            return Err(ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` returned unsupported {provider_label} token_type `{token_type}`"
            )));
        }

        let access_token = body
            .get("access_token")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ConnectorRuntimeError::Provider(anyhow!(
                    "connector handle `{handle_name}` {provider_label} token response is missing `access_token`"
                ))
            })?
            .to_string();
        let expires_in_seconds = parse_optional_expires_in_seconds(&body).map_err(|err| {
            ConnectorRuntimeError::Provider(err.context(format!(
                "connector handle `{handle_name}` has invalid {provider_label} `expires_in`"
            )))
        })?;

        self.store_access_token(handle_name, access_token.clone(), expires_in_seconds);
        Ok(access_token)
    }

    async fn resolve_oauth2_access_token(
        &self,
        handle_name: &str,
        handle: &ConnectorHandleInstance,
    ) -> Result<String, ConnectorRuntimeError> {
        if let Some(access_token) = self.cached_access_token(handle_name) {
            return Ok(access_token);
        }

        let config = oauth2_refresh_provider_config(handle_name, handle)?;
        let client_id = resolve_secret_ref(named_connect_secret_ref(
            handle_name,
            handle,
            "client_id_ref",
        )?)
        .map_err(ConnectorRuntimeError::Provider)?;
        let client_secret = resolve_secret_ref(named_connect_secret_ref(
            handle_name,
            handle,
            "client_secret_ref",
        )?)
        .map_err(ConnectorRuntimeError::Provider)?;
        let refresh_token = resolve_secret_ref(named_connect_secret_ref(
            handle_name,
            handle,
            "refresh_token_ref",
        )?)
        .map_err(ConnectorRuntimeError::Provider)?;

        let mut form_fields = vec![
            ("grant_type".to_string(), "refresh_token".to_string()),
            ("client_id".to_string(), client_id),
            ("client_secret".to_string(), client_secret),
            ("refresh_token".to_string(), refresh_token),
        ];
        if !config.scopes.is_empty() {
            form_fields.push(("scope".to_string(), config.scopes.join(" ")));
        }
        form_fields.extend(config.extra_form_fields);

        self.exchange_access_token(handle_name, "OAuth2 refresh", config.token_url, form_fields)
            .await
    }

    async fn resolve_service_account_access_token(
        &self,
        handle_name: &str,
        handle: &ConnectorHandleInstance,
    ) -> Result<String, ConnectorRuntimeError> {
        if let Some(access_token) = self.cached_access_token(handle_name) {
            return Ok(access_token);
        }

        let config = service_account_jwt_provider_config(handle_name, handle)?;
        let service_account_email = resolve_secret_ref(named_connect_secret_ref(
            handle_name,
            handle,
            "service_account_email_ref",
        )?)
        .map_err(ConnectorRuntimeError::Provider)?;
        let private_key_pem = resolve_secret_ref(named_connect_secret_ref(
            handle_name,
            handle,
            "private_key_ref",
        )?)
        .map_err(ConnectorRuntimeError::Provider)?;

        let assertion = build_service_account_jwt_assertion(
            handle_name,
            &service_account_email,
            &private_key_pem,
            &config,
        )?;

        let mut form_fields = vec![
            (
                "grant_type".to_string(),
                "urn:ietf:params:oauth:grant-type:jwt-bearer".to_string(),
            ),
            ("assertion".to_string(), assertion),
        ];
        form_fields.extend(config.extra_form_fields);

        self.exchange_access_token(
            handle_name,
            "service-account JWT",
            config.token_url,
            form_fields,
        )
        .await
    }
}

#[async_trait]
impl ConnectorRuntime for BindingsLockConnectorRuntime {
    async fn apply_outbound_auth(
        &self,
        scope: &ConnectorBindingScope,
        profile: &OutboundAuthProfileDescriptor,
        request: &mut capabilities::http::HttpRequest,
    ) -> Result<(), ConnectorRuntimeError> {
        let role_key = format!("outbound_auth.{}", profile.name);
        let (handle_name, handle) = self
            .resolve_role_handle(scope, &role_key)
            .map_err(ConnectorRuntimeError::Provider)?;
        if handle.handle_kind != profile.kind.handle_kind() {
            return Err(ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` has handle_kind `{}` but role `{}` expects `{}`",
                handle.handle_kind,
                role_key,
                profile.kind.handle_kind()
            )));
        }

        match handle.provider_kind.as_str() {
            "auth.static_bearer" | "auth.static_secret" => {
                let secret_ref = connector_handle_secret_ref(handle_name, handle)?;
                let secret =
                    resolve_secret_ref(secret_ref).map_err(ConnectorRuntimeError::Provider)?;
                apply_static_auth_to_request(request, profile, secret)?;
            }
            "auth.oauth2.refresh" => {
                let access_token = self
                    .resolve_oauth2_access_token(handle_name, handle)
                    .await?;
                apply_static_auth_to_request(request, profile, access_token)?;
            }
            "auth.service_account_jwt" => {
                let access_token = self
                    .resolve_service_account_access_token(handle_name, handle)
                    .await?;
                apply_static_auth_to_request(request, profile, access_token)?;
            }
            other => {
                return Err(ConnectorRuntimeError::Provider(anyhow!(
                    "connector handle `{handle_name}` uses unsupported auth provider_kind `{other}`"
                )));
            }
        }

        Ok(())
    }

    async fn resolve_endpoint_profile(
        &self,
        scope: &ConnectorBindingScope,
        profile: &EndpointProfileDescriptor,
    ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
        let role_key = format!("endpoint_profile.{}", profile.name);
        let (handle_name, handle) = self
            .resolve_role_handle(scope, &role_key)
            .map_err(ConnectorRuntimeError::Provider)?;
        if handle.handle_kind != "endpoint.profile" {
            return Err(ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` has handle_kind `{}` but role `{}` expects `endpoint.profile`",
                handle.handle_kind,
                role_key,
            )));
        }
        match handle.provider_kind.as_str() {
            "endpoint.profile.static" => {
                resolve_static_endpoint_profile(handle_name, handle, profile)
            }
            other => Err(ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` uses unsupported endpoint provider_kind `{other}`"
            ))),
        }
    }

    async fn resolve_connection(
        &self,
        scope: &ConnectorBindingScope,
    ) -> Result<Option<ResolvedConnectorConnection>, ConnectorRuntimeError> {
        let (connection_name, connection) =
            BindingsLockConnectorRuntime::resolve_bound_connection(self, scope)
                .map_err(ConnectorRuntimeError::Provider)?;
        Ok(Some(ResolvedConnectorConnection {
            connection_name: Some(connection_name.to_string()),
            connector_id: connection.connector_id.clone(),
            config: connection.config.clone(),
        }))
    }

    async fn resolve_required_effect_hints(
        &self,
        scope: &ConnectorBindingScope,
        selected_mode: dag_core::ConnectorResolutionModeDecl,
    ) -> Result<Vec<String>, ConnectorRuntimeError> {
        if selected_mode != dag_core::ConnectorResolutionModeDecl::BoundConnection {
            return Ok(Vec::new());
        }

        let Some(resolved) = self.resolve_connection(scope).await? else {
            return Ok(Vec::new());
        };

        match resolved.connector_id.as_str() {
            "connector.formualizer.sheetport" => {
                let mut hints = Vec::new();
                let config = resolved.config.as_object().ok_or_else(|| {
                    ConnectorRuntimeError::Provider(anyhow!(
                        "sheetport connection config must be an object"
                    ))
                })?;

                if source_kind_is_blob(config.get("workbook_source"))? {
                    hints.push(capabilities::blob::HINT_BLOB_READ.to_string());
                }
                if source_kind_is_blob(config.get("manifest_source"))? {
                    hints.push(capabilities::blob::HINT_BLOB_READ.to_string());
                }
                hints.sort();
                hints.dedup();
                Ok(hints)
            }
            _ => Ok(Vec::new()),
        }
    }
}

fn source_kind_is_blob(value: Option<&JsonValue>) -> Result<bool, ConnectorRuntimeError> {
    let Some(value) = value else {
        return Ok(false);
    };
    let object = value.as_object().ok_or_else(|| {
        ConnectorRuntimeError::Provider(anyhow!("connector source config must be an object"))
    })?;
    Ok(matches!(
        object.get("kind").and_then(JsonValue::as_str),
        Some("blob") | Some("materialized_blob")
    ))
}

fn named_connect_secret_ref<'a>(
    handle_name: &str,
    handle: &'a ConnectorHandleInstance,
    field: &str,
) -> Result<&'a str, ConnectorRuntimeError> {
    handle
        .connect
        .get(field)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` is missing `connect.{field}`"
            ))
        })
}

fn connector_handle_secret_ref<'a>(
    handle_name: &str,
    handle: &'a ConnectorHandleInstance,
) -> Result<&'a str, ConnectorRuntimeError> {
    named_connect_secret_ref(handle_name, handle, "secret_ref")
}

fn oauth2_refresh_provider_config(
    handle_name: &str,
    handle: &ConnectorHandleInstance,
) -> Result<OAuth2RefreshProviderConfig, ConnectorRuntimeError> {
    let token_url = handle
        .config
        .get("token_url")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` is missing `config.token_url`"
            ))
        })?
        .to_string();

    let scopes = handle
        .config
        .get("scopes")
        .map(|value| {
            let values = value.as_array().ok_or_else(|| {
                ConnectorRuntimeError::Provider(anyhow!(
                    "connector handle `{handle_name}` has invalid `config.scopes`"
                ))
            })?;
            values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    value.as_str().map(|value| value.to_string()).ok_or_else(|| {
                        ConnectorRuntimeError::Provider(anyhow!(
                            "connector handle `{handle_name}` has non-string `config.scopes[{index}]`"
                        ))
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    let extra_form_fields = handle
        .config
        .get("extra_form_fields")
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                ConnectorRuntimeError::Provider(anyhow!(
                    "connector handle `{handle_name}` has invalid `config.extra_form_fields`"
                ))
            })?;
            let mut entries = object
                .iter()
                .map(|(field_name, field_value)| {
                    field_value
                        .as_str()
                        .map(|field_value| (field_name.clone(), field_value.to_string()))
                        .ok_or_else(|| {
                            ConnectorRuntimeError::Provider(anyhow!(
                                "connector handle `{handle_name}` has non-string `config.extra_form_fields.{field_name}`"
                            ))
                        })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Ok::<Vec<(String, String)>, ConnectorRuntimeError>(entries)
        })
        .transpose()?
        .unwrap_or_default();

    Ok(OAuth2RefreshProviderConfig {
        token_url,
        scopes,
        extra_form_fields,
    })
}

fn service_account_jwt_provider_config(
    handle_name: &str,
    handle: &ConnectorHandleInstance,
) -> Result<ServiceAccountJwtProviderConfig, ConnectorRuntimeError> {
    let token_url = handle
        .config
        .get("token_url")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` is missing `config.token_url`"
            ))
        })?
        .to_string();

    let scopes = handle
        .config
        .get("scopes")
        .ok_or_else(|| {
            ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` is missing `config.scopes`"
            ))
        })?
        .as_array()
        .ok_or_else(|| {
            ConnectorRuntimeError::Provider(anyhow!(
                "connector handle `{handle_name}` has invalid `config.scopes`"
            ))
        })?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(|value| value.to_string())
                .ok_or_else(|| {
                    ConnectorRuntimeError::Provider(anyhow!(
                        "connector handle `{handle_name}` has invalid `config.scopes[{index}]`"
                    ))
                })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let subject = handle
        .config
        .get("subject")
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(|value| value.to_string())
                .ok_or_else(|| {
                    ConnectorRuntimeError::Provider(anyhow!(
                        "connector handle `{handle_name}` has invalid `config.subject`"
                    ))
                })
        })
        .transpose()?;

    let token_lifetime_seconds = handle
        .config
        .get("token_lifetime_seconds")
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                ConnectorRuntimeError::Provider(anyhow!(
                    "connector handle `{handle_name}` has invalid `config.token_lifetime_seconds`"
                ))
            })
        })
        .transpose()?
        .unwrap_or(3600);
    if !(1..=3600).contains(&token_lifetime_seconds) {
        return Err(ConnectorRuntimeError::Provider(anyhow!(
            "connector handle `{handle_name}` requires `config.token_lifetime_seconds` in 1..=3600"
        )));
    }

    let extra_form_fields = handle
        .config
        .get("extra_form_fields")
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                ConnectorRuntimeError::Provider(anyhow!(
                    "connector handle `{handle_name}` has invalid `config.extra_form_fields`"
                ))
            })?;
            let mut entries = object
                .iter()
                .map(|(field_name, field_value)| {
                    field_value
                        .as_str()
                        .map(|field_value| (field_name.clone(), field_value.to_string()))
                        .ok_or_else(|| {
                            ConnectorRuntimeError::Provider(anyhow!(
                                "connector handle `{handle_name}` has non-string `config.extra_form_fields.{field_name}`"
                            ))
                        })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Ok::<Vec<(String, String)>, ConnectorRuntimeError>(entries)
        })
        .transpose()?
        .unwrap_or_default();

    Ok(ServiceAccountJwtProviderConfig {
        token_url,
        scopes,
        subject,
        token_lifetime_seconds,
        extra_form_fields,
    })
}

fn build_service_account_jwt_assertion(
    handle_name: &str,
    service_account_email: &str,
    private_key_pem: &str,
    config: &ServiceAccountJwtProviderConfig,
) -> Result<String, ConnectorRuntimeError> {
    let issued_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|err| {
            ConnectorRuntimeError::Provider(anyhow!(err).context(format!(
                "connector handle `{handle_name}` failed to determine current time"
            )))
        })?
        .as_secs();
    let claims = ServiceAccountJwtClaims {
        iss: service_account_email.to_string(),
        aud: config.token_url.clone(),
        scope: config.scopes.join(" "),
        iat: issued_at,
        exp: issued_at + config.token_lifetime_seconds,
        sub: config.subject.clone(),
    };

    let private_key_pem = normalize_pem_secret(private_key_pem);
    let encoding_key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes()).map_err(|err| {
        ConnectorRuntimeError::Provider(anyhow!(err).context(format!(
            "connector handle `{handle_name}` failed to parse RSA private key"
        )))
    })?;
    let header = Header::new(Algorithm::RS256);
    jsonwebtoken::encode(&header, &claims, &encoding_key).map_err(|err| {
        ConnectorRuntimeError::Provider(anyhow!(err).context(format!(
            "connector handle `{handle_name}` failed to sign service-account JWT assertion"
        )))
    })
}

fn normalize_pem_secret(secret: &str) -> String {
    if secret.contains("\\n") && !secret.contains('\n') {
        secret.replace("\\n", "\n")
    } else {
        secret.to_string()
    }
}

fn parse_optional_expires_in_seconds(body: &JsonValue) -> Result<Option<u64>> {
    let Some(value) = body.get("expires_in") else {
        return Ok(None);
    };

    if let Some(value) = value.as_u64() {
        return Ok(Some(value));
    }

    if let Some(value) = value.as_i64() {
        return u64::try_from(value)
            .map(Some)
            .map_err(|_| anyhow!("`expires_in` must not be negative"));
    }

    if let Some(value) = value.as_str() {
        return value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| anyhow!("`expires_in` string must parse as u64"));
    }

    Err(anyhow!("`expires_in` must be a number or numeric string"))
}

fn apply_static_auth_to_request(
    request: &mut capabilities::http::HttpRequest,
    profile: &OutboundAuthProfileDescriptor,
    secret: String,
) -> Result<(), ConnectorRuntimeError> {
    match profile.kind {
        OutboundAuthKind::Bearer { .. } => {
            request
                .headers
                .insert("Authorization", format!("Bearer {secret}"));
        }
        OutboundAuthKind::ApiKeyHeader {
            header_name,
            prefix,
            ..
        } => {
            let value = prefix
                .map(|prefix| format!("{prefix} {secret}"))
                .unwrap_or(secret);
            request.headers.insert(header_name, value);
        }
        OutboundAuthKind::ApiKeyQuery { query_name, .. } => {
            let separator = if request.url.contains('?') { '&' } else { '?' };
            request.url.push(separator);
            request.url.push_str(query_name);
            request.url.push('=');
            request.url.push_str(
                &percent_encoding::utf8_percent_encode(&secret, percent_encoding::NON_ALPHANUMERIC)
                    .to_string(),
            );
        }
        OutboundAuthKind::Unsupported { kind_name, .. } => {
            return Err(ConnectorRuntimeError::UnsupportedAuthKind {
                role_name: profile.name,
                kind: kind_name,
            });
        }
    }

    Ok(())
}

fn resolve_static_endpoint_profile(
    handle_name: &str,
    handle: &ConnectorHandleInstance,
    profile: &EndpointProfileDescriptor,
) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
    let base_url = handle
        .config
        .get("base_url")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| ConnectorRuntimeError::InvalidEndpointProfile {
            role_name: profile.name,
            reason: format!("connector handle `{handle_name}` is missing `config.base_url`"),
        })?
        .to_string();

    let mut default_headers = Vec::new();
    if let Some(header_map) = handle.config.get("default_headers") {
        let object = header_map.as_object().ok_or_else(|| {
            ConnectorRuntimeError::InvalidEndpointProfile {
                role_name: profile.name,
                reason: format!(
                    "connector handle `{handle_name}` has non-object `config.default_headers`"
                ),
            }
        })?;
        for (name, value) in object {
            let value =
                value
                    .as_str()
                    .ok_or_else(|| ConnectorRuntimeError::InvalidEndpointProfile {
                        role_name: profile.name,
                        reason: format!(
                            "connector handle `{handle_name}` has non-string header `{name}`"
                        ),
                    })?;
            default_headers.push((name.clone(), value.to_string()));
        }
        default_headers.sort_by(|left, right| left.0.cmp(&right.0));
    }

    Ok(ResolvedEndpointProfile {
        base_url,
        default_headers,
    })
}

fn resolve_secret_ref(secret_ref: &str) -> Result<String> {
    if let Ok(value) = std::env::var(secret_ref) {
        return Ok(value);
    }

    let env_name = format!(
        "LATTICE_SECRET_{}",
        normalize_secret_ref_env_name(secret_ref)
    );
    std::env::var(&env_name).with_context(|| {
        format!("missing secret ref `{secret_ref}`; expected env `{secret_ref}` or `{env_name}`")
    })
}

fn normalize_secret_ref_env_name(secret_ref: &str) -> String {
    secret_ref
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn resource_bag_from_bindings_lock(path: &Path, flow_id: &str) -> Result<ResourceBag> {
    let lock = load_bindings_lock(path)?;

    let flow = lock
        .flows
        .get(flow_id)
        .ok_or_else(|| anyhow!("bindings.lock does not define bindings for flow_id `{flow_id}`"))?;

    let mut instance_names: BTreeSet<&str> = BTreeSet::new();
    for (resource_key, instance_name) in &flow.use_map {
        if !resource_key.starts_with("resource::") && !resource_key.starts_with("durability::") {
            return Err(anyhow!(
                "bindings.lock flow `{flow_id}` contains unsupported key `{resource_key}`"
            ));
        }

        let instance = lock.instances.get(instance_name).ok_or_else(|| {
            anyhow!("bindings.lock flow `{flow_id}` references unknown instance `{instance_name}`")
        })?;

        if !instance_provides(instance, resource_key) {
            return Err(anyhow!(
                "bindings.lock instance `{instance_name}` does not provide `{resource_key}`"
            ));
        }

        instance_names.insert(instance_name.as_str());
    }

    let mut bag = ResourceBag::new();
    for name in instance_names {
        let instance = lock.instances.get(name).expect("instance exists");
        validate_lock_instance_well_formed(name, instance)?;

        match instance.provider_kind.as_str() {
            "kv.memory" => {
                bag = bag.with_kv(Arc::new(capabilities::kv::MemoryKv::new()));
            }
            "blob.memory" => {
                bag = bag.with_blob(Arc::new(capabilities::blob::MemoryBlobStore::new()));
            }
            "checkpoint_store.memory" => {
                bag = attach_checkpoint_store(bag, CheckpointStoreKind::Memory, None);
            }
            "http.reqwest" => {
                let client = Arc::new(cap_http_reqwest::ReqwestHttpClient::default());
                bag = bag.with_http_read(Arc::clone(&client));
                bag = bag.with_http_write(client);
            }
            other => {
                return Err(anyhow!(
                    "unsupported provider_kind `{other}` for instance `{name}`"
                ));
            }
        }
    }

    let connector_runtime = BindingsLockConnectorRuntime::new(&lock, flow_id)?;
    bag = bag.with_connector_runtime(Arc::new(connector_runtime));

    Ok(bag)
}

fn attach_checkpoint_store(
    bag: ResourceBag,
    kind: CheckpointStoreKind,
    checkpoint_dir: Option<&Path>,
) -> ResourceBag {
    let bag = match kind {
        CheckpointStoreKind::Fs => {
            let root = checkpoint_dir
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".flow").join("checkpoints"));
            bag.with_checkpoint_store(Arc::new(local_durability::FsCheckpointStore::with_root(
                root,
            )))
        }
        CheckpointStoreKind::Memory => {
            bag.with_checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
        }
    };

    if bag.max_durability_mode() == DurabilityMode::Off {
        bag.with_max_durability_mode(DurabilityMode::Partial)
    } else {
        bag
    }
}

fn parse_payload_sources(payload: Option<&str>, payload_file: Option<&Path>) -> Result<JsonValue> {
    if payload.is_some() && payload_file.is_some() {
        return Err(anyhow!(
            "--payload and --payload-file cannot be supplied together"
        ));
    }

    if let Some(raw) = payload {
        let value = serde_json::from_str(raw).context("payload is not valid JSON")?;
        return Ok(value);
    }

    if let Some(path) = payload_file {
        let data = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let value = serde_json::from_str(&data)
            .with_context(|| format!("{} does not contain valid JSON", path.display()))?;
        return Ok(value);
    }

    Ok(json!({}))
}

fn parse_payload(args: &LocalArgs) -> Result<JsonValue> {
    parse_payload_sources(args.payload.as_deref(), args.payload_file.as_deref())
}

fn load_example(name: &str) -> Result<ExampleHandle> {
    let (bundle, is_streaming) = match name {
        #[cfg(feature = "example-s1")]
        "s1_echo" => (s1_echo::bundle(), false),
        #[cfg(feature = "example-s2")]
        "s2_site" => (s2_site::bundle(), true),
        #[cfg(feature = "example-s3")]
        "s3_branching" => (s3_branching::bundle(), false),
        #[cfg(feature = "example-s4")]
        "s4_preflight" => (s4_preflight::bundle(), false),
        #[cfg(feature = "example-s5")]
        "s5_unsupported_surface" => (s5_unsupported_surface::bundle(), false),
        #[cfg(feature = "example-s6")]
        "s6_spill" => (s6_spill::bundle(), false),
        #[cfg(feature = "example-s11")]
        "s11_lead_intake" => (s11_lead_intake::bundle(), false),
        #[cfg(feature = "example-github-issues")]
        "connector_github_issues_local_flow" => {
            (example_connector_github_issues_local_flow::bundle(), false)
        }
        #[cfg(feature = "example-google-sheets")]
        "connector_google_sheets_local_flow" => {
            (example_connector_google_sheets_local_flow::bundle(), false)
        }
        #[cfg(feature = "example-s12")]
        "s12_sheetport_quote" => (s12_sheetport_quote::bound_bundle(), false),
        #[cfg(feature = "example-s13")]
        "s13_github_issue_investigator" => (s13_github_issue_investigator::bundle(), false),
        other => return Err(anyhow!("unknown example `{other}`")),
    };

    example_from_bundle(bundle, is_streaming)
}

fn example_from_bundle(
    bundle: host_inproc::FlowBundle,
    is_streaming: bool,
) -> Result<ExampleHandle> {
    example_from_bundle_entrypoint(bundle, is_streaming, None, None)
}

fn example_from_bundle_entrypoint(
    bundle: host_inproc::FlowBundle,
    is_streaming: bool,
    trigger_alias: Option<&str>,
    capture_alias: Option<&str>,
) -> Result<ExampleHandle> {
    let entrypoint = select_bundle_entrypoint(&bundle, trigger_alias, capture_alias)?;
    let trigger_alias = entrypoint.trigger_alias.clone();
    let capture_alias = entrypoint.capture_alias.clone();
    let deadline = entrypoint.deadline;
    let method_str = entrypoint.method.as_deref().unwrap_or("POST");
    let method = method_str
        .parse::<Method>()
        .with_context(|| format!("invalid entrypoint method `{method_str}`"))?;
    let route_path = entrypoint.route_path.as_deref().unwrap_or("/").to_string();

    Ok(ExampleHandle {
        executor: bundle.executor(),
        ir: Arc::new(bundle.validated_ir),
        trigger_alias,
        capture_alias,
        deadline,
        route_path,
        method,
        is_streaming,
        environment_plugins: bundle.environment_plugins,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use base64::Engine as _;
    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::context;
    use connector_github_issues::{GithubIssueCreateInput, github_issues_create};
    use dag_core::diagnostic_codes;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    #[test]
    fn text_diagnostic_includes_summary_and_location() {
        let code = diagnostic_codes()
            .iter()
            .find(|c| c.code == "EFFECT201")
            .expect("EFFECT201 registered");
        let diag = Diagnostic::new(code, "node `writer` declares Pure but requires Effectful")
            .with_location("node:writer");
        let formatted = format_text_diagnostic(&diag);
        assert!(
            formatted.contains("[EFFECT201] error(validation):"),
            "formatted diagnostic missing header:\n{formatted}"
        );
        assert!(formatted.contains("summary: Declared effects do not match bound capabilities"));
        assert!(formatted.contains("location: node:writer"));
    }

    #[test]
    fn json_payload_serialises_determinism_hint() {
        let code = diagnostic_codes()
            .iter()
            .find(|c| c.code == "DET302")
            .expect("DET302 registered");
        let diag = Diagnostic::new(
            code,
            "node `clock` declares Strict determinism but uses clock APIs",
        );
        let payload = DiagnosticPayload::from(&diag);
        let response = GraphCheckResponse {
            status: GraphStatus::Error,
            node_count: 3,
            edge_count: 2,
            diagnostics: vec![payload],
        };
        let json = serde_json::to_string(&response).expect("serialize graph response");
        assert!(json.contains("\"status\":\"error\""));
        assert!(json.contains("\"code\":\"DET302\""));
    }

    fn temp_lock_path() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        let pid = std::process::id();
        let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        path.push(format!("lattice.bindings.lock.{pid}.{counter}.json"));
        path
    }

    fn lock_from_json(mut value: JsonValue) -> BindingsLock {
        let hash = compute_lock_content_hash(&value).expect("hash");
        value["content_hash"] = json!(hash);
        serde_json::from_value(value).expect("bindings lock")
    }

    fn connector_runtime_from_json(
        value: JsonValue,
        flow_id: &str,
    ) -> BindingsLockConnectorRuntime {
        let lock = lock_from_json(value);
        BindingsLockConnectorRuntime::new(&lock, flow_id).expect("connector runtime")
    }

    fn connector_scope(
        flow_id: &str,
        node_alias: &str,
        connector_id: &str,
    ) -> ConnectorBindingScope {
        ConnectorBindingScope::new(
            flow_id.to_string(),
            node_alias.to_string(),
            format!("tests::{node_alias}"),
            connector_id.to_string(),
        )
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    // Deterministic test-only RSA fixture used for service-account JWT coverage.
    // This path is allowlisted in `.gitleaks.toml`.
    const TEST_RSA_PRIVATE_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDIA/LEAPFqnUft
gmeGPFVtcWpJSkDfOtqucdzB7lhvV3qKjHgAijwySNPWYbwq+PqjULtMmD5ishZj
vy86n2oV4dT9wZllpywjgyiwClgLNTmNefZaV7MorK68/rLXlKRZ5w8krokQCDYK
lKU7PF4u2o5FC/iUT1jDXm7pq7YkldfVmq0QVGlABMNqnEmKHvmE4M0ZMv6g17+w
w0KPbbI5J1CCiZF0Dvx8775X3yLn4qEW7Euj4lx0Hb2Xc0plGY3LGG6qeApocjNH
RMg2c15Xb3bSo2JNOoR0CQ58c1ZIh+Eo9kf6foAftrrW6cDoWFdkgGX/3LsNRTz+
nAH4JFnhAgMBAAECggEAF1riq4FiryzLW8/oz7NW1E80dnddqNNB+rGf8eMnX2Tr
EaeCUanSipqXZcaGxsvI1G4WWMTEMBkUZTRLSwCXThPPH4xOIaEKFeF4TEoA6tod
rMfrfLQV3u9+/eGNt3+LS1YgHgvlREJ5MPYXbxnG85igmS5jKco0Fqf9snpS6+WA
W7J7RHLcGIO8FqGZ9Hn6F76zrnV5E2zu4V9Q+eU3KWvQatPjiDEQq0rArTPFgV/v
2WxnMJxwbZ/VPbZR2Mx2HQkOaw74kwZeQKxKWzP2ndw7bv1GUFA9FfhuDWYHRsQ2
mxV2Zgf7JqOTcRcsd0L2KE7ArAjaO5lx3YgEnQKc2wKBgQDwZwqKDoZehUVP/l4s
GpQMGV+rJvD/imUczCJsZnymWlawr2PMLLvG+pNWg4LALISYyhOhW/b4x8W/eHFr
Cmd3Z8LIJIheDNTREMhpaHqutQgORJEtSZjD2Wehhpgm3l9jifFuDh8KbRPUwRyH
TPis/Vz17RWwyvOxei6EoFgOcwKBgQDU/hlYjRUpaQLFlpI63zHbLjmlT7Q0SBNA
UDCwpuLcPsrxVB2lbbWQ25VCnUx9DftSTZt5wbfNpowvHJi+e42TiUgNp8LTLiuv
FjO+HNwjzmZcjfsDkUmOe/hi4UeiZmDYxV7kE5nHeE2fwtnNMAIiJ/LQecMZswQi
uegKUD8tWwKBgHD/rieIfkZtlE/ue6t1bsNlJd/YNQ2YqsBnf4K+hbbX3cm9F0bA
fB8iZyESPeJAyq7axXFiPetgU6YVYhJzWID6x8a1zVeP5nTC08EgOBJoy3mRZ0AH
SQQ964U0M86JVgL+svoNLzACZ4DoqJU8a+M8UHbUUw6/xt5UVQtIJzvbAoGABsrb
sBE/vYRVzEtS+oGnq1+8AuOZ0ZkC1Cg6hUetMGzoN+4AzAfFpIr8JZWynMJXY3aK
IMXmwK4xBkeZL2ntR+k23Qiek/GC/yBsIgH1m0a3yPfWK3T0rZCSiUS57hnpuMAC
mK9vVgcmIpQqMfr39nLjsXZQnH8zAJCBL+MDQMUCgYBC+zi/0AQU1Vo3DDyEoYb6
zvp9oL/loKZA0xJlSz2ZO8ychRWnuJtf5SpAm30O4VJQcOvbV2hwd7sJsh4sAnY1
qIbhEamp5tjBbAdxLON1Q3Qpyt/uemzi+TSKJbcZ3OQHe2bylylyYYh+4zGCSGMY
RGKOKF9RKKgFGiXk5I97qQ==
-----END PRIVATE KEY-----"#;

    fn decode_jwt_payload(jwt: &str) -> JsonValue {
        let payload = jwt.split('.').nth(1).expect("jwt payload");
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .expect("base64 payload");
        serde_json::from_slice(&bytes).expect("payload json")
    }

    #[test]
    fn bindings_lock_hash_round_trips() {
        let mut lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {}
        });
        let hash = compute_lock_content_hash(&lock).expect("hash");
        lock["content_hash"] = json!(hash);

        let path = temp_lock_path();
        std::fs::write(&path, serde_json::to_vec(&lock).expect("json")).expect("write");

        let loaded = load_bindings_lock(&path).expect("load lock");
        assert_eq!(loaded.version, 1);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bindings_lock_hash_mismatch_rejected() {
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "deadbeef",
            "instances": {},
            "flows": {}
        });

        let path = temp_lock_path();
        std::fs::write(&path, serde_json::to_vec(&lock).expect("json")).expect("write");

        let err = load_bindings_lock(&path).expect_err("expected mismatch");
        let msg = err.to_string();
        assert!(msg.contains("content_hash mismatch"), "{msg}");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bindings_lock_builds_resource_bag_for_flow() {
        use capabilities::ResourceAccess;

        let flow_id = "test-flow";
        let mut lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {
                "kv1": {
                    "provider_kind": "kv.memory",
                    "provides": ["resource::kv"],
                    "connect": {},
                    "config": {},
                    "isolation": []
                }
            },
            "flows": {
                flow_id: {
                    "use": {
                        "resource::kv": "kv1"
                    }
                }
            }
        });
        let hash = compute_lock_content_hash(&lock).expect("hash");
        lock["content_hash"] = json!(hash);

        let path = temp_lock_path();
        std::fs::write(&path, serde_json::to_vec(&lock).expect("json")).expect("write");

        let bag = resource_bag_from_bindings_lock(&path, flow_id).expect("bag");
        assert!(bag.kv().is_some());
        assert!(bag.connector_runtime().is_some());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bindings_lock_builds_connector_runtime_for_flow() {
        use capabilities::ResourceAccess;

        let flow_id = "test-flow";
        let mut lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "endpoint.github_public": {
                    "provider_kind": "endpoint.profile.static",
                    "handle_kind": "endpoint.profile",
                    "connect": {},
                    "config": {
                        "base_url": "https://api.github.com",
                        "default_headers": { "Accept": "application/json" }
                    },
                    "grants": {}
                }
            },
            "connector_connections": {
                "github_primary": {
                    "connector_id": "connector.github.issues",
                    "roles": {
                        "endpoint_profile.github_default": "endpoint.github_public"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "github_primary"
                    },
                    "nodes": {}
                }
            }
        });
        let hash = compute_lock_content_hash(&lock).expect("hash");
        lock["content_hash"] = json!(hash);

        let path = temp_lock_path();
        std::fs::write(&path, serde_json::to_vec(&lock).expect("json")).expect("write");

        let bag = resource_bag_from_bindings_lock(&path, flow_id).expect("bag");
        assert!(bag.connector_runtime().is_some());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bindings_lock_rejects_unknown_connector_connection_reference() {
        let flow_id = "test-flow";
        let mut lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {},
            "connector_connections": {},
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "missing_connection"
                    },
                    "nodes": {}
                }
            }
        });
        let hash = compute_lock_content_hash(&lock).expect("hash");
        lock["content_hash"] = json!(hash);

        let path = temp_lock_path();
        std::fs::write(&path, serde_json::to_vec(&lock).expect("json")).expect("write");

        let err = resource_bag_from_bindings_lock(&path, flow_id)
            .err()
            .expect("expected connector binding reject");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown connection `missing_connection`"),
            "{msg}"
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bindings_lock_rejects_non_object_connect() {
        let flow_id = "test-flow";
        let mut lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {
                "kv1": {
                    "provider_kind": "kv.memory",
                    "provides": ["resource::kv"],
                    "connect": "oops",
                    "config": {},
                    "isolation": []
                }
            },
            "flows": {
                flow_id: {
                    "use": {
                        "resource::kv": "kv1"
                    }
                }
            }
        });
        let hash = compute_lock_content_hash(&lock).expect("hash");
        lock["content_hash"] = json!(hash);

        let path = temp_lock_path();
        std::fs::write(&path, serde_json::to_vec(&lock).expect("json")).expect("write");

        let err = resource_bag_from_bindings_lock(&path, flow_id)
            .err()
            .expect("expected connect reject");
        let msg = err.to_string();
        assert!(msg.contains("invalid `connect`"), "{msg}");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bindings_lock_rejects_isolation_wrappers_until_supported() {
        let flow_id = "test-flow";
        let mut lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {
                "kv1": {
                    "provider_kind": "kv.memory",
                    "provides": ["resource::kv"],
                    "connect": {},
                    "config": {},
                    "isolation": [{"kind": "isolation.prefix_keys", "config": {}}]
                }
            },
            "flows": {
                flow_id: {
                    "use": {
                        "resource::kv": "kv1"
                    }
                }
            }
        });
        let hash = compute_lock_content_hash(&lock).expect("hash");
        lock["content_hash"] = json!(hash);

        let path = temp_lock_path();
        std::fs::write(&path, serde_json::to_vec(&lock).expect("json")).expect("write");

        let err = resource_bag_from_bindings_lock(&path, flow_id)
            .err()
            .expect("expected isolation reject");
        let msg = err.to_string();
        assert!(msg.contains("isolation wrappers"), "{msg}");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn bindings_lock_rejects_default_binding_connector_mismatch() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "endpoint.github_public": {
                    "provider_kind": "endpoint.profile.static",
                    "handle_kind": "endpoint.profile",
                    "connect": {},
                    "config": { "base_url": "https://api.github.com" },
                    "grants": {}
                }
            },
            "connector_connections": {
                "github_primary": {
                    "connector_id": "connector.slack.events",
                    "roles": {
                        "endpoint_profile.github_default": "endpoint.github_public"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "github_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected connector mismatch");
        let msg = err.to_string();
        assert!(msg.contains("targeting `connector.slack.events`"), "{msg}");
    }

    #[test]
    fn bindings_lock_rejects_non_object_connector_connection_config() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {},
            "connector_connections": {
                "sheetport_primary": {
                    "connector_id": "connector.formualizer.sheetport",
                    "roles": {},
                    "config": "not-an-object"
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.formualizer.sheetport": "sheetport_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected non-object config reject");
        let msg = err.to_string();
        assert!(msg.contains("invalid `config` (expected object)"), "{msg}");
    }

    #[test]
    fn bindings_lock_rejects_sheetport_connection_without_workbook_source() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {},
            "connector_connections": {
                "sheetport_primary": {
                    "connector_id": "connector.formualizer.sheetport",
                    "roles": {},
                    "config": {
                        "manifest_source": {
                            "kind": "inline_yaml",
                            "value": "spec: fio\n..."
                        }
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.formualizer.sheetport": "sheetport_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected missing workbook_source reject");
        let msg = err.to_string();
        assert!(msg.contains("config.workbook_source"), "{msg}");
    }

    #[test]
    fn bindings_lock_rejects_sheetport_connection_with_invalid_manifest_kind() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {},
            "connector_connections": {
                "sheetport_primary": {
                    "connector_id": "connector.formualizer.sheetport",
                    "roles": {},
                    "config": {
                        "workbook_source": {
                            "kind": "blob",
                            "key": "models/quote.xlsx"
                        },
                        "manifest_source": {
                            "kind": "remote_url",
                            "value": "https://example.test/model.fio.yaml"
                        }
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.formualizer.sheetport": "sheetport_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected invalid manifest kind reject");
        let msg = err.to_string();
        assert!(
            msg.contains("config.manifest_source.kind` `remote_url`"),
            "{msg}"
        );
    }

    #[test]
    fn bindings_lock_accepts_sheetport_materialized_blob_workbook_source() {
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": {
                    flow_id: { "use": {} }
                },
                "connector_handles": {},
                "connector_connections": {
                    "sheetport_primary": {
                        "connector_id": "connector.formualizer.sheetport",
                        "roles": {},
                        "config": {
                            "workbook_source": {
                                "kind": "materialized_blob",
                                "key": "models/quote.materialized.json",
                                "format": "workbook_json_v1"
                            },
                            "manifest_source": {
                                "kind": "inline_yaml",
                                "value": "spec: fio\n..."
                            }
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.formualizer.sheetport": "sheetport_primary"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );

        let resolved = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(runtime.resolve_connection(&connector_scope(
                flow_id,
                "node",
                "connector.formualizer.sheetport",
            )))
            .expect("resolve connection")
            .expect("resolved connection");

        assert_eq!(resolved.connector_id, "connector.formualizer.sheetport");
        assert_eq!(
            resolved.config["workbook_source"]["kind"],
            json!("materialized_blob")
        );
        assert_eq!(
            resolved.config["workbook_source"]["format"],
            json!("workbook_json_v1")
        );
    }

    #[test]
    fn bindings_lock_runtime_derives_sheetport_blob_hint_for_bound_mode() {
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": {
                    flow_id: { "use": {} }
                },
                "connector_handles": {},
                "connector_connections": {
                    "sheetport_primary": {
                        "connector_id": "connector.formualizer.sheetport",
                        "roles": {},
                        "config": {
                            "workbook_source": {
                                "kind": "blob",
                                "key": "models/quote.xlsx"
                            },
                            "manifest_source": {
                                "kind": "inline_yaml",
                                "value": "spec: fio\n..."
                            }
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.formualizer.sheetport": "sheetport_primary"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );

        let hints = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(runtime.resolve_required_effect_hints(
                &connector_scope(flow_id, "node", "connector.formualizer.sheetport"),
                dag_core::ConnectorResolutionModeDecl::BoundConnection,
            ))
            .expect("derive hints");

        assert_eq!(hints, vec![capabilities::blob::HINT_BLOB_READ.to_string()]);
    }

    #[test]
    fn bindings_lock_runtime_derives_sheetport_blob_hint_for_materialized_blob_bound_mode() {
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": {
                    flow_id: { "use": {} }
                },
                "connector_handles": {},
                "connector_connections": {
                    "sheetport_primary": {
                        "connector_id": "connector.formualizer.sheetport",
                        "roles": {},
                        "config": {
                            "workbook_source": {
                                "kind": "materialized_blob",
                                "key": "models/quote.materialized.json",
                                "format": "workbook_json_v1"
                            },
                            "manifest_source": {
                                "kind": "inline_yaml",
                                "value": "spec: fio\n..."
                            }
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.formualizer.sheetport": "sheetport_primary"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );

        let hints = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(runtime.resolve_required_effect_hints(
                &connector_scope(flow_id, "node", "connector.formualizer.sheetport"),
                dag_core::ConnectorResolutionModeDecl::BoundConnection,
            ))
            .expect("derive hints");

        assert_eq!(hints, vec![capabilities::blob::HINT_BLOB_READ.to_string()]);
    }

    #[test]
    fn bindings_lock_runtime_skips_sheetport_blob_hint_for_late_bound_mode() {
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": {
                    flow_id: { "use": {} }
                },
                "connector_handles": {},
                "connector_connections": {
                    "sheetport_primary": {
                        "connector_id": "connector.formualizer.sheetport",
                        "roles": {},
                        "config": {
                            "workbook_source": {
                                "kind": "blob",
                                "key": "models/quote.xlsx"
                            },
                            "manifest_source": {
                                "kind": "inline_yaml",
                                "value": "spec: fio\n..."
                            }
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.formualizer.sheetport": "sheetport_primary"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );

        let hints = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(runtime.resolve_required_effect_hints(
                &connector_scope(flow_id, "node", "connector.formualizer.sheetport"),
                dag_core::ConnectorResolutionModeDecl::LateBoundRefs,
            ))
            .expect("derive hints");

        assert!(hints.is_empty());
    }

    #[test]
    fn bindings_lock_runtime_resolves_connector_connection_config() {
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": {
                    flow_id: { "use": {} }
                },
                "connector_handles": {},
                "connector_connections": {
                    "sheetport_primary": {
                        "connector_id": "connector.formualizer.sheetport",
                        "roles": {},
                        "config": {
                            "workbook_source": {
                                "kind": "blob",
                                "key": "models/quote.xlsx"
                            },
                            "manifest_source": {
                                "kind": "inline_yaml",
                                "value": "spec: fio\n..."
                            },
                            "eval_defaults": {
                                "freeze_volatile": true,
                                "rng_seed": 7
                            }
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.formualizer.sheetport": "sheetport_primary"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );

        let resolved = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(runtime.resolve_connection(&connector_scope(
                flow_id,
                "node",
                "connector.formualizer.sheetport",
            )))
            .expect("connection resolution")
            .expect("bound connection present");

        assert_eq!(
            resolved.connection_name.as_deref(),
            Some("sheetport_primary")
        );
        assert_eq!(resolved.connector_id, "connector.formualizer.sheetport");
        assert_eq!(
            resolved.config["workbook_source"]["key"],
            json!("models/quote.xlsx")
        );
        assert_eq!(resolved.config["eval_defaults"]["rng_seed"], json!(7));
    }

    #[test]
    fn bindings_lock_rejects_unknown_connector_role_kind() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "endpoint.github_public": {
                    "provider_kind": "endpoint.profile.static",
                    "handle_kind": "endpoint.profile",
                    "connect": {},
                    "config": { "base_url": "https://api.github.com" },
                    "grants": {}
                }
            },
            "connector_connections": {
                "github_primary": {
                    "connector_id": "connector.github.issues",
                    "roles": {
                        "mystery.github_default": "endpoint.github_public"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "github_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected role kind reject");
        let msg = err.to_string();
        assert!(msg.contains("unsupported role kind `mystery`"), "{msg}");
    }

    #[test]
    fn bindings_lock_rejects_role_provider_family_mismatch() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "auth.github_secret": {
                    "provider_kind": "auth.static_secret",
                    "handle_kind": "raw.secret",
                    "connect": { "secret_ref": "github_secret" },
                    "config": {},
                    "grants": {}
                }
            },
            "connector_connections": {
                "github_primary": {
                    "connector_id": "connector.github.issues",
                    "roles": {
                        "endpoint_profile.github_default": "auth.github_secret"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "github_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected role/provider mismatch");
        let msg = err.to_string();
        assert!(
            msg.contains("expects the matching provider family"),
            "{msg}"
        );
    }

    #[test]
    fn bindings_lock_rejects_static_secret_handle_without_secret_ref() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "auth.github_secret": {
                    "provider_kind": "auth.static_secret",
                    "handle_kind": "raw.secret",
                    "connect": {},
                    "config": {},
                    "grants": {}
                }
            },
            "connector_connections": {
                "github_primary": {
                    "connector_id": "connector.github.issues",
                    "roles": {
                        "outbound_auth.github_pat": "auth.github_secret"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "github_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected missing secret_ref reject");
        let msg = err.to_string();
        assert!(msg.contains("connect.secret_ref"), "{msg}");
    }

    #[test]
    fn bindings_lock_rejects_static_endpoint_header_values_that_are_not_strings() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "endpoint.github_public": {
                    "provider_kind": "endpoint.profile.static",
                    "handle_kind": "endpoint.profile",
                    "connect": {},
                    "config": {
                        "base_url": "https://api.github.com",
                        "default_headers": {
                            "Accept": 42
                        }
                    },
                    "grants": {}
                }
            },
            "connector_connections": {
                "github_primary": {
                    "connector_id": "connector.github.issues",
                    "roles": {
                        "endpoint_profile.github_default": "endpoint.github_public"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "github_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected header value reject");
        let msg = err.to_string();
        assert!(
            msg.contains("non-string `config.default_headers.Accept`"),
            "{msg}"
        );
    }

    #[test]
    fn bindings_lock_runtime_prefers_node_binding_over_connector_default() {
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": {
                    flow_id: { "use": {} }
                },
                "connector_handles": {
                    "endpoint.github_default": {
                        "provider_kind": "endpoint.profile.static",
                        "handle_kind": "endpoint.profile",
                        "connect": {},
                        "config": { "base_url": "https://default.example.test" },
                        "grants": {}
                    },
                    "endpoint.github_override": {
                        "provider_kind": "endpoint.profile.static",
                        "handle_kind": "endpoint.profile",
                        "connect": {},
                        "config": { "base_url": "https://node.example.test" },
                        "grants": {}
                    }
                },
                "connector_connections": {
                    "github_default": {
                        "connector_id": "connector.github.issues",
                        "roles": {
                            "endpoint_profile.github_default": "endpoint.github_default"
                        }
                    },
                    "github_override": {
                        "connector_id": "connector.github.issues",
                        "roles": {
                            "endpoint_profile.github_default": "endpoint.github_override"
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.github.issues": "github_default"
                        },
                        "nodes": {
                            "target_node": "github_override"
                        }
                    }
                }
            }),
            flow_id,
        );
        let profile = EndpointProfileDescriptor {
            connector_id: "connector.github.issues",
            name: "github_default",
            env_base_url_var: "LATTICE_CONNECTOR_ENDPOINT_GITHUB_DEFAULT_BASE_URL",
            base_url: "https://ignored.example.test",
            default_headers: &[],
        };

        let resolved = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(runtime.resolve_endpoint_profile(
                &connector_scope(flow_id, "target_node", "connector.github.issues"),
                &profile,
            ))
            .expect("endpoint profile");

        assert_eq!(resolved.base_url, "https://node.example.test");
    }

    #[test]
    fn bindings_lock_runtime_applies_query_auth_from_static_secret_handle() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": {
                    flow_id: { "use": {} }
                },
                "connector_handles": {
                    "auth.demo_secret": {
                        "provider_kind": "auth.static_secret",
                        "handle_kind": "raw.secret",
                        "connect": { "secret_ref": "demo_query_secret" },
                        "config": {},
                        "grants": {}
                    }
                },
                "connector_connections": {
                    "demo_connection": {
                        "connector_id": "connector.demo",
                        "roles": {
                            "outbound_auth.demo_query": "auth.demo_secret"
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.demo": "demo_connection"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );
        let profile = OutboundAuthProfileDescriptor {
            connector_id: "connector.demo",
            name: "demo_query",
            env_var: "IGNORED_IN_LOCK_RUNTIME",
            kind: OutboundAuthKind::ApiKeyQuery {
                query_name: "token",
                handle_kind: "raw.secret",
            },
        };
        let mut request = capabilities::http::HttpRequest::new(
            capabilities::http::HttpMethod::Get,
            "https://example.test/items?existing=1",
        );

        unsafe {
            std::env::set_var("demo_query_secret", "abc 123");
        }
        let result = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(runtime.apply_outbound_auth(
                &connector_scope(flow_id, "node", "connector.demo"),
                &profile,
                &mut request,
            ));
        unsafe {
            std::env::remove_var("demo_query_secret");
        }

        result.expect("auth application");
        assert_eq!(
            request.url,
            "https://example.test/items?existing=1&token=abc%20123"
        );
    }

    #[test]
    fn bindings_lock_rejects_oauth2_refresh_handle_without_token_url() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "auth.github_oauth": {
                    "provider_kind": "auth.oauth2.refresh",
                    "handle_kind": "http.bearer",
                    "connect": {
                        "client_id_ref": "github_client_id",
                        "client_secret_ref": "github_client_secret",
                        "refresh_token_ref": "github_refresh_token"
                    },
                    "config": {},
                    "grants": {}
                }
            },
            "connector_connections": {
                "github_primary": {
                    "connector_id": "connector.github.issues",
                    "roles": {
                        "outbound_auth.github_pat": "auth.github_oauth"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "github_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected missing token_url reject");
        let msg = err.to_string();
        assert!(msg.contains("config.token_url"), "{msg}");
    }

    #[tokio::test]
    async fn bindings_lock_runtime_fetches_and_caches_oauth2_refresh_tokens() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let token_server = MockServer::start();
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": { flow_id: { "use": {} } },
                "connector_handles": {
                    "auth.github_oauth": {
                        "provider_kind": "auth.oauth2.refresh",
                        "handle_kind": "http.bearer",
                        "connect": {
                            "client_id_ref": "github_client_id",
                            "client_secret_ref": "github_client_secret",
                            "refresh_token_ref": "github_refresh_token"
                        },
                        "config": {
                            "token_url": format!("{}/oauth/token", token_server.base_url())
                        },
                        "grants": {}
                    }
                },
                "connector_connections": {
                    "github_primary": {
                        "connector_id": "connector.github.issues",
                        "roles": {
                            "outbound_auth.github_pat": "auth.github_oauth"
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.github.issues": "github_primary"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );
        let profile = OutboundAuthProfileDescriptor {
            connector_id: "connector.github.issues",
            name: "github_pat",
            env_var: "IGNORED_IN_LOCK_RUNTIME",
            kind: OutboundAuthKind::Bearer {
                handle_kind: "http.bearer",
            },
        };
        let mut request_one = capabilities::http::HttpRequest::new(
            capabilities::http::HttpMethod::Post,
            "https://example.test/one",
        );
        let mut request_two = capabilities::http::HttpRequest::new(
            capabilities::http::HttpMethod::Post,
            "https://example.test/two",
        );

        unsafe {
            std::env::set_var("github_client_id", "client-id-123");
            std::env::set_var("github_client_secret", "client-secret-456");
            std::env::set_var("github_refresh_token", "refresh-token-789");
        }

        let token_mock = token_server.mock(|when, then| {
            when.method(POST)
                .path("/oauth/token")
                .body_contains("grant_type=refresh_token")
                .body_contains("client_id=client-id-123")
                .body_contains("client_secret=client-secret-456")
                .body_contains("refresh_token=refresh-token-789");
            then.status(200).json_body_obj(&serde_json::json!({
                "access_token": "oauth-access-token-1",
                "token_type": "Bearer",
                "expires_in": 3600
            }));
        });

        runtime
            .apply_outbound_auth(
                &connector_scope(flow_id, "node", "connector.github.issues"),
                &profile,
                &mut request_one,
            )
            .await
            .expect("first auth application");
        runtime
            .apply_outbound_auth(
                &connector_scope(flow_id, "node", "connector.github.issues"),
                &profile,
                &mut request_two,
            )
            .await
            .expect("second auth application");

        unsafe {
            std::env::remove_var("github_client_id");
            std::env::remove_var("github_client_secret");
            std::env::remove_var("github_refresh_token");
        }

        assert_eq!(
            request_one.headers.get("Authorization"),
            Some(&"Bearer oauth-access-token-1".to_string())
        );
        assert_eq!(
            request_two.headers.get("Authorization"),
            Some(&"Bearer oauth-access-token-1".to_string())
        );
        token_mock.assert_hits(1);
    }

    #[tokio::test]
    async fn github_create_node_executes_via_oauth2_refresh_bindings_runtime() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let token_server = MockServer::start();
        let api_server = MockServer::start();
        let flow_id = "flow://github-oauth-runtime";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": { flow_id: { "use": {} } },
                "connector_handles": {
                    "auth.github_oauth": {
                        "provider_kind": "auth.oauth2.refresh",
                        "handle_kind": "http.bearer",
                        "connect": {
                            "client_id_ref": "github_client_id",
                            "client_secret_ref": "github_client_secret",
                            "refresh_token_ref": "github_refresh_token"
                        },
                        "config": {
                            "token_url": format!("{}/oauth/token", token_server.base_url())
                        },
                        "grants": {}
                    },
                    "endpoint.github_local": {
                        "provider_kind": "endpoint.profile.static",
                        "handle_kind": "endpoint.profile",
                        "connect": {},
                        "config": {
                            "base_url": api_server.base_url(),
                            "default_headers": {
                                "Accept": "application/json",
                                "X-GitHub-Api-Version": "2022-11-28"
                            }
                        },
                        "grants": {}
                    }
                },
                "connector_connections": {
                    "github_oauth_local": {
                        "connector_id": "connector.github.issues",
                        "roles": {
                            "outbound_auth.github_pat": "auth.github_oauth",
                            "endpoint_profile.github_default": "endpoint.github_local"
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.github.issues": "github_oauth_local"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );
        let token_mock = token_server.mock(|when, then| {
            when.method(POST)
                .path("/oauth/token")
                .body_contains("grant_type=refresh_token")
                .body_contains("client_id=client-id-123")
                .body_contains("client_secret=client-secret-456")
                .body_contains("refresh_token=refresh-token-789");
            then.status(200).json_body_obj(&serde_json::json!({
                "access_token": "oauth-create-token",
                "token_type": "bearer",
                "expires_in": 3600
            }));
        });
        let create_mock = api_server.mock(|when, then| {
            when.method(POST)
                .path("/repos/octo/demo/issues")
                .header("authorization", "Bearer oauth-create-token")
                .header("accept", "application/json")
                .header("x-github-api-version", "2022-11-28")
                .json_body_obj(&serde_json::json!({
                    "title": "created through oauth runtime",
                    "body": "hello oauth"
                }));
            then.status(201).json_body_obj(&serde_json::json!({
                "number": 321,
                "title": "created through oauth runtime",
                "state": "open",
                "html_url": "https://example.test/issues/321"
            }));
        });

        unsafe {
            std::env::set_var("github_client_id", "client-id-123");
            std::env::set_var("github_client_secret", "client-secret-456");
            std::env::set_var("github_refresh_token", "refresh-token-789");
        }

        let http_client = Arc::new(ReqwestHttpClient::default());
        let resources = Arc::new(
            ResourceBag::default()
                .with_http_write(http_client)
                .with_connector_runtime(Arc::new(runtime))
                .with_connector_scope(connector_scope(
                    flow_id,
                    "create_issue",
                    "connector.github.issues",
                )),
        );

        let output = context::with_resources(resources, async {
            github_issues_create(GithubIssueCreateInput {
                owner: "octo".to_string(),
                repo: "demo".to_string(),
                title: "created through oauth runtime".to_string(),
                body: Some("hello oauth".to_string()),
            })
            .await
            .expect("create succeeds")
        })
        .await;

        unsafe {
            std::env::remove_var("github_client_id");
            std::env::remove_var("github_client_secret");
            std::env::remove_var("github_refresh_token");
        }

        token_mock.assert_hits(1);
        create_mock.assert();
        assert_eq!(output.number, 321);
        assert_eq!(output.title, "created through oauth runtime");
    }

    #[test]
    fn service_account_jwt_assertion_encodes_expected_claims() {
        let config = ServiceAccountJwtProviderConfig {
            token_url: "https://oauth.example.test/token".to_string(),
            scopes: vec!["scope:one".to_string(), "scope:two".to_string()],
            subject: Some("user@example.test".to_string()),
            token_lifetime_seconds: 900,
            extra_form_fields: Vec::new(),
        };

        let assertion = build_service_account_jwt_assertion(
            "handle.demo",
            "svc@example.test",
            TEST_RSA_PRIVATE_KEY_PEM,
            &config,
        )
        .expect("assertion");
        let payload = decode_jwt_payload(&assertion);

        assert_eq!(payload["iss"], json!("svc@example.test"));
        assert_eq!(payload["aud"], json!("https://oauth.example.test/token"));
        assert_eq!(payload["scope"], json!("scope:one scope:two"));
        assert_eq!(payload["sub"], json!("user@example.test"));
        let iat = payload["iat"].as_u64().expect("iat");
        let exp = payload["exp"].as_u64().expect("exp");
        assert_eq!(exp - iat, 900);
    }

    #[test]
    fn bindings_lock_rejects_service_account_handle_without_scopes() {
        let flow_id = "test-flow";
        let lock = json!({
            "version": 1,
            "generated_at": "2025-12-15T00:00:00Z",
            "content_hash": "",
            "instances": {},
            "flows": {
                flow_id: { "use": {} }
            },
            "connector_handles": {
                "auth.google_sa": {
                    "provider_kind": "auth.service_account_jwt",
                    "handle_kind": "http.bearer",
                    "connect": {
                        "service_account_email_ref": "sa_email",
                        "private_key_ref": "sa_private_key"
                    },
                    "config": {
                        "token_url": "https://oauth.example.test/token"
                    },
                    "grants": {}
                }
            },
            "connector_connections": {
                "google_primary": {
                    "connector_id": "connector.github.issues",
                    "roles": {
                        "outbound_auth.github_pat": "auth.google_sa"
                    }
                }
            },
            "connector_bindings": {
                flow_id: {
                    "defaults": {
                        "connector.github.issues": "google_primary"
                    },
                    "nodes": {}
                }
            }
        });

        let err = BindingsLockConnectorRuntime::new(&lock_from_json(lock), flow_id)
            .err()
            .expect("expected missing scopes reject");
        let msg = err.to_string();
        assert!(msg.contains("config.scopes"), "{msg}");
    }

    #[tokio::test]
    async fn bindings_lock_runtime_fetches_and_caches_service_account_tokens() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let token_server = MockServer::start();
        let flow_id = "test-flow";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": { flow_id: { "use": {} } },
                "connector_handles": {
                    "auth.google_sa": {
                        "provider_kind": "auth.service_account_jwt",
                        "handle_kind": "http.bearer",
                        "connect": {
                            "service_account_email_ref": "sa_email",
                            "private_key_ref": "sa_private_key"
                        },
                        "config": {
                            "token_url": format!("{}/oauth/token", token_server.base_url()),
                            "scopes": ["scope:one", "scope:two"],
                            "subject": "user@example.test",
                            "token_lifetime_seconds": 900
                        },
                        "grants": {}
                    }
                },
                "connector_connections": {
                    "google_primary": {
                        "connector_id": "connector.github.issues",
                        "roles": {
                            "outbound_auth.github_pat": "auth.google_sa"
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.github.issues": "google_primary"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );
        let profile = OutboundAuthProfileDescriptor {
            connector_id: "connector.github.issues",
            name: "github_pat",
            env_var: "IGNORED_IN_LOCK_RUNTIME",
            kind: OutboundAuthKind::Bearer {
                handle_kind: "http.bearer",
            },
        };
        let mut request_one = capabilities::http::HttpRequest::new(
            capabilities::http::HttpMethod::Post,
            "https://example.test/one",
        );
        let mut request_two = capabilities::http::HttpRequest::new(
            capabilities::http::HttpMethod::Post,
            "https://example.test/two",
        );

        unsafe {
            std::env::set_var("sa_email", "svc@example.test");
            std::env::set_var("sa_private_key", TEST_RSA_PRIVATE_KEY_PEM);
        }

        let token_mock = token_server.mock(|when, then| {
            when.method(POST)
                .path("/oauth/token")
                .body_contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer")
                .body_contains("assertion=");
            then.status(200).json_body_obj(&serde_json::json!({
                "access_token": "service-account-access-token-1",
                "token_type": "Bearer",
                "expires_in": 3600
            }));
        });

        runtime
            .apply_outbound_auth(
                &connector_scope(flow_id, "node", "connector.github.issues"),
                &profile,
                &mut request_one,
            )
            .await
            .expect("first auth application");
        runtime
            .apply_outbound_auth(
                &connector_scope(flow_id, "node", "connector.github.issues"),
                &profile,
                &mut request_two,
            )
            .await
            .expect("second auth application");

        unsafe {
            std::env::remove_var("sa_email");
            std::env::remove_var("sa_private_key");
        }

        assert_eq!(
            request_one.headers.get("Authorization"),
            Some(&"Bearer service-account-access-token-1".to_string())
        );
        assert_eq!(
            request_two.headers.get("Authorization"),
            Some(&"Bearer service-account-access-token-1".to_string())
        );
        token_mock.assert_hits(1);
    }

    #[tokio::test]
    async fn github_create_node_executes_via_service_account_bindings_runtime() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let token_server = MockServer::start();
        let api_server = MockServer::start();
        let flow_id = "flow://github-service-account-runtime";
        let runtime = connector_runtime_from_json(
            json!({
                "version": 1,
                "generated_at": "2025-12-15T00:00:00Z",
                "content_hash": "",
                "instances": {},
                "flows": { flow_id: { "use": {} } },
                "connector_handles": {
                    "auth.google_sa": {
                        "provider_kind": "auth.service_account_jwt",
                        "handle_kind": "http.bearer",
                        "connect": {
                            "service_account_email_ref": "sa_email",
                            "private_key_ref": "sa_private_key"
                        },
                        "config": {
                            "token_url": format!("{}/oauth/token", token_server.base_url()),
                            "scopes": ["scope:issues.write"],
                            "token_lifetime_seconds": 900
                        },
                        "grants": {}
                    },
                    "endpoint.github_local": {
                        "provider_kind": "endpoint.profile.static",
                        "handle_kind": "endpoint.profile",
                        "connect": {},
                        "config": {
                            "base_url": api_server.base_url(),
                            "default_headers": {
                                "Accept": "application/json",
                                "X-GitHub-Api-Version": "2022-11-28"
                            }
                        },
                        "grants": {}
                    }
                },
                "connector_connections": {
                    "github_service_account_local": {
                        "connector_id": "connector.github.issues",
                        "roles": {
                            "outbound_auth.github_pat": "auth.google_sa",
                            "endpoint_profile.github_default": "endpoint.github_local"
                        }
                    }
                },
                "connector_bindings": {
                    flow_id: {
                        "defaults": {
                            "connector.github.issues": "github_service_account_local"
                        },
                        "nodes": {}
                    }
                }
            }),
            flow_id,
        );
        let token_mock = token_server.mock(|when, then| {
            when.method(POST)
                .path("/oauth/token")
                .body_contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer")
                .body_contains("assertion=");
            then.status(200).json_body_obj(&serde_json::json!({
                "access_token": "service-account-create-token",
                "token_type": "bearer",
                "expires_in": 3600
            }));
        });
        let create_mock = api_server.mock(|when, then| {
            when.method(POST)
                .path("/repos/octo/demo/issues")
                .header("authorization", "Bearer service-account-create-token")
                .header("accept", "application/json")
                .header("x-github-api-version", "2022-11-28")
                .json_body_obj(&serde_json::json!({
                    "title": "created through service account runtime",
                    "body": "hello service account"
                }));
            then.status(201).json_body_obj(&serde_json::json!({
                "number": 654,
                "title": "created through service account runtime",
                "state": "open",
                "html_url": "https://example.test/issues/654"
            }));
        });

        unsafe {
            std::env::set_var("sa_email", "svc@example.test");
            std::env::set_var("sa_private_key", TEST_RSA_PRIVATE_KEY_PEM);
        }

        let http_client = Arc::new(ReqwestHttpClient::default());
        let resources = Arc::new(
            ResourceBag::default()
                .with_http_write(http_client)
                .with_connector_runtime(Arc::new(runtime))
                .with_connector_scope(connector_scope(
                    flow_id,
                    "create_issue",
                    "connector.github.issues",
                )),
        );

        let output = context::with_resources(resources, async {
            github_issues_create(GithubIssueCreateInput {
                owner: "octo".to_string(),
                repo: "demo".to_string(),
                title: "created through service account runtime".to_string(),
                body: Some("hello service account".to_string()),
            })
            .await
            .expect("create succeeds")
        })
        .await;

        unsafe {
            std::env::remove_var("sa_email");
            std::env::remove_var("sa_private_key");
        }

        token_mock.assert_hits(1);
        create_mock.assert();
        assert_eq!(output.number, 654);
        assert_eq!(output.title, "created through service account runtime");
    }
}
