use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use axum::http::Method;
use clap::{Args, Parser, Subcommand};
use dag_core::{Diagnostic, Severity};
use exporters::{to_dot, to_json_value};
use futures::StreamExt;
use host_web_axum::{HostHandle, RouteConfig};
use kernel_exec::{ExecutionResult, FlowExecutor};
use kernel_plan::{ValidatedIR, validate};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::signal;

use capabilities::ResourceBag;
use host_inproc::{EnvironmentPlugin, HostRuntime, Invocation};

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use example_s1_echo as s1_echo;
use example_s2_site as s2_site;
use example_s3_branching as s3_branching;
use example_s4_preflight as s4_preflight;
use example_s5_unsupported_surface as s5_unsupported_surface;
use example_s6_spill as s6_spill;

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
}

#[derive(Subcommand, Debug)]
enum BindingsCommand {
    /// Work with machine-generated bindings lockfiles.
    #[command(subcommand)]
    Lock(LockCommand),
}

#[derive(Subcommand, Debug)]
enum LockCommand {
    /// Generate a bindings.lock.json for a built-in example.
    Generate(LockGenerateArgs),
}

#[derive(Args, Debug)]
struct LockGenerateArgs {
    /// Built-in example name (e.g. `s6_spill`).
    #[arg(long)]
    example: String,
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
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Built-in example to serve (e.g. `s6_spill`).
    #[arg(long, default_value = "s1_echo")]
    example: String,
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
        Command::Bindings(BindingsCommand::Lock(LockCommand::Generate(args))) => {
            run_bindings_lock_generate(args)
        }
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
    trigger_alias: &'static str,
    capture_alias: &'static str,
    deadline: Option<Duration>,
    route_path: &'static str,
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
    if json_mode && stream_mode {
        return Err(anyhow!("--json cannot be combined with --stream"));
    }
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

    let resources = if let Some(lock_path) = &args.bindings_lock {
        resource_bag_from_bindings_lock(lock_path.as_path(), ir.flow().id.as_str())?
    } else {
        resource_bag_from_bindings(&args.bindings)?
    };

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
                Invocation::new(trigger_alias, capture_alias, payload).with_deadline(deadline);

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
            .instantiate(ir.as_ref(), capture_alias)
            .map_err(anyhow::Error::new)?;

        for idx in 0..burst {
            let mut burst_payload = payload.clone();
            if let JsonValue::Object(map) = &mut burst_payload {
                map.insert("lf_burst_index".to_string(), JsonValue::from(idx as u64));
            }
            instance
                .send(trigger_alias, burst_payload)
                .await
                .map_err(anyhow::Error::new)?;
        }

        let mut results = Vec::with_capacity(burst);
        for _ in 0..burst {
            match instance.next().await {
                Some(Ok(kernel_exec::CaptureResult::Value(value))) => {
                    results.push(value);
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

fn run_serve(args: ServeArgs) -> Result<()> {
    let ServeArgs {
        example,
        addr,
        bindings,
        bindings_lock,
    } = args;
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
    } = load_example(&example)?;

    if bindings_lock.is_some() && !bindings.is_empty() {
        return Err(anyhow!("--bindings-lock cannot be combined with --bind"));
    }

    let resources = if let Some(lock_path) = bindings_lock {
        resource_bag_from_bindings_lock(lock_path.as_path(), ir.flow().id.as_str())?
    } else {
        resource_bag_from_bindings(&bindings)?
    };

    let runtime = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to initialise Tokio runtime")?;

    runtime.block_on(async move {
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("failed to bind {addr}"))?;

        let mut config = RouteConfig::new(route_path)
            .with_trigger_alias(trigger_alias)
            .with_capture_alias(capture_alias)
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
        println!("Serving example `{example}` on http://{local_addr}{route_path} (Ctrl+C to stop)");

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
                    "unsupported binding `{binding}`; supported: resource::kv=memory, resource::blob=memory, resource::http::read=reqwest, resource::http::write=reqwest"
                ));
            }
        }
    }

    Ok(bag)
}

#[derive(Debug, Serialize, Deserialize)]
struct BindingsLock {
    version: u32,
    generated_at: String,
    content_hash: String,
    #[serde(default)]
    instances: BTreeMap<String, LockInstance>,
    #[serde(default)]
    flows: BTreeMap<String, LockFlow>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
struct LockFlow {
    #[serde(rename = "use", default)]
    use_map: BTreeMap<String, String>,
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

fn provider_kind_from_binding(key: &str, token: &str) -> Option<&'static str> {
    match token {
        "memory" => {
            if key.starts_with("resource::kv") {
                Some("kv.memory")
            } else if key.starts_with("resource::blob") {
                Some("blob.memory")
            } else {
                None
            }
        }
        "kv.memory" => Some("kv.memory"),
        "blob.memory" => Some("blob.memory"),
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
        if !key.starts_with("resource::") {
            return Err(anyhow!(
                "invalid --bind `{binding}`; expected `resource::*` key after normalization"
            ));
        }

        let token = raw_value.trim();
        let provider_kind = provider_kind_from_binding(&key, token).ok_or_else(|| {
            anyhow!(
                "unsupported provider `{token}` in --bind `{binding}`; supported: memory, kv.memory, blob.memory, reqwest"
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

    let handle = load_example(&args.example)?;
    let flow = handle.ir.flow();
    let flow_id = flow.id.as_str().to_string();

    let required = required_resource_hints(flow);
    let overrides = parse_bindings_for_lock(&args.bindings)?;

    let mut use_map: BTreeMap<String, String> = BTreeMap::new();
    let mut instances: BTreeMap<String, LockInstance> = BTreeMap::new();

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

    let mut flows = BTreeMap::new();
    flows.insert(flow_id, LockFlow { use_map });

    let mut lock = BindingsLock {
        version: 1,
        generated_at: args.generated_at,
        content_hash: String::new(),
        instances,
        flows,
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
        if !provided.starts_with("resource::") {
            return Err(anyhow!(
                "bindings.lock instance `{name}` has non-resource provides entry `{provided}`"
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

fn resource_bag_from_bindings_lock(path: &Path, flow_id: &str) -> Result<ResourceBag> {
    let lock = load_bindings_lock(path)?;

    let flow = lock
        .flows
        .get(flow_id)
        .ok_or_else(|| anyhow!("bindings.lock does not define bindings for flow_id `{flow_id}`"))?;

    let mut instance_names: BTreeSet<&str> = BTreeSet::new();
    for (resource_key, instance_name) in &flow.use_map {
        if !resource_key.starts_with("resource::") {
            return Err(anyhow!(
                "bindings.lock flow `{flow_id}` contains non-resource key `{resource_key}`"
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

    Ok(bag)
}

fn parse_payload(args: &LocalArgs) -> Result<JsonValue> {
    if args.payload.is_some() && args.payload_file.is_some() {
        return Err(anyhow!(
            "--payload and --payload-file cannot be supplied together"
        ));
    }

    if let Some(raw) = &args.payload {
        let value = serde_json::from_str(raw).context("payload is not valid JSON")?;
        return Ok(value);
    }

    if let Some(path) = &args.payload_file {
        let data = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let value = serde_json::from_str(&data)
            .with_context(|| format!("{} does not contain valid JSON", path.display()))?;
        return Ok(value);
    }

    Ok(json!({}))
}

fn load_example(name: &str) -> Result<ExampleHandle> {
    match name {
        "s1_echo" => Ok(ExampleHandle {
            executor: s1_echo::executor(),
            ir: Arc::new(s1_echo::validated_ir()),
            trigger_alias: s1_echo::TRIGGER_ALIAS,
            capture_alias: s1_echo::CAPTURE_ALIAS,
            deadline: Some(s1_echo::DEADLINE),
            route_path: s1_echo::ROUTE_PATH,
            method: Method::POST,
            is_streaming: false,
            environment_plugins: s1_echo::environment_plugins(),
        }),
        "s2_site" => Ok(ExampleHandle {
            executor: s2_site::executor(),
            ir: Arc::new(s2_site::validated_ir()),
            trigger_alias: s2_site::TRIGGER_ALIAS,
            capture_alias: s2_site::CAPTURE_ALIAS,
            deadline: Some(s2_site::DEADLINE),
            route_path: s2_site::ROUTE_PATH,
            method: Method::POST,
            is_streaming: true,
            environment_plugins: Vec::new(),
        }),
        "s3_branching" => Ok(ExampleHandle {
            executor: s3_branching::executor(),
            ir: Arc::new(s3_branching::validated_ir()),
            trigger_alias: s3_branching::TRIGGER_ALIAS,
            capture_alias: s3_branching::CAPTURE_ALIAS,
            deadline: Some(s3_branching::DEADLINE),
            route_path: s3_branching::ROUTE_PATH,
            method: Method::POST,
            is_streaming: false,
            environment_plugins: Vec::new(),
        }),
        "s4_preflight" => Ok(ExampleHandle {
            executor: s4_preflight::executor(),
            ir: Arc::new(s4_preflight::validated_ir()),
            trigger_alias: s4_preflight::TRIGGER_ALIAS,
            capture_alias: s4_preflight::CAPTURE_ALIAS,
            deadline: Some(s4_preflight::DEADLINE),
            route_path: s4_preflight::ROUTE_PATH,
            method: Method::POST,
            is_streaming: false,
            environment_plugins: Vec::new(),
        }),
        "s5_unsupported_surface" => Ok(ExampleHandle {
            executor: s5_unsupported_surface::executor(),
            ir: Arc::new(s5_unsupported_surface::validated_ir()),
            trigger_alias: s5_unsupported_surface::TRIGGER_ALIAS,
            capture_alias: s5_unsupported_surface::CAPTURE_ALIAS,
            deadline: Some(s5_unsupported_surface::DEADLINE),
            route_path: s5_unsupported_surface::ROUTE_PATH,
            method: Method::POST,
            is_streaming: false,
            environment_plugins: Vec::new(),
        }),
        "s6_spill" => Ok(ExampleHandle {
            executor: s6_spill::executor(),
            ir: Arc::new(s6_spill::validated_ir()),
            trigger_alias: s6_spill::TRIGGER_ALIAS,
            capture_alias: s6_spill::CAPTURE_ALIAS,
            deadline: Some(s6_spill::DEADLINE),
            route_path: s6_spill::ROUTE_PATH,
            method: Method::POST,
            is_streaming: false,
            environment_plugins: Vec::new(),
        }),
        other => Err(anyhow!("unknown example `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dag_core::diagnostic_codes;
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
}
