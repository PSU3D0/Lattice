use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::PathBuf;
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
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use tokio::net::TcpListener;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::signal;

use host_inproc::EnvironmentPlugin;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use example_s1_echo as s1_echo;
use example_s2_site as s2_site;

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
    /// Execute or serve workflows locally.
    #[command(subcommand)]
    Run(RunCommand),
}

#[derive(Subcommand, Debug)]
enum GraphCommand {
    /// Validate a Flow IR document and optionally emit artifacts.
    Check(GraphCheckArgs),
}

#[derive(Subcommand, Debug)]
enum RunCommand {
    /// Execute a workflow example in-process and print the result.
    Local(LocalArgs),
    /// Serve a workflow example over HTTP using the Axum host.
    Serve(ServeArgs),
}

#[derive(Args, Debug)]
struct LocalArgs {
    /// Built-in example to execute (e.g. `s1_echo`).
    #[arg(long, default_value = "s1_echo")]
    example: String,
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
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Built-in example to serve (e.g. `s1_echo`).
    #[arg(long, default_value = "s1_echo")]
    example: String,
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
        Command::Run(RunCommand::Local(args)) => run_local(args),
        Command::Run(RunCommand::Serve(args)) => run_serve(args),
    }
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
    if json_mode && stream_mode {
        return Err(anyhow!("--json cannot be combined with --stream"));
    }
    let payload = parse_payload(&args)?;
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
        environment_plugins: _,
        ..
    } = handle;

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
        let result: Result<RunOutcome> = match executor
            .run_once(ir.as_ref(), trigger_alias, payload, capture_alias, deadline)
            .await?
        {
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
        result
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
    let ServeArgs { example, addr } = args;
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
            .with_capture_alias(capture_alias);
        config = config.with_method(method);
        if let Some(deadline) = deadline {
            config = config.with_deadline(deadline);
        }
        for plugin in environment_plugins {
            config = config.with_environment_plugin(plugin);
        }

        let host = HostHandle::new(executor, ir, config);
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
}
