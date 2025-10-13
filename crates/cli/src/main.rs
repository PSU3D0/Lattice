use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use dag_core::{Diagnostic, Severity, diagnostic_codes};
use exporters::{to_dot, to_json_value};
use kernel_plan::validate;
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "flows",
    version,
    author,
    about = "LatticeFlow command-line interface"
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
}

#[derive(Subcommand, Debug)]
enum GraphCommand {
    /// Validate a Flow IR document and optionally emit artifacts.
    Check(GraphCheckArgs),
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Graph(GraphCommand::Check(args)) => run_graph_check(args),
    }
}

fn run_graph_check(args: GraphCheckArgs) -> Result<()> {
    if args.json {
        if args.emit_dot || args.dot.is_some() || args.pretty_json {
            return Err(anyhow!(
                "--json cannot be combined with --emit-dot, --dot, or --pretty-json"
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
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
