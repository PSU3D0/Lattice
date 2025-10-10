use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use exporters::{to_dot, to_json_value};
use kernel_plan::validate;

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
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Graph(GraphCommand::Check(args)) => run_graph_check(args),
    }
}

fn run_graph_check(args: GraphCheckArgs) -> Result<()> {
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

    match validate(&flow) {
        Ok(_) => {
            println!(
                "✓ graph is valid ({} nodes, {} edges)",
                flow.nodes.len(),
                flow.edges.len()
            );
        }
        Err(diags) => {
            eprintln!(
                "✗ graph validation failed with {} diagnostic(s):",
                diags.len()
            );
            for diag in diags {
                eprintln!("  [{}] {}", diag.code.code, diag.message);
            }
            return Err(anyhow!("graph validation failed"));
        }
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
