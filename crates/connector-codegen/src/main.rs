use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use connector_codegen::{generate_files, write_generated_files};
use connector_spec::ConnectorManifest;

#[derive(Debug, Parser)]
#[command(name = "connector-codegen")]
#[command(about = "Generate a Phase-B connector crate from connector.yaml")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Generate {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate { manifest, out } => {
            let text = std::fs::read_to_string(&manifest)?;
            let manifest_model = ConnectorManifest::from_yaml_str(&text)?;
            let files = generate_files(&manifest_model, &text)?;
            write_generated_files(out, &files)?;
        }
    }
    Ok(())
}
