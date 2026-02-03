use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand, ValueEnum};

use capabilities::durability::{
    CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStatus, CheckpointStore,
};
use dag_core::FlowId;
use tokio::runtime::Builder as RuntimeBuilder;

use crate::local_durability::FsCheckpointStore;

#[derive(Subcommand, Debug)]
pub enum ResumeCommand {
    /// List checkpoint records.
    List(ResumeListArgs),
    /// Show a checkpoint record.
    Show(ResumeShowArgs),
    /// Resume a checkpoint.
    Run(ResumeRunArgs),
}

#[derive(Args, Debug)]
pub struct ResumeListArgs {
    /// Filter by flow id.
    #[arg(long)]
    flow: Option<String>,
    /// Filter by run id.
    #[arg(long)]
    run: Option<String>,
    /// Filter by checkpoint status.
    #[arg(long, value_enum)]
    status: Option<ResumeStatus>,
    /// Filter to checkpoints due to resume (resume_after_ms <= now).
    #[arg(long)]
    due: bool,
    /// Root directory for filesystem checkpoints.
    #[arg(long, default_value = ".flow/checkpoints")]
    checkpoint_dir: PathBuf,
}

#[derive(Args, Debug)]
pub struct ResumeShowArgs {
    /// Checkpoint id to show.
    checkpoint_id: String,
    /// Filter by flow id.
    #[arg(long)]
    flow: Option<String>,
    /// Filter by run id.
    #[arg(long)]
    run: Option<String>,
    /// Root directory for filesystem checkpoints.
    #[arg(long, default_value = ".flow/checkpoints")]
    checkpoint_dir: PathBuf,
}

#[derive(Args, Debug)]
pub struct ResumeRunArgs {
    /// Checkpoint id to resume.
    checkpoint_id: String,
    /// Filter by flow id.
    #[arg(long)]
    flow: Option<String>,
    /// Filter by run id.
    #[arg(long)]
    run: Option<String>,
    /// Root directory for filesystem checkpoints.
    #[arg(long, default_value = ".flow/checkpoints")]
    checkpoint_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ResumeStatus {
    Active,
    Completed,
    Expired,
}

impl From<ResumeStatus> for CheckpointStatus {
    fn from(value: ResumeStatus) -> Self {
        match value {
            ResumeStatus::Active => CheckpointStatus::Active,
            ResumeStatus::Completed => CheckpointStatus::Completed,
            ResumeStatus::Expired => CheckpointStatus::Expired,
        }
    }
}

pub fn run_resume(command: ResumeCommand) -> Result<()> {
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialise Tokio runtime")?;

    runtime.block_on(async move {
        match command {
            ResumeCommand::List(args) => resume_list(args).await,
            ResumeCommand::Show(args) => resume_show(args).await,
            ResumeCommand::Run(args) => resume_run(args).await,
        }
    })
}

async fn resume_list(args: ResumeListArgs) -> Result<()> {
    let store = FsCheckpointStore::with_root(&args.checkpoint_dir);
    let status = if args.due && args.status.is_none() {
        Some(ResumeStatus::Active)
    } else {
        args.status
    };
    let filter = CheckpointFilter {
        flow_id: args.flow.as_ref().map(|flow| FlowId(flow.clone())),
        run_id: args.run.clone(),
        status: status.map(CheckpointStatus::from),
    };
    let handles = store.list(filter).await.map_err(map_checkpoint_error)?;
    let mut records = Vec::new();
    let now_ms = now_ms();

    for handle in handles {
        let record = store.get(&handle).await.map_err(map_checkpoint_error)?;
        if args.due && !is_due(&record, now_ms) {
            continue;
        }
        records.push(record);
    }

    records.sort_by(|left, right| {
        (
            left.created_at_ms,
            left.checkpoint_id.as_str(),
            left.flow_id.as_str(),
            left.run_id.as_str(),
        )
            .cmp(&(
                right.created_at_ms,
                right.checkpoint_id.as_str(),
                right.flow_id.as_str(),
                right.run_id.as_str(),
            ))
    });

    println!("{}", serde_json::to_string_pretty(&records)?);
    Ok(())
}

async fn resume_show(args: ResumeShowArgs) -> Result<()> {
    let store = FsCheckpointStore::with_root(&args.checkpoint_dir);
    let handle = find_handle(
        &store,
        &args.checkpoint_id,
        args.flow.as_deref(),
        args.run.as_deref(),
    )
    .await?;
    let record = store.get(&handle).await.map_err(map_checkpoint_error)?;
    println!("{}", serde_json::to_string_pretty(&record)?);
    Ok(())
}

async fn resume_run(args: ResumeRunArgs) -> Result<()> {
    let store = FsCheckpointStore::with_root(&args.checkpoint_dir);
    let _record = store
        .get(
            &find_handle(
                &store,
                &args.checkpoint_id,
                args.flow.as_deref(),
                args.run.as_deref(),
            )
            .await?,
        )
        .await
        .map_err(map_checkpoint_error)?;
    Err(anyhow!("resume execution not yet supported"))
}

async fn find_handle(
    store: &FsCheckpointStore,
    checkpoint_id: &str,
    flow: Option<&str>,
    run: Option<&str>,
) -> Result<CheckpointHandle> {
    let handles = store
        .list(CheckpointFilter {
            flow_id: flow.map(|flow_id| FlowId(flow_id.to_string())),
            run_id: run.map(|run_id| run_id.to_string()),
            status: None,
        })
        .await
        .map_err(map_checkpoint_error)?;

    let matches: Vec<CheckpointHandle> = handles
        .into_iter()
        .filter(|handle| handle.checkpoint_id == checkpoint_id)
        .collect();

    match matches.as_slice() {
        [] => Err(anyhow!("checkpoint `{checkpoint_id}` not found")),
        [handle] => Ok(handle.clone()),
        _ => Err(anyhow!(
            "checkpoint `{checkpoint_id}` matches multiple records; supply --flow or --run to disambiguate"
        )),
    }
}

fn is_due(record: &CheckpointRecord, now_ms: u64) -> bool {
    match record.resume_after_ms {
        Some(resume_after_ms) => resume_after_ms <= now_ms,
        None => false,
    }
}

fn now_ms() -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(now_ms).unwrap_or(u64::MAX)
}

fn map_checkpoint_error(err: capabilities::durability::CheckpointError) -> anyhow::Error {
    anyhow!(err)
}
