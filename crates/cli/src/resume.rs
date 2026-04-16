use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand, ValueEnum};
use flow_bundle::ExecPolicy;
use futures::StreamExt;
use host_inproc::HostRuntime;
use host_wasmtime::load_flow_bundle;
use kernel_exec::ExecutionResult;

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
    /// Explicit built-in example source (e.g. s1_echo, s6_spill).
    #[arg(long)]
    example: Option<String>,
    /// Flow bundle directory source (manifest.json + artifacts).
    #[arg(long)]
    bundle: Option<PathBuf>,
    /// Optional flow id when --bundle contains multiple flows.
    #[arg(long)]
    bundle_flow: Option<String>,
    /// Bind capability providers for required `resource::*` domains.
    #[arg(long = "bind")]
    bindings: Vec<String>,
    /// Path to a machine-generated `bindings.lock.json` file.
    #[arg(long)]
    bindings_lock: Option<PathBuf>,
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

enum ResumeSource {
    Example(String),
    Bundle { path: PathBuf, flow: Option<String> },
}

fn detect_example_for_flow(flow_id: &FlowId) -> Result<Option<String>> {
    const EXAMPLES: &[&str] = &[
        "s1_echo",
        "s2_site",
        "s3_branching",
        "s4_preflight",
        "s5_unsupported_surface",
        "s6_spill",
        "s11_lead_intake",
        "s12_sheetport_quote",
        "s13_github_issue_investigator",
    ];

    for candidate in EXAMPLES {
        let handle = crate::load_example(candidate)?;
        if handle.ir.flow().id == *flow_id {
            return Ok(Some((*candidate).to_string()));
        }
    }

    Ok(None)
}

fn resolve_source(args: &ResumeRunArgs, flow_id: &FlowId) -> Result<ResumeSource> {
    if args.example.is_some() && args.bundle.is_some() {
        return Err(anyhow!(
            "--example and --bundle are mutually exclusive for resume run"
        ));
    }

    if args.bundle_flow.is_some() && args.bundle.is_none() {
        return Err(anyhow!("--bundle-flow requires --bundle"));
    }

    if let Some(example) = &args.example {
        return Ok(ResumeSource::Example(example.clone()));
    }

    if let Some(path) = &args.bundle {
        return Ok(ResumeSource::Bundle {
            path: path.clone(),
            flow: args.bundle_flow.clone(),
        });
    }

    if let Some(example) = detect_example_for_flow(flow_id)? {
        return Ok(ResumeSource::Example(example));
    }

    Err(anyhow!(
        "unable to infer flow source for checkpoint flow `{}`; provide --example or --bundle",
        flow_id.as_str()
    ))
}

fn checkpoint_resources(args: &ResumeRunArgs, flow_id: &str) -> Result<capabilities::ResourceBag> {
    if args.bindings_lock.is_some() && !args.bindings.is_empty() {
        return Err(anyhow!("--bindings-lock cannot be combined with --bind"));
    }

    let mut resources = if let Some(lock_path) = &args.bindings_lock {
        crate::resource_bag_from_bindings_lock(lock_path.as_path(), flow_id)?
    } else {
        crate::resource_bag_from_bindings(&args.bindings)?
    };

    resources = crate::attach_checkpoint_store(
        resources,
        crate::CheckpointStoreKind::Fs,
        Some(args.checkpoint_dir.as_path()),
    );

    Ok(resources)
}

async fn print_execution_result(execution: ExecutionResult) -> Result<()> {
    match execution {
        ExecutionResult::Value(value) => {
            println!("{}", serde_json::to_string_pretty(&value)?);
            Ok(())
        }
        ExecutionResult::Halt { alias, payload } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "halted": true,
                    "node": alias,
                    "payload": payload,
                }))?
            );
            Ok(())
        }
        ExecutionResult::Stream(mut stream) => {
            while let Some(event) = stream.next().await {
                let payload = event.map_err(anyhow::Error::new)?;
                println!("{}", serde_json::to_string(&payload)?);
            }
            Ok(())
        }
    }
}

async fn resume_run(args: ResumeRunArgs) -> Result<()> {
    let store = FsCheckpointStore::with_root(&args.checkpoint_dir);
    let handle = find_handle(
        &store,
        &args.checkpoint_id,
        args.flow.as_deref(),
        args.run.as_deref(),
    )
    .await?;
    let _record = store.get(&handle).await.map_err(map_checkpoint_error)?;

    let resources = checkpoint_resources(&args, handle.flow_id.as_str())?;
    let source = resolve_source(&args, &handle.flow_id)?;

    let execution = match source {
        ResumeSource::Example(example_name) => {
            let example = crate::load_example(&example_name)?;
            if example.ir.flow().id != handle.flow_id {
                return Err(anyhow!(
                    "checkpoint flow `{}` does not match example `{}` flow `{}`",
                    handle.flow_id.as_str(),
                    example_name,
                    example.ir.flow().id.as_str(),
                ));
            }

            let runtime = HostRuntime::with_plugins(
                example.executor,
                example.ir.clone(),
                example.environment_plugins,
            )
            .with_resource_bag(resources);

            runtime
                .resume(&args.checkpoint_id)
                .await
                .map_err(|err| match &err {
                    kernel_exec::ExecutionError::MissingCapabilities { hints } => {
                        anyhow!("[CAP101] missing required capabilities: {hints:?}")
                    }
                    _ => anyhow::Error::new(err),
                })?
        }
        ResumeSource::Bundle { path, flow } => {
            let bundle = load_flow_bundle(
                &path,
                ExecPolicy::Wasm,
                flow.as_deref(),
                Arc::new(resources.clone()),
            )?;

            if bundle.validated_ir.flow().id != handle.flow_id {
                return Err(anyhow!(
                    "checkpoint flow `{}` does not match bundle flow `{}`",
                    handle.flow_id.as_str(),
                    bundle.validated_ir.flow().id.as_str(),
                ));
            }

            let runtime = HostRuntime::with_plugins(
                bundle.executor(),
                Arc::new(bundle.validated_ir.clone()),
                bundle.environment_plugins,
            )
            .with_resource_bag(resources);

            runtime
                .resume(&args.checkpoint_id)
                .await
                .map_err(|err| match &err {
                    kernel_exec::ExecutionError::MissingCapabilities { hints } => {
                        anyhow!("[CAP101] missing required capabilities: {hints:?}")
                    }
                    _ => anyhow::Error::new(err),
                })?
        }
    };

    print_execution_result(execution).await
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
