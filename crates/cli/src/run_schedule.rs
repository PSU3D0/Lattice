//! `flows run schedule` — the local dev scheduler for cron-triggered flows
//! (impl-docs/spec/schedule-trigger.md §7b, packet T2).
//!
//! Two modes:
//! - `--once` (or `--at <rfc3339>`): fire every schedule entrypoint (or the
//!   `--trigger` subset) immediately with a synthetic `ScheduledEvent`, print
//!   each result, and exit non-zero on any failure. This is the agent/test
//!   path and the priority.
//! - default (no `--once`): a tick loop that computes the next fire time from
//!   the cron expression(s) with the same parser used at validation (saffron),
//!   sleeps until then, and fires. Ctrl-C exits cleanly.
//!
//! The fire-time computation and synthetic-event construction live in
//! `host_inproc::schedule` (pure, unit-tested with a mocked clock); this module
//! owns only the CLI surface, the wall-clock/sleep injection, and printing.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use capabilities::ResourceAccess;
use chrono::{DateTime, Utc};
use clap::Args;
use dag_core::DurabilityMode;
use futures::StreamExt;
use host_inproc::schedule::{ScheduleClock, SystemClock, fire_invocation, next_fires, to_epoch_ms};
use host_inproc::{FlowEntrypoint, HostRuntime, Invocation};
use kernel_exec::{ExecutionError, ExecutionResult};
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use tokio::runtime::Builder as RuntimeBuilder;

use crate::{
    CheckpointStoreKind, attach_checkpoint_store, example_bundle, resource_bag_from_bindings,
    resource_bag_from_bindings_lock,
};

#[derive(Args, Debug)]
pub struct ScheduleArgs {
    /// Built-in example to run (must declare at least one schedule entrypoint).
    #[arg(long)]
    example: String,
    /// Fire once immediately instead of running the tick loop.
    #[arg(long)]
    once: bool,
    /// RFC3339 timestamp (UTC) to stamp the synthetic fire's
    /// `scheduled_time_ms`. Implies `--once`.
    #[arg(long)]
    at: Option<String>,
    /// Restrict firing to these trigger aliases (repeatable). Defaults to every
    /// schedule entrypoint.
    #[arg(long = "trigger")]
    trigger: Vec<String>,
    /// Bind capability providers for required `resource::*` domains
    /// (e.g. `--bind resource::kv=memory`).
    #[arg(long = "bind")]
    bindings: Vec<String>,
    /// Path to a machine-generated `bindings.lock.json` file.
    #[arg(long)]
    bindings_lock: Option<PathBuf>,
    /// Checkpoint store implementation (fs or memory).
    #[arg(long, value_enum)]
    checkpoint_store: Option<CheckpointStoreKind>,
    /// Root directory for filesystem checkpoints (used with
    /// `--checkpoint-store fs`).
    #[arg(long)]
    checkpoint_dir: Option<PathBuf>,
}

pub fn run_schedule(args: ScheduleArgs) -> Result<()> {
    if args.bindings_lock.is_some() && !args.bindings.is_empty() {
        return Err(anyhow!("--bindings-lock cannot be combined with --bind"));
    }

    // Validate `--at` up front so a malformed timestamp is reported before any
    // example loading. Presence of `--at` implies a single fire.
    let scheduled_at = match args.at.as_deref() {
        Some(raw) => Some(parse_at(raw)?),
        None => None,
    };
    let single_fire = args.once || scheduled_at.is_some();

    let (bundle, _is_streaming) = example_bundle(&args.example)?;
    let flow_id = bundle.validated_ir.flow().id.as_str().to_string();

    // Move the executor out first (borrows the bundle), then take ownership of
    // the entrypoints and IR.
    let executor = bundle.executor();
    let host_inproc::FlowBundle {
        validated_ir,
        entrypoints,
        environment_plugins,
        ..
    } = bundle;

    // Select the schedule-shaped entrypoints, then narrow to the requested
    // trigger subset (if any).
    let mut selected: Vec<FlowEntrypoint> = entrypoints
        .into_iter()
        .filter(|entry| entry.schedule.is_some())
        .collect();

    if selected.is_empty() {
        return Err(anyhow!(
            "example `{}` has no schedule entrypoints; `flows run schedule` requires a flow with \
             at least one `schedule:` entrypoint (see impl-docs/spec/schedule-trigger.md)",
            args.example
        ));
    }

    if !args.trigger.is_empty() {
        for alias in &args.trigger {
            if !selected.iter().any(|entry| &entry.trigger_alias == alias) {
                return Err(anyhow!(
                    "trigger alias `{alias}` is not a schedule entrypoint of example `{}`",
                    args.example
                ));
            }
        }
        selected.retain(|entry| args.trigger.contains(&entry.trigger_alias));
    }

    // Resource bag mirrors `flows run local`: bindings (or a lock), then a
    // checkpoint store default so durable schedule flows can run.
    let mut resources = if let Some(lock_path) = &args.bindings_lock {
        resource_bag_from_bindings_lock(lock_path.as_path(), &flow_id)?
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

    // Clock is read exactly once at this CLI boundary; the pure core never
    // touches wall time.
    let clock = SystemClock;

    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialise Tokio runtime")?;

    runtime.block_on(async move {
        let host_runtime =
            HostRuntime::with_plugins(executor, Arc::new(validated_ir), environment_plugins)
                .with_resource_bag(resources);

        if single_fire {
            run_once(&host_runtime, &selected, &clock, scheduled_at).await
        } else {
            run_loop(&host_runtime, &selected, &clock).await
        }
    })
}

/// Parse an RFC3339 timestamp for `--at` into a UTC instant.
fn parse_at(raw: &str) -> Result<DateTime<Utc>> {
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("--at value `{raw}` is not a valid RFC3339 timestamp"))?;
    Ok(parsed.with_timezone(&Utc))
}

/// Fire every selected schedule entrypoint once, immediately. Non-zero exit on
/// any failure so agents/tests can gate on it.
async fn run_once(
    runtime: &HostRuntime,
    entrypoints: &[FlowEntrypoint],
    clock: &dyn ScheduleClock,
    scheduled_at: Option<DateTime<Utc>>,
) -> Result<()> {
    let scheduled_ms = to_epoch_ms(scheduled_at.unwrap_or_else(|| clock.now()));

    let mut failures = 0usize;
    for entry in entrypoints {
        match fire(runtime, entry, scheduled_ms).await {
            Ok(()) => {}
            Err(err) => {
                failures += 1;
                eprintln!(
                    "✗ schedule fire for trigger `{}` failed: {err:#}",
                    entry.trigger_alias
                );
            }
        }
    }

    if failures > 0 {
        return Err(anyhow!(
            "{failures} of {} schedule fire(s) failed",
            entrypoints.len()
        ));
    }
    Ok(())
}

/// Tick loop: compute the next fire across all selected crons, sleep until it,
/// fire, repeat. Ctrl-C exits cleanly.
async fn run_loop(
    runtime: &HostRuntime,
    entrypoints: &[FlowEntrypoint],
    clock: &dyn ScheduleClock,
) -> Result<()> {
    eprintln!(
        "scheduler running for {} entrypoint(s); press Ctrl-C to stop",
        entrypoints.len()
    );

    let mut after = clock.now();
    loop {
        let fires = next_fires(after, entrypoints);
        let Some((fire_at, _)) = fires.first().copied() else {
            // No parseable/firing cron among the selection; nothing to do.
            eprintln!("no upcoming fires; exiting");
            return Ok(());
        };

        let wait = (fire_at - clock.now()).to_std().unwrap_or(Duration::ZERO);

        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for Ctrl-C")?;
                eprintln!("received Ctrl-C; shutting down scheduler");
                return Ok(());
            }
        }

        let scheduled_ms = to_epoch_ms(fire_at);
        for (_, entry) in &fires {
            if let Err(err) = fire(runtime, entry, scheduled_ms).await {
                // At-least-once, best-effort: log and keep ticking rather than
                // tearing down the loop on one bad fire.
                eprintln!(
                    "✗ schedule fire for trigger `{}` failed: {err:#}",
                    entry.trigger_alias
                );
            }
        }

        after = fire_at;
    }
}

/// Build and execute one synthetic fire, printing the capture result.
async fn fire(runtime: &HostRuntime, entry: &FlowEntrypoint, scheduled_ms: u64) -> Result<()> {
    let invocation: Invocation =
        fire_invocation(entry, scheduled_ms).context("failed to build scheduled invocation")?;
    let cron = entry.schedule.as_deref().unwrap_or_default();

    eprintln!(
        "▸ firing trigger `{}` (cron `{cron}`) scheduled_time_ms={scheduled_ms}",
        entry.trigger_alias
    );

    let execution = runtime
        .execute(invocation)
        .await
        .map_err(|err| match &err {
            ExecutionError::MissingCapabilities { hints } => {
                anyhow!("[CAP101] missing required capabilities: {hints:?}")
            }
            _ => anyhow::Error::new(err),
        })?;

    match execution {
        ExecutionResult::Value(value) => {
            println!("{}", serde_json::to_string(&value)?);
        }
        ExecutionResult::Halt { alias, payload } => {
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "halted": true,
                    "node": alias,
                    "payload": payload,
                }))?
            );
        }
        ExecutionResult::Stream(mut stream) => {
            while let Some(event) = stream.next().await {
                let payload: JsonValue = event.map_err(anyhow::Error::from)?;
                println!("{}", serde_json::to_string(&payload)?);
            }
        }
    }

    Ok(())
}
