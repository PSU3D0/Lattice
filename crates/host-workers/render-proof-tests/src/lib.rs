//! Render→run proof worker (packets W2 + T3 of
//! ops/phase1-clone-engine-plan-2026-06-12.md).
//!
//! Two proofs share this worker binary:
//!
//! - **fetch (W2)**: serves ONLY the `s1_echo` example flow through the
//!   existing host-workers entry machinery (`host_workers::handle_fetch` +
//!   `get_bundle()`, the same bundle path `workerd-tests` uses), so its
//!   runtime binding needs are exactly what `flows deploy render --example
//!   s1_echo` renders: one `FLOW_DO` Durable Object and nothing else.
//! - **scheduled (T3)**: serves the cron proof flow (below) through
//!   `host_workers::handle_scheduled`. Its rendered config (from the
//!   requirements manifest `emit-cron-requirements` derives) carries the
//!   `[triggers].crons` union plus `FLOW_DO` + `FLOW_KV` bindings; the vitest
//!   harness dispatches miniflare scheduled events with the cron strings read
//!   FROM the rendered file, proving the byte-equal routing contract
//!   end-to-end (impl-docs/spec/schedule-trigger.md §7a/§7c).
//!
//! The vitest harnesses (`src/index.test.ts`, `src/cron.test.ts`) derive
//! their entire miniflare configuration FROM the RENDERED wrangler.toml — the
//! rendered config, not a hand-written one, is what stands the worker up.
//! That is the gate: "the generated config is real".
//!
//! # The cron proof flow
//!
//! Shape (schedule-trigger.md §2): two schedule entrypoints, each its own
//! trigger alias, disjoint chains, `allow_multiple_triggers` opt-in.
//!
//! - `tick -> record` (`*/5 * * * *`): the happy path. `record` DECLARES a KV
//!   write (`resources(kv_write(...))`) and records the fire under
//!   `tick:{scheduled_time_ms}` — the observable side effect the test reads
//!   back, keyed on the scheduled time per the idempotency guidance.
//! - `smuggle_tick -> smuggle` (`*/9 * * * *`): the scoped-capability honesty
//!   arm. `smuggle` declares NOTHING but attempts the same KV write; if
//!   ScopedResources (CAP110) is in force the accessor is denied, the node
//!   fails, and the scheduled invocation errors — which the test asserts,
//!   along with the absence of any `smuggled:*` key.

#[cfg(target_arch = "wasm32")]
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

#[cfg(target_arch = "wasm32")]
use cap_do_workers::{DurableObjectBinding, WorkersDurableObject};
#[cfg(target_arch = "wasm32")]
use capabilities::ResourceBag;
use dag_core::{DurabilityMode, NodeError, NodeResult, ScheduledEvent};
use dag_macros::{def_node, node};
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use kernel_exec::NodeRegistry;
use serde_json::{Value as JsonValue, json};
#[cfg(target_arch = "wasm32")]
use worker::{Context, Env, Request, Response, Result, event};

#[cfg(target_arch = "wasm32")]
pub use cap_do_workers::FlowDurableObject;

/// Cron strings, byte-verbatim everywhere they appear: authored in the
/// `entrypoint!` literals below, derived into FlowRequirements, rendered into
/// `[triggers].crons`, and matched by `handle_scheduled` against the fired
/// event. Keep in sync with the `entrypoint!` literals (the miniflare test
/// fails if they drift: it dispatches the strings read from the RENDERED
/// config, which come from the entrypoint side).
pub const RECORD_CRON: &str = "*/5 * * * *";
pub const SMUGGLE_CRON: &str = "*/9 * * * *";

#[def_node(
    trigger,
    name = "CronTick",
    summary = "Schedule trigger for the cron render proof",
    effects = "Pure",
    determinism = "Strict"
)]
async fn cron_tick(event: ScheduledEvent) -> NodeResult<ScheduledEvent> {
    Ok(event)
}

#[def_node(
    name = "CronRecord",
    summary = "Record the cron fire in KV (declared kv write)",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(kv_write(capabilities::kv::KeyValue))
)]
async fn cron_record(event: ScheduledEvent) -> NodeResult<JsonValue> {
    // Idempotency guidance (schedule-trigger.md §5): key on the scheduled
    // time so redeliveries of one fire dedupe, distinct fires stay distinct.
    let key = format!("tick:{}", event.scheduled_time_ms);
    let value = serde_json::to_vec(&event)
        .map_err(|err| NodeError::new(format!("encode ScheduledEvent: {err}")))?;

    let result = capabilities::context::with_current_async(|resources| async move {
        let kv = resources
            .kv()
            .ok_or_else(|| NodeError::new("cron_record missing KeyValue capability"))?;
        kv.put(&key, &value, None)
            .await
            .map_err(|err| NodeError::new(format!("cron_record kv put failed: {err}")))?;
        Ok(json!({ "recorded": key }))
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new("cron_record missing ResourceAccess context")),
    }
}

#[def_node(
    trigger,
    name = "CronSmuggleTick",
    summary = "Schedule trigger for the undeclared-capability honesty arm",
    effects = "Pure",
    determinism = "Strict"
)]
async fn cron_smuggle_tick(event: ScheduledEvent) -> NodeResult<ScheduledEvent> {
    Ok(event)
}

#[def_node(
    name = "CronSmuggle",
    summary = "Attempts an UNDECLARED KV write; ScopedResources must deny it (CAP110)",
    effects = "Pure",
    determinism = "Strict"
)]
async fn cron_smuggle(event: ScheduledEvent) -> NodeResult<JsonValue> {
    let key = format!("smuggled:{}", event.scheduled_time_ms);
    let result = capabilities::context::with_current_async(|resources| async move {
        match resources.kv() {
            // Scoped capabilities in force: the undeclared accessor is
            // denied. Fail the node so the scheduled invocation errors —
            // that failure IS the expected observable.
            None => Err(NodeError::new(
                "cron_smuggle kv access denied as expected (CAP110)",
            )),
            // Enforcement hole: the write would land and the run would
            // SUCCEED — turning the test red, which is the point.
            Some(kv) => {
                kv.put(&key, b"leak", None)
                    .await
                    .map_err(|err| NodeError::new(format!("smuggle put failed: {err}")))?;
                Ok(json!({ "smuggled": key }))
            }
        }
    })
    .await;

    match result {
        Some(result) => result,
        None => Err(NodeError::new(
            "cron_smuggle missing ResourceAccess context",
        )),
    }
}

dag_macros::flow! {
    name: cron_render_proof_flow,
    version: "1.0.0",
    profile: Web,
    summary: "T3 render->run proof: cron-fired flow with declared KV write and an undeclared-access honesty arm";

    let tick = node!(cron_tick);
    let record = node!(cron_record);
    let smuggle_tick = node!(cron_smuggle_tick);
    let smuggle = node!(cron_smuggle);

    connect!(tick -> record);
    connect!(smuggle_tick -> smuggle);

    entrypoint!({
        trigger: "tick",
        capture: "record",
        schedule: "*/5 * * * *",
        deadline_ms: 30000,
    });
    entrypoint!({
        trigger: "smuggle_tick",
        capture: "smuggle",
        schedule: "*/9 * * * *",
        deadline_ms: 30000,
    });
}

/// The cron proof flow IR with the policies the bundle (and the requirements
/// manifest) must agree on — one source for both, so the rendered config and
/// the runtime can never drift.
pub fn cron_flow_ir() -> dag_core::FlowIR {
    let mut flow = flow();
    // Two schedule entrypoints = multiple triggers (DAG104 opt-in).
    flow.policies.lint.allow_multiple_triggers = Some(true);
    // Durability partial -> needs_checkpoint_store -> FLOW_DO renders.
    flow.policies.durability.mode = DurabilityMode::Partial;
    flow
}

/// Static requirements manifest for `flows deploy render --requirements`
/// (emitted by the `emit-cron-requirements` bin at test time — derived, never
/// hand-written).
pub fn cron_requirements() -> dag_core::FlowRequirements {
    dag_core::FlowRequirements::derive(&cron_flow_ir())
        .expect("derive cron render proof requirements")
}

/// The runtime bundle `get_bundle()` serves for scheduled events.
pub fn cron_bundle() -> FlowBundle {
    let flow = cron_flow_ir();
    let validated_ir = kernel_plan::validate(&flow).expect("cron proof flow validation");

    let mut registry = NodeRegistry::new();
    cron_tick_register(&mut registry).expect("register cron_tick");
    cron_record_register(&mut registry).expect("register cron_record");
    cron_smuggle_tick_register(&mut registry).expect("register cron_smuggle_tick");
    cron_smuggle_register(&mut registry).expect("register cron_smuggle");
    let registry = Arc::new(registry);
    let resolver: Arc<dyn kernel_exec::NodeResolver> =
        Arc::new(kernel_exec::RegistryResolver::new(registry));

    let entrypoints = vec![
        FlowEntrypoint {
            trigger_alias: "tick".to_string(),
            capture_alias: "record".to_string(),
            route_path: None,
            method: None,
            deadline: Some(Duration::from_millis(30000)),
            route_aliases: Vec::new(),
            schedule: Some(RECORD_CRON.to_string()),
        },
        FlowEntrypoint {
            trigger_alias: "smuggle_tick".to_string(),
            capture_alias: "smuggle".to_string(),
            route_path: None,
            method: None,
            deadline: Some(Duration::from_millis(30000)),
            route_aliases: Vec::new(),
            schedule: Some(SMUGGLE_CRON.to_string()),
        },
    ];

    let node_contracts = vec![
        node!(cron_tick),
        node!(cron_record),
        node!(cron_smuggle_tick),
        node!(cron_smuggle),
    ]
    .into_iter()
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

// Which bundle `get_bundle()` serves. Fetch events serve s1_echo (the W2
// proof); scheduled events serve the cron proof flow. Each handler sets the
// selector before touching the bundle, so isolate reuse cannot leak one
// proof's flow into the other.
#[cfg(target_arch = "wasm32")]
thread_local! {
    static SERVE_CRON_BUNDLE: Cell<bool> = const { Cell::new(false) };
}

/// Build the resource bag from exactly the bindings the rendered
/// wrangler.toml provisions: `FLOW_DO` (renderer durability default, which is
/// also the host-workers runtime name — crates/cli/src/deploy.rs and
/// crates/host-workers/workerd-tests agree on it). s1_echo's requirements
/// manifest demands only a checkpoint store (`needs_checkpoint_store: true`,
/// everything else false), so nothing else is wired — if the renderer ever
/// under-provisions, preflight fails closed and this proof goes red.
#[cfg(target_arch = "wasm32")]
fn configure_resources(env: &Env) -> Result<()> {
    let binding = DurableObjectBinding::from_env(env, "FLOW_DO")
        .map_err(|err| worker::Error::RustError(err.to_string()))?;
    let durability = Arc::new(WorkersDurableObject::from_binding(
        binding,
        Some("s1-echo-render-proof".to_string()),
    ));
    let resources = ResourceBag::new()
        .with_checkpoint_store(durability)
        .with_max_durability_mode(DurabilityMode::Partial);
    host_workers::set_resource_bag(resources);
    Ok(())
}

/// Resource bag for the cron proof, from exactly what its rendered config
/// provisions: `FLOW_DO` (checkpoint store) + `FLOW_KV` (the declared KV
/// write of `cron_record`). Per-node access is then scoped by declarations
/// (CAP110) inside the runtime — which the smuggle arm proves.
#[cfg(target_arch = "wasm32")]
fn configure_cron_resources(env: &Env) -> Result<()> {
    let binding = DurableObjectBinding::from_env(env, "FLOW_DO")
        .map_err(|err| worker::Error::RustError(err.to_string()))?;
    let durability = Arc::new(WorkersDurableObject::from_binding(
        binding,
        Some("cron-render-proof".to_string()),
    ));
    let kv = Arc::new(cap_kv_workers::WorkersKv::new(env.kv("FLOW_KV")?));
    let resources = ResourceBag::new()
        .with_checkpoint_store(durability)
        .with_max_durability_mode(DurabilityMode::Partial)
        .with_kv(kv);
    host_workers::set_resource_bag(resources);
    Ok(())
}

#[cfg(target_arch = "wasm32")]
#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    SERVE_CRON_BUNDLE.with(|slot| slot.set(false));
    configure_resources(&env)?;
    host_workers::handle_fetch(req, env, ctx).await
}

#[cfg(target_arch = "wasm32")]
#[event(scheduled)]
async fn scheduled(event: worker::ScheduledEvent, env: Env, ctx: worker::ScheduleContext) {
    SERVE_CRON_BUNDLE.with(|slot| slot.set(true));
    if let Err(err) = configure_cron_resources(&env) {
        panic!("cron resource configuration failed: {err}");
    }
    if let Err(err) = host_workers::handle_scheduled(event, env, ctx).await {
        // scheduled() returns (): panicking is the only way to fail the
        // invocation so the outcome (and miniflare's 500) reflects the error
        // — never a silent drop (schedule-trigger.md §7a).
        panic!("scheduled handler failed: {err}");
    }
}

/// The single-flow bundle behind each event type: exactly what the `flow!`
/// macro generates. A rendered wrangler.toml describes one flow's
/// requirements; this worker serves exactly that flow per event type, so the
/// rendered bindings and the runtime needs line up 1:1.
#[unsafe(no_mangle)]
pub extern "Rust" fn get_bundle() -> FlowBundle {
    #[cfg(target_arch = "wasm32")]
    {
        if SERVE_CRON_BUNDLE.with(|slot| slot.get()) {
            return cron_bundle();
        }
    }
    example_s1_echo::bundle()
}
