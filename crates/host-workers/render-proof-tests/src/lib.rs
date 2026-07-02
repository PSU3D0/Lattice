//! Render→run proof worker (packet W2 of
//! ops/phase1-clone-engine-plan-2026-06-12.md).
//!
//! This fixture serves ONLY the `s1_echo` example flow through the existing
//! host-workers entry machinery (`host_workers::handle_fetch` +
//! `get_bundle()`, the same bundle path `workerd-tests` uses), so its runtime
//! binding needs are exactly what `flows deploy render --example s1_echo`
//! renders: one `FLOW_DO` Durable Object (the durability checkpoint store)
//! and nothing else.
//!
//! The vitest harness (`src/index.test.ts`) derives its entire miniflare
//! configuration FROM the RENDERED wrangler.toml — the rendered config, not a
//! hand-written one, is what stands the worker up. That is the W2 gate: "the
//! generated config is real".

#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

#[cfg(target_arch = "wasm32")]
use cap_do_workers::{DurableObjectBinding, WorkersDurableObject};
#[cfg(target_arch = "wasm32")]
use capabilities::ResourceBag;
#[cfg(target_arch = "wasm32")]
use dag_core::DurabilityMode;
use host_inproc::FlowBundle;
#[cfg(target_arch = "wasm32")]
use worker::{Context, Env, Request, Response, Result, event};

pub use cap_do_workers::FlowDurableObject;

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

#[cfg(target_arch = "wasm32")]
#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    configure_resources(&env)?;
    host_workers::handle_fetch(req, env, ctx).await
}

/// The single-flow bundle: exactly what the `flow!` macro generates for the
/// s1_echo example. A rendered wrangler.toml describes one flow's
/// requirements; this worker serves exactly that flow, so the rendered
/// bindings and the runtime needs line up 1:1.
#[unsafe(no_mangle)]
pub extern "Rust" fn get_bundle() -> FlowBundle {
    example_s1_echo::bundle()
}
