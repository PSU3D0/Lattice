//! Test worker for the cloudflare idempotent ingest example.

use std::sync::Arc;

use cap_do_workers::{DurableObjectBinding, WorkersDurableObject};
use cap_http_workers::WorkersHttpClient;
use cap_kv_workers::WorkersKv;
use capabilities::ResourceBag;
use host_inproc::FlowBundle;
use worker::{Context, Env, Request, Response, Result, event};

use example_s7_cloudflare_idem::{bundle, clear_worker_context, config_from_env, set_worker_context};

pub use cap_do_workers::FlowDurableObject;

#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    let binding = DurableObjectBinding::from_env(&env, "DEDUP_DO")
        .map_err(|err| worker::Error::RustError(err.to_string()))?;
    let config = config_from_env(|key| env.var(key).ok().map(|secret| secret.to_string()));
    set_worker_context(binding.clone(), config);

    let kv_store = env.kv("EVENTS_KV")?;
    let kv = WorkersKv::new(kv_store);
    let http = WorkersHttpClient::new();
    let dedupe = WorkersDurableObject::from_binding(binding, Some("preflight".to_string()));

    let resources = ResourceBag::new()
        .with_kv(Arc::new(kv))
        .with_http_write(Arc::new(http))
        .with_dedupe(Arc::new(dedupe));
    host_workers::set_resource_bag(resources);

    let response = host_workers::handle_fetch(req, env, ctx).await;
    clear_worker_context();
    response
}

#[unsafe(no_mangle)]
pub extern "Rust" fn get_bundle() -> FlowBundle {
    bundle()
}
