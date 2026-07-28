mod broker_transport;

use std::sync::Arc;

use async_trait::async_trait;
use capabilities::ResourceBag;
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
    OutboundAuthKind, OutboundAuthProfileDescriptor, ResolvedEndpointProfile,
};
use capabilities::http::HttpRequest;
use dag_core::DurabilityMode;
use worker::{Context, Env, Request, Response, Result, event};

pub use cap_do_workers::FlowDurableObject;

struct S30ConnectorRuntime;

#[async_trait]
impl ConnectorRuntime for S30ConnectorRuntime {
    async fn apply_outbound_auth(
        &self,
        _scope: &ConnectorBindingScope,
        profile: &OutboundAuthProfileDescriptor,
        request: &mut HttpRequest,
    ) -> std::result::Result<(), ConnectorRuntimeError> {
        match profile.kind {
            OutboundAuthKind::Bearer { .. } => {
                request.headers.insert(
                    "authorization".to_string(),
                    "Bearer broker-mediated-non-credential".to_string(),
                );
                Ok(())
            }
            _ => Err(ConnectorRuntimeError::UnsupportedAuthKind {
                role_name: profile.name,
                kind: profile.kind.kind_name(),
            }),
        }
    }

    async fn resolve_endpoint_profile(
        &self,
        _scope: &ConnectorBindingScope,
        profile: &EndpointProfileDescriptor,
    ) -> std::result::Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
        Ok(ResolvedEndpointProfile {
            base_url: profile.base_url.to_string(),
            default_headers: profile
                .default_headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect(),
        })
    }
}

fn configure_resources(env: &Env) -> Result<()> {
    let broker = broker_transport::WorkersBrokerTransport::from_env(env)?;
    let durability = Arc::new(
        cap_do_workers::WorkersDurableObject::from_env(env, "FLOW_DO", None)
            .map_err(|error| worker::Error::RustError(error.to_string()))?,
    );
    host_workers::set_resource_bag(
        ResourceBag::new()
            .with_checkpoint_store(Arc::clone(&durability))
            .with_resume_scheduler(Arc::clone(&durability))
            .with_resume_signal_source(durability)
            .with_http_read(Arc::clone(&broker))
            .with_http_write(broker)
            .with_connector_runtime(Arc::new(S30ConnectorRuntime))
            .with_max_durability_mode(DurabilityMode::Partial),
    );
    Ok(())
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    configure_resources(&env)?;
    host_workers::handle_fetch(req, env, ctx).await
}

#[unsafe(no_mangle)]
pub extern "Rust" fn get_bundle() -> host_inproc::FlowBundle {
    s30_google_micro::bundle()
}
