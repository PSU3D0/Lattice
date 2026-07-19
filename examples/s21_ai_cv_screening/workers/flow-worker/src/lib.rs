use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use capabilities::ResourceBag;
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
    OutboundAuthKind, OutboundAuthProfileDescriptor, ResolvedEndpointProfile,
};
use capabilities::http::{
    HttpError, HttpMethod, HttpRead, HttpRequest, HttpResponse, HttpResult, HttpWrite,
};
use dag_core::DurabilityMode;
use futures::StreamExt;
use worker::send::IntoSendFuture;
use worker::{
    Context, Env, Fetcher, Headers, Method, Request, RequestInit, Response, Result, event,
};

pub use cap_do_workers::FlowDurableObject;
pub use cap_workspace_workers::WorkspaceDurableObject;

#[derive(Clone)]
struct ServiceFetcher(Fetcher);

// Fetcher handles are isolate-local and used only on the Workers request executor.
unsafe impl Send for ServiceFetcher {}
unsafe impl Sync for ServiceFetcher {}

enum S21HttpBackend {
    AmbientHttps(cap_http_workers::WorkersHttpClient),
    ServiceBinding(ServiceFetcher),
}

struct S21HttpClient {
    backend: S21HttpBackend,
}

impl S21HttpClient {
    fn from_env(env: &Env) -> Result<Self> {
        let mode = env
            .var("LATTICE_S21_HTTP_MODE")
            .map_err(|_| {
                worker::Error::RustError("required LATTICE_S21_HTTP_MODE is absent".into())
            })?
            .to_string();
        let backend = match mode.as_str() {
            "ambient_https" => {
                S21HttpBackend::AmbientHttps(cap_http_workers::WorkersHttpClient::new())
            }
            "service_binding" => S21HttpBackend::ServiceBinding(ServiceFetcher(
                env.service("LATTICE_S21_PROVIDER").map_err(|_| {
                    worker::Error::RustError(
                        "service-binding HTTP mode requires LATTICE_S21_PROVIDER".into(),
                    )
                })?,
            )),
            _ => {
                return Err(worker::Error::RustError(
                    "LATTICE_S21_HTTP_MODE must be ambient_https or service_binding".into(),
                ));
            }
        };
        Ok(Self { backend })
    }

    fn uses_ambient_https(&self) -> bool {
        matches!(self.backend, S21HttpBackend::AmbientHttps(_))
    }

    async fn send_via_service(
        service: &ServiceFetcher,
        request: HttpRequest,
    ) -> HttpResult<HttpResponse> {
        let method = match request.method {
            HttpMethod::Get => Method::Get,
            HttpMethod::Post => Method::Post,
            HttpMethod::Put => Method::Put,
            HttpMethod::Patch => Method::Patch,
            HttpMethod::Delete => Method::Delete,
            HttpMethod::Head => Method::Head,
        };
        let headers = Headers::new();
        for (name, value) in request.headers.iter() {
            headers
                .set(name, value)
                .map_err(|_| HttpError::InvalidResponse("invalid outbound header".into()))?;
        }
        let mut init = RequestInit::new();
        init.with_method(method);
        init.with_headers(headers);
        if let Some(body) = request.body {
            init.with_body(Some(
                worker::js_sys::Uint8Array::from(body.as_slice()).into(),
            ));
        }
        let outbound = Request::new_with_init(&request.url, &init)
            .map_err(|_| HttpError::InvalidResponse("invalid outbound request".into()))?;
        let response = service
            .0
            .fetch_request(outbound)
            .into_send()
            .await
            .map_err(|_| HttpError::InvalidResponse("provider service unavailable".into()))?;
        let status = response.status().as_u16();
        let mut headers = capabilities::http::HttpHeaders::default();
        for (name, value) in response.headers().iter() {
            if let Ok(value) = value.to_str() {
                headers.insert(name.as_str(), value);
            }
        }
        let mut body = Vec::new();
        let mut stream = response.into_body();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|_| HttpError::InvalidResponse("provider response unavailable".into()))?;
            if body.len().saturating_add(chunk.len()) > 2 * 1024 * 1024 {
                return Err(HttpError::InvalidResponse(
                    "provider response exceeded the proof-service limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[async_trait]
impl HttpRead for S21HttpClient {
    async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        match &self.backend {
            S21HttpBackend::ServiceBinding(service) => {
                Self::send_via_service(service, request).await
            }
            S21HttpBackend::AmbientHttps(ambient) => HttpRead::send(ambient, request).await,
        }
    }
}

#[async_trait]
impl HttpWrite for S21HttpClient {
    async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        match &self.backend {
            S21HttpBackend::ServiceBinding(service) => {
                Self::send_via_service(service, request).await
            }
            S21HttpBackend::AmbientHttps(ambient) => HttpWrite::send(ambient, request).await,
        }
    }
}

#[derive(Debug)]
struct S21ConnectorRuntime {
    secrets: BTreeMap<String, String>,
    endpoints: BTreeMap<String, String>,
}

impl S21ConnectorRuntime {
    fn from_env(env: &Env, ambient_https: bool) -> Result<Self> {
        let mut secrets = BTreeMap::new();
        for name in [
            "LATTICE_CONNECTOR_AUTH_LLM_API_KEY",
            "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH",
        ] {
            let value = env
                .secret(name)
                .map(|value| value.to_string())
                .or_else(|_| env.var(name).map(|value| value.to_string()))
                .map_err(|_| {
                    worker::Error::RustError(format!(
                        "required connector secret `{name}` is absent"
                    ))
                })?;
            secrets.insert(name.to_string(), value);
        }

        let mut endpoints = BTreeMap::new();
        for name in [
            "LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL",
            "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL",
            "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL",
        ] {
            if let Ok(value) = env.var(name) {
                let value = value.to_string();
                if !value.trim().is_empty() {
                    if ambient_https && !value.starts_with("https://") {
                        return Err(worker::Error::RustError(format!(
                            "ambient endpoint override `{name}` must use https"
                        )));
                    }
                    endpoints.insert(name.to_string(), value);
                }
            }
        }
        Ok(Self { secrets, endpoints })
    }
}

#[async_trait]
impl ConnectorRuntime for S21ConnectorRuntime {
    async fn apply_outbound_auth(
        &self,
        _scope: &ConnectorBindingScope,
        profile: &OutboundAuthProfileDescriptor,
        request: &mut HttpRequest,
    ) -> std::result::Result<(), ConnectorRuntimeError> {
        let secret = self.secrets.get(profile.env_var).ok_or(
            ConnectorRuntimeError::MissingAuthOverride {
                role_name: profile.name,
                env_var: profile.env_var,
            },
        )?;
        match profile.kind {
            OutboundAuthKind::Bearer { .. } => {
                request
                    .headers
                    .insert("authorization".to_string(), format!("Bearer {secret}"));
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
        let base_url = self
            .endpoints
            .get(profile.env_base_url_var)
            .cloned()
            .unwrap_or_else(|| profile.base_url.to_string());
        if !(base_url.starts_with("https://") || base_url.starts_with("http://")) {
            return Err(ConnectorRuntimeError::InvalidEndpointProfile {
                role_name: profile.name,
                reason: "endpoint must use http or https".to_string(),
            });
        }
        Ok(ResolvedEndpointProfile {
            base_url,
            default_headers: profile
                .default_headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect(),
        })
    }
}

fn configure_resources(env: &Env) -> Result<()> {
    let durability = Arc::new(
        cap_do_workers::WorkersDurableObject::from_env(env, "FLOW_DO", None)
            .map_err(|error| worker::Error::RustError(error.to_string()))?,
    );
    let http = Arc::new(S21HttpClient::from_env(env)?);
    let ambient_https = http.uses_ambient_https();
    let kv = Arc::new(cap_kv_workers::WorkersKv::new(env.kv("FLOW_KV")?));
    let transform = Arc::new(host_workers::WorkersTransformRuntime::from_env(
        env,
        host_workers::WorkersTransformPolicy::default(),
    )?);
    let connectors = Arc::new(S21ConnectorRuntime::from_env(env, ambient_https)?);

    host_workers::set_resource_bag(
        ResourceBag::new()
            .with_checkpoint_store(Arc::clone(&durability))
            .with_resume_scheduler(Arc::clone(&durability))
            .with_resume_signal_source(Arc::clone(&durability))
            .with_http_read(Arc::clone(&http))
            .with_http_write(http)
            .with_kv(kv)
            .with_transform_runtime(transform)
            .with_connector_runtime(connectors)
            .with_max_durability_mode(DurabilityMode::Partial),
    );
    Ok(())
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, ctx: Context) -> Result<Response> {
    configure_resources(&env)?;
    let response = host_workers::handle_fetch(req, env, ctx).await?;
    let status = response.status_code();
    if status < 400 {
        return Ok(response);
    }
    let class = match status {
        400 => "bad_request",
        404 => "not_found",
        405 => "method_not_allowed",
        413 => "payload_too_large",
        429 | 503 => "unavailable",
        _ => "execution_failed",
    };
    Response::from_json(&serde_json::json!({ "error": class }))
        .map(|value| value.with_status(status))
}

#[unsafe(no_mangle)]
pub extern "Rust" fn get_bundle() -> host_inproc::FlowBundle {
    example_s21_ai_cv_screening::bundle()
}
