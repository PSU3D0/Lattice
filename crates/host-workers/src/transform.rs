use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use capabilities::transform::{
    PlatformTransformBudgets, PlatformTransformObservations, TransformBackend, TransformBeginError,
    TransformErrorClass, TransformExecutionRecord, TransformFailure, TransformInstanceModel,
    TransformLease, TransformOutcome, TransformRuntime, TransformTerminationClass,
};
use futures::StreamExt;
use http::HeaderMap;
use sha2::{Digest, Sha256};
use worker::wasm_bindgen::JsValue;
use worker::{Env, Fetcher, Headers, Method, Request, RequestInit};

pub const PDF_EXTRACT_SERVICE_BINDING: &str = "LATTICE_EXTRACT_PDF";
pub const PDF_EXTRACT_TRANSFORM_ID: &str = "lattice.pdf.extract_text.v1";
pub const PDF_EXTRACT_ABI_VERSION: &str = "lattice.transform.v1";
pub const PDF_EXTRACT_COMPATIBILITY_DATE: &str = "2026-07-15";
pub const PDF_EXTRACT_MODULE_SHA256_HEX: &str =
    "048f650aec8502659633289a4ace493c56a7bc6e95c8da3d4a34e293e96d4e96";
pub const PDF_EXTRACT_MAX_INPUT_BYTES: u64 = 8 * 1024 * 1024;
pub const PDF_EXTRACT_MAX_OUTPUT_BYTES: u64 = 4 + 512 * 1024;
pub const PDF_EXTRACT_GUEST_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
pub const WORKERS_ISOLATE_MEMORY_BYTES: u64 = 128 * 1024 * 1024;
pub const PDF_EXTRACT_CPU_MS_LIMIT: u64 = 30_000;

const PDF_EXTRACT_MODULE_SHA256: [u8; 32] = [
    0x04, 0x8f, 0x65, 0x0a, 0xec, 0x85, 0x02, 0x65, 0x96, 0x33, 0x28, 0x9a, 0x4a, 0xce, 0x49, 0x3c,
    0x56, 0xa7, 0xbc, 0x6e, 0x95, 0xc8, 0xda, 0x3d, 0x4a, 0x34, 0xe2, 0x93, 0xe9, 0x6d, 0x4e, 0x96,
];
const SERVICE_URL: &str = "http://lattice-extract-pdf/v1/transform";
const MAX_ERROR_BODY_BYTES: usize = 1024;

thread_local! {
    static TRANSFORM_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkersTransformPolicy {
    pub binding: String,
    pub compatibility_date: String,
    pub cpu_ms_limit: u64,
    pub isolate_memory_bytes: u64,
    pub guest_memory_bytes: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
}

impl Default for WorkersTransformPolicy {
    fn default() -> Self {
        Self {
            binding: PDF_EXTRACT_SERVICE_BINDING.to_string(),
            compatibility_date: PDF_EXTRACT_COMPATIBILITY_DATE.to_string(),
            cpu_ms_limit: PDF_EXTRACT_CPU_MS_LIMIT,
            isolate_memory_bytes: WORKERS_ISOLATE_MEMORY_BYTES,
            guest_memory_bytes: PDF_EXTRACT_GUEST_MEMORY_BYTES,
            input_bytes: PDF_EXTRACT_MAX_INPUT_BYTES,
            output_bytes: PDF_EXTRACT_MAX_OUTPUT_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
struct FetcherHandle(Fetcher);

// SAFETY: Cloudflare Workers runs wasm requests on a single-threaded isolate event loop. The
// worker crate's Fetcher is an isolate-local JS handle and is only used from that event loop.
unsafe impl Send for FetcherHandle {}
// SAFETY: see the Send justification above; no cross-isolate sharing is possible.
unsafe impl Sync for FetcherHandle {}

#[derive(Debug, Clone)]
pub struct WorkersTransformRuntime {
    fetcher: FetcherHandle,
    policy: WorkersTransformPolicy,
}

impl WorkersTransformRuntime {
    pub fn from_env(env: &Env, policy: WorkersTransformPolicy) -> worker::Result<Self> {
        validate_policy(&policy).map_err(worker::Error::RustError)?;
        let fetcher = env.service(&policy.binding)?;
        Ok(Self {
            fetcher: FetcherHandle(fetcher),
            policy,
        })
    }

    pub fn policy(&self) -> &WorkersTransformPolicy {
        &self.policy
    }
}

impl TransformRuntime for WorkersTransformRuntime {
    fn backend(&self) -> TransformBackend {
        TransformBackend::CloudflareWorkers
    }

    fn try_begin(
        &self,
        transform_id: &str,
    ) -> Result<Box<dyn TransformLease>, TransformBeginError> {
        if transform_id != PDF_EXTRACT_TRANSFORM_ID {
            return Err(TransformBeginError::InvalidTransform);
        }
        let acquired = TRANSFORM_ACTIVE.with(|active| {
            if active.get() {
                false
            } else {
                active.set(true);
                true
            }
        });
        if !acquired {
            return Err(TransformBeginError::Busy);
        }
        Ok(Box::new(WorkersTransformLease {
            fetcher: self.fetcher.clone(),
            policy: self.policy.clone(),
            active: true,
        }))
    }
}

struct WorkersTransformLease {
    fetcher: FetcherHandle,
    policy: WorkersTransformPolicy,
    active: bool,
}

// SAFETY: this lease is created, used, and dropped only on the single-threaded Worker isolate.
// The Send bound comes from the backend-neutral TransformLease contract.
unsafe impl Send for WorkersTransformLease {}

impl Drop for WorkersTransformLease {
    fn drop(&mut self) {
        if self.active {
            TRANSFORM_ACTIVE.with(|active| active.set(false));
            self.active = false;
        }
    }
}

#[async_trait(?Send)]
impl TransformLease for WorkersTransformLease {
    fn max_input_bytes(&self) -> u64 {
        self.policy.input_bytes
    }

    async fn run(self: Box<Self>, input: Vec<u8>) -> Result<TransformOutcome, TransformFailure> {
        let started_ms = worker::js_sys::Date::now();
        let input_len = u64::try_from(input.len()).unwrap_or(u64::MAX);
        let input_sha256: [u8; 32] = Sha256::digest(&input).into();
        if input_len > self.policy.input_bytes {
            return Err(self.failure(
                TransformErrorClass::InputTooLarge,
                Some(input_sha256),
                input_len,
                0,
                started_ms,
            ));
        }

        let request = match service_request(input) {
            Ok(request) => request,
            Err(_) => {
                return Err(self.failure(
                    TransformErrorClass::RuntimeUnavailable,
                    Some(input_sha256),
                    input_len,
                    0,
                    started_ms,
                ));
            }
        };
        let response = match self.fetcher.0.fetch_request(request).await {
            Ok(response) => response,
            Err(_) => {
                return Err(self.failure(
                    TransformErrorClass::PlatformTerminated,
                    Some(input_sha256),
                    input_len,
                    0,
                    started_ms,
                ));
            }
        };

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body_limit = if status == 200 {
            usize::try_from(self.policy.output_bytes)
                .unwrap_or(usize::MAX)
                .saturating_add(1)
        } else {
            MAX_ERROR_BODY_BYTES
        };
        let body = match collect_bounded(response.into_body(), body_limit).await {
            Ok(body) => body,
            Err(CollectError::TooLarge) if status == 200 => {
                return Err(self.failure(
                    TransformErrorClass::OutputTooLarge,
                    Some(input_sha256),
                    input_len,
                    0,
                    started_ms,
                ));
            }
            Err(CollectError::TooLarge) => {
                return Err(self.failure(
                    TransformErrorClass::InvalidOutput,
                    Some(input_sha256),
                    input_len,
                    0,
                    started_ms,
                ));
            }
            Err(CollectError::Transport) => {
                return Err(self.failure(
                    TransformErrorClass::PlatformTerminated,
                    Some(input_sha256),
                    input_len,
                    0,
                    started_ms,
                ));
            }
        };

        if status != 200 {
            let class = checked_error_class(status, &headers, &body)
                .unwrap_or(TransformErrorClass::InvalidOutput);
            return Err(self.failure(class, Some(input_sha256), input_len, 0, started_ms));
        }
        if !success_headers_match(&headers) {
            return Err(self.failure(
                TransformErrorClass::InvalidOutput,
                Some(input_sha256),
                input_len,
                0,
                started_ms,
            ));
        }
        let output_len = u64::try_from(body.len()).unwrap_or(u64::MAX);
        if output_len > self.policy.output_bytes {
            return Err(self.failure(
                TransformErrorClass::OutputTooLarge,
                Some(input_sha256),
                input_len,
                0,
                started_ms,
            ));
        }
        let output_sha256: [u8; 32] = Sha256::digest(&body).into();
        let record = self
            .record(
                Some(input_sha256),
                Some(output_sha256),
                TransformTerminationClass::Success,
                input_len,
                output_len,
                started_ms,
            )
            .expect("checked Workers transform success record is valid");
        TransformOutcome::new(body, record).map_err(|_| {
            self.failure(
                TransformErrorClass::InvalidOutput,
                Some(input_sha256),
                input_len,
                0,
                started_ms,
            )
        })
    }
}

impl WorkersTransformLease {
    fn budgets(&self) -> PlatformTransformBudgets {
        PlatformTransformBudgets::new(
            self.policy.cpu_ms_limit,
            self.policy.isolate_memory_bytes,
            self.policy.guest_memory_bytes,
            self.policy.input_bytes,
            self.policy.output_bytes,
            1,
            TransformInstanceModel::FreshPerInvocation,
        )
        .expect("Workers transform policy was validated at construction")
    }

    fn record(
        &self,
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
        termination_class: TransformTerminationClass,
        input_bytes: u64,
        output_bytes: u64,
        started_ms: f64,
    ) -> Result<TransformExecutionRecord, capabilities::transform::TransformRecordError> {
        TransformExecutionRecord::platform(
            PDF_EXTRACT_TRANSFORM_ID,
            PDF_EXTRACT_MODULE_SHA256,
            PDF_EXTRACT_ABI_VERSION,
            self.policy.compatibility_date.clone(),
            self.budgets(),
            input_sha256,
            output_sha256,
            termination_class,
            PlatformTransformObservations::new(elapsed(started_ms), input_bytes, output_bytes),
        )
    }

    fn failure(
        &self,
        class: TransformErrorClass,
        input_sha256: Option<[u8; 32]>,
        input_bytes: u64,
        output_bytes: u64,
        started_ms: f64,
    ) -> TransformFailure {
        let record = self
            .record(
                input_sha256,
                None,
                TransformTerminationClass::Failure(class),
                input_bytes,
                output_bytes,
                started_ms,
            )
            .expect("checked Workers transform failure record is valid");
        TransformFailure::new(class, record, None)
            .expect("Workers failure class matches terminal record")
    }
}

fn validate_policy(policy: &WorkersTransformPolicy) -> Result<(), String> {
    if policy.binding != PDF_EXTRACT_SERVICE_BINDING
        || policy.compatibility_date != PDF_EXTRACT_COMPATIBILITY_DATE
        || policy.cpu_ms_limit != PDF_EXTRACT_CPU_MS_LIMIT
        || policy.isolate_memory_bytes != WORKERS_ISOLATE_MEMORY_BYTES
        || policy.guest_memory_bytes != PDF_EXTRACT_GUEST_MEMORY_BYTES
        || policy.input_bytes != PDF_EXTRACT_MAX_INPUT_BYTES
        || policy.output_bytes != PDF_EXTRACT_MAX_OUTPUT_BYTES
    {
        return Err(
            "Workers transform policy does not match the pinned deployment contract".into(),
        );
    }
    PlatformTransformBudgets::new(
        policy.cpu_ms_limit,
        policy.isolate_memory_bytes,
        policy.guest_memory_bytes,
        policy.input_bytes,
        policy.output_bytes,
        1,
        TransformInstanceModel::FreshPerInvocation,
    )
    .map(|_| ())
    .map_err(|error| error.to_string())
}

fn service_request(input: Vec<u8>) -> worker::Result<Request> {
    let headers = Headers::new();
    headers.set("content-type", "application/pdf")?;
    headers.set("x-lattice-transform-id", PDF_EXTRACT_TRANSFORM_ID)?;
    headers.set("x-lattice-transform-abi", PDF_EXTRACT_ABI_VERSION)?;
    let array = worker::js_sys::Uint8Array::from(input.as_slice());
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(JsValue::from(array)));
    Request::new_with_init(SERVICE_URL, &init)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollectError {
    TooLarge,
    Transport,
}

async fn collect_bounded(
    mut body: worker::Body,
    max_bytes: usize,
) -> Result<Vec<u8>, CollectError> {
    let mut output = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| CollectError::Transport)?;
        let next = output
            .len()
            .checked_add(chunk.len())
            .ok_or(CollectError::TooLarge)?;
        if next > max_bytes {
            return Err(CollectError::TooLarge);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

fn checked_error_class(
    status: u16,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<TransformErrorClass> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ErrorEnvelope {
        error: String,
    }

    if header(headers, "content-type") != Some("application/json; charset=utf-8")
        || !identity_headers_match(headers)
    {
        return None;
    }
    let envelope: ErrorEnvelope = serde_json::from_slice(body).ok()?;
    let (expected_status, class) = match envelope.error.as_str() {
        "invalid_module" => (500, TransformErrorClass::InvalidModule),
        "invalid_abi" => (500, TransformErrorClass::InvalidAbi),
        "input_too_large" => (413, TransformErrorClass::InputTooLarge),
        "busy" => (503, TransformErrorClass::Busy),
        "runtime_unavailable" => (503, TransformErrorClass::RuntimeUnavailable),
        "output_too_large" => (502, TransformErrorClass::OutputTooLarge),
        "guest_failed" => (422, TransformErrorClass::GuestFailed),
        "invalid_output" => (500, TransformErrorClass::InvalidOutput),
        "unsupported_document" => (422, TransformErrorClass::UnsupportedDocument),
        "platform_terminated" => (503, TransformErrorClass::PlatformTerminated),
        _ => return None,
    };
    (status == expected_status).then_some(class)
}

fn success_headers_match(headers: &HeaderMap) -> bool {
    header(headers, "content-type") == Some("application/octet-stream")
        && identity_headers_match(headers)
}

fn identity_headers_match(headers: &HeaderMap) -> bool {
    header(headers, "x-lattice-transform-id") == Some(PDF_EXTRACT_TRANSFORM_ID)
        && header(headers, "x-lattice-transform-abi") == Some(PDF_EXTRACT_ABI_VERSION)
        && header(headers, "x-lattice-module-sha256-attestation")
            == Some(PDF_EXTRACT_MODULE_SHA256_HEX)
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn elapsed(started_ms: f64) -> Duration {
    let elapsed_ms = (worker::js_sys::Date::now() - started_ms).max(0.0);
    Duration::from_secs_f64(elapsed_ms / 1_000.0)
}

pub fn runtime_handle(runtime: WorkersTransformRuntime) -> Arc<dyn TransformRuntime> {
    Arc::new(runtime)
}
