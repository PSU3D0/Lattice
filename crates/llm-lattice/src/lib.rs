//! LLM lattice bridge for Lattice HTTP resources.
//!
//! This crate adapts Lattice resource access to the portable `llm_types`
//! HTTP client abstraction.

use bytes::Bytes;
use capabilities::{
    context,
    http::{
        HttpMethod as CapabilityMethod, HttpRead, HttpRequest as CapabilityRequest,
        HttpResponse as CapabilityResponse, HttpWrite,
    },
    ResourceAccess,
};
use http::{HeaderValue, Method, StatusCode};
pub use llm_types::http_client::{
    Error, HttpClientExt, LazyBody, MultipartForm, NoBody, Request, Response, StreamingResponse,
};
use llm_types::http_client::{self as http_client};
use llm_types::wasm_compat::WasmCompatSend;
use std::{fmt, future::Future, sync::Arc};

pub type Result<T> = http_client::Result<T>;

#[derive(Debug, thiserror::Error)]
enum BridgeError {
    #[error("unsupported HTTP method {0}")]
    UnsupportedMethod(String),
    #[error("{method} requests require a HTTP {kind} capability")]
    MissingHttpCapability {
        method: &'static str,
        kind: &'static str,
    },
    #[error("{operation} requests are unsupported by LatticeHttpClient")]
    UnsupportedOperation { operation: &'static str },
    #[error("invalid HTTP status returned by capability: {0}")]
    InvalidStatus(u16),
}

#[cfg(not(target_family = "wasm"))]
fn instance_error<E>(error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::Instance(Box::new(error))
}

#[cfg(target_family = "wasm")]
fn instance_error<E>(error: E) -> Error
where
    E: std::error::Error + 'static,
{
    Error::Instance(Box::new(error))
}

fn unsupported_error(operation: &'static str) -> Error {
    instance_error(BridgeError::UnsupportedOperation { operation })
}

fn missing_capability_error(method: &'static str, kind: &'static str) -> Error {
    instance_error(BridgeError::MissingHttpCapability { method, kind })
}

fn unsupported_method_error(method: Method) -> Error {
    instance_error(BridgeError::UnsupportedMethod(method.as_str().to_owned()))
}

fn capability_error(error: capabilities::http::HttpError) -> Error {
    instance_error(error)
}

fn invalid_status_error(status: u16) -> Error {
    instance_error(BridgeError::InvalidStatus(status))
}

/// Bridge client that resolves the ambient Lattice resource access.
#[derive(Clone)]
pub struct LatticeHttpClient {
    resources: Arc<dyn ResourceAccess>,
}

impl fmt::Debug for LatticeHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatticeHttpClient").finish_non_exhaustive()
    }
}

impl LatticeHttpClient {
    /// Create a client from an explicit Lattice resource access handle.
    pub fn from_resources(resources: Arc<dyn ResourceAccess>) -> Self {
        Self { resources }
    }

    /// Create a client from a single HTTP capability that supports both read and write.
    pub fn from_http_client<C>(client: Arc<C>) -> Self
    where
        C: HttpRead + HttpWrite + 'static,
    {
        Self::from_http_clients(client.clone(), client)
    }

    /// Create a client from distinct HTTP read and write capabilities.
    pub fn from_http_clients<R, W>(read: Arc<R>, write: Arc<W>) -> Self
    where
        R: HttpRead + 'static,
        W: HttpWrite + 'static,
    {
        let access = ExplicitHttpAccess {
            http_read: read,
            http_write: write,
        };
        Self::from_resources(Arc::new(access))
    }

    /// Capture the currently scoped Lattice resources, if any.
    pub fn from_current_resources() -> Option<Self> {
        context::current_handle().map(Self::from_resources)
    }

    /// Create a client from the current scoped resources, or an empty bridge if none are scoped.
    pub fn current() -> Self {
        Self::from_current_resources().unwrap_or_else(Self::empty)
    }

    /// Borrow the underlying resource access handle.
    pub fn resources(&self) -> &dyn ResourceAccess {
        self.resources.as_ref()
    }

    fn empty() -> Self {
        Self::from_resources(Arc::new(NoHttpAccess))
    }
}

impl Default for LatticeHttpClient {
    fn default() -> Self {
        Self::current()
    }
}

/// Convert an `http::Request<T>` into a Lattice capability request.
pub fn request_to_http_request<T>(request: Request<T>) -> Result<CapabilityRequest>
where
    T: Into<Bytes>,
{
    let (parts, body) = request.into_parts();
    let method = method_to_capability(parts.method)?;

    let mut request = CapabilityRequest::new(method, parts.uri.to_string());
    for (name, value) in parts.headers.iter() {
        let value = value.to_str().map_err(instance_error)?;
        request.headers.insert(name.as_str().to_owned(), value.to_owned());
    }

    let body = body.into();
    if !body.is_empty() {
        request.body = Some(body.to_vec());
    }

    Ok(request)
}

/// Convert a Lattice capability response into a lazy `http::Response` body.
pub fn response_from_http_response<U>(response: CapabilityResponse) -> Result<Response<LazyBody<U>>>
where
    U: From<Bytes> + 'static,
{
    let CapabilityResponse {
        status,
        headers,
        body,
    } = response;

    let status = StatusCode::from_u16(status).map_err(|_| invalid_status_error(status))?;
    let mut builder = Response::builder().status(status);

    for (name, value) in headers.iter() {
        let value = HeaderValue::from_str(value)?;
        builder = builder.header(name.as_str(), value);
    }

    let body: LazyBody<U> = Box::pin(async move { Ok(U::from(Bytes::from(body))) });
    builder.body(body).map_err(Error::Protocol)
}

fn method_to_capability(method: Method) -> Result<CapabilityMethod> {
    match method.as_str() {
        "GET" => Ok(CapabilityMethod::Get),
        "HEAD" => Ok(CapabilityMethod::Head),
        "POST" => Ok(CapabilityMethod::Post),
        "PUT" => Ok(CapabilityMethod::Put),
        "PATCH" => Ok(CapabilityMethod::Patch),
        "DELETE" => Ok(CapabilityMethod::Delete),
        _ => Err(unsupported_method_error(method)),
    }
}

fn capability_method_uses_read_client(method: CapabilityMethod) -> bool {
    matches!(method, CapabilityMethod::Get | CapabilityMethod::Head)
}

async fn dispatch_http_request(
    resources: Arc<dyn ResourceAccess>,
    request: CapabilityRequest,
) -> Result<CapabilityResponse> {
    let method = request.method;

    if capability_method_uses_read_client(method) {
        let client = resources
            .http_read()
            .ok_or_else(|| missing_capability_error(method.as_str(), "read"))?;
        client.send(request).await.map_err(capability_error)
    } else {
        let client = resources
            .http_write()
            .ok_or_else(|| missing_capability_error(method.as_str(), "write"))?;
        client.send(request).await.map_err(capability_error)
    }
}

impl HttpClientExt for LatticeHttpClient {
    fn send<T, U>(
        &self,
        request: Request<T>,
    ) -> impl Future<Output = Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        T: Into<Bytes> + WasmCompatSend,
        U: From<Bytes> + WasmCompatSend + 'static,
    {
        let resources = Arc::clone(&self.resources);
        let request = request_to_http_request(request);

        async move {
            let request = request?;
            let response = dispatch_http_request(resources, request).await?;
            response_from_http_response(response)
        }
    }

    fn send_multipart<U>(
        &self,
        _request: Request<MultipartForm>,
    ) -> impl Future<Output = Result<Response<LazyBody<U>>>> + WasmCompatSend + 'static
    where
        U: From<Bytes> + WasmCompatSend + 'static,
    {
        async move { Err(unsupported_error("multipart")) }
    }

    fn send_streaming<T>(
        &self,
        _request: Request<T>,
    ) -> impl Future<Output = Result<StreamingResponse>> + WasmCompatSend
    where
        T: Into<Bytes>,
    {
        async move { Err(unsupported_error("streaming")) }
    }
}

#[derive(Clone, Debug, Default)]
struct NoHttpAccess;

impl ResourceAccess for NoHttpAccess {}

#[derive(Clone)]
struct ExplicitHttpAccess<R, W> {
    http_read: Arc<R>,
    http_write: Arc<W>,
}

impl<R, W> fmt::Debug for ExplicitHttpAccess<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExplicitHttpAccess").finish_non_exhaustive()
    }
}

impl<R, W> ResourceAccess for ExplicitHttpAccess<R, W>
where
    R: HttpRead + 'static,
    W: HttpWrite + 'static,
{
    fn http_read(&self) -> Option<&dyn HttpRead> {
        Some(self.http_read.as_ref())
    }

    fn http_write(&self) -> Option<&dyn HttpWrite> {
        Some(self.http_write.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use capabilities::http::{HttpHeaders, HttpResult};
    use futures::executor::block_on;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingState {
        read_calls: usize,
        write_calls: usize,
        read_request: Option<CapabilityRequest>,
        write_request: Option<CapabilityRequest>,
    }

    #[derive(Clone)]
    struct RecordingHttp {
        state: Arc<Mutex<RecordingState>>,
        read_response: CapabilityResponse,
        write_response: CapabilityResponse,
    }

    impl RecordingHttp {
        fn new(
            state: Arc<Mutex<RecordingState>>,
            read_response: CapabilityResponse,
            write_response: CapabilityResponse,
        ) -> Self {
            Self {
                state,
                read_response,
                write_response,
            }
        }
    }

    #[async_trait]
    impl HttpRead for RecordingHttp {
        async fn send(&self, request: CapabilityRequest) -> HttpResult<CapabilityResponse> {
            let mut state = self.state.lock().expect("state mutex poisoned");
            state.read_calls += 1;
            state.read_request = Some(request);
            Ok(self.read_response.clone())
        }
    }

    #[async_trait]
    impl HttpWrite for RecordingHttp {
        async fn send(&self, request: CapabilityRequest) -> HttpResult<CapabilityResponse> {
            let mut state = self.state.lock().expect("state mutex poisoned");
            state.write_calls += 1;
            state.write_request = Some(request);
            Ok(self.write_response.clone())
        }
    }

    fn capability_response(
        status: u16,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> CapabilityResponse {
        let mut response_headers = HttpHeaders::default();
        for (name, value) in headers {
            response_headers.insert((*name).to_owned(), (*value).to_owned());
        }

        CapabilityResponse {
            status,
            headers: response_headers,
            body: body.to_vec(),
        }
    }

    #[test]
    fn request_conversion_preserves_method_headers_and_body() {
        let request = Request::builder()
            .method(Method::PATCH)
            .uri("https://example.test/items/42")
            .header("x-test", "lattice")
            .header("content-type", "application/json")
            .body(Bytes::from_static(br#"{\"name\":\"alpha\"}"#))
            .expect("request");

        let capability = request_to_http_request(request).expect("converted request");

        assert_eq!(capability.method, CapabilityMethod::Patch);
        assert_eq!(capability.url, "https://example.test/items/42");
        assert_eq!(capability.headers.get("x-test").map(String::as_str), Some("lattice"));
        assert_eq!(
            capability.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(capability.body, Some(br#"{\"name\":\"alpha\"}"#.to_vec()));
    }

    #[test]
    fn response_conversion_preserves_status_headers_and_body() {
        let mut response = capability_response(
            201,
            &[("content-type", "application/json"), ("x-test", "bridge")],
            br#"{\"ok\":true}"#,
        );
        response.headers.insert("x-extra", "value");

        let response = response_from_http_response::<Bytes>(response).expect("converted response");

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            response.headers().get("x-test").and_then(|value| value.to_str().ok()),
            Some("bridge")
        );

        let body = block_on(response.into_body()).expect("response body");
        assert_eq!(body, Bytes::from_static(br#"{\"ok\":true}"#));
    }

    #[test]
    fn get_routes_through_read_client_and_post_routes_through_write_client() {
        let state = Arc::new(Mutex::new(RecordingState::default()));
        let client = LatticeHttpClient::from_http_client(Arc::new(RecordingHttp::new(
            Arc::clone(&state),
            capability_response(200, &[("x-source", "read")], b"read-ok"),
            capability_response(202, &[("x-source", "write")], b"write-ok"),
        )));

        let get_request = Request::builder()
            .method(Method::GET)
            .uri("https://example.test/read")
            .header("x-test", "lattice")
            .body(Bytes::from_static(b"payload"))
            .expect("get request");

        let response = block_on(client.send::<Bytes, Bytes>(get_request)).expect("get response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(block_on(response.into_body()).expect("get body"), Bytes::from_static(b"read-ok"));

        let post_request = Request::builder()
            .method(Method::POST)
            .uri("https://example.test/write")
            .header("x-test", "lattice")
            .body(Bytes::from_static(b"payload"))
            .expect("post request");

        let response = block_on(client.send::<Bytes, Bytes>(post_request)).expect("post response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(block_on(response.into_body()).expect("post body"), Bytes::from_static(b"write-ok"));

        let state = state.lock().expect("state mutex poisoned");
        assert_eq!(state.read_calls, 1);
        assert_eq!(state.write_calls, 1);

        let read_request = state.read_request.as_ref().expect("read request");
        assert_eq!(read_request.method, CapabilityMethod::Get);
        assert_eq!(read_request.url, "https://example.test/read");
        assert_eq!(read_request.headers.get("x-test").map(String::as_str), Some("lattice"));
        assert_eq!(read_request.body.as_deref(), Some(b"payload".as_ref()));

        let write_request = state.write_request.as_ref().expect("write request");
        assert_eq!(write_request.method, CapabilityMethod::Post);
        assert_eq!(write_request.url, "https://example.test/write");
        assert_eq!(write_request.headers.get("x-test").map(String::as_str), Some("lattice"));
        assert_eq!(write_request.body.as_deref(), Some(b"payload".as_ref()));
    }

    #[test]
    fn unsupported_streaming_and_multipart_are_explicit() {
        let client = LatticeHttpClient::default();

        let streaming_request = Request::builder()
            .method(Method::GET)
            .uri("https://example.test/stream")
            .body(NoBody)
            .expect("streaming request");
        let streaming_error = block_on(client.send_streaming(streaming_request))
            .err()
            .expect("streaming error");
        assert!(streaming_error.to_string().contains("streaming requests are unsupported"));

        let multipart_request = Request::builder()
            .method(Method::POST)
            .uri("https://example.test/upload")
            .body(MultipartForm::new().text("field", "value"))
            .expect("multipart request");
        let multipart_error = block_on(client.send_multipart::<Bytes>(multipart_request))
            .err()
            .expect("multipart error");
        assert!(multipart_error.to_string().contains("multipart requests are unsupported"));
    }
}
