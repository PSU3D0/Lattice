use capabilities::http::{self, HttpError, HttpRequest, HttpResponse, HttpResult, HttpWrite};

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use std::time::Duration;

    use async_trait::async_trait;
    use futures_util::future::Either;
    use tracing::instrument;
    use worker::send::IntoSendFuture;
    use worker::{AbortController, Fetch, Request as WorkerRequest, RequestInit};

    /// Workers fetch-backed HTTP capability implementing both read and write traits.
    pub struct WorkersHttpClient;

    impl WorkersHttpClient {
        /// Construct a new Workers fetch client.
        pub fn new() -> Self {
            http::ensure_registered();
            Self
        }

        async fn execute(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
            let url = request.url.clone();

            let response = if let Some(timeout_ms) = request.timeout_ms {
                fetch_with_timeout(url, &request, timeout_ms).await?
            } else {
                let worker_request = {
                    let init = request_init_from_http(&request)?;
                    WorkerRequest::new_with_init(&url, &init).map_err(map_worker_error)?
                };
                Fetch::Request(worker_request)
                    .send()
                    .await
                    .map_err(map_worker_error)?
            };

            http_response_from_worker(response).await
        }
    }

    impl Default for WorkersHttpClient {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl http::HttpRead for WorkersHttpClient {
        #[instrument(name = "cap_http_workers.read", skip(self, request))]
        async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
            self.execute(request).into_send().await
        }
    }

    #[async_trait]
    impl HttpWrite for WorkersHttpClient {
        #[instrument(name = "cap_http_workers.write", skip(self, request))]
        async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
            self.execute(request).into_send().await
        }
    }

    fn request_init_from_http(request: &HttpRequest) -> HttpResult<RequestInit> {
        let mut init = RequestInit::new();
        init.with_method(method_from_http(request.method));

        let headers = headers_from_http(&request.headers)?;
        init.with_headers(headers);

        if let Some(body) = request.body.as_ref() {
            let array = js_sys::Uint8Array::from(body.as_slice());
            init.with_body(Some(array.into()));
        }

        Ok(init)
    }

    fn method_from_http(method: http::HttpMethod) -> worker::Method {
        match method {
            http::HttpMethod::Get => worker::Method::Get,
            http::HttpMethod::Head => worker::Method::Head,
            http::HttpMethod::Post => worker::Method::Post,
            http::HttpMethod::Put => worker::Method::Put,
            http::HttpMethod::Patch => worker::Method::Patch,
            http::HttpMethod::Delete => worker::Method::Delete,
        }
    }

    fn headers_from_http(headers: &capabilities::http::HttpHeaders) -> HttpResult<worker::Headers> {
        let worker_headers = worker::Headers::new();
        for (key, value) in headers.iter() {
            worker_headers
                .append(key.as_str(), value.as_str())
                .map_err(map_worker_error)?;
        }
        Ok(worker_headers)
    }

    async fn http_response_from_worker(mut response: worker::Response) -> HttpResult<HttpResponse> {
        let status = response.status_code();
        let mut collected = capabilities::http::HttpHeaders::default();
        for (key, value) in response.headers().entries() {
            collected.insert(key, value);
        }

        let bytes = response.bytes().await.map_err(map_worker_error)?;

        Ok(HttpResponse {
            status,
            headers: collected,
            body: bytes,
        })
    }

    async fn fetch_with_timeout(
        url: String,
        request: &HttpRequest,
        timeout_ms: u64,
    ) -> HttpResult<worker::Response> {
        let controller = AbortController::default();
        let signal = controller.signal();
        let worker_request = {
            let init = request_init_from_http(request)?;
            WorkerRequest::new_with_init(&url, &init).map_err(map_worker_error)?
        };

        let fetch_request = Fetch::Request(worker_request);
        let fetch = fetch_request.send_with_signal(&signal);
        let timeout = worker::Delay::from(Duration::from_millis(timeout_ms));

        futures_util::pin_mut!(fetch, timeout);

        match futures_util::future::select(fetch, timeout).await {
            Either::Left((result, _)) => result.map_err(map_worker_error),
            Either::Right((_elapsed, _)) => {
                controller.abort();
                Err(HttpError::Timeout(timeout_ms))
            }
        }
    }

    fn map_worker_error(err: worker::Error) -> HttpError {
        match err {
            worker::Error::RustError(msg) => HttpError::InvalidResponse(msg),
            worker::Error::BodyUsed => {
                HttpError::InvalidResponse("response body already used".into())
            }
            _ => HttpError::Transport(anyhow::Error::new(err)),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use capabilities::http::{
            HINT_HTTP, HINT_HTTP_READ, HINT_HTTP_WRITE, HttpMethod, HttpRead,
        };
        use dag_core::{
            determinism::constraint_for_hint as det_hint, effects_registry::constraint_for_hint,
        };
        use std::cell::RefCell;
        use std::collections::HashMap;
        use std::rc::Rc;
        use wasm_bindgen_test::wasm_bindgen_test;
        use worker::wasm_bindgen::{JsCast, JsValue, closure::Closure};
        use worker::wasm_bindgen_futures::{JsFuture, future_to_promise};
        use worker::web_sys::{AbortSignal, Request as WebRequest};
        use worker::{Headers, Response};

        thread_local! {
            static FETCH_STUB: RefCell<Option<Closure<dyn FnMut(JsValue, JsValue) -> js_sys::Promise>>> =
                RefCell::new(None);
        }

        #[derive(Default)]
        struct MockState {
            calls: usize,
            method: Option<String>,
            url: Option<String>,
            headers: HashMap<String, String>,
            body: Option<Vec<u8>>,
            signal: Option<AbortSignal>,
            response_status: u16,
            response_headers: Vec<(String, String)>,
            response_body: Vec<u8>,
            response_delay_ms: u64,
        }

        fn install_fetch_stub(state: Rc<RefCell<MockState>>) {
            let closure = Closure::wrap(Box::new(move |input: JsValue, init: JsValue| {
                let state = Rc::clone(&state);
                future_to_promise(async move {
                    let request = if let Ok(req) = input.clone().dyn_into::<WebRequest>() {
                        req
                    } else if let Some(url) = input.as_string() {
                        WebRequest::new_with_str(&url)?
                    } else {
                        return Err(JsValue::from_str("unsupported fetch input"));
                    };

                    let method = request.method();
                    let url = request.url();
                    let headers = Headers(request.headers());
                    let mut header_map = HashMap::new();
                    for (key, value) in headers.entries() {
                        header_map.insert(key, value);
                    }

                    let signal = extract_signal(&init);

                    let body = if request.body().is_some() {
                        let buffer = JsFuture::from(request.array_buffer()?).await?;
                        let bytes = js_sys::Uint8Array::new(&buffer).to_vec();
                        if bytes.is_empty() { None } else { Some(bytes) }
                    } else {
                        None
                    };

                    {
                        let mut snapshot = state.borrow_mut();
                        snapshot.calls += 1;
                        snapshot.method = Some(method);
                        snapshot.url = Some(url);
                        snapshot.headers = header_map;
                        snapshot.body = body;
                        snapshot.signal = signal;
                    }

                    let (delay_ms, status, response_headers, response_body) = {
                        let snapshot = state.borrow();
                        (
                            snapshot.response_delay_ms,
                            snapshot.response_status,
                            snapshot.response_headers.clone(),
                            snapshot.response_body.clone(),
                        )
                    };

                    if delay_ms > 0 {
                        worker::Delay::from(Duration::from_millis(delay_ms)).await;
                    }

                    let headers = Headers::new();
                    for (key, value) in response_headers {
                        headers
                            .append(&key, &value)
                            .map_err(|err| JsValue::from_str(&err.to_string()))?;
                    }

                    let response = Response::from_bytes(response_body)
                        .map_err(|err| JsValue::from_str(&err.to_string()))?
                        .with_status(status)
                        .with_headers(headers);
                    let web_response: worker::web_sys::Response = response.into();
                    Ok(web_response.into())
                })
            })
                as Box<dyn FnMut(JsValue, JsValue) -> js_sys::Promise>);

            let global = js_sys::global();
            js_sys::Reflect::set(&global, &JsValue::from_str("fetch"), closure.as_ref())
                .expect("install fetch shim");

            FETCH_STUB.with(|slot| {
                *slot.borrow_mut() = Some(closure);
            });
        }

        fn extract_signal(init: &JsValue) -> Option<AbortSignal> {
            if init.is_null() || init.is_undefined() {
                return None;
            }
            let signal = js_sys::Reflect::get(init, &JsValue::from_str("signal")).ok()?;
            if signal.is_null() || signal.is_undefined() {
                None
            } else {
                signal.dyn_into().ok()
            }
        }

        fn configure_response(
            state: &Rc<RefCell<MockState>>,
            status: u16,
            headers: &[(&str, &str)],
            body: &[u8],
            delay_ms: u64,
        ) {
            let mut snapshot = state.borrow_mut();
            snapshot.response_status = status;
            snapshot.response_headers = headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            snapshot.response_body = body.to_vec();
            snapshot.response_delay_ms = delay_ms;
        }

        #[wasm_bindgen_test]
        fn registration_occurs_on_init() {
            let _client = WorkersHttpClient::new();
            assert!(constraint_for_hint(HINT_HTTP_READ).is_some());
            assert!(constraint_for_hint(HINT_HTTP_WRITE).is_some());
            assert!(det_hint(HINT_HTTP).is_some());
        }

        #[wasm_bindgen_test(async)]
        async fn http_get_maps_response_and_headers() {
            let state = Rc::new(RefCell::new(MockState::default()));
            configure_response(&state, 200, &[("content-type", "text/plain")], b"ok", 0);
            install_fetch_stub(Rc::clone(&state));

            let client = WorkersHttpClient::new();
            let request = HttpRequest::new(HttpMethod::Get, "https://example.test/hello");
            let response = HttpRead::send(&client, request).await.expect("response ok");

            assert_eq!(response.status, 200);
            assert_eq!(String::from_utf8_lossy(&response.body), "ok");
            assert_eq!(
                response.headers.get("content-type").map(String::as_str),
                Some("text/plain")
            );

            let snapshot = state.borrow();
            assert_eq!(snapshot.calls, 1);
            assert_eq!(snapshot.method.as_deref(), Some("GET"));
            assert_eq!(snapshot.url.as_deref(), Some("https://example.test/hello"));
            assert!(snapshot.body.is_none());
        }

        #[wasm_bindgen_test(async)]
        async fn http_post_sends_body_and_headers() {
            let state = Rc::new(RefCell::new(MockState::default()));
            configure_response(&state, 201, &[("x-response", "ok")], b"created", 0);
            install_fetch_stub(Rc::clone(&state));

            let client = WorkersHttpClient::new();
            let request = HttpRequest::new(HttpMethod::Post, "https://example.test/submit")
                .with_header("x-test", "lattice")
                .with_body("payload");
            let response = http::HttpWrite::send(&client, request)
                .await
                .expect("response ok");

            assert_eq!(response.status, 201);
            assert_eq!(String::from_utf8_lossy(&response.body), "created");

            let snapshot = state.borrow();
            assert_eq!(snapshot.method.as_deref(), Some("POST"));
            assert_eq!(
                snapshot.headers.get("x-test").map(String::as_str),
                Some("lattice")
            );
            assert_eq!(snapshot.body.as_deref(), Some(b"payload".as_slice()));
        }

        // This test requires worker::Delay which only works in the Workers runtime.
        // Run via `npm test` in crates/cap-http-workers/ (Vitest + workerd).
        #[wasm_bindgen_test(async)]
        #[ignore]
        async fn timeout_aborts_fetch() {
            let state = Rc::new(RefCell::new(MockState::default()));
            configure_response(&state, 200, &[], b"late", 200);
            install_fetch_stub(Rc::clone(&state));

            let client = WorkersHttpClient::new();
            let mut request = HttpRequest::new(HttpMethod::Get, "https://example.test/slow");
            request.timeout_ms = Some(10);

            let result = HttpRead::send(&client, request).await;
            assert!(matches!(result, Err(HttpError::Timeout(10))));

            worker::Delay::from(Duration::from_millis(25)).await;
            let snapshot = state.borrow();
            let signal = snapshot.signal.as_ref().expect("signal captured");
            assert!(signal.aborted());
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::WorkersHttpClient;

#[cfg(not(target_arch = "wasm32"))]
pub struct WorkersHttpClient;

#[cfg(not(target_arch = "wasm32"))]
impl WorkersHttpClient {
    pub fn new() -> Self {
        http::ensure_registered();
        Self
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for WorkersHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl http::HttpRead for WorkersHttpClient {
    async fn send(&self, _request: HttpRequest) -> HttpResult<HttpResponse> {
        Err(HttpError::Transport(anyhow::anyhow!(
            "cap-http-workers requires wasm32"
        )))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl HttpWrite for WorkersHttpClient {
    async fn send(&self, _request: HttpRequest) -> HttpResult<HttpResponse> {
        Err(HttpError::Transport(anyhow::anyhow!(
            "cap-http-workers requires wasm32"
        )))
    }
}
