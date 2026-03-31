use std::sync::{Arc, Mutex};

use base64::Engine;
use bytes::Bytes;
use http::{HeaderMap, Method, Response, StatusCode};
use llm_agent::image_generation::ImageGenerationModel as _;
use llm_agent::prelude::ImageGenerationClient;
use llm_provider_openai::{Client, DALL_E_3, GPT_IMAGE_1_5};
use llm_types::http_client::{
    self, HttpClientExt, LazyBody, MultipartForm, Request, StreamingResponse,
};
use serde_json::json;

#[derive(Debug, Clone, Default)]
struct RecordingHttpClient {
    state: Arc<Mutex<Option<RecordedRequest>>>,
    response_body: Arc<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: Method,
    uri: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl RecordingHttpClient {
    fn with_response_body(response_body: Vec<u8>) -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
            response_body: Arc::new(response_body),
        }
    }

    fn take_request(&self) -> RecordedRequest {
        self.state
            .lock()
            .expect("recording mutex poisoned")
            .take()
            .expect("request should have been recorded")
    }
}

impl HttpClientExt for RecordingHttpClient {
    fn send<T, U>(
        &self,
        req: Request<T>,
    ) -> impl std::future::Future<Output = http_client::Result<Response<LazyBody<U>>>>
    + llm_types::wasm_compat::WasmCompatSend
    + 'static
    where
        T: Into<Bytes> + llm_types::wasm_compat::WasmCompatSend,
        U: From<Bytes> + llm_types::wasm_compat::WasmCompatSend + 'static,
    {
        let state = Arc::clone(&self.state);
        let response_body = Arc::clone(&self.response_body);
        let (parts, body) = req.into_parts();
        let body: Bytes = body.into();

        async move {
            let recorded = RecordedRequest {
                method: parts.method,
                uri: parts.uri.to_string(),
                headers: parts.headers,
                body: body.to_vec(),
            };

            *state.lock().expect("recording mutex poisoned") = Some(recorded);

            let body: LazyBody<U> =
                Box::pin(async move { Ok(U::from(Bytes::from((*response_body).clone()))) });
            let response = Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .map_err(http_client::Error::Protocol)?;

            Ok(response)
        }
    }

    fn send_multipart<U>(
        &self,
        _req: Request<MultipartForm>,
    ) -> impl std::future::Future<Output = http_client::Result<Response<LazyBody<U>>>>
    + llm_types::wasm_compat::WasmCompatSend
    + 'static
    where
        U: From<Bytes> + llm_types::wasm_compat::WasmCompatSend + 'static,
    {
        async move {
            Err(http_client::Error::InvalidStatusCode(
                StatusCode::NOT_IMPLEMENTED,
            ))
        }
    }

    fn send_streaming<T>(
        &self,
        _req: Request<T>,
    ) -> impl std::future::Future<Output = http_client::Result<StreamingResponse>>
    + llm_types::wasm_compat::WasmCompatSend
    where
        T: Into<Bytes>,
    {
        async move {
            Err(http_client::Error::InvalidStatusCode(
                StatusCode::NOT_IMPLEMENTED,
            ))
        }
    }
}

#[test]
fn image_generation_uses_client_surface_and_dall_e_3_payload() {
    let response_json = json!({
        "created": 1,
        "data": [{
            "b64_json": base64::engine::general_purpose::STANDARD.encode(b"test-bytes")
        }]
    });
    let http = RecordingHttpClient::with_response_body(serde_json::to_vec(&response_json).unwrap());

    let client = Client::<RecordingHttpClient>::builder()
        .api_key("test-key")
        .http_client(http.clone())
        .build()
        .expect("client should build");

    let model = client.image_generation_model(DALL_E_3);
    let response = futures::executor::block_on(
        model
            .image_generation_request()
            .prompt("A neon sign over a rainy street")
            .width(1024)
            .height(1024)
            .send(),
    )
    .expect("image generation should succeed");

    assert_eq!(response.image, b"test-bytes");
    assert_eq!(response.response.created, 1);

    let recorded = http.take_request();
    assert_eq!(recorded.method, Method::POST);
    assert_eq!(recorded.uri, "https://api.openai.com/v1/images/generations");
    assert_eq!(
        recorded
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer test-key")
    );
    assert_eq!(
        recorded
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );

    let body: serde_json::Value = serde_json::from_slice(&recorded.body).unwrap();
    assert_eq!(body["model"], DALL_E_3);
    assert_eq!(body["prompt"], "A neon sign over a rainy street");
    assert_eq!(body["size"], "1024x1024");
    assert_eq!(body["response_format"], "b64_json");
}

#[test]
fn image_generation_uses_gpt_image_1_5_without_legacy_response_format() {
    let response_json = json!({
        "created": 1,
        "data": [{
            "b64_json": base64::engine::general_purpose::STANDARD.encode(b"gpt-image-bytes")
        }]
    });
    let http = RecordingHttpClient::with_response_body(serde_json::to_vec(&response_json).unwrap());

    let client = Client::<RecordingHttpClient>::builder()
        .api_key("test-key")
        .http_client(http.clone())
        .build()
        .expect("client should build");

    let model = client.image_generation_model(GPT_IMAGE_1_5);
    let response = futures::executor::block_on(
        model
            .image_generation_request()
            .prompt("A modern abstract hero graphic")
            .width(1024)
            .height(1024)
            .send(),
    )
    .expect("image generation should succeed");

    assert_eq!(response.image, b"gpt-image-bytes");
    let recorded = http.take_request();
    let body: serde_json::Value = serde_json::from_slice(&recorded.body).unwrap();
    assert_eq!(body["model"], GPT_IMAGE_1_5);
    assert_eq!(body["prompt"], "A modern abstract hero graphic");
    assert_eq!(body["size"], "1024x1024");
    assert!(body.get("response_format").is_none());
}
