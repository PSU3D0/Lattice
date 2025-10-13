use anyhow::Context;
use async_trait::async_trait;
use capabilities::http::{
    self, HttpError, HttpHeaders, HttpRequest, HttpResponse, HttpResult, HttpWrite,
};
use reqwest::{self, Client};
use std::time::Duration;
use tracing::instrument;

/// Reqwest-backed HTTP capability implementing both read and write traits.
pub struct ReqwestHttpClient {
    client: Client,
}

impl ReqwestHttpClient {
    /// Construct a client from an existing `reqwest::Client`.
    pub fn new(client: Client) -> Self {
        http::ensure_registered();
        Self { client }
    }

    /// Build a client with the default TLS configuration.
    pub fn with_default_tls() -> Result<Self, reqwest::Error> {
        let client = Client::builder().build()?;
        Ok(Self::new(client))
    }

    async fn execute(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        let method = reqwest::Method::from_bytes(request.method.as_str().as_bytes())
            .context("invalid HTTP method")?;
        let mut builder = self.client.request(method, &request.url);

        builder = apply_headers(builder, &request.headers);
        if let Some(timeout_ms) = request.timeout_ms {
            builder = builder.timeout(Duration::from_millis(timeout_ms));
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        let response = builder.send().await.map_err(map_reqwest_error)?;
        let status = response.status().as_u16();
        let headers = response.headers();
        let mut collected = HttpHeaders::default();
        for (key, value) in headers.iter() {
            if let Ok(val_str) = value.to_str() {
                collected.insert(key.as_str().to_string(), val_str.to_string());
            }
        }
        let bytes = response.bytes().await.map_err(map_reqwest_error)?;

        Ok(HttpResponse {
            status,
            headers: collected,
            body: bytes.to_vec(),
        })
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        ReqwestHttpClient::with_default_tls()
            .expect("building default reqwest client should not fail")
    }
}

#[async_trait]
impl http::HttpRead for ReqwestHttpClient {
    #[instrument(name = "cap_http_reqwest.read", skip(self, request))]
    async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        self.execute(request).await
    }
}

#[async_trait]
impl HttpWrite for ReqwestHttpClient {
    #[instrument(name = "cap_http_reqwest.write", skip(self, request))]
    async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        self.execute(request).await
    }
}

fn apply_headers(
    mut builder: reqwest::RequestBuilder,
    headers: &HttpHeaders,
) -> reqwest::RequestBuilder {
    for (key, value) in headers.iter() {
        builder = builder.header(key.as_str(), value.as_str());
    }
    builder
}

fn map_reqwest_error(err: reqwest::Error) -> HttpError {
    if err.is_timeout() {
        HttpError::Timeout(0)
    } else {
        HttpError::Transport(anyhow::Error::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use capabilities::http::{
        HINT_HTTP, HINT_HTTP_READ, HINT_HTTP_WRITE, HttpMethod, HttpRead, HttpWrite,
    };
    use dag_core::{
        determinism::constraint_for_hint as det_hint, effects_registry::constraint_for_hint,
    };
    use httpmock::prelude::*;

    #[tokio::test]
    async fn get_request_fetches_body() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/hello");
            then.status(200)
                .header("content-type", "text/plain")
                .body("world");
        });

        let client = ReqwestHttpClient::with_default_tls().expect("client");
        let request = HttpRequest::new(HttpMethod::Get, format!("{}/hello", server.base_url()));
        let response = HttpRead::send(&client, request)
            .await
            .expect("successful response");

        mock.assert();
        assert_eq!(response.status, 200);
        assert_eq!(String::from_utf8_lossy(&response.body), "world");
        assert_eq!(
            response
                .headers
                .get("content-type")
                .map(|s| s.as_str())
                .unwrap_or_default(),
            "text/plain"
        );

        // Registration should have occurred implicitly.
        assert!(constraint_for_hint(HINT_HTTP_WRITE).is_some());
        assert!(constraint_for_hint(HINT_HTTP_READ).is_some());
        assert!(det_hint(HINT_HTTP).is_some());
    }

    #[tokio::test]
    async fn post_request_sends_body() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/echo")
                .header("x-test", "lattice")
                .body("payload");
            then.status(201).body("created");
        });

        let client = ReqwestHttpClient::default();
        let request = HttpRequest::new(HttpMethod::Post, format!("{}/echo", server.base_url()))
            .with_header("x-test", "lattice")
            .with_body("payload");
        let response = HttpWrite::send(&client, request)
            .await
            .expect("successful response");

        mock.assert();
        assert_eq!(response.status, 201);
        assert_eq!(String::from_utf8_lossy(&response.body), "created");
    }
}
