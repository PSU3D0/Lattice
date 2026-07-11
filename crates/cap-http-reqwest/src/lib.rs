//! Reqwest-backed HTTP capability for Lattice.
//!
//! KEEP as its own crate (decision recorded packet E2,
//! verifiability-substrate-hardening plan). Rationale: this crate isolates the
//! heavy `reqwest` (+ TLS) dependency from the lightweight `capabilities` trait
//! crate, so flows/connectors that only need the HTTP traits do not transitively
//! pull in reqwest. Folding it back into `capabilities` would re-couple that
//! dependency tree. Do not re-litigate without revisiting that tradeoff.

use anyhow::Context;
use async_trait::async_trait;
use capabilities::http::{
    self, HttpError, HttpHeaders, HttpRequest, HttpResponse, HttpResult, HttpWrite, RedirectMode,
};
use reqwest::{self, Client, redirect};
use std::time::Duration;
use tracing::instrument;

/// Reqwest-backed HTTP capability implementing both read and write traits.
///
/// Reqwest fixes its redirect policy at `Client`-build time, so honoring the
/// per-request `HttpRequest::redirect` field (packet H2a) requires two
/// clients: the shared follow-redirects client (historical behavior) and a
/// companion built with `redirect::Policy::none()` used only when a request
/// sets `RedirectMode::Off`.
pub struct ReqwestHttpClient {
    client: Client,
    no_redirect_client: Client,
}

impl ReqwestHttpClient {
    /// Construct a client from an existing `reqwest::Client`.
    ///
    /// The provided client serves `RedirectMode::Follow` requests unchanged
    /// (the pre-H2a shared-client path). Because reqwest cannot re-derive a
    /// builder from a built `Client`, `RedirectMode::Off` requests go through
    /// a companion client built with the DEFAULT configuration plus
    /// `redirect::Policy::none()`; callers that need custom TLS/proxy
    /// settings on the no-redirect path should use
    /// [`ReqwestHttpClient::new_with_clients`].
    pub fn new(client: Client) -> Self {
        let no_redirect_client = Client::builder()
            .redirect(redirect::Policy::none())
            .build()
            .expect("building default no-redirect reqwest client should not fail");
        Self::new_with_clients(client, no_redirect_client)
    }

    /// Construct from an explicit pair of clients: one for
    /// `RedirectMode::Follow` requests and one — which MUST be built with
    /// `redirect::Policy::none()` — for `RedirectMode::Off` requests.
    pub fn new_with_clients(client: Client, no_redirect_client: Client) -> Self {
        http::ensure_registered();
        Self {
            client,
            no_redirect_client,
        }
    }

    /// Build a client with the default TLS configuration.
    pub fn with_default_tls() -> Result<Self, reqwest::Error> {
        let client = Client::builder().build()?;
        let no_redirect_client = Client::builder()
            .redirect(redirect::Policy::none())
            .build()?;
        Ok(Self::new_with_clients(client, no_redirect_client))
    }

    async fn execute(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        let method = reqwest::Method::from_bytes(request.method.as_str().as_bytes())
            .context("invalid HTTP method")?;
        let client = match request.redirect {
            RedirectMode::Follow => &self.client,
            RedirectMode::Off => &self.no_redirect_client,
        };
        let mut builder = client.request(method, &request.url);

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

    #[tokio::test]
    async fn redirect_off_surfaces_3xx_and_never_follows() {
        let server = MockServer::start();
        let target_url = format!("{}/target", server.base_url());
        let hop = server.mock(|when, then| {
            when.method(GET).path("/hop");
            then.status(302).header("location", target_url.as_str());
        });
        let target = server.mock(|when, then| {
            when.method(GET).path("/target");
            then.status(200).body("followed");
        });

        let client = ReqwestHttpClient::default();
        let request = HttpRequest::new(HttpMethod::Get, format!("{}/hop", server.base_url()))
            .with_redirect(capabilities::http::RedirectMode::Off);
        let response = HttpRead::send(&client, request)
            .await
            .expect("3xx must be surfaced as a response, not an error");

        // The 3xx itself is observable as response data (status + Location)...
        hop.assert();
        assert_eq!(response.status, 302);
        assert_eq!(
            response.headers.get("location").map(String::as_str),
            Some(target_url.as_str())
        );
        // ...and the redirect target was NEVER fetched.
        target.assert_hits(0);
    }

    #[tokio::test]
    async fn redirect_default_still_follows() {
        // Backward-compat lock: requests that never set the field keep the
        // historical follow-redirects behavior.
        let server = MockServer::start();
        let target_url = format!("{}/target", server.base_url());
        let hop = server.mock(|when, then| {
            when.method(GET).path("/hop");
            then.status(302).header("location", target_url.as_str());
        });
        let target = server.mock(|when, then| {
            when.method(GET).path("/target");
            then.status(200).body("followed");
        });

        let client = ReqwestHttpClient::default();
        let request = HttpRequest::new(HttpMethod::Get, format!("{}/hop", server.base_url()));
        let response = HttpRead::send(&client, request)
            .await
            .expect("followed response");

        hop.assert();
        target.assert();
        assert_eq!(response.status, 200);
        assert_eq!(String::from_utf8_lossy(&response.body), "followed");
    }
}
