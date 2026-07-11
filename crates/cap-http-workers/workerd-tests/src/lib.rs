use cap_http_workers::WorkersHttpClient;
use capabilities::http::{HttpMethod, HttpRead, HttpRequest, RedirectMode};
use worker::{event, Context, Env, Request, Response, Result, RouteContext, Router};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/timeout", handle_timeout)
        .get_async("/redirect-off", handle_redirect_off)
        .get_async("/redirect-follow", handle_redirect_follow)
        .get_async("/health", handle_health)
        .run(req, env)
        .await
}

/// Tests that WorkersHttpClient properly times out and returns HttpError::Timeout.
/// The mock endpoint at https://miniflare.mocks/slow delays for 500ms,
/// but we set a 50ms timeout, so we should get a timeout error.
async fn handle_timeout(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    let client = WorkersHttpClient::new();

    let mut request = HttpRequest::new(HttpMethod::Get, "https://miniflare.mocks/slow");
    request.timeout_ms = Some(50);

    match HttpRead::send(&client, request).await {
        Err(capabilities::http::HttpError::Timeout(ms)) => {
            Response::ok(format!("Timeout({})", ms))
        }
        Err(e) => Response::ok(format!("UnexpectedError: {:?}", e)),
        Ok(resp) => Response::ok(format!("UnexpectedSuccess: status={}", resp.status)),
    }
}

/// Extract the `base` query parameter — the origin of the local HTTP server
/// the vitest harness runs (real outbound fetch; see redirect.spec.ts for
/// why `fetchMock` cannot be used here).
fn base_from(req: &Request) -> Result<String> {
    let url = req.url()?;
    url.query_pairs()
        .find(|(key, _)| key == "base")
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| worker::Error::RustError("missing `base` query parameter".into()))
}

/// Packet H2a definition of done (Workers provider path): with
/// `RedirectMode::Off`, a 3xx from `{base}/hop` is SURFACED to the caller
/// (status + Location as response data) and never followed to `/target`.
async fn handle_redirect_off(req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    let client = WorkersHttpClient::new();

    let request = HttpRequest::new(HttpMethod::Get, format!("{}/hop", base_from(&req)?))
        .with_redirect(RedirectMode::Off);

    match HttpRead::send(&client, request).await {
        Ok(resp) => {
            let location = resp
                .headers
                .get("location")
                .cloned()
                .unwrap_or_else(|| "<missing>".to_string());
            Response::ok(format!(
                "status={} location={} body={}",
                resp.status,
                location,
                String::from_utf8_lossy(&resp.body)
            ))
        }
        Err(e) => Response::ok(format!("Error: {:?}", e)),
    }
}

/// Backward-compat lock: the serde-default (`Follow`) keeps the historical
/// fetch behavior — the same 3xx hop is followed through to the target.
async fn handle_redirect_follow(req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    let client = WorkersHttpClient::new();

    let request = HttpRequest::new(HttpMethod::Get, format!("{}/hop", base_from(&req)?));

    match HttpRead::send(&client, request).await {
        Ok(resp) => Response::ok(format!(
            "status={} body={}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        )),
        Err(e) => Response::ok(format!("Error: {:?}", e)),
    }
}

async fn handle_health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    Response::ok("ok")
}
