use cap_http_workers::WorkersHttpClient;
use capabilities::http::{HttpMethod, HttpRead, HttpRequest};
use worker::{event, Context, Env, Request, Response, Result, RouteContext, Router};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/timeout", handle_timeout)
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

async fn handle_health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    Response::ok("ok")
}
