use base64::Engine as _;
use capabilities::http::{HttpMethod, HttpRequest};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

use crate::{
    DEFAULT_ZOOM_HTTP_TIMEOUT_MS, DEFAULT_ZOOM_OAUTH_TOKEN_URL, ZoomRequestConfig, apply_timeout,
};

pub const ZOOM_SERVER_TO_SERVER_GRANT_TYPE: &str = "account_credentials";

const QUERY_COMPONENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b'/')
    .add(b'=')
    .add(b'?');

pub fn basic_authorization_value(client_id: &str, client_secret: &str) -> String {
    let credentials = format!("{client_id}:{client_secret}");
    let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
    format!("Basic {encoded}")
}

pub fn bearer_authorization_value(access_token: &str) -> String {
    format!("Bearer {access_token}")
}

pub fn apply_bearer_authorization(request: &mut HttpRequest, access_token: &str) {
    request
        .headers
        .insert("Authorization", bearer_authorization_value(access_token));
}

pub fn server_to_server_token_request(
    account_id: &str,
    client_id: &str,
    client_secret: &str,
) -> HttpRequest {
    server_to_server_token_request_with_parts(
        account_id,
        client_id,
        client_secret,
        DEFAULT_ZOOM_OAUTH_TOKEN_URL,
        Some(DEFAULT_ZOOM_HTTP_TIMEOUT_MS),
    )
}

pub fn server_to_server_token_request_with_config(
    account_id: &str,
    client_id: &str,
    client_secret: &str,
    config: &ZoomRequestConfig,
) -> HttpRequest {
    server_to_server_token_request_with_parts(
        account_id,
        client_id,
        client_secret,
        &config.oauth_token_url,
        config.timeout_ms,
    )
}

fn server_to_server_token_request_with_parts(
    account_id: &str,
    client_id: &str,
    client_secret: &str,
    oauth_token_url: &str,
    timeout_ms: Option<u64>,
) -> HttpRequest {
    let url = format!(
        "{oauth_token_url}?grant_type={grant_type}&account_id={account_id}",
        grant_type =
            utf8_percent_encode(ZOOM_SERVER_TO_SERVER_GRANT_TYPE, QUERY_COMPONENT_ENCODE_SET,),
        account_id = utf8_percent_encode(account_id, QUERY_COMPONENT_ENCODE_SET),
    );
    let mut request = HttpRequest::new(HttpMethod::Post, url);
    apply_timeout(&mut request, timeout_ms);
    request.headers.insert("Accept", "application/json");
    request.headers.insert(
        "Authorization",
        basic_authorization_value(client_id, client_secret),
    );
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_to_server_token_request_when_using_defaults_builds_zoom_oauth_request() {
        let request = server_to_server_token_request("acct-123", "client-id", "client-secret");

        assert_eq!(request.method, HttpMethod::Post);
        assert_eq!(
            request.url,
            "https://zoom.us/oauth/token?grant_type=account_credentials&account_id=acct-123"
        );
        assert_eq!(request.timeout_ms, Some(10_000));
        assert_eq!(
            request.headers.get("Accept"),
            Some(&"application/json".to_string())
        );
        assert_eq!(
            request.headers.get("Authorization"),
            Some(&"Basic Y2xpZW50LWlkOmNsaWVudC1zZWNyZXQ=".to_string())
        );
        assert!(request.body.is_none());
    }

    #[test]
    fn test_server_to_server_token_request_with_config_when_overridden_uses_custom_url_and_timeout()
    {
        let config = ZoomRequestConfig::default()
            .with_oauth_token_url("https://zoom.example.test/oauth/token")
            .with_timeout(None);

        let request = server_to_server_token_request_with_config(
            "acct 123",
            "client-id",
            "client-secret",
            &config,
        );

        assert_eq!(request.method, HttpMethod::Post);
        assert_eq!(
            request.url,
            "https://zoom.example.test/oauth/token?grant_type=account_credentials&account_id=acct%20123"
        );
        assert_eq!(request.timeout_ms, None);
    }
}
