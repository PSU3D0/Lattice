//! Handwritten HTTP transport for `connector.http`.
//!
//! This is the security-load-bearing core the whole family exists to protect:
//! path/header hygiene (§10), lock-origin pinning (HTTP105), Tier-2 SSRF
//! guarding, optional outbound-auth application applied *after* URL composition
//! and origin assertion (§7), `redirect: Off` on every request it builds
//! (H2a), and the §5 response-decode modes with their fail-closed semantics.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use capabilities::connector::{
    ConnectorRuntimeError as HostConnectorRuntimeError, OutboundAuthProfileDescriptor,
};
use capabilities::http::{HttpMethod, HttpRequest, HttpResponse, RedirectMode};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::http::append_query_pair;
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    resolve_endpoint_with_context, send_request_from_current,
};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

use crate::generated::profiles::HTTP_TARGET_ENDPOINT_PROFILE;
use crate::generated::types::HttpFullResponse;
use crate::runtime::errors::{ConnectorRuntimeError, HttpConnectorError};

const BODY_EXCERPT_CHARS: usize = 240;

/// Case-insensitive header-name denylist (§10). `Authorization` must come from
/// an auth handle, never from node data.
const HEADER_NAME_DENYLIST: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "cookie",
];

/// Allowlisted response headers surfaced by `full_response` (§10). Set-Cookie
/// never crosses into flow data.
const RESPONSE_HEADER_ALLOWLIST: &[&str] = &[
    "content-type",
    "etag",
    "location",
    "retry-after",
    "x-request-id",
];

/// Path-segment percent-encoding set: encode everything non-alphanumeric except
/// the unreserved marks so ordinary path characters survive.
const PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// A single HTTP call assembled from runtime node input.
pub struct HttpCall<'a> {
    pub method: HttpMethod,
    /// Relative path (Tier 0/1) or absolute URL (Tier 2).
    pub path_or_url: &'a str,
    pub query: &'a [(String, String)],
    pub headers: &'a BTreeMap<String, String>,
    pub body: Option<&'a JsonValue>,
    /// Optional outbound-auth descriptor. Tier 2 callers MUST pass `None`.
    pub auth: Option<&'static OutboundAuthProfileDescriptor>,
}

/// A single HTTP call carrying a pre-assembled raw body + explicit
/// `Content-Type` (the byte-egress path — `multipart/form-data` for
/// `post_multipart`/`put_multipart`, spec §16.5). Distinct from [`HttpCall`],
/// whose body is JSON that the transport serializes and content-types itself.
pub struct HttpRawCall<'a> {
    pub method: HttpMethod,
    pub path_or_url: &'a str,
    pub query: &'a [(String, String)],
    pub headers: &'a BTreeMap<String, String>,
    /// `Content-Type` header value for the whole body (carries the multipart
    /// boundary).
    pub content_type: &'a str,
    pub body: Vec<u8>,
    pub auth: Option<&'static OutboundAuthProfileDescriptor>,
}

enum Binding {
    /// Tier 0/1: origin bound at lock time.
    Origin(ResolvedEndpointProfile),
    /// Tier 2: absolute URL supplied at runtime, no lock origin, no lock auth.
    AnyOrigin,
}

pub struct HttpApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    binding: Binding,
}

impl HttpApi {
    /// Tier 0/1 constructor: resolve the bound endpoint profile origin.
    pub async fn for_target(action_id: &'static str) -> Result<Self, HttpConnectorError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&HTTP_TARGET_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            binding: Binding::Origin(endpoint),
        })
    }

    /// Tier 2 constructor: no origin resolution; the URL arrives as data.
    pub async fn for_any_origin(action_id: &'static str) -> Result<Self, HttpConnectorError> {
        let context = current_connector_context(action_id).await?;
        Ok(Self {
            action_id,
            context,
            binding: Binding::AnyOrigin,
        })
    }

    /// Build + send the request, returning the raw response (no status policy
    /// applied). Decode via [`decode_json`]/[`decode_typed`]/etc.
    pub async fn send(&self, call: HttpCall<'_>) -> Result<HttpResponse, HttpConnectorError> {
        let url = match &self.binding {
            Binding::Origin(endpoint) => compose_tier0_url(endpoint, call.path_or_url, call.query)?,
            Binding::AnyOrigin => compose_tier2_url(call.path_or_url, call.query)?,
        };

        let mut request = HttpRequest::new(call.method, url);
        request.timeout_ms = Some(10_000);
        // Security invariant (§10 / H2a): connector.http never follows redirects.
        request.redirect = RedirectMode::Off;

        if let Binding::Origin(endpoint) = &self.binding {
            apply_default_headers(&mut request.headers, endpoint);
        }

        apply_user_headers(&mut request, call.headers)?;

        if let Some(body) = call.body {
            request
                .headers
                .insert("Content-Type", "application/json; charset=utf-8");
            request.body = Some(serde_json::to_vec(body)?);
        }

        // Auth is applied AFTER composition + origin assertion (§7). Tier 2
        // callers pass `None`; lock-granted credentials cannot attach there.
        if let Some(auth) = call.auth {
            apply_optional_auth(&mut request, auth, &self.context).await?;
        }

        let response = send_request_from_current(self.action_id, call.method, request).await?;
        Ok(response)
    }

    /// `json` mode (spec §5): decoded 2xx JSON value; non-2xx → HTTP101.
    pub async fn json(&self, call: HttpCall<'_>) -> Result<JsonValue, HttpConnectorError> {
        decode_json(&self.send(call).await?)
    }

    /// `typed<T>` mode (spec §5): fail-closed serde decode into `T`.
    pub async fn typed<T: DeserializeOwned>(
        &self,
        call: HttpCall<'_>,
    ) -> Result<T, HttpConnectorError> {
        decode_typed(&self.send(call).await?)
    }

    /// `text` mode (spec §5): UTF-8 body string; non-UTF-8 → HTTP104.
    pub async fn text(&self, call: HttpCall<'_>) -> Result<String, HttpConnectorError> {
        decode_text(&self.send(call).await?)
    }

    /// `full_response` mode (spec §5): in-band status, lenient body, no error
    /// on non-2xx.
    pub async fn full(&self, call: HttpCall<'_>) -> Result<HttpFullResponse, HttpConnectorError> {
        Ok(decode_full(&self.send(call).await?))
    }

    /// Binary/artifact ingress (spec §16.5): send, require a 2xx status (else
    /// HTTP101, unchanged from §5), and return the raw response body plus its
    /// `Content-Type` (defaulting to `application/octet-stream`). The caller
    /// stages the bytes into the run workspace through the `workspace_write()`
    /// view — the transport never touches the byte plane.
    pub async fn bytes(&self, call: HttpCall<'_>) -> Result<(Vec<u8>, String), HttpConnectorError> {
        let response = self.send(call).await?;
        if !response.is_success() {
            return Err(non_success_error(&response));
        }
        let content_type = response_content_type(&response);
        Ok((response.body, content_type))
    }

    /// Build + send a request carrying a pre-assembled raw body + explicit
    /// `Content-Type` (byte egress; spec §16.5). Mirrors [`Self::send`] but sets
    /// the caller's body/content-type instead of JSON-serializing.
    pub async fn send_raw(
        &self,
        call: HttpRawCall<'_>,
    ) -> Result<HttpResponse, HttpConnectorError> {
        let url = match &self.binding {
            Binding::Origin(endpoint) => compose_tier0_url(endpoint, call.path_or_url, call.query)?,
            Binding::AnyOrigin => compose_tier2_url(call.path_or_url, call.query)?,
        };

        let mut request = HttpRequest::new(call.method, url);
        request.timeout_ms = Some(10_000);
        request.redirect = RedirectMode::Off;

        if let Binding::Origin(endpoint) = &self.binding {
            apply_default_headers(&mut request.headers, endpoint);
        }
        apply_user_headers(&mut request, call.headers)?;

        request.headers.insert("Content-Type", call.content_type);
        request.body = Some(call.body);

        if let Some(auth) = call.auth {
            apply_optional_auth(&mut request, auth, &self.context).await?;
        }

        let response = send_request_from_current(self.action_id, call.method, request).await?;
        Ok(response)
    }

    /// `json` decode over a raw-body send (byte egress). Non-2xx → HTTP101.
    pub async fn json_raw(&self, call: HttpRawCall<'_>) -> Result<JsonValue, HttpConnectorError> {
        decode_json(&self.send_raw(call).await?)
    }
}

/// The 2xx response `Content-Type` (case-insensitive header lookup), or
/// `application/octet-stream` when absent (spec §16.5).
pub fn response_content_type(response: &HttpResponse) -> String {
    response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

/// Apply an OPTIONAL outbound-auth descriptor. An unbound optional role
/// (dev-adapter env var absent) is not an error — the request simply carries no
/// auth, honoring `required: false` (spec §7).
async fn apply_optional_auth(
    request: &mut HttpRequest,
    auth: &'static OutboundAuthProfileDescriptor,
    context: &CurrentConnectorContext,
) -> Result<(), HttpConnectorError> {
    match apply_outbound_auth_with_context(auth, request, context).await {
        Ok(()) => Ok(()),
        Err(ConnectorRuntimeError::ConnectorRuntime(
            HostConnectorRuntimeError::MissingAuthOverride { .. },
        )) => Ok(()),
        Err(err) => Err(HttpConnectorError::Runtime(err)),
    }
}

// ---- URL composition -------------------------------------------------------

fn compose_tier0_url(
    endpoint: &ResolvedEndpointProfile,
    path: &str,
    query: &[(String, String)],
) -> Result<String, HttpConnectorError> {
    validate_path(path)?;
    let base = endpoint.base_url.trim_end_matches('/');
    let mut url = format!("{base}{}", encode_path(path));
    for (name, value) in query {
        append_query_pair(&mut url, name, value);
    }
    assert_same_origin(&url, &endpoint.base_url)?;
    Ok(url)
}

fn compose_tier2_url(url: &str, query: &[(String, String)]) -> Result<String, HttpConnectorError> {
    ssrf_guard(url)?;
    let mut composed = url.to_string();
    for (name, value) in query {
        append_query_pair(&mut composed, name, value);
    }
    Ok(composed)
}

/// Validate a Tier-0/1 request path (spec §10). Public so the SSRF unit-vector
/// suite can assert each rejection directly.
pub fn validate_path(path: &str) -> Result<(), HttpConnectorError> {
    if !path.starts_with('/') {
        return Err(HttpConnectorError::PathInvalid(format!(
            "path must start with '/': `{path}`"
        )));
    }
    if path.starts_with("//") {
        return Err(HttpConnectorError::PathInvalid(
            "path must not start with '//' (network-path reference)".to_string(),
        ));
    }
    if path.contains('\\') {
        return Err(HttpConnectorError::PathInvalid(
            "path must not contain backslashes".to_string(),
        ));
    }
    if path.chars().any(|ch| matches!(ch, '\r' | '\n' | '\0')) {
        return Err(HttpConnectorError::PathInvalid(
            "path must not contain CR/LF/NUL control characters".to_string(),
        ));
    }
    if path.split('/').any(|segment| segment == "..") {
        return Err(HttpConnectorError::PathInvalid(
            "path must not contain `..` traversal segments".to_string(),
        ));
    }
    Ok(())
}

fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| utf8_percent_encode(segment, PATH_SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// Origin = `scheme://authority` up to the first `/`, `?`, or `#`.
fn origin_of(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    if scheme.is_empty() || authority.is_empty() {
        return None;
    }
    Some(format!(
        "{}://{}",
        scheme.to_ascii_lowercase(),
        authority.to_ascii_lowercase()
    ))
}

/// Assert the composed URL's origin equals the granted profile origin
/// (HTTP105, spec §7). Public for the SSRF unit-vector suite.
pub fn assert_same_origin(composed: &str, base: &str) -> Result<(), HttpConnectorError> {
    let composed_origin = origin_of(composed);
    let base_origin = origin_of(base);
    match (composed_origin, base_origin) {
        (Some(c), Some(b)) if c == b => Ok(()),
        (c, b) => Err(HttpConnectorError::OriginMismatch {
            composed: c.unwrap_or_else(|| composed.to_string()),
            granted: b.unwrap_or_else(|| base.to_string()),
        }),
    }
}

// ---- Tier 2 SSRF guard (§10) ----------------------------------------------

/// Tier-2 SSRF guard: https-only + hostname/IP denylist (spec §10). Public for
/// the SSRF unit-vector suite.
pub fn ssrf_guard(url: &str) -> Result<(), HttpConnectorError> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| HttpConnectorError::Ssrf(format!("absolute URL required: `{url}`")))?;
    if !scheme.eq_ignore_ascii_case("https") {
        return Err(HttpConnectorError::Ssrf(format!(
            "https scheme required for any-origin requests, got `{scheme}`"
        )));
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    // Strip userinfo and port.
    let host = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    let host = strip_port(host);
    if host.is_empty() {
        return Err(HttpConnectorError::Ssrf("empty host".to_string()));
    }
    let host_lower = host.to_ascii_lowercase();
    if host_lower == "localhost"
        || host_lower.ends_with(".localhost")
        || host_lower == "metadata.google.internal"
    {
        return Err(HttpConnectorError::Ssrf(format!(
            "denylisted host `{host}`"
        )));
    }
    if let Ok(ip) = host_lower.trim_matches(['[', ']']).parse::<IpAddr>()
        && is_forbidden_ip(ip)
    {
        return Err(HttpConnectorError::Ssrf(format!(
            "denylisted IP literal `{host}`"
        )));
    }
    Ok(())
}

fn strip_port(host: &str) -> &str {
    if let Some(stripped) = host.strip_prefix('[') {
        // IPv6 literal `[::1]:port`.
        return stripped.split(']').next().unwrap_or(stripped);
    }
    host.split(':').next().unwrap_or(host)
}

fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_forbidden_ipv4(v4),
        IpAddr::V6(v6) => is_forbidden_ipv6(v6),
    }
}

fn is_forbidden_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_documentation()
        || a == 0
        // CGNAT / shared address space 100.64.0.0/10 (RFC 6598).
        || (a == 100 && (64..=127).contains(&b))
}

fn is_forbidden_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        // Unique local fc00::/7.
        || (segments[0] & 0xfe00) == 0xfc00
        // Link-local fe80::/10.
        || (segments[0] & 0xffc0) == 0xfe80
        // IPv4-mapped ::ffff:0:0/96 → check the embedded v4.
        || ip.to_ipv4_mapped().is_some_and(is_forbidden_ipv4)
}

// ---- Header hygiene (§10, HTTP106) ----------------------------------------

fn apply_user_headers(
    request: &mut HttpRequest,
    headers: &BTreeMap<String, String>,
) -> Result<(), HttpConnectorError> {
    for (name, value) in headers {
        validate_header_pair(name, value)?;
        request.headers.insert(name.clone(), value.clone());
    }
    Ok(())
}

/// Validate a single user-supplied header (spec §10, HTTP106): RFC 7230 token
/// name, not on the credential/hop-by-hop denylist, no CR/LF/NUL in the value.
/// Public for the SSRF unit-vector suite.
pub fn validate_header_pair(name: &str, value: &str) -> Result<(), HttpConnectorError> {
    if !is_rfc7230_token(name) {
        return Err(HttpConnectorError::ForbiddenHeader(format!(
            "malformed header name `{name}`"
        )));
    }
    if HEADER_NAME_DENYLIST.contains(&name.to_ascii_lowercase().as_str()) {
        return Err(HttpConnectorError::ForbiddenHeader(format!(
            "header name `{name}` is denylisted (credential/hop-by-hop; move auth to a handle)"
        )));
    }
    if value.bytes().any(|byte| matches!(byte, b'\r' | b'\n' | 0)) {
        return Err(HttpConnectorError::ForbiddenHeader(format!(
            "CR/LF/NUL not allowed in value of header `{name}`"
        )));
    }
    Ok(())
}

fn is_rfc7230_token(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

// ---- Response decode (§5) --------------------------------------------------

fn body_excerpt(response: &HttpResponse) -> String {
    String::from_utf8_lossy(&response.body)
        .chars()
        .take(BODY_EXCERPT_CHARS)
        .collect()
}

fn non_success_error(response: &HttpResponse) -> HttpConnectorError {
    HttpConnectorError::NonSuccessStatus {
        status: response.status,
        excerpt: body_excerpt(response),
    }
}

pub fn decode_json(response: &HttpResponse) -> Result<JsonValue, HttpConnectorError> {
    if !response.is_success() {
        return Err(non_success_error(response));
    }
    if response.body.is_empty() {
        return Ok(JsonValue::Null);
    }
    serde_json::from_slice(&response.body)
        .map_err(|err| HttpConnectorError::BodyNotJson(err.to_string()))
}

pub fn decode_typed<T: DeserializeOwned>(response: &HttpResponse) -> Result<T, HttpConnectorError> {
    if !response.is_success() {
        return Err(non_success_error(response));
    }
    let value = if response.body.is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_slice::<JsonValue>(&response.body)
            .map_err(|err| HttpConnectorError::BodyNotJson(err.to_string()))?
    };
    serde_json::from_value(value).map_err(|err| HttpConnectorError::TypedMismatch(err.to_string()))
}

pub fn decode_text(response: &HttpResponse) -> Result<String, HttpConnectorError> {
    if !response.is_success() {
        return Err(non_success_error(response));
    }
    String::from_utf8(response.body.clone()).map_err(|_| HttpConnectorError::BodyNotUtf8)
}

pub fn decode_full(response: &HttpResponse) -> HttpFullResponse {
    let headers = response
        .headers
        .iter()
        .filter(|(name, _)| RESPONSE_HEADER_ALLOWLIST.contains(&name.to_ascii_lowercase().as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();

    let (body, body_text, utf8_lossy) = match std::str::from_utf8(&response.body) {
        Ok(text) => {
            let body = serde_json::from_str(text).unwrap_or(JsonValue::Null);
            (body, text.to_string(), false)
        }
        Err(_) => (
            JsonValue::Null,
            String::from_utf8_lossy(&response.body).into_owned(),
            true,
        ),
    };

    HttpFullResponse {
        status: response.status,
        headers,
        body,
        body_text,
        utf8_lossy,
    }
}
