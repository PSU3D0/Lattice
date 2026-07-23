use std::collections::{BTreeMap, BTreeSet};

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use broker_core::{
    BrokerError,
    credential::{
        CredentialResponsePolicyV2,
        spi::{
            AuthenticatedRequestSink, BoundedRawResponse, CredentialMaterial, FirewallResult,
            OpaqueUnauthenticatedPlan, PrivateCredentialUpdate, PrivilegedResponseFirewall,
            ScrubbedProviderResponse, TrustedAuthDriver,
        },
    },
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

use crate::profile::{AuthProfile, AuthScheme};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryPair {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct FinalizedUnsignedRequest {
    endpoint_key: String,
    method: String,
    url: String,
    path: String,
    query: Vec<QueryPair>,
    headers: Vec<Header>,
    body: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestWire {
    endpoint_key: String,
    method: String,
    url: String,
    path: String,
    query: Vec<QueryPair>,
    headers: Vec<Header>,
    body_b64u: String,
}

impl FinalizedUnsignedRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn validate(
        profile: &AuthProfile,
        endpoint_key: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        query: Vec<QueryPair>,
        headers: Vec<Header>,
        body: Vec<u8>,
    ) -> Result<Self, BrokerError> {
        let endpoint_key = endpoint_key.into();
        let url = profile.endpoint(&endpoint_key)?.to_owned();
        let request = Self {
            endpoint_key,
            method: method.into(),
            url,
            path: path.into(),
            query,
            headers,
            body,
        };
        request.validate_for(profile)?;
        Ok(request)
    }

    pub fn into_plan(self) -> Result<OpaqueUnauthenticatedPlan, BrokerError> {
        let wire = RequestWire {
            endpoint_key: self.endpoint_key,
            method: self.method,
            url: self.url,
            path: self.path,
            query: self.query,
            headers: self.headers,
            body_b64u: URL_SAFE_NO_PAD.encode(self.body),
        };
        OpaqueUnauthenticatedPlan::from_canonical_bytes(
            serde_json::to_vec(&wire).map_err(|_| BrokerError::Brk305)?,
        )
    }

    fn from_plan(
        plan: &OpaqueUnauthenticatedPlan,
        profile: &AuthProfile,
    ) -> Result<Self, BrokerError> {
        let wire: RequestWire =
            serde_json::from_slice(plan.canonical_bytes()).map_err(|_| BrokerError::Brk305)?;
        if URL_SAFE_NO_PAD.encode(
            URL_SAFE_NO_PAD
                .decode(&wire.body_b64u)
                .map_err(|_| BrokerError::Brk305)?,
        ) != wire.body_b64u
        {
            return Err(BrokerError::Brk305);
        }
        let request = Self {
            endpoint_key: wire.endpoint_key,
            method: wire.method,
            url: wire.url,
            path: wire.path,
            query: wire.query,
            headers: wire.headers,
            body: URL_SAFE_NO_PAD
                .decode(wire.body_b64u)
                .map_err(|_| BrokerError::Brk305)?,
        };
        request.validate_for(profile)?;
        Ok(request)
    }

    fn validate_for(&self, profile: &AuthProfile) -> Result<(), BrokerError> {
        if self.url != profile.endpoint(&self.endpoint_key)?
            || !matches!(
                self.method.as_str(),
                "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
            )
            || !self.path.starts_with('/')
            || self.path.contains('#')
            || self.body.len() > 1024 * 1024
        {
            return Err(BrokerError::Brk302);
        }
        let mut header_names = BTreeSet::new();
        for header in &self.headers {
            let name = header.name.to_ascii_lowercase();
            if !valid_header_name(&name)
                || header.value.bytes().any(|b| b == b'\r' || b == b'\n')
                || !header_names.insert(name.clone())
            {
                return Err(BrokerError::Brk305);
            }
            if matches!(
                name.as_str(),
                "authorization"
                    | "proxy-authorization"
                    | "cookie"
                    | "set-cookie"
                    | "x-api-key"
                    | "x-amz-security-token"
            ) {
                return Err(BrokerError::Brk305);
            }
        }
        let mut query_names = BTreeSet::new();
        for item in &self.query {
            if item.name.is_empty()
                || item.name.bytes().any(|b| b.is_ascii_control())
                || !query_names.insert(item.name.clone())
            {
                return Err(BrokerError::Brk305);
            }
        }
        let (forbidden_header, forbidden_query) = match &profile.scheme {
            AuthScheme::HeaderKey { name, .. } => (Some(name.as_str()), None),
            AuthScheme::QueryKey { name } => (None, Some(name.as_str())),
            AuthScheme::Bearer { header, .. } | AuthScheme::Basic { header } => {
                (Some(header.as_str()), None)
            }
            AuthScheme::SignedRequest {
                timestamp_header, ..
            } => (Some(timestamp_header.as_str()), None),
            _ => (Some("Authorization"), None),
        };
        if forbidden_header.is_some_and(|forbidden| {
            self.headers
                .iter()
                .any(|h| h.name.eq_ignore_ascii_case(forbidden))
        }) || forbidden_query
            .is_some_and(|forbidden| self.query.iter().any(|q| q.name == forbidden))
        {
            return Err(BrokerError::Brk305);
        }
        Ok(())
    }
}

pub struct ProfileAuthDriver {
    profile: AuthProfile,
    clock_seconds: Box<dyn Fn() -> i64 + Send + Sync>,
}
impl ProfileAuthDriver {
    pub fn new(
        profile: AuthProfile,
        clock_seconds: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Result<Self, BrokerError> {
        profile.validate()?;
        Ok(Self {
            profile,
            clock_seconds: Box::new(clock_seconds),
        })
    }
}

#[derive(Deserialize)]
struct MaterialFields {
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    #[serde(default)]
    session_token: Option<String>,
}
impl Drop for MaterialFields {
    fn drop(&mut self) {
        for value in [
            &mut self.secret,
            &mut self.username,
            &mut self.password,
            &mut self.access_token,
            &mut self.access_key_id,
            &mut self.secret_access_key,
            &mut self.session_token,
        ]
        .into_iter()
        .flatten()
        {
            value.zeroize();
        }
    }
}

#[derive(Serialize)]
struct AuthenticatedWire<'a> {
    endpoint_key: &'a str,
    method: &'a str,
    url: &'a str,
    path: &'a str,
    query: &'a [QueryPair],
    headers: &'a [Header],
    body_b64u: String,
}

impl TrustedAuthDriver for ProfileAuthDriver {
    fn authorize(
        &self,
        plan: &OpaqueUnauthenticatedPlan,
        material: CredentialMaterial<'_>,
        broker_context_jcs: &[u8],
        sink: &mut dyn AuthenticatedRequestSink,
    ) -> Result<(), BrokerError> {
        let mut request = FinalizedUnsignedRequest::from_plan(plan, &self.profile)?;
        let decoded = Zeroizing::new(
            URL_SAFE_NO_PAD
                .decode(material.base64url())
                .map_err(|_| BrokerError::Brk109)?,
        );
        if URL_SAFE_NO_PAD.encode(&*decoded) != material.base64url() {
            return Err(BrokerError::Brk109);
        }
        let fields: MaterialFields =
            serde_json::from_slice(&decoded).map_err(|_| BrokerError::Brk109)?;
        match &self.profile.scheme {
            AuthScheme::HeaderKey { name, prefix } => request.headers.push(Header {
                name: name.clone(),
                value: secret_value(prefix, fields.secret.as_deref())?,
            }),
            AuthScheme::QueryKey { name } => request.query.push(QueryPair {
                name: name.clone(),
                value: required(fields.secret.as_deref())?.into(),
            }),
            AuthScheme::Bearer { header, prefix } => request.headers.push(Header {
                name: header.clone(),
                value: secret_value(
                    prefix,
                    fields.access_token.as_deref().or(fields.secret.as_deref()),
                )?,
            }),
            AuthScheme::Basic { header } => {
                let username = required(fields.username.as_deref())?;
                let password = required(fields.password.as_deref())?;
                if username.contains(':') {
                    return Err(BrokerError::Brk109);
                }
                request.headers.push(Header {
                    name: header.clone(),
                    value: format!(
                        "Basic {}",
                        STANDARD.encode(format!("{username}:{password}"))
                    ),
                });
            }
            AuthScheme::SignedRequest {
                region,
                service,
                timestamp_header,
                signed_headers,
            } => {
                let key_id = required(fields.access_key_id.as_deref())?;
                let key = required(fields.secret_access_key.as_deref())?;
                let timestamp = (self.clock_seconds)();
                request.headers.push(Header {
                    name: timestamp_header.clone(),
                    value: timestamp.to_string(),
                });
                if let Some(token) = fields.session_token.as_deref() {
                    request.headers.push(Header {
                        name: "x-amz-security-token".into(),
                        value: token.into(),
                    });
                }
                let canonical = canonical_signed_request(
                    &request,
                    region,
                    service,
                    signed_headers,
                    timestamp,
                    broker_context_jcs,
                )?;
                let signature = hmac_hex(key.as_bytes(), canonical.as_bytes());
                request.headers.push(Header { name: "Authorization".into(), value: format!("SIGV4 Credential={key_id},Region={region},Service={service},SignedHeaders={},Signature={signature}", signed_headers.iter().cloned().collect::<Vec<_>>().join(";")) });
            }
            AuthScheme::OAuthPkce { .. } | AuthScheme::WorkloadTokenExchange { .. } => {
                request.headers.push(Header {
                    name: "Authorization".into(),
                    value: format!("Bearer {}", required(fields.access_token.as_deref())?),
                })
            }
            AuthScheme::ExternalCustodian { .. } => return Err(BrokerError::Brk004),
        }
        let bytes = serde_json::to_vec(&AuthenticatedWire {
            endpoint_key: &request.endpoint_key,
            method: &request.method,
            url: &request.url,
            path: &request.path,
            query: &request.query,
            headers: &request.headers,
            body_b64u: URL_SAFE_NO_PAD.encode(&request.body),
        })
        .map_err(|_| BrokerError::Brk305)?;
        sink.seal(bytes)
    }
}

fn canonical_signed_request(
    request: &FinalizedUnsignedRequest,
    region: &str,
    service: &str,
    signed_headers: &BTreeSet<String>,
    timestamp: i64,
    context: &[u8],
) -> Result<String, BrokerError> {
    for required_header in signed_headers {
        if !request
            .headers
            .iter()
            .any(|h| h.name.eq_ignore_ascii_case(required_header))
        {
            return Err(BrokerError::Brk305);
        }
    }
    let query = request
        .query
        .iter()
        .map(|q| format!("{}={}", pct(&q.name), pct(&q.value)))
        .collect::<Vec<_>>()
        .join("&");
    let mut headers = request
        .headers
        .iter()
        .filter(|h| signed_headers.contains(&h.name.to_ascii_lowercase()))
        .map(|h| format!("{}:{}", h.name.to_ascii_lowercase(), h.value.trim()))
        .collect::<Vec<_>>();
    headers.sort();
    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        request.path,
        query,
        headers.join("\n"),
        hex(&Sha256::digest(&request.body)),
        region,
        service,
        hash_bytes(context, timestamp)
    ))
}
fn hash_bytes(context: &[u8], timestamp: i64) -> String {
    let mut h = Sha256::new();
    h.update(context);
    h.update(timestamp.to_be_bytes());
    hex(&h.finalize())
}
fn secret_value(prefix: &str, secret: Option<&str>) -> Result<String, BrokerError> {
    let value = required(secret)?;
    Ok(if prefix.is_empty() {
        value.into()
    } else {
        format!("{prefix} {value}")
    })
}
fn required(value: Option<&str>) -> Result<&str, BrokerError> {
    value
        .filter(|v| {
            !v.is_empty() && v.len() <= 64 * 1024 && !v.bytes().any(|b| b == b'\r' || b == b'\n')
        })
        .ok_or(BrokerError::Brk109)
}
fn valid_header_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}
fn hmac_hex(key: &[u8], value: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key");
    mac.update(value);
    hex(&mac.finalize().into_bytes())
}
fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut o = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        o.push(H[(b >> 4) as usize] as char);
        o.push(H[(b & 15) as usize] as char)
    }
    o
}
fn pct(value: &str) -> String {
    let mut o = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            o.push(b as char)
        } else {
            o.push('%');
            o.push_str(&format!("{b:02X}"))
        }
    }
    o
}

#[derive(Clone, Debug)]
pub enum FirewallPolicy {
    Forbidden {
        sensitive_pointers: BTreeSet<String>,
    },
    PrivilegedExtract {
        credential_pointers: BTreeMap<String, usize>,
        echo_pointers: BTreeSet<String>,
    },
}

pub struct StrictResponseFirewall;
impl StrictResponseFirewall {
    pub fn apply(
        &self,
        response: BoundedRawResponse,
        policy: &FirewallPolicy,
    ) -> Result<FirewallResult, BrokerError> {
        let bytes = response.bytes();
        let mut value: Value = serde_json::from_slice(bytes).map_err(|_| BrokerError::Brk305)?;
        match policy {
            FirewallPolicy::Forbidden { sensitive_pointers } => {
                if contains_credential_shape(&value)
                    || sensitive_pointers
                        .iter()
                        .any(|p| value.pointer(p).is_some())
                {
                    return Err(BrokerError::Brk305);
                }
                Ok(FirewallResult::new(
                    ScrubbedProviderResponse::from_privileged_firewall(value),
                    None,
                ))
            }
            FirewallPolicy::PrivilegedExtract {
                credential_pointers,
                echo_pointers,
            } => {
                let mut private = BTreeMap::new();
                for (pointer, max) in credential_pointers {
                    let extracted =
                        take_pointer(&mut value, pointer)?.ok_or(BrokerError::Brk305)?;
                    let encoded =
                        serde_json::to_vec(&extracted).map_err(|_| BrokerError::Brk305)?;
                    if encoded.len() > *max {
                        return Err(BrokerError::Brk305);
                    }
                    private.insert(pointer.clone(), extracted);
                }
                for pointer in echo_pointers {
                    let _ = take_pointer(&mut value, pointer)?;
                }
                if contains_credential_shape(&value) {
                    return Err(BrokerError::Brk305);
                }
                let update = PrivateCredentialUpdate::from_privileged_bytes(
                    serde_json::to_vec(&private).map_err(|_| BrokerError::Brk305)?,
                )?;
                Ok(FirewallResult::new(
                    ScrubbedProviderResponse::from_privileged_firewall(value),
                    Some(update),
                ))
            }
        }
    }
}
impl PrivilegedResponseFirewall for StrictResponseFirewall {
    fn classify_and_extract(
        &self,
        response: BoundedRawResponse,
        policy: &CredentialResponsePolicyV2,
    ) -> Result<FirewallResult, BrokerError> {
        let value = policy.as_value();
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk004)?;
        let policy = if kind == "forbidden" {
            FirewallPolicy::Forbidden {
                sensitive_pointers: value
                    .get("sensitive_json_pointers")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect(),
            }
        } else {
            return Err(BrokerError::Brk004);
        };
        self.apply(response, &policy)
    }
}
fn contains_credential_shape(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(k, v)| {
            let k = k.to_ascii_lowercase();
            matches!(
                k.as_str(),
                "access_token"
                    | "refresh_token"
                    | "id_token"
                    | "token"
                    | "token_type"
                    | "authorization"
                    | "cookie"
                    | "set-cookie"
                    | "api_key"
                    | "private_key"
                    | "signed_url"
            ) || contains_credential_shape(v)
        }),
        Value::Array(a) => a.iter().any(contains_credential_shape),
        _ => false,
    }
}
fn take_pointer(value: &mut Value, pointer: &str) -> Result<Option<Value>, BrokerError> {
    let (parent, last) = pointer.rsplit_once('/').ok_or(BrokerError::Brk004)?;
    let key = last.replace("~1", "/").replace("~0", "~");
    let target = if parent.is_empty() {
        value
    } else {
        value.pointer_mut(parent).ok_or(BrokerError::Brk305)?
    };
    match target {
        Value::Object(map) => Ok(map.remove(&key)),
        _ => Err(BrokerError::Brk305),
    }
}
