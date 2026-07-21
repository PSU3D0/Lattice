use http::{HeaderMap, HeaderValue};
use llm_agent::client::Client as ProviderClient;
use llm_types::http_client::HttpClientExt;
use std::fmt;

const HOST: &str = "gateway.ai.cloudflare.com";
const AUTH_HEADER: &str = "cf-aig-authorization";
const PAYLOAD_LOG_HEADER: &str = "cf-aig-collect-log-payload";

/// Provider-native Cloudflare AI Gateway route. This configures the existing
/// provider stack; it does not define a second adapter or semantic contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Provider {
    OpenAi,
    Anthropic,
}

impl Provider {
    fn component(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
        }
    }
}

#[derive(Clone)]
pub struct Authorization(String);

impl Authorization {
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
        let value = value.into();
        if value.len() < 32
            || value.len() > 4096
            || !value.is_ascii()
            || value.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(ConfigError::InvalidAuthorization);
        }
        Ok(Self(value))
    }
}

impl fmt::Debug for Authorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Authorization([REDACTED])")
    }
}

impl Drop for Authorization {
    fn drop(&mut self) {
        self.0.clear();
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ConfigError {
    #[error("invalid AI Gateway account identifier")]
    InvalidAccountId,
    #[error("invalid AI Gateway gateway identifier")]
    InvalidGatewayId,
    #[error("invalid AI Gateway authorization")]
    InvalidAuthorization,
    #[error("invalid AI Gateway header value")]
    InvalidHeader,
    #[error("AI Gateway client configuration failed")]
    ClientConfiguration,
}

#[derive(Clone)]
pub struct Config {
    account_id: String,
    gateway_id: String,
    authorization: Authorization,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("account_id", &self.account_id)
            .field("gateway_id", &self.gateway_id)
            .field("authorization", &"[REDACTED]")
            .finish()
    }
}

impl Config {
    pub fn new(
        account_id: impl Into<String>,
        gateway_id: impl Into<String>,
        authorization: Authorization,
    ) -> Result<Self, ConfigError> {
        let account_id = account_id.into();
        let gateway_id = gateway_id.into();
        if account_id.len() != 32 || !account_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ConfigError::InvalidAccountId);
        }
        if gateway_id.is_empty()
            || gateway_id.len() > 64
            || !gateway_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            })
        {
            return Err(ConfigError::InvalidGatewayId);
        }
        Ok(Self {
            account_id: account_id.to_ascii_lowercase(),
            gateway_id,
            authorization,
        })
    }

    pub fn provider_base_url(&self, provider: Provider) -> String {
        format!(
            "https://{HOST}/v1/{}/{}/{}",
            self.account_id,
            self.gateway_id,
            provider.component()
        )
    }

    fn headers(&self) -> Result<HeaderMap, ConfigError> {
        let mut headers = HeaderMap::new();
        let authorization = HeaderValue::from_str(&format!("Bearer {}", self.authorization.0))
            .map_err(|_| ConfigError::InvalidHeader)?;
        headers.insert(AUTH_HEADER, authorization);
        headers.insert(PAYLOAD_LOG_HEADER, HeaderValue::from_static("false"));
        Ok(headers)
    }

    pub fn openai_client<H>(
        &self,
        provider_api_key: impl Into<String>,
        http_client: H,
    ) -> Result<llm_provider_openai::Client<H>, ConfigError>
    where
        H: HttpClientExt + Default,
    {
        ProviderClient::<llm_provider_openai::OpenAICompletionsExt, H>::builder()
            .api_key(provider_api_key.into())
            .base_url(self.provider_base_url(Provider::OpenAi))
            .http_client(http_client)
            .http_headers(self.headers()?)
            .build()
            .map_err(|_| ConfigError::ClientConfiguration)
    }

    pub fn anthropic_client<H>(
        &self,
        provider_api_key: impl Into<String>,
        http_client: H,
    ) -> Result<llm_provider_anthropic::Client<H>, ConfigError>
    where
        H: HttpClientExt + Default,
    {
        ProviderClient::<llm_provider_anthropic::client::AnthropicExt, H>::builder()
            .api_key(provider_api_key.into())
            .base_url(self.provider_base_url(Provider::Anthropic))
            .http_client(http_client)
            .http_headers(self.headers()?)
            .build()
            .map_err(|_| ConfigError::ClientConfiguration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::{StreamExt, executor::block_on};
    use llm_types::{
        http_client::{
            Error, HttpClientExt, LazyBody, MultipartForm, Request, Response, StreamingResponse,
        },
        wasm_compat::WasmCompatSend,
    };
    use std::{
        future::Future,
        sync::{Arc, Mutex},
    };

    const ACCOUNT: &str = "0123456789abcdef0123456789abcdef";
    const TOKEN: &str = "host-only-gateway-token-000000000000";

    #[derive(Clone, Debug, Default)]
    struct PassthroughHttp {
        request: Arc<Mutex<Option<(String, HeaderMap, Vec<u8>)>>>,
        stream: Arc<Vec<u8>>,
    }

    impl HttpClientExt for PassthroughHttp {
        fn send<T, U>(
            &self,
            request: Request<T>,
        ) -> impl Future<Output = Result<Response<LazyBody<U>>, Error>> + WasmCompatSend + 'static
        where
            T: Into<Bytes> + WasmCompatSend,
            U: From<Bytes> + WasmCompatSend + 'static,
        {
            let (parts, body) = request.into_parts();
            let bytes = body.into().to_vec();
            *self.request.lock().unwrap() =
                Some((parts.uri.to_string(), parts.headers, bytes.clone()));
            async move {
                Response::builder()
                    .status(200)
                    .body(Box::pin(async move { Ok(U::from(Bytes::from(bytes))) }) as LazyBody<U>)
                    .map_err(Error::Protocol)
            }
        }

        fn send_multipart<U>(
            &self,
            _request: Request<MultipartForm>,
        ) -> impl Future<Output = Result<Response<LazyBody<U>>, Error>> + WasmCompatSend + 'static
        where
            U: From<Bytes> + WasmCompatSend + 'static,
        {
            async { Err(Error::NoHeaders) }
        }

        fn send_streaming<T>(
            &self,
            request: Request<T>,
        ) -> impl Future<Output = Result<StreamingResponse, Error>> + WasmCompatSend
        where
            T: Into<Bytes>,
        {
            let (parts, body) = request.into_parts();
            let bytes = body.into().to_vec();
            *self.request.lock().unwrap() = Some((parts.uri.to_string(), parts.headers, bytes));
            let stream = self.stream.clone();
            async move {
                let body: llm_types::http_client::sse::BoxedStream =
                    Box::pin(futures::stream::once(async move {
                        Ok(Bytes::copy_from_slice(&stream))
                    }));
                Response::builder()
                    .status(200)
                    .body(body)
                    .map_err(Error::Protocol)
            }
        }
    }

    fn config() -> Config {
        Config::new(ACCOUNT, "gateway-1", Authorization::new(TOKEN).unwrap()).unwrap()
    }

    #[test]
    fn accepts_only_constructed_provider_native_routes_and_redacts_auth() {
        let config = config();
        assert_eq!(
            config.provider_base_url(Provider::OpenAi),
            format!("https://{HOST}/v1/{ACCOUNT}/gateway-1/openai")
        );
        assert_eq!(
            config.provider_base_url(Provider::Anthropic),
            format!("https://{HOST}/v1/{ACCOUNT}/gateway-1/anthropic")
        );
        assert!(!format!("{config:?}").contains(TOKEN));
        assert_eq!(
            Config::new(ACCOUNT, "../escape", Authorization::new(TOKEN).unwrap()).unwrap_err(),
            ConfigError::InvalidGatewayId
        );
    }

    #[test]
    fn preserves_tool_and_stream_payload_bytes() {
        let stream_bytes = Arc::new(b"data: {\"tool_calls\":[{\"id\":\"call-1\"}]}\n\n".to_vec());
        let transport = PassthroughHttp {
            request: Arc::new(Mutex::new(None)),
            stream: stream_bytes.clone(),
        };
        let client = config()
            .openai_client("provider-key", transport.clone())
            .unwrap();
        let tool_bytes = Bytes::from_static(
            br#"{"messages":[{"role":"assistant","tool_calls":[{"id":"call-1"}]}]}"#,
        );
        let request = Request::builder()
            .method("POST")
            .uri(format!("{}/chat/completions", client.base_url()))
            .body(tool_bytes.clone())
            .unwrap();
        let response = block_on(client.send::<Bytes, Bytes>(request)).unwrap();
        assert_eq!(block_on(response.into_body()).unwrap(), tool_bytes);
        assert_eq!(
            transport.request.lock().unwrap().as_ref().unwrap().2,
            tool_bytes.as_ref()
        );

        let request = Request::builder()
            .method("POST")
            .uri(format!("{}/chat/completions", client.base_url()))
            .body(Bytes::from_static(b"stream-request"))
            .unwrap();
        let response = block_on(client.send_streaming(request)).unwrap();
        let chunks = block_on(response.into_body().collect::<Vec<_>>());
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].as_ref().unwrap().as_ref(),
            stream_bytes.as_slice()
        );
    }

    #[test]
    fn wires_host_owned_headers_without_exposing_token_in_debug() {
        let transport = PassthroughHttp::default();
        let openai = config().openai_client("provider-key", transport).unwrap();
        assert_eq!(
            openai.base_url(),
            format!("https://{HOST}/v1/{ACCOUNT}/gateway-1/openai")
        );
        assert_eq!(openai.headers().get(PAYLOAD_LOG_HEADER).unwrap(), "false");
        assert_eq!(
            openai.headers().get(AUTH_HEADER).unwrap().to_str().unwrap(),
            format!("Bearer {TOKEN}")
        );
        let rendered = format!("{openai:?}");
        assert!(!rendered.contains(TOKEN));
        assert!(!rendered.contains("provider-key"));
    }
}
