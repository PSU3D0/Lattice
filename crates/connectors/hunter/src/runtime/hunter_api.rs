//! Handwritten Hunter.io runtime: semantic `verify_email` over the shared
//! connectors-std transport (endpoint + auth resolved from the current connector
//! context, exactly like `connector_telegram`'s `TelegramApi`).
//!
//! ## Token-in-query auth
//!
//! Hunter authenticates with an `api_key` QUERY parameter, not an
//! `Authorization` header. The connector auth framework only mutates a request
//! (it never returns the raw secret), so — mirroring the Telegram token-in-path
//! pattern — we resolve the key through the standard `apply_outbound_auth` path
//! against a throwaway probe request, read the `Authorization: Bearer <key>` it
//! wrote, and append `<key>` as the `api_key` query parameter. A missing/denied
//! binding therefore still fails with the framework's role+env-named error.

use capabilities::http::{HttpMethod, HttpRequest};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::http::append_query_pair;
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::generated::profiles::{
    HUNTER_API_KEY_AUTH_OUTBOUND_AUTH, HUNTER_DEFAULT_ENDPOINT_PROFILE,
};
use crate::generated::types::{HunterVerifyEmailInput, HunterVerifyEmailOutput};
use crate::runtime::errors::ConnectorRuntimeError;

pub struct HunterApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl HunterApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&HUNTER_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn verify_email(
        &self,
        input: &HunterVerifyEmailInput,
    ) -> Result<HunterVerifyEmailOutput, ConnectorRuntimeError> {
        let api_key = self.resolve_api_key().await?;

        let mut url = format!(
            "{}{}",
            self.endpoint.base_url.trim_end_matches('/'),
            EMAIL_VERIFIER_PATH
        );
        append_query_pair(&mut url, "email", &input.email);
        append_query_pair(&mut url, "api_key", &api_key);

        let mut request = HttpRequest::new(HttpMethod::Get, url);
        apply_default_headers(&mut request.headers, &self.endpoint);

        let response = send_request_from_current(self.action_id, HttpMethod::Get, request).await?;
        let body = decode_json_response_body(&response)?;
        verify_email_output_from_response(&body)
    }

    /// Resolve the Hunter API key by applying the standard outbound-auth flow to
    /// a throwaway probe request and reading the bearer value it writes.
    async fn resolve_api_key(&self) -> Result<String, ConnectorRuntimeError> {
        let mut probe = HttpRequest::new(HttpMethod::Get, "hunter://api-key-probe");
        apply_outbound_auth_with_context(
            &HUNTER_API_KEY_AUTH_OUTBOUND_AUTH,
            &mut probe,
            &self.context,
        )
        .await?;

        probe
            .headers
            .get("Authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|key| !key.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ConnectorRuntimeError::invalid_response(
                    "hunter api key unavailable after outbound auth resolution",
                )
            })
    }
}

const EMAIL_VERIFIER_PATH: &str = "/v2/email-verifier";

#[derive(Deserialize)]
struct WireEnvelope {
    data: WireData,
}

#[derive(Deserialize)]
struct WireData {
    #[serde(default)]
    email: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    result: String,
    #[serde(default)]
    score: i64,
}

fn verify_email_output_from_response(
    body: &JsonValue,
) -> Result<HunterVerifyEmailOutput, ConnectorRuntimeError> {
    let envelope: WireEnvelope = serde_json::from_value(body.clone()).map_err(|err| {
        ConnectorRuntimeError::invalid_response(format!(
            "Hunter email-verifier response did not match the expected shape: {err}"
        ))
    })?;

    Ok(HunterVerifyEmailOutput {
        email: envelope.data.email,
        status: envelope.data.status,
        result: envelope.data.result,
        score: envelope.data.score,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_data_envelope() {
        let body = json!({
            "data": {
                "status": "valid",
                "result": "deliverable",
                "score": 91,
                "email": "ada@leads.test"
            },
            "meta": { "params": { "email": "ada@leads.test" } }
        });
        let out = verify_email_output_from_response(&body).expect("decode");
        assert_eq!(out.email, "ada@leads.test");
        assert_eq!(out.status, "valid");
        assert_eq!(out.result, "deliverable");
        assert_eq!(out.score, 91);
    }

    #[test]
    fn missing_data_is_invalid() {
        let body = json!({ "meta": {} });
        let err = verify_email_output_from_response(&body).expect_err("must reject");
        assert!(matches!(err, ConnectorRuntimeError::InvalidResponse(_)));
    }
}
