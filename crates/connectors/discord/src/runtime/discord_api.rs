//! Handwritten Discord webhook runtime: semantic `send_message` over the shared
//! connectors-std transport (endpoint + auth resolved from the current connector
//! context, exactly like `connector_telegram`'s `TelegramApi`).
//!
//! ## Token-in-path auth
//!
//! Discord executes a webhook by POSTing to a URL whose PATH embeds the
//! webhook id and token (`/api/webhooks/<id>/<token>`); the whole `<id>/<token>`
//! pair is the secret. The connector auth framework only mutates a request (it
//! never returns the raw secret), so — mirroring the Telegram bot-token pattern
//! — we resolve the secret through the standard `apply_outbound_auth` path
//! against a throwaway probe request, read the `Authorization: Bearer <token>`
//! it wrote, and splice `<token>` into the URL path. A missing/denied binding
//! therefore still fails with the framework's role+env-named error.
//!
//! ## Response shape
//!
//! A plain webhook execution returns `204 No Content` (no body). Executed with
//! `wait=true` it returns `200` with the created message object; this runtime
//! surfaces the message id when present but treats any 2xx as delivered.

use capabilities::http::{HttpMethod, HttpRequest, HttpResponse};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    resolve_endpoint_with_context, send_request_from_current,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use crate::generated::profiles::{
    DISCORD_WEBHOOK_AUTH_OUTBOUND_AUTH, DISCORD_WEBHOOK_DEFAULT_ENDPOINT_PROFILE,
};
use crate::generated::types::{DiscordEmbed, DiscordSendMessageInput, DiscordSendMessageOutput};
use crate::runtime::errors::ConnectorRuntimeError;

pub struct DiscordApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl DiscordApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&DISCORD_WEBHOOK_DEFAULT_ENDPOINT_PROFILE, &context)
                .await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn send_message(
        &self,
        input: &DiscordSendMessageInput,
    ) -> Result<DiscordSendMessageOutput, ConnectorRuntimeError> {
        let token = self.resolve_webhook_token().await?;
        let body = build_message_body(input);

        let response = self
            .request_post(&discord_webhook_path(&token), body)
            .await?;

        send_message_output_from_response(&response)
    }

    /// Resolve the webhook `<id>/<token>` secret by applying the standard
    /// outbound-auth flow to a throwaway probe request and reading the bearer
    /// value it writes.
    async fn resolve_webhook_token(&self) -> Result<String, ConnectorRuntimeError> {
        let mut probe = HttpRequest::new(HttpMethod::Post, "discord://webhook-token-probe");
        apply_outbound_auth_with_context(
            &DISCORD_WEBHOOK_AUTH_OUTBOUND_AUTH,
            &mut probe,
            &self.context,
        )
        .await?;

        probe
            .headers
            .get("Authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|token| !token.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ConnectorRuntimeError::invalid_response(
                    "discord webhook token unavailable after outbound auth resolution",
                )
            })
    }

    async fn request_post(
        &self,
        path: &str,
        body: JsonValue,
    ) -> Result<HttpResponse, ConnectorRuntimeError> {
        let url = format!("{}{}", self.endpoint.base_url.trim_end_matches('/'), path);

        let mut request = HttpRequest::new(HttpMethod::Post, url);
        apply_default_headers(&mut request.headers, &self.endpoint);
        request.headers.insert("Content-Type", "application/json");
        request.body = Some(serde_json::to_vec(&body)?);

        send_request_from_current(self.action_id, HttpMethod::Post, request).await
    }
}

/// `/api/webhooks/<id>/<token>` — the Discord webhook execution path shape. The
/// resolved secret already carries the `<id>/<token>` pair.
fn discord_webhook_path(token: &str) -> String {
    format!("/api/webhooks/{token}")
}

/// Compose the `{ content?, embeds }` Discord webhook body — only the fields the
/// caller actually set are serialized.
fn build_message_body(input: &DiscordSendMessageInput) -> JsonValue {
    let embeds: Vec<JsonValue> = input.embeds.iter().map(embed_to_json).collect();
    let mut map = JsonMap::new();
    if let Some(content) = &input.content {
        if !content.is_empty() {
            map.insert("content".to_string(), json!(content));
        }
    }
    map.insert("embeds".to_string(), JsonValue::Array(embeds));
    JsonValue::Object(map)
}

fn embed_to_json(embed: &DiscordEmbed) -> JsonValue {
    let mut map = JsonMap::new();
    if let Some(title) = &embed.title {
        map.insert("title".to_string(), json!(title));
    }
    if let Some(description) = &embed.description {
        map.insert("description".to_string(), json!(description));
    }
    if let Some(color) = embed.color {
        map.insert("color".to_string(), json!(color));
    }
    if let Some(author) = &embed.author_name {
        map.insert("author".to_string(), json!({ "name": author }));
    }
    JsonValue::Object(map)
}

fn send_message_output_from_response(
    response: &HttpResponse,
) -> Result<DiscordSendMessageOutput, ConnectorRuntimeError> {
    if !response.is_success() {
        let body = String::from_utf8_lossy(&response.body);
        let body = body.chars().take(240).collect::<String>();
        return Err(ConnectorRuntimeError::HttpStatus {
            status: response.status,
            body,
        });
    }

    // A plain webhook post returns 204 with no body; `wait=true` returns the
    // created message JSON. Surface the id only when a body is present.
    let message_id = if response.body.is_empty() {
        None
    } else {
        serde_json::from_slice::<JsonValue>(&response.body)
            .ok()
            .and_then(|value| {
                value
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string)
            })
    };

    Ok(DiscordSendMessageOutput {
        delivered: true,
        message_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_path_embeds_id_and_token() {
        assert_eq!(
            discord_webhook_path("123456789/abcXYZ-token"),
            "/api/webhooks/123456789/abcXYZ-token"
        );
    }

    #[test]
    fn body_composes_content_and_embeds() {
        let input = DiscordSendMessageInput {
            content: Some("hi".to_string()),
            embeds: vec![DiscordEmbed {
                title: Some("New Lead".to_string()),
                description: Some("Ada".to_string()),
                color: Some(0x00_FF_F2),
                author_name: Some("Lattice".to_string()),
            }],
        };
        let body = build_message_body(&input);
        assert_eq!(body["content"], json!("hi"));
        assert_eq!(body["embeds"][0]["title"], json!("New Lead"));
        assert_eq!(body["embeds"][0]["color"], json!(0x00_FF_F2));
        assert_eq!(body["embeds"][0]["author"]["name"], json!("Lattice"));
    }

    #[test]
    fn empty_content_is_omitted() {
        let input = DiscordSendMessageInput {
            content: Some(String::new()),
            embeds: vec![DiscordEmbed::default()],
        };
        let body = build_message_body(&input);
        assert!(body.get("content").is_none());
        assert!(body["embeds"].is_array());
    }

    #[test]
    fn no_content_204_is_delivered_without_message_id() {
        let response = HttpResponse {
            status: 204,
            headers: Default::default(),
            body: Vec::new(),
        };
        let out = send_message_output_from_response(&response).expect("delivered");
        assert!(out.delivered);
        assert_eq!(out.message_id, None);
    }

    #[test]
    fn wait_true_body_surfaces_message_id() {
        let response = HttpResponse {
            status: 200,
            headers: Default::default(),
            body: serde_json::to_vec(&json!({ "id": "998877" })).unwrap(),
        };
        let out = send_message_output_from_response(&response).expect("delivered");
        assert_eq!(out.message_id.as_deref(), Some("998877"));
    }

    #[test]
    fn non_success_maps_to_http_status() {
        let response = HttpResponse {
            status: 401,
            headers: Default::default(),
            body: b"{\"message\":\"Invalid Webhook Token\"}".to_vec(),
        };
        let err = send_message_output_from_response(&response).expect_err("must reject");
        match err {
            ConnectorRuntimeError::HttpStatus { status, body } => {
                assert_eq!(status, 401);
                assert!(body.contains("Invalid Webhook Token"), "got: {body}");
            }
            other => panic!("expected HttpStatus, got: {other}"),
        }
    }
}
