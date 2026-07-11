//! Handwritten Notion API runtime: semantic database-page create over the
//! shared connectors-std transport (endpoint + auth resolved from the current
//! connector context, mirroring `connector_google_sheets`'s `SheetsApi`).
//!
//! Notion's "create a page" is a single `POST /v1/pages` whose JSON body sets
//! `parent.database_id` plus the page `properties`. Every request must carry a
//! pinned `Notion-Version` header, which this runtime always sets explicitly so
//! the op stays correct regardless of endpoint-profile default headers.

use capabilities::http::{HttpMethod, HttpRequest};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::generated::profiles::{NOTION_API_AUTH_OUTBOUND_AUTH, NOTION_DEFAULT_ENDPOINT_PROFILE};
use crate::generated::types::{NotionCreatePageInput, NotionCreatePageOutput};
use crate::runtime::errors::ConnectorRuntimeError;

/// Notion REST API origin. The versioned `/v1/...` path is appended per request;
/// the profile base URL is overridable via env/bindings for testing.
pub const NOTION_BASE_URL: &str = "https://api.notion.com";

/// Pinned Notion API version. Notion requires this header on every request and
/// evolves behavior by date; the connector pins one known-good version.
pub const NOTION_VERSION: &str = "2022-06-28";

pub const fn notion_pages_path() -> &'static str {
    "/v1/pages"
}

pub struct NotionApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl NotionApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&NOTION_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn create_page(
        &self,
        input: &NotionCreatePageInput,
    ) -> Result<NotionCreatePageOutput, ConnectorRuntimeError> {
        let mut body = json!({
            "parent": { "database_id": input.database_id },
            "properties": input.properties.clone(),
        });
        if let Some(icon) = &input.icon {
            body["icon"] = icon.clone();
        }

        let response = self
            .request_json(HttpMethod::Post, notion_pages_path(), Some(body))
            .await?;

        create_page_output_from_response(&response)
    }

    async fn request_json(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<JsonValue>,
    ) -> Result<JsonValue, ConnectorRuntimeError> {
        let url = format!("{}{}", self.endpoint.base_url.trim_end_matches('/'), path);

        let mut request = HttpRequest::new(method, url);
        apply_default_headers(&mut request.headers, &self.endpoint);
        request.headers.insert("Notion-Version", NOTION_VERSION);
        apply_outbound_auth_with_context(
            &NOTION_API_AUTH_OUTBOUND_AUTH,
            &mut request,
            &self.context,
        )
        .await?;

        if let Some(body) = body {
            request.headers.insert("Content-Type", "application/json");
            request.body = Some(serde_json::to_vec(&body)?);
        }

        let response = send_request_from_current(self.action_id, method, request).await?;
        decode_json_response_body(&response)
    }
}

#[derive(Deserialize)]
struct WirePage {
    id: String,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    object: Option<String>,
}

fn create_page_output_from_response(
    body: &JsonValue,
) -> Result<NotionCreatePageOutput, ConnectorRuntimeError> {
    let page: WirePage = serde_json::from_value(body.clone()).map_err(|err| {
        ConnectorRuntimeError::invalid_response(format!(
            "Notion create-page response did not match the expected shape: {err}"
        ))
    })?;

    Ok(NotionCreatePageOutput {
        id: page.id,
        url: page.url,
        object: page.object,
    })
}
