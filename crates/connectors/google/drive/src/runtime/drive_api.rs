//! Handwritten Google Drive API runtime: semantic file search over the shared
//! connectors-std transport (endpoint + auth resolved from the current
//! connector context, mirroring `connector_google_sheets`'s `SheetsApi`).

use capabilities::http::{HttpMethod, HttpRequest};
use connector_google_platform::drive::{GoogleDriveFilesListQuery, drive_files_path};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::http::append_query_pair;
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::generated::profiles::{
    GOOGLE_DRIVE_DEFAULT_ENDPOINT_PROFILE, GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH,
};
use crate::generated::types::{
    GoogleDriveFileHit, GoogleDrivePermissionSummary, GoogleDriveSearchFilesInput,
    GoogleDriveSearchFilesOutput,
};
use crate::runtime::errors::ConnectorRuntimeError;

/// Fixed projection for search results: identity + sharing posture per file.
/// Permissions are only populated by the API when explicitly requested via
/// `fields`, so the op always asks for them.
const SEARCH_FIELDS: &str = "nextPageToken,files(id,name,mimeType,shared,webViewLink,permissions(id,type,role,emailAddress))";

pub struct DriveApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl DriveApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&GOOGLE_DRIVE_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn search_files(
        &self,
        input: &GoogleDriveSearchFilesInput,
    ) -> Result<GoogleDriveSearchFilesOutput, ConnectorRuntimeError> {
        let query = GoogleDriveFilesListQuery {
            q: Some(input.query.clone()),
            fields: Some(SEARCH_FIELDS.to_string()),
            page_size: input.page_size,
            ..GoogleDriveFilesListQuery::default()
        };

        let response = self
            .request_json(HttpMethod::Get, drive_files_path(), &query.to_query_pairs())
            .await?;

        search_output_from_response(response)
    }

    async fn request_json(
        &self,
        method: HttpMethod,
        path: &str,
        query: &[(String, String)],
    ) -> Result<JsonValue, ConnectorRuntimeError> {
        let mut url = format!("{}{}", self.endpoint.base_url.trim_end_matches('/'), path);
        for (name, value) in query {
            append_query_pair(&mut url, name, value);
        }

        let mut request = HttpRequest::new(method, url);
        apply_default_headers(&mut request.headers, &self.endpoint);
        apply_outbound_auth_with_context(
            &GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH,
            &mut request,
            &self.context,
        )
        .await?;

        let response = send_request_from_current(self.action_id, method, request).await?;
        decode_json_response_body(&response)
    }
}

// ---- Wire shapes (Drive `files.list` response, camelCase) -----------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireFilesList {
    #[serde(default)]
    files: Vec<WireFile>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireFile {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    shared: Option<bool>,
    #[serde(default)]
    web_view_link: Option<String>,
    #[serde(default)]
    permissions: Vec<WirePermission>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WirePermission {
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "type")]
    grantee_type: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    email_address: Option<String>,
}

fn search_output_from_response(
    body: JsonValue,
) -> Result<GoogleDriveSearchFilesOutput, ConnectorRuntimeError> {
    let wire: WireFilesList = serde_json::from_value(body).map_err(|err| {
        ConnectorRuntimeError::invalid_response(format!(
            "Google Drive files.list response did not match the expected shape: {err}"
        ))
    })?;

    Ok(GoogleDriveSearchFilesOutput {
        items: wire
            .files
            .into_iter()
            .map(|file| GoogleDriveFileHit {
                id: file.id,
                name: file.name,
                mime_type: file.mime_type,
                shared: file.shared,
                web_view_link: file.web_view_link,
                permissions: file
                    .permissions
                    .into_iter()
                    .map(|permission| GoogleDrivePermissionSummary {
                        permission_id: permission.id,
                        grantee_type: permission.grantee_type,
                        role: permission.role,
                        email_address: permission.email_address,
                    })
                    .collect(),
            })
            .collect(),
        next_page_token: wire.next_page_token,
    })
}
