use anyhow::Result;
use capabilities::http::{HttpMethod, HttpRequest};
use connector_google_platform::sheets::{row_range, wide_read_range};
use connector_google_sheets::generated::profiles::{
    GOOGLE_SHEETS_DEFAULT_ENDPOINT_PROFILE, GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH,
};
use connector_google_sheets::{
    GoogleSheetsCreateSheetInput, GoogleSheetsCreateSpreadsheetInput, GoogleSheetsSheetSummary,
    GoogleSheetsUpsertAction, GoogleSheetsUpsertRowInput,
};
use connectors_std::endpoint::apply_default_headers;
use connectors_std::errors::ConnectorRuntimeError;
use connectors_std::http::append_query_pair;
use connectors_std::{
    apply_outbound_auth_with_context, current_connector_context, decode_json_response_body,
    resolve_endpoint_with_context, send_request_from_current,
};
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

const DEFAULT_SPREADSHEET_TITLE: &str = "Lattice CRM Smoke";
const DEFAULT_SHEET_TITLE: &str = "Leads";
const CRM_HEADERS: [&str; 3] = ["email", "name", "summary"];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleSubmissionInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spreadsheet_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spreadsheet_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sheet: Option<String>,
    pub email: String,
    pub name: String,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PreparedLeadInput {
    spreadsheet_id: Option<String>,
    spreadsheet_title: String,
    sheet: String,
    email: String,
    name: String,
    summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProvisionedLeadInput {
    spreadsheet_id: String,
    spreadsheet_url: Option<String>,
    spreadsheet_title: String,
    sheet: String,
    email: String,
    name: String,
    summary: String,
    created_spreadsheet: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PreparedUpsertLead {
    spreadsheet_id: String,
    spreadsheet_url: Option<String>,
    sheet: String,
    email: String,
    name: String,
    summary: String,
    created_spreadsheet: bool,
    created_sheet: bool,
    initialized_headers: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExampleFlowOutput {
    pub spreadsheet_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spreadsheet_url: Option<String>,
    pub sheet: String,
    pub created_spreadsheet: bool,
    pub created_sheet: bool,
    pub initialized_headers: bool,
    pub action: GoogleSheetsUpsertAction,
    pub updated_range: String,
    pub row_index: Option<u32>,
}

#[derive(Clone, Debug)]
struct SpreadsheetMetadata {
    spreadsheet_url: Option<String>,
    sheets: Vec<GoogleSheetsSheetSummary>,
}

#[def_node(
    trigger,
    name = "ExampleTrigger",
    summary = "Seed a semantic Google Sheets CRM request",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn example_trigger(input: ExampleSubmissionInput) -> NodeResult<ExampleSubmissionInput> {
    Ok(input)
}

#[def_node(
    name = "NormalizeSubmission",
    summary = "Resolve defaults for the self-bootstrapping CRM sheet flow",
    effects = "Pure",
    determinism = "Strict"
)]
async fn normalize_submission(input: ExampleSubmissionInput) -> NodeResult<PreparedLeadInput> {
    Ok(PreparedLeadInput {
        spreadsheet_id: input.spreadsheet_id,
        spreadsheet_title: input
            .spreadsheet_title
            .unwrap_or_else(|| DEFAULT_SPREADSHEET_TITLE.to_string()),
        sheet: input
            .sheet
            .unwrap_or_else(|| DEFAULT_SHEET_TITLE.to_string()),
        email: input.email,
        name: input.name,
        summary: input.summary,
    })
}

#[def_node(
    name = "EnsureSpreadsheet",
    summary = "Create a spreadsheet when no spreadsheet_id is supplied",
    identifier = "connector.google.sheets.ensure_spreadsheet",
    connector_ops(connector_google_sheets::ops::GoogleSheetsCreateSpreadsheet)
)]
async fn ensure_spreadsheet(input: PreparedLeadInput) -> NodeResult<ProvisionedLeadInput> {
    if let Some(spreadsheet_id) = input.spreadsheet_id {
        return Ok(ProvisionedLeadInput {
            spreadsheet_id,
            spreadsheet_url: None,
            spreadsheet_title: input.spreadsheet_title,
            sheet: input.sheet,
            email: input.email,
            name: input.name,
            summary: input.summary,
            created_spreadsheet: false,
        });
    }

    let created = connector_google_sheets::ops::GoogleSheetsCreateSpreadsheet::invoke(
        &GoogleSheetsCreateSpreadsheetInput {
            title: input.spreadsheet_title.clone(),
            locale: None,
            time_zone: None,
            initial_sheet_title: Some(input.sheet.clone()),
        },
    )
    .await
    .map_err(node_error)?;

    Ok(ProvisionedLeadInput {
        spreadsheet_id: created.spreadsheet_id,
        spreadsheet_url: created.spreadsheet_url,
        spreadsheet_title: input.spreadsheet_title,
        sheet: input.sheet,
        email: input.email,
        name: input.name,
        summary: input.summary,
        created_spreadsheet: true,
    })
}

#[def_node(
    name = "EnsureSheetHeaders",
    summary = "Ensure the target sheet and CRM headers exist before row upsert",
    identifier = "connector.google.sheets.ensure_sheet_headers",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        http_read(capabilities::http::HttpRead),
        http_write(capabilities::http::HttpWrite)
    ),
    connector_ops(connector_google_sheets::ops::GoogleSheetsCreateSheet)
)]
async fn ensure_sheet_headers(input: ProvisionedLeadInput) -> NodeResult<PreparedUpsertLead> {
    let mut spreadsheet_url = input.spreadsheet_url.clone();
    let mut created_sheet = false;

    if !input.created_spreadsheet {
        let metadata = fetch_spreadsheet_metadata(
            "connector.google.sheets.ensure_sheet_headers",
            &input.spreadsheet_id,
        )
        .await
        .map_err(node_error)?;
        spreadsheet_url = spreadsheet_url.or(metadata.spreadsheet_url);

        let sheet_exists = metadata
            .sheets
            .iter()
            .any(|sheet| sheet.title == input.sheet);
        if !sheet_exists {
            connector_google_sheets::ops::GoogleSheetsCreateSheet::invoke(
                &GoogleSheetsCreateSheetInput {
                    spreadsheet_id: input.spreadsheet_id.clone(),
                    title: input.sheet.clone(),
                    index: None,
                    row_count: Some(1000),
                    column_count: Some(26),
                },
            )
            .await
            .map_err(node_error)?;
            created_sheet = true;
        }
    }

    let initialized_headers = ensure_headers_exist(
        "connector.google.sheets.ensure_sheet_headers",
        &input.spreadsheet_id,
        &input.sheet,
        &CRM_HEADERS,
    )
    .await
    .map_err(node_error)?;

    Ok(PreparedUpsertLead {
        spreadsheet_id: input.spreadsheet_id,
        spreadsheet_url,
        sheet: input.sheet,
        email: input.email,
        name: input.name,
        summary: input.summary,
        created_spreadsheet: input.created_spreadsheet,
        created_sheet,
        initialized_headers,
    })
}

#[def_node(
    name = "UpsertLeadRow",
    summary = "Upsert the CRM lead row after spreadsheet bootstrapping",
    identifier = "connector.google.sheets.crm_upsert",
    connector_ops(connector_google_sheets::ops::GoogleSheetsUpsertRow)
)]
async fn upsert_lead_row(input: PreparedUpsertLead) -> NodeResult<ExampleFlowOutput> {
    let output =
        connector_google_sheets::ops::GoogleSheetsUpsertRow::invoke(&GoogleSheetsUpsertRowInput {
            spreadsheet_id: input.spreadsheet_id.clone(),
            sheet: input.sheet.clone(),
            match_on: vec!["email".to_string()],
            row: json!({
                "email": input.email,
                "name": input.name,
                "summary": input.summary,
            }),
            header_row: 1,
            value_input_option: None,
        })
        .await
        .map_err(node_error)?;

    Ok(ExampleFlowOutput {
        spreadsheet_id: input.spreadsheet_id,
        spreadsheet_url: input.spreadsheet_url,
        sheet: input.sheet,
        created_spreadsheet: input.created_spreadsheet,
        created_sheet: input.created_sheet,
        initialized_headers: input.initialized_headers,
        action: output.action,
        updated_range: output.updated_range,
        row_index: output.row_index,
    })
}

dag_macros::flow! {
    name: connector_google_sheets_local_flow,
    version: "0.1.0",
    profile: Dev,
    summary: "Connector-owned self-bootstrapping CRM flow example for connector_google_sheets";
    let trigger = node!(example_trigger);
    let normalize = node!(normalize_submission);
    let ensure_spreadsheet = node!(ensure_spreadsheet);
    let ensure_sheet_headers = node!(ensure_sheet_headers);
    let upsert = node!(upsert_lead_row);
    connect!(trigger -> normalize);
    connect!(normalize -> ensure_spreadsheet);
    connect!(ensure_spreadsheet -> ensure_sheet_headers);
    connect!(ensure_sheet_headers -> upsert);
    entrypoint!({
        trigger: "trigger",
        capture: "upsert",
        route_aliases: ["/google/sheets/local"],
        method: "POST",
        deadline_ms: 5_000,
    });
}

async fn fetch_spreadsheet_metadata(
    action: &'static str,
    spreadsheet_id: &str,
) -> Result<SpreadsheetMetadata, ConnectorRuntimeError> {
    let body = google_request_json(
        action,
        HttpMethod::Get,
        &format!("/v4/spreadsheets/{}", encode_path_segment(spreadsheet_id)),
        &[(
            "fields".to_string(),
            "spreadsheetId,spreadsheetUrl,sheets.properties(sheetId,title,index,gridProperties(rowCount,columnCount))"
                .to_string(),
        )],
        None,
    )
    .await?;

    let spreadsheet_url = body
        .get("spreadsheetUrl")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    let sheets = body
        .get("sheets")
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let properties = item.get("properties").ok_or_else(|| {
                        ConnectorRuntimeError::invalid_response(
                            "spreadsheet metadata sheet entry missing `properties`",
                        )
                    })?;
                    sheet_summary_from_properties(properties)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(SpreadsheetMetadata {
        spreadsheet_url,
        sheets,
    })
}

async fn ensure_headers_exist(
    action: &'static str,
    spreadsheet_id: &str,
    sheet: &str,
    required_headers: &[&str],
) -> Result<bool, ConnectorRuntimeError> {
    let read_range = wide_read_range(sheet, 1);
    let read_body = google_request_json(
        action,
        HttpMethod::Get,
        &values_path(spreadsheet_id, &read_range),
        &[],
        None,
    )
    .await?;

    let current_headers = current_headers_from_values(&read_body)?;
    if let Some(current_headers) = current_headers {
        if header_row_matches(&current_headers, required_headers) {
            return Ok(false);
        }
        return Err(ConnectorRuntimeError::invalid_response(format!(
            "sheet `{sheet}` already has incompatible headers {:?}; expected prefix {:?}",
            current_headers, required_headers
        )));
    }

    let header_range = row_range(sheet, 1, required_headers.len());
    let body = json!({
        "majorDimension": "ROWS",
        "values": [required_headers.iter().map(|value| JsonValue::String((*value).to_string())).collect::<Vec<_>>()],
    });
    google_request_json(
        action,
        HttpMethod::Put,
        &values_path(spreadsheet_id, &header_range),
        &[("valueInputOption".to_string(), "RAW".to_string())],
        Some(body),
    )
    .await?;

    Ok(true)
}

async fn google_request_json(
    action: &'static str,
    method: HttpMethod,
    path: &str,
    query: &[(String, String)],
    body: Option<JsonValue>,
) -> Result<JsonValue, ConnectorRuntimeError> {
    let context = current_connector_context(action).await?;
    let endpoint =
        resolve_endpoint_with_context(&GOOGLE_SHEETS_DEFAULT_ENDPOINT_PROFILE, &context).await?;
    let mut url = format!("{}{}", endpoint.base_url.trim_end_matches('/'), path);
    for (name, value) in query {
        append_query_pair(&mut url, name, value);
    }

    let mut request = HttpRequest::new(method, url);
    apply_default_headers(&mut request.headers, &endpoint);
    apply_outbound_auth_with_context(&GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH, &mut request, &context)
        .await?;

    if let Some(body) = body {
        request.headers.insert("Content-Type", "application/json");
        request.body = Some(serde_json::to_vec(&body)?);
    }

    let response = send_request_from_current(action, method, request).await?;
    decode_json_response_body(&response)
}

fn current_headers_from_values(
    body: &JsonValue,
) -> Result<Option<Vec<String>>, ConnectorRuntimeError> {
    let Some(values) = body.get("values") else {
        return Ok(None);
    };
    let rows = values.as_array().ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(
            "Google Sheets values response must contain an array field `values`",
        )
    })?;
    let Some(first_row) = rows.first() else {
        return Ok(None);
    };
    let first_row = first_row.as_array().ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(
            "Google Sheets values response header row must be an array",
        )
    })?;

    let headers = first_row
        .iter()
        .map(json_scalar_to_string)
        .collect::<Result<Vec<_>, _>>()?;

    if headers.iter().all(|value| value.trim().is_empty()) {
        Ok(None)
    } else {
        Ok(Some(headers))
    }
}

fn header_row_matches(current_headers: &[String], required_headers: &[&str]) -> bool {
    current_headers.len() >= required_headers.len()
        && required_headers
            .iter()
            .enumerate()
            .all(|(index, expected)| current_headers[index].trim() == *expected)
}

fn sheet_summary_from_properties(
    properties: &JsonValue,
) -> Result<GoogleSheetsSheetSummary, ConnectorRuntimeError> {
    Ok(GoogleSheetsSheetSummary {
        sheet_id: required_u32_pointer(properties, "/sheetId", "sheetId")?,
        title: required_string_pointer(properties, "/title", "title")?,
        index: optional_u32_pointer(properties, "/index")?,
        row_count: optional_u32_pointer(properties, "/gridProperties/rowCount")?,
        column_count: optional_u32_pointer(properties, "/gridProperties/columnCount")?,
    })
}

fn required_string_pointer(
    body: &JsonValue,
    pointer: &str,
    field_name: &str,
) -> Result<String, ConnectorRuntimeError> {
    body.pointer(pointer)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(format!(
                "Google Sheets response did not contain string field `{field_name}`"
            ))
        })
}

fn optional_u32_pointer(
    body: &JsonValue,
    pointer: &str,
) -> Result<Option<u32>, ConnectorRuntimeError> {
    body.pointer(pointer).map(json_u32).transpose()
}

fn required_u32_pointer(
    body: &JsonValue,
    pointer: &str,
    field_name: &str,
) -> Result<u32, ConnectorRuntimeError> {
    let value = body.pointer(pointer).ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(format!(
            "Google Sheets response did not contain numeric field `{field_name}`"
        ))
    })?;
    json_u32(value)
}

fn json_u32(value: &JsonValue) -> Result<u32, ConnectorRuntimeError> {
    let value = value.as_u64().ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(
            "Google Sheets response field must be an unsigned integer",
        )
    })?;
    u32::try_from(value).map_err(|_| {
        ConnectorRuntimeError::invalid_response(
            "Google Sheets response integer field exceeded supported u32 range",
        )
    })
}

fn json_scalar_to_string(value: &JsonValue) -> Result<String, ConnectorRuntimeError> {
    match value {
        JsonValue::Null => Ok(String::new()),
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(ConnectorRuntimeError::invalid_response(
            "header rows must contain only scalar-compatible values",
        )),
    }
}

fn values_path(spreadsheet_id: &str, range: &str) -> String {
    format!(
        "/v4/spreadsheets/{}/values/{}",
        encode_path_segment(spreadsheet_id),
        encode_path_segment(range)
    )
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::ResourceBag;
    use connector_google_sheets::runtime::transport::EnvConnectorRuntime;
    use host_inproc::FlowBundle;
    use kernel_exec::ExecutionResult;

    use super::*;

    const GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV: &str =
        "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL";
    const GOOGLE_WORKSPACE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(previous) => unsafe {
                    std::env::set_var(self.key, previous);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    fn http_resources() -> ResourceBag {
        let client = Arc::new(ReqwestHttpClient::default());
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
    }

    async fn execute_flow(
        bundle: FlowBundle,
        input: ExampleSubmissionInput,
    ) -> anyhow::Result<ExampleFlowOutput> {
        let entrypoint = bundle.entrypoints.first().expect("entrypoint");
        let payload = serde_json::to_value(&input)?;

        let result = bundle
            .executor()
            .with_resource_bag(http_resources())
            .run_once(
                &bundle.validated_ir,
                entrypoint.trigger_alias.as_str(),
                payload,
                entrypoint.capture_alias.as_str(),
                entrypoint.deadline,
            )
            .await?;

        let value = match result {
            ExecutionResult::Value(value) => value,
            ExecutionResult::Stream(_) => anyhow::bail!("expected a value response"),
            ExecutionResult::Halt { alias, .. } => {
                anyhow::bail!("expected a completed value response, flow halted at {alias}")
            }
        };

        Ok(serde_json::from_value(value)?)
    }

    #[test]
    fn example_flow_contains_self_bootstrap_nodes() {
        let ir = flow();
        let identifiers = ir
            .nodes
            .iter()
            .map(|node| node.identifier.as_str())
            .collect::<Vec<_>>();
        assert!(identifiers.contains(&"connector.google.sheets.ensure_spreadsheet"));
        assert!(identifiers.contains(&"connector.google.sheets.ensure_sheet_headers"));
        assert!(identifiers.contains(&"connector.google.sheets.crm_upsert"));
    }

    #[tokio::test]
    async fn normalize_defaults_sheet_and_spreadsheet_title() {
        let prepared = normalize_submission(ExampleSubmissionInput {
            spreadsheet_id: None,
            spreadsheet_title: None,
            sheet: None,
            email: "ada@example.test".to_string(),
            name: "Ada Lovelace".to_string(),
            summary: "normalized".to_string(),
        })
        .await
        .expect("normalize succeeds");

        assert_eq!(prepared.spreadsheet_title, DEFAULT_SPREADSHEET_TITLE);
        assert_eq!(prepared.sheet, DEFAULT_SHEET_TITLE);
    }

    #[tokio::test]
    async fn local_flow_runs_against_existing_sheet_mock() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = httpmock::MockServer::start();
        let _endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
        let _auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-local-demo-token");

        let input = ExampleSubmissionInput {
            spreadsheet_id: Some("demo-spreadsheet".to_string()),
            spreadsheet_title: Some("CRM Smoke".to_string()),
            sheet: Some("Leads".to_string()),
            email: "ada@example.test".to_string(),
            name: "Ada Lovelace".to_string(),
            summary: "from local flow test".to_string(),
        };
        let sheet = input.sheet.clone().expect("sheet");
        let spreadsheet_id = input.spreadsheet_id.clone().expect("spreadsheet_id");
        let read_range = wide_read_range(&sheet, 1);
        let append_range = connector_google_platform::sheets::append_table_range(&sheet, 1, 3);

        let metadata_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(format!("/v4/spreadsheets/{}", encode_path_segment(&spreadsheet_id)))
                .query_param(
                    "fields",
                    "spreadsheetId,spreadsheetUrl,sheets.properties(sheetId,title,index,gridProperties(rowCount,columnCount))",
                )
                .header("authorization", "Bearer google-local-demo-token")
                .header("accept", "application/json");
            then.status(200).json_body_obj(&json!({
                "spreadsheetId": spreadsheet_id,
                "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/demo-spreadsheet/edit",
                "sheets": [
                    {
                        "properties": {
                            "sheetId": 0,
                            "title": sheet,
                            "index": 0,
                            "gridProperties": {
                                "rowCount": 1000,
                                "columnCount": 26
                            }
                        }
                    }
                ]
            }));
        });

        let read_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(values_path(&spreadsheet_id, &read_range))
                .header("authorization", "Bearer google-local-demo-token")
                .header("accept", "application/json");
            then.status(200).json_body_obj(&json!({
                "range": read_range,
                "values": [["email", "name", "summary"]]
            }));
        });

        let append_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path(format!(
                    "{}:append",
                    values_path(&spreadsheet_id, &append_range)
                ))
                .header("authorization", "Bearer google-local-demo-token")
                .header("accept", "application/json")
                .header("content-type", "application/json")
                .query_param("insertDataOption", "INSERT_ROWS")
                .query_param("valueInputOption", "RAW")
                .json_body_obj(&json!({
                    "majorDimension": "ROWS",
                    "values": [["ada@example.test", "Ada Lovelace", "from local flow test"]]
                }));
            then.status(200).json_body_obj(&json!({
                "updates": {
                    "updatedRange": "'Leads'!A2:C2"
                }
            }));
        });

        let output = execute_flow(bundle(), input).await.expect("flow succeeds");

        metadata_mock.assert();
        read_mock.assert_hits(2);
        append_mock.assert();
        assert_eq!(output.action, GoogleSheetsUpsertAction::Inserted);
        assert_eq!(output.row_index, Some(2));
        assert!(!output.created_spreadsheet);
        assert!(!output.created_sheet);
        assert!(!output.initialized_headers);
        assert_eq!(
            output.spreadsheet_url,
            Some("https://docs.google.com/spreadsheets/d/demo-spreadsheet/edit".to_string())
        );
    }
}
