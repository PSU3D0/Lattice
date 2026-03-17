use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_google_platform::sheets::{append_table_range, row_range, wide_read_range};
use connector_google_sheets::runtime::transport::EnvConnectorRuntime;
use connector_google_sheets::{
    GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, GOOGLE_WORKSPACE_AUTH_ENV, GoogleSheetsAppendRowInput,
    GoogleSheetsAppendRowOutput, GoogleSheetsCreateSheetInput, GoogleSheetsCreateSheetOutput,
    GoogleSheetsCreateSpreadsheetInput, GoogleSheetsCreateSpreadsheetOutput,
    GoogleSheetsFindRowsInput, GoogleSheetsRowMatch, GoogleSheetsSheetSummary,
    GoogleSheetsUpsertAction, GoogleSheetsUpsertRowInput, google_sheets_append_row,
    google_sheets_create_sheet, google_sheets_create_spreadsheet, google_sheets_find_rows,
};
use dag_core::{Determinism, Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::{GET, POST, PUT};
use httpmock::MockServer;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::json;

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

fn http_resources() -> Arc<ResourceBag> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://tests",
                "runtime_test",
                "connector.google.sheets.test",
                "connector.google.sheets",
            )),
    )
}

fn values_path(spreadsheet_id: &str, range: &str) -> String {
    format!(
        "/v4/spreadsheets/{}/values/{}",
        utf8_percent_encode(spreadsheet_id, NON_ALPHANUMERIC),
        utf8_percent_encode(range, NON_ALPHANUMERIC)
    )
}

fn batch_update_path(spreadsheet_id: &str) -> String {
    format!(
        "/v4/spreadsheets/{}:batchUpdate",
        utf8_percent_encode(spreadsheet_id, NON_ALPHANUMERIC)
    )
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MaybeSyncLeadInput {
    should_sync: bool,
    spreadsheet_id: String,
    sheet: String,
    email: String,
    name: String,
    summary: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct MaybeSyncLeadOutput {
    synced: bool,
    action: Option<GoogleSheetsUpsertAction>,
    row_index: Option<u32>,
}

#[def_node(
    name = "MaybeSyncLead",
    summary = "Custom node that reuses the semantic Google Sheets upsert connector operation",
    connector_ops(connector_google_sheets::ops::GoogleSheetsUpsertRow)
)]
async fn maybe_sync_lead(input: MaybeSyncLeadInput) -> NodeResult<MaybeSyncLeadOutput> {
    if !input.should_sync {
        return Ok(MaybeSyncLeadOutput {
            synced: false,
            action: None,
            row_index: None,
        });
    }

    let output =
        connector_google_sheets::ops::GoogleSheetsUpsertRow::invoke(&GoogleSheetsUpsertRowInput {
            spreadsheet_id: input.spreadsheet_id,
            sheet: input.sheet,
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
        .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(MaybeSyncLeadOutput {
        synced: true,
        action: Some(output.action),
        row_index: output.row_index,
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = maybe_sync_lead_node_spec();
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_READ)
    );
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.google.sheets.upsert_row")
    );

    let canonical = connector_google_sheets::google_sheets_upsert_row_node_spec();
    assert_eq!(canonical.effects, Effects::Effectful);
    assert_eq!(canonical.determinism, Determinism::BestEffort);
    assert!(
        canonical
            .connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.google.sheets.upsert_row")
    );
}

#[tokio::test]
async fn create_spreadsheet_returns_spreadsheet_metadata() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-test-token");

    let create_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v4/spreadsheets")
            .header("authorization", "Bearer google-test-token")
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .json_body_obj(&json!({
                "properties": {
                    "title": "Lead Intake",
                    "locale": "en_US",
                    "timeZone": "America/Chicago"
                },
                "sheets": [
                    {
                        "properties": {
                            "title": "Leads"
                        }
                    }
                ]
            }));
        then.status(200).json_body_obj(&json!({
            "spreadsheetId": "spreadsheet-123",
            "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/spreadsheet-123/edit",
            "properties": {
                "title": "Lead Intake"
            },
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": "Leads",
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

    let output = context::with_resources(http_resources(), async {
        google_sheets_create_spreadsheet(GoogleSheetsCreateSpreadsheetInput {
            title: "Lead Intake".to_string(),
            locale: Some("en_US".to_string()),
            time_zone: Some("America/Chicago".to_string()),
            initial_sheet_title: Some("Leads".to_string()),
        })
        .await
        .expect("create spreadsheet succeeds")
    })
    .await;

    create_mock.assert();
    assert_eq!(
        output,
        GoogleSheetsCreateSpreadsheetOutput {
            spreadsheet_id: "spreadsheet-123".to_string(),
            spreadsheet_url: Some(
                "https://docs.google.com/spreadsheets/d/spreadsheet-123/edit".to_string()
            ),
            title: "Lead Intake".to_string(),
            sheets: vec![GoogleSheetsSheetSummary {
                sheet_id: 0,
                title: "Leads".to_string(),
                index: Some(0),
                row_count: Some(1000),
                column_count: Some(26),
            }],
        }
    );
}

#[tokio::test]
async fn create_sheet_adds_sheet_via_batch_update() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-test-token");

    let spreadsheet_id = "spreadsheet-123";
    let create_mock = server.mock(|when, then| {
        when.method(POST)
            .path(batch_update_path(spreadsheet_id))
            .header("authorization", "Bearer google-test-token")
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .json_body_obj(&json!({
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": "Archive",
                                "index": 1,
                                "gridProperties": {
                                    "rowCount": 200,
                                    "columnCount": 8
                                }
                            }
                        }
                    }
                ]
            }));
        then.status(200).json_body_obj(&json!({
            "replies": [
                {
                    "addSheet": {
                        "properties": {
                            "sheetId": 77,
                            "title": "Archive",
                            "index": 1,
                            "gridProperties": {
                                "rowCount": 200,
                                "columnCount": 8
                            }
                        }
                    }
                }
            ]
        }));
    });

    let output = context::with_resources(http_resources(), async {
        google_sheets_create_sheet(GoogleSheetsCreateSheetInput {
            spreadsheet_id: spreadsheet_id.to_string(),
            title: "Archive".to_string(),
            index: Some(1),
            row_count: Some(200),
            column_count: Some(8),
        })
        .await
        .expect("create sheet succeeds")
    })
    .await;

    create_mock.assert();
    assert_eq!(
        output,
        GoogleSheetsCreateSheetOutput {
            spreadsheet_id: spreadsheet_id.to_string(),
            sheet: GoogleSheetsSheetSummary {
                sheet_id: 77,
                title: "Archive".to_string(),
                index: Some(1),
                row_count: Some(200),
                column_count: Some(8),
            },
        }
    );
}

#[tokio::test]
async fn append_row_executes_against_sheet_headers() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-test-token");

    let spreadsheet_id = "demo-spreadsheet";
    let sheet = "Leads";
    let read_range = wide_read_range(sheet, 1);
    let append_range = append_table_range(sheet, 1, 3);

    let read_mock = server.mock(|when, then| {
        when.method(GET)
            .path(values_path(spreadsheet_id, &read_range))
            .header("authorization", "Bearer google-test-token")
            .header("accept", "application/json");
        then.status(200).json_body_obj(&json!({
            "range": read_range,
            "values": [["email", "name", "summary"]]
        }));
    });

    let append_mock = server.mock(|when, then| {
        when.method(POST)
            .path(format!(
                "{}:append",
                values_path(spreadsheet_id, &append_range)
            ))
            .header("authorization", "Bearer google-test-token")
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .query_param("insertDataOption", "INSERT_ROWS")
            .query_param("valueInputOption", "RAW")
            .json_body_obj(&json!({
                "majorDimension": "ROWS",
                "values": [["a@example.test", "Ada", "new lead"]]
            }));
        then.status(200).json_body_obj(&json!({
            "updates": {
                "updatedRange": "'Leads'!A2:C2"
            }
        }));
    });

    let output = context::with_resources(http_resources(), async {
        google_sheets_append_row(GoogleSheetsAppendRowInput {
            spreadsheet_id: spreadsheet_id.to_string(),
            sheet: sheet.to_string(),
            row: json!({
                "email": "a@example.test",
                "name": "Ada",
                "summary": "new lead"
            }),
            header_row: 1,
            value_input_option: None,
        })
        .await
        .expect("append row succeeds")
    })
    .await;

    read_mock.assert();
    append_mock.assert();
    assert_eq!(
        output,
        GoogleSheetsAppendRowOutput {
            spreadsheet_id: spreadsheet_id.to_string(),
            sheet: sheet.to_string(),
            updated_range: "'Leads'!A2:C2".to_string(),
            row_index: Some(2),
        }
    );
}

#[tokio::test]
async fn find_rows_returns_matching_semantic_rows() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-test-token");

    let spreadsheet_id = "demo-spreadsheet";
    let sheet = "Leads";
    let read_range = wide_read_range(sheet, 1);

    let read_mock = server.mock(|when, then| {
        when.method(GET)
            .path(values_path(spreadsheet_id, &read_range))
            .header("authorization", "Bearer google-test-token")
            .header("accept", "application/json");
        then.status(200).json_body_obj(&json!({
            "range": read_range,
            "values": [
                ["email", "name", "summary"],
                ["a@example.test", "Ada", "first"],
                ["b@example.test", "Grace", "second"]
            ]
        }));
    });

    let output = context::with_resources(http_resources(), async {
        google_sheets_find_rows(GoogleSheetsFindRowsInput {
            spreadsheet_id: spreadsheet_id.to_string(),
            sheet: sheet.to_string(),
            filters: json!({ "email": "a@example.test" }),
            limit: None,
            header_row: 1,
        })
        .await
        .expect("find rows succeeds")
    })
    .await;

    read_mock.assert();
    assert_eq!(
        output.items,
        vec![GoogleSheetsRowMatch {
            row_number: 2,
            values: json!({
                "email": "a@example.test",
                "name": "Ada",
                "summary": "first"
            }),
        }]
    );
}

#[tokio::test]
async fn custom_node_reuses_semantic_upsert_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-test-token");

    let spreadsheet_id = "demo-spreadsheet";
    let sheet = "Leads";
    let read_range = wide_read_range(sheet, 1);
    let update_range = row_range(sheet, 2, 3);

    let read_mock = server.mock(|when, then| {
        when.method(GET)
            .path(values_path(spreadsheet_id, &read_range))
            .header("authorization", "Bearer google-test-token")
            .header("accept", "application/json");
        then.status(200).json_body_obj(&json!({
            "range": read_range,
            "values": [
                ["email", "name", "summary"],
                ["a@example.test", "Ada", "old summary"]
            ]
        }));
    });

    let update_mock = server.mock(|when, then| {
        when.method(PUT)
            .path(values_path(spreadsheet_id, &update_range))
            .header("authorization", "Bearer google-test-token")
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .query_param("valueInputOption", "RAW")
            .json_body_obj(&json!({
                "majorDimension": "ROWS",
                "values": [["a@example.test", "Ada Lovelace", "fresh summary"]]
            }));
        then.status(200).json_body_obj(&json!({
            "updatedRange": "'Leads'!A2:C2"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        maybe_sync_lead(MaybeSyncLeadInput {
            should_sync: true,
            spreadsheet_id: spreadsheet_id.to_string(),
            sheet: sheet.to_string(),
            email: "a@example.test".to_string(),
            name: "Ada Lovelace".to_string(),
            summary: "fresh summary".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    read_mock.assert();
    update_mock.assert();
    assert_eq!(
        output,
        MaybeSyncLeadOutput {
            synced: true,
            action: Some(GoogleSheetsUpsertAction::Updated),
            row_index: Some(2),
        }
    );
}
