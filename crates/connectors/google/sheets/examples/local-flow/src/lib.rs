use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::ResourceBag;
use connector_google_platform::sheets::{append_table_range, wide_read_range};
use connector_google_sheets::runtime::transport::EnvConnectorRuntime;
use connector_google_sheets::{
    GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, GOOGLE_WORKSPACE_AUTH_ENV, GoogleSheetsUpsertRowInput,
    GoogleSheetsUpsertRowOutput,
};
use dag_core::NodeResult;
use dag_macros::{def_node, node};
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use kernel_exec::{ExecutionResult, NodeRegistry, NodeResolver, RegistryResolver};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const SPREADSHEET_ENV: &str = "LATTICE_EXAMPLE_GOOGLE_SHEETS_SPREADSHEET_ID";
pub const SHEET_ENV: &str = "LATTICE_EXAMPLE_GOOGLE_SHEETS_SHEET";

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleSubmissionInput {
    pub spreadsheet_id: String,
    pub sheet: String,
    pub email: String,
    pub name: String,
    pub summary: String,
}

#[def_node(
    trigger,
    name = "ExampleTrigger",
    summary = "Seed a semantic Google Sheets upsert request",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn example_trigger(input: ExampleSubmissionInput) -> NodeResult<ExampleSubmissionInput> {
    Ok(input)
}

#[def_node(
    name = "NormalizeSubmission",
    summary = "Normalize a submission into the canonical Google Sheets upsert connector input",
    effects = "Pure",
    determinism = "Strict"
)]
async fn normalize_submission(
    input: ExampleSubmissionInput,
) -> NodeResult<GoogleSheetsUpsertRowInput> {
    Ok(GoogleSheetsUpsertRowInput {
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
}

#[def_node(
    name = "ExampleCapture",
    summary = "Return Google Sheets connector output unchanged",
    effects = "Pure",
    determinism = "Strict"
)]
async fn example_capture(
    input: GoogleSheetsUpsertRowOutput,
) -> NodeResult<GoogleSheetsUpsertRowOutput> {
    Ok(input)
}

dag_macros::flow! {
    name: connector_google_sheets_local_flow,
    version: "0.1.0",
    profile: Dev,
    summary: "Connector-owned local flow example for the Google Sheets connector";
    let trigger = node!(example_trigger);
    let normalize = node!(normalize_submission);
    let upsert = node!(connector_google_sheets::google_sheets_upsert_row);
    let capture = node!(example_capture);
    connect!(trigger -> normalize);
    connect!(normalize -> upsert);
    connect!(upsert -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
    });
}

pub struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    pub fn set(key: &'static str, value: &str) -> Self {
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

pub struct LocalMockHandle {
    _server: httpmock::MockServer,
    _endpoint: EnvGuard,
    _auth: EnvGuard,
}

pub fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().expect("env lock")
}

pub fn example_bundle() -> FlowBundle {
    let validated_ir = validated_ir();
    let mut registry = NodeRegistry::new();
    example_trigger_register(&mut registry).expect("register example trigger");
    normalize_submission_register(&mut registry).expect("register normalize node");
    example_capture_register(&mut registry).expect("register capture node");
    connector_google_sheets::register_all(&mut registry).expect("register connector nodes");
    let registry = Arc::new(registry);
    let resolver: Arc<dyn NodeResolver> = Arc::new(RegistryResolver::new(Arc::clone(&registry)));
    let entrypoints = vec![FlowEntrypoint {
        trigger_alias: "trigger".to_string(),
        capture_alias: "capture".to_string(),
        route_path: Some("/google/sheets/local".to_string()),
        method: Some("POST".to_string()),
        deadline: Some(Duration::from_millis(5_000)),
        route_aliases: vec!["/google/sheets/local".to_string()],
    }];
    let node_contracts = validated_ir
        .flow()
        .nodes
        .iter()
        .map(|node| NodeContract {
            identifier: node.identifier.clone(),
            contract_hash: None,
            source: NodeSource::Local,
        })
        .collect();

    FlowBundle {
        validated_ir,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

pub fn http_resources() -> ResourceBag {
    let client = Arc::new(ReqwestHttpClient::default());
    ResourceBag::default()
        .with_http_read(Arc::clone(&client))
        .with_http_write(client)
        .with_connector_runtime(Arc::new(EnvConnectorRuntime))
}

pub fn example_input_from_env() -> ExampleSubmissionInput {
    ExampleSubmissionInput {
        spreadsheet_id: std::env::var(SPREADSHEET_ENV)
            .unwrap_or_else(|_| "demo-spreadsheet".to_string()),
        sheet: std::env::var(SHEET_ENV).unwrap_or_else(|_| "Leads".to_string()),
        email: "ada@example.test".to_string(),
        name: "Ada Lovelace".to_string(),
        summary: "connector local mock row".to_string(),
    }
}

pub fn maybe_start_mock_server() -> Option<LocalMockHandle> {
    if std::env::var(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV).is_ok() {
        println!(
            "Using configured upstream from {GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV}; no local mock server will be started."
        );
        return None;
    }

    let input = example_input_from_env();
    let server = httpmock::MockServer::start();
    let read_range = wide_read_range(&input.sheet, 1);
    let append_range = append_table_range(&input.sheet, 1, 3);

    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path(values_path(&input.spreadsheet_id, &read_range));
        then.status(200).json_body_obj(&json!({
            "range": read_range,
            "values": [["email", "name", "summary"]]
        }));
    });

    server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path(format!(
                "{}:append",
                values_path(&input.spreadsheet_id, &append_range)
            ))
            .query_param("insertDataOption", "INSERT_ROWS")
            .query_param("valueInputOption", "RAW");
        then.status(200).json_body_obj(&json!({
            "updates": {
                "updatedRange": format!("'{}'!A2:C2", input.sheet)
            }
        }));
    });

    let endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
    let auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-local-demo-token");
    println!(
        "No {GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV} override detected; started local mock Google Sheets at {}",
        server.base_url()
    );
    Some(LocalMockHandle {
        _server: server,
        _endpoint: endpoint,
        _auth: auth,
    })
}

pub async fn run_flow(input: ExampleSubmissionInput) -> Result<GoogleSheetsUpsertRowOutput> {
    let bundle = example_bundle();
    let entrypoint = bundle.entrypoints.first().expect("entrypoint");
    let payload = serde_json::to_value(&input).expect("serialize input");

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

fn values_path(spreadsheet_id: &str, range: &str) -> String {
    format!(
        "/v4/spreadsheets/{}/values/{}",
        utf8_percent_encode(spreadsheet_id, NON_ALPHANUMERIC),
        utf8_percent_encode(range, NON_ALPHANUMERIC)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_google_sheets::GOOGLE_SHEETS_UPSERT_ROW_IDENTIFIER;

    #[test]
    fn example_flow_contains_connector_node() {
        let ir = flow();
        let identifiers = ir
            .nodes
            .iter()
            .map(|node| node.identifier.as_str())
            .collect::<Vec<_>>();
        assert!(identifiers.contains(&GOOGLE_SHEETS_UPSERT_ROW_IDENTIFIER));
    }

    #[tokio::test]
    async fn local_flow_runs_against_mock_server() {
        let _env_lock = env_lock();
        let server = httpmock::MockServer::start();
        let _endpoint = EnvGuard::set(GOOGLE_SHEETS_DEFAULT_ENDPOINT_ENV, &server.base_url());
        let _auth = EnvGuard::set(GOOGLE_WORKSPACE_AUTH_ENV, "google-local-demo-token");

        let input = ExampleSubmissionInput {
            spreadsheet_id: "demo-spreadsheet".to_string(),
            sheet: "Leads".to_string(),
            email: "ada@example.test".to_string(),
            name: "Ada Lovelace".to_string(),
            summary: "from local flow test".to_string(),
        };
        let read_range = wide_read_range(&input.sheet, 1);
        let append_range = append_table_range(&input.sheet, 1, 3);

        let read_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path(values_path(&input.spreadsheet_id, &read_range))
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
                    values_path(&input.spreadsheet_id, &append_range)
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

        let output = run_flow(input).await.expect("flow succeeds");

        read_mock.assert();
        append_mock.assert();
        assert_eq!(
            output.action,
            connector_google_sheets::GoogleSheetsUpsertAction::Inserted
        );
        assert_eq!(output.row_index, Some(2));
    }
}
