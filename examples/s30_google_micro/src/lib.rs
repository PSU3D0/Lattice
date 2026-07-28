//! S30 is the smallest brokered Google effect flow: it creates a disposable
//! spreadsheet with one header, appends one data row using the returned ID,
//! sends one plain-text email containing the returned URL, and captures all
//! durable provider identifiers. It uses no LLM, artifacts, KV, or extraction.

use connector_google_gmail::GoogleGmailSendMessageInput;
use connector_google_gmail::ops::GoogleGmailSendMessage;
use connector_google_sheets::ops::{GoogleSheetsAppendRow, GoogleSheetsCreateSpreadsheet};
use connector_google_sheets::{GoogleSheetsAppendRowInput, GoogleSheetsCreateSpreadsheetInput};
use dag_core::{BrokerAuthority, BrokerOperationBudget, NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

const SHEET: &str = "Micro";
const HEADER: &str = "note";
const AGGREGATE: &str = "google-workspace";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GoogleMicroInput {
    pub title: String,
    pub recipient: String,
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreatedSpreadsheet {
    pub spreadsheet_id: String,
    pub spreadsheet_url: String,
    pub recipient: String,
    pub note: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotifiedSpreadsheet {
    pub spreadsheet_id: String,
    pub spreadsheet_url: String,
    pub message_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GoogleMicroResult {
    pub spreadsheet_id: String,
    pub spreadsheet_url: String,
    pub message_id: String,
    pub appended: bool,
}

fn node_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::new(error.to_string())
}

#[def_node(
    trigger,
    name = "GoogleMicroTrigger",
    summary = "Accept a spreadsheet title, email recipient, and note",
    effects = "Pure",
    determinism = "Strict"
)]
async fn trigger(input: GoogleMicroInput) -> NodeResult<GoogleMicroInput> {
    Ok(input)
}

#[def_node(
    name = "CreateSpreadsheet",
    identifier = "connector.google.sheets.google_micro_create",
    summary = "Create a disposable spreadsheet with the note header",
    connector_ops(GoogleSheetsCreateSpreadsheet)
)]
async fn create(input: GoogleMicroInput) -> NodeResult<CreatedSpreadsheet> {
    let created = GoogleSheetsCreateSpreadsheet::invoke(&GoogleSheetsCreateSpreadsheetInput {
        title: input.title,
        locale: None,
        time_zone: None,
        initial_sheet_title: Some(SHEET.to_string()),
        header_row: vec![HEADER.to_string()],
    })
    .await
    .map_err(node_error)?;
    Ok(CreatedSpreadsheet {
        spreadsheet_id: created.spreadsheet_id,
        spreadsheet_url: created
            .spreadsheet_url
            .ok_or_else(|| NodeError::new("Google did not return spreadsheetUrl"))?,
        recipient: input.recipient,
        note: input.note,
    })
}

#[def_node(
    name = "AppendRow",
    identifier = "connector.google.sheets.google_micro_append",
    summary = "Append the note to the newly created spreadsheet",
    connector_ops(GoogleSheetsAppendRow)
)]
async fn append(created: CreatedSpreadsheet) -> NodeResult<CreatedSpreadsheet> {
    GoogleSheetsAppendRow::invoke(&GoogleSheetsAppendRowInput {
        spreadsheet_id: created.spreadsheet_id.clone(),
        sheet: SHEET.to_string(),
        row: json!({ (HEADER): created.note }),
        header_row: 1,
        value_input_option: None,
    })
    .await
    .map_err(node_error)?;
    Ok(created)
}

#[def_node(
    name = "Notify",
    identifier = "connector.google.gmail.google_micro_notify",
    summary = "Email the recipient a link to the disposable spreadsheet",
    connector_ops(GoogleGmailSendMessage)
)]
async fn notify(created: CreatedSpreadsheet) -> NodeResult<NotifiedSpreadsheet> {
    let sent = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
        to: created.recipient,
        cc: None,
        bcc: None,
        subject: "Google micro flow complete".to_string(),
        text_body: format!("Your disposable spreadsheet is {}", created.spreadsheet_url),
    })
    .await
    .map_err(node_error)?;
    Ok(NotifiedSpreadsheet {
        spreadsheet_id: created.spreadsheet_id,
        spreadsheet_url: created.spreadsheet_url,
        message_id: sent.id,
    })
}

#[def_node(
    name = "Capture",
    summary = "Return durable Google identifiers for independent verification",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(notified: NotifiedSpreadsheet) -> NodeResult<GoogleMicroResult> {
    Ok(GoogleMicroResult {
        spreadsheet_id: notified.spreadsheet_id,
        spreadsheet_url: notified.spreadsheet_url,
        message_id: notified.message_id,
        appended: true,
    })
}

fn broker_authority(contract_id: &str, slot: &str) -> BrokerAuthority {
    BrokerAuthority::new(
        vec![BrokerOperationBudget {
            contract_id: contract_id.to_string(),
            semantic_effect_slots: vec![slot.to_string()],
            max_logical_calls: 1,
            max_dispatch_attempts_per_call: 1,
            connection_aggregate_key: Some(AGGREGATE.to_string()),
        }],
        Some(3),
        std::collections::BTreeMap::from([(AGGREGATE.to_string(), 3)]),
    )
    .expect("valid Google micro broker authority")
}

dag_macros::flow! {
    name: s30_google_micro_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Minimal brokered Google create spreadsheet, append row, and send email proof";
    let trigger = node!(trigger);
    let create = node!(create);
    let append = node!(append);
    let notify = node!(notify);
    let capture = node!(capture);
    broker_authority!(create, broker_authority("connector.google.sheets.create_spreadsheet@1", "create_spreadsheet"));
    broker_authority!(append, broker_authority("connector.google.sheets.append_row@1", "append_row"));
    broker_authority!(notify, broker_authority("connector.google.gmail.send_message@1", "send_message"));
    connect!(trigger -> create);
    connect!(create -> append);
    connect!(append -> notify);
    connect!(notify -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/google-micro"],
        method: "POST",
    });
}

pub fn canonical_flow_ir() -> broker_core::canonical::CanonicalJson {
    let bytes = serde_json::to_vec(&flow()).expect("serialize Google micro Flow IR");
    broker_core::canonical::canonicalize_bounded(&bytes, 1024 * 1024)
        .expect("canonical Google micro Flow IR")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_nodes_declare_exact_operations_and_consistent_budgets() {
        let ir = flow();
        let expected = [
            (
                "create",
                "connector.google.sheets.create_spreadsheet",
                "connector.google.sheets.create_spreadsheet@1",
                "create_spreadsheet",
            ),
            (
                "append",
                "connector.google.sheets.append_row",
                "connector.google.sheets.append_row@1",
                "append_row",
            ),
            (
                "notify",
                "connector.google.gmail.send_message",
                "connector.google.gmail.send_message@1",
                "send_message",
            ),
        ];
        for (alias, operation_id, contract_id, slot) in expected {
            let node = ir.node(alias).expect("broker node present");
            assert_eq!(node.connector_ops.len(), 1);
            assert_eq!(node.connector_ops[0].operation_id, operation_id);
            let authority = node.broker_authority.as_ref().expect("authority present");
            assert_eq!(authority.operation_budgets().len(), 1);
            let budget = &authority.operation_budgets()[0];
            assert_eq!(budget.contract_id, contract_id);
            assert_eq!(budget.semantic_effect_slots, [slot]);
            assert_eq!(budget.max_logical_calls, 1);
            assert_eq!(authority.flow_aggregate_max_logical_calls(), Some(3));
            assert_eq!(
                authority.connection_aggregate_max_logical_calls()[AGGREGATE],
                3
            );
        }
    }

    #[test]
    fn broker_authority_validates() {
        flow().validate_broker_authority().expect("valid authority");
    }

    #[test]
    fn canonical_ir_round_trips_byte_for_byte() {
        let canonical = canonical_flow_ir();
        let checked =
            broker_core::canonical::canonicalize_bounded(canonical.as_bytes(), 1024 * 1024)
                .expect("canonical bytes validate");
        assert_eq!(checked.as_bytes(), canonical.as_bytes());
    }
}
