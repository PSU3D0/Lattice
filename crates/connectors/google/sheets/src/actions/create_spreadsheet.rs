use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{
    GoogleSheetsCreateSpreadsheetInput, GoogleSheetsCreateSpreadsheetOutput,
};
use crate::ops::GoogleSheetsCreateSpreadsheet;

#[def_node(
    name = "GoogleSheetsCreateSpreadsheet",
    summary = "Create a spreadsheet with optional initial sheet metadata",
    identifier = "connector.google.sheets.create_spreadsheet",
    connector_ops(crate::ops::GoogleSheetsCreateSpreadsheet)
)]
pub async fn google_sheets_create_spreadsheet(
    input: GoogleSheetsCreateSpreadsheetInput,
) -> NodeResult<GoogleSheetsCreateSpreadsheetOutput> {
    GoogleSheetsCreateSpreadsheet::invoke(&input)
        .await
        .map_err(|err| {
            NodeError::new(format!(
                "connector.google.sheets.create_spreadsheet failed: {err}"
            ))
        })
}
