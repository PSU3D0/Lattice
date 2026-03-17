use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{GoogleSheetsCreateSheetInput, GoogleSheetsCreateSheetOutput};
use crate::ops::GoogleSheetsCreateSheet;

#[def_node(
    name = "GoogleSheetsCreateSheet",
    summary = "Add a new sheet tab to an existing spreadsheet",
    identifier = "connector.google.sheets.create_sheet",
    connector_ops(crate::ops::GoogleSheetsCreateSheet)
)]
pub async fn google_sheets_create_sheet(
    input: GoogleSheetsCreateSheetInput,
) -> NodeResult<GoogleSheetsCreateSheetOutput> {
    GoogleSheetsCreateSheet::invoke(&input)
        .await
        .map_err(|err| {
            NodeError::new(format!(
                "connector.google.sheets.create_sheet failed: {err}"
            ))
        })
}
