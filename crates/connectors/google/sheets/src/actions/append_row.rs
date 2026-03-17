use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{GoogleSheetsAppendRowInput, GoogleSheetsAppendRowOutput};
use crate::ops::GoogleSheetsAppendRow;

#[def_node(
    name = "GoogleSheetsAppendRow",
    summary = "Append one semantic row to a sheet",
    identifier = "connector.google.sheets.append_row",
    connector_ops(crate::ops::GoogleSheetsAppendRow)
)]
pub async fn google_sheets_append_row(
    input: GoogleSheetsAppendRowInput,
) -> NodeResult<GoogleSheetsAppendRowOutput> {
    GoogleSheetsAppendRow::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.google.sheets.append_row failed: {err}")))
}
