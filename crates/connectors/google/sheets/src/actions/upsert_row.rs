use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{GoogleSheetsUpsertRowInput, GoogleSheetsUpsertRowOutput};
use crate::ops::GoogleSheetsUpsertRow;

#[def_node(
    name = "GoogleSheetsUpsertRow",
    summary = "Update a matching row or append a new one",
    identifier = "connector.google.sheets.upsert_row",
    connector_ops(crate::ops::GoogleSheetsUpsertRow)
)]
pub async fn google_sheets_upsert_row(
    input: GoogleSheetsUpsertRowInput,
) -> NodeResult<GoogleSheetsUpsertRowOutput> {
    GoogleSheetsUpsertRow::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.google.sheets.upsert_row failed: {err}")))
}
