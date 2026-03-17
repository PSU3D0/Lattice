use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{GoogleSheetsFindRowsInput, GoogleSheetsFindRowsOutput};
use crate::ops::GoogleSheetsFindRows;

#[def_node(
    name = "GoogleSheetsFindRows",
    summary = "Find semantic rows in a sheet by column filters",
    identifier = "connector.google.sheets.find_rows",
    connector_ops(crate::ops::GoogleSheetsFindRows)
)]
pub async fn google_sheets_find_rows(
    input: GoogleSheetsFindRowsInput,
) -> NodeResult<GoogleSheetsFindRowsOutput> {
    GoogleSheetsFindRows::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.google.sheets.find_rows failed: {err}")))
}
