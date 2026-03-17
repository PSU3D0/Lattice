pub use connector_google_platform::sheets::GoogleSheetsValueInputOption;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

fn __default_header_row() -> u32 {
    1
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleSheetsAppendRowInput {
    pub spreadsheet_id: String,
    pub sheet: String,
    pub row: JsonValue,
    #[serde(default = "__default_header_row")]
    pub header_row: u32,
    pub value_input_option: Option<GoogleSheetsValueInputOption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleSheetsAppendRowOutput {
    pub spreadsheet_id: String,
    pub sheet: String,
    pub updated_range: String,
    pub row_index: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoogleSheetsUpsertAction {
    Inserted,
    Updated,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleSheetsUpsertRowInput {
    pub spreadsheet_id: String,
    pub sheet: String,
    pub match_on: Vec<String>,
    pub row: JsonValue,
    #[serde(default = "__default_header_row")]
    pub header_row: u32,
    pub value_input_option: Option<GoogleSheetsValueInputOption>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleSheetsUpsertRowOutput {
    pub action: GoogleSheetsUpsertAction,
    pub spreadsheet_id: String,
    pub sheet: String,
    pub updated_range: String,
    pub row_index: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleSheetsFindRowsInput {
    pub spreadsheet_id: String,
    pub sheet: String,
    pub filters: JsonValue,
    pub limit: Option<u32>,
    #[serde(default = "__default_header_row")]
    pub header_row: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleSheetsRowMatch {
    pub row_number: u32,
    pub values: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleSheetsFindRowsOutput {
    pub items: Vec<GoogleSheetsRowMatch>,
}
