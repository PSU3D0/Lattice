use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

pub const GOOGLE_SHEETS_BASE_URL: &str = "https://sheets.googleapis.com";
pub const WIDE_READ_END_COLUMN: &str = "ZZZ";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoogleSheetsValueInputOption {
    #[default]
    Raw,
    UserEntered,
}

impl GoogleSheetsValueInputOption {
    pub const fn as_google_api_value(self) -> &'static str {
        match self {
            GoogleSheetsValueInputOption::Raw => "RAW",
            GoogleSheetsValueInputOption::UserEntered => "USER_ENTERED",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SheetTableRow {
    pub row_number: u32,
    pub values: Map<String, JsonValue>,
    pub ordered_cells: Vec<JsonValue>,
}

pub fn quote_sheet_name(sheet: &str) -> String {
    format!("'{}'", sheet.replace('\'', "''"))
}

pub fn column_letters(mut column_number: usize) -> String {
    assert!(
        column_number > 0,
        "column_number must be 1-based and non-zero"
    );
    let mut letters = String::new();
    while column_number > 0 {
        let remainder = (column_number - 1) % 26;
        letters.insert(0, (b'A' + remainder as u8) as char);
        column_number = (column_number - 1) / 26;
    }
    letters
}

pub fn wide_read_range(sheet: &str, header_row: u32) -> String {
    format!(
        "{}!A{}:{}",
        quote_sheet_name(sheet),
        header_row,
        WIDE_READ_END_COLUMN
    )
}

pub fn row_range(sheet: &str, row_number: u32, column_count: usize) -> String {
    format!(
        "{}!A{}:{}{}",
        quote_sheet_name(sheet),
        row_number,
        column_letters(column_count.max(1)),
        row_number
    )
}

pub fn append_table_range(sheet: &str, header_row: u32, column_count: usize) -> String {
    format!(
        "{}!A{}:{}",
        quote_sheet_name(sheet),
        header_row,
        column_letters(column_count.max(1))
    )
}

pub fn expect_object<'a>(
    value: &'a JsonValue,
    context: &str,
) -> Result<&'a Map<String, JsonValue>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{context} must be a JSON object"))
}

pub fn ordered_row_values(
    headers: &[String],
    row: &Map<String, JsonValue>,
) -> Result<Vec<JsonValue>, String> {
    ensure_known_keys(headers, row.keys().map(String::as_str), "row")?;
    Ok(headers
        .iter()
        .map(|header| {
            row.get(header)
                .map(value_to_cell)
                .unwrap_or_else(blank_cell)
        })
        .collect())
}

pub fn merged_row_values(
    headers: &[String],
    existing: &Map<String, JsonValue>,
    patch: &Map<String, JsonValue>,
) -> Result<Vec<JsonValue>, String> {
    ensure_known_keys(headers, patch.keys().map(String::as_str), "row")?;
    Ok(headers
        .iter()
        .map(|header| {
            patch
                .get(header)
                .map(value_to_cell)
                .or_else(|| existing.get(header).map(value_to_cell))
                .unwrap_or_else(blank_cell)
        })
        .collect())
}

pub fn parse_sheet_table(
    values: &[Vec<JsonValue>],
    header_row: u32,
) -> Result<(Vec<String>, Vec<SheetTableRow>), String> {
    let Some(header_cells) = values.first() else {
        return Err(format!(
            "sheet did not return a header row at row {header_row}; share the sheet and ensure the header row exists"
        ));
    };

    let headers = header_cells
        .iter()
        .enumerate()
        .map(|(index, value)| scalar_to_string(value, &format!("header cell {}", index + 1)))
        .collect::<Result<Vec<_>, _>>()?;

    if headers.is_empty() {
        return Err("header row must contain at least one column".to_string());
    }

    for (index, header) in headers.iter().enumerate() {
        if header.trim().is_empty() {
            return Err(format!("header column {} must not be blank", index + 1));
        }
        if headers[..index].iter().any(|prior| prior == header) {
            return Err(format!("header row contains duplicate column `{header}`"));
        }
    }

    let mut rows = Vec::new();
    for (offset, cells) in values.iter().enumerate().skip(1) {
        let ordered_cells = headers
            .iter()
            .enumerate()
            .map(|(index, _)| cells.get(index).cloned().unwrap_or_else(blank_cell))
            .collect::<Vec<_>>();

        if ordered_cells.iter().all(is_blank_cell) {
            continue;
        }

        let mut mapped = Map::new();
        for (header, cell) in headers.iter().zip(ordered_cells.iter()) {
            mapped.insert(header.clone(), cell.clone());
        }

        rows.push(SheetTableRow {
            row_number: header_row + offset as u32,
            values: mapped,
            ordered_cells,
        });
    }

    Ok((headers, rows))
}

pub fn row_matches_filters(
    row: &Map<String, JsonValue>,
    filters: &Map<String, JsonValue>,
) -> Result<bool, String> {
    for (column, expected) in filters {
        let Some(actual) = row.get(column) else {
            return Err(format!("filter references unknown column `{column}`"));
        };
        if comparable_scalar(actual)? != comparable_scalar(expected)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn last_row_from_a1_range(range: &str) -> Option<u32> {
    let mut digits = String::new();
    for ch in range.chars().rev() {
        if ch.is_ascii_digit() {
            digits.insert(0, ch);
        } else if !digits.is_empty() {
            break;
        }
    }

    if digits.is_empty() {
        None
    } else {
        digits.parse::<u32>().ok()
    }
}

fn ensure_known_keys<'a>(
    headers: &[String],
    keys: impl Iterator<Item = &'a str>,
    context: &str,
) -> Result<(), String> {
    for key in keys {
        if !headers.iter().any(|header| header == key) {
            return Err(format!("{context} references unknown column `{key}`"));
        }
    }
    Ok(())
}

fn comparable_scalar(value: &JsonValue) -> Result<String, String> {
    match value {
        JsonValue::Null => Ok(String::new()),
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(
            "filters and row values must be scalar-compatible when used for row matching"
                .to_string(),
        ),
    }
}

fn scalar_to_string(value: &JsonValue, context: &str) -> Result<String, String> {
    match value {
        JsonValue::Null => Ok(String::new()),
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Err(format!("{context} must be scalar-compatible"))
        }
    }
}

fn value_to_cell(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Null => blank_cell(),
        JsonValue::String(value) => JsonValue::String(value.clone()),
        JsonValue::Bool(value) => JsonValue::Bool(*value),
        JsonValue::Number(value) => JsonValue::Number(value.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            JsonValue::String(serde_json::to_string(value).expect("serialize JSON cell value"))
        }
    }
}

fn blank_cell() -> JsonValue {
    JsonValue::String(String::new())
}

fn is_blank_cell(value: &JsonValue) -> bool {
    match value {
        JsonValue::Null => true,
        JsonValue::String(value) => value.trim().is_empty(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn column_letters_expand_past_z() {
        assert_eq!(column_letters(1), "A");
        assert_eq!(column_letters(26), "Z");
        assert_eq!(column_letters(27), "AA");
        assert_eq!(column_letters(52), "AZ");
        assert_eq!(column_letters(53), "BA");
    }

    #[test]
    fn quoted_sheet_name_escapes_single_quotes() {
        assert_eq!(quote_sheet_name("Leads"), "'Leads'");
        assert_eq!(quote_sheet_name("Team's Leads"), "'Team''s Leads'");
    }

    #[test]
    fn parse_sheet_table_skips_blank_rows_and_tracks_row_numbers() {
        let values = vec![
            vec![json!("email"), json!("name")],
            vec![json!("a@example.test"), json!("Ada")],
            vec![json!(""), json!("")],
            vec![json!("b@example.test")],
        ];

        let (headers, rows) = parse_sheet_table(&values, 2).expect("table parses");
        assert_eq!(headers, vec!["email", "name"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].row_number, 3);
        assert_eq!(rows[1].row_number, 5);
        assert_eq!(rows[1].values["name"], json!(""));
    }

    #[test]
    fn merged_row_values_preserves_existing_cells_when_patch_is_partial() {
        let headers = vec!["email".to_string(), "name".to_string(), "score".to_string()];
        let existing = serde_json::from_value::<Map<String, JsonValue>>(json!({
            "email": "a@example.test",
            "name": "Ada",
            "score": 4
        }))
        .expect("existing row object");
        let patch = serde_json::from_value::<Map<String, JsonValue>>(json!({
            "name": "Ada Lovelace"
        }))
        .expect("patch row object");

        let merged = merged_row_values(&headers, &existing, &patch).expect("merged values");
        assert_eq!(
            merged,
            vec![json!("a@example.test"), json!("Ada Lovelace"), json!(4)]
        );
    }

    #[test]
    fn row_matches_filters_compares_scalar_values() {
        let row = serde_json::from_value::<Map<String, JsonValue>>(json!({
            "email": "a@example.test",
            "score": 4
        }))
        .expect("row object");
        let yes = serde_json::from_value::<Map<String, JsonValue>>(json!({
            "email": "a@example.test",
            "score": 4
        }))
        .expect("filters");
        let no = serde_json::from_value::<Map<String, JsonValue>>(json!({
            "email": "missing@example.test"
        }))
        .expect("filters");

        assert!(row_matches_filters(&row, &yes).expect("filters evaluate"));
        assert!(!row_matches_filters(&row, &no).expect("filters evaluate"));
    }
}
