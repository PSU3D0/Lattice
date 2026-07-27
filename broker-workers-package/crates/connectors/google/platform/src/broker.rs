use serde_json::{Map, Value};

use crate::{
    gmail::{base64url_no_pad, build_plain_text_email},
    sheets::{GoogleSheetsValueInputOption, append_table_range, ordered_row_values},
};

pub const SHEETS_APPEND_ROW_ADAPTER_ID: &str = "google.sheets.append_row.v1";
pub const SHEETS_HEADERS_AUTHORITY_POINTER: &str = "/google/sheets/headers";
pub const SHEETS_APPEND_ROW_ADAPTER_VERSION: &str = "1";
pub const SHEETS_APPEND_ROW_ADAPTER_HASH: &str =
    "sha256:54db6603967e1ce4e46ef45ece7bfa947c129564e40a290989fa2ae901a23966";
pub const SHEETS_CREATE_SPREADSHEET_ADAPTER_ID: &str = "google.sheets.create_spreadsheet.v1";
pub const SHEETS_CREATE_SPREADSHEET_ADAPTER_VERSION: &str = "1";
pub const SHEETS_CREATE_SPREADSHEET_ADAPTER_HASH: &str =
    "sha256:ed9e62ea7bd0e93fc3de07ffe4a8d168f840faf61d463bcc8ca6564bb5b82755";
pub const GMAIL_RFC822_ADAPTER_ID: &str = "google.gmail.rfc822_message.v1";
pub const GMAIL_RFC822_ADAPTER_VERSION: &str = "1";
pub const GMAIL_RFC822_ADAPTER_HASH: &str =
    "sha256:5a5de77f756b49aac0fb5339bf764f9e53a9437cdc41619c2c978aa5dbb4e3fc";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoogleBrokerAdapterError {
    InvalidInput,
    MissingAuthorityFacts,
}

pub fn adapt_sheets_create_spreadsheet(
    input: &Value,
) -> Result<Map<String, Value>, GoogleBrokerAdapterError> {
    let input = input
        .as_object()
        .ok_or(GoogleBrokerAdapterError::InvalidInput)?;
    let title = required_string(input, "title")?;
    if title.len() > 256
        || input.iter().any(|(field, value)| {
            field != "title"
                && (!matches!(
                    field.as_str(),
                    "locale" | "time_zone" | "initial_sheet_title"
                ) || !value.is_null())
        })
    {
        return Err(GoogleBrokerAdapterError::InvalidInput);
    }
    Ok(Map::from_iter([(
        "title".into(),
        serde_json::json!({ "title": title }),
    )]))
}

pub fn adapt_sheets_append_row(
    input: &Value,
    authority_facts: &Value,
) -> Result<Map<String, Value>, GoogleBrokerAdapterError> {
    let input = input
        .as_object()
        .ok_or(GoogleBrokerAdapterError::InvalidInput)?;
    let spreadsheet_id = required_string(input, "spreadsheet_id")?;
    let sheet = required_string(input, "sheet")?;
    let row = input
        .get("row")
        .and_then(Value::as_object)
        .ok_or(GoogleBrokerAdapterError::InvalidInput)?;
    let header_row = match input.get("header_row") {
        None => 1,
        Some(value) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or(GoogleBrokerAdapterError::InvalidInput)?,
    };
    let value_input_option = match input
        .get("value_input_option")
        .and_then(Value::as_str)
        .unwrap_or("raw")
    {
        "raw" => GoogleSheetsValueInputOption::Raw,
        "user_entered" => GoogleSheetsValueInputOption::UserEntered,
        _ => return Err(GoogleBrokerAdapterError::InvalidInput),
    };
    let headers = authority_facts
        .pointer(SHEETS_HEADERS_AUTHORITY_POINTER)
        .and_then(Value::as_array)
        .ok_or(GoogleBrokerAdapterError::MissingAuthorityFacts)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or(GoogleBrokerAdapterError::MissingAuthorityFacts)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if headers.is_empty() {
        return Err(GoogleBrokerAdapterError::MissingAuthorityFacts);
    }
    let values =
        ordered_row_values(&headers, row).map_err(|_| GoogleBrokerAdapterError::InvalidInput)?;
    let range = append_table_range(sheet, header_row, headers.len());

    Ok(Map::from_iter([
        (
            "spreadsheet_id".into(),
            Value::String(spreadsheet_id.into()),
        ),
        ("sheet".into(), Value::String(range)),
        (
            "value_input_option".into(),
            Value::String(value_input_option.as_google_api_value().into()),
        ),
        ("row".into(), Value::Array(vec![Value::Array(values)])),
    ]))
}

pub fn adapt_gmail_rfc822_message(
    input: &Value,
) -> Result<Map<String, Value>, GoogleBrokerAdapterError> {
    let input = input
        .as_object()
        .ok_or(GoogleBrokerAdapterError::InvalidInput)?;
    let to = required_string(input, "to")?;
    let subject = required_string(input, "subject")?;
    let text_body = required_string(input, "text_body")?;
    let cc = optional_string(input, "cc")?;
    let bcc = optional_string(input, "bcc")?;
    let message = build_plain_text_email(to, cc, bcc, subject, text_body);
    Ok(Map::from_iter([(
        "text_body".into(),
        Value::String(base64url_no_pad(message.as_bytes())),
    )]))
}

fn required_string<'a>(
    input: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, GoogleBrokerAdapterError> {
    input
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(GoogleBrokerAdapterError::InvalidInput)
}

fn optional_string<'a>(
    input: &'a Map<String, Value>,
    field: &str,
) -> Result<Option<&'a str>, GoogleBrokerAdapterError> {
    match input.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(GoogleBrokerAdapterError::InvalidInput),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sheets_adapter_orders_values_and_builds_exact_provider_fields() {
        let input = serde_json::json!({
            "spreadsheet_id": "sheet/one",
            "sheet": "Leads",
            "row": {"name": "Ada", "email": "ada@example.test"},
            "header_row": 1,
            "value_input_option": "user_entered"
        });
        let facts = serde_json::json!({"google":{"sheets":{"headers":["email","name"]}}});
        let adapted = adapt_sheets_append_row(&input, &facts).unwrap();
        assert_eq!(adapted["spreadsheet_id"], "sheet/one");
        assert_eq!(adapted["sheet"], "'Leads'!A1:B");
        assert_eq!(adapted["value_input_option"], "USER_ENTERED");
        assert_eq!(
            adapted["row"],
            serde_json::json!([["ada@example.test", "Ada"]])
        );
    }

    #[test]
    fn create_spreadsheet_adapter_builds_only_bounded_properties() {
        let adapted = adapt_sheets_create_spreadsheet(&serde_json::json!({
            "title": "Disposable proof sheet"
        }))
        .unwrap();
        assert_eq!(
            Value::Object(adapted),
            serde_json::json!({"title":{"title":"Disposable proof sheet"}})
        );
        assert!(
            adapt_sheets_create_spreadsheet(&serde_json::json!({"title":"x".repeat(257)})).is_err()
        );
        assert!(
            adapt_sheets_create_spreadsheet(&serde_json::json!({"title":"sheet","locale":"en_US"}))
                .is_err()
        );
        assert!(
            adapt_sheets_create_spreadsheet(
                &serde_json::json!({"title":"sheet","passthrough":null})
            )
            .is_err()
        );
    }

    #[test]
    fn gmail_adapter_reuses_rfc822_and_base64url_helpers() {
        let input = serde_json::json!({
            "to": "ops@example.test",
            "subject": "Hello",
            "text_body": "body"
        });
        let adapted = adapt_gmail_rfc822_message(&input).unwrap();
        let expected = build_plain_text_email("ops@example.test", None, None, "Hello", "body");
        assert_eq!(adapted["text_body"], base64url_no_pad(expected.as_bytes()));
    }
}
