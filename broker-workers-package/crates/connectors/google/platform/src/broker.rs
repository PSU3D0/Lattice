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
    "sha256:273c32a2a836c676333259b5d6d644c778a47dac8559afdbd5a1544d24834afd";
pub const SHEETS_CREATE_MAX_HEADER_COUNT: usize = 64;
pub const SHEETS_CREATE_MAX_HEADER_BYTES: usize = 256;
pub const GMAIL_RFC822_ADAPTER_ID: &str = "google.gmail.rfc822_message.v1";
pub const GMAIL_RFC822_ADAPTER_VERSION: &str = "1";
pub const GMAIL_RFC822_ADAPTER_HASH: &str =
    "sha256:5a5de77f756b49aac0fb5339bf764f9e53a9437cdc41619c2c978aa5dbb4e3fc";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoogleBrokerAdapterError {
    InvalidInput,
    MissingAuthorityFacts,
}

pub fn validate_sheets_create_headers(headers: &[String]) -> Result<(), GoogleBrokerAdapterError> {
    if headers.len() > SHEETS_CREATE_MAX_HEADER_COUNT
        || headers.iter().any(|header| {
            header.trim().is_empty() || header.as_bytes().len() > SHEETS_CREATE_MAX_HEADER_BYTES
        })
        || headers
            .iter()
            .enumerate()
            .any(|(index, header)| headers[..index].contains(header))
    {
        return Err(GoogleBrokerAdapterError::InvalidInput);
    }
    Ok(())
}

pub fn adapt_sheets_create_spreadsheet(
    input: &Value,
) -> Result<Map<String, Value>, GoogleBrokerAdapterError> {
    let input = input
        .as_object()
        .ok_or(GoogleBrokerAdapterError::InvalidInput)?;
    if input.keys().any(|field| {
        !matches!(
            field.as_str(),
            "title" | "locale" | "time_zone" | "initial_sheet_title" | "header_row"
        )
    }) {
        return Err(GoogleBrokerAdapterError::InvalidInput);
    }
    let title = required_bounded_string(input, "title", 256)?;
    let locale = optional_bounded_string(input, "locale", 64)?;
    let time_zone = optional_bounded_string(input, "time_zone", 128)?;
    let sheet_title = optional_bounded_string(input, "initial_sheet_title", 100)?;
    let headers = input
        .get("header_row")
        .and_then(Value::as_array)
        .ok_or(GoogleBrokerAdapterError::InvalidInput)?
        .iter()
        .map(|header| {
            header
                .as_str()
                .map(str::to_owned)
                .ok_or(GoogleBrokerAdapterError::InvalidInput)
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_sheets_create_headers(&headers)?;

    let mut properties = serde_json::json!({ "title": title });
    if let Some(locale) = locale {
        properties["locale"] = Value::String(locale.into());
    }
    if let Some(time_zone) = time_zone {
        properties["timeZone"] = Value::String(time_zone.into());
    }
    let mut sheet = Map::new();
    if let Some(sheet_title) = sheet_title {
        sheet.insert(
            "properties".into(),
            serde_json::json!({ "title": sheet_title }),
        );
    }
    if !headers.is_empty() {
        sheet.insert(
            "data".into(),
            serde_json::json!([{
                "rowData": [{
                    "values": headers.iter().map(|header| serde_json::json!({
                        "userEnteredValue": { "stringValue": header }
                    })).collect::<Vec<_>>()
                }]
            }]),
        );
    }
    Ok(Map::from_iter([
        ("title".into(), properties),
        (
            "header_row".into(),
            Value::Array(vec![Value::Object(sheet)]),
        ),
    ]))
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

fn required_bounded_string<'a>(
    input: &'a Map<String, Value>,
    field: &str,
    max_bytes: usize,
) -> Result<&'a str, GoogleBrokerAdapterError> {
    required_string(input, field).and_then(|value| {
        (value.as_bytes().len() <= max_bytes)
            .then_some(value)
            .ok_or(GoogleBrokerAdapterError::InvalidInput)
    })
}

fn optional_bounded_string<'a>(
    input: &'a Map<String, Value>,
    field: &str,
    max_bytes: usize,
) -> Result<Option<&'a str>, GoogleBrokerAdapterError> {
    optional_string(input, field).and_then(|value| {
        value
            .is_none_or(|value| !value.is_empty() && value.as_bytes().len() <= max_bytes)
            .then_some(value)
            .ok_or(GoogleBrokerAdapterError::InvalidInput)
    })
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
    fn create_spreadsheet_adapter_builds_a_bounded_header_row() {
        let adapted = adapt_sheets_create_spreadsheet(&serde_json::json!({
            "title": "Disposable proof sheet",
            "locale": null,
            "time_zone": null,
            "initial_sheet_title": "Micro",
            "header_row": ["note"]
        }))
        .unwrap();
        assert_eq!(
            Value::Object(adapted),
            serde_json::json!({
                "title": {"title":"Disposable proof sheet"},
                "header_row": [{
                    "properties": {"title":"Micro"},
                    "data": [{"rowData":[{"values":[{
                        "userEnteredValue":{"stringValue":"note"}
                    }]}]}]
                }]
            })
        );
    }

    #[test]
    fn create_spreadsheet_adapter_rejects_oversized_or_smuggled_fields() {
        let base = || {
            serde_json::json!({
                "title": "sheet",
                "locale": null,
                "time_zone": null,
                "initial_sheet_title": "Micro",
                "header_row": ["note"]
            })
        };
        let mut too_many = base();
        too_many["header_row"] = serde_json::json!(
            (0..=SHEETS_CREATE_MAX_HEADER_COUNT)
                .map(|index| format!("column-{index}"))
                .collect::<Vec<_>>()
        );
        assert!(adapt_sheets_create_spreadsheet(&too_many).is_err());
        let mut too_long = base();
        too_long["header_row"] =
            serde_json::json!(["x".repeat(SHEETS_CREATE_MAX_HEADER_BYTES + 1)]);
        assert!(adapt_sheets_create_spreadsheet(&too_long).is_err());
        let mut smuggled = base();
        smuggled["namedRanges"] = serde_json::json!([]);
        assert!(adapt_sheets_create_spreadsheet(&smuggled).is_err());
        smuggled.as_object_mut().unwrap().remove("namedRanges");
        smuggled["developerMetadata"] = serde_json::json!([]);
        assert!(adapt_sheets_create_spreadsheet(&smuggled).is_err());
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
