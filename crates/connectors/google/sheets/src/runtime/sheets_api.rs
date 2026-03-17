use capabilities::http::{HttpMethod, HttpRequest};
use connector_google_platform::sheets::{
    GoogleSheetsValueInputOption, SheetTableRow, append_table_range, expect_object,
    last_row_from_a1_range, merged_row_values, ordered_row_values, parse_sheet_table,
    row_matches_filters, row_range, wide_read_range,
};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::http::append_query_pair;
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Map, Value as JsonValue, json};

use crate::generated::profiles::{
    GOOGLE_SHEETS_DEFAULT_ENDPOINT_PROFILE, GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH,
};
use crate::generated::types::{
    GoogleSheetsAppendRowInput, GoogleSheetsAppendRowOutput, GoogleSheetsCreateSheetInput,
    GoogleSheetsCreateSheetOutput, GoogleSheetsCreateSpreadsheetInput,
    GoogleSheetsCreateSpreadsheetOutput, GoogleSheetsFindRowsInput, GoogleSheetsFindRowsOutput,
    GoogleSheetsRowMatch, GoogleSheetsSheetSummary, GoogleSheetsUpsertAction,
    GoogleSheetsUpsertRowInput, GoogleSheetsUpsertRowOutput,
};
use crate::runtime::errors::ConnectorRuntimeError;

pub struct SheetsApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl SheetsApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&GOOGLE_SHEETS_DEFAULT_ENDPOINT_PROFILE, &context)
                .await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn create_spreadsheet(
        &self,
        input: &GoogleSheetsCreateSpreadsheetInput,
    ) -> Result<GoogleSheetsCreateSpreadsheetOutput, ConnectorRuntimeError> {
        let mut properties = Map::new();
        properties.insert("title".to_string(), JsonValue::String(input.title.clone()));
        if let Some(locale) = &input.locale {
            properties.insert("locale".to_string(), JsonValue::String(locale.clone()));
        }
        if let Some(time_zone) = &input.time_zone {
            properties.insert("timeZone".to_string(), JsonValue::String(time_zone.clone()));
        }

        let mut body = Map::new();
        body.insert("properties".to_string(), JsonValue::Object(properties));
        if let Some(initial_sheet_title) = &input.initial_sheet_title {
            body.insert(
                "sheets".to_string(),
                JsonValue::Array(vec![json!({
                    "properties": {
                        "title": initial_sheet_title,
                    }
                })]),
            );
        }

        let response = self
            .request_json(
                HttpMethod::Post,
                spreadsheets_path(),
                &[],
                Some(JsonValue::Object(body)),
            )
            .await?;

        spreadsheet_created_output_from_response(&response)
    }

    pub async fn create_sheet(
        &self,
        input: &GoogleSheetsCreateSheetInput,
    ) -> Result<GoogleSheetsCreateSheetOutput, ConnectorRuntimeError> {
        let mut properties = Map::new();
        properties.insert("title".to_string(), JsonValue::String(input.title.clone()));
        if let Some(index) = input.index {
            properties.insert("index".to_string(), JsonValue::from(index));
        }
        if input.row_count.is_some() || input.column_count.is_some() {
            let mut grid_properties = Map::new();
            if let Some(row_count) = input.row_count {
                grid_properties.insert("rowCount".to_string(), JsonValue::from(row_count));
            }
            if let Some(column_count) = input.column_count {
                grid_properties.insert("columnCount".to_string(), JsonValue::from(column_count));
            }
            properties.insert(
                "gridProperties".to_string(),
                JsonValue::Object(grid_properties),
            );
        }

        let body = json!({
            "requests": [
                {
                    "addSheet": {
                        "properties": properties,
                    }
                }
            ]
        });

        let response = self
            .request_json(
                HttpMethod::Post,
                &spreadsheet_batch_update_path(&input.spreadsheet_id),
                &[],
                Some(body),
            )
            .await?;

        let properties = response
            .pointer("/replies/0/addSheet/properties")
            .ok_or_else(|| {
                ConnectorRuntimeError::invalid_response(
                    "Google Sheets create_sheet response did not contain `replies[0].addSheet.properties`",
                )
            })?;

        Ok(GoogleSheetsCreateSheetOutput {
            spreadsheet_id: input.spreadsheet_id.clone(),
            sheet: sheet_summary_from_properties(properties)?,
        })
    }

    pub async fn append_row(
        &self,
        input: &GoogleSheetsAppendRowInput,
    ) -> Result<GoogleSheetsAppendRowOutput, ConnectorRuntimeError> {
        let row = expect_object(&input.row, "append_row input.row")
            .map_err(ConnectorRuntimeError::invalid_response)?;
        let (headers, _) = self
            .read_table(&input.spreadsheet_id, &input.sheet, input.header_row)
            .await?;
        let ordered =
            ordered_row_values(&headers, row).map_err(ConnectorRuntimeError::invalid_response)?;
        let updated_range = self
            .append_values(
                &input.spreadsheet_id,
                &input.sheet,
                input.header_row,
                headers.len(),
                input.value_input_option.unwrap_or_default(),
                ordered,
            )
            .await?;

        Ok(GoogleSheetsAppendRowOutput {
            spreadsheet_id: input.spreadsheet_id.clone(),
            sheet: input.sheet.clone(),
            row_index: last_row_from_a1_range(&updated_range),
            updated_range,
        })
    }

    pub async fn find_rows(
        &self,
        input: &GoogleSheetsFindRowsInput,
    ) -> Result<GoogleSheetsFindRowsOutput, ConnectorRuntimeError> {
        let filters = expect_object(&input.filters, "find_rows input.filters")
            .map_err(ConnectorRuntimeError::invalid_response)?;
        let (_, rows) = self
            .read_table(&input.spreadsheet_id, &input.sheet, input.header_row)
            .await?;
        let mut items = Vec::new();
        let limit = input.limit.map(|limit| limit as usize);

        for row in rows {
            if row_matches_filters(&row.values, filters)
                .map_err(ConnectorRuntimeError::invalid_response)?
            {
                items.push(GoogleSheetsRowMatch {
                    row_number: row.row_number,
                    values: JsonValue::Object(row.values.clone()),
                });
                if let Some(limit) = limit {
                    if items.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(GoogleSheetsFindRowsOutput { items })
    }

    pub async fn upsert_row(
        &self,
        input: &GoogleSheetsUpsertRowInput,
    ) -> Result<GoogleSheetsUpsertRowOutput, ConnectorRuntimeError> {
        if input.match_on.is_empty() {
            return Err(ConnectorRuntimeError::invalid_response(
                "upsert_row requires at least one match_on column",
            ));
        }

        let row = expect_object(&input.row, "upsert_row input.row")
            .map_err(ConnectorRuntimeError::invalid_response)?;
        let (headers, rows) = self
            .read_table(&input.spreadsheet_id, &input.sheet, input.header_row)
            .await?;

        let mut filters = Map::new();
        for column in &input.match_on {
            let value = row.get(column).ok_or_else(|| {
                ConnectorRuntimeError::invalid_response(format!(
                    "upsert_row input.row must include match_on column `{column}`"
                ))
            })?;
            filters.insert(column.clone(), value.clone());
        }

        if let Some(existing) = rows
            .iter()
            .find(|candidate| row_matches_filters(&candidate.values, &filters).unwrap_or(false))
        {
            let merged = merged_row_values(&headers, &existing.values, row)
                .map_err(ConnectorRuntimeError::invalid_response)?;
            let updated_range = self
                .update_values(
                    &input.spreadsheet_id,
                    &input.sheet,
                    existing.row_number,
                    headers.len(),
                    input.value_input_option.unwrap_or_default(),
                    merged,
                )
                .await?;

            return Ok(GoogleSheetsUpsertRowOutput {
                action: GoogleSheetsUpsertAction::Updated,
                spreadsheet_id: input.spreadsheet_id.clone(),
                sheet: input.sheet.clone(),
                row_index: last_row_from_a1_range(&updated_range),
                updated_range,
            });
        }

        let ordered =
            ordered_row_values(&headers, row).map_err(ConnectorRuntimeError::invalid_response)?;
        let updated_range = self
            .append_values(
                &input.spreadsheet_id,
                &input.sheet,
                input.header_row,
                headers.len(),
                input.value_input_option.unwrap_or_default(),
                ordered,
            )
            .await?;

        Ok(GoogleSheetsUpsertRowOutput {
            action: GoogleSheetsUpsertAction::Inserted,
            spreadsheet_id: input.spreadsheet_id.clone(),
            sheet: input.sheet.clone(),
            row_index: last_row_from_a1_range(&updated_range),
            updated_range,
        })
    }

    async fn read_table(
        &self,
        spreadsheet_id: &str,
        sheet: &str,
        header_row: u32,
    ) -> Result<(Vec<String>, Vec<SheetTableRow>), ConnectorRuntimeError> {
        let path = values_path(spreadsheet_id, &wide_read_range(sheet, header_row));
        let body = self.request_json(HttpMethod::Get, &path, &[], None).await?;
        let values = values_matrix_from_response(&body)?;
        parse_sheet_table(&values, header_row).map_err(ConnectorRuntimeError::invalid_response)
    }

    async fn append_values(
        &self,
        spreadsheet_id: &str,
        sheet: &str,
        header_row: u32,
        column_count: usize,
        value_input_option: GoogleSheetsValueInputOption,
        ordered_values: Vec<JsonValue>,
    ) -> Result<String, ConnectorRuntimeError> {
        let range = append_table_range(sheet, header_row, column_count);
        let path = format!("{}:append", values_path(spreadsheet_id, &range));
        let query = vec![
            (
                "valueInputOption".to_string(),
                value_input_option.as_google_api_value().to_string(),
            ),
            ("insertDataOption".to_string(), "INSERT_ROWS".to_string()),
        ];
        let body = json!({
            "majorDimension": "ROWS",
            "values": [ordered_values],
        });
        let response = self
            .request_json(HttpMethod::Post, &path, &query, Some(body))
            .await?;
        extract_updated_range(&response)
    }

    async fn update_values(
        &self,
        spreadsheet_id: &str,
        sheet: &str,
        row_number: u32,
        column_count: usize,
        value_input_option: GoogleSheetsValueInputOption,
        ordered_values: Vec<JsonValue>,
    ) -> Result<String, ConnectorRuntimeError> {
        let range = row_range(sheet, row_number, column_count);
        let path = values_path(spreadsheet_id, &range);
        let query = vec![(
            "valueInputOption".to_string(),
            value_input_option.as_google_api_value().to_string(),
        )];
        let body = json!({
            "majorDimension": "ROWS",
            "values": [ordered_values],
        });
        let response = self
            .request_json(HttpMethod::Put, &path, &query, Some(body))
            .await?;
        extract_updated_range(&response)
    }

    async fn request_json(
        &self,
        method: HttpMethod,
        path: &str,
        query: &[(String, String)],
        body: Option<JsonValue>,
    ) -> Result<JsonValue, ConnectorRuntimeError> {
        let mut url = format!("{}{}", self.endpoint.base_url.trim_end_matches('/'), path);
        for (name, value) in query {
            append_query_pair(&mut url, name, value);
        }

        let mut request = HttpRequest::new(method, url);
        apply_default_headers(&mut request.headers, &self.endpoint);
        apply_outbound_auth_with_context(
            &GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH,
            &mut request,
            &self.context,
        )
        .await?;

        if let Some(body) = body {
            request.headers.insert("Content-Type", "application/json");
            request.body = Some(serde_json::to_vec(&body)?);
        }

        let response = send_request_from_current(self.action_id, method, request).await?;
        decode_json_response_body(&response)
    }
}

fn spreadsheets_path() -> &'static str {
    "/v4/spreadsheets"
}

fn spreadsheet_batch_update_path(spreadsheet_id: &str) -> String {
    format!(
        "/v4/spreadsheets/{}:batchUpdate",
        encode_path_segment(spreadsheet_id)
    )
}

fn values_path(spreadsheet_id: &str, range: &str) -> String {
    format!(
        "/v4/spreadsheets/{}/values/{}",
        encode_path_segment(spreadsheet_id),
        encode_path_segment(range)
    )
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn values_matrix_from_response(
    body: &JsonValue,
) -> Result<Vec<Vec<JsonValue>>, ConnectorRuntimeError> {
    let Some(values) = body.get("values") else {
        return Ok(Vec::new());
    };
    let rows = values.as_array().ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(
            "Google Sheets values response must contain an array field `values`",
        )
    })?;

    rows.iter()
        .map(|row| {
            row.as_array().cloned().ok_or_else(|| {
                ConnectorRuntimeError::invalid_response(
                    "Google Sheets values response rows must be arrays",
                )
            })
        })
        .collect()
}

fn spreadsheet_created_output_from_response(
    body: &JsonValue,
) -> Result<GoogleSheetsCreateSpreadsheetOutput, ConnectorRuntimeError> {
    let spreadsheet_id = required_string_pointer(body, "/spreadsheetId", "spreadsheetId")?;
    let title = required_string_pointer(body, "/properties/title", "properties.title")?;
    let spreadsheet_url = body
        .get("spreadsheetUrl")
        .and_then(JsonValue::as_str)
        .map(str::to_string);

    let sheets = match body.get("sheets") {
        Some(JsonValue::Array(items)) => items
            .iter()
            .map(|sheet| {
                let properties = sheet.get("properties").ok_or_else(|| {
                    ConnectorRuntimeError::invalid_response(
                        "Google Sheets create_spreadsheet response sheets must contain `properties`",
                    )
                })?;
                sheet_summary_from_properties(properties)
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => {
            return Err(ConnectorRuntimeError::invalid_response(
                "Google Sheets create_spreadsheet response field `sheets` must be an array",
            ));
        }
        None => Vec::new(),
    };

    Ok(GoogleSheetsCreateSpreadsheetOutput {
        spreadsheet_id,
        spreadsheet_url,
        title,
        sheets,
    })
}

fn sheet_summary_from_properties(
    properties: &JsonValue,
) -> Result<GoogleSheetsSheetSummary, ConnectorRuntimeError> {
    Ok(GoogleSheetsSheetSummary {
        sheet_id: required_u32_pointer(properties, "/sheetId", "sheetId")?,
        title: required_string_pointer(properties, "/title", "title")?,
        index: optional_u32_pointer(properties, "/index")?,
        row_count: optional_u32_pointer(properties, "/gridProperties/rowCount")?,
        column_count: optional_u32_pointer(properties, "/gridProperties/columnCount")?,
    })
}

fn required_string_pointer(
    body: &JsonValue,
    pointer: &str,
    field_name: &str,
) -> Result<String, ConnectorRuntimeError> {
    body.pointer(pointer)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(format!(
                "Google Sheets response did not contain string field `{field_name}`"
            ))
        })
}

fn optional_u32_pointer(
    body: &JsonValue,
    pointer: &str,
) -> Result<Option<u32>, ConnectorRuntimeError> {
    body.pointer(pointer).map(json_u32).transpose()
}

fn required_u32_pointer(
    body: &JsonValue,
    pointer: &str,
    field_name: &str,
) -> Result<u32, ConnectorRuntimeError> {
    let value = body.pointer(pointer).ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(format!(
            "Google Sheets response did not contain numeric field `{field_name}`"
        ))
    })?;
    json_u32(value)
}

fn json_u32(value: &JsonValue) -> Result<u32, ConnectorRuntimeError> {
    let value = value.as_u64().ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(
            "Google Sheets response field must be an unsigned integer",
        )
    })?;
    u32::try_from(value).map_err(|_| {
        ConnectorRuntimeError::invalid_response(
            "Google Sheets response integer field exceeded supported u32 range",
        )
    })
}

fn extract_updated_range(body: &JsonValue) -> Result<String, ConnectorRuntimeError> {
    body.pointer("/updates/updatedRange")
        .or_else(|| body.pointer("/updatedRange"))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(
                "Google Sheets write response did not contain `updatedRange`",
            )
        })
}
