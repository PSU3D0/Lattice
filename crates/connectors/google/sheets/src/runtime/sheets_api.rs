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
    GoogleSheetsAppendRowInput, GoogleSheetsAppendRowOutput, GoogleSheetsFindRowsInput,
    GoogleSheetsFindRowsOutput, GoogleSheetsRowMatch, GoogleSheetsUpsertAction,
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
