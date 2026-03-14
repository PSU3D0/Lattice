use capabilities::http::HttpHeaders;
use serde_json::{Map, Value};

use crate::errors::ConnectorRuntimeError;

#[derive(Debug, Clone, Copy)]
pub struct PaginationDescriptor {
    pub enabled_from: &'static str,
    pub page_size_param: &'static str,
    pub page_size: u32,
    pub max_items_from: Option<&'static str>,
}

pub fn pagination_enabled(
    input: &Map<String, Value>,
    descriptor: &PaginationDescriptor,
) -> Result<bool, ConnectorRuntimeError> {
    let value = input.get(descriptor.enabled_from).ok_or_else(|| {
        ConnectorRuntimeError::MissingInputField {
            field: descriptor.enabled_from.to_string(),
        }
    })?;
    match value {
        Value::Bool(flag) => Ok(*flag),
        other => Err(ConnectorRuntimeError::invalid_response(format!(
            "pagination flag `{}` must be bool, found {other}",
            descriptor.enabled_from
        ))),
    }
}

pub fn max_items(
    input: &Map<String, Value>,
    descriptor: &PaginationDescriptor,
) -> Result<Option<usize>, ConnectorRuntimeError> {
    let Some(field) = descriptor.max_items_from else {
        return Ok(None);
    };
    let value = input
        .get(field)
        .ok_or_else(|| ConnectorRuntimeError::MissingInputField {
            field: field.to_string(),
        })?;
    match value {
        Value::Null => Ok(None),
        Value::Number(number) => number
            .as_u64()
            .map(|value| Some(value as usize))
            .ok_or_else(|| {
                ConnectorRuntimeError::invalid_response(format!(
                    "pagination max_items field `{field}` must be a non-negative integer"
                ))
            }),
        other => Err(ConnectorRuntimeError::invalid_response(format!(
            "pagination max_items field `{field}` must be numeric, found {other}"
        ))),
    }
}

pub fn next_link(headers: &HttpHeaders) -> Option<String> {
    let link_value = header_value(headers, "link")?;
    parse_next_link(link_value)
}

fn header_value<'a>(headers: &'a HttpHeaders, name: &str) -> Option<&'a str> {
    headers.iter().find_map(|(candidate, value)| {
        candidate
            .eq_ignore_ascii_case(name)
            .then_some(value.as_str())
    })
}

fn parse_next_link(link_value: &str) -> Option<String> {
    for segment in link_value.split(',') {
        let segment = segment.trim();
        let (url_part, params_part) = segment.split_once(';')?;
        let url = url_part
            .trim()
            .strip_prefix('<')?
            .strip_suffix('>')?
            .to_string();
        if params_part
            .split(';')
            .any(|param| param.trim() == "rel=\"next\"")
        {
            return Some(url);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_header_parser_finds_next_relation() {
        let mut headers = HttpHeaders::default();
        headers.insert(
            "link",
            "<https://example.test/page/2>; rel=\"next\", <https://example.test/page/5>; rel=\"last\"",
        );

        assert_eq!(
            next_link(&headers).as_deref(),
            Some("https://example.test/page/2")
        );
    }
}
