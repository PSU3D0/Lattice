use capabilities::http::{HttpHeaders, HttpMethod, HttpRequest};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Map, Value};

use crate::errors::ConnectorRuntimeError;

#[derive(Debug, Clone, Copy)]
pub struct FieldBinding {
    pub wire_name: &'static str,
    pub input_field: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub struct StaticHeaderDescriptor {
    pub name: &'static str,
    pub value: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub struct RequestDescriptor {
    pub method: HttpMethod,
    pub path_template: &'static str,
    pub path_params: &'static [FieldBinding],
    pub query: &'static [FieldBinding],
    pub body: &'static [FieldBinding],
    pub headers: &'static [StaticHeaderDescriptor],
}

pub fn build_request(
    base_url: &str,
    input: &Map<String, Value>,
    request: &RequestDescriptor,
    extra_query: &[(String, String)],
) -> Result<HttpRequest, ConnectorRuntimeError> {
    let mut url = format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        render_path_template(input, request.path_template, request.path_params)?
    );

    for binding in request.query {
        if let Some(value) = lookup_scalar_string(input, binding.input_field, "query")? {
            append_query_pair(&mut url, binding.wire_name, &value);
        }
    }

    for (name, value) in extra_query {
        append_query_pair(&mut url, name, value);
    }

    let mut headers = HttpHeaders::default();
    for header in request.headers {
        headers.insert(header.name, header.value);
    }

    let body = build_json_body(input, request.body)?;
    if body.is_some() && headers.get("Content-Type").is_none() {
        headers.insert("Content-Type", "application/json");
    }

    Ok(HttpRequest {
        method: request.method,
        url,
        headers,
        body,
        timeout_ms: Some(10_000),
    })
}

pub fn build_followup_request(url: String, request: &RequestDescriptor) -> HttpRequest {
    let mut headers = HttpHeaders::default();
    for header in request.headers {
        headers.insert(header.name, header.value);
    }

    HttpRequest {
        method: request.method,
        url,
        headers,
        body: None,
        timeout_ms: Some(10_000),
    }
}

pub fn append_query_pair(url: &mut String, name: &str, value: &str) {
    let separator = if url.contains('?') { '&' } else { '?' };
    url.push(separator);
    url.push_str(&encode_component(name));
    url.push('=');
    url.push_str(&encode_component(value));
}

fn build_json_body(
    input: &Map<String, Value>,
    bindings: &[FieldBinding],
) -> Result<Option<Vec<u8>>, ConnectorRuntimeError> {
    if bindings.is_empty() {
        return Ok(None);
    }

    let mut body = serde_json::Map::new();
    for binding in bindings {
        if let Some(value) = input.get(binding.input_field) {
            // Generated input types currently serialize optional unset fields as explicit `null`.
            // We intentionally omit those entries rather than sending `"field": null`.
            if !value.is_null() {
                body.insert(binding.wire_name.to_string(), value.clone());
            }
        } else {
            return Err(ConnectorRuntimeError::MissingInputField {
                field: binding.input_field.to_string(),
            });
        }
    }

    Ok(Some(serde_json::to_vec(&Value::Object(body))?))
}

fn render_path_template(
    input: &Map<String, Value>,
    template: &str,
    bindings: &[FieldBinding],
) -> Result<String, ConnectorRuntimeError> {
    let mut rendered = template.to_string();
    for binding in bindings {
        let value = lookup_scalar_string(input, binding.input_field, "path")?.ok_or_else(|| {
            ConnectorRuntimeError::MissingInputField {
                field: binding.input_field.to_string(),
            }
        })?;
        let placeholder = format!("{{{}}}", binding.wire_name);
        if !rendered.contains(&placeholder) {
            return Err(ConnectorRuntimeError::InvalidPathTemplate(format!(
                "path template `{template}` does not contain placeholder `{placeholder}`"
            )));
        }
        rendered = rendered.replace(&placeholder, &encode_component(&value));
    }
    if rendered.contains('{') || rendered.contains('}') {
        return Err(ConnectorRuntimeError::InvalidPathTemplate(format!(
            "path template `{template}` still contains unresolved placeholders"
        )));
    }
    Ok(rendered)
}

fn lookup_scalar_string(
    input: &Map<String, Value>,
    field: &str,
    usage: &'static str,
) -> Result<Option<String>, ConnectorRuntimeError> {
    let value = input
        .get(field)
        .ok_or_else(|| ConnectorRuntimeError::MissingInputField {
            field: field.to_string(),
        })?;

    if value.is_null() {
        return Ok(None);
    }

    match value {
        Value::String(value) => Ok(Some(value.clone())),
        Value::Bool(value) => Ok(Some(value.to_string())),
        Value::Number(value) => Ok(Some(value.to_string())),
        _ => Err(ConnectorRuntimeError::InvalidScalarField {
            field: field.to_string(),
            usage,
        }),
    }
}

fn encode_component(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const DEMO_HEADERS: &[StaticHeaderDescriptor] = &[StaticHeaderDescriptor {
        name: "X-Static",
        value: "demo",
    }];

    #[test]
    fn followup_request_preserves_static_headers() {
        let request = RequestDescriptor {
            method: HttpMethod::Get,
            path_template: "/items",
            path_params: &[],
            query: &[],
            body: &[],
            headers: DEMO_HEADERS,
        };

        let followup =
            build_followup_request("https://example.test/items?page=2".to_string(), &request);

        assert_eq!(followup.headers.get("X-Static"), Some(&"demo".to_string()));
    }

    #[test]
    fn build_request_encodes_query_and_path_values() {
        let request = RequestDescriptor {
            method: HttpMethod::Get,
            path_template: "/repos/{owner}",
            path_params: &[FieldBinding {
                wire_name: "owner",
                input_field: "owner",
            }],
            query: &[FieldBinding {
                wire_name: "state",
                input_field: "state",
            }],
            body: &[],
            headers: &[],
        };
        let input = json!({ "owner": "octo corp", "state": "open" });
        let input = input.as_object().expect("input object");

        let built =
            build_request("https://example.test", input, &request, &[]).expect("request built");

        assert_eq!(
            built.url,
            "https://example.test/repos/octo%20corp?state=open"
        );
    }
}
