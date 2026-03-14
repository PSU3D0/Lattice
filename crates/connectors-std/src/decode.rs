use capabilities::http::HttpResponse;
use serde_json::{Map, Value};

use crate::errors::ConnectorRuntimeError;

#[derive(Debug, Clone, Copy)]
pub struct ResponseDescriptor {
    pub root_path: &'static str,
    pub collection_field: Option<&'static str>,
}

pub fn decode_response_root(
    response: &HttpResponse,
    descriptor: &ResponseDescriptor,
) -> Result<Value, ConnectorRuntimeError> {
    let body = if response.body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice::<Value>(&response.body)?
    };

    let tokens = descriptor.root_path.split('.').collect::<Vec<_>>();
    let tokens = match tokens.as_slice() {
        ["body"] => &tokens[1..],
        ["body", rest @ ..] => rest,
        _ => {
            return Err(ConnectorRuntimeError::invalid_response(format!(
                "unsupported response root_path `{}`; expected `body` or `body.<field>`",
                descriptor.root_path
            )));
        }
    };

    let mut current = &body;
    for token in tokens {
        current = current.get(*token).ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(format!(
                "response root_path `{}` missing field `{token}`",
                descriptor.root_path
            ))
        })?;
    }

    Ok(current.clone())
}

pub fn finalize_output_value(
    value: Value,
    descriptor: &ResponseDescriptor,
) -> Result<Value, ConnectorRuntimeError> {
    match descriptor.collection_field {
        Some(field) => match value {
            Value::Array(items) => {
                let mut object = Map::new();
                object.insert(field.to_string(), Value::Array(items));
                Ok(Value::Object(object))
            }
            other => Err(ConnectorRuntimeError::invalid_response(format!(
                "response expected array to wrap into `{field}`, found {other}"
            ))),
        },
        None => Ok(value),
    }
}

pub fn extract_collection_items(value: Value) -> Result<Vec<Value>, ConnectorRuntimeError> {
    match value {
        Value::Array(items) => Ok(items),
        other => Err(ConnectorRuntimeError::invalid_response(format!(
            "paginated response expected array payload, found {other}"
        ))),
    }
}
