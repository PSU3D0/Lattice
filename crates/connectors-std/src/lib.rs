pub mod auth;
pub mod decode;
pub mod endpoint;
pub mod errors;
pub mod http;
pub mod pagination;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use capabilities::context;
use capabilities::http::{HttpMethod, HttpRequest, HttpResponse};

use crate::auth::{OutboundAuthProfileDescriptor, apply_outbound_auth};
use crate::decode::{
    ResponseDescriptor, decode_response_root, extract_collection_items, finalize_output_value,
};
use crate::endpoint::{EndpointProfileDescriptor, apply_default_headers, resolve_base_url};
use crate::errors::ConnectorRuntimeError;
use crate::http::{RequestDescriptor, build_followup_request, build_request};
use crate::pagination::{PaginationDescriptor, max_items, next_link, pagination_enabled};

#[derive(Debug, Clone, Copy)]
pub struct ActionDescriptor {
    pub identifier: &'static str,
    pub endpoint: &'static EndpointProfileDescriptor,
    pub auth: Option<&'static OutboundAuthProfileDescriptor>,
    pub request: &'static RequestDescriptor,
    pub pagination: Option<&'static PaginationDescriptor>,
    pub response: &'static ResponseDescriptor,
}

pub async fn run_action_from_current<In, Out>(
    input: &In,
    action: &'static ActionDescriptor,
) -> Result<Out, ConnectorRuntimeError>
where
    In: Serialize,
    Out: DeserializeOwned,
{
    let input_value = serde_json::to_value(input)?;
    let input_object = input_value
        .as_object()
        .ok_or(ConnectorRuntimeError::InvalidInputObject)?;

    let base_url = resolve_base_url(action.endpoint);
    let pagination = action.pagination.copied();
    let should_paginate = match pagination {
        Some(descriptor) => pagination_enabled(input_object, &descriptor)?,
        None => false,
    };
    let limit = match pagination {
        Some(descriptor) if should_paginate => max_items(input_object, &descriptor)?,
        _ => None,
    };

    if should_paginate {
        let paginated = execute_paginated_action(input_object, action, &base_url, limit).await?;
        return serde_json::from_value(paginated).map_err(ConnectorRuntimeError::from);
    }

    let extra_query = pagination
        .map(|descriptor| {
            vec![(
                descriptor.page_size_param.to_string(),
                descriptor.page_size.to_string(),
            )]
        })
        .unwrap_or_default();
    let mut request = build_request(&base_url, input_object, action.request, &extra_query)?;
    apply_action_defaults(&mut request, action)?;
    let response = send_request(action, request).await?;
    let root = decode_success_response(&response, action.response)?;
    let output = finalize_output_value(root, action.response)?;
    serde_json::from_value(output).map_err(ConnectorRuntimeError::from)
}

async fn execute_paginated_action(
    input: &serde_json::Map<String, Value>,
    action: &'static ActionDescriptor,
    base_url: &str,
    limit: Option<usize>,
) -> Result<Value, ConnectorRuntimeError> {
    let pagination = action
        .pagination
        .expect("paginated execution requires descriptor");
    let extra_query = vec![(
        pagination.page_size_param.to_string(),
        pagination.page_size.to_string(),
    )];
    let mut request = build_request(base_url, input, action.request, &extra_query)?;
    apply_action_defaults(&mut request, action)?;

    let mut items = Vec::new();
    loop {
        let response = send_request(action, request).await?;
        let page_root = decode_success_response(&response, action.response)?;
        items.extend(extract_collection_items(page_root)?);

        if let Some(max_items) = limit {
            if items.len() >= max_items {
                items.truncate(max_items);
                break;
            }
        }

        let Some(next) = next_link(&response.headers) else {
            break;
        };

        let followup_url = if next.starts_with("http://") || next.starts_with("https://") {
            next
        } else {
            format!("{}{}", base_url.trim_end_matches('/'), next)
        };

        request = build_followup_request(followup_url, action.request);
        apply_action_defaults(&mut request, action)?;
    }

    finalize_output_value(Value::Array(items), action.response)
}

fn apply_action_defaults(
    request: &mut HttpRequest,
    action: &'static ActionDescriptor,
) -> Result<(), ConnectorRuntimeError> {
    apply_default_headers(&mut request.headers, action.endpoint);
    if let Some(profile) = action.auth {
        apply_outbound_auth(&mut request.url, &mut request.headers, profile)?;
    }
    Ok(())
}

fn decode_success_response(
    response: &HttpResponse,
    descriptor: &ResponseDescriptor,
) -> Result<Value, ConnectorRuntimeError> {
    if !response.is_success() {
        let body = String::from_utf8_lossy(&response.body);
        let body = body.chars().take(240).collect::<String>();
        return Err(ConnectorRuntimeError::HttpStatus {
            status: response.status,
            body,
        });
    }
    decode_response_root(response, descriptor)
}

async fn send_request(
    action: &'static ActionDescriptor,
    request: HttpRequest,
) -> Result<HttpResponse, ConnectorRuntimeError> {
    context::with_current_async(|resources| async move {
        match action.request.method {
            HttpMethod::Get | HttpMethod::Head => {
                let client =
                    resources
                        .http_read()
                        .ok_or(ConnectorRuntimeError::MissingHttpRead {
                            action: action.identifier,
                        })?;
                client
                    .send(request)
                    .await
                    .map_err(ConnectorRuntimeError::from)
            }
            HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch | HttpMethod::Delete => {
                let client =
                    resources
                        .http_write()
                        .ok_or(ConnectorRuntimeError::MissingHttpWrite {
                            action: action.identifier,
                        })?;
                client
                    .send(request)
                    .await
                    .map_err(ConnectorRuntimeError::from)
            }
        }
    })
    .await
    .ok_or(ConnectorRuntimeError::MissingResourceContext)?
}
