pub mod auth;
pub mod decode;
pub mod dev;
pub mod endpoint;
pub mod errors;
pub mod http;
pub mod pagination;

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use capabilities::connector::{ConnectorBindingScope, ConnectorRuntime};
use capabilities::context;
use capabilities::http::{HttpMethod, HttpRequest, HttpResponse};

use crate::auth::OutboundAuthProfileDescriptor;
use crate::decode::{
    ResponseDescriptor, decode_response_root, extract_collection_items, finalize_output_value,
};
use crate::endpoint::{EndpointProfileDescriptor, ResolvedEndpointProfile, apply_default_headers};
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

struct ConnectorExecutionContext {
    runtime: Arc<dyn ConnectorRuntime>,
    scope: ConnectorBindingScope,
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

    let connector_context = connector_execution_context(action).await?;
    let endpoint = resolve_endpoint_profile(action, &connector_context).await?;
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
        let paginated =
            execute_paginated_action(input_object, action, &connector_context, &endpoint, limit)
                .await?;
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
    let mut request = build_request(
        &endpoint.base_url,
        input_object,
        action.request,
        &extra_query,
    )?;
    apply_action_defaults(&mut request, action, &connector_context, &endpoint).await?;
    let response = send_request(action, request).await?;
    let root = decode_success_response(&response, action.response)?;
    let output = finalize_output_value(root, action.response)?;
    serde_json::from_value(output).map_err(ConnectorRuntimeError::from)
}

async fn execute_paginated_action(
    input: &serde_json::Map<String, Value>,
    action: &'static ActionDescriptor,
    connector_context: &ConnectorExecutionContext,
    endpoint: &ResolvedEndpointProfile,
    limit: Option<usize>,
) -> Result<Value, ConnectorRuntimeError> {
    let pagination = action
        .pagination
        .expect("paginated execution requires descriptor");
    let extra_query = vec![(
        pagination.page_size_param.to_string(),
        pagination.page_size.to_string(),
    )];
    let mut request = build_request(&endpoint.base_url, input, action.request, &extra_query)?;
    apply_action_defaults(&mut request, action, connector_context, endpoint).await?;

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
            format!("{}{}", endpoint.base_url.trim_end_matches('/'), next)
        };

        request = build_followup_request(followup_url, action.request);
        apply_action_defaults(&mut request, action, connector_context, endpoint).await?;
    }

    finalize_output_value(Value::Array(items), action.response)
}

async fn connector_execution_context(
    action: &'static ActionDescriptor,
) -> Result<ConnectorExecutionContext, ConnectorRuntimeError> {
    context::with_current_async(|resources| async move {
        let runtime = resources.connector_runtime().ok_or(
            ConnectorRuntimeError::MissingConnectorRuntime {
                action: action.identifier,
            },
        )?;
        let scope =
            resources
                .connector_scope()
                .ok_or(ConnectorRuntimeError::MissingConnectorScope {
                    action: action.identifier,
                })?;
        Ok(ConnectorExecutionContext { runtime, scope })
    })
    .await
    .ok_or(ConnectorRuntimeError::MissingResourceContext)?
}

async fn resolve_endpoint_profile(
    action: &'static ActionDescriptor,
    connector_context: &ConnectorExecutionContext,
) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
    connector_context
        .runtime
        .resolve_endpoint_profile(&connector_context.scope, action.endpoint)
        .await
        .map_err(ConnectorRuntimeError::from)
}

async fn apply_action_defaults(
    request: &mut HttpRequest,
    action: &'static ActionDescriptor,
    connector_context: &ConnectorExecutionContext,
    endpoint: &ResolvedEndpointProfile,
) -> Result<(), ConnectorRuntimeError> {
    apply_default_headers(&mut request.headers, endpoint);
    if let Some(profile) = action.auth {
        connector_context
            .runtime
            .apply_outbound_auth(&connector_context.scope, profile, request)
            .await
            .map_err(ConnectorRuntimeError::from)?;
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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use capabilities::ResourceBag;
    use capabilities::connector::{
        ConnectorBindingScope, ConnectorRuntime,
        ConnectorRuntimeError as HostConnectorRuntimeError, EndpointProfileDescriptor,
        OutboundAuthKind, OutboundAuthProfileDescriptor, ResolvedEndpointProfile,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value as JsonValue;
    use std::sync::Arc;

    use super::*;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestInput {
        value: String,
    }

    const TEST_SCOPE: &str = "flow://connector-std-tests";
    const TEST_ENDPOINT: EndpointProfileDescriptor = EndpointProfileDescriptor {
        connector_id: "connector.test",
        name: "default",
        env_base_url_var: "LATTICE_CONNECTOR_ENDPOINT_TEST_BASE_URL",
        base_url: "https://example.test",
        default_headers: &[("Accept", "application/json")],
    };
    const TEST_AUTH: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {
        connector_id: "connector.test",
        name: "test_pat",
        env_var: "LATTICE_CONNECTOR_AUTH_TEST_PAT",
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };
    const TEST_REQUEST: RequestDescriptor = RequestDescriptor {
        method: HttpMethod::Get,
        path_template: "/items",
        path_params: &[],
        query: &[],
        body: &[],
        headers: &[],
    };
    const TEST_RESPONSE: ResponseDescriptor = ResponseDescriptor {
        root_path: "body",
        collection_field: None,
    };
    const ACTION_NO_AUTH: ActionDescriptor = ActionDescriptor {
        identifier: "connector.test.items.get",
        endpoint: &TEST_ENDPOINT,
        auth: None,
        request: &TEST_REQUEST,
        pagination: None,
        response: &TEST_RESPONSE,
    };
    const ACTION_WITH_AUTH: ActionDescriptor = ActionDescriptor {
        identifier: "connector.test.items.auth_get",
        endpoint: &TEST_ENDPOINT,
        auth: Some(&TEST_AUTH),
        request: &TEST_REQUEST,
        pagination: None,
        response: &TEST_RESPONSE,
    };

    #[derive(Clone, Default)]
    struct SuccessfulRuntime;

    #[async_trait]
    impl ConnectorRuntime for SuccessfulRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            _request: &mut HttpRequest,
        ) -> Result<(), HostConnectorRuntimeError> {
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, HostConnectorRuntimeError> {
            Ok(ResolvedEndpointProfile {
                base_url: profile.base_url.to_string(),
                default_headers: profile
                    .default_headers
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
            })
        }
    }

    struct EndpointFailureRuntime;

    #[async_trait]
    impl ConnectorRuntime for EndpointFailureRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            _request: &mut HttpRequest,
        ) -> Result<(), HostConnectorRuntimeError> {
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, HostConnectorRuntimeError> {
            Err(HostConnectorRuntimeError::InvalidEndpointProfile {
                role_name: profile.name,
                reason: "endpoint lookup failed".to_string(),
            })
        }
    }

    struct AuthFailureRuntime;

    #[async_trait]
    impl ConnectorRuntime for AuthFailureRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &OutboundAuthProfileDescriptor,
            _request: &mut HttpRequest,
        ) -> Result<(), HostConnectorRuntimeError> {
            Err(HostConnectorRuntimeError::MissingAuthOverride {
                role_name: profile.name,
                env_var: profile.env_var,
            })
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, HostConnectorRuntimeError> {
            Ok(ResolvedEndpointProfile {
                base_url: profile.base_url.to_string(),
                default_headers: profile
                    .default_headers
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
            })
        }
    }

    fn scoped_resources() -> Arc<ResourceBag> {
        Arc::new(
            ResourceBag::new()
                .with_connector_runtime(Arc::new(SuccessfulRuntime))
                .with_connector_scope(ConnectorBindingScope::new(
                    TEST_SCOPE,
                    "node-a",
                    ACTION_NO_AUTH.identifier,
                    "connector.test",
                )),
        )
    }

    #[tokio::test]
    async fn missing_connector_runtime_fails_before_endpoint_resolution() {
        let bag = Arc::new(
            ResourceBag::new().with_connector_scope(ConnectorBindingScope::new(
                TEST_SCOPE,
                "node-a",
                ACTION_NO_AUTH.identifier,
                "connector.test",
            )),
        );

        let err = context::with_resources(bag, async {
            run_action_from_current::<_, JsonValue>(
                &TestInput {
                    value: "demo".to_string(),
                },
                &ACTION_NO_AUTH,
            )
            .await
            .expect_err("missing runtime should fail")
        })
        .await;

        assert!(matches!(
            err,
            ConnectorRuntimeError::MissingConnectorRuntime { action }
                if action == ACTION_NO_AUTH.identifier
        ));
    }

    #[tokio::test]
    async fn missing_connector_scope_fails_before_endpoint_resolution() {
        let bag = Arc::new(ResourceBag::new().with_connector_runtime(Arc::new(SuccessfulRuntime)));

        let err = context::with_resources(bag, async {
            run_action_from_current::<_, JsonValue>(
                &TestInput {
                    value: "demo".to_string(),
                },
                &ACTION_NO_AUTH,
            )
            .await
            .expect_err("missing scope should fail")
        })
        .await;

        assert!(matches!(
            err,
            ConnectorRuntimeError::MissingConnectorScope { action }
                if action == ACTION_NO_AUTH.identifier
        ));
    }

    #[tokio::test]
    async fn endpoint_resolution_failure_propagates_clearly() {
        let bag = Arc::new(
            ResourceBag::new()
                .with_connector_runtime(Arc::new(EndpointFailureRuntime))
                .with_connector_scope(ConnectorBindingScope::new(
                    TEST_SCOPE,
                    "node-a",
                    ACTION_NO_AUTH.identifier,
                    "connector.test",
                )),
        );

        let err = context::with_resources(bag, async {
            run_action_from_current::<_, JsonValue>(
                &TestInput {
                    value: "demo".to_string(),
                },
                &ACTION_NO_AUTH,
            )
            .await
            .expect_err("endpoint resolution failure should propagate")
        })
        .await;

        assert!(matches!(
            err,
            ConnectorRuntimeError::ConnectorRuntime(
                HostConnectorRuntimeError::InvalidEndpointProfile { role_name, .. }
            ) if role_name == TEST_ENDPOINT.name
        ));
    }

    #[tokio::test]
    async fn auth_application_failure_propagates_clearly() {
        let bag = Arc::new(
            ResourceBag::new()
                .with_connector_runtime(Arc::new(AuthFailureRuntime))
                .with_connector_scope(ConnectorBindingScope::new(
                    TEST_SCOPE,
                    "node-a",
                    ACTION_WITH_AUTH.identifier,
                    "connector.test",
                )),
        );

        let err = context::with_resources(bag, async {
            run_action_from_current::<_, JsonValue>(
                &TestInput {
                    value: "demo".to_string(),
                },
                &ACTION_WITH_AUTH,
            )
            .await
            .expect_err("auth application failure should propagate")
        })
        .await;

        assert!(matches!(
            err,
            ConnectorRuntimeError::ConnectorRuntime(
                HostConnectorRuntimeError::MissingAuthOverride { role_name, .. }
            ) if role_name == TEST_AUTH.name
        ));
    }

    #[tokio::test]
    async fn connector_execution_context_collects_runtime_and_scope_once() {
        let context = context::with_resources(scoped_resources(), async {
            connector_execution_context(&ACTION_NO_AUTH)
                .await
                .expect("connector context resolves")
        })
        .await;

        assert_eq!(context.scope.flow_id, TEST_SCOPE);
        assert_eq!(context.scope.connector_id, "connector.test");
    }
}
