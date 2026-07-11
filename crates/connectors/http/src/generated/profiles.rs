use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

pub const HTTP_TARGET_BASE_URL: &str = "https://api.example.invalid";
pub const HTTP_TARGET_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL";
pub const HTTP_TARGET_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_HTTP_TARGET_AUTH";

/// Tier 0 endpoint profile. The `base_url` here is a placeholder — the real
/// origin is bound at lock time by the `endpoint.profile` handle and resolved
/// through the connector runtime (env override on the dev adapter).
pub const HTTP_TARGET_ENDPOINT_PROFILE: EndpointProfileDescriptor = EndpointProfileDescriptor {
    connector_id: "connector.http",
    name: "http_target",
    env_base_url_var: HTTP_TARGET_ENDPOINT_ENV,
    base_url: HTTP_TARGET_BASE_URL,
    default_headers: &[("Accept", "application/json")],
};

// The connector.http outbound-auth role (`http_target_auth`) is OPTIONAL and
// supports the union {Bearer, ApiKeyHeader, ApiKeyQuery, Basic}. The concrete
// kind is lock-time data; these four canonical descriptors let the transport
// apply whichever kind the bound handle carries. All share the same role name
// and dev env var; they differ only in `kind`.

pub const HTTP_TARGET_AUTH_BEARER: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {
    connector_id: "connector.http",
    name: "http_target_auth",
    env_var: HTTP_TARGET_AUTH_ENV,
    kind: OutboundAuthKind::Bearer {
        handle_kind: "http.bearer",
    },
};

pub const HTTP_TARGET_AUTH_API_KEY_HEADER: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.http",
        name: "http_target_auth",
        env_var: HTTP_TARGET_AUTH_ENV,
        kind: OutboundAuthKind::ApiKeyHeader {
            header_name: "X-Api-Key",
            prefix: None,
            handle_kind: "http.api_key",
        },
    };

pub const HTTP_TARGET_AUTH_API_KEY_QUERY: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.http",
        name: "http_target_auth",
        env_var: HTTP_TARGET_AUTH_ENV,
        kind: OutboundAuthKind::ApiKeyQuery {
            query_name: "api_key",
            handle_kind: "http.api_key",
        },
    };

pub const HTTP_TARGET_AUTH_BASIC: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {
    connector_id: "connector.http",
    name: "http_target_auth",
    env_var: HTTP_TARGET_AUTH_ENV,
    kind: OutboundAuthKind::Basic {
        handle_kind: "http.basic",
    },
};
