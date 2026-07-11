use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

pub const HUNTER_API_KEY_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_HUNTER_API_KEY_AUTH";
pub const HUNTER_DEFAULT_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HUNTER_DEFAULT_BASE_URL";

/// Public base URL for the Hunter.io REST API. The API key is passed as an
/// `api_key` QUERY parameter (not a header), so this endpoint carries no secret.
pub const HUNTER_BASE_URL: &str = "https://api.hunter.io";

pub const HUNTER_API_KEY_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.hunter",
        name: "hunter_api_key_auth",
        env_var: HUNTER_API_KEY_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const HUNTER_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor = EndpointProfileDescriptor {
    connector_id: "connector.hunter",
    name: "hunter_default",
    env_base_url_var: HUNTER_DEFAULT_ENDPOINT_ENV,
    base_url: HUNTER_BASE_URL,
    default_headers: &[("Accept", "application/json")],
};
