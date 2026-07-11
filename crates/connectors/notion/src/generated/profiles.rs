use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

use crate::runtime::notion_api::NOTION_BASE_URL;

pub const NOTION_API_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_NOTION_API_AUTH";
pub const NOTION_DEFAULT_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_NOTION_DEFAULT_BASE_URL";

pub const NOTION_API_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.notion",
        name: "notion_api_auth",
        env_var: NOTION_API_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const NOTION_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor = EndpointProfileDescriptor {
    connector_id: "connector.notion",
    name: "notion_default",
    env_base_url_var: NOTION_DEFAULT_ENDPOINT_ENV,
    base_url: NOTION_BASE_URL,
    default_headers: &[("Accept", "application/json")],
};
