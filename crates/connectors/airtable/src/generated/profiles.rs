use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

use crate::runtime::airtable_api::AIRTABLE_BASE_URL;

pub const AIRTABLE_TOKEN_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_AIRTABLE_TOKEN_AUTH";
pub const AIRTABLE_DEFAULT_ENDPOINT_ENV: &str =
    "LATTICE_CONNECTOR_ENDPOINT_AIRTABLE_DEFAULT_BASE_URL";

pub const AIRTABLE_TOKEN_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.airtable",
        name: "airtable_token_auth",
        env_var: AIRTABLE_TOKEN_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const AIRTABLE_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor =
    EndpointProfileDescriptor {
        connector_id: "connector.airtable",
        name: "airtable_default",
        env_base_url_var: AIRTABLE_DEFAULT_ENDPOINT_ENV,
        base_url: AIRTABLE_BASE_URL,
        default_headers: &[("Accept", "application/json")],
    };
