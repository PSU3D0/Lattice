use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};
use connector_google_platform::gmail::GOOGLE_GMAIL_BASE_URL;

pub const GOOGLE_WORKSPACE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
pub const GOOGLE_GMAIL_DEFAULT_ENDPOINT_ENV: &str =
    "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL";

pub const GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.google.gmail",
        name: "google_workspace_auth",
        env_var: GOOGLE_WORKSPACE_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const GOOGLE_GMAIL_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor =
    EndpointProfileDescriptor {
        connector_id: "connector.google.gmail",
        name: "google_gmail_default",
        env_base_url_var: GOOGLE_GMAIL_DEFAULT_ENDPOINT_ENV,
        base_url: GOOGLE_GMAIL_BASE_URL,
        default_headers: &[("Accept", "application/json")],
    };
