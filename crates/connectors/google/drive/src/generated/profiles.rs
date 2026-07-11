use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};
use connector_google_platform::drive::GOOGLE_DRIVE_BASE_URL;

pub const GOOGLE_WORKSPACE_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";
pub const GOOGLE_DRIVE_DEFAULT_ENDPOINT_ENV: &str =
    "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_DRIVE_DEFAULT_BASE_URL";

pub const GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.google.drive",
        name: "google_workspace_auth",
        env_var: GOOGLE_WORKSPACE_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const GOOGLE_DRIVE_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor =
    EndpointProfileDescriptor {
        connector_id: "connector.google.drive",
        name: "google_drive_default",
        env_base_url_var: GOOGLE_DRIVE_DEFAULT_ENDPOINT_ENV,
        base_url: GOOGLE_DRIVE_BASE_URL,
        default_headers: &[("Accept", "application/json")],
    };
