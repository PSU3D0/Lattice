use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

pub const SLACK_BASE_URL: &str = "https://slack.com/api";
pub const SLACK_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_SLACK_AUTH";
pub const SLACK_DEFAULT_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_SLACK_DEFAULT_BASE_URL";

pub const SLACK_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {
    connector_id: "connector.slack.core",
    name: "slack_auth",
    env_var: SLACK_AUTH_ENV,
    kind: OutboundAuthKind::Bearer {
        handle_kind: "http.bearer",
    },
};

pub const SLACK_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor = EndpointProfileDescriptor {
    connector_id: "connector.slack.core",
    name: "slack_default",
    env_base_url_var: SLACK_DEFAULT_ENDPOINT_ENV,
    base_url: SLACK_BASE_URL,
    default_headers: &[("Accept", "application/json")],
};
