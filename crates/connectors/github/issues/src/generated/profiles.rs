use crate::runtime::transport::{EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor};

pub const GITHUB_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor = EndpointProfileDescriptor {
    connector_id: "connector.github.issues",
    name: "github_default",
    env_base_url_var: "LATTICE_CONNECTOR_ENDPOINT_GITHUB_DEFAULT_BASE_URL",
    base_url: "https://api.github.com",
    default_headers: &[
        ("Accept", "application/json"),
        ("X-GitHub-Api-Version", "2022-11-28"),
    ],
};

pub const GITHUB_PAT_OUTBOUND_AUTH: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {
    connector_id: "connector.github.issues",
    name: "github_pat",
    env_var: "LATTICE_CONNECTOR_AUTH_GITHUB_PAT",
    kind: OutboundAuthKind::Bearer { handle_kind: "http.bearer" },
};
