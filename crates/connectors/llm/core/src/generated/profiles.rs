use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

pub const LLM_API_KEY_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_LLM_API_KEY";
pub const LLM_DEFAULT_ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL";

pub const LLM_API_KEY_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.llm",
        name: "llm_api_key",
        env_var: LLM_API_KEY_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const LLM_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor = EndpointProfileDescriptor {
    connector_id: "connector.llm",
    name: "llm_default",
    env_base_url_var: LLM_DEFAULT_ENDPOINT_ENV,
    base_url: "https://api.openai.com/v1",
    default_headers: &[],
};
