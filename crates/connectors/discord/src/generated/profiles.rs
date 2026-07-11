use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

pub const DISCORD_WEBHOOK_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_DISCORD_WEBHOOK_AUTH";
pub const DISCORD_WEBHOOK_DEFAULT_ENDPOINT_ENV: &str =
    "LATTICE_CONNECTOR_ENDPOINT_DISCORD_WEBHOOK_DEFAULT_BASE_URL";

/// Public origin for Discord webhook execution. The webhook id + token pair is
/// embedded in the request PATH (`/api/webhooks/<id>/<token>`), so this endpoint
/// carries no secret.
pub const DISCORD_WEBHOOK_BASE_URL: &str = "https://discord.com";

pub const DISCORD_WEBHOOK_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.discord",
        name: "discord_webhook_auth",
        env_var: DISCORD_WEBHOOK_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const DISCORD_WEBHOOK_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor =
    EndpointProfileDescriptor {
        connector_id: "connector.discord",
        name: "discord_webhook_default",
        env_base_url_var: DISCORD_WEBHOOK_DEFAULT_ENDPOINT_ENV,
        base_url: DISCORD_WEBHOOK_BASE_URL,
        default_headers: &[("Accept", "application/json")],
    };
