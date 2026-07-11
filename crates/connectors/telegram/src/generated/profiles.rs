use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};

pub const TELEGRAM_BOT_AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_TELEGRAM_BOT_AUTH";
pub const TELEGRAM_BOT_DEFAULT_ENDPOINT_ENV: &str =
    "LATTICE_CONNECTOR_ENDPOINT_TELEGRAM_BOT_DEFAULT_BASE_URL";

/// Public base URL for the Telegram Bot API. The bot token is embedded in the
/// request path (`/bot<token>/<method>`), so this endpoint carries no token.
pub const TELEGRAM_BOT_BASE_URL: &str = "https://api.telegram.org";

pub const TELEGRAM_BOT_AUTH_OUTBOUND_AUTH: OutboundAuthProfileDescriptor =
    OutboundAuthProfileDescriptor {
        connector_id: "connector.telegram",
        name: "telegram_bot_auth",
        env_var: TELEGRAM_BOT_AUTH_ENV,
        kind: OutboundAuthKind::Bearer {
            handle_kind: "http.bearer",
        },
    };

pub const TELEGRAM_BOT_DEFAULT_ENDPOINT_PROFILE: EndpointProfileDescriptor =
    EndpointProfileDescriptor {
        connector_id: "connector.telegram",
        name: "telegram_bot_default",
        env_base_url_var: TELEGRAM_BOT_DEFAULT_ENDPOINT_ENV,
        base_url: TELEGRAM_BOT_BASE_URL,
        default_headers: &[("Accept", "application/json")],
    };
