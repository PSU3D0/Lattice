pub mod actions;
pub mod ext;
pub mod generated;
pub mod ops;
pub mod runtime;

pub use actions::*;
pub use generated::manifest::*;
pub use generated::profiles::*;
#[cfg(feature = "host-bundle")]
pub use generated::register::register_all;
pub use generated::types::*;

pub const CONNECTOR_FAMILY: &str = "connector.discord";
pub const DISCORD_SEND_MESSAGE_IDENTIFIER: &str = "connector.discord.send_message";
