pub mod ext;
pub mod generated;
pub mod runtime;

pub use generated::manifest::*;
pub use generated::profiles::*;
pub use generated::types::*;
pub use generated::actions::*;
pub mod ops {
    pub use crate::generated::ops::*;
}
#[cfg(feature = "host-bundle")]
pub use generated::register::register_all;

pub const CONNECTOR_FAMILY: &str = "connector.github.issues";
pub const GITHUB_ISSUES_CREATE_IDENTIFIER: &str = "connector.github.issues.create";
pub const GITHUB_ISSUES_GET_IDENTIFIER: &str = "connector.github.issues.get";
pub const GITHUB_ISSUES_LIST_IDENTIFIER: &str = "connector.github.issues.list";
