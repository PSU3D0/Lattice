//! `connector.http` — the generic HTTP request connector (spec
//! `impl-docs/spec/http-request-node.md`). Method is static (one op per
//! method), origin binds at lock time (Tier 0) or arrives as data on the
//! `*_any_origin` variants (Tier 2), and path/query/headers/body are runtime
//! data. Effect metadata is honest by construction: GET/HEAD are ReadOnly +
//! `http_read`, the rest are Effectful + `http_write`.

pub mod actions;
pub mod ext;
pub mod form;
pub mod generated;
pub mod ops;
pub mod runtime;

pub use actions::*;
pub use generated::manifest::*;
pub use generated::profiles::*;
#[cfg(feature = "host-bundle")]
pub use generated::register::register_all;
pub use generated::types::*;

// Byte-plane types re-exported at the crate root so `form!` and node inputs can
// name them as `connector_http::{Artifact, ByteSource}` (spec §16.1).
pub use capabilities::{Artifact, ByteSource};

pub const CONNECTOR_FAMILY: &str = "connector.http";
