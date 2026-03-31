//! Dynamic client builder support from rig is intentionally not ported in Phase 2.
//!
//! The `llm-agent` crate keeps the strongly typed `Client` API in `client/mod.rs`.
//! This module remains as a lightweight placeholder so the module tree matches rig's layout.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("dynamic client builder support is not available in llm-agent")]
    Unavailable,
}
