#![forbid(unsafe_code)]

pub mod activation;
pub mod admission_authority;
pub mod composition;
pub mod credential_state;
pub mod cutover;
pub mod durable;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod host_integration;
pub mod hpke;
pub mod management;
pub mod operator_bundle;
pub mod protocol;
pub mod refresh;
pub mod registry;

#[cfg(test)]
mod admission_authority_tests;

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(target_arch = "wasm32")]
pub use wasm::{
    AdmissionAuthorityDurableObject, ConnectionRefreshDurableObject, CredentialStateDurableObject,
    V2AuthorityDurableObject,
};

#[cfg(all(target_arch = "wasm32", feature = "test-fixtures"))]
pub use wasm::BrokerLedgerDurableObject;
