#![forbid(unsafe_code)]

mod binding;
mod executor;
mod manifest;
mod scope;

pub use binding::{BrokerBindingEvidence, VerifiedBinding};
pub use executor::{
    BrokerOperation, BrokerTransport, ConnectorExecutor, ConnectorOutcome, LocalBrokerConfig,
    LocalBrokerExecutor, LocalContractApproval, RemoteBrokerExecutor, RemoteInvokeRequest,
    RemoteInvokeResponse, TestBrokerTransport, UnavailableBrokerTransport,
};
pub use manifest::{
    AuthorityManifestError, ManifestProvenance, NodeOperationMetadata, derive_authority_manifest,
};
pub use scope::{GuestScopeAssertion, TrustedHostScope};

use broker_core::BrokerError;

/// Sanitized host-path failures. Variants intentionally carry no input,
/// provider response, connection display name, or other guest-controlled text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BrokerHostError {
    #[error("broker binding evidence is unavailable")]
    MissingBindingEvidence,
    #[error("broker executor is unavailable")]
    ExecutorUnavailable,
    #[error("guest broker scope assertion does not match trusted host scope")]
    GuestScopeMismatch,
    #[error(transparent)]
    Broker(#[from] BrokerError),
}
