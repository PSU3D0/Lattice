#![forbid(unsafe_code)]

mod adapter;
mod binding;
mod executor;
mod manifest;
mod policy;
mod scope;
mod v2;
mod v2_executor;

pub use adapter::{
    CredentialBlindPlannerImplementation, ResponseFirewallImplementation,
    ResponseProjectorImplementation, TrustedAdapterRegistry,
};
pub use binding::{BindingLockRecord, BrokerBindingEvidence, VerifiedBinding};
pub use executor::{
    BrokerDescriptorRegistry, BrokerOperation, BrokerTransport, ConnectorExecutor,
    ConnectorOutcome, LocalBrokerConfig, LocalBrokerExecutor, ReceiptTrustStore,
    RemoteBrokerExecutor, RemoteInvokeRequest, RemoteInvokeResponse, TestBrokerTransport,
    UnavailableBrokerTransport, descriptor_implementation_facts, descriptor_plan_template,
    descriptor_response_projection,
};
pub use manifest::{
    ApprovedAuthorityContract, AuthorityManifestError, ManifestProvenance, NodeOperationMetadata,
    VerifiedAuthorityManifest, derive_authority_manifest, verify_authority_manifest,
};
pub use policy::{
    DeterministicEqualityEvaluator, PolicyEvaluationInput, PolicyEvaluatorImplementation,
    PolicyEvaluatorRegistry, SYNTHETIC_EQUALITY_PROFILE_REF,
};
pub use scope::{
    GuestScopeAssertion, HostBootstrapIdentity, HostContext, TrustedHostScope,
    bootstrap_host_context,
};
pub use v2::{
    BindingDerivationV2, BindingVerificationLockV2, DerivedGrantV2, ExactGrantRefV2,
    InMemoryLiveConnectionAuthority, LiveConnectionAuthority, NodeLeaseLimitsV2, NodeLeaseStoreV2,
    VerifiedBindingV2, issue_binding_v2, verify_binding_v2,
};
pub use v2_executor::{
    LocalV2ConnectorExecutor, RemoteV2ConnectorExecutor, V2AttemptStage, V2BrokerTransport,
    V2ConnectorOutcome, V2DispatchEvidence, V2ReceiptTrustStore, V2RemoteInvokeRequest,
    V2RemoteInvokeResponse,
};

use broker_core::BrokerError;

/// Sanitized host-path failures. Variants intentionally carry no input,
/// provider response, connection display name, or other guest-controlled text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BrokerHostError {
    #[error("broker binding evidence is unavailable")]
    MissingBindingEvidence,
    #[error("broker executor is unavailable")]
    ExecutorUnavailable,
    #[error("binding lock record is invalid")]
    InvalidBindingLock,
    #[error("binding attestation signature is invalid")]
    BindingSignatureRejected,
    #[error("binding attestation issuer does not match the lock")]
    BindingIssuerMismatch,
    #[error("binding attestation key does not match the lock")]
    BindingKeyMismatch,
    #[error("binding attestation organization does not match the lock")]
    BindingOrgMismatch,
    #[error("binding attestation connection does not match the lock")]
    BindingConnectionMismatch,
    #[error("binding attestation provider does not match the lock")]
    BindingProviderMismatch,
    #[error("binding attestation account does not match the lock")]
    BindingAccountMismatch,
    #[error("binding attestation roles do not match the lock")]
    BindingRolesMismatch,
    #[error("binding attestation scopes do not match the lock")]
    BindingScopesMismatch,
    #[error("binding attestation lane does not match the lock")]
    BindingLaneMismatch,
    #[error("broker descriptor is invalid or does not match the operation")]
    DescriptorMismatch,
    #[error("verified implementation registry rejected the exact entry")]
    RegistryRejected,
    #[error("V2 host derivation evidence is invalid")]
    V2DerivationRejected,
    #[error("live connection authority or material generation drifted")]
    V2AuthorityDrift,
    #[error("broker response projection exceeds the descriptor policy")]
    ResponsePolicyExceeded,
    #[error("remote broker receipt verification failed")]
    ReceiptVerificationFailed,
    #[error("guest broker scope assertion does not match trusted host scope")]
    GuestScopeMismatch,
    #[error(transparent)]
    Broker(#[from] BrokerError),
}
