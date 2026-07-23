#![forbid(unsafe_code)]

/// Private activation material is neither serializable nor openable by callers.
///
/// ```compile_fail
/// use broker_auth::PrivateMaterial;
/// let material = PrivateMaterial::new(b"secret".to_vec()).unwrap();
/// material.expose(|bytes| println!("{bytes:?}"));
/// ```
///
/// ```compile_fail
/// fn require_serialize<T: serde::Serialize>() {}
/// require_serialize::<broker_auth::PrivateMaterial>();
/// ```
pub mod activation;
pub mod custody;
pub mod driver;
pub mod profile;

pub use activation::{
    ActivationEngine, ActivationMaterial, ActivationRequest, DeterministicNonceSource,
    EncryptedSubmission, NextAction, NonceSource, OAuthCallback, PrivateMaterial,
    PublicActivationSnapshot, SubmissionAad, TokenExchangeRequest, TokenService,
    WorkloadExchangeRequest, WorkloadPresentation, encrypt_submission,
};
pub use custody::{
    AuthoritySnapshot, ConnectionStatus, CredentialVault, FencePhase, MaterialMetadata,
    ProviderRotationResult, RotationPhase, RotationRecord,
};
pub use driver::{
    FinalizedUnsignedRequest, FirewallPolicy, Header, ProfileAuthDriver, QueryPair,
    StrictResponseFirewall,
};
pub use profile::{
    ActivationKind, ApprovedRegistry, AuthProfile, AuthScheme, ClaimRelation, NormalizedClaims,
    PublicClaimsPolicy, RegistryPin,
};
