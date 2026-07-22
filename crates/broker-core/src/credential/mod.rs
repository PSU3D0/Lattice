mod model;

macro_rules! public_type {
    ($name:ident, $tag:ident, $schema:literal) => {
        pub enum $tag {}
        impl crate::credential::model::ModelTag for $tag {
            const SCHEMA: &'static str = $schema;
        }
        pub type $name = crate::credential::model::PublicModel<$tag>;
    };
}
pub mod commitment;
pub mod connection;
pub mod grant;
pub mod legacy;
pub mod policy;
mod private_codec;
pub mod profile;
pub mod receipt;
pub mod registry;
pub mod rotation;
pub mod signing;
pub mod spi;

pub use model::{ParsedV2, SchemaType, parse};
/// Privileged codecs deliberately have no `Serialize`, `Debug`, or public
/// field access.
///
/// ```compile_fail
/// use broker_core::credential::CredentialLeaseV2;
/// fn leak<T: serde::Serialize>() {}
/// leak::<CredentialLeaseV2>();
/// ```
///
/// ```compile_fail
/// use broker_core::credential::SecretEnvelopeV2;
/// let secret: SecretEnvelopeV2 = todo!();
/// let _ = secret.0;
/// ```
pub use private_codec::{
    CredentialLeaseV2, InMemoryReplayState, PrivateAuthenticatedRequest,
    RemoteAuthorizeDispatchPrivateV2, RemoteDispatchResultPrivateV2, ReplayReservation,
    ReplayState, SecretEnvelopeV2,
};

public_type!(AuthProfileRefV2, AuthProfileRefTag, "AuthProfileRef");
public_type!(RegistryPinV2, RegistryPinTag, "RegistryPin");
public_type!(CommitmentV2, CommitmentTag, "Commitment");
public_type!(
    PrincipalCommitmentV2,
    PrincipalCommitmentTag,
    "PrincipalCommitment"
);
public_type!(SignatureV2, SignatureTag, "Signature");
public_type!(
    DeploymentEndpointSetV2,
    DeploymentEndpointSetTag,
    "DeploymentEndpointSet"
);
public_type!(
    DeploymentPublicConfigV2,
    DeploymentPublicConfigTag,
    "DeploymentPublicConfig"
);
public_type!(CreateActivationV2, CreateActivationTag, "CreateActivation");
public_type!(NextActionV2, NextActionTag, "NextAction");
public_type!(
    PrivateMaterialSubmissionV2,
    PrivateMaterialSubmissionTag,
    "PrivateMaterialSubmission"
);
public_type!(
    StandingAuthorityV2,
    StandingAuthorityTag,
    "StandingAuthority"
);
public_type!(ContractSetV2, ContractSetTag, "ContractSet");
public_type!(
    CallbackCorrelationRecordV2,
    CallbackCorrelationRecordTag,
    "CallbackCorrelationRecord"
);
public_type!(
    CrossVersionCredentialFenceV2,
    CrossVersionCredentialFenceTag,
    "CrossVersionCredentialFence"
);
public_type!(
    CredentialResponsePolicyV2,
    CredentialResponsePolicyTag,
    "CredentialResponsePolicy"
);

#[cfg(test)]
mod tests;
