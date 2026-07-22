use super::{
    CredentialLeaseV2, CredentialResponsePolicyV2, RegistryPinV2,
    private_codec::PrivateAuthenticatedRequest,
};
use crate::BrokerError;
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct PublicAuthorityProjection(Value);
impl PublicAuthorityProjection {
    pub fn value(&self) -> &Value {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct OpaqueUnauthenticatedPlan(Vec<u8>);
impl OpaqueUnauthenticatedPlan {
    pub fn from_canonical_bytes(bytes: Vec<u8>) -> Result<Self, BrokerError> {
        let canonical =
            crate::canonical::canonicalize_bounded(&bytes, crate::canonical::MAX_OPERATION_BYTES)?;
        Ok(Self(canonical.into_bytes()))
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct ScrubbedProviderResponse(Value);
impl ScrubbedProviderResponse {
    pub fn from_privileged_firewall(value: Value) -> Self {
        Self(value)
    }

    pub fn value(&self) -> &Value {
        &self.0
    }
}

pub struct PrivateCredentialUpdate(Vec<u8>);
impl PrivateCredentialUpdate {
    pub fn from_privileged_bytes(bytes: Vec<u8>) -> Result<Self, BrokerError> {
        if bytes.is_empty() || bytes.len() > 1024 * 1024 {
            return Err(BrokerError::Brk305);
        }
        Ok(Self(bytes))
    }
}
impl Drop for PrivateCredentialUpdate {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.0.zeroize();
    }
}

pub struct BoundedRawResponse(Vec<u8>);
impl BoundedRawResponse {
    pub fn from_privileged_transport(bytes: Vec<u8>) -> Result<Self, BrokerError> {
        if bytes.len() > 1024 * 1024 {
            return Err(BrokerError::Brk305);
        }
        Ok(Self(bytes))
    }
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}
impl Drop for BoundedRawResponse {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.0.zeroize();
    }
}

pub trait CredentialBlindPlanner: Send + Sync {
    type TypedInput;
    fn plan(
        &self,
        input: &Self::TypedInput,
        authority: &PublicAuthorityProjection,
    ) -> Result<OpaqueUnauthenticatedPlan, BrokerError>;
}

pub trait ResponseProjector: Send + Sync {
    type TypedOutput;
    fn project(
        &self,
        response: &ScrubbedProviderResponse,
    ) -> Result<Self::TypedOutput, BrokerError>;
}

pub struct FirewallResult {
    pub scrubbed: ScrubbedProviderResponse,
    private_update: Option<PrivateCredentialUpdate>,
}
impl FirewallResult {
    pub fn new(
        scrubbed: ScrubbedProviderResponse,
        private_update: Option<PrivateCredentialUpdate>,
    ) -> Self {
        Self {
            scrubbed,
            private_update,
        }
    }

    pub fn take_private_update(&mut self) -> Option<PrivateCredentialUpdate> {
        self.private_update.take()
    }
}

pub trait PrivilegedResponseFirewall: Send + Sync {
    fn classify_and_extract(
        &self,
        response: BoundedRawResponse,
        policy: &CredentialResponsePolicyV2,
    ) -> Result<FirewallResult, BrokerError>;
}

pub trait CredentialCustodian: Send + Sync {
    fn authority_view_jcs(&self, connection_ref: &str) -> Result<Vec<u8>, BrokerError>;
    fn lease(
        &self,
        effect_grant_hash: &str,
        dispatch_attempt: u8,
    ) -> Result<CredentialLeaseV2, BrokerError>;
    fn rotate(&self, connection_ref: &str) -> Result<(), BrokerError>;
    fn revoke(&self, connection_ref: &str) -> Result<(), BrokerError>;
    fn destroy(&self, connection_ref: &str) -> Result<(), BrokerError>;
}

pub trait AuthenticatedRequestSink {
    fn seal(&mut self, bytes: Vec<u8>) -> Result<(), BrokerError>;
}

pub struct CredentialMaterial<'a>(&'a str);
impl CredentialMaterial<'_> {
    pub fn base64url(&self) -> &str {
        self.0
    }
}

pub trait TrustedAuthDriver: Send + Sync {
    fn authorize(
        &self,
        plan: &OpaqueUnauthenticatedPlan,
        material: CredentialMaterial<'_>,
        broker_context_jcs: &[u8],
        sink: &mut dyn AuthenticatedRequestSink,
    ) -> Result<(), BrokerError>;
}

pub fn authorize_with_driver(
    driver: &dyn TrustedAuthDriver,
    plan: &OpaqueUnauthenticatedPlan,
    lease: &CredentialLeaseV2,
    broker_context_jcs: &[u8],
) -> Result<PrivateAuthenticatedRequest, BrokerError> {
    struct Sink(Option<PrivateAuthenticatedRequest>);
    impl AuthenticatedRequestSink for Sink {
        fn seal(&mut self, bytes: Vec<u8>) -> Result<(), BrokerError> {
            if self.0.is_some() {
                return Err(BrokerError::Brk305);
            }
            self.0 = Some(PrivateAuthenticatedRequest::new(bytes)?);
            Ok(())
        }
    }
    let mut sink = Sink(None);
    lease.expose(|value| {
        let material = value
            .get("private_material_b64u")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?;
        driver.authorize(
            plan,
            CredentialMaterial(material),
            broker_context_jcs,
            &mut sink,
        )
    })?;
    sink.0.ok_or(BrokerError::Brk305)
}

pub trait BrokerTransport: Send + Sync {
    fn dispatch(
        &self,
        request: PrivateAuthenticatedRequest,
    ) -> Result<BoundedRawResponse, BrokerError>;
    fn registry_pin(&self) -> &RegistryPinV2;
}
