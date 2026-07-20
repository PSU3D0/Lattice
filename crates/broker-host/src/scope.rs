use broker_core::{BrokerError, grant};
use serde::Deserialize;

use crate::BrokerHostError;

/// Authenticated deployment identity admitted by host bootstrap. Node code
/// cannot turn this data into a trusted scope because `HostContext` has no
/// public constructor.
#[derive(Clone, Debug)]
pub struct HostBootstrapIdentity {
    pub org_id: String,
    pub principal_id: String,
    pub bundle_id: String,
    pub flow_ir_hash: String,
    pub binding_lock_hash: String,
    pub flow_id: String,
    pub run_id: String,
}

/// Sealed trusted-host capability. It is created only by
/// [`bootstrap_host_context`] and is intentionally not `Clone`.
pub struct HostContext {
    identity: HostBootstrapIdentity,
    authority: grant::HostAuthority,
}

/// Host bootstrap boundary. Production hosts call this only after deployment
/// and scheduler authentication; flow/node code receives `TrustedHostScope`,
/// never this capability.
pub fn bootstrap_host_context(
    identity: HostBootstrapIdentity,
) -> Result<HostContext, BrokerHostError> {
    if [
        identity.org_id.as_str(),
        identity.principal_id.as_str(),
        identity.bundle_id.as_str(),
        identity.flow_ir_hash.as_str(),
        identity.binding_lock_hash.as_str(),
        identity.flow_id.as_str(),
        identity.run_id.as_str(),
    ]
    .contains(&"")
    {
        return Err(BrokerError::Brk101.into());
    }
    Ok(HostContext {
        identity,
        authority: grant::bootstrap_host_authority(),
    })
}

impl HostContext {
    /// Installs a deployment-owned custodian while this authenticated host
    /// bootstrap capability is still in control. Invocation callers cannot
    /// replace it after executor construction.
    pub fn local_broker_executor<C: broker_core::custodian::CredentialCustodian>(
        &self,
        config: crate::LocalBrokerConfig,
        custodian: C,
        trusted_adapters: crate::TrustedAdapterRegistry,
    ) -> Result<crate::LocalBrokerExecutor<C>, BrokerHostError> {
        crate::executor::LocalBrokerExecutor::new_with_custodian(
            config,
            custodian,
            trusted_adapters,
        )
    }

    pub fn scope_for_activation(
        &self,
        node_id: impl Into<String>,
        node_alias: impl Into<String>,
        activation_ordinal: u64,
    ) -> Result<TrustedHostScope, BrokerHostError> {
        let node_id = node_id.into();
        let node_alias = node_alias.into();
        let core = self.authority.trusted_scope(
            &self.identity.org_id,
            &self.identity.principal_id,
            &self.identity.bundle_id,
            &self.identity.flow_ir_hash,
            &self.identity.binding_lock_hash,
            &self.identity.flow_id,
            &node_id,
            &node_alias,
            &self.identity.run_id,
        )?;
        Ok(TrustedHostScope {
            core,
            org_id: self.identity.org_id.clone(),
            principal_id: self.identity.principal_id.clone(),
            bundle_id: self.identity.bundle_id.clone(),
            flow_ir_hash: self.identity.flow_ir_hash.clone(),
            binding_lock_hash: self.identity.binding_lock_hash.clone(),
            flow_id: self.identity.flow_id.clone(),
            node_id,
            node_alias,
            run_id: self.identity.run_id.clone(),
            activation_ordinal,
        })
    }
}

/// Host-owned execution identity. It can only be minted from `HostContext`.
///
/// ```compile_fail
/// use broker_host::TrustedHostScope;
/// // There is deliberately no public constructor at the invocation boundary.
/// let forged = TrustedHostScope::from_host_execution_identity();
/// ```
#[derive(Clone, Debug)]
pub struct TrustedHostScope {
    core: grant::TrustedHostScope,
    org_id: String,
    principal_id: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    node_id: String,
    node_alias: String,
    run_id: String,
    activation_ordinal: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestScopeAssertion {
    org_id: String,
    principal_id: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    node_id: String,
    node_alias: String,
    run_id: String,
    activation_ordinal: u64,
}

impl TrustedHostScope {
    pub fn activation_ordinal(&self) -> u64 {
        self.activation_ordinal
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub(crate) fn as_core(&self) -> &grant::TrustedHostScope {
        &self.core
    }

    pub(crate) fn matches_receipt(
        &self,
        receipt: &broker_core::artifacts::InvocationReceipt,
    ) -> bool {
        receipt.org_id == self.org_id
            && receipt.bundle_id == self.bundle_id
            && receipt.flow_ir_hash == self.flow_ir_hash
            && receipt.binding_lock_hash == self.binding_lock_hash
            && receipt.flow_id == self.flow_id
            && receipt.run_id == self.run_id
            && receipt.node_id == self.node_id
            && receipt.node_alias == self.node_alias
    }

    /// Ignores ordinary scope-like input fields. Only the reserved explicit
    /// assertion object is compare-and-reject checked; it never supplies scope.
    pub fn compare_guest_payload(&self, canonical_input: &[u8]) -> Result<(), BrokerHostError> {
        let canonical = broker_core::canonical::canonicalize_bounded(
            canonical_input,
            broker_core::canonical::MAX_OPERATION_BYTES,
        )?;
        if canonical.as_bytes() != canonical_input {
            return Err(BrokerError::Brk001.into());
        }
        let payload: serde_json::Value = serde_json::from_slice(canonical_input)
            .map_err(|_| BrokerHostError::Broker(BrokerError::Brk001))?;
        let Some(assertion) = payload
            .as_object()
            .and_then(|object| object.get("_lattice_broker_scope"))
        else {
            return Ok(());
        };
        let assertion: GuestScopeAssertion = serde_json::from_value(assertion.clone())
            .map_err(|_| BrokerHostError::GuestScopeMismatch)?;
        if assertion.org_id != self.org_id
            || assertion.principal_id != self.principal_id
            || assertion.bundle_id != self.bundle_id
            || assertion.flow_ir_hash != self.flow_ir_hash
            || assertion.binding_lock_hash != self.binding_lock_hash
            || assertion.flow_id != self.flow_id
            || assertion.node_id != self.node_id
            || assertion.node_alias != self.node_alias
            || assertion.run_id != self.run_id
            || assertion.activation_ordinal != self.activation_ordinal
        {
            return Err(BrokerHostError::GuestScopeMismatch);
        }
        Ok(())
    }
}
