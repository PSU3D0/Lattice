use broker_core::{BrokerError, grant};
use serde::Deserialize;

use crate::BrokerHostError;

/// Host-owned execution identity. The constructor is a trust-boundary API:
/// callers must populate it only from authenticated deployment and scheduler
/// state, never from node arguments, graph JSON, or connector input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedHostScope {
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
    #[allow(clippy::too_many_arguments)]
    pub fn from_host_execution_identity(
        org_id: impl Into<String>,
        principal_id: impl Into<String>,
        bundle_id: impl Into<String>,
        flow_ir_hash: impl Into<String>,
        binding_lock_hash: impl Into<String>,
        flow_id: impl Into<String>,
        node_id: impl Into<String>,
        node_alias: impl Into<String>,
        run_id: impl Into<String>,
        activation_ordinal: u64,
    ) -> Result<Self, BrokerHostError> {
        let value = Self {
            org_id: org_id.into(),
            principal_id: principal_id.into(),
            bundle_id: bundle_id.into(),
            flow_ir_hash: flow_ir_hash.into(),
            binding_lock_hash: binding_lock_hash.into(),
            flow_id: flow_id.into(),
            node_id: node_id.into(),
            node_alias: node_alias.into(),
            run_id: run_id.into(),
            activation_ordinal,
        };
        value.as_core()?;
        Ok(value)
    }

    pub fn activation_ordinal(&self) -> u64 {
        self.activation_ordinal
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub(crate) fn as_core(&self) -> Result<grant::TrustedHostScope, BrokerError> {
        grant::TrustedHostScope::from_authenticated_host(
            &self.org_id,
            &self.principal_id,
            &self.bundle_id,
            &self.flow_ir_hash,
            &self.binding_lock_hash,
            &self.flow_id,
            &self.node_id,
            &self.node_alias,
            &self.run_id,
        )
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
