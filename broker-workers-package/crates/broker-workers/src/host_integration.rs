use broker_core::{
    BrokerError, commitment::CommitmentKey, credential::parse, signing::BrokerSigner,
};
use broker_host::{
    DerivedGrantV2, LiveConnectionAuthority, NodeLeaseLimitsV2, NodeLeaseStoreV2, TrustedHostScope,
    VerifiedBindingV2,
};

use crate::credential_state::{CredentialStateV2, PublicRecordSchema};

/// Test-fenced worker integration for C4. Production routes remain V1 until
/// C5; only the explicitly compiled test surface can install V2 leases/grants.
#[cfg(any(test, feature = "test-fixtures"))]
pub struct TestFencedV2Host {
    leases: NodeLeaseStoreV2,
}

#[cfg(any(test, feature = "test-fixtures"))]
impl TestFencedV2Host {
    pub fn new() -> Self {
        Self {
            leases: NodeLeaseStoreV2::default(),
        }
    }

    pub fn install_node_lease(
        &self,
        test_fence: bool,
        state: &mut CredentialStateV2,
        org_id: &str,
        binding: &VerifiedBindingV2,
        scope: &TrustedHostScope,
        limits: NodeLeaseLimitsV2,
        signer: &BrokerSigner,
    ) -> Result<Vec<u8>, BrokerError> {
        require_test_fence(test_fence, state)?;
        let lease = self
            .leases
            .issue(binding, scope, limits, signer)
            .map_err(host_error)?;
        let canonical = lease.canonical_bytes().to_vec();
        let reference = lease
            .view
            .as_value()
            .get("node_lease_ref")
            .and_then(serde_json::Value::as_str)
            .ok_or(BrokerError::Brk109)?
            .to_owned();
        state.put_public(
            org_id,
            &reference,
            PublicRecordSchema::NodeLease,
            &canonical,
        )?;
        Ok(canonical)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn derive_exact_grant(
        &self,
        test_fence: bool,
        state: &mut CredentialStateV2,
        org_id: &str,
        node_lease_ref: &str,
        scope: &TrustedHostScope,
        semantic_effect_slot: &str,
        canonical_input: &[u8],
        now: &str,
        expected_cas_version: u64,
        pop_proof: &[u8],
        expected_pop_proof: &[u8],
        live: &dyn LiveConnectionAuthority,
        commitments: &CommitmentKey,
    ) -> Result<DerivedGrantV2, BrokerError> {
        require_test_fence(test_fence, state)?;
        let derived = self
            .leases
            .derive_child(
                node_lease_ref,
                scope,
                semantic_effect_slot,
                canonical_input,
                now,
                expected_cas_version,
                pop_proof,
                expected_pop_proof,
                live,
                commitments,
            )
            .map_err(host_error)?;
        state.put_public(
            org_id,
            derived.grant_ref.as_str(),
            PublicRecordSchema::ExecutionGrant,
            &derived.canonical_grant,
        )?;
        Ok(derived)
    }
}

#[cfg(any(test, feature = "test-fixtures"))]
impl Default for TestFencedV2Host {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-fixtures"))]
fn require_test_fence(test_fence: bool, state: &CredentialStateV2) -> Result<(), BrokerError> {
    if !test_fence {
        return Err(BrokerError::Brk107);
    }
    let fence = parse::<broker_core::credential::CrossVersionCredentialFenceV2>(&state.fence_json)?;
    let value = fence.view.as_value();
    if value.get("phase").and_then(serde_json::Value::as_str) != Some("v2_authoritative")
        || value
            .get("v1_leasing_disabled")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || value
            .get("v2_lease_ever_issued")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return Err(BrokerError::Brk106);
    }
    Ok(())
}

#[cfg(any(test, feature = "test-fixtures"))]
fn host_error(error: broker_host::BrokerHostError) -> BrokerError {
    match error {
        broker_host::BrokerHostError::Broker(error) => error,
        broker_host::BrokerHostError::V2AuthorityDrift => BrokerError::Brk106,
        _ => BrokerError::Brk109,
    }
}
