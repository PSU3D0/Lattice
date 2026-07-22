use crate::BrokerError;
use std::{collections::BTreeMap, sync::Mutex};

public_type!(BudgetsV2, BudgetsTag, "Budgets");
public_type!(GrantScopeV2, GrantScopeTag, "GrantScope");
public_type!(
    GrantDerivationEvidenceV2,
    DerivationEvidenceTag,
    "DerivationEvidence"
);
public_type!(NodeLeaseV2, NodeLeaseTag, "NodeLease");
public_type!(ExecutionGrantV2, ExecutionGrantTag, "ExecutionGrant");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildReservation {
    pub node_lease_ref: String,
    pub logical_effect_id: String,
    pub canonical_input_commitment: String,
    pub remaining_budget: u64,
    pub cas_version: u64,
}

pub trait ChildDerivationStore: Send + Sync {
    fn reserve_child(
        &self,
        node_lease_ref: &str,
        logical_effect_id: &str,
        canonical_input_commitment: &str,
        expected_cas_version: u64,
    ) -> Result<ChildReservation, BrokerError>;
}

#[derive(Default)]
pub struct InMemoryChildDerivationStore {
    leases: Mutex<BTreeMap<String, LeaseState>>,
}

#[derive(Default)]
struct LeaseState {
    remaining: u64,
    cas_version: u64,
    children: BTreeMap<String, ChildReservation>,
}

impl InMemoryChildDerivationStore {
    pub fn insert_lease(
        &self,
        node_lease_ref: impl Into<String>,
        budget: u64,
    ) -> Result<(), BrokerError> {
        if budget == 0 {
            return Err(BrokerError::Brk201);
        }
        let mut leases = self.leases.lock().map_err(|_| BrokerError::Brk401)?;
        if leases
            .insert(
                node_lease_ref.into(),
                LeaseState {
                    remaining: budget,
                    cas_version: 0,
                    children: BTreeMap::new(),
                },
            )
            .is_some()
        {
            return Err(BrokerError::Brk204);
        }
        Ok(())
    }
}

impl ChildDerivationStore for InMemoryChildDerivationStore {
    fn reserve_child(
        &self,
        node_lease_ref: &str,
        logical_effect_id: &str,
        canonical_input_commitment: &str,
        expected_cas_version: u64,
    ) -> Result<ChildReservation, BrokerError> {
        let mut leases = self.leases.lock().map_err(|_| BrokerError::Brk401)?;
        let lease = leases.get_mut(node_lease_ref).ok_or(BrokerError::Brk103)?;
        if let Some(existing) = lease.children.get(logical_effect_id) {
            return if existing.canonical_input_commitment == canonical_input_commitment {
                Ok(existing.clone())
            } else {
                Err(BrokerError::Brk203)
            };
        }
        if lease.cas_version != expected_cas_version {
            return Err(BrokerError::Brk204);
        }
        lease.remaining = lease.remaining.checked_sub(1).ok_or(BrokerError::Brk201)?;
        lease.cas_version = lease
            .cas_version
            .checked_add(1)
            .ok_or(BrokerError::Brk401)?;
        let reservation = ChildReservation {
            node_lease_ref: node_lease_ref.to_owned(),
            logical_effect_id: logical_effect_id.to_owned(),
            canonical_input_commitment: canonical_input_commitment.to_owned(),
            remaining_budget: lease.remaining,
            cas_version: lease.cas_version,
        };
        lease
            .children
            .insert(logical_effect_id.to_owned(), reservation.clone());
        Ok(reservation)
    }
}

/// Dispatch accepts only an exact execution grant. A node lease has a distinct
/// Rust type, schema, audience, signature domain, and cannot be passed here.
pub fn verify_canonical_input(
    grant: &ExecutionGrantV2,
    opening: &crate::commitment::DisclosureKey,
    input_json: &[u8],
) -> Result<(), BrokerError> {
    let canonical =
        crate::canonical::canonicalize_bounded(input_json, crate::canonical::MAX_OPERATION_BYTES)?;
    let envelope: crate::artifacts::CommitmentEnvelope = serde_json::from_value(
        grant
            .as_value()
            .get("canonical_input_commitment")
            .cloned()
            .ok_or(BrokerError::Brk109)?,
    )
    .map_err(|_| BrokerError::Brk109)?;
    if opening.opens(&envelope, canonical.as_bytes()) {
        Ok(())
    } else {
        Err(BrokerError::Brk203)
    }
}

pub fn require_invocable(grant: &ExecutionGrantV2) -> Result<(), BrokerError> {
    if grant.as_value().get("audience").and_then(|v| v.as_str()) == Some("broker-execution") {
        Ok(())
    } else {
        Err(BrokerError::Brk107)
    }
}
