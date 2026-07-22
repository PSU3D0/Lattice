use crate::BrokerError;
use std::{collections::BTreeMap, sync::Mutex};

public_type!(RotationRecordV2, RotationRecordTag, "RotationRecord");

const PHASES: [&str; 9] = [
    "prepared",
    "provider_request_recorded",
    "provider_result_observed",
    "new_material_sealed",
    "authority_reconciled",
    "switched",
    "retirement_enqueued",
    "old_material_destroyed",
    "complete",
];

pub fn verify_transition(
    current: &RotationRecordV2,
    next: &RotationRecordV2,
) -> Result<(), BrokerError> {
    let a = current.as_value();
    let b = next.as_value();
    for field in [
        "rotation_ref",
        "old_generation",
        "new_generation",
        "authority_view_hash",
        "expected_authority_epoch",
    ] {
        if a.get(field) != b.get(field) {
            return Err(BrokerError::Brk106);
        }
    }
    let current_phase = a
        .get("phase")
        .and_then(|v| v.as_str())
        .ok_or(BrokerError::Brk109)?;
    let next_phase = b
        .get("phase")
        .and_then(|v| v.as_str())
        .ok_or(BrokerError::Brk109)?;
    let current_index = PHASES
        .iter()
        .position(|p| *p == current_phase)
        .ok_or(BrokerError::Brk004)?;
    if PHASES.get(current_index + 1).copied() != Some(next_phase)
        || b.get("cas_version").and_then(|v| v.as_u64())
            != a.get("cas_version")
                .and_then(|v| v.as_u64())
                .and_then(|n| n.checked_add(1))
    {
        return Err(BrokerError::Brk204);
    }
    Ok(())
}

pub trait RotationCasStore: Send + Sync {
    fn load(&self, rotation_ref: &str) -> Result<RotationRecordV2, BrokerError>;
    fn compare_and_swap(
        &self,
        current: &RotationRecordV2,
        next: RotationRecordV2,
    ) -> Result<(), BrokerError>;
}

#[derive(Default)]
pub struct InMemoryRotationStore(Mutex<BTreeMap<String, RotationRecordV2>>);

impl InMemoryRotationStore {
    pub fn insert_prepared(&self, record: RotationRecordV2) -> Result<(), BrokerError> {
        if record.as_value().get("phase").and_then(|v| v.as_str()) != Some("prepared") {
            return Err(BrokerError::Brk109);
        }
        let key = record
            .as_value()
            .get("rotation_ref")
            .and_then(|v| v.as_str())
            .ok_or(BrokerError::Brk109)?
            .to_owned();
        if self
            .0
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .insert(key, record)
            .is_some()
        {
            return Err(BrokerError::Brk204);
        }
        Ok(())
    }
}

impl RotationCasStore for InMemoryRotationStore {
    fn load(&self, rotation_ref: &str) -> Result<RotationRecordV2, BrokerError> {
        self.0
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .get(rotation_ref)
            .cloned()
            .ok_or(BrokerError::Brk103)
    }

    fn compare_and_swap(
        &self,
        current: &RotationRecordV2,
        next: RotationRecordV2,
    ) -> Result<(), BrokerError> {
        verify_transition(current, &next)?;
        let key = current
            .as_value()
            .get("rotation_ref")
            .and_then(|v| v.as_str())
            .ok_or(BrokerError::Brk109)?;
        let mut records = self.0.lock().map_err(|_| BrokerError::Brk401)?;
        if records.get(key) != Some(current) {
            return Err(BrokerError::Brk204);
        }
        records.insert(key.to_owned(), next);
        Ok(())
    }
}
