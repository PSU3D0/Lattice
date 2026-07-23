use broker_core::BrokerError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutoverPhase {
    Inventoried,
    MaterialSealed,
    RegistryVerified,
    BindingVerified,
    FenceSwitched,
    LegacyMaterialDestroyed,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutoverEvidence {
    pub material_generation: u64,
    pub sealed_envelope_hash: String,
    pub sealed_readback_hash: String,
    pub profile_descriptor_hash: String,
    pub authority_view_hash: String,
    pub registry_decision_hash: String,
    pub binding_hash: String,
    pub legacy_destruction_confirmation_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutoverEvent {
    pub sequence: u64,
    pub phase: CutoverPhase,
    pub event_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutoverReconciler {
    pub org_id: String,
    pub connection_ref: String,
    pub phase: CutoverPhase,
    pub cas_version: u64,
    pub v2_lease_ever_issued: bool,
    pub v2_rotation_ever_started: bool,
    pub events: Vec<CutoverEvent>,
}

impl CutoverReconciler {
    pub fn new(
        org_id: impl Into<String>,
        connection_ref: impl Into<String>,
    ) -> Result<Self, BrokerError> {
        let org_id = org_id.into();
        let connection_ref = connection_ref.into();
        if org_id.is_empty() || connection_ref.is_empty() {
            return Err(BrokerError::Brk101);
        }
        let mut value = Self {
            org_id,
            connection_ref,
            phase: CutoverPhase::Inventoried,
            cas_version: 0,
            v2_lease_ever_issued: false,
            v2_rotation_ever_started: false,
            events: Vec::new(),
        };
        value.append_event(CutoverPhase::Inventoried)?;
        Ok(value)
    }

    pub fn advance(
        &mut self,
        expected_cas: u64,
        next: CutoverPhase,
        evidence: &CutoverEvidence,
    ) -> Result<&CutoverEvent, BrokerError> {
        if expected_cas != self.cas_version {
            return Err(BrokerError::Brk204);
        }
        if next == self.phase {
            return self.events.last().ok_or(BrokerError::Brk401);
        }
        if next as u8 != self.phase as u8 + 1 {
            return Err(BrokerError::Brk106);
        }
        validate_evidence(next, evidence)?;
        self.phase = next;
        self.cas_version = self.cas_version.checked_add(1).ok_or(BrokerError::Brk401)?;
        self.append_event(next)
    }

    pub fn record_v2_lease(&mut self) -> Result<(), BrokerError> {
        if self.phase < CutoverPhase::FenceSwitched {
            return Err(BrokerError::Brk106);
        }
        self.v2_lease_ever_issued = true;
        Ok(())
    }

    pub fn record_v2_rotation(&mut self) -> Result<(), BrokerError> {
        if self.phase < CutoverPhase::FenceSwitched {
            return Err(BrokerError::Brk106);
        }
        self.v2_rotation_ever_started = true;
        Ok(())
    }

    pub fn rollback_to_v1(&mut self) -> Result<(), BrokerError> {
        if self.phase >= CutoverPhase::FenceSwitched
            || self.v2_lease_ever_issued
            || self.v2_rotation_ever_started
        {
            return Err(BrokerError::Brk106);
        }
        Err(BrokerError::Brk106)
    }

    fn append_event(&mut self, phase: CutoverPhase) -> Result<&CutoverEvent, BrokerError> {
        let sequence = self.events.len() as u64;
        let preimage = format!(
            "lattice.credential-cutover.v0.2\0{}\0{}\0{sequence}\0{phase:?}",
            self.org_id, self.connection_ref
        );
        self.events.push(CutoverEvent {
            sequence,
            phase,
            event_hash: format!(
                "sha256:{}",
                hex::encode(Sha256::digest(preimage.as_bytes()))
            ),
        });
        self.events.last().ok_or(BrokerError::Brk401)
    }
}

fn valid_hash(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_evidence(phase: CutoverPhase, evidence: &CutoverEvidence) -> Result<(), BrokerError> {
    if evidence.material_generation == 0
        || !valid_hash(&evidence.sealed_envelope_hash)
        || evidence.sealed_envelope_hash != evidence.sealed_readback_hash
    {
        return Err(BrokerError::Brk106);
    }
    if phase >= CutoverPhase::RegistryVerified
        && (!valid_hash(&evidence.profile_descriptor_hash)
            || !valid_hash(&evidence.authority_view_hash)
            || !valid_hash(&evidence.registry_decision_hash))
    {
        return Err(BrokerError::Brk106);
    }
    if phase >= CutoverPhase::BindingVerified && !valid_hash(&evidence.binding_hash) {
        return Err(BrokerError::Brk106);
    }
    if phase >= CutoverPhase::LegacyMaterialDestroyed
        && !evidence
            .legacy_destruction_confirmation_hash
            .as_deref()
            .is_some_and(valid_hash)
    {
        return Err(BrokerError::Brk106);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn evidence() -> CutoverEvidence {
        CutoverEvidence {
            material_generation: 1,
            sealed_envelope_hash: hash('a'),
            sealed_readback_hash: hash('a'),
            profile_descriptor_hash: hash('b'),
            authority_view_hash: hash('c'),
            registry_decision_hash: hash('d'),
            binding_hash: hash('e'),
            legacy_destruction_confirmation_hash: Some(hash('f')),
        }
    }

    #[test]
    fn every_phase_is_journaled_and_restart_replay_is_idempotent() {
        let mut state = CutoverReconciler::new("org", "connection").unwrap();
        for phase in [
            CutoverPhase::MaterialSealed,
            CutoverPhase::RegistryVerified,
            CutoverPhase::BindingVerified,
            CutoverPhase::FenceSwitched,
            CutoverPhase::LegacyMaterialDestroyed,
            CutoverPhase::Complete,
        ] {
            let expected = state.cas_version;
            let event = state.advance(expected, phase, &evidence()).unwrap().clone();
            let encoded = serde_json::to_vec(&state).unwrap();
            let mut restarted: CutoverReconciler = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(
                restarted
                    .advance(restarted.cas_version, phase, &evidence())
                    .unwrap(),
                &event
            );
            state = restarted;
        }
        assert_eq!(state.events.len(), 7);
        assert_eq!(state.phase, CutoverPhase::Complete);
    }

    #[test]
    fn fence_waits_for_seal_registry_binding_and_destruction_confirmation() {
        let mut state = CutoverReconciler::new("org", "connection").unwrap();
        let mut invalid = evidence();
        invalid.sealed_readback_hash = hash('9');
        assert_eq!(
            state.advance(0, CutoverPhase::MaterialSealed, &invalid),
            Err(BrokerError::Brk106)
        );
        let evidence = evidence();
        state
            .advance(0, CutoverPhase::MaterialSealed, &evidence)
            .unwrap();
        assert_eq!(
            state.advance(1, CutoverPhase::BindingVerified, &evidence),
            Err(BrokerError::Brk106)
        );
        state
            .advance(1, CutoverPhase::RegistryVerified, &evidence)
            .unwrap();
        state
            .advance(2, CutoverPhase::BindingVerified, &evidence)
            .unwrap();
        state
            .advance(3, CutoverPhase::FenceSwitched, &evidence)
            .unwrap();
        let mut missing = evidence.clone();
        missing.legacy_destruction_confirmation_hash = None;
        assert_eq!(
            state.advance(4, CutoverPhase::LegacyMaterialDestroyed, &missing),
            Err(BrokerError::Brk106)
        );
    }

    #[test]
    fn stale_cas_and_every_post_fence_rollback_fail_closed() {
        let mut state = CutoverReconciler::new("org", "connection").unwrap();
        let evidence = evidence();
        for phase in [
            CutoverPhase::MaterialSealed,
            CutoverPhase::RegistryVerified,
            CutoverPhase::BindingVerified,
            CutoverPhase::FenceSwitched,
        ] {
            state.advance(state.cas_version, phase, &evidence).unwrap();
        }
        assert_eq!(
            state.advance(2, CutoverPhase::LegacyMaterialDestroyed, &evidence),
            Err(BrokerError::Brk204)
        );
        state.record_v2_lease().unwrap();
        state.record_v2_rotation().unwrap();
        assert_eq!(state.rollback_to_v1(), Err(BrokerError::Brk106));
    }
}
