use std::collections::BTreeMap;

use broker_core::{
    BrokerError,
    credential::{
        CrossVersionCredentialFenceV2, connection::verify_fence_successor, parse, rotation,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_SEALED_BYTES: usize = 1024 * 1024;
const MAX_PUBLIC_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedMaterialGeneration {
    pub generation: u64,
    pub envelope_hash: String,
    pub sealed_envelope: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicV2Record {
    pub schema: PublicRecordSchema,
    pub content_hash: String,
    pub canonical_json: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicRecordSchema {
    AuthProfileDescriptor,
    ConnectionAuthorityView,
    ConnectionSnapshot,
    StandingAuthority,
    ContractSet,
    PolicyInstance,
    NodeLease,
    ExecutionGrant,
    BindingAttestation,
    InvocationReceipt,
    LegacyAdmissionInventory,
    LegacyInventoryDecision,
    HistoricalVerificationKeyArchive,
}

impl PublicRecordSchema {
    fn validate(self, bytes: &[u8]) -> Result<(Vec<u8>, String), BrokerError> {
        macro_rules! parse_public {
            ($ty:ty) => {{
                let parsed = parse::<$ty>(bytes)?;
                (parsed.canonical_bytes().to_vec(), parsed.content_hash())
            }};
        }
        Ok(match self {
            Self::AuthProfileDescriptor => {
                parse_public!(broker_core::credential::profile::AuthProfileDescriptorV2)
            }
            Self::ConnectionAuthorityView => {
                parse_public!(broker_core::credential::connection::ConnectionAuthorityViewV2)
            }
            Self::ConnectionSnapshot => {
                parse_public!(broker_core::credential::connection::ConnectionSnapshotV2)
            }
            Self::StandingAuthority => {
                parse_public!(broker_core::credential::StandingAuthorityV2)
            }
            Self::ContractSet => parse_public!(broker_core::credential::ContractSetV2),
            Self::PolicyInstance => {
                parse_public!(broker_core::credential::policy::PolicyInstanceV2)
            }
            Self::NodeLease => parse_public!(broker_core::credential::grant::NodeLeaseV2),
            Self::ExecutionGrant => {
                parse_public!(broker_core::credential::grant::ExecutionGrantV2)
            }
            Self::BindingAttestation => {
                parse_public!(broker_core::credential::receipt::BindingAttestationV2)
            }
            Self::InvocationReceipt => {
                parse_public!(broker_core::credential::receipt::InvocationReceiptV2)
            }
            Self::LegacyAdmissionInventory => {
                parse_public!(broker_core::credential::legacy::LegacyAdmissionInventoryV2)
            }
            Self::LegacyInventoryDecision => {
                parse_public!(broker_core::credential::legacy::LegacyInventoryDecisionV2)
            }
            Self::HistoricalVerificationKeyArchive => {
                parse_public!(broker_core::credential::legacy::HistoricalVerificationKeyArchiveV2)
            }
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialStateV2 {
    pub org_id: String,
    pub connection_ref: String,
    pub fence_json: Vec<u8>,
    pub public_records: BTreeMap<String, PublicV2Record>,
    pub sealed_material: BTreeMap<u64, SealedMaterialGeneration>,
    pub rotation_journal_json: Option<Vec<u8>>,
    #[serde(default)]
    pub destroyed_material_generations: Vec<u64>,
    #[serde(default)]
    pub revocation_evidence_hash: Option<String>,
    #[serde(default)]
    pub revocation_fence_hash: Option<String>,
    #[serde(default)]
    pub remote_custodian_binding_hash: Option<String>,
}

impl CredentialStateV2 {
    pub fn initialize(
        org_id: impl Into<String>,
        connection_ref: impl Into<String>,
        fence_json: &[u8],
    ) -> Result<Self, BrokerError> {
        let org_id = org_id.into();
        let connection_ref = connection_ref.into();
        if org_id.is_empty() || connection_ref.is_empty() {
            return Err(BrokerError::Brk101);
        }
        let fence = parse::<CrossVersionCredentialFenceV2>(fence_json)?;
        require_pre_use_fence(&fence.view)?;
        Ok(Self {
            org_id,
            connection_ref,
            fence_json: fence.canonical_bytes().to_vec(),
            public_records: BTreeMap::new(),
            sealed_material: BTreeMap::new(),
            rotation_journal_json: None,
            destroyed_material_generations: vec![],
            revocation_evidence_hash: None,
            revocation_fence_hash: None,
            remote_custodian_binding_hash: None,
        })
    }

    pub fn put_public(
        &mut self,
        org_id: &str,
        record_ref: &str,
        schema: PublicRecordSchema,
        source: &[u8],
    ) -> Result<&PublicV2Record, BrokerError> {
        self.require_tenant(org_id)?;
        if record_ref.is_empty() || source.len() > MAX_PUBLIC_BYTES {
            return Err(BrokerError::Brk001);
        }
        let (canonical_json, content_hash) = schema.validate(source)?;
        let candidate = PublicV2Record {
            schema,
            content_hash,
            canonical_json,
        };
        if let Some(existing) = self.public_records.get(record_ref) {
            if existing != &candidate {
                return Err(BrokerError::Brk203);
            }
        } else {
            self.public_records
                .insert(record_ref.to_string(), candidate);
        }
        Ok(self.public_records.get(record_ref).expect("stored record"))
    }

    pub fn seal_material(
        &mut self,
        org_id: &str,
        generation: u64,
        sealed_envelope: Vec<u8>,
    ) -> Result<&SealedMaterialGeneration, BrokerError> {
        self.require_tenant(org_id)?;
        self.require_material_write_allowed()?;
        if generation == 0 || sealed_envelope.is_empty() || sealed_envelope.len() > MAX_SEALED_BYTES
        {
            return Err(BrokerError::Brk001);
        }
        let envelope_hash = format!("sha256:{}", hex::encode(Sha256::digest(&sealed_envelope)));
        let candidate = SealedMaterialGeneration {
            generation,
            envelope_hash,
            sealed_envelope,
        };
        if let Some(existing) = self.sealed_material.get(&generation) {
            if existing != &candidate {
                return Err(BrokerError::Brk203);
            }
        } else {
            if self
                .sealed_material
                .keys()
                .next_back()
                .is_some_and(|last| generation <= *last)
            {
                return Err(BrokerError::Brk106);
            }
            self.sealed_material.insert(generation, candidate);
        }
        Ok(self
            .sealed_material
            .get(&generation)
            .expect("stored material"))
    }

    pub fn lease_material_for_dispatch(
        &mut self,
        org_id: &str,
        generation: u64,
    ) -> Result<Vec<u8>, BrokerError> {
        self.require_tenant(org_id)?;
        if self.revocation_fence_hash.is_some()
            || self.revocation_evidence_hash.is_some()
            || self.remote_custodian_binding_hash.is_some()
            || self.destroyed_material_generations.contains(&generation)
        {
            return Err(BrokerError::Brk106);
        }
        let current = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        let current_value = current.view.as_value();
        if current_value
            .get("phase")
            .and_then(serde_json::Value::as_str)
            != Some("v2_authoritative")
            || current_value
                .get("v1_leasing_disabled")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || current_value
                .get("active_v2_generation")
                .and_then(serde_json::Value::as_u64)
                != Some(generation)
        {
            return Err(BrokerError::Brk106);
        }
        let sealed = self
            .sealed_material
            .get(&generation)
            .ok_or(BrokerError::Brk103)?
            .clone();
        if format!(
            "sha256:{}",
            hex::encode(Sha256::digest(&sealed.sealed_envelope))
        ) != sealed.envelope_hash
        {
            return Err(BrokerError::Brk106);
        }
        if current_value
            .get("v2_lease_ever_issued")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            let mut next = current_value.clone();
            next["v2_lease_ever_issued"] = true.into();
            next["cas_version"] = current_value
                .get("cas_version")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| value.checked_add(1))
                .ok_or(BrokerError::Brk401)?
                .into();
            let canonical = broker_core::canonical::from_serde(&next, 64 * 1024)?;
            let next = parse::<CrossVersionCredentialFenceV2>(canonical.as_bytes())?;
            verify_fence_successor(&current.view, &next.view)?;
            self.verify_authoritative_cutover(next.view.as_value())?;
            self.fence_json = next.canonical_bytes().to_vec();
        }
        Ok(sealed.sealed_envelope)
    }

    pub fn bind_remote_custodian(
        &mut self,
        org_id: &str,
        binding_hash: &str,
    ) -> Result<(), BrokerError> {
        self.require_tenant(org_id)?;
        self.require_material_write_allowed()?;
        if !binding_hash.starts_with("sha256:") || binding_hash.len() != 71 {
            return Err(BrokerError::Brk001);
        }
        if !self.sealed_material.is_empty() {
            return Err(BrokerError::Brk106);
        }
        match &self.remote_custodian_binding_hash {
            Some(existing) if existing == binding_hash => Ok(()),
            Some(_) => Err(BrokerError::Brk203),
            None => {
                self.remote_custodian_binding_hash = Some(binding_hash.to_owned());
                Ok(())
            }
        }
    }

    pub fn begin_revocation(&mut self, org_id: &str, fence_hash: &str) -> Result<(), BrokerError> {
        self.require_tenant(org_id)?;
        if !fence_hash.starts_with("sha256:") || fence_hash.len() != 71 {
            return Err(BrokerError::Brk001);
        }
        match &self.revocation_fence_hash {
            Some(existing) if existing == fence_hash => Ok(()),
            Some(_) => Err(BrokerError::Brk203),
            None => {
                self.revocation_fence_hash = Some(fence_hash.to_owned());
                Ok(())
            }
        }
    }

    pub fn material_for_revocation(
        &self,
        org_id: &str,
        generation: u64,
    ) -> Result<Vec<u8>, BrokerError> {
        self.require_tenant(org_id)?;
        if self.revocation_evidence_hash.is_some() {
            return Err(BrokerError::Brk203);
        }
        let fence = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        let phase = fence.view.as_value()["phase"].as_str();
        if !matches!(phase, Some("v2_prepared" | "v2_authoritative"))
            || (phase == Some("v2_authoritative")
                && fence.view.as_value()["active_v2_generation"] != generation)
        {
            return Err(BrokerError::Brk106);
        }
        self.sealed_material
            .get(&generation)
            .map(|value| value.sealed_envelope.clone())
            .ok_or(BrokerError::Brk103)
    }

    pub fn destroy_material(
        &mut self,
        org_id: &str,
        expected_generation: u64,
        evidence_hash: &str,
    ) -> Result<(), BrokerError> {
        self.require_tenant(org_id)?;
        if !evidence_hash.starts_with("sha256:") || evidence_hash.len() != 71 {
            return Err(BrokerError::Brk001);
        }
        if let Some(existing) = &self.revocation_evidence_hash {
            return if existing == evidence_hash {
                Ok(())
            } else {
                Err(BrokerError::Brk203)
            };
        }
        let fence = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        let phase = fence.view.as_value()["phase"].as_str();
        if !matches!(phase, Some("v2_prepared" | "v2_authoritative"))
            || (phase == Some("v2_authoritative")
                && fence.view.as_value()["active_v2_generation"] != expected_generation)
            || (!self.sealed_material.contains_key(&expected_generation)
                && self.remote_custodian_binding_hash.is_none())
        {
            return Err(BrokerError::Brk106);
        }
        self.destroyed_material_generations = if self.remote_custodian_binding_hash.is_some() {
            vec![expected_generation]
        } else {
            self.sealed_material.keys().copied().collect()
        };
        self.sealed_material.clear();
        self.revocation_evidence_hash = Some(evidence_hash.to_owned());
        Ok(())
    }

    pub fn put_rotation_journal(
        &mut self,
        org_id: &str,
        source: &[u8],
    ) -> Result<Vec<u8>, BrokerError> {
        self.require_tenant(org_id)?;
        self.require_material_write_allowed()?;
        let next = parse::<rotation::RotationRecordV2>(source)?;
        match &self.rotation_journal_json {
            None => {
                if next.view.as_value().get("phase").and_then(|v| v.as_str()) != Some("prepared") {
                    return Err(BrokerError::Brk109);
                }
            }
            Some(current) => {
                let current = parse::<rotation::RotationRecordV2>(current)?;
                if current.canonical_bytes() == next.canonical_bytes() {
                    return Ok(current.canonical_bytes().to_vec());
                }
                rotation::verify_transition(&current.view, &next.view)?;
            }
        }
        let canonical = next.canonical_bytes().to_vec();
        self.rotation_journal_json = Some(canonical.clone());
        Ok(canonical)
    }

    pub fn advance_fence(&mut self, org_id: &str, source: &[u8]) -> Result<(), BrokerError> {
        self.require_tenant(org_id)?;
        let current = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        let next = parse::<CrossVersionCredentialFenceV2>(source)?;
        verify_fence_successor(&current.view, &next.view)?;
        let value = next.view.as_value();
        match value.get("phase").and_then(serde_json::Value::as_str) {
            Some("v1_authoritative" | "v2_prepared") => require_pre_use_fence(&next.view)?,
            Some("v2_authoritative") => self.verify_authoritative_cutover(value)?,
            _ => return Err(BrokerError::Brk106),
        }
        self.fence_json = next.canonical_bytes().to_vec();
        Ok(())
    }

    fn verify_authoritative_cutover(&self, fence: &serde_json::Value) -> Result<(), BrokerError> {
        let generation = fence
            .get("active_v2_generation")
            .and_then(serde_json::Value::as_u64)
            .ok_or(BrokerError::Brk106)?;
        let local_sealed_valid = self.sealed_material.get(&generation).is_some_and(|sealed| {
            format!(
                "sha256:{}",
                hex::encode(Sha256::digest(&sealed.sealed_envelope))
            ) == sealed.envelope_hash
        });
        let remote_valid =
            self.remote_custodian_binding_hash.is_some() && self.sealed_material.is_empty();
        if (!local_sealed_valid && !remote_valid)
            || fence
                .get("v1_leasing_disabled")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err(BrokerError::Brk106);
        }
        for required in [
            PublicRecordSchema::AuthProfileDescriptor,
            PublicRecordSchema::ConnectionAuthorityView,
            PublicRecordSchema::BindingAttestation,
        ] {
            if !self
                .public_records
                .values()
                .any(|record| record.schema == required)
            {
                return Err(BrokerError::Brk106);
            }
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn seal_rotated_material_for_test(
        &mut self,
        org_id: &str,
        generation: u64,
        sealed_envelope: Vec<u8>,
    ) -> Result<&SealedMaterialGeneration, BrokerError> {
        self.require_tenant(org_id)?;
        let fence = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        if fence
            .view
            .as_value()
            .get("phase")
            .and_then(|value| value.as_str())
            != Some("v2_authoritative")
            || generation == 0
            || sealed_envelope.is_empty()
            || sealed_envelope.len() > MAX_SEALED_BYTES
            || self
                .sealed_material
                .keys()
                .next_back()
                .is_some_and(|last| generation <= *last)
        {
            return Err(BrokerError::Brk106);
        }
        let envelope_hash = format!("sha256:{}", hex::encode(Sha256::digest(&sealed_envelope)));
        self.sealed_material.insert(
            generation,
            SealedMaterialGeneration {
                generation,
                envelope_hash,
                sealed_envelope,
            },
        );
        self.sealed_material
            .get(&generation)
            .ok_or(BrokerError::Brk401)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn material_for_internal_test(
        &self,
        org_id: &str,
        generation: u64,
    ) -> Result<Vec<u8>, BrokerError> {
        self.require_tenant(org_id)?;
        self.sealed_material
            .get(&generation)
            .map(|material| material.sealed_envelope.clone())
            .ok_or(BrokerError::Brk103)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn activate_v2_for_test(&mut self, org_id: &str, source: &[u8]) -> Result<(), BrokerError> {
        self.require_tenant(org_id)?;
        let current = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        require_pre_use_fence(&current.view)?;
        let next = parse::<CrossVersionCredentialFenceV2>(source)?;
        verify_fence_successor(&current.view, &next.view)?;
        let value = next.view.as_value();
        let generation = value
            .get("active_v2_generation")
            .and_then(|value| value.as_u64())
            .ok_or(BrokerError::Brk106)?;
        let sealed = self
            .sealed_material
            .get(&generation)
            .ok_or(BrokerError::Brk106)?;
        let readback_hash = format!(
            "sha256:{}",
            hex::encode(Sha256::digest(&sealed.sealed_envelope))
        );
        if readback_hash != sealed.envelope_hash
            || value.get("phase").and_then(|value| value.as_str()) != Some("v2_authoritative")
            || value
                .get("v1_leasing_disabled")
                .and_then(|value| value.as_bool())
                != Some(true)
            || (value
                .get("v2_lease_ever_issued")
                .and_then(|value| value.as_bool())
                != Some(true)
                && value
                    .get("v2_rotation_ever_started")
                    .and_then(|value| value.as_bool())
                    != Some(true))
        {
            return Err(BrokerError::Brk106);
        }
        self.fence_json = next.canonical_bytes().to_vec();
        Ok(())
    }

    fn require_tenant(&self, org_id: &str) -> Result<(), BrokerError> {
        if org_id == self.org_id {
            Ok(())
        } else {
            Err(BrokerError::Brk107)
        }
    }

    fn require_material_write_allowed(&self) -> Result<(), BrokerError> {
        let fence = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        let value = fence.view.as_value();
        if value.get("phase").and_then(serde_json::Value::as_str) == Some("v2_authoritative")
            && value
                .get("v1_leasing_disabled")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            && value
                .get("active_v2_generation")
                .and_then(serde_json::Value::as_u64)
                .is_some()
        {
            Ok(())
        } else {
            require_pre_use_fence(&fence.view)
        }
    }
}

fn require_pre_use_fence(fence: &CrossVersionCredentialFenceV2) -> Result<(), BrokerError> {
    let value = fence.as_value();
    if matches!(
        value.get("phase").and_then(|v| v.as_str()),
        Some("v1_authoritative" | "v2_prepared")
    ) && value.get("v2_lease_ever_issued").and_then(|v| v.as_bool()) == Some(false)
        && value
            .get("v2_rotation_ever_started")
            .and_then(|v| v.as_bool())
            == Some(false)
        && value.get("v1_leasing_disabled").and_then(|v| v.as_bool()) == Some(false)
        && value
            .get("active_v2_generation")
            .is_some_and(serde_json::Value::is_null)
    {
        Ok(())
    } else {
        Err(BrokerError::Brk106)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence(cas_version: u64, generation: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": "0.2",
            "critical_fields": [],
            "extensions": {},
            "phase": "v1_authoritative",
            "fence_generation": generation,
            "v2_lease_ever_issued": false,
            "v2_rotation_ever_started": false,
            "v1_leasing_disabled": false,
            "active_v2_generation": null,
            "cas_version": cas_version
        }))
        .unwrap()
    }

    #[test]
    fn tenant_idempotency_fence_and_sealed_public_separation() {
        let mut state = CredentialStateV2::initialize("org-a", "connection-a", &fence(0, 0))
            .expect("initial fence");
        let first = state
            .seal_material("org-a", 1, b"opaque-sealed-envelope".to_vec())
            .unwrap()
            .envelope_hash
            .clone();
        assert_eq!(
            state
                .seal_material("org-a", 1, b"opaque-sealed-envelope".to_vec())
                .unwrap()
                .envelope_hash,
            first
        );
        assert_eq!(
            state
                .seal_material("org-a", 1, b"different".to_vec())
                .unwrap_err(),
            BrokerError::Brk203
        );
        assert_eq!(
            state
                .seal_material("org-b", 2, b"opaque".to_vec())
                .unwrap_err(),
            BrokerError::Brk107
        );
        state.advance_fence("org-a", &fence(1, 1)).unwrap();
        assert!(state.public_records.is_empty());
        assert_eq!(state.sealed_material.len(), 1);

        let rollback = fence(2, 0);
        assert_eq!(
            state.advance_fence("org-a", &rollback).unwrap_err(),
            BrokerError::Brk106
        );
        let mut activated: serde_json::Value = serde_json::from_slice(&fence(2, 2)).unwrap();
        activated["v2_lease_ever_issued"] = true.into();
        assert!(matches!(
            state
                .advance_fence("org-a", &serde_json::to_vec(&activated).unwrap())
                .unwrap_err(),
            BrokerError::Brk004 | BrokerError::Brk106
        ));
    }

    #[test]
    fn v2_fence_requires_sealed_readback_and_never_rolls_back_after_use() {
        let mut state =
            CredentialStateV2::initialize("org-a", "connection-a", &fence(0, 0)).unwrap();
        state
            .seal_material("org-a", 1, b"sealed-v2".to_vec())
            .unwrap();
        let mut prepared: serde_json::Value = serde_json::from_slice(&fence(1, 1)).unwrap();
        prepared["phase"] = "v2_prepared".into();
        state
            .advance_fence("org-a", &serde_json::to_vec(&prepared).unwrap())
            .unwrap();
        let mut active = prepared.clone();
        active["phase"] = "v2_authoritative".into();
        active["cas_version"] = 2.into();
        active["v2_lease_ever_issued"] = true.into();
        active["v1_leasing_disabled"] = true.into();
        active["active_v2_generation"] = 1.into();
        state
            .activate_v2_for_test("org-a", &serde_json::to_vec(&active).unwrap())
            .unwrap();
        assert_eq!(
            state.advance_fence("org-a", &fence(3, 2)).unwrap_err(),
            BrokerError::Brk106
        );

        let mut missing =
            CredentialStateV2::initialize("org-a", "connection-b", &fence(0, 0)).unwrap();
        let mut invalid = active;
        invalid["cas_version"] = 1.into();
        assert_eq!(
            missing
                .activate_v2_for_test("org-a", &serde_json::to_vec(&invalid).unwrap())
                .unwrap_err(),
            BrokerError::Brk106
        );
    }

    #[test]
    fn every_rotation_phase_shape_is_accepted_only_in_order() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../impl-docs/spec/credential-plane-protocol-vectors.json"
        ))
        .unwrap();
        let fixtures = vectors["union_fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|fixture| fixture["schema_pointer"] == "/$defs/RotationRecord")
            .collect::<Vec<_>>();
        assert_eq!(fixtures.len(), 9);
        let mut state =
            CredentialStateV2::initialize("org-a", "connection-a", &fence(0, 0)).unwrap();
        for (index, fixture) in fixtures.iter().enumerate() {
            let mut instance = fixture["instance"].clone();
            instance["cas_version"] = (index as u64).into();
            instance["new_generation"] = 1_u64.into();
            state
                .put_rotation_journal("org-a", &serde_json::to_vec(&instance).unwrap())
                .unwrap_or_else(|error| {
                    panic!("phase {} failed: {error:?}", fixture["branch_tag"])
                });
        }
        let current = state.rotation_journal_json.clone().unwrap();
        assert_eq!(
            parse::<rotation::RotationRecordV2>(&current)
                .unwrap()
                .view
                .as_value()["phase"],
            "complete"
        );
    }
}
