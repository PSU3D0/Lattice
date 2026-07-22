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
        self.require_v1_authoritative()?;
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

    pub fn put_rotation_journal(
        &mut self,
        org_id: &str,
        source: &[u8],
    ) -> Result<Vec<u8>, BrokerError> {
        self.require_tenant(org_id)?;
        self.require_v1_authoritative()?;
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
        // C2 may prepare state but cannot make V2 authoritative, issue a V2
        // lease, start a V2 rotation, or disable V1 leasing.
        require_pre_use_fence(&next.view)?;
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

    fn require_v1_authoritative(&self) -> Result<(), BrokerError> {
        let fence = parse::<CrossVersionCredentialFenceV2>(&self.fence_json)?;
        require_pre_use_fence(&fence.view)
    }
}

fn require_pre_use_fence(fence: &CrossVersionCredentialFenceV2) -> Result<(), BrokerError> {
    let value = fence.as_value();
    if value.get("phase").and_then(|v| v.as_str()) == Some("v1_authoritative")
        && value.get("v2_lease_ever_issued").and_then(|v| v.as_bool()) == Some(false)
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
