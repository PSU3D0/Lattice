use crate::{BrokerError, artifacts::SignatureEnvelope, canonical, signing::BrokerVerifyingKey};

public_type!(LegacyCommitmentV1, LegacyCommitmentTag, "LegacyCommitment");
public_type!(LegacyGrantItemV1, LegacyGrantItemTag, "LegacyGrantItem");
public_type!(
    LegacyBindingItemV1,
    LegacyBindingItemTag,
    "LegacyBindingItem"
);
public_type!(
    LegacyAdmissionInventoryV2,
    LegacyAdmissionInventoryTag,
    "LegacyAdmissionInventory"
);
public_type!(
    LegacyInventoryDecisionV2,
    LegacyInventoryDecisionTag,
    "LegacyInventoryDecision"
);
public_type!(
    LegacyCommitmentEnvelopeV1,
    LegacyCommitmentEnvelopeTag,
    "LegacyCommitmentEnvelopeV1"
);
public_type!(
    HistoricalVerificationKeyArchiveV2,
    HistoricalArchiveTag,
    "HistoricalVerificationKeyArchive"
);
public_type!(
    HistoricalKeyValidityEvidenceV2,
    HistoricalValidityTag,
    "HistoricalKeyValidityEvidence"
);
public_type!(
    HistoricalKeyRevocationEvidenceV2,
    HistoricalRevocationTag,
    "HistoricalKeyRevocationEvidence"
);

pub fn verify_historical_archive(
    archive: &crate::credential::ParsedV2<HistoricalVerificationKeyArchiveV2>,
    archive_authority_key: &BrokerVerifyingKey,
    evidence_authority_key: &BrokerVerifyingKey,
) -> Result<(), BrokerError> {
    crate::credential::signing::verify_signed(archive, archive_authority_key)?;
    HistoricalV1ReceiptVerifier::new(&archive.view)?;
    let value = archive.view.as_value();
    for (field, domain) in [
        (
            "validity_evidence",
            crate::credential::signing::HISTORICAL_KEY_VALIDITY_DOMAIN,
        ),
        (
            "revocation_evidence",
            crate::credential::signing::HISTORICAL_KEY_REVOCATION_DOMAIN,
        ),
    ] {
        let evidence = value.get(field).ok_or(BrokerError::Brk109)?;
        let bytes = canonical::from_serde(evidence, 1024 * 1024)?;
        let signature: SignatureEnvelope = serde_json::from_value(
            evidence
                .get("signature")
                .cloned()
                .ok_or(BrokerError::Brk109)?,
        )
        .map_err(|_| BrokerError::Brk109)?;
        evidence_authority_key.verify_json(domain, bytes.as_bytes(), &signature)?;
    }
    Ok(())
}

pub struct ExecutableV1Admission<'a> {
    pub inventory: &'a LegacyAdmissionInventoryV2,
    pub decision: &'a LegacyInventoryDecisionV2,
}

impl ExecutableV1Admission<'_> {
    pub fn admits_hash(&self, inventory_hash: &str) -> Result<(), BrokerError> {
        let decision = self.decision.as_value();
        if decision.get("status").and_then(|v| v.as_str()) != Some("approved")
            || decision.get("inventory_hash").and_then(|v| v.as_str()) != Some(inventory_hash)
            || decision.get("inventory_ref") != self.inventory.as_value().get("inventory_ref")
        {
            return Err(BrokerError::Brk106);
        }
        Ok(())
    }
}

/// Historical verification evidence is intentionally not executable authority.
pub struct HistoricalV1ReceiptVerifier<'a> {
    archive: &'a HistoricalVerificationKeyArchiveV2,
}

impl<'a> HistoricalV1ReceiptVerifier<'a> {
    pub fn new(archive: &'a HistoricalVerificationKeyArchiveV2) -> Result<Self, BrokerError> {
        let value = archive.as_value();
        let validity = value.get("validity_evidence").ok_or(BrokerError::Brk109)?;
        for field in [
            "issuer",
            "key_id",
            "algorithm",
            "public_key_encoding",
            "public_key_base64url",
            "valid_from",
            "valid_until",
        ] {
            if value.get(field) != validity.get(field) {
                return Err(BrokerError::Brk109);
            }
        }
        Ok(Self { archive })
    }

    pub fn archive(&self) -> &HistoricalVerificationKeyArchiveV2 {
        self.archive
    }
}
