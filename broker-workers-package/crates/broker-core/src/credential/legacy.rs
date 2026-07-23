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
    let archive_signature: SignatureEnvelope = serde_json::from_value(
        archive
            .view
            .as_value()
            .get("signature")
            .cloned()
            .ok_or(BrokerError::Brk109)?,
    )
    .map_err(|_| BrokerError::Brk109)?;
    if archive
        .view
        .as_value()
        .get("archive_key_id")
        .and_then(|value| value.as_str())
        != Some(archive_signature.key_id.as_str())
    {
        return Err(BrokerError::Brk109);
    }
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
        if evidence
            .get("evidence_key_id")
            .and_then(|value| value.as_str())
            != Some(signature.key_id.as_str())
        {
            return Err(BrokerError::Brk109);
        }
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

    /// Executable V1 admission is exact inventory membership. Historical keys
    /// and archives are deliberately not consulted by this path.
    pub fn admits_exact(
        &self,
        inventory_hash: &str,
        binding_hash: &str,
        grant_ref: &str,
        canonical_grant_hash: &str,
        now: &str,
    ) -> Result<(), BrokerError> {
        self.admits_hash(inventory_hash)?;
        let inventory = self.inventory.as_value();
        let decision = self.decision.as_value();
        if inventory
            .get("expires_at")
            .and_then(|v| v.as_str())
            .is_none_or(|value| value <= now)
            || decision
                .get("effective_at")
                .and_then(|v| v.as_str())
                .is_none_or(|value| value > now)
            || decision
                .get("expires_at")
                .and_then(|v| v.as_str())
                .is_none_or(|value| value <= now)
        {
            return Err(BrokerError::Brk106);
        }
        let binding = inventory
            .get("items")
            .and_then(|v| v.as_array())
            .and_then(|items| {
                items.iter().find(|item| {
                    item.get("binding_hash").and_then(|v| v.as_str()) == Some(binding_hash)
                })
            })
            .ok_or(BrokerError::Brk106)?;
        let grant = binding
            .get("grants")
            .and_then(|v| v.as_array())
            .and_then(|items| {
                items.iter().find(|item| {
                    item.get("grant_ref").and_then(|v| v.as_str()) == Some(grant_ref)
                        && item.get("canonical_grant_hash").and_then(|v| v.as_str())
                            == Some(canonical_grant_hash)
                })
            })
            .ok_or(BrokerError::Brk106)?;
        if binding
            .get("maximum_binding_expiry")
            .and_then(|v| v.as_str())
            .is_none_or(|value| value <= now)
            || grant
                .get("not_before")
                .and_then(|v| v.as_str())
                .is_none_or(|value| value > now)
            || grant
                .get("expires_at")
                .and_then(|v| v.as_str())
                .is_none_or(|value| value <= now)
            || grant
                .get("maximum_expiry")
                .and_then(|v| v.as_str())
                .is_none_or(|value| value <= now)
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
        let revocation = value
            .get("revocation_evidence")
            .ok_or(BrokerError::Brk109)?;
        for field in ["issuer", "key_id"] {
            if value.get(field) != revocation.get(field) {
                return Err(BrokerError::Brk109);
            }
        }
        Ok(Self { archive })
    }

    pub fn archive(&self) -> &HistoricalVerificationKeyArchiveV2 {
        self.archive
    }

    /// Verify a historical V1 receipt for audit only. The return type is the
    /// V1 receipt, never executable V2 authority.
    pub fn verify_receipt(
        &self,
        canonical_receipt: &[u8],
    ) -> Result<crate::artifacts::ParsedArtifact<crate::artifacts::InvocationReceipt>, BrokerError>
    {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        let receipt: crate::artifacts::ParsedArtifact<crate::artifacts::InvocationReceipt> =
            crate::artifacts::parse(canonical_receipt)?;
        if receipt.canonical_bytes() != canonical_receipt {
            return Err(BrokerError::Brk109);
        }
        let archive = self.archive.as_value();
        if receipt.view.issuer
            != archive
                .get("issuer")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
            || receipt.view.broker_key_id
                != archive
                    .get("key_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
            || receipt.view.issued_at.as_str()
                < archive
                    .get("valid_from")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
            || receipt.view.issued_at.as_str()
                >= archive
                    .get("valid_until")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
        {
            return Err(BrokerError::Brk106);
        }
        let revocation = archive
            .get("revocation_evidence")
            .ok_or(BrokerError::Brk109)?;
        match revocation.get("status").and_then(|v| v.as_str()) {
            Some("not_revoked_through")
                if revocation
                    .get("observed_through")
                    .and_then(|v| v.as_str())
                    .is_some_and(|through| through >= receipt.view.issued_at.as_str()) => {}
            Some("revoked")
                if revocation
                    .get("revoked_at")
                    .and_then(|v| v.as_str())
                    .is_some_and(|at| at > receipt.view.issued_at.as_str()) => {}
            _ => return Err(BrokerError::Brk106),
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(
                archive
                    .get("public_key_base64url")
                    .and_then(|v| v.as_str())
                    .ok_or(BrokerError::Brk109)?,
            )
            .map_err(|_| BrokerError::Brk109)?;
        let key_bytes: [u8; 32] = bytes.try_into().map_err(|_| BrokerError::Brk109)?;
        let key = BrokerVerifyingKey::from_bytes(receipt.view.broker_key_id.clone(), key_bytes)?;
        key.verify_json(
            crate::signing::RECEIPT_DOMAIN,
            canonical_receipt,
            &receipt.view.signature,
        )?;
        receipt.view.validate_semantics()?;
        Ok(receipt)
    }
}
