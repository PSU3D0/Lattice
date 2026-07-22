public_type!(
    RegistryClassPayloadV2,
    RegistryClassPayloadTag,
    "RegistryClassPayload"
);
public_type!(
    RegistryDefinitionV2,
    RegistryDefinitionTag,
    "RegistryDefinition"
);
public_type!(RegistryDecisionV2, RegistryDecisionTag, "RegistryDecision");

impl RegistryDecisionTag {
    pub fn verify_pin(
        pin: &crate::credential::RegistryPinV2,
        decision: &RegistryDecisionV2,
    ) -> Result<(), crate::BrokerError> {
        let pin = pin.as_value();
        let decision = decision.as_value();
        for field in [
            "entry_ref",
            "version",
            "definition_hash",
            "approval_epoch",
            "revocation_epoch",
        ] {
            if pin.get(field) != decision.get(field) {
                return Err(crate::BrokerError::Brk106);
            }
        }
        if decision.get("approval_status").and_then(|v| v.as_str()) != Some("approved")
            || decision.get("revocation_status").and_then(|v| v.as_str()) != Some("active")
        {
            return Err(crate::BrokerError::Brk106);
        }
        Ok(())
    }

    pub fn is_current_authority(
        decision: &RegistryDecisionV2,
        approval_epoch: u64,
        revocation_epoch: u64,
    ) -> bool {
        let value = decision.as_value();
        value.get("approval_status").and_then(|v| v.as_str()) == Some("approved")
            && value.get("revocation_status").and_then(|v| v.as_str()) == Some("active")
            && value.get("approval_epoch").and_then(|v| v.as_u64()) == Some(approval_epoch)
            && value.get("revocation_epoch").and_then(|v| v.as_u64()) == Some(revocation_epoch)
    }
}
