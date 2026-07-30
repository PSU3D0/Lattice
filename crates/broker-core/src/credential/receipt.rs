public_type!(
    ImplementationRefV2,
    ImplementationRefTag,
    "ImplementationRef"
);
public_type!(
    BindingAttestationV2,
    BindingAttestationTag,
    "BindingAttestation"
);
public_type!(ReceiptClaimsV2, ReceiptClaimsTag, "ReceiptClaims");
public_type!(LeaseGenerationV2, LeaseGenerationTag, "LeaseGeneration");
public_type!(
    HashOrNotComputedV2,
    HashOrNotComputedTag,
    "HashOrNotComputed"
);
public_type!(
    InvocationReceiptV2,
    InvocationReceiptTag,
    "InvocationReceipt"
);

pub(crate) fn verify_lifecycle_terminal_consistency(
    receipt: &serde_json::Value,
) -> Result<(), crate::BrokerError> {
    let terminal = receipt
        .get("terminal_state")
        .and_then(serde_json::Value::as_str)
        .ok_or(crate::BrokerError::Brk109)?;
    let observation = receipt
        .get("provider_dispatch_observation")
        .and_then(serde_json::Value::as_str)
        .ok_or(crate::BrokerError::Brk109)?;
    match terminal {
        "terminal_success" | "terminal_failure" if observation == "provider_response_observed" => {
            Ok(())
        }
        "terminal_ambiguous"
            if matches!(
                observation,
                "credential_exported" | "remote_operation_started" | "provider_response_observed"
            ) =>
        {
            Ok(())
        }
        _ => Err(crate::BrokerError::Brk109),
    }
}
