//! Lifecycle-separated-1 protocol 0.2 models.
//!
//! These tags intentionally do not alias or convert the pre-fix protocol 0.2
//! models. Construction always validates the distinct `LS1*` schema branch.

public_type!(
    AuthorityModelCutoverLs1,
    AuthorityModelCutoverLs1Tag,
    "LS1AuthorityModelCutover"
);
public_type!(
    AuthorizationObservationLs1,
    AuthorizationObservationLs1Tag,
    "LS1AuthorizationObservation"
);
public_type!(
    ProviderGrantVersionLs1,
    ProviderGrantVersionLs1Tag,
    "LS1ProviderGrantVersion"
);
public_type!(
    ProviderGrantAdoptionRecordLs1,
    ProviderGrantAdoptionRecordLs1Tag,
    "LS1ProviderGrantAdoptionRecord"
);
public_type!(
    ConnectionAliasRecordLs1,
    ConnectionAliasRecordLs1Tag,
    "LS1ConnectionAliasRecord"
);
public_type!(
    StandingAuthorityLs1,
    StandingAuthorityLs1Tag,
    "LS1StandingAuthority"
);
public_type!(ContractSetLs1, ContractSetLs1Tag, "LS1ContractSet");
public_type!(PolicyInstanceLs1, PolicyInstanceLs1Tag, "LS1PolicyInstance");
public_type!(
    RegistryDefinitionLs1,
    RegistryDefinitionLs1Tag,
    "LS1RegistryDefinition"
);
public_type!(
    RegistryDecisionLs1,
    RegistryDecisionLs1Tag,
    "LS1RegistryDecision"
);
public_type!(
    RegistryDecisionVectorLs1,
    RegistryDecisionVectorLs1Tag,
    "LS1RegistryDecisionVector"
);
public_type!(
    CeilingAmendmentLs1,
    CeilingAmendmentLs1Tag,
    "LS1CeilingAmendment"
);
public_type!(
    CorrectedBindingAttestationLs1,
    CorrectedBindingAttestationLs1Tag,
    "LS1CorrectedBindingAttestation"
);
public_type!(
    DispatchAdmissionLs1,
    DispatchAdmissionLs1Tag,
    "LS1DispatchAdmission"
);
public_type!(
    InvocationReceiptLs1,
    InvocationReceiptLs1Tag,
    "LS1InvocationReceipt",
    64 * 1024
);
public_type!(
    LegacyAttemptInventoryLs1,
    LegacyAttemptInventoryLs1Tag,
    "LS1LegacyAttemptInventory"
);
public_type!(
    ReceiptVerificationKeysetLs1,
    ReceiptVerificationKeysetLs1Tag,
    "LS1ReceiptVerificationKeyset"
);
public_type!(
    ReceiptKeyCompromiseRecordLs1,
    ReceiptKeyCompromiseRecordLs1Tag,
    "LS1ReceiptKeyCompromiseRecord"
);

public_type!(
    AuthenticatedActorContextLs1,
    AuthenticatedActorContextLs1Tag,
    "LS1AuthenticatedActorContext"
);
public_type!(
    ActorConnectionAclLs1,
    ActorConnectionAclLs1Tag,
    "LS1ActorConnectionAcl"
);
public_type!(MaterialStateLs1, MaterialStateLs1Tag, "LS1MaterialState");
public_type!(
    CustodyReservationLs1,
    CustodyReservationLs1Tag,
    "LS1CustodyReservation"
);
public_type!(
    CustodyCancellationEvidenceLs1,
    CustodyCancellationEvidenceLs1Tag,
    "LS1CustodyCancellationEvidence"
);
public_type!(
    CrossingAuthorizationLs1,
    CrossingAuthorizationLs1Tag,
    "LS1CrossingAuthorization"
);
public_type!(
    CrossingEvidenceLs1,
    CrossingEvidenceLs1Tag,
    "LS1CrossingEvidence"
);
public_type!(RunLedgerLs1, RunLedgerLs1Tag, "LS1RunLedger");
public_type!(EffectStateLs1, EffectStateLs1Tag, "LS1EffectState");
public_type!(AttemptStateLs1, AttemptStateLs1Tag, "LS1AttemptState");
