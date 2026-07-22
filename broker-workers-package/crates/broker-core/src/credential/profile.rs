public_type!(ActivationKindV2, ActivationKindTag, "ActivationKind");
public_type!(SchemeConfigV2, SchemeConfigTag, "SchemeConfig");
public_type!(
    AuthProfileDescriptorV2,
    AuthProfileDescriptorTag,
    "AuthProfileDescriptor"
);
public_type!(
    PublicClaimsProjectionPolicyV2,
    ProjectionPolicyTag,
    "PublicClaimsProjectionPolicy"
);
public_type!(
    PublicClaimsProjectionEvidenceV2,
    ProjectionEvidenceTag,
    "PublicClaimsProjectionEvidence"
);

pub fn verify_projection_evidence(
    policy: &PublicClaimsProjectionPolicyV2,
    evidence: &PublicClaimsProjectionEvidenceV2,
) -> Result<(), crate::BrokerError> {
    let expected_hash = crate::canonical::from_serde(policy.as_value(), 1024 * 1024)?.sha256();
    let policy = policy.as_value();
    let evidence = evidence.as_value();
    if evidence.get("policy_hash").and_then(|v| v.as_str()) != Some(expected_hash.as_str()) {
        return Err(crate::BrokerError::Brk109);
    }
    match policy.get("kind").and_then(|v| v.as_str()) {
        Some("none") if evidence.get("kind").and_then(|v| v.as_str()) == Some("none") => Ok(()),
        Some("allowlisted")
            if evidence.get("kind").and_then(|v| v.as_str()) == Some("projected") =>
        {
            for field in ["schema_hash", "projector"] {
                if policy.get(field) != evidence.get(field) {
                    return Err(crate::BrokerError::Brk109);
                }
            }
            Ok(())
        }
        _ => Err(crate::BrokerError::Brk109),
    }
}
