public_type!(FactSelectorV2, FactSelectorTag, "FactSelector");
public_type!(
    DynamicSourceBindingV2,
    DynamicSourceBindingTag,
    "DynamicSourceBinding"
);
public_type!(EvaluatorOutputV2, EvaluatorOutputTag, "EvaluatorOutput");
public_type!(PolicyInstanceV2, PolicyInstanceTag, "PolicyInstance");
public_type!(
    AssurancePredicateV2,
    AssurancePredicateTag,
    "AssurancePredicate"
);
public_type!(
    AssuranceEvidenceV2,
    AssuranceEvidenceTag,
    "AssuranceEvidence"
);

pub fn verify_assurance(
    predicates: &[AssurancePredicateV2],
    evidence: &[AssuranceEvidenceV2],
) -> Result<(), crate::BrokerError> {
    if predicates.len() != evidence.len() {
        return Err(crate::BrokerError::Brk109);
    }
    for predicate in predicates {
        let required = predicate.as_value();
        let predicate_id = required.get("predicate_id");
        let kind = required.get("kind");
        let actual = evidence
            .iter()
            .find(|item| {
                item.as_value().get("predicate_id") == predicate_id
                    && item.as_value().get("kind") == kind
            })
            .ok_or(crate::BrokerError::Brk109)?;
        if kind.and_then(|v| v.as_str()) == Some("provider_constraint") {
            for (required_field, evidence_field) in [
                ("constraint_ref", "constraint_ref"),
                ("evidence_schema_hash", "evidence_schema_hash"),
                ("required_issuer", "issuer"),
            ] {
                if required.get(required_field) != actual.as_value().get(evidence_field) {
                    return Err(crate::BrokerError::Brk109);
                }
            }
        }
    }
    Ok(())
}
