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
    use std::collections::BTreeSet;

    if predicates.len() != evidence.len() {
        return Err(crate::BrokerError::Brk109);
    }
    let mut required_ids = BTreeSet::new();
    let mut evidence_ids = BTreeSet::new();
    for item in evidence {
        let value = item.as_value();
        let identity = (
            value
                .get("predicate_id")
                .and_then(|v| v.as_str())
                .ok_or(crate::BrokerError::Brk109)?,
            value
                .get("kind")
                .and_then(|v| v.as_str())
                .ok_or(crate::BrokerError::Brk109)?,
        );
        if !evidence_ids.insert(identity) {
            return Err(crate::BrokerError::Brk109);
        }
    }
    for predicate in predicates {
        let required = predicate.as_value();
        let identity = (
            required
                .get("predicate_id")
                .and_then(|v| v.as_str())
                .ok_or(crate::BrokerError::Brk109)?,
            required
                .get("kind")
                .and_then(|v| v.as_str())
                .ok_or(crate::BrokerError::Brk109)?,
        );
        if !required_ids.insert(identity) {
            return Err(crate::BrokerError::Brk109);
        }
        let actual = evidence
            .iter()
            .find(|item| {
                item.as_value().get("predicate_id").and_then(|v| v.as_str()) == Some(identity.0)
                    && item.as_value().get("kind").and_then(|v| v.as_str()) == Some(identity.1)
            })
            .ok_or(crate::BrokerError::Brk109)?
            .as_value();
        match identity.1 {
            "brokered_count" => {
                for field in [
                    "grant_hash",
                    "authority_view_hash",
                    "authority_epoch",
                    "ledger_transition_hash",
                    "budget_before",
                    "budget_after",
                    "pop_transcript_hash",
                    "registry_decision_set_hash",
                ] {
                    if actual.get(field).is_none() {
                        return Err(crate::BrokerError::Brk109);
                    }
                }
            }
            "semantic_policy" => {
                let hashes = required
                    .get("required_policy_instance_hashes")
                    .and_then(|v| v.as_array())
                    .ok_or(crate::BrokerError::Brk109)?;
                if hashes.len() != 1
                    || hashes.first() != actual.get("policy_instance_hash")
                    || actual.get("evaluator_output_hash").is_none()
                    || actual.get("evaluator").is_none()
                {
                    return Err(crate::BrokerError::Brk109);
                }
            }
            "provider_constraint" => {
                for (required_field, evidence_field) in [
                    ("constraint_ref", "constraint_ref"),
                    ("evidence_schema_hash", "evidence_schema_hash"),
                    ("required_issuer", "issuer"),
                ] {
                    if required.get(required_field) != actual.get(evidence_field) {
                        return Err(crate::BrokerError::Brk109);
                    }
                }
                if actual.get("evidence_commitment").is_none()
                    || actual.get("verification_result_hash").is_none()
                {
                    return Err(crate::BrokerError::Brk109);
                }
            }
            _ => return Err(crate::BrokerError::Brk109),
        }
    }
    if required_ids != evidence_ids {
        return Err(crate::BrokerError::Brk109);
    }
    Ok(())
}
