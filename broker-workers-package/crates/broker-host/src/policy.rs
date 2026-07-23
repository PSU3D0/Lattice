use std::{collections::BTreeMap, sync::Arc};

use broker_core::{
    BrokerError,
    credential::{
        ParsedV2, parse,
        policy::PolicyInstanceV2,
        registry::{RegistryDecisionTag, RegistryDecisionV2, RegistryDefinitionV2},
        signing::verify_signed,
    },
    signing::BrokerVerifyingKey,
};
use serde_json::Value;

pub const SYNTHETIC_EQUALITY_PROFILE_REF: &str = "policy.synthetic.canonical-equality";

pub trait PolicyEvaluatorImplementation: Send + Sync {
    fn evaluate(
        &self,
        policy_instance: &PolicyInstanceV2,
        selected_facts: &Value,
        resolved_constraint: &Value,
    ) -> Result<Value, BrokerError>;
}

struct EvaluatorEntry {
    definition: ParsedV2<RegistryDefinitionV2>,
    decision: ParsedV2<RegistryDecisionV2>,
    implementation: Arc<dyn PolicyEvaluatorImplementation>,
}

#[derive(Default)]
pub struct PolicyEvaluatorRegistry {
    entries: BTreeMap<(String, String, String), EvaluatorEntry>,
}
impl PolicyEvaluatorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn install(
        &mut self,
        definition_bytes: &[u8],
        decision_bytes: &[u8],
        publisher_key: &BrokerVerifyingKey,
        authority_key: &BrokerVerifyingKey,
        now: &str,
        implementation: Arc<dyn PolicyEvaluatorImplementation>,
    ) -> Result<(), BrokerError> {
        let definition = parse::<RegistryDefinitionV2>(definition_bytes)?;
        let decision = parse::<RegistryDecisionV2>(decision_bytes)?;
        verify_signed(&definition, publisher_key)?;
        verify_signed(&decision, authority_key)?;
        let d = definition.view.as_value();
        let a = decision.view.as_value();
        if definition.canonical_bytes() != definition_bytes
            || decision.canonical_bytes() != decision_bytes
            || d.get("class").and_then(Value::as_str) != Some("policy_evaluator")
            || d.get("class") != d.pointer("/class_payload/kind")
            || a.get("entry_ref") != d.get("entry_ref")
            || a.get("version") != d.get("version")
            || a.get("definition_hash").and_then(Value::as_str)
                != Some(definition.content_hash().as_str())
            || a.get("not_before")
                .and_then(Value::as_str)
                .is_none_or(|value| value > now)
            || a.get("expires_at")
                .and_then(Value::as_str)
                .is_none_or(|value| value <= now)
            || !RegistryDecisionTag::is_current_authority(
                &decision.view,
                a.get("approval_epoch")
                    .and_then(Value::as_u64)
                    .ok_or(BrokerError::Brk106)?,
                a.get("revocation_epoch")
                    .and_then(Value::as_u64)
                    .ok_or(BrokerError::Brk106)?,
            )
        {
            return Err(BrokerError::Brk106);
        }
        let key = (
            d["entry_ref"]
                .as_str()
                .ok_or(BrokerError::Brk004)?
                .to_owned(),
            d["version"].as_str().ok_or(BrokerError::Brk004)?.to_owned(),
            definition.content_hash(),
        );
        if self
            .entries
            .insert(
                key,
                EvaluatorEntry {
                    definition,
                    decision,
                    implementation,
                },
            )
            .is_some()
        {
            return Err(BrokerError::Brk204);
        }
        Ok(())
    }

    /// Empty semantic profiles need no evaluator (`brokered_count`). Every
    /// non-empty profile is resolved to one exact approved evaluator or fails
    /// closed. No fallback evaluator or universal policy language exists.
    pub fn evaluate(
        &self,
        policy: &PolicyInstanceV2,
        selected_facts: &Value,
        resolved_constraint: &Value,
    ) -> Result<Value, BrokerError> {
        let value = policy.as_value();
        let pin = value.get("evaluator").ok_or(BrokerError::Brk004)?;
        let key = (
            pin.get("entry_ref")
                .and_then(Value::as_str)
                .ok_or(BrokerError::Brk004)?
                .to_owned(),
            pin.get("version")
                .and_then(Value::as_str)
                .ok_or(BrokerError::Brk004)?
                .to_owned(),
            pin.get("definition_hash")
                .and_then(Value::as_str)
                .ok_or(BrokerError::Brk004)?
                .to_owned(),
        );
        let entry = self.entries.get(&key).ok_or(BrokerError::Brk004)?;
        RegistryDecisionTag::verify_pin(&parse_pin(pin)?, &entry.decision.view)?;
        let payload = &entry.definition.view.as_value()["class_payload"];
        for (field, policy_field) in [
            ("supported_policy_schema_hashes", "schema_hash"),
            ("relation_schema_hashes", "relation_schema_hash"),
            ("output_schema_hashes", "evaluator_output_schema_hash"),
        ] {
            if !payload[field].as_array().is_some_and(|values| {
                values
                    .iter()
                    .any(|candidate| Some(candidate) == value.get(policy_field))
            }) {
                return Err(BrokerError::Brk004);
            }
        }
        let output = entry
            .implementation
            .evaluate(policy, selected_facts, resolved_constraint)?;
        if output.get("decision").and_then(Value::as_str) != Some("satisfied")
            || value.get("output") != Some(&output)
        {
            return Err(BrokerError::Brk205);
        }
        Ok(output)
    }

    pub fn evaluate_required(
        &self,
        policies: &[PolicyEvaluationInput<'_>],
    ) -> Result<Vec<String>, BrokerError> {
        let mut hashes = Vec::with_capacity(policies.len());
        for item in policies {
            let output =
                self.evaluate(item.policy, item.selected_facts, item.resolved_constraint)?;
            hashes.push(broker_core::canonical::from_serde(&output, 1024 * 1024)?.sha256());
        }
        hashes.sort();
        if hashes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(BrokerError::Brk109);
        }
        Ok(hashes)
    }
}

pub struct PolicyEvaluationInput<'a> {
    pub policy: &'a PolicyInstanceV2,
    pub selected_facts: &'a Value,
    pub resolved_constraint: &'a Value,
}

/// One deliberately narrow synthetic profile proving registry dispatch. It is
/// not a general DSL: only canonical equality is supported.
pub struct DeterministicEqualityEvaluator;
impl PolicyEvaluatorImplementation for DeterministicEqualityEvaluator {
    fn evaluate(
        &self,
        policy_instance: &PolicyInstanceV2,
        selected_facts: &Value,
        resolved_constraint: &Value,
    ) -> Result<Value, BrokerError> {
        if policy_instance
            .as_value()
            .pointer("/profile_ref/profile_ref")
            .and_then(Value::as_str)
            != Some(SYNTHETIC_EQUALITY_PROFILE_REF)
            || selected_facts != resolved_constraint
        {
            return Err(BrokerError::Brk205);
        }
        policy_instance
            .as_value()
            .get("output")
            .cloned()
            .ok_or(BrokerError::Brk109)
    }
}

fn parse_pin(value: &Value) -> Result<broker_core::credential::RegistryPinV2, BrokerError> {
    let bytes = broker_core::canonical::from_serde(value, 64 * 1024)?;
    Ok(broker_core::credential::parse(bytes.as_bytes())?.view)
}
