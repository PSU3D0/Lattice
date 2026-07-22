use std::collections::BTreeMap;

use broker_core::{
    BrokerError,
    credential::{
        ParsedV2, parse,
        registry::{RegistryDecisionTag, RegistryDecisionV2, RegistryDefinitionV2},
        signing::verify_signed,
    },
    signing::BrokerVerifyingKey,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryClass {
    AuthProfile,
    CapsulePlanner,
    ResponseProjector,
    AuthDriver,
    Custodian,
    Transport,
    PolicyEvaluator,
    DynamicSource,
    ClaimNormalizer,
    PrivilegedResponseFirewall,
    LegacyInventory,
}

impl RegistryClass {
    pub const ALL: [Self; 11] = [
        Self::AuthProfile,
        Self::CapsulePlanner,
        Self::ResponseProjector,
        Self::AuthDriver,
        Self::Custodian,
        Self::Transport,
        Self::PolicyEvaluator,
        Self::DynamicSource,
        Self::ClaimNormalizer,
        Self::PrivilegedResponseFirewall,
        Self::LegacyInventory,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthProfile => "auth_profile",
            Self::CapsulePlanner => "capsule_planner",
            Self::ResponseProjector => "response_projector",
            Self::AuthDriver => "auth_driver",
            Self::Custodian => "custodian",
            Self::Transport => "transport",
            Self::PolicyEvaluator => "policy_evaluator",
            Self::DynamicSource => "dynamic_source",
            Self::ClaimNormalizer => "claim_normalizer",
            Self::PrivilegedResponseFirewall => "privileged_response_firewall",
            Self::LegacyInventory => "legacy_inventory",
        }
    }
}

pub struct StaticRegistryEntry {
    pub class: RegistryClass,
    pub definition: ParsedV2<RegistryDefinitionV2>,
    pub decision: ParsedV2<RegistryDecisionV2>,
}

#[derive(Default)]
pub struct StaticRegistry {
    entries: BTreeMap<(String, String), StaticRegistryEntry>,
}

impl StaticRegistry {
    pub fn load(
        seeds: &[(Vec<u8>, Vec<u8>)],
        publisher_roots: &BTreeMap<String, BrokerVerifyingKey>,
        decision_roots: &BTreeMap<String, BrokerVerifyingKey>,
        now: &str,
    ) -> Result<Self, BrokerError> {
        let mut entries: BTreeMap<(String, String), StaticRegistryEntry> = BTreeMap::new();
        for (definition_json, decision_json) in seeds {
            let definition = parse::<RegistryDefinitionV2>(definition_json)?;
            let decision = parse::<RegistryDecisionV2>(decision_json)?;
            let definition_value = definition.view.as_value();
            let decision_value = decision.view.as_value();
            let publisher_key_id = text(definition_value, "publisher_key_id")?;
            let authority_key_id = text(decision_value, "authority_key_id")?;
            verify_signed(
                &definition,
                publisher_roots
                    .get(publisher_key_id)
                    .ok_or(BrokerError::Brk004)?,
            )?;
            verify_signed(
                &decision,
                decision_roots
                    .get(authority_key_id)
                    .ok_or(BrokerError::Brk004)?,
            )?;
            let class = parse_class(text(definition_value, "class")?)?;
            if definition_value
                .pointer("/class_payload/kind")
                .and_then(|v| v.as_str())
                != Some(class.as_str())
                || decision_value.get("entry_ref") != definition_value.get("entry_ref")
                || decision_value.get("version") != definition_value.get("version")
                || decision_value
                    .get("definition_hash")
                    .and_then(|v| v.as_str())
                    != Some(definition.content_hash().as_str())
                || decision_value
                    .get("approval_status")
                    .and_then(|v| v.as_str())
                    != Some("approved")
                || decision_value
                    .get("revocation_status")
                    .and_then(|v| v.as_str())
                    != Some("active")
                || text(decision_value, "not_before")? > now
                || text(decision_value, "expires_at")? <= now
            {
                return Err(BrokerError::Brk106);
            }
            let pin = serde_json::json!({
                "entry_ref": text(decision_value, "entry_ref")?,
                "version": text(decision_value, "version")?,
                "definition_hash": text(decision_value, "definition_hash")?,
                "approval_epoch": decision_value.get("approval_epoch").ok_or(BrokerError::Brk004)?,
                "revocation_epoch": decision_value.get("revocation_epoch").ok_or(BrokerError::Brk004)?,
            });
            let pin = parse::<broker_core::credential::RegistryPinV2>(
                &serde_json::to_vec(&pin).map_err(|_| BrokerError::Brk004)?,
            )?;
            RegistryDecisionTag::verify_pin(&pin.view, &decision.view)?;
            let key = (
                text(definition_value, "entry_ref")?.to_string(),
                text(definition_value, "version")?.to_string(),
            );
            let candidate = StaticRegistryEntry {
                class,
                definition,
                decision,
            };
            if let Some(existing) = entries.get(&key) {
                if existing.definition.canonical_bytes() != candidate.definition.canonical_bytes()
                    || existing.decision.canonical_bytes() != candidate.decision.canonical_bytes()
                {
                    return Err(BrokerError::Brk203);
                }
            } else {
                entries.insert(key, candidate);
            }
        }
        Ok(Self { entries })
    }

    pub fn required(
        &self,
        entry_ref: &str,
        version: &str,
        class: RegistryClass,
    ) -> Result<&StaticRegistryEntry, BrokerError> {
        let entry = self
            .entries
            .get(&(entry_ref.to_string(), version.to_string()))
            .ok_or(BrokerError::Brk004)?;
        if entry.class != class {
            return Err(BrokerError::Brk106);
        }
        Ok(entry)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn text<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str, BrokerError> {
    value
        .get(field)
        .and_then(|value| value.as_str())
        .ok_or(BrokerError::Brk004)
}

fn parse_class(value: &str) -> Result<RegistryClass, BrokerError> {
    RegistryClass::ALL
        .into_iter()
        .find(|class| class.as_str() == value)
        .ok_or(BrokerError::Brk004)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::{credential::signing::REGISTRY_DECISION_DOMAIN, signing::BrokerSigner};

    fn loaded_registry() -> StaticRegistry {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../impl-docs/spec/credential-plane-protocol-vectors.json"
        ))
        .unwrap();
        let definition = vectors["signed_artifact_vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|vector| vector["artifact_schema"] == "RegistryDefinition")
            .unwrap()["artifact"]
            .clone();
        let definition_bytes = serde_json::to_vec(&definition).unwrap();
        let definition_hash = parse::<RegistryDefinitionV2>(&definition_bytes)
            .unwrap()
            .content_hash();
        let signer = BrokerSigner::from_seed(
            "test-ed25519-1",
            <[u8; 32]>::try_from(
                hex::decode(vectors["signing_keys"][0]["seed_hex"].as_str().unwrap()).unwrap(),
            )
            .unwrap(),
        );
        let mut decision = vectors["signed_artifact_vectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|vector| vector["artifact_schema"] == "RegistryDecision")
            .unwrap()["artifact"]
            .clone();
        decision["definition_hash"] = definition_hash.into();
        decision["expires_at"] = "2027-01-01T00:00:00Z".into();
        let signature = signer
            .sign_json(
                REGISTRY_DECISION_DOMAIN,
                &serde_json::to_vec(&decision).unwrap(),
            )
            .unwrap();
        decision["signature"] = serde_json::to_value(signature).unwrap();
        let roots = BTreeMap::from([("test-ed25519-1".into(), signer.verifying_key())]);
        StaticRegistry::load(
            &[(definition_bytes, serde_json::to_vec(&decision).unwrap())],
            &roots,
            &roots,
            "2026-06-01T00:00:00Z",
        )
        .unwrap()
    }

    #[test]
    fn signatures_digests_and_unknown_required_entries_fail_closed() {
        let registry = loaded_registry();
        assert_eq!(registry.len(), 1);
        registry
            .required("x", "x", RegistryClass::AuthProfile)
            .unwrap();
        assert_eq!(
            registry
                .required("missing", "x", RegistryClass::AuthProfile)
                .err()
                .unwrap(),
            BrokerError::Brk004
        );
    }

    #[test]
    fn every_registry_class_rejects_every_class_substitution() {
        for expected in RegistryClass::ALL {
            let mut registry = loaded_registry();
            registry
                .entries
                .get_mut(&("x".into(), "x".into()))
                .unwrap()
                .class = expected;
            registry.required("x", "x", expected).unwrap();
            for substitute in RegistryClass::ALL {
                if substitute != expected {
                    assert_eq!(
                        registry.required("x", "x", substitute).err().unwrap(),
                        BrokerError::Brk106
                    );
                }
            }
        }
    }
}
