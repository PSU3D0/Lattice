use std::collections::BTreeMap;

use broker_core::{
    BrokerError,
    artifacts::{BindingAttestation, ParsedArtifact, parse},
    signing::{BINDING_DOMAIN, BrokerVerifyingKey},
};

use crate::BrokerHostError;

struct EvidenceEntry {
    attestation: ParsedArtifact<BindingAttestation>,
    issuer_key: BrokerVerifyingKey,
}

/// Host configuration keyed by the existing connection-selection alias. It
/// extends bindings.lock selection and never performs discovery or network I/O.
#[derive(Default)]
pub struct BrokerBindingEvidence {
    entries: BTreeMap<String, EvidenceEntry>,
}

#[derive(Debug)]
pub struct VerifiedBinding<'a> {
    pub attestation: &'a ParsedArtifact<BindingAttestation>,
}

impl BrokerBindingEvidence {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        connection_alias: impl Into<String>,
        canonical_attestation: &[u8],
        issuer_key: BrokerVerifyingKey,
    ) -> Result<(), BrokerHostError> {
        let alias = connection_alias.into();
        if alias.is_empty() || self.entries.contains_key(&alias) {
            return Err(BrokerError::Brk109.into());
        }
        let attestation = parse(canonical_attestation)?;
        self.entries.insert(
            alias,
            EvidenceEntry {
                attestation,
                issuer_key,
            },
        );
        Ok(())
    }

    pub fn verify(
        &self,
        connection_alias: &str,
        contract_id: &str,
        contract_hash: &str,
        current_revocation_epoch: u64,
        now: &str,
    ) -> Result<VerifiedBinding<'_>, BrokerHostError> {
        let entry = self
            .entries
            .get(connection_alias)
            .ok_or(BrokerHostError::MissingBindingEvidence)?;
        let view = &entry.attestation.view;
        if view.lane != "semantic_broker"
            || view.issuer.is_empty()
            || view.broker_key_id.is_empty()
            || view.signature.key_id != view.broker_key_id
            || !view.scope_alignment.satisfied
        {
            return Err(BrokerError::Brk109.into());
        }
        if now < view.observed_at.as_str() {
            return Err(BrokerError::Brk104.into());
        }
        if now >= view.expires_at.as_str() {
            return Err(BrokerError::Brk105.into());
        }
        if view.revocation_epoch != current_revocation_epoch {
            return Err(BrokerError::Brk106.into());
        }
        if !view.supported_contracts.iter().any(|contract| {
            contract.contract_id == contract_id && contract.contract_hash == contract_hash
        }) {
            return Err(BrokerError::Brk108.into());
        }
        entry
            .issuer_key
            .verify_json(
                BINDING_DOMAIN,
                entry.attestation.canonical_bytes(),
                &view.signature,
            )
            .map_err(|_| BrokerHostError::Broker(BrokerError::Brk109))?;
        Ok(VerifiedBinding {
            attestation: &entry.attestation,
        })
    }

    pub(crate) fn configured_epoch(&self, connection_ref: &str) -> Option<u64> {
        self.entries
            .values()
            .find(|entry| entry.attestation.view.connection_ref == connection_ref)
            .map(|entry| entry.attestation.view.revocation_epoch)
    }

    pub(crate) fn trust_keys(&self) -> Vec<(String, String, BrokerVerifyingKey)> {
        self.entries
            .values()
            .map(|entry| {
                (
                    entry.attestation.view.issuer.clone(),
                    entry.attestation.view.broker_key_id.clone(),
                    entry.issuer_key.clone(),
                )
            })
            .collect()
    }
}
