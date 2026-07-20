use std::collections::BTreeMap;

use broker_core::{
    BrokerError,
    artifacts::{BindingAttestation, CommitmentEnvelope, ParsedArtifact, parse},
    signing::{BINDING_DOMAIN, BrokerVerifyingKey},
};

use crate::BrokerHostError;

/// Immutable host record loaded from `bindings.lock`. Every attestation field
/// that can affect authority is pinned here; no issuer or key advertised by an
/// attestation is ever promoted into trust.
#[derive(Clone, Debug)]
pub struct BindingLockRecord {
    expected_issuer: String,
    expected_key_id: String,
    verifying_key: BrokerVerifyingKey,
    expected_org_id: String,
    expected_connection_ref: String,
    expected_provider: String,
    expected_account_commitment: CommitmentEnvelope,
    expected_roles: BTreeMap<String, String>,
    expected_required_scopes: Vec<String>,
    expected_actual_scopes: Vec<String>,
}

impl BindingLockRecord {
    pub(crate) fn connection_ref(&self) -> &str {
        &self.expected_connection_ref
    }
    pub(crate) fn provider(&self) -> &str {
        &self.expected_provider
    }
    pub(crate) fn account_commitment(&self) -> &CommitmentEnvelope {
        &self.expected_account_commitment
    }
    pub(crate) fn actual_scopes(&self) -> &[String] {
        &self.expected_actual_scopes
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_binding_lock(
        expected_issuer: impl Into<String>,
        expected_key_id: impl Into<String>,
        verifying_key: BrokerVerifyingKey,
        expected_org_id: impl Into<String>,
        expected_connection_ref: impl Into<String>,
        expected_provider: impl Into<String>,
        expected_account_commitment: CommitmentEnvelope,
        expected_roles: BTreeMap<String, String>,
        expected_required_scopes: Vec<String>,
        expected_actual_scopes: Vec<String>,
    ) -> Result<Self, BrokerHostError> {
        let record = Self {
            expected_issuer: expected_issuer.into(),
            expected_key_id: expected_key_id.into(),
            verifying_key,
            expected_org_id: expected_org_id.into(),
            expected_connection_ref: expected_connection_ref.into(),
            expected_provider: expected_provider.into(),
            expected_account_commitment,
            expected_roles,
            expected_required_scopes,
            expected_actual_scopes,
        };
        if record.expected_issuer.is_empty()
            || record.expected_key_id.is_empty()
            || record.expected_org_id.is_empty()
            || record.expected_connection_ref.is_empty()
            || record.expected_provider.is_empty()
            || record.expected_roles.is_empty()
            || !sorted_unique(&record.expected_required_scopes)
            || !sorted_unique(&record.expected_actual_scopes)
        {
            return Err(BrokerHostError::InvalidBindingLock);
        }
        Ok(record)
    }
}

fn sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1]) && values.iter().all(|value| !value.is_empty())
}

struct EvidenceEntry {
    canonical_attestation: Vec<u8>,
    lock: BindingLockRecord,
}

/// Host configuration keyed by the existing connection-selection alias. It
/// extends bindings.lock selection and never performs discovery or network I/O.
#[derive(Default)]
pub struct BrokerBindingEvidence {
    entries: BTreeMap<String, EvidenceEntry>,
}

#[derive(Debug)]
pub struct VerifiedBinding {
    pub(crate) attestation: ParsedArtifact<BindingAttestation>,
}

impl BrokerBindingEvidence {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install evidence only together with its authoritative lock record.
    pub fn insert_lock_record(
        &mut self,
        connection_alias: impl Into<String>,
        lock: BindingLockRecord,
        canonical_attestation: &[u8],
    ) -> Result<(), BrokerHostError> {
        let alias = connection_alias.into();
        if alias.is_empty()
            || canonical_attestation.len() > broker_core::artifacts::BINDING_MAX
            || self.entries.contains_key(&alias)
        {
            return Err(BrokerError::Brk109.into());
        }
        self.entries.insert(
            alias,
            EvidenceEntry {
                canonical_attestation: canonical_attestation.to_vec(),
                lock,
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
    ) -> Result<VerifiedBinding, BrokerHostError> {
        self.verify_internal(
            connection_alias,
            contract_id,
            contract_hash,
            Some(current_revocation_epoch),
            now,
        )
    }

    pub(crate) fn verify_preflight(
        &self,
        connection_alias: &str,
        contract_id: &str,
        contract_hash: &str,
        now: &str,
    ) -> Result<VerifiedBinding, BrokerHostError> {
        self.verify_internal(connection_alias, contract_id, contract_hash, None, now)
    }

    fn verify_internal(
        &self,
        connection_alias: &str,
        contract_id: &str,
        contract_hash: &str,
        current_revocation_epoch: Option<u64>,
        now: &str,
    ) -> Result<VerifiedBinding, BrokerHostError> {
        let entry = self
            .entries
            .get(connection_alias)
            .ok_or(BrokerHostError::MissingBindingEvidence)?;

        // SECURITY ORDERING: extract only the signature envelope, then verify
        // with the lock-pinned key before trusting or comparing any claim.
        let untrusted: BindingAttestation = serde_json::from_slice(&entry.canonical_attestation)
            .map_err(|_| BrokerHostError::BindingSignatureRejected)?;
        entry
            .lock
            .verifying_key
            .verify_json(
                BINDING_DOMAIN,
                &entry.canonical_attestation,
                &untrusted.signature,
            )
            .map_err(|_| BrokerHostError::BindingSignatureRejected)?;

        let view = &untrusted;
        let lock = &entry.lock;
        if view.issuer != lock.expected_issuer {
            return Err(BrokerHostError::BindingIssuerMismatch);
        }
        if view.broker_key_id != lock.expected_key_id
            || view.signature.key_id != lock.expected_key_id
        {
            return Err(BrokerHostError::BindingKeyMismatch);
        }
        if view.org_id != lock.expected_org_id {
            return Err(BrokerHostError::BindingOrgMismatch);
        }
        if view.connection_ref != lock.expected_connection_ref {
            return Err(BrokerHostError::BindingConnectionMismatch);
        }
        if view.provider != lock.expected_provider {
            return Err(BrokerHostError::BindingProviderMismatch);
        }
        if view.account_commitment != lock.expected_account_commitment {
            return Err(BrokerHostError::BindingAccountMismatch);
        }
        if view.roles != lock.expected_roles {
            return Err(BrokerHostError::BindingRolesMismatch);
        }
        if view.scope_alignment.required_scopes != lock.expected_required_scopes
            || view.scope_alignment.actual_scopes != lock.expected_actual_scopes
            || !view.scope_alignment.satisfied
        {
            return Err(BrokerHostError::BindingScopesMismatch);
        }

        // Freshness/lane/epoch checks occur only after signature and all lock
        // comparisons, so malformed claims cannot influence trust selection.
        if view.lane != "semantic_broker" {
            return Err(BrokerHostError::BindingLaneMismatch);
        }
        if now < view.observed_at.as_str() {
            return Err(BrokerError::Brk104.into());
        }
        if now >= view.expires_at.as_str() {
            return Err(BrokerError::Brk105.into());
        }
        if current_revocation_epoch.is_some_and(|current| view.revocation_epoch != current) {
            return Err(BrokerError::Brk106.into());
        }
        if !view.supported_contracts.iter().any(|contract| {
            contract.contract_id == contract_id && contract.contract_hash == contract_hash
        }) {
            return Err(BrokerError::Brk108.into());
        }
        let attestation: ParsedArtifact<BindingAttestation> =
            parse(&entry.canonical_attestation)
                .map_err(|_| BrokerHostError::BindingSignatureRejected)?;
        if attestation.canonical_bytes() != entry.canonical_attestation {
            return Err(BrokerHostError::BindingSignatureRejected);
        }
        Ok(VerifiedBinding { attestation })
    }

    pub(crate) fn entries_alias_for_bootstrap(&self) -> Option<&str> {
        self.entries.keys().next().map(String::as_str)
    }

    pub(crate) fn lock_for_alias(&self, alias: &str) -> Option<&BindingLockRecord> {
        self.entries.get(alias).map(|entry| &entry.lock)
    }

    pub(crate) fn trust_keys(&self) -> Vec<(String, String, BrokerVerifyingKey)> {
        self.entries
            .values()
            .map(|entry| {
                (
                    entry.lock.expected_issuer.clone(),
                    entry.lock.expected_key_id.clone(),
                    entry.lock.verifying_key.clone(),
                )
            })
            .collect()
    }
}
