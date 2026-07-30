use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use crate::{
    BrokerError,
    artifacts::{CommitmentEnvelope, VerificationTier},
    canonical,
    commitment::{CommitmentKey, DisclosureKey},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueEncoding {
    Jcs,
    OpaqueBytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitmentContextV2 {
    AuthorizationClaims {
        connection_ref: String,
        authority_epoch: u64,
    },
    PrincipalAccountSubject {
        connection_ref: String,
        authority_epoch: u64,
    },
    DynamicSourceValue {
        instance_id: String,
        source_ref: String,
    },
    BrokerInstance {
        connection_ref: String,
        authority_epoch: u64,
    },
    ServiceIdentity {
        entry_ref: String,
    },
    ConnectionCommitment {
        issuer: String,
        run_id: String,
        node_id: String,
        effect: String,
        attempt: u64,
    },
    GrantCanonicalInput {
        issuer: String,
        grant_ref: String,
        effect: String,
    },
    CanonicalInputCommitment {
        issuer: String,
        run_id: String,
        node_id: String,
        effect: String,
        attempt: u64,
    },
    ResponseCommitment {
        issuer: String,
        run_id: String,
        node_id: String,
        effect: String,
        attempt: u64,
    },
    PolicyConstraint {
        instance_id: String,
    },
    SelectedFacts {
        instance_id: String,
    },
    EffectivePolicyValue {
        instance_id: String,
    },
    ProviderConstraintEvidence {
        issuer: String,
        run_id: String,
        node_id: String,
        effect: String,
        attempt: u64,
        predicate_id: String,
    },
    RotationProviderResult {
        connection_ref: String,
        rotation_ref: String,
    },
}

impl CommitmentContextV2 {
    pub fn field_name(&self) -> &'static str {
        match self {
            Self::AuthorizationClaims { .. } => "authorization_claims",
            Self::PrincipalAccountSubject { .. } => "principal_account_subject",
            Self::DynamicSourceValue { .. } => "dynamic_source_value",
            Self::BrokerInstance { .. } => "broker_instance",
            Self::ServiceIdentity { .. } => "service_identity",
            Self::ConnectionCommitment { .. } => "connection_commitment",
            Self::GrantCanonicalInput { .. } => "grant_canonical_input",
            Self::CanonicalInputCommitment { .. } => "canonical_input_commitment",
            Self::ResponseCommitment { .. } => "response_commitment",
            Self::PolicyConstraint { .. } => "policy_constraint",
            Self::SelectedFacts { .. } => "selected_facts",
            Self::EffectivePolicyValue { .. } => "effective_policy_value",
            Self::ProviderConstraintEvidence { .. } => "provider_constraint_evidence",
            Self::RotationProviderResult { .. } => "rotation_provider_result",
        }
    }

    pub fn value_encoding(&self) -> ValueEncoding {
        match self {
            Self::PrincipalAccountSubject { .. }
            | Self::BrokerInstance { .. }
            | Self::ServiceIdentity { .. }
            | Self::ConnectionCommitment { .. } => ValueEncoding::OpaqueBytes,
            _ => ValueEncoding::Jcs,
        }
    }

    pub fn salt_context(&self) -> Result<Vec<u8>, BrokerError> {
        let value = match self {
            Self::AuthorizationClaims {
                connection_ref,
                authority_epoch,
            } => serde_json::json!([
                "credential-plane.v0.2",
                "authority_view",
                connection_ref,
                authority_epoch,
                "authorization_claims"
            ]),
            Self::PrincipalAccountSubject {
                connection_ref,
                authority_epoch,
            } => serde_json::json!([
                "credential-plane.v0.2",
                "authority_view",
                connection_ref,
                authority_epoch,
                "principal",
                "account_subject"
            ]),
            Self::DynamicSourceValue {
                instance_id,
                source_ref,
            } => serde_json::json!([
                "credential-plane.v0.2",
                "policy",
                instance_id,
                source_ref,
                "dynamic_value"
            ]),
            Self::BrokerInstance {
                connection_ref,
                authority_epoch,
            } => serde_json::json!([
                "credential-plane.v0.2",
                "authority_view",
                connection_ref,
                authority_epoch,
                "broker_instance"
            ]),
            Self::ServiceIdentity { entry_ref } => serde_json::json!([
                "credential-plane.v0.2",
                "registry",
                entry_ref,
                "service_identity"
            ]),
            Self::ConnectionCommitment {
                issuer,
                run_id,
                node_id,
                effect,
                attempt,
            } => receipt_context(
                issuer,
                run_id,
                node_id,
                effect,
                *attempt,
                "connection_commitment",
            ),
            Self::GrantCanonicalInput {
                issuer,
                grant_ref,
                effect,
            } => serde_json::json!([
                "credential-plane.v0.2",
                "grant",
                issuer,
                grant_ref,
                effect,
                "canonical_input"
            ]),
            Self::CanonicalInputCommitment {
                issuer,
                run_id,
                node_id,
                effect,
                attempt,
            } => receipt_context(
                issuer,
                run_id,
                node_id,
                effect,
                *attempt,
                "canonical_input_commitment",
            ),
            Self::ResponseCommitment {
                issuer,
                run_id,
                node_id,
                effect,
                attempt,
            } => receipt_context(
                issuer,
                run_id,
                node_id,
                effect,
                *attempt,
                "response_commitment",
            ),
            Self::PolicyConstraint { instance_id } => {
                serde_json::json!(["credential-plane.v0.2", "policy", instance_id, "constraint"])
            }
            Self::SelectedFacts { instance_id } => serde_json::json!([
                "credential-plane.v0.2",
                "policy",
                instance_id,
                "selected_facts"
            ]),
            Self::EffectivePolicyValue { instance_id } => serde_json::json!([
                "credential-plane.v0.2",
                "policy",
                instance_id,
                "effective_value"
            ]),
            Self::ProviderConstraintEvidence {
                issuer,
                run_id,
                node_id,
                effect,
                attempt,
                predicate_id,
            } => serde_json::json!([
                "credential-plane.v0.2",
                "receipt",
                issuer,
                run_id,
                node_id,
                effect,
                attempt,
                "provider_constraint_evidence",
                predicate_id
            ]),
            Self::RotationProviderResult {
                connection_ref,
                rotation_ref,
            } => serde_json::json!([
                "credential-plane.v0.2",
                "rotation",
                connection_ref,
                rotation_ref,
                "provider_result"
            ]),
        };
        canonical::from_serde(&value, canonical::MAX_OPERATION_BYTES)
            .map(canonical::CanonicalJson::into_bytes)
    }
}

fn receipt_context(
    issuer: &str,
    run_id: &str,
    node_id: &str,
    effect: &str,
    attempt: u64,
    field: &str,
) -> serde_json::Value {
    serde_json::json!([
        "credential-plane.v0.2",
        "receipt",
        issuer,
        run_id,
        node_id,
        effect,
        attempt,
        field
    ])
}

const LS1_COMMITMENT_PREFIX: &str =
    "lattice.credential-plane.0.2.lifecycle-separated-1.commitment.";
const LS1_CONTEXT_FIELDS: [&str; 8] = [
    "tenant_id",
    "issuer",
    "provider",
    "auth_profile_ref",
    "oauth_client_id",
    "audience",
    "artifact_ref",
    "purpose",
];
const LS1_CONTEXTS: [&str; 9] = [
    "account-subject",
    "normalized-claims",
    "oauth-client-audience",
    "provider-grant-ref",
    "actor-subject",
    "dispatch-admission",
    "canonical-input",
    "provider-response",
    "alias-intent",
];

type HmacSha256 = Hmac<Sha256>;

pub struct LifecycleCommitmentContext<'a> {
    context: &'a str,
    fields: [&'a [u8]; 8],
}

impl<'a> LifecycleCommitmentContext<'a> {
    pub fn try_from_fields(
        context: &'a str,
        fields: &'a [(&'a str, &'a [u8])],
    ) -> Result<Self, BrokerError> {
        let purpose = context
            .strip_prefix(LS1_COMMITMENT_PREFIX)
            .filter(|suffix| LS1_CONTEXTS.contains(suffix))
            .ok_or(BrokerError::Brk004)?;
        if fields.len() != LS1_CONTEXT_FIELDS.len()
            || fields
                .iter()
                .zip(LS1_CONTEXT_FIELDS)
                .any(|((name, value), expected)| {
                    *name != expected
                        || value.is_empty()
                        || value.len() > canonical::MAX_STRING_BYTES
                })
            || fields.last().map(|(_, value)| *value) != Some(purpose.as_bytes())
        {
            return Err(BrokerError::Brk004);
        }
        let framed_len = fields.iter().try_fold(0_usize, |total, (name, value)| {
            total
                .checked_add(4 + name.len() + 8 + value.len())
                .ok_or(BrokerError::Brk004)
        })?;
        if framed_len > canonical::MAX_OPERATION_BYTES {
            return Err(BrokerError::Brk004);
        }
        let fields = fields
            .iter()
            .map(|(_, value)| *value)
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| BrokerError::Brk004)?;
        Ok(Self { context, fields })
    }
}

#[cfg(test)]
pub(crate) struct LifecycleCommitmentIntermediates {
    pub(crate) framed_context: Vec<u8>,
    pub(crate) opening_preimage: Vec<u8>,
    pub(crate) scoped_key: [u8; 32],
    pub(crate) commitment_preimage: Vec<u8>,
    pub(crate) value: String,
}

#[cfg(test)]
impl Drop for LifecycleCommitmentIntermediates {
    fn drop(&mut self) {
        self.framed_context.zeroize();
        self.opening_preimage.zeroize();
        self.scoped_key.zeroize();
        self.commitment_preimage.zeroize();
    }
}

pub fn commit_lifecycle_separated(
    root_key: &[u8; 32],
    context: &LifecycleCommitmentContext<'_>,
    private_value: &[u8],
) -> Result<String, BrokerError> {
    Ok(compute_lifecycle_commitment(root_key, context, private_value)?.0)
}

#[cfg(test)]
pub(crate) fn commit_lifecycle_separated_with_intermediates(
    root_key: &[u8; 32],
    context: &LifecycleCommitmentContext<'_>,
    private_value: &[u8],
) -> Result<LifecycleCommitmentIntermediates, BrokerError> {
    let (value, intermediates) = compute_lifecycle_commitment(root_key, context, private_value)?;
    let (framed_context, opening_preimage, scoped_key, commitment_preimage) = intermediates;
    Ok(LifecycleCommitmentIntermediates {
        framed_context: framed_context.as_slice().to_vec(),
        opening_preimage: opening_preimage.as_slice().to_vec(),
        scoped_key: *scoped_key,
        commitment_preimage: commitment_preimage.as_slice().to_vec(),
        value,
    })
}

type LifecycleIntermediates = (
    Zeroizing<Vec<u8>>,
    Zeroizing<Vec<u8>>,
    Zeroizing<[u8; 32]>,
    Zeroizing<Vec<u8>>,
);

fn compute_lifecycle_commitment(
    root_key: &[u8; 32],
    validated: &LifecycleCommitmentContext<'_>,
    private_value: &[u8],
) -> Result<(String, LifecycleIntermediates), BrokerError> {
    if private_value.is_empty() || private_value.len() > canonical::MAX_OPERATION_BYTES {
        return Err(BrokerError::Brk004);
    }
    let context = validated.context;
    let purpose = context
        .strip_prefix(LS1_COMMITMENT_PREFIX)
        .filter(|suffix| LS1_CONTEXTS.contains(suffix))
        .ok_or(BrokerError::Brk004)?;
    if validated.fields[7] != purpose.as_bytes() {
        return Err(BrokerError::Brk004);
    }
    let mut framed = Zeroizing::new(Vec::new());
    for (name, value) in LS1_CONTEXT_FIELDS.iter().zip(validated.fields) {
        let name_len = u32::try_from(name.len()).map_err(|_| BrokerError::Brk004)?;
        let value_len = u64::try_from(value.len()).map_err(|_| BrokerError::Brk004)?;
        framed.extend_from_slice(&name_len.to_be_bytes());
        framed.extend_from_slice(name.as_bytes());
        framed.extend_from_slice(&value_len.to_be_bytes());
        framed.extend_from_slice(value);
    }
    let context_len = u32::try_from(context.len()).map_err(|_| BrokerError::Brk004)?;
    let framed_len = u64::try_from(framed.len()).map_err(|_| BrokerError::Brk004)?;
    let mut opening_preimage = Zeroizing::new(b"lattice.commitment-opening.v0.1\0".to_vec());
    opening_preimage.extend_from_slice(&context_len.to_be_bytes());
    opening_preimage.extend_from_slice(context.as_bytes());
    opening_preimage.extend_from_slice(&framed_len.to_be_bytes());
    opening_preimage.extend_from_slice(&framed);
    let mut opening_mac = HmacSha256::new_from_slice(root_key).map_err(|_| BrokerError::Brk401)?;
    opening_mac.update(&opening_preimage);
    let mut scoped_key_bytes: [u8; 32] = opening_mac.finalize().into_bytes().into();
    let scoped_key = Zeroizing::new(scoped_key_bytes);
    scoped_key_bytes.zeroize();

    let private_len = u64::try_from(private_value.len()).map_err(|_| BrokerError::Brk004)?;
    let mut commitment_preimage = Zeroizing::new(b"lattice.commitment.v0.1\0".to_vec());
    commitment_preimage.extend_from_slice(&context_len.to_be_bytes());
    commitment_preimage.extend_from_slice(context.as_bytes());
    commitment_preimage.extend_from_slice(&framed_len.to_be_bytes());
    commitment_preimage.extend_from_slice(&framed);
    commitment_preimage.extend_from_slice(&private_len.to_be_bytes());
    commitment_preimage.extend_from_slice(private_value);
    let mut commitment_mac =
        HmacSha256::new_from_slice(&*scoped_key).map_err(|_| BrokerError::Brk401)?;
    commitment_mac.update(&commitment_preimage);
    let commitment = commitment_mac.finalize().into_bytes();
    Ok((
        format!("hmac-sha256:{}", hex::encode(commitment)),
        (framed, opening_preimage, scoped_key, commitment_preimage),
    ))
}

pub fn commit_v2(
    key: &CommitmentKey,
    org_id: &str,
    context: &CommitmentContextV2,
    value: &[u8],
    encoding: ValueEncoding,
    tier: VerificationTier,
) -> Result<(CommitmentEnvelope, DisclosureKey), BrokerError> {
    if context.value_encoding() != encoding {
        return Err(BrokerError::Brk004);
    }
    let value = if encoding == ValueEncoding::Jcs {
        canonical::canonicalize_bounded(value, canonical::MAX_OPERATION_BYTES)?.into_bytes()
    } else {
        value.to_vec()
    };
    key.commit(
        org_id,
        context.field_name(),
        &context.salt_context()?,
        &value,
        tier,
    )
}
