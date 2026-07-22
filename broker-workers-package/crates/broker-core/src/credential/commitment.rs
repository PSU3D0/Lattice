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
