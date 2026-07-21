use crate::{
    BrokerError,
    artifacts::{
        InvocationReceipt, Outcome, PluginTrustTier, PrincipalKind, PrincipalRef, ReceiptClaims,
        SignatureAlg, SignatureEnvelope,
    },
    canonical,
    commitment::{CommitmentKey, receipt_salt_context},
    grant::{Clock, ExecutionGrantRecord},
    ledger::EntrySnapshot,
    signing::{BrokerSigner, RECEIPT_DOMAIN},
};

pub struct ReceiptImplementation<'a> {
    pub policy_hash: &'a str,
    pub plugin_module_sha256: &'a str,
    pub plugin_trust_tier: PluginTrustTier,
}

pub struct ReceiptIssueRequest<'a> {
    pub grant: &'a ExecutionGrantRecord,
    pub logical_effect_id: &'a str,
    pub state: &'a EntrySnapshot,
    pub implementation: ReceiptImplementation<'a>,
    pub outcome: Outcome,
    pub request_plan_hash: Option<String>,
    pub authority_facts_hash: Option<String>,
    pub canonical_input: &'a [u8],
    pub response_projection: &'a [u8],
    pub provider_request_id: Option<String>,
    pub dispatched: bool,
}

pub struct IssuedReceipt {
    pub receipt: InvocationReceipt,
    pub canonical_receipt: Vec<u8>,
}

pub struct ReceiptIssuer<'a> {
    pub signer: &'a BrokerSigner,
    pub commitments: &'a CommitmentKey,
    pub clock: &'a dyn Clock,
    pub broker_principal_id: &'a str,
}

impl ReceiptIssuer<'_> {
    pub fn issue(&self, request: ReceiptIssueRequest<'_>) -> Result<IssuedReceipt, BrokerError> {
        let grant = request.grant.grant();
        let (bundle, flow_hash, lock_hash, flow, node, alias, run) = grant.subject.flow_node_run();
        let attempt = request.state.dispatch_attempts as u64;
        let connection_salt = receipt_salt_context(
            &grant.issuer,
            run,
            node,
            request.logical_effect_id,
            attempt,
            "connection_commitment",
        )?;
        let input_salt = receipt_salt_context(
            &grant.issuer,
            run,
            node,
            request.logical_effect_id,
            attempt,
            "canonical_input_commitment",
        )?;
        let response_salt = receipt_salt_context(
            &grant.issuer,
            run,
            node,
            request.logical_effect_id,
            attempt,
            "response_commitment",
        )?;
        let connection = self
            .commitments
            .commit(
                &grant.org_id,
                "connection_commitment",
                &connection_salt,
                grant.connection_ref.as_bytes(),
                crate::artifacts::VerificationTier::VerifierWithDisclosure,
            )?
            .0;
        let input_commitment = self
            .commitments
            .commit(
                &grant.org_id,
                "canonical_input_commitment",
                &input_salt,
                request.canonical_input,
                crate::artifacts::VerificationTier::VerifierWithDisclosure,
            )?
            .0;
        let response_commitment = self
            .commitments
            .commit(
                &grant.org_id,
                "response_commitment",
                &response_salt,
                request.response_projection,
                crate::artifacts::VerificationTier::BrokerOnly,
            )?
            .0;
        let placeholder = SignatureEnvelope {
            alg: SignatureAlg::Ed25519,
            key_id: self.signer.key_id().into(),
            value: String::new(),
        };
        let mut receipt = InvocationReceipt {
            schema_version: "0.1".into(),
            critical_fields: vec![],
            org_id: grant.org_id.clone(),
            principal: PrincipalRef {
                kind: PrincipalKind::Broker,
                id: self.broker_principal_id.into(),
            },
            issuer: grant.issuer.clone(),
            broker_key_id: self.signer.key_id().into(),
            grant_hash: request.grant.hash(),
            policy_hash: request.implementation.policy_hash.into(),
            contract_hash: grant.contract_hash.clone(),
            plugin_module_sha256: request.implementation.plugin_module_sha256.into(),
            plugin_trust_tier: request.implementation.plugin_trust_tier,
            bundle_id: bundle.into(),
            flow_ir_hash: flow_hash.into(),
            binding_lock_hash: lock_hash.into(),
            flow_id: flow.into(),
            run_id: run.into(),
            node_id: node.into(),
            node_alias: alias.into(),
            logical_effect_id: request.logical_effect_id.into(),
            dispatch_attempt: attempt,
            connection_commitment: connection,
            canonical_input_commitment: input_commitment,
            request_plan_hash: request.request_plan_hash,
            authority_facts_hash: request.authority_facts_hash,
            budget_before: request.state.budget_before,
            budget_after: request.state.budget_after,
            provider_request_id: request.provider_request_id,
            response_commitment,
            outcome: request.outcome,
            claims: ReceiptClaims {
                trusted_host_scope_authenticated: true,
                broker_admission_enforced: true,
                provider_dispatch_observed: request.dispatched,
                remote_durable_state_proven: false,
                verifiable_execution_proven: false,
            },
            issued_at: self.clock.now_rfc3339(),
            signature: placeholder,
            extensions: Default::default(),
        };
        receipt.validate_semantics()?;
        let unsigned = canonical::from_serde(&receipt, crate::artifacts::RECEIPT_MAX)?.into_bytes();
        receipt.signature = self.signer.sign_json(RECEIPT_DOMAIN, &unsigned)?;
        let canonical_receipt =
            canonical::from_serde(&receipt, crate::artifacts::RECEIPT_MAX)?.into_bytes();
        Ok(IssuedReceipt {
            receipt,
            canonical_receipt,
        })
    }
}
