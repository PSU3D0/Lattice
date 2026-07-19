use crate::{
    BrokerError,
    artifacts::*,
    canonical,
    commitment::{CommitmentKey, receipt_salt_context},
    custodian::CredentialCustodian,
    dispatch::{DispatchResult, FinalRequestPlan, ProviderDispatcher},
    grant::{
        Clock, ExecutionGrantRecord, PopSession, PopVerifier, TrustedHostScope, validate_grant,
    },
    ledger::{
        InvocationState, Ledger, PlannedData, ReservationKey, ReserveRequest, ReserveResult,
        TerminalOutcome,
    },
    signing::{BrokerSigner, BrokerVerifyingKey, RECEIPT_DOMAIN},
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub struct ImplementationApproval {
    pub contract_hash: String,
    pub implementation: String,
    pub plugin_module_sha256: String,
    pub trust_tier: PluginTrustTier,
    pub policy_hash: String,
}
pub trait TrustRegistry: Send + Sync {
    fn resolve(
        &self,
        contract_id: &str,
        contract_hash: &str,
    ) -> Result<ImplementationApproval, BrokerError>;
    fn binding_key(&self, issuer: &str, key_id: &str) -> Option<BrokerVerifyingKey>;
}
#[derive(Clone, Debug)]
pub struct StaticTrustRegistry {
    pub approval: ImplementationApproval,
    pub contract_id: String,
    pub binding_issuer: String,
    pub binding_key_id: String,
    pub verifier: BrokerVerifyingKey,
}
impl TrustRegistry for StaticTrustRegistry {
    fn resolve(
        &self,
        contract_id: &str,
        contract_hash: &str,
    ) -> Result<ImplementationApproval, BrokerError> {
        if contract_id != self.contract_id || contract_hash != self.approval.contract_hash {
            Err(BrokerError::Brk108)
        } else {
            Ok(self.approval.clone())
        }
    }
    fn binding_key(&self, issuer: &str, key_id: &str) -> Option<BrokerVerifyingKey> {
        (issuer == self.binding_issuer && key_id == self.binding_key_id)
            .then(|| self.verifier.clone())
    }
}

#[derive(Clone, Debug)]
pub struct PlannedRequest {
    pub template: Vec<u8>,
    pub authority_facts: Vec<u8>,
    pub implementation: String,
    pub endpoint_origin: String,
}
pub trait RequestPlanner: Send + Sync {
    fn plan(
        &self,
        canonical_input: &[u8],
        access_material: &[u8],
    ) -> Result<PlannedRequest, BrokerError>;
}
#[derive(Clone, Debug)]
pub struct FixedTemplatePlanner {
    pub template: Vec<u8>,
    pub facts: Vec<u8>,
    pub implementation: String,
    pub endpoint_origin: String,
}
impl RequestPlanner for FixedTemplatePlanner {
    fn plan(
        &self,
        _canonical_input: &[u8],
        _access_material: &[u8],
    ) -> Result<PlannedRequest, BrokerError> {
        Ok(PlannedRequest {
            template: self.template.clone(),
            authority_facts: self.facts.clone(),
            implementation: self.implementation.clone(),
            endpoint_origin: self.endpoint_origin.clone(),
        })
    }
}

pub struct InvokeRequest<'a> {
    pub scope: &'a TrustedHostScope,
    pub grant: &'a ExecutionGrantRecord,
    pub binding: &'a ParsedArtifact<BindingAttestation>,
    pub pop: &'a PopSession,
    pub pop_verifier: &'a dyn PopVerifier,
    pub current_revocation_epoch: u64,
    pub asserted_operation_contract: &'a str,
    pub asserted_connection_ref: &'a str,
    pub logical_effect_id: &'a str,
    pub canonical_input: &'a [u8],
    pub lease_seconds: i64,
}
#[derive(Clone, Debug)]
pub struct InvokeResult {
    pub receipt: InvocationReceipt,
    pub canonical_receipt: Vec<u8>,
    pub redelivery: bool,
}

pub struct BrokerEngine<'a, L, P, D, C, T> {
    pub ledger: &'a L,
    pub planner: &'a P,
    pub dispatcher: &'a D,
    pub custodian: &'a C,
    pub trust: &'a T,
    pub clock: &'a dyn Clock,
    pub signer: &'a BrokerSigner,
    pub commitments: &'a CommitmentKey,
    pub broker_principal_id: &'a str,
}
impl<L: Ledger, P: RequestPlanner, D: ProviderDispatcher, C: CredentialCustodian, T: TrustRegistry>
    BrokerEngine<'_, L, P, D, C, T>
{
    pub fn invoke(&self, request: InvokeRequest<'_>) -> Result<InvokeResult, BrokerError> {
        let now_text = self.clock.now_rfc3339();
        validate_grant(
            request.grant,
            request.scope,
            request.pop,
            request.pop_verifier,
            request.current_revocation_epoch,
            &now_text,
        )?;
        let grant = &request.grant.grant;
        if request.asserted_operation_contract != grant.operation_contract {
            return Err(BrokerError::Brk107);
        }
        if request.asserted_connection_ref != grant.connection_ref {
            return Err(BrokerError::Brk109);
        }
        if request.binding.view.expires_at <= now_text
            || request.binding.view.revocation_epoch != request.current_revocation_epoch
            || request.binding.view.connection_ref != grant.connection_ref
            || request.binding.view.lane != "semantic_broker"
        {
            return Err(BrokerError::Brk109);
        }
        let binding_key = self
            .trust
            .binding_key(
                &request.binding.view.issuer,
                &request.binding.view.broker_key_id,
            )
            .ok_or(BrokerError::Brk108)?;
        binding_key
            .verify_json(
                crate::signing::BINDING_DOMAIN,
                request.binding.canonical_bytes(),
                &request.binding.view.signature,
            )
            .map_err(|_| BrokerError::Brk109)?;
        let approval = self
            .trust
            .resolve(&grant.operation_contract, &grant.contract_hash)?;
        let input = canonical::canonicalize_bounded(
            request.canonical_input,
            canonical::MAX_OPERATION_BYTES,
        )?;
        if input.as_bytes() != request.canonical_input {
            return Err(BrokerError::Brk001);
        }
        let (_, flow_ir_hash, _, _, node_id, _, run_id) = grant.subject.flow_node_run();
        let key = ReservationKey {
            org_id: grant.org_id.clone(),
            deployment_id: grant.principal.id.clone(),
            flow_ir_hash: flow_ir_hash.into(),
            run_id: run_id.into(),
            node_id: node_id.into(),
            logical_effect_id: request.logical_effect_id.into(),
            operation_contract: grant.operation_contract.clone(),
            connection_ref: grant.connection_ref.clone(),
        };
        let tick = self.clock.monotonic_seconds();
        let lease_deadline = tick
            .checked_add(request.lease_seconds)
            .ok_or(BrokerError::Brk401)?;
        let reservation = self.ledger.reserve(
            ReserveRequest {
                key: key.clone(),
                canonical_input: input.as_bytes().to_vec(),
                max_logical_calls: grant.budgets.logical_calls,
                max_dispatch_attempts: grant.budgets.dispatch_attempts_per_call,
                lease_deadline,
            },
            tick,
        )?;
        let acquired = match reservation {
            ReserveResult::Acquired(value) => value,
            ReserveResult::Redelivery(value) => return self.redelivery(value),
        };
        let planned = match self.custodian.with_access_material(|material| {
            self.planner
                .plan(input.as_bytes(), material.expose_to_broker())
        }) {
            Ok(value) => value,
            Err(_) => {
                let released = self
                    .ledger
                    .release(&key, acquired.lease_token, tick, false)?;
                return self.pre_dispatch_receipt(request, approval, released, input.as_bytes());
            }
        };
        if planned.implementation != approval.implementation {
            self.ledger
                .release(&key, acquired.lease_token, tick, false)?;
            return Err(BrokerError::Brk108);
        }
        if !request
            .binding
            .view
            .endpoint_origins
            .contains(&planned.endpoint_origin)
        {
            self.ledger
                .release(&key, acquired.lease_token, tick, false)?;
            return Err(BrokerError::Brk302);
        }
        let facts = canonical::canonicalize_bounded(
            &planned.authority_facts,
            canonical::MAX_OPERATION_BYTES,
        )
        .map_err(|_| BrokerError::Brk301)?;
        let final_plan =
            FinalRequestPlan::from_template(&planned.template, request.logical_effect_id)
                .map_err(|_| BrokerError::Brk301)?;
        let plan_hash = final_plan.hash();
        let facts_hash = format!("sha256:{}", hex::encode(Sha256::digest(facts.as_bytes())));
        let planned_state = self.ledger.plan(
            &key,
            acquired.lease_token,
            tick,
            PlannedData {
                request_plan_hash: plan_hash.clone(),
                authority_facts_hash: facts_hash.clone(),
                implementation: approval.implementation.clone(),
                endpoint: planned.endpoint_origin,
                next_attempt: 0,
            },
        )?;
        let dispatched = self
            .ledger
            .mark_dispatched(&key, acquired.lease_token, tick)?;
        let (outcome, response, provider_id) = match self.dispatcher.dispatch(&final_plan) {
            Ok(DispatchResult::Confirmed(v)) => (
                Outcome::Confirmed,
                v.bounded_projection,
                v.provider_request_id,
            ),
            Ok(DispatchResult::Failed) => (Outcome::Failed, Vec::new(), None),
            Ok(DispatchResult::Ambiguous) | Err(_) => (Outcome::Ambiguous, Vec::new(), None),
        };
        let terminal = match outcome {
            Outcome::Confirmed => TerminalOutcome::Confirmed,
            Outcome::Failed => TerminalOutcome::Failed,
            Outcome::Ambiguous => TerminalOutcome::Ambiguous,
            Outcome::Rejected => unreachable!(),
        };
        self.ledger.finish(&key, terminal)?;
        let receipt = self.make_receipt(
            request,
            &approval,
            &dispatched,
            outcome,
            Some(plan_hash),
            Some(facts_hash),
            input.as_bytes(),
            &response,
            provider_id,
            true,
        )?;
        self.ledger.issue_receipt(&key, receipt.1.clone())?;
        let _ = planned_state;
        Ok(InvokeResult {
            receipt: receipt.0,
            canonical_receipt: receipt.1,
            redelivery: false,
        })
    }

    fn redelivery(&self, state: crate::ledger::EntrySnapshot) -> Result<InvokeResult, BrokerError> {
        match state.state {
            InvocationState::ReceiptIssued { receipt, .. } => {
                let parsed: ParsedArtifact<InvocationReceipt> = crate::artifacts::parse(&receipt)?;
                Ok(InvokeResult {
                    receipt: parsed.view,
                    canonical_receipt: receipt,
                    redelivery: true,
                })
            }
            InvocationState::Terminal(TerminalOutcome::Ambiguous) => Err(BrokerError::Brk306),
            _ => Err(BrokerError::Brk204),
        }
    }

    fn pre_dispatch_receipt(
        &self,
        request: InvokeRequest<'_>,
        approval: ImplementationApproval,
        released: crate::ledger::EntrySnapshot,
        input: &[u8],
    ) -> Result<InvokeResult, BrokerError> {
        let receipt = self.make_receipt(
            request,
            &approval,
            &released,
            Outcome::Rejected,
            None,
            None,
            input,
            &[],
            None,
            false,
        )?;
        self.ledger
            .issue_receipt(&released.key, receipt.1.clone())?;
        Ok(InvokeResult {
            receipt: receipt.0,
            canonical_receipt: receipt.1,
            redelivery: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn make_receipt(
        &self,
        request: InvokeRequest<'_>,
        approval: &ImplementationApproval,
        state: &crate::ledger::EntrySnapshot,
        outcome: Outcome,
        plan_hash: Option<String>,
        facts_hash: Option<String>,
        input: &[u8],
        response: &[u8],
        provider_id: Option<String>,
        dispatched: bool,
    ) -> Result<(InvocationReceipt, Vec<u8>), BrokerError> {
        let grant = &request.grant.grant;
        let (bundle, flow_hash, lock_hash, flow, node, alias, run) = grant.subject.flow_node_run();
        let attempt = state.dispatch_attempts as u64;
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
                VerificationTier::VerifierWithDisclosure,
            )?
            .0;
        let input_commitment = self
            .commitments
            .commit(
                &grant.org_id,
                "canonical_input_commitment",
                &input_salt,
                input,
                VerificationTier::VerifierWithDisclosure,
            )?
            .0;
        let response_commitment = self
            .commitments
            .commit(
                &grant.org_id,
                "response_commitment",
                &response_salt,
                response,
                VerificationTier::BrokerOnly,
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
            policy_hash: approval.policy_hash.clone(),
            contract_hash: grant.contract_hash.clone(),
            plugin_module_sha256: approval.plugin_module_sha256.clone(),
            plugin_trust_tier: approval.trust_tier.clone(),
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
            request_plan_hash: plan_hash,
            authority_facts_hash: facts_hash,
            budget_before: state.budget_before,
            budget_after: state.budget_after,
            provider_request_id: provider_id,
            response_commitment,
            outcome,
            claims: ReceiptClaims {
                trusted_host_scope_authenticated: true,
                broker_admission_enforced: true,
                provider_dispatch_observed: dispatched,
                remote_durable_state_proven: false,
                verifiable_execution_proven: false,
            },
            issued_at: self.clock.now_rfc3339(),
            signature: placeholder,
            extensions: Default::default(),
        };
        let unsigned = canonical::from_serde(&receipt, RECEIPT_MAX)?.into_bytes();
        receipt.signature = self.signer.sign_json(RECEIPT_DOMAIN, &unsigned)?;
        let bytes = canonical::from_serde(&receipt, RECEIPT_MAX)?.into_bytes();
        Ok((receipt, bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        commitment::CommitmentKey,
        custodian::SyntheticCustodian,
        dispatch::{MockDispatcher, ScriptedDispatch},
        effect_id,
        grant::{ExactPopVerifier, FixedClock},
        ledger::InMemoryLedger,
        signing::{BINDING_DOMAIN, BrokerSigner},
    };

    fn signed_binding(signer: &BrokerSigner) -> ParsedArtifact<BindingAttestation> {
        let placeholder = SignatureEnvelope {
            alg: SignatureAlg::Ed25519,
            key_id: signer.key_id().into(),
            value: String::new(),
        };
        let mut binding = BindingAttestation {
            schema_version: "0.1".into(),
            critical_fields: vec![],
            org_id: "org".into(),
            principal: PrincipalRef {
                kind: PrincipalKind::Broker,
                id: "broker".into(),
            },
            issuer: "issuer".into(),
            broker_key_id: signer.key_id().into(),
            lane: "semantic_broker".into(),
            connection_ref: "connection".into(),
            provider: "synthetic".into(),
            account_commitment: CommitmentEnvelope {
                alg: CommitmentAlg::HmacSha256,
                key_id: "commit-v1".into(),
                verification_tier: None,
                value: format!("hmac-sha256:{}", "0".repeat(64)),
                extensions: Default::default(),
            },
            roles: [("role".into(), "synthetic.secret".into())].into(),
            scope_alignment: ScopeAlignment {
                required_scopes: vec!["scope".into()],
                actual_scopes: vec!["scope".into()],
                satisfied: true,
                extensions: Default::default(),
            },
            supported_contracts: vec![SupportedContract {
                contract_id: "contract@1".into(),
                contract_hash: format!("sha256:{}", "1".repeat(64)),
                observed_plugin_module_sha256: None,
                attenuation_profiles: vec![],
                extensions: Default::default(),
            }],
            endpoint_origins: vec!["https://provider.example".into()],
            revocation_epoch: 4,
            observed_at: "2026-07-19T11:00:00Z".into(),
            expires_at: "2026-07-19T13:00:00Z".into(),
            signature: placeholder,
            extensions: Default::default(),
        };
        let unsigned = canonical::from_serde(&binding, BINDING_MAX)
            .unwrap()
            .into_bytes();
        binding.signature = signer.sign_json(BINDING_DOMAIN, &unsigned).unwrap();
        let bytes = canonical::from_serde(&binding, BINDING_MAX)
            .unwrap()
            .into_bytes();
        crate::artifacts::parse(&bytes).unwrap()
    }

    fn grant(
        scope: &TrustedHostScope,
        binding: &BindingAttestation,
        pop: &PopSession,
    ) -> ExecutionGrantRecord {
        let subject = GrantSubject::FlowNodeRun {
            bundle_id: "bundle".into(),
            flow_ir_hash: format!("sha256:{}", "2".repeat(64)),
            binding_lock_hash: format!("sha256:{}", "3".repeat(64)),
            flow_id: scope.flow_id().into(),
            node_id: scope.node_id().into(),
            node_alias: scope.node_alias().into(),
            run_id: scope.run_id().into(),
        };
        ExecutionGrantRecord::from_grant(&ExecutionGrant {
            schema_version: "0.1".into(),
            critical_fields: vec![],
            org_id: scope.org_id().into(),
            principal: PrincipalRef {
                kind: PrincipalKind::Deployment,
                id: "deployment".into(),
            },
            grant_ref: "grant_00000000000000000000000000000001".into(),
            issuer: "issuer".into(),
            audience: "broker-execution".into(),
            channel_binding: ChannelBinding {
                method: pop.method.clone(),
                key_thumbprint: pop.key_thumbprint.clone(),
                session_id: pop.session_id.clone(),
            },
            subject,
            operation_contract: "contract@1".into(),
            contract_hash: format!("sha256:{}", "1".repeat(64)),
            connection_ref: binding.connection_ref.clone(),
            budgets: GrantBudgets {
                logical_calls: 1,
                dispatch_attempts_per_call: 1,
            },
            aggregate_budgets: None,
            minimum_assurance: Assurance::BrokeredCount,
            required_attenuations: vec![],
            revocation_epoch: 4,
            not_before: "2026-07-19T11:55:00Z".into(),
            expires_at: "2026-07-19T12:05:00Z".into(),
            jti: "jti_00000000000000000000000000000001".into(),
            extensions: Default::default(),
        })
        .unwrap()
    }

    #[test]
    fn adversarial_replay_redelivery_receipt_and_secret_hygiene() {
        let binding_signer = BrokerSigner::from_seed("binding-key", [1; 32]);
        let receipt_signer = BrokerSigner::from_seed("receipt-key", [2; 32]);
        let binding = signed_binding(&binding_signer);
        let scope = TrustedHostScope::from_authenticated_host(
            "org",
            "deployment",
            "bundle",
            format!("sha256:{}", "2".repeat(64)),
            format!("sha256:{}", "3".repeat(64)),
            "flow",
            "node",
            "alias",
            "run",
        )
        .unwrap();
        let pop = PopSession {
            method: ChannelMethod::DeploymentKey,
            key_thumbprint: format!("sha256:{}", "4".repeat(64)),
            session_id: "session_000000000000000000000000000001".into(),
            proof: b"proof".to_vec(),
        };
        let pop_verifier = ExactPopVerifier {
            expected_proof: b"proof".to_vec(),
        };
        let grant = grant(&scope, &binding.view, &pop);
        let approval = ImplementationApproval {
            contract_hash: grant.grant.contract_hash.clone(),
            implementation: "impl-v1".into(),
            plugin_module_sha256: format!("sha256:{}", "5".repeat(64)),
            trust_tier: PluginTrustTier::LatticeFirstParty,
            policy_hash: format!("sha256:{}", "6".repeat(64)),
        };
        let trust = StaticTrustRegistry {
            approval,
            contract_id: "contract@1".into(),
            binding_issuer: "issuer".into(),
            binding_key_id: "binding-key".into(),
            verifier: binding_signer.verifying_key(),
        };
        let ledger = InMemoryLedger::new();
        let planner = FixedTemplatePlanner {
            template: br#"{"idempotency":{"$broker":"idempotency_key"},"method":"POST"}"#.to_vec(),
            facts: br#"{"allowed":true}"#.to_vec(),
            implementation: "impl-v1".into(),
            endpoint_origin: "https://provider.example".into(),
        };
        let dispatcher = MockDispatcher::new([ScriptedDispatch::Confirmed {
            projection: br#"{"ok":true}"#.to_vec(),
            provider_request_id: Some("request-1".into()),
        }]);
        let custodian = SyntheticCustodian::new(
            "connection",
            "synthetic",
            "account",
            ["scope".into()],
            b"custodian-super-secret".to_vec(),
        );
        let clock = FixedClock("2026-07-19T12:00:00Z".into());
        let commitments = CommitmentKey::new("commit-v1", [8; 32]).unwrap();
        let engine = BrokerEngine {
            ledger: &ledger,
            planner: &planner,
            dispatcher: &dispatcher,
            custodian: &custodian,
            trust: &trust,
            clock: &clock,
            signer: &receipt_signer,
            commitments: &commitments,
            broker_principal_id: "broker",
        };
        let effect = effect_id::derive("run", "node", 1, "send").unwrap();
        let input = br#"{"message":"input-super-secret"}"#;
        let invoke = || InvokeRequest {
            scope: &scope,
            grant: &grant,
            binding: &binding,
            pop: &pop,
            pop_verifier: &pop_verifier,
            current_revocation_epoch: 4,
            asserted_operation_contract: "contract@1",
            asserted_connection_ref: "connection",
            logical_effect_id: &effect,
            canonical_input: input,
            lease_seconds: 30,
        };
        assert_eq!(
            engine
                .invoke(InvokeRequest {
                    asserted_operation_contract: "wrong-contract",
                    ..invoke()
                })
                .unwrap_err(),
            BrokerError::Brk107
        );
        assert_eq!(
            engine
                .invoke(InvokeRequest {
                    asserted_connection_ref: "wrong-connection",
                    ..invoke()
                })
                .unwrap_err(),
            BrokerError::Brk109
        );
        assert!(dispatcher.recorded_plans().is_empty());
        let first = engine.invoke(invoke()).unwrap();
        assert!(!first.redelivery);
        assert!(first.receipt.claims.provider_dispatch_observed);
        assert!(!first.receipt.claims.remote_durable_state_proven);
        assert!(!first.receipt.claims.verifiable_execution_proven);
        assert_eq!(
            (first.receipt.budget_before, first.receipt.budget_after),
            (1, 0)
        );
        assert_eq!(first.receipt.grant_hash, grant.hash());
        receipt_signer
            .verifying_key()
            .verify_json(
                RECEIPT_DOMAIN,
                &first.canonical_receipt,
                &first.receipt.signature,
            )
            .unwrap();
        let tampered = String::from_utf8(first.canonical_receipt.clone())
            .unwrap()
            .replace("request-1", "request-2");
        assert!(
            receipt_signer
                .verifying_key()
                .verify_json(
                    RECEIPT_DOMAIN,
                    tampered.as_bytes(),
                    &first.receipt.signature
                )
                .is_err()
        );
        let second = engine.invoke(invoke()).unwrap();
        assert!(second.redelivery);
        assert_eq!(dispatcher.recorded_plans().len(), 1);
        assert_eq!(first.canonical_receipt, second.canonical_receipt);
        let altered = InvokeRequest {
            canonical_input: br#"{"message":"altered"}"#,
            ..invoke()
        };
        let error = engine.invoke(altered).unwrap_err();
        assert_eq!(error, BrokerError::Brk203);
        assert_eq!(error.severity(), crate::Severity::Fatal);
        assert_eq!(dispatcher.recorded_plans().len(), 1);
        let public = format!(
            "{error} {:?} {}",
            first.receipt,
            String::from_utf8_lossy(&first.canonical_receipt)
        );
        assert!(!public.contains("custodian-super-secret"));
        assert!(!public.contains("input-super-secret"));
    }

    #[test]
    fn admission_codes_fail_before_dispatch() {
        let signer = BrokerSigner::from_seed("binding-key", [1; 32]);
        let binding = signed_binding(&signer);
        let scope = TrustedHostScope::from_authenticated_host(
            "org",
            "deployment",
            "bundle",
            format!("sha256:{}", "2".repeat(64)),
            format!("sha256:{}", "3".repeat(64)),
            "flow",
            "node",
            "alias",
            "run",
        )
        .unwrap();
        let pop = PopSession {
            method: ChannelMethod::DeploymentKey,
            key_thumbprint: format!("sha256:{}", "4".repeat(64)),
            session_id: "session".into(),
            proof: b"proof".to_vec(),
        };
        let verifier = ExactPopVerifier {
            expected_proof: b"proof".to_vec(),
        };
        let record = grant(&scope, &binding.view, &pop);
        assert_eq!(
            crate::grant::validate_grant(
                &record,
                &scope,
                &pop,
                &ExactPopVerifier {
                    expected_proof: b"wrong".to_vec()
                },
                4,
                "2026-07-19T12:00:00Z"
            ),
            Err(BrokerError::Brk102)
        );
        assert_eq!(
            crate::grant::validate_grant(
                &record,
                &scope,
                &pop,
                &verifier,
                5,
                "2026-07-19T12:00:00Z"
            ),
            Err(BrokerError::Brk106)
        );
        assert_eq!(
            crate::grant::validate_grant(
                &record,
                &scope,
                &pop,
                &verifier,
                4,
                "2026-07-19T11:00:00Z"
            ),
            Err(BrokerError::Brk104)
        );
        assert_eq!(
            crate::grant::validate_grant(
                &record,
                &scope,
                &pop,
                &verifier,
                4,
                "2026-07-19T13:00:00Z"
            ),
            Err(BrokerError::Brk105)
        );
        let wrong = TrustedHostScope::from_authenticated_host(
            "org",
            "deployment",
            "bundle",
            format!("sha256:{}", "2".repeat(64)),
            format!("sha256:{}", "3".repeat(64)),
            "wrong-flow",
            "node",
            "alias",
            "run",
        )
        .unwrap();
        assert_eq!(
            crate::grant::validate_grant(
                &record,
                &wrong,
                &pop,
                &verifier,
                4,
                "2026-07-19T12:00:00Z"
            ),
            Err(BrokerError::Brk107)
        );
    }
}
