use crate::admission_authority::*;
use broker_core::{
    artifacts::SignatureAlg,
    canonical,
    credential::{lifecycle::DispatchAdmissionLs1, parse},
    signing::BrokerSigner,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const TENANT: &str = "tenant-a";
const DEPLOYMENT: &str = "deployment-a";
const ACTOR: &str = "hmac-sha256:1111111111111111111111111111111111111111111111111111111111111111";
const ACCOUNT: &str =
    "hmac-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const INPUT_A: &str =
    "hmac-sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const INPUT_B: &str =
    "hmac-sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const CONTROL_KEY_ID: &str = "control-key";
const ADMISSION_KEY_ID: &str = "admission-key";

fn hash(label: &str) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(label.as_bytes())))
}

fn head(reference: &str, epoch: u64) -> ArtifactHead {
    ArtifactHead {
        reference: reference.into(),
        hash: hash(reference),
        epoch,
    }
}

fn admission_signer() -> BrokerSigner {
    BrokerSigner::from_seed(ADMISSION_KEY_ID, [9; 32])
}

fn control_signer() -> BrokerSigner {
    BrokerSigner::from_seed(CONTROL_KEY_ID, [7; 32])
}

fn grant(lineage: &str, version: &str, epoch: u64) -> ProviderGrantHead {
    ProviderGrantHead {
        provider_grant_lineage_ref: lineage.into(),
        provider_grant_version_ref: version.into(),
        provider_grant_version_hash: hash(version),
        account_subject_commitment: ACCOUNT.into(),
        authority_epoch: epoch,
        fence_epoch: 0,
        status: ProviderGrantStatus::Current,
    }
}

fn acl(version: &str, epoch: u64) -> AclHead {
    AclHead {
        key: AclKey {
            actor_subject_commitment: ACTOR.into(),
            provider_grant_version_ref: version.into(),
        },
        account_subject_commitment: ACCOUNT.into(),
        acl_epoch: epoch,
        selector_hash: hash("selector"),
        record_commitment:
            "hmac-sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into(),
        record_hash: hash("acl-record"),
        status: AclStatus::Active,
    }
}

fn ceilings(maximum: u64) -> BudgetCeilings {
    BudgetCeilings {
        flow_max: maximum,
        node_max: maximum,
        connection_lineage_max: maximum,
        account_partition_max: maximum,
    }
}

fn bootstrap(maximum: u64) -> AuthorityBootstrap {
    let control = control_signer();
    AuthorityBootstrap {
        tenant_id: TENANT.into(),
        deployment_id: DEPLOYMENT.into(),
        cutover: head("cutover-1", 1),
        control_epoch: 1,
        heads: ControlHeads {
            standing: head("standing-1", 1),
            contract_set: head("contracts-1", 1),
            policy: head("policy-1", 1),
            registry_vector: head("registry-1", 1),
        },
        provider_grants: vec![
            grant("lineage-a", "grant-a1", 1),
            grant("lineage-b", "grant-b1", 1),
        ],
        acls: vec![acl("grant-a1", 1), acl("grant-b1", 1)],
        legacy_inventory: SealedLegacyInventory {
            inventory_ref: "legacy-inventory-1".into(),
            inventory_hash: hash("legacy-inventory-1"),
            sealed: true,
        },
        trusted_broker_partitions: BTreeSet::from(["broker-a".into(), "broker-b".into()]),
        default_ceilings: ceilings(maximum),
        control_key_id: CONTROL_KEY_ID.into(),
        control_public_key: control.verifying_key().to_bytes(),
        admission_key_id: ADMISSION_KEY_ID.into(),
    }
}

fn initialized(maximum: u64) -> AdmissionAuthorityState {
    let result = transition(
        None,
        AdmissionAuthorityCommand::Initialize(bootstrap(maximum)),
        &admission_signer(),
    )
    .unwrap();
    result.state.unwrap()
}

fn context(
    effect_label: &str,
    binding: &str,
    lineage: &str,
    version: &str,
    broker: &str,
) -> AdmissionContext {
    AdmissionContext {
        broker_partition: broker.into(),
        run_id: "run-a".into(),
        node_id: "node-a".into(),
        logical_effect_id: hash(effect_label),
        canonical_input_commitment: INPUT_A.into(),
        binding_ref: binding.into(),
        binding_hash: hash(binding),
        provider_grant_version_ref: version.into(),
        provider_grant_version_hash: hash(version),
        provider_grant_lineage_ref: lineage.into(),
        account_partition: AccountBudgetPartition {
            provider: "google".into(),
            auth_profile_ref: "auth.google.workspace.oauth2".into(),
            auth_profile_version: "1".into(),
            account_subject_commitment: ACCOUNT.into(),
        },
        actor_subject_commitment: ACTOR.into(),
        acl_epoch: 1,
        acl_selector_hash: hash("selector"),
        acl_record_commitment:
            "hmac-sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into(),
        acl_record_hash: hash("acl-record"),
        contract_id: "connector.google.test@1".into(),
        contract_hash: hash("contract"),
        registry_vector_ref: "registry-1".into(),
        registry_vector_hash: hash("registry-1"),
        registry_vector_epoch: 1,
    }
}

fn reserve(
    state: AdmissionAuthorityState,
    context: AdmissionContext,
) -> Result<(AdmissionAuthorityState, AdmissionAuthorityReply), AdmissionAuthorityError> {
    let command = ReserveEffectCommand {
        expected_state_version: state.state_version,
        expected_control_epoch: state.control_epoch,
        context,
    };
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::ReserveEffect(command),
        &admission_signer(),
    )?;
    Ok((result.state.unwrap(), result.reply))
}

fn issue(
    state: AdmissionAuthorityState,
    effect: &str,
    broker: &str,
) -> Result<(AdmissionAuthorityState, Vec<u8>), AdmissionAuthorityError> {
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::IssueAdmission(IssueAdmissionCommand {
            expected_state_version: state.state_version,
            expected_control_epoch: state.control_epoch,
            broker_partition: broker.into(),
            run_id: "run-a".into(),
            logical_effect_id: hash(effect),
            attempt: 1,
            not_before: "2026-07-30T12:00:00Z".into(),
            expires_at: "2026-07-30T12:05:00Z".into(),
            issued_at: "2026-07-30T12:00:00Z".into(),
        }),
        &admission_signer(),
    )?;
    let AdmissionAuthorityReply::Admission {
        canonical_admission,
        ..
    } = result.reply
    else {
        panic!("expected admission")
    };
    Ok((result.state.unwrap(), canonical_admission))
}

fn reserve_and_issue(
    state: AdmissionAuthorityState,
    effect: &str,
    binding: &str,
    lineage: &str,
    version: &str,
    broker: &str,
) -> AdmissionAuthorityState {
    let (state, _) = reserve(state, context(effect, binding, lineage, version, broker)).unwrap();
    issue(state, effect, broker).unwrap().0
}

fn replace_control(
    state: &AdmissionAuthorityState,
    maximum: u64,
) -> RegistryControlAuthorityCommand {
    RegistryControlAuthorityCommand {
        expected_state_version: state.state_version,
        expected_control_epoch: state.control_epoch,
        new_control_epoch: state.control_epoch + 1,
        heads: ControlHeads {
            standing: head("standing-2", 2),
            contract_set: head("contracts-2", 2),
            policy: head("policy-2", 2),
            registry_vector: head("registry-2", 2),
        },
        replacement_policy_ceilings: ceilings(maximum),
    }
}

fn accept_grant_and_acl(
    mut state: AdmissionAuthorityState,
    head: ProviderGrantHead,
    acl: AclHead,
) -> AdmissionAuthorityState {
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::ProviderGrant(ProviderGrantAuthorityCommand::AcceptHead {
            expected_state_version: state.state_version,
            expected_control_epoch: state.control_epoch,
            head,
        }),
        &admission_signer(),
    )
    .unwrap();
    state = result.state.unwrap();
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::ActorAcl(ActorAclAuthorityCommand::Accept {
            expected_state_version: state.state_version,
            expected_control_epoch: state.control_epoch,
            head: acl,
        }),
        &admission_signer(),
    )
    .unwrap();
    result.state.unwrap()
}

fn signed_amendment(state: &AdmissionAuthorityState, maximum: u64) -> Vec<u8> {
    let signer = control_signer();
    let run = &state.runs["run-a"];
    let critical_fields = vec![
        "/amendment_epoch",
        "/artifact_type",
        "/authority_model_revision",
        "/ceiling_amendment_ref",
        "/change_kind",
        "/deployment_id",
        "/issued_at",
        "/issuer",
        "/key_id",
        "/new_ceilings",
        "/previous_ceiling_hash",
        "/run_id",
        "/schema_version",
        "/standing_authority_ref",
        "/tenant_id",
    ];
    let mut value = serde_json::json!({
        "schema_version":"0.2",
        "authority_model_revision":"lifecycle-separated-1",
        "artifact_type":"CeilingAmendment",
        "ceiling_amendment_ref":format!("ceiling-{}", run.ceiling_amendment_epoch + 1),
        "tenant_id":TENANT,
        "deployment_id":DEPLOYMENT,
        "run_id":"run-a",
        "amendment_epoch":run.ceiling_amendment_epoch + 1,
        "previous_ceiling_hash":run.ceiling_amendment_head_hash,
        "new_ceilings":ceilings(maximum),
        "change_kind":"monotonic_widening",
        "standing_authority_ref":state.heads.standing.reference,
        "issuer":"operator-a",
        "key_id":CONTROL_KEY_ID,
        "issued_at":"2026-07-30T12:00:00Z",
        "critical_fields":critical_fields,
        "signature":{"algorithm":"Ed25519","key_id":CONTROL_KEY_ID,"value":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
    });
    let unsigned = canonical::from_serde(&value, 64 * 1024).unwrap();
    let signature = signer
        .sign_json(
            "lattice.credential-plane.0.2.lifecycle-separated-1.ceiling-amendment",
            unsigned.as_bytes(),
        )
        .unwrap();
    value["signature"] = serde_json::json!({
        "algorithm":match signature.alg { SignatureAlg::Ed25519 => "Ed25519" },
        "key_id":signature.key_id,
        "value":signature.value,
    });
    canonical::from_serde(&value, 64 * 1024)
        .unwrap()
        .into_bytes()
}

#[test]
fn admission_authority_budgets_span_binding_revisions() {
    let state = initialized(4);
    let state = reserve_and_issue(
        state,
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let state = reserve_and_issue(
        state,
        "effect-2",
        "binding-2",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let run = &state.runs["run-a"];
    assert_eq!(run.flow.as_ref().unwrap().consumed, 2);
    assert_eq!(run.connection_partitions["lineage-a"].consumed, 2);
}

#[test]
fn admission_authority_budgets_span_versions_and_account_lineages() {
    let state = reserve_and_issue(
        initialized(5),
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let state = accept_grant_and_acl(state, grant("lineage-a", "grant-a2", 2), acl("grant-a2", 1));
    let state = reserve_and_issue(
        state,
        "effect-2",
        "binding-2",
        "lineage-a",
        "grant-a2",
        "broker-a",
    );
    let state = reserve_and_issue(
        state,
        "effect-3",
        "binding-3",
        "lineage-b",
        "grant-b1",
        "broker-a",
    );
    let run = &state.runs["run-a"];
    assert_eq!(run.connection_partitions["lineage-a"].consumed, 2);
    assert_eq!(
        run.account_partitions
            .values()
            .next()
            .unwrap()
            .counter
            .consumed,
        3
    );
}

#[test]
fn admission_authority_alias_changes_cannot_reset_counts() {
    let state = reserve_and_issue(
        initialized(3),
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let state = reserve_and_issue(
        state,
        "effect-2",
        "binding-2",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    assert_eq!(state.runs["run-a"].account_partitions.len(), 1);
    assert_eq!(
        state.runs["run-a"]
            .account_partitions
            .values()
            .next()
            .unwrap()
            .counter
            .consumed,
        2
    );
}

#[test]
fn admission_authority_broker_partitions_share_one_ceiling_and_outsider_fails() {
    let state = reserve_and_issue(
        initialized(2),
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let state = reserve_and_issue(
        state,
        "effect-2",
        "binding-2",
        "lineage-a",
        "grant-a1",
        "broker-b",
    );
    let exhausted = reserve(
        state.clone(),
        context("effect-3", "binding-3", "lineage-a", "grant-a1", "broker-a"),
    )
    .unwrap_err();
    assert_eq!(exhausted, AdmissionAuthorityError::BudgetExhausted);
    let outside = reserve(
        state,
        context("effect-3", "binding-3", "lineage-a", "grant-a1", "broker-c"),
    )
    .unwrap_err();
    assert_eq!(outside, AdmissionAuthorityError::BrokerOutsideAuthority);
}

#[test]
fn admission_authority_tightening_blocks_and_replacement_cannot_widen() {
    let state = reserve_and_issue(
        initialized(3),
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let (state, _) = reserve(
        state,
        context(
            "effect-reserved",
            "binding-reserved",
            "lineage-a",
            "grant-a1",
            "broker-a",
        ),
    )
    .unwrap();
    assert_eq!(state.runs["run-a"].flow.as_ref().unwrap().consumed, 1);
    assert_eq!(state.runs["run-a"].flow.as_ref().unwrap().reserved, 1);
    let widened = transition(
        Some(&state),
        AdmissionAuthorityCommand::RegistryControl(replace_control(&state, 4)),
        &admission_signer(),
    )
    .unwrap_err();
    assert_eq!(widened, AdmissionAuthorityError::WideningRequiresAmendment);
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::RegistryControl(replace_control(&state, 1)),
        &admission_signer(),
    )
    .unwrap();
    let state = result.state.unwrap();
    let mut next = context("effect-2", "binding-2", "lineage-a", "grant-a1", "broker-a");
    next.registry_vector_ref = "registry-2".into();
    next.registry_vector_hash = hash("registry-2");
    next.registry_vector_epoch = 2;
    assert_eq!(
        reserve(state, next).unwrap_err(),
        AdmissionAuthorityError::BudgetExhausted
    );
}

#[test]
fn admission_authority_signed_amendment_widens_once() {
    let state = reserve_and_issue(
        initialized(1),
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let amendment = signed_amendment(&state, 2);
    let expected = state.state_version;
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::ApplyCeilingAmendment {
            expected_state_version: expected,
            amendment: amendment.clone(),
        },
        &admission_signer(),
    )
    .unwrap();
    let state = result.state.unwrap();
    let redelivery = transition(
        Some(&state),
        AdmissionAuthorityCommand::ApplyCeilingAmendment {
            expected_state_version: expected,
            amendment,
        },
        &admission_signer(),
    )
    .unwrap();
    assert!(matches!(
        redelivery.reply,
        AdmissionAuthorityReply::AmendmentAccepted {
            redelivery: true,
            ..
        }
    ));
    reserve_and_issue(
        state,
        "effect-2",
        "binding-2",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
}

#[test]
fn admission_authority_changed_binding_aborts_without_duplicate() {
    let (state, _) = reserve(
        initialized(2),
        context("effect-1", "binding-1", "lineage-a", "grant-a1", "broker-a"),
    )
    .unwrap();
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::ReserveEffect(ReserveEffectCommand {
            expected_state_version: state.state_version,
            expected_control_epoch: state.control_epoch,
            context: context("effect-1", "binding-2", "lineage-a", "grant-a1", "broker-a"),
        }),
        &admission_signer(),
    )
    .unwrap();
    let state = result.state.unwrap();
    let run = &state.runs["run-a"];
    assert_eq!(run.effects.len(), 1);
    assert_eq!(
        run.effects[&hash("effect-1")].status,
        ReservationStatus::Aborted
    );
    assert_eq!(run.flow.as_ref().unwrap().reserved, 0);
}

#[test]
fn admission_authority_different_input_conflicts() {
    let (state, _) = reserve(
        initialized(2),
        context("effect-1", "binding-1", "lineage-a", "grant-a1", "broker-a"),
    )
    .unwrap();
    let mut conflicting = context("effect-1", "binding-1", "lineage-a", "grant-a1", "broker-a");
    conflicting.canonical_input_commitment = INPUT_B.into();
    assert_eq!(
        reserve(state, conflicting).unwrap_err(),
        AdmissionAuthorityError::EffectConflict
    );
}

#[test]
fn admission_authority_crash_boundaries_redeliver_exact_state() {
    let state = initialized(2);
    let command = ReserveEffectCommand {
        expected_state_version: state.state_version,
        expected_control_epoch: state.control_epoch,
        context: context("effect-1", "binding-1", "lineage-a", "grant-a1", "broker-a"),
    };
    let unpersisted = transition(
        Some(&state),
        AdmissionAuthorityCommand::ReserveEffect(command.clone()),
        &admission_signer(),
    )
    .unwrap();
    assert!(state.runs.is_empty());
    let persisted = unpersisted.state.unwrap();
    let redelivery = transition(
        Some(&persisted),
        AdmissionAuthorityCommand::ReserveEffect(command),
        &admission_signer(),
    )
    .unwrap();
    assert!(!redelivery.mutated);
    let (persisted, admission) = issue(persisted, "effect-1", "broker-a").unwrap();
    let repeated = transition(
        Some(&persisted),
        AdmissionAuthorityCommand::IssueAdmission(IssueAdmissionCommand {
            expected_state_version: persisted.state_version - 1,
            expected_control_epoch: persisted.control_epoch,
            broker_partition: "broker-a".into(),
            run_id: "run-a".into(),
            logical_effect_id: hash("effect-1"),
            attempt: 1,
            not_before: "2026-07-30T12:00:00Z".into(),
            expires_at: "2026-07-30T12:05:00Z".into(),
            issued_at: "2026-07-30T12:00:00Z".into(),
        }),
        &admission_signer(),
    )
    .unwrap();
    let AdmissionAuthorityReply::Admission {
        canonical_admission,
        redelivery,
        ..
    } = repeated.reply
    else {
        panic!()
    };
    assert!(redelivery);
    assert_eq!(canonical_admission, admission);
    parse::<DispatchAdmissionLs1>(&admission).unwrap();
}

#[test]
fn admission_authority_stale_cas_and_concurrent_fence_order() {
    let (reserved, _) = reserve(
        initialized(2),
        context("effect-1", "binding-1", "lineage-a", "grant-a1", "broker-a"),
    )
    .unwrap();
    let stale_epoch = transition(
        Some(&reserved),
        AdmissionAuthorityCommand::ProviderGrant(ProviderGrantAuthorityCommand::Fence {
            expected_state_version: reserved.state_version,
            expected_control_epoch: reserved.control_epoch - 1,
            provider_grant_lineage_ref: "lineage-a".into(),
            fence_epoch: 1,
            revoked: true,
        }),
        &admission_signer(),
    )
    .unwrap_err();
    assert_eq!(stale_epoch, AdmissionAuthorityError::StaleCas);
    let fence = ProviderGrantAuthorityCommand::Fence {
        expected_state_version: reserved.state_version,
        expected_control_epoch: reserved.control_epoch,
        provider_grant_lineage_ref: "lineage-a".into(),
        fence_epoch: 1,
        revoked: true,
    };
    let fenced = transition(
        Some(&reserved),
        AdmissionAuthorityCommand::ProviderGrant(fence.clone()),
        &admission_signer(),
    )
    .unwrap()
    .state
    .unwrap();
    assert_eq!(
        issue(fenced, "effect-1", "broker-a").unwrap_err(),
        AdmissionAuthorityError::ProviderGrantBlocked
    );
    let (admitted, _) = issue(reserved.clone(), "effect-1", "broker-a").unwrap();
    let stale = transition(
        Some(&admitted),
        AdmissionAuthorityCommand::ProviderGrant(fence),
        &admission_signer(),
    )
    .unwrap_err();
    assert_eq!(stale, AdmissionAuthorityError::StaleCas);
    assert_eq!(
        admitted.runs["run-a"].effects[&hash("effect-1")].status,
        ReservationStatus::Admitted
    );
}

#[test]
fn admission_authority_control_before_issuance_blocks_but_after_does_not_revoke() {
    let (reserved, _) = reserve(
        initialized(3),
        context("effect-1", "binding-1", "lineage-a", "grant-a1", "broker-a"),
    )
    .unwrap();
    let changed = transition(
        Some(&reserved),
        AdmissionAuthorityCommand::RegistryControl(replace_control(&reserved, 2)),
        &admission_signer(),
    )
    .unwrap()
    .state
    .unwrap();
    assert_eq!(
        issue(changed, "effect-1", "broker-a").unwrap_err(),
        AdmissionAuthorityError::ReservationBlocked
    );

    let (reserved, _) = reserve(
        initialized(3),
        context("effect-2", "binding-1", "lineage-a", "grant-a1", "broker-a"),
    )
    .unwrap();
    let (admitted, _) = issue(reserved, "effect-2", "broker-a").unwrap();
    let changed = transition(
        Some(&admitted),
        AdmissionAuthorityCommand::RegistryControl(replace_control(&admitted, 2)),
        &admission_signer(),
    )
    .unwrap()
    .state
    .unwrap();
    assert_eq!(
        changed.runs["run-a"].effects[&hash("effect-2")].status,
        ReservationStatus::Admitted
    );
}

#[test]
fn admission_authority_terminal_redelivery_is_idempotent() {
    let state = reserve_and_issue(
        initialized(2),
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let admission = state.runs["run-a"].effects[&hash("effect-1")].attempts[&1]
        .canonical_admission
        .clone();
    let command = TerminalReceiptAuthorityCommand {
        expected_state_version: state.state_version,
        expected_control_epoch: state.control_epoch,
        run_id: "run-a".into(),
        logical_effect_id: hash("effect-1"),
        attempt: 1,
        admission_hash: format!("sha256:{}", hex::encode(Sha256::digest(&admission))),
        terminal_evidence_commitment: "terminal-evidence-a".into(),
    };
    let result = transition(
        Some(&state),
        AdmissionAuthorityCommand::TerminalReceipt(command.clone()),
        &admission_signer(),
    )
    .unwrap();
    let AdmissionAuthorityReply::TerminalAccepted {
        terminal_evidence_commitment: first_evidence,
        redelivery: false,
        ..
    } = result.reply
    else {
        panic!("expected first terminal evidence")
    };
    let state = result.state.unwrap();
    let consumed = state.runs["run-a"].flow.as_ref().unwrap().consumed;
    let repeated = transition(
        Some(&state),
        AdmissionAuthorityCommand::TerminalReceipt(command),
        &admission_signer(),
    )
    .unwrap();
    assert!(!repeated.mutated);
    let AdmissionAuthorityReply::TerminalAccepted {
        terminal_evidence_commitment: repeated_evidence,
        redelivery: true,
        ..
    } = repeated.reply
    else {
        panic!("expected repeated terminal evidence")
    };
    assert_eq!(repeated_evidence, first_evidence);
    assert_eq!(
        state.runs["run-a"].flow.as_ref().unwrap().consumed,
        consumed
    );
}

#[test]
fn admission_authority_restore_preserves_counters_and_effects() {
    let state = reserve_and_issue(
        initialized(3),
        "effect-1",
        "binding-1",
        "lineage-a",
        "grant-a1",
        "broker-a",
    );
    let restored: AdmissionAuthorityState =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    assert_eq!(restored, state);
    assert_eq!(restored.runs["run-a"].flow.as_ref().unwrap().consumed, 1);
}

#[test]
fn admission_authority_extension_interfaces_are_separate_and_typed() {
    let state = initialized(2);
    let retry = RetryReconciliationAuthorityCommand {
        expected_state_version: state.state_version,
        expected_control_epoch: state.control_epoch,
        run_id: "run-a".into(),
        logical_effect_id: hash("effect-1"),
        reconciliation_commitment: "reconciliation-a".into(),
    };
    let serialized =
        serde_json::to_value(AdmissionAuthorityCommand::RetryReconciliation(retry)).unwrap();
    assert_eq!(
        serialized["command"],
        Value::String("retry_reconciliation".into())
    );
    assert!(
        serde_json::to_value(AdmissionAuthorityCommand::ProviderGrant(
            ProviderGrantAuthorityCommand::Fence {
                expected_state_version: 1,
                expected_control_epoch: 1,
                provider_grant_lineage_ref: "lineage-a".into(),
                fence_epoch: 1,
                revoked: false,
            }
        ))
        .is_ok()
    );
}
