use broker_core::{
    artifacts::{SignatureAlg, SignatureEnvelope},
    canonical,
    credential::{
        lifecycle::{CeilingAmendmentLs1, DispatchAdmissionLs1},
        parse,
    },
    signing::{BrokerSigner, BrokerVerifyingKey},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const ADMISSION_AUTHORITY_STORAGE_KEY: &str =
    "broker:admission-authority:lifecycle-separated-1";
const DISPATCH_ADMISSION_DOMAIN: &str =
    "lattice.credential-plane.0.2.lifecycle-separated-1.dispatch-admission";
const CEILING_AMENDMENT_DOMAIN: &str =
    "lattice.credential-plane.0.2.lifecycle-separated-1.ceiling-amendment";
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactHead {
    pub reference: String,
    pub hash: String,
    pub epoch: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlHeads {
    pub standing: ArtifactHead,
    pub contract_set: ArtifactHead,
    pub policy: ArtifactHead,
    pub registry_vector: ArtifactHead,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BudgetCeilings {
    pub flow_max: u64,
    pub node_max: u64,
    pub connection_lineage_max: u64,
    pub account_partition_max: u64,
}

impl BudgetCeilings {
    fn widens(&self, previous: &Self) -> bool {
        self.flow_max > previous.flow_max
            || self.node_max > previous.node_max
            || self.connection_lineage_max > previous.connection_lineage_max
            || self.account_partition_max > previous.account_partition_max
    }

    fn is_monotonic_widening_of(&self, previous: &Self) -> bool {
        self.flow_max >= previous.flow_max
            && self.node_max >= previous.node_max
            && self.connection_lineage_max >= previous.connection_lineage_max
            && self.account_partition_max >= previous.account_partition_max
            && self.widens(previous)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AccountBudgetPartition {
    pub provider: String,
    pub auth_profile_ref: String,
    pub auth_profile_version: String,
    pub account_subject_commitment: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderGrantStatus {
    Current,
    Fenced,
    Revoked,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderGrantHead {
    pub provider_grant_lineage_ref: String,
    pub provider_grant_version_ref: String,
    pub provider_grant_version_hash: String,
    pub account_subject_commitment: String,
    pub authority_epoch: u64,
    pub fence_epoch: u64,
    pub status: ProviderGrantStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AclStatus {
    Active,
    Revoked,
    Superseded,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AclKey {
    pub actor_subject_commitment: String,
    pub provider_grant_version_ref: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AclHead {
    pub key: AclKey,
    pub account_subject_commitment: String,
    pub acl_epoch: u64,
    pub selector_hash: String,
    pub record_commitment: String,
    pub record_hash: String,
    pub status: AclStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SealedLegacyInventory {
    pub inventory_ref: String,
    pub inventory_hash: String,
    pub sealed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityBootstrap {
    pub tenant_id: String,
    pub deployment_id: String,
    pub cutover: ArtifactHead,
    pub control_epoch: u64,
    pub heads: ControlHeads,
    pub provider_grants: Vec<ProviderGrantHead>,
    pub acls: Vec<AclHead>,
    pub legacy_inventory: SealedLegacyInventory,
    pub trusted_broker_partitions: BTreeSet<String>,
    pub default_ceilings: BudgetCeilings,
    pub control_key_id: String,
    pub control_public_key: [u8; 32],
    pub admission_key_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerCounter {
    pub ceiling: u64,
    pub consumed: u64,
    pub reserved: u64,
    pub ceiling_amendment_head_hash: String,
}

impl LedgerCounter {
    fn new(ceiling: u64, head: &str) -> Self {
        Self {
            ceiling,
            consumed: 0,
            reserved: 0,
            ceiling_amendment_head_hash: head.to_owned(),
        }
    }

    fn has_capacity(&self) -> bool {
        self.consumed
            .checked_add(self.reserved)
            .and_then(|value| value.checked_add(1))
            .is_some_and(|value| value <= self.ceiling)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservationStatus {
    Reserved,
    Admitted,
    Terminal,
    Aborted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdmissionContext {
    pub broker_partition: String,
    pub run_id: String,
    pub node_id: String,
    pub logical_effect_id: String,
    pub canonical_input_commitment: String,
    pub binding_ref: String,
    pub binding_hash: String,
    pub provider_grant_version_ref: String,
    pub provider_grant_version_hash: String,
    pub provider_grant_lineage_ref: String,
    pub account_partition: AccountBudgetPartition,
    pub actor_subject_commitment: String,
    pub acl_epoch: u64,
    pub acl_selector_hash: String,
    pub acl_record_commitment: String,
    pub acl_record_hash: String,
    pub contract_id: String,
    pub contract_hash: String,
    pub registry_vector_ref: String,
    pub registry_vector_hash: String,
    pub registry_vector_epoch: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Admitted,
    Terminal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdmissionAttempt {
    pub attempt: u8,
    pub canonical_admission: Vec<u8>,
    pub status: AttemptStatus,
    pub terminal_evidence_commitment: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectReservation {
    pub context: AdmissionContext,
    pub reservation_hash: String,
    pub control_epoch: u64,
    pub status: ReservationStatus,
    pub attempts: BTreeMap<u8, AdmissionAttempt>,
    pub retry_reconciliation_commitment: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AccountLedgerPartition {
    pub partition: AccountBudgetPartition,
    pub counter: LedgerCounter,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunLedger {
    pub ceilings: Option<BudgetCeilings>,
    pub ceiling_amendment_epoch: u64,
    pub ceiling_amendment_head_hash: String,
    pub applied_amendments: BTreeMap<String, String>,
    pub flow: Option<LedgerCounter>,
    pub nodes: BTreeMap<String, LedgerCounter>,
    pub connection_partitions: BTreeMap<String, LedgerCounter>,
    pub account_partitions: BTreeMap<String, AccountLedgerPartition>,
    pub effects: BTreeMap<String, EffectReservation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdmissionAuthorityState {
    pub tenant_id: String,
    pub deployment_id: String,
    pub state_version: u64,
    pub cutover: ArtifactHead,
    pub control_epoch: u64,
    pub heads: ControlHeads,
    pub provider_grants: BTreeMap<String, ProviderGrantHead>,
    pub acls: BTreeMap<String, AclHead>,
    pub legacy_inventory: SealedLegacyInventory,
    pub trusted_broker_partitions: BTreeSet<String>,
    pub default_ceilings: BudgetCeilings,
    pub control_key_id: String,
    pub control_public_key: [u8; 32],
    pub admission_key_id: String,
    pub runs: BTreeMap<String, RunLedger>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReserveEffectCommand {
    pub expected_state_version: u64,
    pub expected_control_epoch: u64,
    pub context: AdmissionContext,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IssueAdmissionCommand {
    pub expected_state_version: u64,
    pub expected_control_epoch: u64,
    pub broker_partition: String,
    pub run_id: String,
    pub logical_effect_id: String,
    pub attempt: u8,
    pub not_before: String,
    pub expires_at: String,
    pub issued_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AbortReservationCommand {
    pub expected_state_version: u64,
    pub expected_control_epoch: u64,
    pub run_id: String,
    pub logical_effect_id: String,
    pub reason_commitment: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "transition", content = "body", rename_all = "snake_case")]
pub enum ProviderGrantAuthorityCommand {
    AcceptHead {
        expected_state_version: u64,
        expected_control_epoch: u64,
        head: ProviderGrantHead,
    },
    Fence {
        expected_state_version: u64,
        expected_control_epoch: u64,
        provider_grant_lineage_ref: String,
        fence_epoch: u64,
        revoked: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "transition", content = "body", rename_all = "snake_case")]
pub enum ActorAclAuthorityCommand {
    Accept {
        expected_state_version: u64,
        expected_control_epoch: u64,
        head: AclHead,
    },
    Revoke {
        expected_state_version: u64,
        expected_control_epoch: u64,
        key: AclKey,
        acl_epoch: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegistryControlAuthorityCommand {
    pub expected_state_version: u64,
    pub expected_control_epoch: u64,
    pub new_control_epoch: u64,
    pub heads: ControlHeads,
    pub replacement_policy_ceilings: BudgetCeilings,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminalReceiptAuthorityCommand {
    pub expected_state_version: u64,
    pub expected_control_epoch: u64,
    pub run_id: String,
    pub logical_effect_id: String,
    pub attempt: u8,
    pub admission_hash: String,
    pub terminal_evidence_commitment: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetryReconciliationAuthorityCommand {
    pub expected_state_version: u64,
    pub expected_control_epoch: u64,
    pub run_id: String,
    pub logical_effect_id: String,
    pub reconciliation_commitment: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "command", content = "body", rename_all = "snake_case")]
pub enum AdmissionAuthorityCommand {
    Initialize(AuthorityBootstrap),
    ProviderGrant(ProviderGrantAuthorityCommand),
    ActorAcl(ActorAclAuthorityCommand),
    RegistryControl(RegistryControlAuthorityCommand),
    ReserveEffect(ReserveEffectCommand),
    AbortReservation(AbortReservationCommand),
    IssueAdmission(IssueAdmissionCommand),
    ApplyCeilingAmendment {
        expected_state_version: u64,
        amendment: Vec<u8>,
    },
    TerminalReceipt(TerminalReceiptAuthorityCommand),
    RetryReconciliation(RetryReconciliationAuthorityCommand),
    Read,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "reply", content = "body", rename_all = "snake_case")]
pub enum AdmissionAuthorityReply {
    Initialized {
        state_version: u64,
    },
    ControlAccepted {
        state_version: u64,
        control_epoch: u64,
    },
    Reservation {
        state_version: u64,
        reservation_hash: String,
        redelivery: bool,
    },
    ReservationAborted {
        state_version: u64,
        redelivery: bool,
    },
    Admission {
        state_version: u64,
        canonical_admission: Vec<u8>,
        redelivery: bool,
    },
    AmendmentAccepted {
        state_version: u64,
        redelivery: bool,
    },
    TerminalAccepted {
        state_version: u64,
        terminal_evidence_commitment: String,
        redelivery: bool,
    },
    ReconciliationAccepted {
        state_version: u64,
        redelivery: bool,
    },
    State(Box<Option<AdmissionAuthorityState>>),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionAuthorityError {
    #[error("authority already initialized")]
    AlreadyInitialized,
    #[error("authority is not initialized")]
    NotInitialized,
    #[error("authority identity mismatch")]
    IdentityMismatch,
    #[error("invalid authority input")]
    InvalidInput,
    #[error("stale state or control epoch")]
    StaleCas,
    #[error("monotonic control transition required")]
    NonMonotonic,
    #[error("policy replacement cannot widen ceilings")]
    WideningRequiresAmendment,
    #[error("run budget exhausted")]
    BudgetExhausted,
    #[error("logical effect conflicts with the existing reservation")]
    EffectConflict,
    #[error("reservation is not issuable")]
    ReservationBlocked,
    #[error("broker partition is outside this admission authority")]
    BrokerOutsideAuthority,
    #[error("provider grant is not current")]
    ProviderGrantBlocked,
    #[error("actor ACL is not current")]
    AclBlocked,
    #[error("corrected artifact validation failed")]
    ArtifactInvalid,
}

#[derive(Debug)]
pub struct AdmissionAuthorityTransition {
    pub state: Option<AdmissionAuthorityState>,
    pub reply: AdmissionAuthorityReply,
    pub mutated: bool,
}

pub fn transition(
    current: Option<&AdmissionAuthorityState>,
    command: AdmissionAuthorityCommand,
    admission_signer: &BrokerSigner,
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    if let AdmissionAuthorityCommand::Read = command {
        return Ok(AdmissionAuthorityTransition {
            state: current.cloned(),
            reply: AdmissionAuthorityReply::State(Box::new(current.cloned())),
            mutated: false,
        });
    }
    if let AdmissionAuthorityCommand::Initialize(bootstrap) = command {
        if current.is_some() {
            return Err(AdmissionAuthorityError::AlreadyInitialized);
        }
        let state = initialize(bootstrap, admission_signer)?;
        return Ok(AdmissionAuthorityTransition {
            reply: AdmissionAuthorityReply::Initialized {
                state_version: state.state_version,
            },
            state: Some(state),
            mutated: true,
        });
    }
    let mut state = current
        .cloned()
        .ok_or(AdmissionAuthorityError::NotInitialized)?;
    let reply = match command {
        AdmissionAuthorityCommand::ProviderGrant(command) => {
            apply_provider_grant(&mut state, command)?
        }
        AdmissionAuthorityCommand::ActorAcl(command) => apply_acl(&mut state, command)?,
        AdmissionAuthorityCommand::RegistryControl(command) => {
            apply_registry_control(&mut state, command)?
        }
        AdmissionAuthorityCommand::ReserveEffect(command) => {
            return reserve_effect(state, command);
        }
        AdmissionAuthorityCommand::AbortReservation(command) => {
            return abort_reservation(state, command);
        }
        AdmissionAuthorityCommand::IssueAdmission(command) => {
            return issue_admission(state, command, admission_signer);
        }
        AdmissionAuthorityCommand::ApplyCeilingAmendment {
            expected_state_version,
            amendment,
        } => return apply_ceiling_amendment(state, expected_state_version, &amendment),
        AdmissionAuthorityCommand::TerminalReceipt(command) => {
            return terminalize(state, command);
        }
        AdmissionAuthorityCommand::RetryReconciliation(command) => {
            return reconcile_retry(state, command);
        }
        AdmissionAuthorityCommand::Initialize(_) | AdmissionAuthorityCommand::Read => {
            unreachable!()
        }
    };
    Ok(AdmissionAuthorityTransition {
        state: Some(state),
        reply,
        mutated: true,
    })
}

fn initialize(
    bootstrap: AuthorityBootstrap,
    admission_signer: &BrokerSigner,
) -> Result<AdmissionAuthorityState, AdmissionAuthorityError> {
    if bootstrap.tenant_id.is_empty()
        || bootstrap.deployment_id.is_empty()
        || !bootstrap.legacy_inventory.sealed
        || bootstrap.trusted_broker_partitions.is_empty()
        || bootstrap
            .trusted_broker_partitions
            .iter()
            .any(String::is_empty)
        || bootstrap.control_key_id.is_empty()
        || bootstrap.admission_key_id != admission_signer.key_id()
        || BrokerVerifyingKey::from_bytes(
            bootstrap.control_key_id.clone(),
            bootstrap.control_public_key,
        )
        .is_err()
    {
        return Err(AdmissionAuthorityError::InvalidInput);
    }
    let mut provider_grants = BTreeMap::new();
    for head in bootstrap.provider_grants {
        if provider_grants
            .insert(head.provider_grant_lineage_ref.clone(), head)
            .is_some()
        {
            return Err(AdmissionAuthorityError::InvalidInput);
        }
    }
    let mut acls = BTreeMap::new();
    for head in bootstrap.acls {
        let key = acl_key(&head.key)?;
        if acls.insert(key, head).is_some() {
            return Err(AdmissionAuthorityError::InvalidInput);
        }
    }
    Ok(AdmissionAuthorityState {
        tenant_id: bootstrap.tenant_id,
        deployment_id: bootstrap.deployment_id,
        state_version: 1,
        cutover: bootstrap.cutover,
        control_epoch: bootstrap.control_epoch,
        heads: bootstrap.heads,
        provider_grants,
        acls,
        legacy_inventory: bootstrap.legacy_inventory,
        trusted_broker_partitions: bootstrap.trusted_broker_partitions,
        default_ceilings: bootstrap.default_ceilings,
        control_key_id: bootstrap.control_key_id,
        control_public_key: bootstrap.control_public_key,
        admission_key_id: bootstrap.admission_key_id,
        runs: BTreeMap::new(),
    })
}

fn check_cas(
    state: &AdmissionAuthorityState,
    expected_state_version: u64,
    expected_control_epoch: u64,
) -> Result<(), AdmissionAuthorityError> {
    if state.state_version != expected_state_version
        || state.control_epoch != expected_control_epoch
    {
        return Err(AdmissionAuthorityError::StaleCas);
    }
    Ok(())
}

fn apply_provider_grant(
    state: &mut AdmissionAuthorityState,
    command: ProviderGrantAuthorityCommand,
) -> Result<AdmissionAuthorityReply, AdmissionAuthorityError> {
    match command {
        ProviderGrantAuthorityCommand::AcceptHead {
            expected_state_version,
            expected_control_epoch,
            head,
        } => {
            check_cas(state, expected_state_version, expected_control_epoch)?;
            if state
                .provider_grants
                .get(&head.provider_grant_lineage_ref)
                .is_some_and(|old| head.authority_epoch <= old.authority_epoch)
            {
                return Err(AdmissionAuthorityError::NonMonotonic);
            }
            state
                .provider_grants
                .insert(head.provider_grant_lineage_ref.clone(), head);
        }
        ProviderGrantAuthorityCommand::Fence {
            expected_state_version,
            expected_control_epoch,
            provider_grant_lineage_ref,
            fence_epoch,
            revoked,
        } => {
            check_cas(state, expected_state_version, expected_control_epoch)?;
            let head = state
                .provider_grants
                .get_mut(&provider_grant_lineage_ref)
                .ok_or(AdmissionAuthorityError::ProviderGrantBlocked)?;
            if fence_epoch <= head.fence_epoch {
                return Err(AdmissionAuthorityError::NonMonotonic);
            }
            head.fence_epoch = fence_epoch;
            head.status = if revoked {
                ProviderGrantStatus::Revoked
            } else {
                ProviderGrantStatus::Fenced
            };
        }
    }
    state.state_version += 1;
    Ok(AdmissionAuthorityReply::ControlAccepted {
        state_version: state.state_version,
        control_epoch: state.control_epoch,
    })
}

fn apply_acl(
    state: &mut AdmissionAuthorityState,
    command: ActorAclAuthorityCommand,
) -> Result<AdmissionAuthorityReply, AdmissionAuthorityError> {
    match command {
        ActorAclAuthorityCommand::Accept {
            expected_state_version,
            expected_control_epoch,
            head,
        } => {
            check_cas(state, expected_state_version, expected_control_epoch)?;
            if state
                .acls
                .get(&acl_key(&head.key)?)
                .is_some_and(|old| head.acl_epoch <= old.acl_epoch)
            {
                return Err(AdmissionAuthorityError::NonMonotonic);
            }
            state.acls.insert(acl_key(&head.key)?, head);
        }
        ActorAclAuthorityCommand::Revoke {
            expected_state_version,
            expected_control_epoch,
            key,
            acl_epoch,
        } => {
            check_cas(state, expected_state_version, expected_control_epoch)?;
            let head = state
                .acls
                .get_mut(&acl_key(&key)?)
                .ok_or(AdmissionAuthorityError::AclBlocked)?;
            if acl_epoch <= head.acl_epoch {
                return Err(AdmissionAuthorityError::NonMonotonic);
            }
            head.acl_epoch = acl_epoch;
            head.status = AclStatus::Revoked;
        }
    }
    state.state_version += 1;
    Ok(AdmissionAuthorityReply::ControlAccepted {
        state_version: state.state_version,
        control_epoch: state.control_epoch,
    })
}

fn apply_registry_control(
    state: &mut AdmissionAuthorityState,
    command: RegistryControlAuthorityCommand,
) -> Result<AdmissionAuthorityReply, AdmissionAuthorityError> {
    check_cas(
        state,
        command.expected_state_version,
        command.expected_control_epoch,
    )?;
    if command.new_control_epoch != state.control_epoch + 1 {
        return Err(AdmissionAuthorityError::NonMonotonic);
    }
    if command
        .replacement_policy_ceilings
        .widens(&state.default_ceilings)
    {
        return Err(AdmissionAuthorityError::WideningRequiresAmendment);
    }
    state.control_epoch = command.new_control_epoch;
    state.heads = command.heads;
    state.default_ceilings = command.replacement_policy_ceilings.clone();
    for run in state.runs.values_mut() {
        if let Some(ceilings) = run.ceilings.as_mut() {
            ceilings.flow_max = ceilings
                .flow_max
                .min(command.replacement_policy_ceilings.flow_max);
            ceilings.node_max = ceilings
                .node_max
                .min(command.replacement_policy_ceilings.node_max);
            ceilings.connection_lineage_max = ceilings
                .connection_lineage_max
                .min(command.replacement_policy_ceilings.connection_lineage_max);
            ceilings.account_partition_max = ceilings
                .account_partition_max
                .min(command.replacement_policy_ceilings.account_partition_max);
            if let Some(flow) = run.flow.as_mut() {
                flow.ceiling = ceilings.flow_max;
            }
            for counter in run.nodes.values_mut() {
                counter.ceiling = ceilings.node_max;
            }
            for counter in run.connection_partitions.values_mut() {
                counter.ceiling = ceilings.connection_lineage_max;
            }
            for partition in run.account_partitions.values_mut() {
                partition.counter.ceiling = ceilings.account_partition_max;
            }
        }
    }
    state.state_version += 1;
    Ok(AdmissionAuthorityReply::ControlAccepted {
        state_version: state.state_version,
        control_epoch: state.control_epoch,
    })
}

fn reserve_effect(
    mut state: AdmissionAuthorityState,
    command: ReserveEffectCommand,
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    if let Some(existing) = state
        .runs
        .get(&command.context.run_id)
        .and_then(|run| run.effects.get(&command.context.logical_effect_id))
    {
        if existing.context.canonical_input_commitment != command.context.canonical_input_commitment
        {
            return Err(AdmissionAuthorityError::EffectConflict);
        }
        if existing.context == command.context && existing.status == ReservationStatus::Reserved {
            let state_version = state.state_version;
            let reservation_hash = existing.reservation_hash.clone();
            return Ok(AdmissionAuthorityTransition {
                state: Some(state),
                reply: AdmissionAuthorityReply::Reservation {
                    state_version,
                    reservation_hash,
                    redelivery: true,
                },
                mutated: false,
            });
        }
        if existing.context.binding_hash != command.context.binding_hash {
            if existing.context.broker_partition != command.context.broker_partition {
                return Err(AdmissionAuthorityError::BrokerOutsideAuthority);
            }
            if !binding_change_only(&existing.context, &command.context) {
                return Err(AdmissionAuthorityError::EffectConflict);
            }
            check_cas(
                &state,
                command.expected_state_version,
                command.expected_control_epoch,
            )?;
            return abort_changed_binding(state, &command.context);
        }
        return Err(AdmissionAuthorityError::EffectConflict);
    }
    check_cas(
        &state,
        command.expected_state_version,
        command.expected_control_epoch,
    )?;
    validate_context(&state, &command.context)?;
    let reservation_hash = reservation_hash(&state, &command.context)?;
    let default_ceilings = state.default_ceilings.clone();
    let run = state
        .runs
        .entry(command.context.run_id.clone())
        .or_default();
    initialize_run(run, &default_ceilings)?;
    let ceilings = run
        .ceilings
        .as_ref()
        .ok_or(AdmissionAuthorityError::InvalidInput)?;
    let flow = run
        .flow
        .as_mut()
        .ok_or(AdmissionAuthorityError::InvalidInput)?;
    let node = run
        .nodes
        .entry(command.context.node_id.clone())
        .or_insert_with(|| LedgerCounter::new(ceilings.node_max, &run.ceiling_amendment_head_hash));
    let connection = run
        .connection_partitions
        .entry(command.context.provider_grant_lineage_ref.clone())
        .or_insert_with(|| {
            LedgerCounter::new(
                ceilings.connection_lineage_max,
                &run.ceiling_amendment_head_hash,
            )
        });
    let account_key = account_partition_key(&command.context.account_partition)?;
    let account = run
        .account_partitions
        .entry(account_key)
        .or_insert_with(|| AccountLedgerPartition {
            partition: command.context.account_partition.clone(),
            counter: LedgerCounter::new(
                ceilings.account_partition_max,
                &run.ceiling_amendment_head_hash,
            ),
        });
    if !flow.has_capacity()
        || !node.has_capacity()
        || !connection.has_capacity()
        || !account.counter.has_capacity()
    {
        return Err(AdmissionAuthorityError::BudgetExhausted);
    }
    flow.reserved += 1;
    node.reserved += 1;
    connection.reserved += 1;
    account.counter.reserved += 1;
    run.effects.insert(
        command.context.logical_effect_id.clone(),
        EffectReservation {
            context: command.context,
            reservation_hash: reservation_hash.clone(),
            control_epoch: state.control_epoch,
            status: ReservationStatus::Reserved,
            attempts: BTreeMap::new(),
            retry_reconciliation_commitment: None,
        },
    );
    state.state_version += 1;
    Ok(AdmissionAuthorityTransition {
        reply: AdmissionAuthorityReply::Reservation {
            state_version: state.state_version,
            reservation_hash,
            redelivery: false,
        },
        state: Some(state),
        mutated: true,
    })
}

fn binding_change_only(existing: &AdmissionContext, replacement: &AdmissionContext) -> bool {
    let mut normalized = replacement.clone();
    normalized.binding_ref.clone_from(&existing.binding_ref);
    normalized.binding_hash.clone_from(&existing.binding_hash);
    &normalized == existing
}

fn abort_changed_binding(
    mut state: AdmissionAuthorityState,
    context: &AdmissionContext,
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    let run = state
        .runs
        .get_mut(&context.run_id)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    release_reservation(run, &context.logical_effect_id)?;
    let effect = run
        .effects
        .get_mut(&context.logical_effect_id)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    effect.status = ReservationStatus::Aborted;
    state.state_version += 1;
    Ok(AdmissionAuthorityTransition {
        reply: AdmissionAuthorityReply::ReservationAborted {
            state_version: state.state_version,
            redelivery: false,
        },
        state: Some(state),
        mutated: true,
    })
}

fn abort_reservation(
    mut state: AdmissionAuthorityState,
    command: AbortReservationCommand,
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    if command.reason_commitment.is_empty() {
        return Err(AdmissionAuthorityError::InvalidInput);
    }
    let existing = state
        .runs
        .get(&command.run_id)
        .and_then(|run| run.effects.get(&command.logical_effect_id));
    if existing.is_some_and(|effect| effect.status == ReservationStatus::Aborted) {
        let state_version = state.state_version;
        return Ok(AdmissionAuthorityTransition {
            state: Some(state),
            reply: AdmissionAuthorityReply::ReservationAborted {
                state_version,
                redelivery: true,
            },
            mutated: false,
        });
    }
    check_cas(
        &state,
        command.expected_state_version,
        command.expected_control_epoch,
    )?;
    let run = state
        .runs
        .get_mut(&command.run_id)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    release_reservation(run, &command.logical_effect_id)?;
    run.effects
        .get_mut(&command.logical_effect_id)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?
        .status = ReservationStatus::Aborted;
    state.state_version += 1;
    Ok(AdmissionAuthorityTransition {
        reply: AdmissionAuthorityReply::ReservationAborted {
            state_version: state.state_version,
            redelivery: false,
        },
        state: Some(state),
        mutated: true,
    })
}

fn issue_admission(
    mut state: AdmissionAuthorityState,
    command: IssueAdmissionCommand,
    signer: &BrokerSigner,
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    if let Some(effect) = state
        .runs
        .get(&command.run_id)
        .and_then(|run| run.effects.get(&command.logical_effect_id))
        && let Some(attempt) = effect.attempts.get(&command.attempt)
    {
        if effect.context.broker_partition != command.broker_partition {
            return Err(AdmissionAuthorityError::BrokerOutsideAuthority);
        }
        let canonical_admission = attempt.canonical_admission.clone();
        let state_version = state.state_version;
        return Ok(AdmissionAuthorityTransition {
            state: Some(state),
            reply: AdmissionAuthorityReply::Admission {
                state_version,
                canonical_admission,
                redelivery: true,
            },
            mutated: false,
        });
    }
    check_cas(
        &state,
        command.expected_state_version,
        command.expected_control_epoch,
    )?;
    if signer.key_id() != state.admission_key_id {
        return Err(AdmissionAuthorityError::InvalidInput);
    }
    let effect = state
        .runs
        .get(&command.run_id)
        .and_then(|run| run.effects.get(&command.logical_effect_id))
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    if effect.status != ReservationStatus::Reserved || command.attempt != 1 {
        return Err(AdmissionAuthorityError::ReservationBlocked);
    }
    let context = effect.context.clone();
    if context.broker_partition != command.broker_partition {
        return Err(AdmissionAuthorityError::BrokerOutsideAuthority);
    }
    let reservation_epoch = state
        .runs
        .get(&command.run_id)
        .and_then(|run| run.effects.get(&command.logical_effect_id))
        .map(|effect| effect.control_epoch)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    if reservation_epoch != state.control_epoch {
        return Err(AdmissionAuthorityError::ReservationBlocked);
    }
    validate_context(&state, &context)?;
    let snapshot = run_budget_snapshot(
        state
            .runs
            .get(&command.run_id)
            .ok_or(AdmissionAuthorityError::ReservationBlocked)?,
    )?;
    let reservation_hash = state.runs[&command.run_id].effects[&command.logical_effect_id]
        .reservation_hash
        .clone();
    let canonical_admission = build_dispatch_admission(
        &state,
        &context,
        &reservation_hash,
        snapshot,
        &command,
        signer,
    )?;
    let run = state
        .runs
        .get_mut(&command.run_id)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    consume_reservation(run, &command.logical_effect_id)?;
    let effect = run
        .effects
        .get_mut(&command.logical_effect_id)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    effect.status = ReservationStatus::Admitted;
    effect.retry_reconciliation_commitment = None;
    effect.attempts.insert(
        command.attempt,
        AdmissionAttempt {
            attempt: command.attempt,
            canonical_admission: canonical_admission.clone(),
            status: AttemptStatus::Admitted,
            terminal_evidence_commitment: None,
        },
    );
    state.state_version += 1;
    Ok(AdmissionAuthorityTransition {
        reply: AdmissionAuthorityReply::Admission {
            state_version: state.state_version,
            canonical_admission,
            redelivery: false,
        },
        state: Some(state),
        mutated: true,
    })
}

fn terminalize(
    mut state: AdmissionAuthorityState,
    command: TerminalReceiptAuthorityCommand,
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    if let Some(attempt) = state
        .runs
        .get(&command.run_id)
        .and_then(|run| run.effects.get(&command.logical_effect_id))
        .and_then(|effect| effect.attempts.get(&command.attempt))
        && attempt.status == AttemptStatus::Terminal
    {
        if attempt.terminal_evidence_commitment.as_deref()
            != Some(command.terminal_evidence_commitment.as_str())
            || sha256(&attempt.canonical_admission) != command.admission_hash
        {
            return Err(AdmissionAuthorityError::EffectConflict);
        }
        let state_version = state.state_version;
        let terminal_evidence_commitment = command.terminal_evidence_commitment.clone();
        return Ok(AdmissionAuthorityTransition {
            state: Some(state),
            reply: AdmissionAuthorityReply::TerminalAccepted {
                state_version,
                terminal_evidence_commitment,
                redelivery: true,
            },
            mutated: false,
        });
    }
    check_cas(
        &state,
        command.expected_state_version,
        command.expected_control_epoch,
    )?;
    let effect = state
        .runs
        .get_mut(&command.run_id)
        .and_then(|run| run.effects.get_mut(&command.logical_effect_id))
        .filter(|effect| effect.status == ReservationStatus::Admitted)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    let attempt = effect
        .attempts
        .get_mut(&command.attempt)
        .filter(|attempt| attempt.status == AttemptStatus::Admitted)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    if sha256(&attempt.canonical_admission) != command.admission_hash
        || command.terminal_evidence_commitment.is_empty()
    {
        return Err(AdmissionAuthorityError::EffectConflict);
    }
    attempt.status = AttemptStatus::Terminal;
    attempt.terminal_evidence_commitment = Some(command.terminal_evidence_commitment.clone());
    effect.status = ReservationStatus::Terminal;
    state.state_version += 1;
    Ok(AdmissionAuthorityTransition {
        reply: AdmissionAuthorityReply::TerminalAccepted {
            state_version: state.state_version,
            terminal_evidence_commitment: command.terminal_evidence_commitment,
            redelivery: false,
        },
        state: Some(state),
        mutated: true,
    })
}

fn reconcile_retry(
    mut state: AdmissionAuthorityState,
    command: RetryReconciliationAuthorityCommand,
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    if let Some(effect) = state
        .runs
        .get(&command.run_id)
        .and_then(|run| run.effects.get(&command.logical_effect_id))
        && let Some(existing) = &effect.retry_reconciliation_commitment
    {
        if existing != &command.reconciliation_commitment {
            return Err(AdmissionAuthorityError::EffectConflict);
        }
        let state_version = state.state_version;
        return Ok(AdmissionAuthorityTransition {
            state: Some(state),
            reply: AdmissionAuthorityReply::ReconciliationAccepted {
                state_version,
                redelivery: true,
            },
            mutated: false,
        });
    }
    check_cas(
        &state,
        command.expected_state_version,
        command.expected_control_epoch,
    )?;
    let effect = state
        .runs
        .get_mut(&command.run_id)
        .and_then(|run| run.effects.get_mut(&command.logical_effect_id))
        .filter(|effect| effect.status == ReservationStatus::Terminal)
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    if command.reconciliation_commitment.is_empty() {
        return Err(AdmissionAuthorityError::InvalidInput);
    }
    effect.retry_reconciliation_commitment = Some(command.reconciliation_commitment);
    state.state_version += 1;
    Ok(AdmissionAuthorityTransition {
        reply: AdmissionAuthorityReply::ReconciliationAccepted {
            state_version: state.state_version,
            redelivery: false,
        },
        state: Some(state),
        mutated: true,
    })
}

fn apply_ceiling_amendment(
    mut state: AdmissionAuthorityState,
    expected_state_version: u64,
    bytes: &[u8],
) -> Result<AdmissionAuthorityTransition, AdmissionAuthorityError> {
    let parsed = parse::<CeilingAmendmentLs1>(bytes)
        .map_err(|_| AdmissionAuthorityError::ArtifactInvalid)?;
    let value = parsed.view.as_value();
    let amendment_ref = text(value, "ceiling_amendment_ref")?;
    let run_id = text(value, "run_id")?;
    let artifact_hash = parsed.content_hash();
    if let Some(existing) = state
        .runs
        .get(run_id)
        .and_then(|run| run.applied_amendments.get(amendment_ref))
    {
        if existing != &artifact_hash {
            return Err(AdmissionAuthorityError::EffectConflict);
        }
        let state_version = state.state_version;
        return Ok(AdmissionAuthorityTransition {
            state: Some(state),
            reply: AdmissionAuthorityReply::AmendmentAccepted {
                state_version,
                redelivery: true,
            },
            mutated: false,
        });
    }
    if state.state_version != expected_state_version {
        return Err(AdmissionAuthorityError::StaleCas);
    }
    verify_lifecycle_signature(
        value,
        CEILING_AMENDMENT_DOMAIN,
        &state.control_key_id,
        state.control_public_key,
    )?;
    if value["tenant_id"] != state.tenant_id
        || value["deployment_id"] != state.deployment_id
        || value["standing_authority_ref"] != state.heads.standing.reference
    {
        return Err(AdmissionAuthorityError::IdentityMismatch);
    }
    let run = state
        .runs
        .get_mut(run_id)
        .ok_or(AdmissionAuthorityError::InvalidInput)?;
    let epoch = value["amendment_epoch"]
        .as_u64()
        .ok_or(AdmissionAuthorityError::ArtifactInvalid)?;
    if epoch != run.ceiling_amendment_epoch + 1
        || value["previous_ceiling_hash"] != run.ceiling_amendment_head_hash
    {
        return Err(AdmissionAuthorityError::NonMonotonic);
    }
    let next: BudgetCeilings = serde_json::from_value(value["new_ceilings"].clone())
        .map_err(|_| AdmissionAuthorityError::ArtifactInvalid)?;
    let previous = run
        .ceilings
        .as_ref()
        .ok_or(AdmissionAuthorityError::InvalidInput)?;
    if !next.is_monotonic_widening_of(previous) {
        return Err(AdmissionAuthorityError::NonMonotonic);
    }
    run.ceilings = Some(next.clone());
    run.ceiling_amendment_epoch = epoch;
    run.ceiling_amendment_head_hash = artifact_hash.clone();
    run.applied_amendments
        .insert(amendment_ref.to_owned(), artifact_hash.clone());
    if let Some(flow) = run.flow.as_mut() {
        flow.ceiling = next.flow_max;
        flow.ceiling_amendment_head_hash.clone_from(&artifact_hash);
    }
    for counter in run.nodes.values_mut() {
        counter.ceiling = next.node_max;
        counter
            .ceiling_amendment_head_hash
            .clone_from(&artifact_hash);
    }
    for counter in run.connection_partitions.values_mut() {
        counter.ceiling = next.connection_lineage_max;
        counter
            .ceiling_amendment_head_hash
            .clone_from(&artifact_hash);
    }
    for partition in run.account_partitions.values_mut() {
        partition.counter.ceiling = next.account_partition_max;
        partition
            .counter
            .ceiling_amendment_head_hash
            .clone_from(&artifact_hash);
    }
    state.state_version += 1;
    Ok(AdmissionAuthorityTransition {
        reply: AdmissionAuthorityReply::AmendmentAccepted {
            state_version: state.state_version,
            redelivery: false,
        },
        state: Some(state),
        mutated: true,
    })
}

fn initialize_run(
    run: &mut RunLedger,
    default_ceilings: &BudgetCeilings,
) -> Result<(), AdmissionAuthorityError> {
    if run.ceilings.is_none() {
        let head = sha256(
            canonical::from_serde(default_ceilings, 4096)
                .map_err(|_| AdmissionAuthorityError::InvalidInput)?
                .as_bytes(),
        );
        run.ceilings = Some(default_ceilings.clone());
        run.ceiling_amendment_head_hash.clone_from(&head);
        run.flow = Some(LedgerCounter::new(default_ceilings.flow_max, &head));
    }
    Ok(())
}

fn validate_context(
    state: &AdmissionAuthorityState,
    context: &AdmissionContext,
) -> Result<(), AdmissionAuthorityError> {
    if !state
        .trusted_broker_partitions
        .contains(&context.broker_partition)
    {
        return Err(AdmissionAuthorityError::BrokerOutsideAuthority);
    }
    let grant = state
        .provider_grants
        .get(&context.provider_grant_lineage_ref)
        .filter(|head| {
            head.status == ProviderGrantStatus::Current
                && head.provider_grant_version_ref == context.provider_grant_version_ref
                && head.provider_grant_version_hash == context.provider_grant_version_hash
                && head.account_subject_commitment
                    == context.account_partition.account_subject_commitment
        })
        .ok_or(AdmissionAuthorityError::ProviderGrantBlocked)?;
    if grant.account_subject_commitment != context.account_partition.account_subject_commitment {
        return Err(AdmissionAuthorityError::ProviderGrantBlocked);
    }
    let acl_identity = AclKey {
        actor_subject_commitment: context.actor_subject_commitment.clone(),
        provider_grant_version_ref: context.provider_grant_version_ref.clone(),
    };
    let acl = state
        .acls
        .get(&acl_key(&acl_identity)?)
        .filter(|head| {
            head.status == AclStatus::Active
                && head.acl_epoch == context.acl_epoch
                && head.account_subject_commitment
                    == context.account_partition.account_subject_commitment
                && head.selector_hash == context.acl_selector_hash
                && head.record_commitment == context.acl_record_commitment
                && head.record_hash == context.acl_record_hash
        })
        .ok_or(AdmissionAuthorityError::AclBlocked)?;
    if acl.account_subject_commitment != context.account_partition.account_subject_commitment
        || context.registry_vector_ref != state.heads.registry_vector.reference
        || context.registry_vector_hash != state.heads.registry_vector.hash
        || context.registry_vector_epoch != state.heads.registry_vector.epoch
    {
        return Err(AdmissionAuthorityError::AclBlocked);
    }
    Ok(())
}

fn acl_key(key: &AclKey) -> Result<String, AdmissionAuthorityError> {
    canonical_key(key)
}

fn canonical_key(value: &impl Serialize) -> Result<String, AdmissionAuthorityError> {
    canonical::from_serde(value, 4096)
        .and_then(|value| {
            String::from_utf8(value.into_bytes()).map_err(|_| broker_core::BrokerError::Brk401)
        })
        .map_err(|_| AdmissionAuthorityError::InvalidInput)
}

fn account_partition_key(
    partition: &AccountBudgetPartition,
) -> Result<String, AdmissionAuthorityError> {
    canonical_key(partition)
}

fn release_reservation(
    run: &mut RunLedger,
    effect_id: &str,
) -> Result<(), AdmissionAuthorityError> {
    let context = run
        .effects
        .get(effect_id)
        .filter(|effect| effect.status == ReservationStatus::Reserved)
        .map(|effect| effect.context.clone())
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    adjust_reserved(run, &context, false)
}

fn consume_reservation(
    run: &mut RunLedger,
    effect_id: &str,
) -> Result<(), AdmissionAuthorityError> {
    let context = run
        .effects
        .get(effect_id)
        .filter(|effect| effect.status == ReservationStatus::Reserved)
        .map(|effect| effect.context.clone())
        .ok_or(AdmissionAuthorityError::ReservationBlocked)?;
    adjust_reserved(run, &context, true)
}

fn adjust_reserved(
    run: &mut RunLedger,
    context: &AdmissionContext,
    consume: bool,
) -> Result<(), AdmissionAuthorityError> {
    let counters = [
        run.flow.as_mut(),
        run.nodes.get_mut(&context.node_id),
        run.connection_partitions
            .get_mut(&context.provider_grant_lineage_ref),
        run.account_partitions
            .get_mut(&account_partition_key(&context.account_partition)?)
            .map(|partition| &mut partition.counter),
    ];
    for counter in counters {
        let counter = counter.ok_or(AdmissionAuthorityError::InvalidInput)?;
        counter.reserved = counter
            .reserved
            .checked_sub(1)
            .ok_or(AdmissionAuthorityError::InvalidInput)?;
        if consume {
            counter.consumed = counter
                .consumed
                .checked_add(1)
                .ok_or(AdmissionAuthorityError::InvalidInput)?;
        }
    }
    Ok(())
}

fn reservation_hash(
    state: &AdmissionAuthorityState,
    context: &AdmissionContext,
) -> Result<String, AdmissionAuthorityError> {
    let value = serde_json::json!({
        "authority_model_revision":"lifecycle-separated-1",
        "tenant_id":state.tenant_id,
        "deployment_id":state.deployment_id,
        "control_epoch":state.control_epoch,
        "context":context,
    });
    let canonical = canonical::from_serde(&value, 64 * 1024)
        .map_err(|_| AdmissionAuthorityError::InvalidInput)?;
    Ok(sha256(canonical.as_bytes()))
}

fn run_budget_snapshot(run: &RunLedger) -> Result<Value, AdmissionAuthorityError> {
    let mut connection_partitions = run
        .connection_partitions
        .iter()
        .map(|(lineage, counter)| {
            serde_json::json!({
                "provider_grant_lineage_ref":lineage,
                "ceiling":counter.ceiling,
                "consumed":counter.consumed,
                "reserved":counter.reserved,
                "ceiling_amendment_head_hash":counter.ceiling_amendment_head_hash,
            })
        })
        .collect::<Vec<_>>();
    let mut account_partitions = run
        .account_partitions
        .values()
        .map(|entry| {
            serde_json::json!({
                "provider":entry.partition.provider,
                "auth_profile_ref":entry.partition.auth_profile_ref,
                "auth_profile_version":entry.partition.auth_profile_version,
                "account_subject_commitment":entry.partition.account_subject_commitment,
                "ceiling":entry.counter.ceiling,
                "consumed":entry.counter.consumed,
                "reserved":entry.counter.reserved,
                "ceiling_amendment_head_hash":entry.counter.ceiling_amendment_head_hash,
            })
        })
        .collect::<Vec<_>>();
    sort_canonical(&mut connection_partitions)?;
    sort_canonical(&mut account_partitions)?;
    Ok(serde_json::json!({
        "connection_partitions":connection_partitions,
        "account_partitions":account_partitions,
    }))
}

fn sort_canonical(values: &mut [Value]) -> Result<(), AdmissionAuthorityError> {
    let mut keyed = values
        .iter()
        .map(|value| {
            canonical::from_serde(value, 64 * 1024)
                .map(|canonical| (canonical.into_bytes(), value.clone()))
                .map_err(|_| AdmissionAuthorityError::InvalidInput)
        })
        .collect::<Result<Vec<_>, _>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    for (target, (_, value)) in values.iter_mut().zip(keyed) {
        *target = value;
    }
    Ok(())
}

fn build_dispatch_admission(
    state: &AdmissionAuthorityState,
    context: &AdmissionContext,
    reservation_hash: &str,
    run_budget_snapshot: Value,
    command: &IssueAdmissionCommand,
    signer: &BrokerSigner,
) -> Result<Vec<u8>, AdmissionAuthorityError> {
    let admission_ref = format!("admission-{}-{}", &reservation_hash[7..39], command.attempt);
    let critical_fields = [
        "/account_budget_partition",
        "/account_subject_commitment",
        "/acl_epoch",
        "/acl_record_commitment",
        "/acl_record_hash",
        "/acl_selector_hash",
        "/actor_subject_commitment",
        "/admission_ref",
        "/artifact_type",
        "/attempt",
        "/authority_model_revision",
        "/authority_owner",
        "/binding_hash",
        "/binding_ref",
        "/canonical_input_commitment",
        "/connection_budget_partition",
        "/contract_hash",
        "/contract_id",
        "/deployment_id",
        "/expires_at",
        "/issued_at",
        "/issuer",
        "/key_id",
        "/logical_effect_id",
        "/not_before",
        "/one_use",
        "/provider_grant_lineage_ref",
        "/provider_grant_version_hash",
        "/provider_grant_version_ref",
        "/registry_vector_epoch",
        "/registry_vector_hash",
        "/registry_vector_ref",
        "/reservation_hash",
        "/run_budget_snapshot",
        "/run_id",
        "/schema_version",
        "/tenant_id",
    ]
    .to_vec();
    let mut value = serde_json::json!({
        "schema_version":"0.2",
        "authority_model_revision":"lifecycle-separated-1",
        "artifact_type":"DispatchAdmission",
        "critical_fields":critical_fields,
        "authority_owner":"admission_authority",
        "one_use":true,
        "admission_ref":admission_ref,
        "tenant_id":state.tenant_id,
        "deployment_id":state.deployment_id,
    });
    let object = value
        .as_object_mut()
        .ok_or(AdmissionAuthorityError::ArtifactInvalid)?;
    for (key, item) in [
        ("run_id", serde_json::json!(context.run_id)),
        (
            "logical_effect_id",
            serde_json::json!(context.logical_effect_id),
        ),
        ("attempt", serde_json::json!(command.attempt)),
        ("binding_ref", serde_json::json!(context.binding_ref)),
        ("binding_hash", serde_json::json!(context.binding_hash)),
        (
            "provider_grant_version_ref",
            serde_json::json!(context.provider_grant_version_ref),
        ),
        (
            "provider_grant_version_hash",
            serde_json::json!(context.provider_grant_version_hash),
        ),
        (
            "provider_grant_lineage_ref",
            serde_json::json!(context.provider_grant_lineage_ref),
        ),
        (
            "account_subject_commitment",
            serde_json::json!(context.account_partition.account_subject_commitment),
        ),
        (
            "actor_subject_commitment",
            serde_json::json!(context.actor_subject_commitment),
        ),
        ("acl_epoch", serde_json::json!(context.acl_epoch)),
        (
            "acl_selector_hash",
            serde_json::json!(context.acl_selector_hash),
        ),
        (
            "acl_record_commitment",
            serde_json::json!(context.acl_record_commitment),
        ),
        (
            "acl_record_hash",
            serde_json::json!(context.acl_record_hash),
        ),
        ("contract_id", serde_json::json!(context.contract_id)),
        ("contract_hash", serde_json::json!(context.contract_hash)),
        (
            "canonical_input_commitment",
            serde_json::json!(context.canonical_input_commitment),
        ),
        (
            "registry_vector_ref",
            serde_json::json!(context.registry_vector_ref),
        ),
        (
            "registry_vector_hash",
            serde_json::json!(context.registry_vector_hash),
        ),
        (
            "registry_vector_epoch",
            serde_json::json!(context.registry_vector_epoch),
        ),
        (
            "connection_budget_partition",
            serde_json::json!({"provider_grant_lineage_ref":context.provider_grant_lineage_ref}),
        ),
        (
            "account_budget_partition",
            serde_json::json!(context.account_partition),
        ),
        ("run_budget_snapshot", run_budget_snapshot),
        ("reservation_hash", serde_json::json!(reservation_hash)),
        ("not_before", serde_json::json!(command.not_before)),
        ("expires_at", serde_json::json!(command.expires_at)),
        ("issuer", serde_json::json!("admission-authority")),
        ("key_id", serde_json::json!(state.admission_key_id)),
        ("issued_at", serde_json::json!(command.issued_at)),
        (
            "signature",
            serde_json::json!({"algorithm":"Ed25519","key_id":state.admission_key_id,"value":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}),
        ),
    ] {
        object.insert(key.to_owned(), item);
    }
    let unsigned = canonical::from_serde(&value, 1024 * 1024)
        .map_err(|_| AdmissionAuthorityError::ArtifactInvalid)?;
    let signature = signer
        .sign_json(DISPATCH_ADMISSION_DOMAIN, unsigned.as_bytes())
        .map_err(|_| AdmissionAuthorityError::ArtifactInvalid)?;
    value["signature"] = lifecycle_signature(&signature);
    let canonical = canonical::from_serde(&value, 1024 * 1024)
        .map_err(|_| AdmissionAuthorityError::ArtifactInvalid)?;
    parse::<DispatchAdmissionLs1>(canonical.as_bytes())
        .map_err(|_| AdmissionAuthorityError::ArtifactInvalid)?;
    verify_lifecycle_signature(
        &value,
        DISPATCH_ADMISSION_DOMAIN,
        signer.key_id(),
        signer.verifying_key().to_bytes(),
    )?;
    Ok(canonical.into_bytes())
}

fn verify_lifecycle_signature(
    value: &Value,
    domain: &str,
    expected_key_id: &str,
    public_key: [u8; 32],
) -> Result<(), AdmissionAuthorityError> {
    let signature = value
        .get("signature")
        .and_then(Value::as_object)
        .ok_or(AdmissionAuthorityError::ArtifactInvalid)?;
    if signature.get("algorithm").and_then(Value::as_str) != Some("Ed25519")
        || signature.get("key_id").and_then(Value::as_str) != Some(expected_key_id)
        || value.get("key_id").and_then(Value::as_str) != Some(expected_key_id)
    {
        return Err(AdmissionAuthorityError::ArtifactInvalid);
    }
    let envelope = SignatureEnvelope {
        alg: SignatureAlg::Ed25519,
        key_id: expected_key_id.to_owned(),
        value: signature
            .get("value")
            .and_then(Value::as_str)
            .ok_or(AdmissionAuthorityError::ArtifactInvalid)?
            .to_owned(),
    };
    BrokerVerifyingKey::from_bytes(expected_key_id, public_key)
        .and_then(|key| {
            let bytes = canonical::from_serde(value, 1024 * 1024)?;
            key.verify_json(domain, bytes.as_bytes(), &envelope)
        })
        .map_err(|_| AdmissionAuthorityError::ArtifactInvalid)
}

fn lifecycle_signature(signature: &SignatureEnvelope) -> Value {
    serde_json::json!({
        "algorithm":match signature.alg { SignatureAlg::Ed25519 => "Ed25519" },
        "key_id":signature.key_id,
        "value":signature.value,
    })
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str, AdmissionAuthorityError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(AdmissionAuthorityError::ArtifactInvalid)
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}
