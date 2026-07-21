use broker_core::{
    BrokerError,
    ledger::{
        DurableRecord, EntrySnapshot, InMemoryLedger, InvocationState, Ledger, LedgerSnapshot,
        LedgerTail, PlannedData, ReservationKey, ReserveRequest, ReserveResult, TerminalOutcome,
        finish_durable_kernel_output, issue_released_durable_kernel_output,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Mutex;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LedgerAuthority {
    pub org_id: String,
    pub grant_ref: String,
}

impl LedgerAuthority {
    pub fn route_name(&self) -> Result<String, BrokerError> {
        if self.org_id.is_empty() || self.grant_ref.is_empty() {
            return Err(BrokerError::Brk101);
        }
        let mut hash = Sha256::new();
        hash.update(b"lattice.broker-ledger.v1");
        hash.update([0]);
        hash.update(self.org_id.as_bytes());
        hash.update([0]);
        hash.update(self.grant_ref.as_bytes());
        Ok(format!("authority-{}", hex::encode(hash.finalize())))
    }

    fn validates(&self, key: &ReservationKey) -> bool {
        self.org_id == key.org_id && !self.grant_ref.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum LedgerCommand {
    Reserve {
        request: ReserveRequest,
        now: i64,
    },
    Plan {
        key: ReservationKey,
        lease_token: u64,
        now: i64,
        data: PlannedData,
    },
    MarkDispatched {
        key: ReservationKey,
        lease_token: u64,
        now: i64,
    },
    Finish {
        key: ReservationKey,
        outcome: TerminalOutcome,
        receipt: Vec<u8>,
        response_projection: Vec<u8>,
        provider_request_id: Option<String>,
    },
    Release {
        key: ReservationKey,
        lease_token: u64,
        now: i64,
        expired: bool,
    },
    IssueReceipt {
        key: ReservationKey,
    },
    IssueReleasedReceipt {
        key: ReservationKey,
        receipt: Vec<u8>,
    },
    RecoverDispatched {
        key: ReservationKey,
    },
    Get {
        key: ReservationKey,
    },
}

impl LedgerCommand {
    fn key(&self) -> &ReservationKey {
        match self {
            Self::Reserve { request, .. } => &request.key,
            Self::Plan { key, .. }
            | Self::MarkDispatched { key, .. }
            | Self::Finish { key, .. }
            | Self::Release { key, .. }
            | Self::IssueReceipt { key }
            | Self::IssueReleasedReceipt { key, .. }
            | Self::RecoverDispatched { key }
            | Self::Get { key } => key,
        }
    }
}

pub const COMPACTION_THRESHOLD: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerPersistence {
    pub snapshot: Option<LedgerSnapshot>,
    pub tail: LedgerTail,
}

impl Default for LedgerPersistence {
    fn default() -> Self {
        Self {
            snapshot: None,
            tail: LedgerTail {
                start_sequence: 0,
                records: Vec::new(),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct PersistedLedgerReply {
    pub reply: LedgerReply,
    pub persistence: LedgerPersistence,
    pub compacted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LedgerReply {
    pub snapshot: Option<EntrySnapshot>,
    pub acquired: Option<bool>,
    /// Internal DO persistence material; never returned over the stub RPC.
    #[serde(default, skip_serializing)]
    pub records: Vec<DurableRecord>,
}

pub fn apply_persisted_command(
    authority: &LedgerAuthority,
    persistence: &LedgerPersistence,
    command: LedgerCommand,
) -> Result<PersistedLedgerReply, BrokerError> {
    if !authority.validates(command.key()) {
        return Err(BrokerError::Brk107);
    }
    let ledger = InMemoryLedger::restore(persistence.snapshot.as_ref(), &persistence.tail)?;
    let mut acquired = None;
    let snapshot = match command {
        LedgerCommand::Reserve { request, now } => match ledger.reserve(request, now)? {
            ReserveResult::Acquired(snapshot) => {
                acquired = Some(true);
                Some(snapshot)
            }
            ReserveResult::Redelivery(snapshot) => {
                acquired = Some(false);
                Some(snapshot)
            }
        },
        LedgerCommand::Plan {
            key,
            lease_token,
            now,
            data,
        } => Some(ledger.plan(&key, lease_token, now, data)?),
        LedgerCommand::MarkDispatched {
            key,
            lease_token,
            now,
        } => Some(ledger.mark_dispatched(&key, lease_token, now)?),
        LedgerCommand::Finish {
            key,
            outcome,
            receipt,
            response_projection,
            provider_request_id,
        } => Some(finish_durable_kernel_output(
            &ledger,
            &key,
            outcome,
            receipt,
            response_projection,
            provider_request_id,
        )?),
        LedgerCommand::Release {
            key,
            lease_token,
            now,
            expired,
        } => Some(ledger.release(&key, lease_token, now, expired)?),
        LedgerCommand::IssueReceipt { key } => Some(ledger.issue_receipt(&key)?),
        LedgerCommand::IssueReleasedReceipt { key, receipt } => Some(
            issue_released_durable_kernel_output(&ledger, &key, receipt)?,
        ),
        LedgerCommand::RecoverDispatched { key } => {
            let recovered =
                InMemoryLedger::recover_durable(persistence.snapshot.as_ref(), &ledger.tail()?)?;
            recovered.get(&key)?
        }
        LedgerCommand::Get { key } => ledger.get(&key)?,
    };
    let mut tail = ledger.tail()?;
    let recovered = matches!(
        snapshot.as_ref().map(|value| &value.state),
        Some(InvocationState::Terminal(TerminalOutcome::Ambiguous))
    ) && !tail.records.iter().any(|record| {
        matches!(record, DurableRecord::Terminal { key, .. } if Some(key) == snapshot.as_ref().map(|value| &value.key))
    });
    if recovered {
        let recovered = InMemoryLedger::recover_durable(persistence.snapshot.as_ref(), &tail)?;
        tail = recovered.tail()?;
    }
    let compacted = tail.records.len() >= COMPACTION_THRESHOLD;
    let next_persistence = if compacted {
        let state = InMemoryLedger::restore(persistence.snapshot.as_ref(), &tail)?;
        let snapshot = state.snapshot()?;
        LedgerPersistence {
            tail: LedgerTail {
                start_sequence: snapshot.covered_records,
                records: Vec::new(),
            },
            snapshot: Some(snapshot),
        }
    } else {
        LedgerPersistence {
            snapshot: persistence.snapshot.clone(),
            tail,
        }
    };
    Ok(PersistedLedgerReply {
        reply: LedgerReply {
            snapshot,
            acquired,
            records: Vec::new(),
        },
        persistence: next_persistence,
        compacted,
    })
}

pub fn apply_command(
    authority: &LedgerAuthority,
    records: &[DurableRecord],
    command: LedgerCommand,
) -> Result<LedgerReply, BrokerError> {
    let mut persisted = apply_persisted_command(
        authority,
        &LedgerPersistence {
            snapshot: None,
            tail: LedgerTail {
                start_sequence: 0,
                records: records.to_vec(),
            },
        },
        command,
    )?;
    persisted.reply.records = persisted.persistence.tail.records;
    Ok(persisted.reply)
}

#[derive(Default)]
pub struct DurableLedgerModel {
    persistence: Mutex<LedgerPersistence>,
}

impl DurableLedgerModel {
    pub fn execute(
        &self,
        authority: &LedgerAuthority,
        command: LedgerCommand,
    ) -> Result<LedgerReply, BrokerError> {
        let mut persistence = self.persistence.lock().map_err(|_| BrokerError::Brk401)?;
        let result = apply_persisted_command(authority, &persistence, command)?;
        *persistence = result.persistence;
        Ok(result.reply)
    }

    pub fn records(&self) -> Result<Vec<DurableRecord>, BrokerError> {
        self.persistence
            .lock()
            .map(|state| state.tail.records.clone())
            .map_err(|_| BrokerError::Brk401)
    }

    pub fn persistence(&self) -> Result<LedgerPersistence, BrokerError> {
        self.persistence
            .lock()
            .map(|state| state.clone())
            .map_err(|_| BrokerError::Brk401)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn authority() -> LedgerAuthority {
        LedgerAuthority {
            org_id: "org-1".into(),
            grant_ref: "grant-1".into(),
        }
    }

    fn key(effect: &str) -> ReservationKey {
        ReservationKey {
            org_id: "org-1".into(),
            deployment_id: "deployment-1".into(),
            flow_ir_hash: "flow-hash".into(),
            run_id: "run-1".into(),
            node_id: "node-1".into(),
            logical_effect_id: effect.into(),
            operation_contract: "contract@1".into(),
            connection_ref: "connection-1".into(),
        }
    }

    fn reserve(effect: &str, budget: u64) -> LedgerCommand {
        LedgerCommand::Reserve {
            request: ReserveRequest {
                key: key(effect),
                canonical_input: format!(r#"{{"effect":"{effect}"}}"#).into_bytes(),
                max_logical_calls: budget,
                max_dispatch_attempts: 1,
                lease_deadline: 30,
            },
            now: 0,
        }
    }

    fn planned() -> PlannedData {
        PlannedData {
            request_plan_hash: format!("sha256:{}", "1".repeat(64)),
            authority_facts_hash: format!("sha256:{}", "2".repeat(64)),
            implementation: "google-v1-request-plan".into(),
            endpoint: "https://provider.example".into(),
            next_attempt: 0,
        }
    }

    #[test]
    fn deterministic_routes_are_tenant_and_grant_scoped() {
        let a = authority();
        let mut b = a.clone();
        b.org_id = "org-2".into();
        let mut c = a.clone();
        c.grant_ref = "grant-2".into();
        assert_eq!(a.route_name().unwrap(), a.route_name().unwrap());
        assert_ne!(a.route_name().unwrap(), b.route_name().unwrap());
        assert_ne!(a.route_name().unwrap(), c.route_name().unwrap());
    }

    #[test]
    fn concurrent_effects_never_resurrect_budget_and_replay_is_exact() {
        let ledger = Arc::new(DurableLedgerModel::default());
        let authority = authority();
        let mut threads = Vec::new();
        for effect in ["a", "b"] {
            let ledger = ledger.clone();
            let authority = authority.clone();
            threads.push(std::thread::spawn(move || {
                ledger.execute(&authority, reserve(effect, 2))
            }));
        }
        let replies = threads
            .into_iter()
            .map(|thread| thread.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            replies
                .iter()
                .filter(|reply| reply.acquired == Some(true))
                .count(),
            2
        );
        assert_eq!(
            ledger.execute(&authority, reserve("c", 2)).unwrap_err(),
            BrokerError::Brk201
        );
        let replay = ledger.execute(&authority, reserve("a", 2)).unwrap();
        assert_eq!(replay.acquired, Some(false));
        let mut altered = reserve("a", 2);
        if let LedgerCommand::Reserve { request, .. } = &mut altered {
            request.canonical_input = br#"{"effect":"altered"}"#.to_vec();
        }
        assert_eq!(
            ledger.execute(&authority, altered).unwrap_err(),
            BrokerError::Brk203
        );
    }

    #[test]
    fn compaction_commit_has_only_old_or_new_restorable_pairs() {
        let authority = authority();
        let mut old = LedgerPersistence::default();
        for index in 0..(COMPACTION_THRESHOLD - 1) {
            old = apply_persisted_command(
                &authority,
                &old,
                reserve(&format!("boundary-{index:03}"), COMPACTION_THRESHOLD as u64),
            )
            .unwrap()
            .persistence;
        }
        assert!(old.snapshot.is_none());
        assert_eq!(old.tail.records.len(), COMPACTION_THRESHOLD - 1);
        let committed = apply_persisted_command(
            &authority,
            &old,
            reserve("boundary-final", COMPACTION_THRESHOLD as u64),
        )
        .unwrap();
        assert!(committed.compacted);
        let new = committed.persistence;

        // A storage crash can expose the complete old pair or complete new
        // pair. Both restore, and only the committed pair contains the command.
        let old_state = InMemoryLedger::restore(old.snapshot.as_ref(), &old.tail).unwrap();
        assert!(old_state.get(&key("boundary-final")).unwrap().is_none());
        let new_state = InMemoryLedger::restore(new.snapshot.as_ref(), &new.tail).unwrap();
        assert!(new_state.get(&key("boundary-final")).unwrap().is_some());
        assert_eq!(new_state.snapshot().unwrap(), new.snapshot.clone().unwrap());

        // This was the former two-write crash boundary: the new snapshot with
        // the 255-record old tail is structurally inconsistent and fails closed.
        assert!(matches!(
            InMemoryLedger::restore(new.snapshot.as_ref(), &old.tail),
            Err(BrokerError::Brk401)
        ));
    }

    #[test]
    fn compaction_keeps_long_histories_operable_and_tail_bounded() {
        let ledger = DurableLedgerModel::default();
        let authority = authority();
        for index in 0..4_100 {
            let effect = format!("long-{index:05}");
            ledger.execute(&authority, reserve(&effect, 4_101)).unwrap();
        }
        let state = ledger.persistence().unwrap();
        assert!(state.snapshot.is_some());
        assert!(state.tail.records.len() < COMPACTION_THRESHOLD);
        let replay = ledger
            .execute(&authority, reserve("long-00000", 4_101))
            .unwrap();
        assert_eq!(replay.acquired, Some(false));
        ledger
            .execute(&authority, reserve("long-final", 4_101))
            .unwrap();
        assert_eq!(
            ledger
                .execute(&authority, reserve("long-exhausted", 4_101))
                .unwrap_err(),
            BrokerError::Brk201
        );
    }

    #[test]
    fn lease_release_is_once_and_post_dispatch_recovery_is_ambiguous_without_refund() {
        let ledger = DurableLedgerModel::default();
        let authority = authority();
        let acquired = ledger
            .execute(&authority, reserve("release", 1))
            .unwrap()
            .snapshot
            .unwrap();
        let released = ledger
            .execute(
                &authority,
                LedgerCommand::Release {
                    key: key("release"),
                    lease_token: acquired.lease_token,
                    now: 30,
                    expired: true,
                },
            )
            .unwrap()
            .snapshot
            .unwrap();
        assert_eq!(released.budget_after, 1);
        let second = ledger
            .execute(
                &authority,
                LedgerCommand::Release {
                    key: key("release"),
                    lease_token: acquired.lease_token,
                    now: 30,
                    expired: true,
                },
            )
            .unwrap()
            .snapshot
            .unwrap();
        assert_eq!(second.budget_after, 1);

        let dispatched = ledger
            .execute(&authority, reserve("dispatched", 1))
            .unwrap()
            .snapshot
            .unwrap();
        ledger
            .execute(
                &authority,
                LedgerCommand::Plan {
                    key: key("dispatched"),
                    lease_token: dispatched.lease_token,
                    now: 0,
                    data: planned(),
                },
            )
            .unwrap();
        ledger
            .execute(
                &authority,
                LedgerCommand::MarkDispatched {
                    key: key("dispatched"),
                    lease_token: dispatched.lease_token,
                    now: 0,
                },
            )
            .unwrap();
        let recovered = ledger
            .execute(
                &authority,
                LedgerCommand::RecoverDispatched {
                    key: key("dispatched"),
                },
            )
            .unwrap()
            .snapshot
            .unwrap();
        assert!(matches!(
            recovered.state,
            InvocationState::Terminal(TerminalOutcome::Ambiguous)
        ));
        assert_eq!(recovered.budget_after, 0);
        assert_eq!(
            ledger.execute(&authority, reserve("after", 1)).unwrap_err(),
            BrokerError::Brk201
        );
    }

    #[test]
    fn terminal_outbox_recovery_is_byte_identical() {
        let ledger = DurableLedgerModel::default();
        let authority = authority();
        let acquired = ledger
            .execute(&authority, reserve("outbox", 1))
            .unwrap()
            .snapshot
            .unwrap();
        ledger
            .execute(
                &authority,
                LedgerCommand::Plan {
                    key: key("outbox"),
                    lease_token: acquired.lease_token,
                    now: 0,
                    data: planned(),
                },
            )
            .unwrap();
        ledger
            .execute(
                &authority,
                LedgerCommand::MarkDispatched {
                    key: key("outbox"),
                    lease_token: acquired.lease_token,
                    now: 0,
                },
            )
            .unwrap();
        let receipt = br#"{"receipt":"bounded-and-signed"}"#.to_vec();
        ledger
            .execute(
                &authority,
                LedgerCommand::Finish {
                    key: key("outbox"),
                    outcome: TerminalOutcome::Confirmed,
                    receipt: receipt.clone(),
                    response_projection: b"{}".to_vec(),
                    provider_request_id: None,
                },
            )
            .unwrap();
        let recovered = apply_command(
            &authority,
            &ledger.records().unwrap(),
            LedgerCommand::IssueReceipt { key: key("outbox") },
        )
        .unwrap();
        assert!(matches!(
            recovered.snapshot.unwrap().state,
            InvocationState::ReceiptIssued { receipt: bytes, .. } if bytes == receipt
        ));
    }
}
