use crate::BrokerError;
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ReservationKey {
    pub org_id: String,
    pub deployment_id: String,
    pub flow_ir_hash: String,
    pub run_id: String,
    pub node_id: String,
    pub logical_effect_id: String,
    pub operation_contract: String,
    pub connection_ref: String,
}
#[derive(Clone, Debug)]
pub struct ReserveRequest {
    pub key: ReservationKey,
    pub canonical_input: Vec<u8>,
    pub max_logical_calls: u64,
    pub max_dispatch_attempts: u8,
    pub lease_deadline: i64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedData {
    pub request_plan_hash: String,
    pub authority_facts_hash: String,
    pub implementation: String,
    pub endpoint: String,
    pub next_attempt: u8,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TerminalOutcome {
    Confirmed,
    Failed,
    Ambiguous,
    ReleasedExpired,
    ReleasedCancelled,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationState {
    Reserved,
    Planned(PlannedData),
    Dispatched {
        attempt: u8,
        planned: PlannedData,
    },
    Terminal(TerminalOutcome),
    ReceiptIssued {
        outcome: TerminalOutcome,
        receipt: Vec<u8>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntrySnapshot {
    pub key: ReservationKey,
    pub state: InvocationState,
    pub lease_token: u64,
    pub lease_deadline: i64,
    pub dispatch_attempts: u8,
    pub budget_before: u64,
    pub budget_after: u64,
}
#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalInputIdentity(Vec<u8>);
impl std::fmt::Debug for CanonicalInputIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CanonicalInputIdentity([REDACTED])")
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
/// A receipt built and validated by the kernel. External callers cannot
/// construct one and therefore cannot inject arbitrary signed bytes.
pub struct KernelReceipt {
    pub(crate) bytes: Vec<u8>,
    pub(crate) response_projection: Vec<u8>,
    pub(crate) provider_request_id: Option<String>,
}
impl KernelReceipt {
    pub(crate) fn new(
        bytes: Vec<u8>,
        response_projection: Vec<u8>,
        provider_request_id: Option<String>,
    ) -> Result<Self, BrokerError> {
        if bytes.len() > crate::artifacts::RECEIPT_MAX
            || response_projection.len() > crate::canonical::MAX_OPERATION_BYTES
        {
            return Err(BrokerError::Brk001);
        }
        Ok(Self {
            bytes,
            response_projection,
            provider_request_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableRecord {
    Reserved {
        key: ReservationKey,
        canonical_input: CanonicalInputIdentity,
        max_logical_calls: u64,
        max_dispatch_attempts: u8,
        lease_token: u64,
        lease_deadline: i64,
        budget_before: u64,
        budget_after: u64,
    },
    Planned {
        key: ReservationKey,
        lease_token: u64,
        data: PlannedData,
    },
    Dispatched {
        key: ReservationKey,
        lease_token: u64,
        attempt: u8,
    },
    /// Post-dispatch terminal records deliberately carry no aggregate budget.
    /// They can never overwrite a later concurrent reservation.
    Terminal {
        key: ReservationKey,
        outcome: TerminalOutcome,
        /// Complete bounded signed receipt outbox, atomically persisted with
        /// the terminal transition.
        pending_receipt: Vec<u8>,
        response_projection: Vec<u8>,
        provider_request_id: Option<String>,
    },
    /// The sole pre-dispatch compare-and-swap refund record.
    Released {
        key: ReservationKey,
        lease_token: u64,
        outcome: TerminalOutcome,
        budget_after: u64,
    },
    ReceiptIssued {
        key: ReservationKey,
        outcome: TerminalOutcome,
        receipt: Vec<u8>,
    },
    RetryAuthorized {
        key: ReservationKey,
        next_attempt: u8,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReserveResult {
    Acquired(EntrySnapshot),
    Redelivery(EntrySnapshot),
}

pub trait Ledger: Send + Sync {
    fn reserve(&self, request: ReserveRequest, now: i64) -> Result<ReserveResult, BrokerError>;
    fn plan(
        &self,
        key: &ReservationKey,
        token: u64,
        now: i64,
        data: PlannedData,
    ) -> Result<EntrySnapshot, BrokerError>;
    fn mark_dispatched(
        &self,
        key: &ReservationKey,
        token: u64,
        now: i64,
    ) -> Result<EntrySnapshot, BrokerError>;
    fn finish(
        &self,
        key: &ReservationKey,
        outcome: TerminalOutcome,
        receipt: KernelReceipt,
    ) -> Result<EntrySnapshot, BrokerError>;
    fn release(
        &self,
        key: &ReservationKey,
        token: u64,
        now: i64,
        expired: bool,
    ) -> Result<EntrySnapshot, BrokerError>;
    fn issue_receipt(&self, key: &ReservationKey) -> Result<EntrySnapshot, BrokerError>;
    fn issue_released_receipt(
        &self,
        key: &ReservationKey,
        receipt: KernelReceipt,
    ) -> Result<EntrySnapshot, BrokerError>;
    fn get(&self, key: &ReservationKey) -> Result<Option<EntrySnapshot>, BrokerError>;
    fn records(&self) -> Result<Vec<DurableRecord>, BrokerError>;
}

#[derive(Clone)]
struct Entry {
    key: ReservationKey,
    canonical_input: Vec<u8>,
    max_dispatch_attempts: u8,
    state: InvocationState,
    lease_token: u64,
    lease_deadline: i64,
    dispatch_attempts: u8,
    budget_before: u64,
    budget_after: u64,
    pending_receipt: Option<Vec<u8>>,
    last_planned: Option<PlannedData>,
}
#[derive(Default)]
struct Inner {
    entries: BTreeMap<ReservationKey, Entry>,
    budgets: BTreeMap<BudgetKey, u64>,
    records: Vec<DurableRecord>,
    next_token: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct BudgetKey {
    org: String,
    deployment: String,
    flow: String,
    run: String,
    node: String,
    operation: String,
}
impl From<&ReservationKey> for BudgetKey {
    fn from(k: &ReservationKey) -> Self {
        Self {
            org: k.org_id.clone(),
            deployment: k.deployment_id.clone(),
            flow: k.flow_ir_hash.clone(),
            run: k.run_id.clone(),
            node: k.node_id.clone(),
            operation: k.operation_contract.clone(),
        }
    }
}

#[derive(Default)]
pub struct InMemoryLedger {
    inner: Mutex<Inner>,
}
impl InMemoryLedger {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn recover(records: &[DurableRecord]) -> Result<Self, BrokerError> {
        let ledger = Self::new();
        {
            let mut inner = ledger.inner.lock().map_err(|_| BrokerError::Brk401)?;
            for record in records {
                apply_record(&mut inner, record.clone())?;
            }
            let dispatched: Vec<_> = inner
                .entries
                .values()
                .filter(|e| matches!(e.state, InvocationState::Dispatched { .. }))
                .map(|e| e.key.clone())
                .collect();
            for key in dispatched {
                // A dispatched record without its following atomic terminal
                // record has no response material. Recovery marks ambiguity
                // with an empty outbox; engine recovery must synthesize and
                // persist the ambiguous receipt before redelivery.
                let record = DurableRecord::Terminal {
                    key,
                    outcome: TerminalOutcome::Ambiguous,
                    pending_receipt: Vec::new(),
                    response_projection: Vec::new(),
                    provider_request_id: None,
                };
                apply_record(&mut inner, record)?;
            }
        }
        Ok(ledger)
    }
    pub fn retry_after_ambiguous(
        &self,
        key: &ReservationKey,
        safe_idempotent_retry: bool,
    ) -> Result<(), BrokerError> {
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = inner.entries.get(key).ok_or(BrokerError::Brk401)?;
        if matches!(
            entry.state,
            InvocationState::Terminal(TerminalOutcome::Ambiguous)
                | InvocationState::ReceiptIssued {
                    outcome: TerminalOutcome::Ambiguous,
                    ..
                }
        ) && !safe_idempotent_retry
        {
            return Err(BrokerError::Brk306);
        }
        if entry.dispatch_attempts >= entry.max_dispatch_attempts {
            return Err(BrokerError::Brk202);
        }
        if !matches!(
            entry.state,
            InvocationState::Terminal(TerminalOutcome::Ambiguous)
                | InvocationState::ReceiptIssued {
                    outcome: TerminalOutcome::Ambiguous,
                    ..
                }
        ) {
            return Err(BrokerError::Brk204);
        }
        let next_attempt = entry
            .dispatch_attempts
            .checked_add(1)
            .ok_or(BrokerError::Brk202)?;
        apply_record(
            &mut inner,
            DurableRecord::RetryAuthorized {
                key: key.clone(),
                next_attempt,
            },
        )?;
        Ok(())
    }
}

impl Ledger for InMemoryLedger {
    fn reserve(&self, request: ReserveRequest, now: i64) -> Result<ReserveResult, BrokerError> {
        if request.max_logical_calls == 0
            || request.max_dispatch_attempts == 0
            || request.lease_deadline <= now
        {
            return Err(BrokerError::Brk205);
        }
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        if let Some((existing_key, existing)) = inner.entries.iter().find(|(k, _)| {
            k.org_id == request.key.org_id
                && k.deployment_id == request.key.deployment_id
                && k.run_id == request.key.run_id
                && k.node_id == request.key.node_id
                && k.logical_effect_id == request.key.logical_effect_id
        }) {
            if existing_key != &request.key || existing.canonical_input != request.canonical_input {
                return Err(BrokerError::Brk203);
            }
            return Ok(ReserveResult::Redelivery(snapshot(existing)));
        }
        let budget_key = BudgetKey::from(&request.key);
        let remaining = inner
            .budgets
            .entry(budget_key)
            .or_insert(request.max_logical_calls);
        if *remaining == 0 {
            return Err(BrokerError::Brk201);
        }
        let before = *remaining;
        *remaining -= 1;
        let after = *remaining;
        inner.next_token = inner.next_token.checked_add(1).ok_or(BrokerError::Brk401)?;
        let token = inner.next_token;
        let key = request.key;
        let record = DurableRecord::Reserved {
            key: key.clone(),
            canonical_input: CanonicalInputIdentity(request.canonical_input),
            max_logical_calls: request.max_logical_calls,
            max_dispatch_attempts: request.max_dispatch_attempts,
            lease_token: token,
            lease_deadline: request.lease_deadline,
            budget_before: before,
            budget_after: after,
        };
        apply_record(&mut inner, record)?;
        Ok(ReserveResult::Acquired(snapshot(&inner.entries[&key])))
    }
    fn plan(
        &self,
        key: &ReservationKey,
        token: u64,
        now: i64,
        mut data: PlannedData,
    ) -> Result<EntrySnapshot, BrokerError> {
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = owned_for_work(&inner, key, token, now)?;
        if entry.dispatch_attempts >= entry.max_dispatch_attempts {
            return Err(BrokerError::Brk202);
        }
        data.next_attempt = entry.dispatch_attempts + 1;
        let record = DurableRecord::Planned {
            key: key.clone(),
            lease_token: token,
            data,
        };
        apply_record(&mut inner, record)?;
        Ok(snapshot(&inner.entries[key]))
    }
    fn mark_dispatched(
        &self,
        key: &ReservationKey,
        token: u64,
        now: i64,
    ) -> Result<EntrySnapshot, BrokerError> {
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = owned_for_work(&inner, key, token, now)?;
        let attempt = match &entry.state {
            InvocationState::Planned(data) => data.next_attempt,
            _ => return Err(BrokerError::Brk401),
        };
        if attempt > entry.max_dispatch_attempts {
            return Err(BrokerError::Brk202);
        }
        let record = DurableRecord::Dispatched {
            key: key.clone(),
            lease_token: token,
            attempt,
        };
        apply_record(&mut inner, record)?;
        Ok(snapshot(&inner.entries[key]))
    }
    fn finish(
        &self,
        key: &ReservationKey,
        outcome: TerminalOutcome,
        receipt: KernelReceipt,
    ) -> Result<EntrySnapshot, BrokerError> {
        if matches!(
            outcome,
            TerminalOutcome::ReleasedExpired | TerminalOutcome::ReleasedCancelled
        ) {
            return Err(BrokerError::Brk401);
        }
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = inner.entries.get(key).ok_or(BrokerError::Brk401)?;
        if !matches!(entry.state, InvocationState::Dispatched { .. }) {
            return Err(BrokerError::Brk401);
        }
        let record = DurableRecord::Terminal {
            key: key.clone(),
            outcome,
            pending_receipt: receipt.bytes,
            response_projection: receipt.response_projection,
            provider_request_id: receipt.provider_request_id,
        };
        apply_record(&mut inner, record)?;
        Ok(snapshot(&inner.entries[key]))
    }
    fn release(
        &self,
        key: &ReservationKey,
        token: u64,
        now: i64,
        expired: bool,
    ) -> Result<EntrySnapshot, BrokerError> {
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = inner.entries.get(key).ok_or(BrokerError::Brk401)?.clone();
        if matches!(
            entry.state,
            InvocationState::Terminal(
                TerminalOutcome::ReleasedExpired | TerminalOutcome::ReleasedCancelled
            ) | InvocationState::ReceiptIssued {
                outcome: TerminalOutcome::ReleasedExpired | TerminalOutcome::ReleasedCancelled,
                ..
            }
        ) {
            return Ok(snapshot(&entry));
        }
        if entry.lease_token != token
            || (!expired && now >= entry.lease_deadline)
            || (expired && now < entry.lease_deadline)
        {
            return Err(BrokerError::Brk205);
        }
        if !matches!(
            entry.state,
            InvocationState::Reserved | InvocationState::Planned(_)
        ) {
            return Err(BrokerError::Brk401);
        }
        let budget_key = BudgetKey::from(key);
        let remaining = inner
            .budgets
            .get_mut(&budget_key)
            .ok_or(BrokerError::Brk401)?;
        *remaining = remaining.checked_add(1).ok_or(BrokerError::Brk401)?;
        let outcome = if expired {
            TerminalOutcome::ReleasedExpired
        } else {
            TerminalOutcome::ReleasedCancelled
        };
        let record = DurableRecord::Released {
            key: key.clone(),
            lease_token: token,
            outcome,
            budget_after: *remaining,
        };
        apply_record(&mut inner, record)?;
        Ok(snapshot(&inner.entries[key]))
    }
    fn issue_receipt(&self, key: &ReservationKey) -> Result<EntrySnapshot, BrokerError> {
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = inner.entries.get(key).ok_or(BrokerError::Brk401)?;
        if matches!(entry.state, InvocationState::ReceiptIssued { .. }) {
            return Ok(snapshot(entry));
        }
        let outcome = match &entry.state {
            InvocationState::Terminal(o) => o.clone(),
            _ => return Err(BrokerError::Brk401),
        };
        let receipt = entry
            .pending_receipt
            .clone()
            .filter(|bytes| !bytes.is_empty())
            .ok_or(BrokerError::Brk401)?;
        let record = DurableRecord::ReceiptIssued {
            key: key.clone(),
            outcome,
            receipt,
        };
        apply_record(&mut inner, record)?;
        Ok(snapshot(&inner.entries[key]))
    }
    fn issue_released_receipt(
        &self,
        key: &ReservationKey,
        receipt: KernelReceipt,
    ) -> Result<EntrySnapshot, BrokerError> {
        let mut inner = self.inner.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = inner.entries.get(key).ok_or(BrokerError::Brk401)?;
        if matches!(entry.state, InvocationState::ReceiptIssued { .. }) {
            return Ok(snapshot(entry));
        }
        let outcome = match &entry.state {
            InvocationState::Terminal(
                outcome @ (TerminalOutcome::ReleasedExpired | TerminalOutcome::ReleasedCancelled),
            ) => outcome.clone(),
            _ => return Err(BrokerError::Brk401),
        };
        apply_record(
            &mut inner,
            DurableRecord::ReceiptIssued {
                key: key.clone(),
                outcome,
                receipt: receipt.bytes,
            },
        )?;
        Ok(snapshot(&inner.entries[key]))
    }
    fn get(&self, key: &ReservationKey) -> Result<Option<EntrySnapshot>, BrokerError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .entries
            .get(key)
            .map(snapshot))
    }
    fn records(&self) -> Result<Vec<DurableRecord>, BrokerError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .records
            .clone())
    }
}
fn owned_for_work(
    inner: &Inner,
    key: &ReservationKey,
    token: u64,
    now: i64,
) -> Result<Entry, BrokerError> {
    let entry = inner.entries.get(key).ok_or(BrokerError::Brk401)?;
    if entry.lease_token != token || now >= entry.lease_deadline {
        return Err(BrokerError::Brk205);
    }
    Ok(entry.clone())
}
fn snapshot(e: &Entry) -> EntrySnapshot {
    EntrySnapshot {
        key: e.key.clone(),
        state: e.state.clone(),
        lease_token: e.lease_token,
        lease_deadline: e.lease_deadline,
        dispatch_attempts: e.dispatch_attempts,
        budget_before: e.budget_before,
        budget_after: e.budget_after,
    }
}

fn apply_record(inner: &mut Inner, record: DurableRecord) -> Result<(), BrokerError> {
    match &record {
        DurableRecord::Reserved {
            key,
            canonical_input,
            max_logical_calls,
            max_dispatch_attempts,
            lease_token,
            lease_deadline,
            budget_before,
            budget_after,
        } => {
            let budget_key = BudgetKey::from(key);
            inner.budgets.insert(budget_key, *budget_after);
            inner.next_token = inner.next_token.max(*lease_token);
            inner.entries.insert(
                key.clone(),
                Entry {
                    key: key.clone(),
                    canonical_input: canonical_input.0.clone(),
                    max_dispatch_attempts: *max_dispatch_attempts,
                    state: InvocationState::Reserved,
                    lease_token: *lease_token,
                    lease_deadline: *lease_deadline,
                    dispatch_attempts: 0,
                    budget_before: *budget_before,
                    budget_after: *budget_after,
                    pending_receipt: None,
                    last_planned: None,
                },
            );
            let _ = max_logical_calls;
        }
        DurableRecord::Planned {
            key,
            lease_token,
            data,
        } => {
            let e = inner.entries.get_mut(key).ok_or(BrokerError::Brk401)?;
            if e.lease_token != *lease_token
                || !matches!(
                    e.state,
                    InvocationState::Reserved | InvocationState::Planned(_)
                )
            {
                return Err(BrokerError::Brk401);
            }
            e.last_planned = Some(data.clone());
            e.state = InvocationState::Planned(data.clone());
        }
        DurableRecord::Dispatched {
            key,
            lease_token,
            attempt,
        } => {
            let e = inner.entries.get_mut(key).ok_or(BrokerError::Brk401)?;
            if e.lease_token != *lease_token {
                return Err(BrokerError::Brk401);
            }
            let planned = match &e.state {
                InvocationState::Planned(p) => p.clone(),
                _ => return Err(BrokerError::Brk401),
            };
            e.dispatch_attempts = *attempt;
            e.state = InvocationState::Dispatched {
                attempt: *attempt,
                planned,
            };
        }
        DurableRecord::Terminal {
            key,
            outcome,
            pending_receipt,
            response_projection: _,
            provider_request_id: _,
        } => {
            let e = inner.entries.get_mut(key).ok_or(BrokerError::Brk401)?;
            if !matches!(e.state, InvocationState::Dispatched { .. }) {
                return Err(BrokerError::Brk401);
            }
            e.pending_receipt = Some(pending_receipt.clone());
            e.state = InvocationState::Terminal(outcome.clone());
        }
        DurableRecord::Released {
            key,
            lease_token,
            outcome,
            budget_after,
        } => {
            let e = inner.entries.get_mut(key).ok_or(BrokerError::Brk401)?;
            if e.lease_token != *lease_token
                || !matches!(
                    e.state,
                    InvocationState::Reserved | InvocationState::Planned(_)
                )
                || !matches!(
                    outcome,
                    TerminalOutcome::ReleasedExpired | TerminalOutcome::ReleasedCancelled
                )
            {
                return Err(BrokerError::Brk401);
            }
            inner.budgets.insert(BudgetKey::from(key), *budget_after);
            let e = inner.entries.get_mut(key).ok_or(BrokerError::Brk401)?;
            e.budget_after = *budget_after;
            e.pending_receipt = None;
            e.state = InvocationState::Terminal(outcome.clone());
        }
        DurableRecord::ReceiptIssued {
            key,
            outcome,
            receipt,
        } => {
            let e = inner.entries.get_mut(key).ok_or(BrokerError::Brk401)?;
            e.state = InvocationState::ReceiptIssued {
                outcome: outcome.clone(),
                receipt: receipt.clone(),
            };
        }
        DurableRecord::RetryAuthorized { key, next_attempt } => {
            let e = inner.entries.get_mut(key).ok_or(BrokerError::Brk401)?;
            if *next_attempt != e.dispatch_attempts + 1
                || *next_attempt > e.max_dispatch_attempts
                || !matches!(
                    e.state,
                    InvocationState::Terminal(TerminalOutcome::Ambiguous)
                        | InvocationState::ReceiptIssued {
                            outcome: TerminalOutcome::Ambiguous,
                            ..
                        }
                )
            {
                return Err(BrokerError::Brk401);
            }
            let mut planned = e.last_planned.clone().ok_or(BrokerError::Brk401)?;
            planned.next_attempt = *next_attempt;
            e.pending_receipt = None;
            e.state = InvocationState::Planned(planned);
        }
    }
    inner.records.push(record);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(effect: &str) -> ReservationKey {
        ReservationKey {
            org_id: "o".into(),
            deployment_id: "d".into(),
            flow_ir_hash: "f".into(),
            run_id: "r".into(),
            node_id: "n".into(),
            logical_effect_id: effect.into(),
            operation_contract: "c".into(),
            connection_ref: "x".into(),
        }
    }
    #[test]
    fn replay_budget_release_and_recovery() {
        let l = InMemoryLedger::new();
        let req = ReserveRequest {
            key: key("e1"),
            canonical_input: b"secret".to_vec(),
            max_logical_calls: 1,
            max_dispatch_attempts: 1,
            lease_deadline: 10,
        };
        let acquired = match l.reserve(req.clone(), 0).unwrap() {
            ReserveResult::Acquired(v) => v,
            _ => panic!(),
        };
        assert!(matches!(
            l.reserve(req.clone(), 0).unwrap(),
            ReserveResult::Redelivery(_)
        ));
        let mut altered = req.clone();
        altered.canonical_input = b"other".to_vec();
        assert_eq!(l.reserve(altered, 0).unwrap_err(), BrokerError::Brk203);
        let released = l.release(&req.key, acquired.lease_token, 10, true).unwrap();
        assert_eq!(released.budget_after, 1);
        assert_eq!(
            l.release(&req.key, acquired.lease_token, 10, true)
                .unwrap()
                .budget_after,
            1
        );
        let recovered = InMemoryLedger::recover(&l.records().unwrap()).unwrap();
        assert_eq!(recovered.get(&req.key).unwrap().unwrap().budget_after, 1);
    }
    #[test]
    fn logical_and_dispatch_budgets_fail_closed() {
        let ledger = InMemoryLedger::new();
        let first = key("one");
        let acquired = match ledger
            .reserve(
                ReserveRequest {
                    key: first.clone(),
                    canonical_input: b"{}".to_vec(),
                    max_logical_calls: 1,
                    max_dispatch_attempts: 1,
                    lease_deadline: 10,
                },
                0,
            )
            .unwrap()
        {
            ReserveResult::Acquired(value) => value,
            _ => panic!(),
        };
        assert_eq!(
            ledger
                .reserve(
                    ReserveRequest {
                        key: key("two"),
                        canonical_input: b"{}".to_vec(),
                        max_logical_calls: 1,
                        max_dispatch_attempts: 1,
                        lease_deadline: 10
                    },
                    0
                )
                .unwrap_err(),
            BrokerError::Brk201
        );
        ledger
            .plan(
                &first,
                acquired.lease_token,
                0,
                PlannedData {
                    request_plan_hash: "p".into(),
                    authority_facts_hash: "f".into(),
                    implementation: "i".into(),
                    endpoint: "e".into(),
                    next_attempt: 0,
                },
            )
            .unwrap();
        ledger
            .mark_dispatched(&first, acquired.lease_token, 0)
            .unwrap();
        ledger
            .finish(
                &first,
                TerminalOutcome::Ambiguous,
                KernelReceipt::new(b"{}".to_vec(), Vec::new(), None).unwrap(),
            )
            .unwrap();
        assert_eq!(
            ledger.retry_after_ambiguous(&first, true),
            Err(BrokerError::Brk202)
        );
    }

    #[test]
    fn terminal_finish_never_resurrects_concurrent_budget_and_replay_preserves_it() {
        let ledger = InMemoryLedger::new();
        let reserve = |effect: &str| ReserveRequest {
            key: key(effect),
            canonical_input: b"{}".to_vec(),
            max_logical_calls: 2,
            max_dispatch_attempts: 1,
            lease_deadline: 10,
        };
        let a = match ledger.reserve(reserve("a"), 0).unwrap() {
            ReserveResult::Acquired(value) => value,
            _ => unreachable!(),
        };
        let _b = ledger.reserve(reserve("b"), 0).unwrap();
        ledger
            .plan(
                &a.key,
                a.lease_token,
                0,
                PlannedData {
                    request_plan_hash: "p".into(),
                    authority_facts_hash: "f".into(),
                    implementation: "i".into(),
                    endpoint: "https://provider.example".into(),
                    next_attempt: 0,
                },
            )
            .unwrap();
        ledger.mark_dispatched(&a.key, a.lease_token, 0).unwrap();
        ledger
            .finish(
                &a.key,
                TerminalOutcome::Confirmed,
                KernelReceipt::new(b"receipt".to_vec(), Vec::new(), None).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            ledger.reserve(reserve("c"), 0),
            Err(BrokerError::Brk201)
        ));

        let recovered = InMemoryLedger::recover(&ledger.records().unwrap()).unwrap();
        assert!(matches!(
            recovered.reserve(reserve("c"), 0),
            Err(BrokerError::Brk201)
        ));
    }

    #[test]
    fn recovery_from_reserved_and_planned_releases_exactly_once() {
        for planned in [false, true] {
            let original = InMemoryLedger::new();
            let effect = if planned { "planned" } else { "reserved" };
            let key = key(effect);
            let acquired = match original
                .reserve(
                    ReserveRequest {
                        key: key.clone(),
                        canonical_input: b"{}".to_vec(),
                        max_logical_calls: 1,
                        max_dispatch_attempts: 1,
                        lease_deadline: 10,
                    },
                    0,
                )
                .unwrap()
            {
                ReserveResult::Acquired(value) => value,
                _ => panic!(),
            };
            if planned {
                original
                    .plan(
                        &key,
                        acquired.lease_token,
                        0,
                        PlannedData {
                            request_plan_hash: "p".into(),
                            authority_facts_hash: "f".into(),
                            implementation: "i".into(),
                            endpoint: "e".into(),
                            next_attempt: 0,
                        },
                    )
                    .unwrap();
            }
            let recovered = InMemoryLedger::recover(&original.records().unwrap()).unwrap();
            assert_eq!(
                recovered
                    .release(&key, acquired.lease_token, 10, true)
                    .unwrap()
                    .budget_after,
                1
            );
            assert_eq!(
                recovered
                    .release(&key, acquired.lease_token, 10, true)
                    .unwrap()
                    .budget_after,
                1
            );
        }
    }

    #[test]
    fn dispatched_loss_is_ambiguous_and_never_refunds() {
        let l = InMemoryLedger::new();
        let k = key("e");
        let acquired = match l
            .reserve(
                ReserveRequest {
                    key: k.clone(),
                    canonical_input: b"{}".to_vec(),
                    max_logical_calls: 1,
                    max_dispatch_attempts: 1,
                    lease_deadline: 10,
                },
                0,
            )
            .unwrap()
        {
            ReserveResult::Acquired(v) => v,
            _ => panic!(),
        };
        l.plan(
            &k,
            acquired.lease_token,
            0,
            PlannedData {
                request_plan_hash: "p".into(),
                authority_facts_hash: "f".into(),
                implementation: "i".into(),
                endpoint: "e".into(),
                next_attempt: 0,
            },
        )
        .unwrap();
        l.mark_dispatched(&k, acquired.lease_token, 0).unwrap();
        let recovered = InMemoryLedger::recover(&l.records().unwrap()).unwrap();
        assert!(matches!(
            recovered.get(&k).unwrap().unwrap().state,
            InvocationState::Terminal(TerminalOutcome::Ambiguous)
        ));
        assert_eq!(
            recovered.retry_after_ambiguous(&k, false),
            Err(BrokerError::Brk306)
        );
        assert_eq!(recovered.get(&k).unwrap().unwrap().budget_after, 0);
    }
}
