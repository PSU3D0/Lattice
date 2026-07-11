//! S15 — the cron canary.
//!
//! The first shipped example that composes all three of the substrate's
//! headline surfaces in one flow:
//!
//! 1. a **schedule (cron) trigger** — the entrypoint fires every 5 minutes and
//!    the trigger node receives a typed [`dag_core::ScheduledEvent`]
//!    (`impl-docs/spec/schedule-trigger.md`);
//! 2. a **real connector op** — the poll uses
//!    [`connector_github_issues::ops::GithubIssuesList`] (a `ReadOnly`,
//!    `resource::http::read` op) to list open issues for a repository;
//! 3. **honest, enforced effects** — every node declares its effects/hints
//!    truthfully. `poll_open_issues` declares only the connector op (so its
//!    single grant is `resource::http::read`); `record_summary` declares
//!    `resource::kv::{read,write}`. CAP110 denies any undeclared access at
//!    runtime; EFFECT202 rejects a dishonest declaration at validation.
//!
//! ## Idempotency (the teaching artifact)
//!
//! Cron delivery is **at-least-once** (platform retries, dev-loop `--once`
//! replays). The spec's guidance is to key effectful downstream writes on the
//! *scheduled* time, which is stable across redeliveries of one fire:
//!
//! > `key = "<flow>:<trigger>:{scheduled_time_ms}"`
//!
//! `record_summary` does exactly that: it derives a deterministic KV key from
//! `scheduled_time_ms` and treats the write as an **upsert** — if a row for
//! this fire already exists it does not write a second one. Two deliveries of
//! the same fire therefore collapse to one row (`stored: false` on the
//! redelivery), while two *distinct* fires produce two rows. There is no
//! reliance on wall-clock time anywhere in the write path.

use capabilities::context;
use connector_github_issues::ops::GithubIssuesList;
use connector_github_issues::{GithubIssueState, GithubIssuesListInput};
use dag_core::{NodeError, NodeResult, ScheduledEvent};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};

/// The repository this canary polls. Real deployments would source these from
/// config/bindings; constants keep the example self-contained.
const POLL_OWNER: &str = "rust-lang";
const POLL_REPO: &str = "cargo";
/// Cadence: every five minutes, UTC (Cloudflare cron dialect).
pub const SCHEDULE_CRON: &str = "*/5 * * * *";
/// Trigger alias — also the middle segment of the idempotency key.
pub const TRIGGER_ALIAS: &str = "poll_trigger";
/// Flow name — the leading segment of the idempotency key.
pub const FLOW_NAME: &str = "s15_scheduled_poll_flow";
/// Cap the poll so the summary stays bounded regardless of repo size.
const MAX_ISSUES: u32 = 20;

/// Planned poll window derived from one scheduled fire. Carries
/// `scheduled_time_ms` forward so the terminal write can key on it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PollWindow {
    pub scheduled_time_ms: u64,
    pub cron: String,
    pub owner: String,
    pub repo: String,
}

/// The summary the flow computes for one fire — also the row body persisted to
/// KV. Deterministic given a fire's issues, so a redelivered write is a no-op.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IssueSnapshot {
    pub scheduled_time_ms: u64,
    pub cron: String,
    pub owner: String,
    pub repo: String,
    pub open_count: u32,
    pub issue_numbers: Vec<u64>,
    pub sample_titles: Vec<String>,
}

/// Terminal capture: what the write did, without re-embedding the whole row.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SummaryRecord {
    /// The idempotency-bearing KV key: `<flow>:<trigger>:{scheduled_time_ms}`.
    pub key: String,
    /// `true` when this fire wrote a new row; `false` when the row already
    /// existed (a redelivery of the same fire — deduplicated).
    pub stored: bool,
    pub open_count: u32,
    pub scheduled_time_ms: u64,
}

/// The idempotency key for one scheduled fire. Public so tests (and readers)
/// can see it is a pure function of the *scheduled* time — the spec's
/// `<flow>:<trigger>:{scheduled_time_ms}` shape.
pub fn summary_key(scheduled_time_ms: u64) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{scheduled_time_ms}")
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

/// Schedule trigger. Its input type is **exactly** `ScheduledEvent` (the macro
/// asserts this for schedule entrypoints); it passes the fire through so
/// downstream nodes see the scheduled time and cron verbatim.
#[def_node(
    trigger,
    name = "PollTrigger",
    summary = "Cron ingress; receives the typed ScheduledEvent for this fire",
    effects = "Pure",
    determinism = "Strict"
)]
async fn poll_trigger(event: ScheduledEvent) -> NodeResult<ScheduledEvent> {
    Ok(event)
}

/// Turn a fire into a concrete poll window (which repo, which scheduled time).
#[def_node(
    name = "PlanPoll",
    summary = "Derive the poll window (repo + scheduled time) from the fire",
    effects = "Pure",
    determinism = "Strict"
)]
async fn plan_poll(event: ScheduledEvent) -> NodeResult<PollWindow> {
    Ok(PollWindow {
        scheduled_time_ms: event.scheduled_time_ms,
        cron: event.cron,
        owner: POLL_OWNER.to_string(),
        repo: POLL_REPO.to_string(),
    })
}

/// Poll open issues via the real GitHub connector op. Declaring
/// `connector_ops(GithubIssuesList)` is what makes the effect declaration
/// honest: it contributes exactly the op's `resource::http::read` hint and
/// nothing more, so CAP110 would deny any attempt to reach further.
#[def_node(
    name = "PollOpenIssues",
    // The identifier is namespaced under the GitHub issues connector: this node
    // is bound to `connector.github.issues`, and the runtime derives a node's
    // bound-connection scope from the connector-prefixed identifier (the op is
    // BoundConnection-only). `connector_ops(GithubIssuesList)` keeps the effect
    // declaration honest — the node's sole grant is the op's http_read hint.
    identifier = "connector.github.issues.scheduled_poll",
    summary = "List open issues for the repository using connector.github.issues.list",
    effects = "ReadOnly",
    determinism = "BestEffort",
    connector_ops(GithubIssuesList)
)]
async fn poll_open_issues(window: PollWindow) -> NodeResult<IssueSnapshot> {
    let listed = GithubIssuesList::invoke(&GithubIssuesListInput {
        owner: window.owner.clone(),
        repo: window.repo.clone(),
        state: Some(GithubIssueState::Open),
        return_all: false,
        limit: Some(MAX_ISSUES),
    })
    .await
    .map_err(|err| node_error(format!("connector.github.issues.list failed: {err}")))?;

    let issue_numbers = listed.items.iter().map(|issue| issue.number).collect();
    let sample_titles = listed
        .items
        .iter()
        .take(5)
        .map(|issue| issue.title.clone())
        .collect();

    Ok(IssueSnapshot {
        scheduled_time_ms: window.scheduled_time_ms,
        cron: window.cron,
        owner: window.owner,
        repo: window.repo,
        open_count: listed.items.len() as u32,
        issue_numbers,
        sample_titles,
    })
}

/// Effectful terminal write. Idempotent by construction: the KV key is a pure
/// function of `scheduled_time_ms` (stable across redeliveries), and the write
/// is an upsert that no-ops when the row already exists.
#[def_node(
    name = "RecordSummary",
    summary = "Upsert the fire's summary row into KV, keyed on scheduled_time_ms",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_summary(snapshot: IssueSnapshot) -> NodeResult<SummaryRecord> {
    let key = summary_key(snapshot.scheduled_time_ms);
    let scheduled_time_ms = snapshot.scheduled_time_ms;
    let open_count = snapshot.open_count;

    let stored = context::with_current_async(|resources| async move {
        let kv = resources.kv().ok_or_else(|| {
            NodeError::new("record_summary requires a KV capability (declare resource::kv::write)")
        })?;

        // At-least-once + idempotency (schedule-trigger.md §5): the scheduled
        // time is identical across redeliveries of one fire, so a redelivery
        // recomputes this exact key and finds the row already present — we do
        // NOT write a duplicate. Distinct fires have distinct keys.
        if kv
            .get(&key)
            .await
            .map_err(|err| node_error(format!("kv get failed: {err}")))?
            .is_some()
        {
            return Ok::<bool, NodeError>(false);
        }

        let row = serde_json::to_vec(&snapshot)
            .map_err(|err| node_error(format!("serialize summary row: {err}")))?;
        kv.put(&key, &row, None)
            .await
            .map_err(|err| node_error(format!("kv put failed: {err}")))?;
        Ok(true)
    })
    .await
    .ok_or_else(|| NodeError::new("record_summary missing ResourceAccess context"))??;

    Ok(SummaryRecord {
        key: summary_key(scheduled_time_ms),
        stored,
        open_count,
        scheduled_time_ms,
    })
}

/// Terminal capture. A schedule run has no caller, so this output is logged and
/// discarded; the durable effect is the KV row `record_summary` wrote.
#[def_node(
    name = "Capture",
    summary = "Capture the scheduled poll's summary record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: SummaryRecord) -> NodeResult<SummaryRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s15_scheduled_poll_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Cron canary: every-5-minutes GitHub issue poll writing an idempotent summary row keyed on the fire's scheduled_time_ms";

    let poll_trigger = node!(poll_trigger);
    let plan_poll = node!(plan_poll);
    let poll_open_issues = node!(poll_open_issues);
    let record_summary = node!(record_summary);
    let capture = node!(capture);

    connect!(poll_trigger -> plan_poll);
    connect!(plan_poll -> poll_open_issues);
    connect!(poll_open_issues -> record_summary);
    connect!(record_summary -> capture);

    entrypoint!({
        trigger: "poll_trigger",
        capture: "capture",
        schedule: "*/5 * * * *",
        deadline_ms: 30_000,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use std::collections::BTreeMap;
    use std::time::Duration;

    use async_trait::async_trait;
    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::durability::{
        CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore,
        Lease,
    };
    use capabilities::kv::{KeyValue, MemoryKv};
    use capabilities::scoped::ScopedResources;
    use capabilities::{Capability, ResourceAccess, ResourceBag, context};
    use connector_github_issues::runtime::transport::EnvConnectorRuntime;
    use dag_core::EffectHint;
    use dag_core::requirements::TriggerKind;
    use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use kernel_plan::derive_requirements;

    /// Minimal in-memory checkpoint store so the Effectful write path satisfies
    /// the flow's durability requirement in-process (the CLI dev scheduler
    /// attaches its own; here we supply one directly).
    #[derive(Default)]
    struct MemoryCheckpointStore {
        records: Mutex<BTreeMap<String, CheckpointRecord>>,
    }

    impl Capability for MemoryCheckpointStore {
        fn name(&self) -> &'static str {
            "checkpoint_store.memory"
        }
    }

    #[async_trait]
    impl CheckpointStore for MemoryCheckpointStore {
        async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
            let handle = CheckpointHandle {
                checkpoint_id: record.checkpoint_id.clone(),
                flow_id: record.flow_id.clone(),
                run_id: record.run_id.clone(),
            };
            self.records
                .lock()
                .expect("records")
                .insert(record.checkpoint_id.clone(), record);
            Ok(handle)
        }

        async fn get(
            &self,
            handle: &CheckpointHandle,
        ) -> Result<CheckpointRecord, CheckpointError> {
            self.records
                .lock()
                .expect("records")
                .get(&handle.checkpoint_id)
                .cloned()
                .ok_or(CheckpointError::NotFound)
        }

        async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
            self.records
                .lock()
                .expect("records")
                .remove(&handle.checkpoint_id);
            Ok(())
        }

        async fn lease(
            &self,
            handle: &CheckpointHandle,
            ttl: Duration,
        ) -> Result<Lease, CheckpointError> {
            Ok(Lease {
                lease_id: format!("lease:{}", handle.checkpoint_id),
                expires_at_ms: ttl.as_millis().try_into().unwrap_or(u64::MAX),
            })
        }

        async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn list(
            &self,
            _filter: CheckpointFilter,
        ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
            Ok(Vec::new())
        }
    }

    const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GITHUB_DEFAULT_BASE_URL";
    const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GITHUB_PAT";

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe { std::env::remove_var(key) };
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    fn sample_snapshot(scheduled_time_ms: u64) -> IssueSnapshot {
        IssueSnapshot {
            scheduled_time_ms,
            cron: SCHEDULE_CRON.to_string(),
            owner: POLL_OWNER.to_string(),
            repo: POLL_REPO.to_string(),
            open_count: 2,
            issue_numbers: vec![7, 9],
            sample_titles: vec!["first".to_string(), "second".to_string()],
        }
    }

    fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
        let kv = Arc::new(MemoryKv::new());
        let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
        (kv, bag)
    }

    fn scheduled_event(scheduled_time_ms: u64) -> serde_json::Value {
        serde_json::to_value(ScheduledEvent {
            scheduled_time_ms,
            cron: SCHEDULE_CRON.to_string(),
        })
        .expect("serialize scheduled event")
    }

    // ---- Flow validation + FlowRequirements derivation ------------------

    #[test]
    fn flow_validates_and_derives_schedule_requirement() {
        let ir = validated_ir();
        let requirements = derive_requirements(&ir);

        // Exactly one trigger, wired to a schedule entrypoint carrying the cron.
        let trigger = requirements
            .triggers
            .iter()
            .find(|t| t.alias == TRIGGER_ALIAS)
            .expect("poll_trigger requirement present");
        assert_eq!(trigger.kind, TriggerKind::Schedule);
        assert_eq!(trigger.crons, vec![SCHEDULE_CRON.to_string()]);

        let entrypoint = requirements
            .entrypoints
            .iter()
            .find(|e| e.trigger_alias == TRIGGER_ALIAS)
            .expect("schedule entrypoint requirement present");
        assert_eq!(entrypoint.schedule.as_deref(), Some(SCHEDULE_CRON));
        assert_eq!(entrypoint.capture_alias, "capture");
    }

    #[test]
    fn flow_shape_has_connector_poll_and_effectful_write() {
        let ir = flow();
        let aliases: Vec<&str> = ir.nodes.iter().map(|n| n.alias.as_str()).collect();
        assert!(aliases.contains(&"poll_open_issues"));
        assert!(aliases.contains(&"record_summary"));

        // The poll node carries the connector op; the write node carries the
        // kv hints. This is the honest declaration EFFECT202 enforces.
        let poll = ir
            .nodes
            .iter()
            .find(|n| n.alias == "poll_open_issues")
            .expect("poll node");
        assert!(
            poll.effect_hints
                .iter()
                .any(|h| h == EffectHint::HttpRead.as_str()),
            "poll node must honestly declare http_read: {:?}",
            poll.effect_hints
        );
        let write = ir
            .nodes
            .iter()
            .find(|n| n.alias == "record_summary")
            .expect("write node");
        assert!(
            write
                .effect_hints
                .iter()
                .any(|h| h == EffectHint::KvWrite.as_str()),
            "write node must honestly declare kv_write: {:?}",
            write.effect_hints
        );
    }

    // ---- Tier C honesty: declared hints are sufficient AND load-bearing --

    #[tokio::test]
    async fn record_summary_succeeds_under_exactly_declared_kv_hints() {
        let (_kv, bag) = kv_bag();
        // Grant exactly what the node declares (kv_read + kv_write).
        let scoped = Arc::new(ScopedResources::new(
            "record_summary",
            bag,
            [EffectHint::KvRead, EffectHint::KvWrite],
        ));
        let view: Arc<dyn ResourceAccess> = scoped.clone();

        let record = context::with_resources(view, async {
            record_summary(sample_snapshot(1_782_993_900_000))
                .await
                .expect("write succeeds under declared hints")
        })
        .await;

        assert!(record.stored, "first write stores the row");
        assert!(
            scoped.take_denials().is_empty(),
            "declared hints must be sufficient: no CAP110 denials"
        );
    }

    #[tokio::test]
    async fn record_summary_denied_without_kv_grant() {
        let (_kv, bag) = kv_bag();
        // Grant nothing: the kv access the node really performs must be denied,
        // proving the declared hint is load-bearing (not an over-claim).
        let scoped = Arc::new(ScopedResources::new("record_summary", bag, []));
        let view: Arc<dyn ResourceAccess> = scoped.clone();

        let err = context::with_resources(view, async {
            record_summary(sample_snapshot(1_782_993_900_000))
                .await
                .expect_err("write must fail when kv is not granted")
        })
        .await;
        assert!(
            err.to_string().contains("KV capability"),
            "expected a missing-kv error, got: {err}"
        );

        let denials = scoped.take_denials();
        assert!(
            denials.iter().any(|d| d.capability == "kv"),
            "expected a CAP110 kv denial, got: {denials:?}"
        );
    }

    #[tokio::test]
    async fn record_summary_is_idempotent_on_scheduled_time() {
        let (kv, bag) = kv_bag();
        let scoped = Arc::new(ScopedResources::new(
            "record_summary",
            bag,
            [EffectHint::KvRead, EffectHint::KvWrite],
        ));

        // Two deliveries of the SAME fire (identical scheduled_time_ms).
        let scheduled_time_ms = 1_782_993_900_000;
        let first = context::with_resources(scoped.clone(), async {
            record_summary(sample_snapshot(scheduled_time_ms))
                .await
                .expect("first delivery")
        })
        .await;
        let second = context::with_resources(scoped.clone(), async {
            record_summary(sample_snapshot(scheduled_time_ms))
                .await
                .expect("redelivery")
        })
        .await;

        assert!(first.stored, "first delivery writes the row");
        assert!(!second.stored, "redelivery is deduplicated (upsert no-op)");
        assert_eq!(first.key, second.key, "same fire => same key");

        // Exactly one row exists for this fire.
        let raw = kv
            .get(&summary_key(scheduled_time_ms))
            .await
            .expect("kv get")
            .expect("row present");
        let row: IssueSnapshot = serde_json::from_slice(&raw).expect("decode row");
        assert_eq!(row.scheduled_time_ms, scheduled_time_ms);

        // A DISTINCT fire writes a second, independent row.
        let other = context::with_resources(scoped.clone(), async {
            record_summary(sample_snapshot(scheduled_time_ms + 300_000))
                .await
                .expect("distinct fire")
        })
        .await;
        assert!(other.stored, "a distinct fire writes its own row");
        assert_ne!(first.key, other.key);
    }

    // ---- Compose: schedule fire -> connector poll -> enforced kv write ---

    // The env lock is deliberately held across awaits: it serializes the
    // process-global endpoint/auth env vars for the whole fire so a parallel
    // test cannot mutate them mid-run.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn scheduled_fire_polls_github_and_records_idempotent_summary() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
        let _auth = EnvGuard::remove(AUTH_ENV);

        let issues = server.mock(|when, then| {
            when.method(GET);
            then.status(200).json_body_obj(&serde_json::json!([
                { "number": 101, "title": "flaky test", "state": "open",
                  "html_url": "https://example.test/issues/101" },
                { "number": 102, "title": "docs typo", "state": "open",
                  "html_url": "https://example.test/issues/102" }
            ]));
        });

        let kv = Arc::new(MemoryKv::new());
        let bag = ResourceBag::default()
            .with_http_read(Arc::new(ReqwestHttpClient::default()))
            .with_http_write(Arc::new(ReqwestHttpClient::default()))
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_checkpoint_store(Arc::new(MemoryCheckpointStore::default()))
            .with_kv(kv.clone());

        let bundle = bundle();
        let runtime = HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir))
            .with_resource_bag(bag);

        let scheduled_time_ms = 1_782_993_900_000;
        let fire = || Invocation::new(TRIGGER_ALIAS, "capture", scheduled_event(scheduled_time_ms));

        // First fire writes the summary row.
        let first = runtime.execute(fire()).await.expect("first fire runs");
        let first = match first {
            HostExecutionResult::Value(value) => {
                serde_json::from_value::<SummaryRecord>(value).expect("decode record")
            }
            _ => panic!("expected a value result from the first fire"),
        };
        assert!(first.stored);
        assert_eq!(first.open_count, 2);
        assert_eq!(first.key, summary_key(scheduled_time_ms));

        // Redelivery of the same fire dedupes: no second row.
        let second = runtime.execute(fire()).await.expect("redelivery runs");
        let second = match second {
            HostExecutionResult::Value(value) => {
                serde_json::from_value::<SummaryRecord>(value).expect("decode record")
            }
            _ => panic!("expected a value result from the redelivery"),
        };
        assert!(!second.stored, "redelivery must not write a duplicate row");

        // The connector op really ran (poll hit the mock) and one row exists.
        issues.assert_hits(2);
        let stored = kv
            .get(&summary_key(scheduled_time_ms))
            .await
            .expect("kv get")
            .expect("summary row present");
        let row: IssueSnapshot = serde_json::from_slice(&stored).expect("decode row");
        assert_eq!(row.open_count, 2);
        assert_eq!(row.issue_numbers, vec![101, 102]);
    }
}
