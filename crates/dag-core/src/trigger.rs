//! Trigger event payload types (impl-docs/spec/schedule-trigger.md §2).
//!
//! `ScheduledEvent` lives in dag-core because it is referenced by
//! macro-generated typed entrypoints, kernel-plan, both hosts, and examples;
//! dag-core is the shared, wasm32-clean vocabulary crate already on every one
//! of those dependency paths. stdlib holds node *implementations*, and hosts
//! must not depend on it.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Payload delivered to the trigger node of a schedule (cron) entrypoint.
///
/// Delivery is at-least-once: a single cron fire may be observed more than
/// once (platform retries, dev-loop `--once` replays). Effectful downstream
/// nodes should key idempotency on `scheduled_time_ms`, e.g.
/// `key = "<flow>:<trigger>:{scheduled_time_ms}"`, so redeliveries of one
/// fire dedupe while distinct fires stay distinct.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ScheduledEvent {
    /// Scheduled fire time, epoch milliseconds, UTC. This is the *scheduled*
    /// time, not the observed wall clock, so it is stable across
    /// redeliveries of the same fire.
    pub scheduled_time_ms: u64,
    /// The cron expression that fired, byte-identical to the authored string.
    pub cron: String,
}
