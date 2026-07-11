//! End-to-end firing test for the dev scheduler core
//! (impl-docs/spec/schedule-trigger.md §7b).
//!
//! Builds a minimal test-local `trigger -> capture` flow (no examples/ crate),
//! computes the next fire from a cron with a MOCKED clock, then fires the
//! resulting `Invocation` through a real `HostRuntime` and asserts the trigger
//! received a correctly-populated `ScheduledEvent`. No wall-clock sleeps.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dag_core::ScheduledEvent;
use dag_core::prelude::*;
use dag_core::{DurabilityMode, FlowIR};
use host_inproc::schedule::{fire_invocation, next_fires, to_epoch_ms};
use host_inproc::{FlowEntrypoint, HostRuntime, Invocation};
use kernel_exec::{ExecutionResult, FlowExecutor, NodeRegistry};
use kernel_plan::validate;
use serde_json::Value as JsonValue;

/// Two-node passthrough flow `trigger -> capture`; the trigger's input payload
/// flows through unchanged and is captured as the result. Durability is forced
/// Off so no checkpoint store is required.
fn passthrough_flow(flow_id: &str) -> FlowIR {
    let mut builder = FlowBuilder::new(flow_id, Version::new(1, 0, 0), Profile::Dev);
    let trigger = builder
        .add_node(
            "trigger",
            &NodeSpec::inline(
                "tests::trigger",
                "Trigger",
                SchemaSpec::Opaque,
                SchemaSpec::Opaque,
                Effects::Pure,
                Determinism::Strict,
                None,
            ),
        )
        .expect("trigger added");
    let capture = builder
        .add_node(
            "capture",
            &NodeSpec::inline(
                "tests::capture",
                "Capture",
                SchemaSpec::Opaque,
                SchemaSpec::Opaque,
                Effects::Pure,
                Determinism::Strict,
                None,
            ),
        )
        .expect("capture added");
    builder.connect(&trigger, &capture);

    let mut flow = builder.build();
    flow.policies.durability.mode = DurabilityMode::Off;
    flow
}

fn passthrough_registry() -> NodeRegistry {
    let mut registry = NodeRegistry::new();
    registry
        .register_fn(
            "tests::trigger",
            |value: JsonValue| async move { Ok(value) },
        )
        .expect("trigger registered");
    registry
        .register_fn(
            "tests::capture",
            |value: JsonValue| async move { Ok(value) },
        )
        .expect("capture registered");
    registry
}

fn at(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("valid rfc3339")
        .with_timezone(&Utc)
}

fn schedule_entrypoint(cron: &str) -> FlowEntrypoint {
    FlowEntrypoint {
        trigger_alias: "trigger".to_string(),
        capture_alias: "capture".to_string(),
        route_path: None,
        method: None,
        deadline: None,
        route_aliases: Vec::new(),
        schedule: Some(cron.to_string()),
    }
}

async fn execute(runtime: &HostRuntime, invocation: Invocation) -> JsonValue {
    match runtime
        .execute(invocation)
        .await
        .expect("scheduled fire executes")
    {
        ExecutionResult::Value(value) => value,
        ExecutionResult::Stream(_) => panic!("expected value result, got stream"),
        ExecutionResult::Halt { .. } => panic!("expected value result, got halt"),
    }
}

#[tokio::test]
async fn firing_delivers_scheduled_event_to_trigger() {
    // Mocked clock: "now" is 12:02:30; "*/5 * * * *" next fires at 12:05:00.
    let entrypoints = vec![schedule_entrypoint("*/5 * * * *")];
    let now = at("2026-07-02T12:02:30Z");

    let fires = next_fires(now, &entrypoints);
    assert_eq!(fires.len(), 1);
    let (fire_at, entry) = fires[0];
    assert_eq!(fire_at, at("2026-07-02T12:05:00Z"));

    let scheduled_ms = to_epoch_ms(fire_at);
    let invocation = fire_invocation(entry, scheduled_ms).expect("invocation built");

    let ir = Arc::new(validate(&passthrough_flow("t2_schedule_fire")).expect("flow validates"));
    let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(passthrough_registry())), ir);

    let captured = execute(&runtime, invocation).await;

    // The trigger's input payload (the synthetic ScheduledEvent) flowed through
    // unchanged: the fire delivered exactly what the scheduler computed.
    let event: ScheduledEvent =
        serde_json::from_value(captured).expect("captured value is a ScheduledEvent");
    assert_eq!(event.cron, "*/5 * * * *");
    assert_eq!(event.scheduled_time_ms, scheduled_ms);
    // scheduled_time_ms is the SCHEDULED time (12:05:00), not the mocked "now".
    assert_eq!(
        event.scheduled_time_ms,
        to_epoch_ms(at("2026-07-02T12:05:00Z"))
    );
    assert_ne!(event.scheduled_time_ms, to_epoch_ms(now));
}

#[tokio::test]
async fn once_replays_are_stable_across_redeliveries() {
    // At-least-once: firing the same scheduled instant twice (a --once replay)
    // yields byte-identical ScheduledEvents — idempotency can key on it.
    let entry = schedule_entrypoint("0 * * * *");
    let scheduled_ms = to_epoch_ms(at("2026-07-02T13:00:00Z"));

    let ir = Arc::new(validate(&passthrough_flow("t2_schedule_replay")).expect("flow validates"));
    let runtime = HostRuntime::new(FlowExecutor::new(Arc::new(passthrough_registry())), ir);

    let first = execute(&runtime, fire_invocation(&entry, scheduled_ms).unwrap()).await;
    let second = execute(&runtime, fire_invocation(&entry, scheduled_ms).unwrap()).await;

    assert_eq!(first, second, "redeliveries of one fire are identical");
    let event: ScheduledEvent = serde_json::from_value(first).unwrap();
    assert_eq!(event.scheduled_time_ms, scheduled_ms);
}
