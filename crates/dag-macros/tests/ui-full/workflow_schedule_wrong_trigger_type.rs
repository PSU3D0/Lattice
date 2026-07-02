// A schedule entrypoint's trigger input type must be EXACTLY
// `dag_core::ScheduledEvent` (no `Into`-flexibility in v1); anything else is
// a rustc type error (impl-docs/spec/schedule-trigger.md §2).
#![allow(unused_imports)]

use dag_core::{NodeResult, ScheduledEvent};
use dag_macros::{def_node, flow, node};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NotAScheduledEvent {
    pub payload: String,
}

#[def_node(
    trigger,
    name = "Tick",
    summary = "Trigger with the wrong input type for a schedule entrypoint",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn tick(event: NotAScheduledEvent) -> NodeResult<NotAScheduledEvent> {
    Ok(event)
}

#[def_node(
    name = "Report",
    summary = "Consume the tick",
    effects = "Pure",
    determinism = "Strict"
)]
async fn report(event: NotAScheduledEvent) -> NodeResult<NotAScheduledEvent> {
    Ok(event)
}

flow! {
    name: schedule_wrong_trigger_type_flow,
    version: "0.1.0",
    profile: Web;
    let tick = node!(tick);
    let report = node!(report);
    connect!(tick -> report);
    entrypoint!({
        trigger: "tick",
        capture: "report",
        schedule: "*/5 * * * *",
    });
}

fn main() {}
