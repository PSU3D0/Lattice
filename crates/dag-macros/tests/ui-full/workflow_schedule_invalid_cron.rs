// TRIG001: an invalid Cloudflare-dialect cron in `schedule:` fails at macro
// expansion time (impl-docs/spec/schedule-trigger.md §4).
#![allow(unused_imports)]

use dag_core::{NodeResult, ScheduledEvent};
use dag_macros::{def_node, flow, node};

#[def_node(
    trigger,
    name = "Tick",
    summary = "Cron tick trigger",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn tick(event: ScheduledEvent) -> NodeResult<ScheduledEvent> {
    Ok(event)
}

#[def_node(
    name = "Report",
    summary = "Consume the tick",
    effects = "Pure",
    determinism = "Strict"
)]
async fn report(event: ScheduledEvent) -> NodeResult<ScheduledEvent> {
    Ok(event)
}

flow! {
    name: bad_cron_flow,
    version: "0.1.0",
    profile: Web;
    let tick = node!(tick);
    let report = node!(report);
    connect!(tick -> report);
    entrypoint!({
        trigger: "tick",
        capture: "report",
        schedule: "61 * * * *",
    });
}

fn main() {}
