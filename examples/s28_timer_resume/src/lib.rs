//! S28 is the smallest durable timer example: trigger, `std.timer.wait`, and
//! terminal capture. Local CLI execution follows timer resumes in the foreground
//! by default; `--no-follow-resumes` preserves the process-crossing manual path.

use dag_core::NodeResult;
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use stdlib::timer::{TimerWaitInput, TimerWaitOutput};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TimerResumeResult {
    pub resumed: bool,
    pub payload: Value,
    pub scheduled_at_ms: i64,
}

#[def_node(
    trigger,
    name = "TimerTrigger",
    summary = "Accept a timer target and payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn timer_trigger(input: TimerWaitInput) -> NodeResult<TimerWaitInput> {
    Ok(input)
}

#[def_node(
    name = "TimerCapture",
    summary = "Confirm the halted timer resumed",
    effects = "Pure",
    determinism = "Strict"
)]
async fn timer_capture(output: TimerWaitOutput) -> NodeResult<TimerResumeResult> {
    Ok(TimerResumeResult {
        resumed: true,
        payload: output.payload,
        scheduled_at_ms: output.scheduled_at_ms,
    })
}

dag_macros::flow! {
    name: s28_timer_resume_flow,
    version: "1.0.0",
    profile: Dev,
    summary: "Canonical local std.timer.wait suspend and process-crossing resume";
    let trigger = node!(timer_trigger);
    let wait = node!(stdlib::timer::timer_wait);
    let capture = node!(timer_capture);
    connect!(trigger -> wait);
    connect!(wait -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/timer-resume"],
        method: "POST",
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn flow_contains_halting_timer() {
        let flow = super::flow();
        let wait = flow.nodes.iter().find(|node| node.alias == "wait").unwrap();
        assert_eq!(wait.identifier, "std.timer.wait");
        assert!(wait.durability.halts);
    }
}
