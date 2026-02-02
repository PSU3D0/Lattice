use chrono::{DateTime, Utc};
use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::time::Duration;

use capabilities::context;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerWaitInput {
    #[serde(default, with = "humantime_serde")]
    pub duration: Option<Duration>,
    pub until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub payload: JsonValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimerWaitOutput {
    pub payload: JsonValue,
    pub scheduled_at_ms: i64,
}

#[def_node(
    name = "TimerWait",
    summary = "Pause execution until the requested time",
    identifier = "std.timer.wait",
    effects = "Pure",
    determinism = "Nondeterministic",
    halts = true
)]
async fn timer_wait(input: TimerWaitInput) -> NodeResult<TimerWaitOutput> {
    let (duration, until) = match (input.duration, input.until) {
        (Some(_), Some(_)) => {
            return Err(NodeError::new(
                "std.timer.wait requires exactly one of duration or until",
            ));
        }
        (None, None) => {
            return Err(NodeError::new(
                "std.timer.wait requires duration or until",
            ));
        }
        (duration, until) => (duration, until),
    };

    let now = Utc::now();
    let target = if let Some(duration) = duration {
        let chrono_duration = chrono::Duration::from_std(duration).map_err(|err| {
            NodeError::new(format!("std.timer.wait invalid duration: {err}"))
        })?;
        now + chrono_duration
    } else if let Some(until) = until {
        until
    } else {
        now
    };

    let schedule_result = context::with_current_async(|resources| async move {
        let scheduler = resources.resume_scheduler().ok_or_else(|| {
            NodeError::new("std.timer.wait requires ResumeScheduler")
        })?;
        let handle = context::current_checkpoint_handle()
            .ok_or_else(|| NodeError::new("std.timer.wait missing checkpoint handle"))?;
        if let Some(duration) = duration {
            scheduler
                .schedule_after(handle, duration)
                .await
                .map_err(|err| NodeError::new(format!("std.timer.wait schedule failed: {err}")))?;
        } else {
            let at_ms = target.timestamp_millis();
            if at_ms < 0 {
                return Err(NodeError::new(
                    "std.timer.wait target time must be after unix epoch",
                ));
            }
            scheduler
                .schedule_at(handle, at_ms as u64)
                .await
                .map_err(|err| NodeError::new(format!("std.timer.wait schedule failed: {err}")))?;
        }
        Ok(())
    })
    .await;

    if schedule_result.is_none() {
        return Err(NodeError::new("std.timer.wait missing ResourceAccess context"));
    }
    schedule_result.unwrap()?;

    Ok(TimerWaitOutput {
        payload: input.payload,
        scheduled_at_ms: target.timestamp_millis(),
    })
}
