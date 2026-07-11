//! Dev scheduler core for cron-triggered flows
//! (impl-docs/spec/schedule-trigger.md §7b).
//!
//! This module holds the *pure* pieces the CLI dev scheduler is built from:
//! computing next fire times from cron expressions and constructing the
//! synthetic `ScheduledEvent` invocation for a fire. The tick loop, wall-clock
//! sleeps, and Ctrl-C handling live in the CLI (`crates/cli/src/run_schedule.rs`)
//! so this core stays trivially unit-testable with a mocked clock and no
//! real time passing.
//!
//! Fire times are computed with `saffron` — the same parser Cloudflare runs for
//! Cron Triggers and the same parser used at validation time (kernel-plan /
//! dag-macros). Dev fires therefore match the times CF would fire by
//! construction. All computation is in UTC (CF crons are UTC-only).

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use dag_core::ScheduledEvent;

use crate::{FlowEntrypoint, Invocation};

/// Injectable clock so the CLI boundary reads wall time exactly once and tests
/// can step a fixed instant through fires without sleeping.
pub trait ScheduleClock: Send + Sync {
    /// Current UTC instant.
    fn now(&self) -> DateTime<Utc>;
}

/// Wall-clock implementation used by the CLI. Reads `SystemTime` and converts
/// to a UTC instant; never touches `chrono`'s `clock` feature.
pub struct SystemClock;

impl ScheduleClock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
    }
}

/// Epoch milliseconds (UTC) for a UTC instant, clamped at 0 (the epoch).
///
/// `ScheduledEvent::scheduled_time_ms` is a `u64`; negative pre-epoch instants
/// cannot occur for cron fire times but are clamped defensively.
pub fn to_epoch_ms(dt: DateTime<Utc>) -> u64 {
    dt.timestamp_millis().max(0) as u64
}

/// Build the synthetic payload delivered to a schedule trigger for one fire.
///
/// `scheduled_time_ms` is the *scheduled* time (from `--at` or the CLI-boundary
/// clock), not the observed wall clock, so it is stable across `--once`
/// replays — matching the at-least-once semantics in the spec (§5).
pub fn scheduled_event(scheduled_time_ms: u64, cron: &str) -> ScheduledEvent {
    ScheduledEvent {
        scheduled_time_ms,
        cron: cron.to_string(),
    }
}

/// Every schedule-shaped entrypoint (those carrying a cron), in declaration
/// order.
pub fn schedule_entrypoints(entrypoints: &[FlowEntrypoint]) -> Vec<&FlowEntrypoint> {
    entrypoints
        .iter()
        .filter(|entry| entry.schedule.is_some())
        .collect()
}

/// Compute the soonest fire time strictly after `after` across all schedule
/// entrypoints, returning every `(time, entrypoint)` that fires at that soonest
/// instant.
///
/// Returning all entrypoints tied at the soonest instant is what lets the tick
/// loop fire multiple cadences (or multiple crons wired into one DAG) that
/// coincide on a given minute — "fire for whichever matches" falls out because
/// every schedule entrypoint is considered independently.
///
/// Entrypoints without a schedule are ignored. Entrypoints whose cron fails to
/// parse or never fires are skipped defensively (validation — TRIG001 — already
/// rejects those before a flow can be run, so this is belt-and-suspenders).
pub fn next_fires(
    after: DateTime<Utc>,
    entrypoints: &[FlowEntrypoint],
) -> Vec<(DateTime<Utc>, &FlowEntrypoint)> {
    let mut soonest: Option<DateTime<Utc>> = None;
    let mut fires: Vec<(DateTime<Utc>, &FlowEntrypoint)> = Vec::new();

    for entry in entrypoints {
        let Some(cron_str) = entry.schedule.as_deref() else {
            continue;
        };
        let Ok(cron) = cron_str.parse::<saffron::Cron>() else {
            continue;
        };
        let Some(next) = cron.next_after(after) else {
            continue;
        };

        match soonest {
            None => {
                soonest = Some(next);
                fires.push((next, entry));
            }
            Some(best) => {
                if next < best {
                    soonest = Some(next);
                    fires.clear();
                    fires.push((next, entry));
                } else if next == best {
                    fires.push((next, entry));
                }
                // next > best: not the soonest fire, ignore.
            }
        }
    }

    fires
}

/// Construct the invocation for firing one schedule entrypoint at
/// `scheduled_time_ms`. The trigger receives a `ScheduledEvent` carrying the
/// entrypoint's own cron string (byte-identical to the authored expression),
/// and the entrypoint's execution `deadline` is preserved.
///
/// Returns an error only if `ScheduledEvent` fails to serialise, which is
/// infallible in practice.
pub fn fire_invocation(
    entry: &FlowEntrypoint,
    scheduled_time_ms: u64,
) -> Result<Invocation, serde_json::Error> {
    let cron = entry.schedule.as_deref().unwrap_or_default();
    let payload = serde_json::to_value(scheduled_event(scheduled_time_ms, cron))?;
    Ok(Invocation::new(
        entry.trigger_alias.clone(),
        entry.capture_alias.clone(),
        payload,
    )
    .with_deadline(entry.deadline))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn schedule_entry(trigger: &str, capture: &str, cron: &str) -> FlowEntrypoint {
        FlowEntrypoint {
            trigger_alias: trigger.to_string(),
            capture_alias: capture.to_string(),
            route_path: None,
            method: None,
            deadline: None,
            route_aliases: Vec::new(),
            schedule: Some(cron.to_string()),
        }
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }

    #[test]
    fn next_fire_for_five_minute_cron_from_mocked_now() {
        // Mocked "now" at 12:02:30 UTC; "*/5 * * * *" next fires at 12:05:00.
        let entrypoints = vec![schedule_entry("tick", "report", "*/5 * * * *")];
        let now = at("2026-07-02T12:02:30Z");

        let fires = next_fires(now, &entrypoints);

        assert_eq!(fires.len(), 1, "exactly one entrypoint should fire");
        let (when, entry) = fires[0];
        assert_eq!(when, at("2026-07-02T12:05:00Z"));
        assert_eq!(entry.trigger_alias, "tick");
    }

    #[test]
    fn next_fires_is_strictly_after_the_given_instant() {
        // Exactly on a fire boundary: the *next* fire must be the following one,
        // never the current instant (next_after is exclusive).
        let entrypoints = vec![schedule_entry("tick", "report", "*/5 * * * *")];
        let on_boundary = at("2026-07-02T12:05:00Z");

        let fires = next_fires(on_boundary, &entrypoints);

        assert_eq!(fires[0].0, at("2026-07-02T12:10:00Z"));
    }

    #[test]
    fn coinciding_cadences_all_fire_together() {
        // A 5-minute and a 15-minute cadence coincide at 12:15:00; both fire.
        let entrypoints = vec![
            schedule_entry("fast", "report", "*/5 * * * *"),
            schedule_entry("slow", "report", "*/15 * * * *"),
        ];
        // Start just after 12:10 so the next 5-min fire (12:15) equals the next
        // 15-min fire (12:15).
        let now = at("2026-07-02T12:10:30Z");

        let fires = next_fires(now, &entrypoints);

        assert_eq!(
            fires.len(),
            2,
            "both cadences fire at the coinciding minute"
        );
        assert!(fires.iter().all(|(t, _)| *t == at("2026-07-02T12:15:00Z")));
        let mut aliases: Vec<&str> = fires
            .iter()
            .map(|(_, e)| e.trigger_alias.as_str())
            .collect();
        aliases.sort_unstable();
        assert_eq!(aliases, vec!["fast", "slow"]);
    }

    #[test]
    fn only_the_soonest_cadence_fires_when_they_differ() {
        let entrypoints = vec![
            schedule_entry("fast", "report", "*/5 * * * *"),
            schedule_entry("hourly", "report", "0 * * * *"),
        ];
        let now = at("2026-07-02T12:02:30Z");

        let fires = next_fires(now, &entrypoints);

        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].1.trigger_alias, "fast");
        assert_eq!(fires[0].0, at("2026-07-02T12:05:00Z"));
    }

    #[test]
    fn http_entrypoints_are_ignored() {
        let http = FlowEntrypoint {
            trigger_alias: "http".to_string(),
            capture_alias: "report".to_string(),
            route_path: Some("/".to_string()),
            method: Some("POST".to_string()),
            deadline: None,
            route_aliases: Vec::new(),
            schedule: None,
        };
        assert!(schedule_entrypoints(&[http.clone_for_test()]).is_empty());
        assert!(next_fires(at("2026-07-02T12:00:00Z"), &[http]).is_empty());
    }

    #[test]
    fn fire_invocation_populates_scheduled_event() {
        let entry = FlowEntrypoint {
            deadline: Some(Duration::from_millis(30_000)),
            ..schedule_entry("tick", "report", "*/5 * * * *")
        };
        let scheduled_ms = to_epoch_ms(at("2026-07-02T12:05:00Z"));

        let invocation = fire_invocation(&entry, scheduled_ms).expect("invocation built");
        let parts = invocation.into_parts();

        assert_eq!(parts.trigger_alias, "tick");
        assert_eq!(parts.capture_alias, "report");
        assert_eq!(parts.deadline, Some(Duration::from_millis(30_000)));

        let event: ScheduledEvent =
            serde_json::from_value(parts.payload).expect("payload is a ScheduledEvent");
        assert_eq!(event.scheduled_time_ms, scheduled_ms);
        assert_eq!(event.cron, "*/5 * * * *");
    }

    #[test]
    fn to_epoch_ms_matches_scheduled_time() {
        // 2026-07-02T12:05:00Z == 1_782_993_900_000 ms since epoch.
        assert_eq!(to_epoch_ms(at("2026-07-02T12:05:00Z")), 1_782_993_900_000);
    }

    // Small helper so the HTTP-ignored test can reuse one struct twice without
    // deriving Clone on the public FlowEntrypoint type.
    impl FlowEntrypoint {
        fn clone_for_test(&self) -> FlowEntrypoint {
            FlowEntrypoint {
                trigger_alias: self.trigger_alias.clone(),
                capture_alias: self.capture_alias.clone(),
                route_path: self.route_path.clone(),
                method: self.method.clone(),
                deadline: self.deadline,
                route_aliases: self.route_aliases.clone(),
                schedule: self.schedule.clone(),
            }
        }
    }
}
