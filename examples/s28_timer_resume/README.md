# S28 timer resume

This canonical flow accepts a `stdlib::timer::TimerWaitInput`, halts at
`std.timer.wait`, and emits `{ "resumed": true, ... }` after resume.

For built-in local examples, `flows run local` now follows timer resumes in the
foreground by default: it persists the checkpoint, waits for and atomically
claims the process-local schedule, and calls `HostRuntime::resume` until the flow
finishes or reaches a non-timer halt. S28 proves this native CLI path with an
immediate/past target; it is not a Workers-parity claim.

Use `flows run local --no-follow-resumes` for the manual, process-crossing path.
The filesystem checkpoint's `resume_after_ms` is the only durable due source;
inspect it with `flows resume list --due` and consume it with
`flows resume run`. Future timed checkpoints require explicit `--force`.
