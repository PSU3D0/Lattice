//! Live smoke — env-gated, NEVER part of default CI.
//!
//! The Slack first slice ships only the Effectful `post_message` op, and live
//! smoke from tests must stay read-only (connector-verification-guide §e):
//! effectful live verification belongs in a dedicated example a human runs
//! deliberately. This gated entry point therefore only proves the two gates
//! compose (ignore + LATTICE_LIVE_SMOKE); it posts nothing.
//!
//! Deliberate effectful verification path: run the s20 example against real
//! bindings (`flows run local --example s20_form_signup_notify
//! --payload <submission.json> --bindings-lock <real-lock.json>`).

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1; Slack's only op is Effectful, so live posts are reserved for a deliberate example run"]
async fn live_smoke_gate_only() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );
    println!(
        "slack live smoke: no read-only op in this family slice; \
         effectful post verification is a deliberate example run"
    );
}
