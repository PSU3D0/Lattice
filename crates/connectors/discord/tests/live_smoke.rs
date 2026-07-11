//! Live smoke — env-gated, NEVER part of default CI.
//!
//! The Discord first slice ships only the Effectful `send_message` op, and live
//! smoke from tests must stay read-only (connector-verification-guide §e):
//! effectful live verification belongs in a dedicated example binary a human
//! runs deliberately. This gated entry point therefore only proves the two gates
//! compose (ignore + LATTICE_LIVE_SMOKE); it sends nothing.
//!
//! Deliberate effectful verification path: run the s24 example against real
//! bindings (`flows run local --example s24_lead_intake_verify --bindings-lock
//! <real-lock.json> --payload <lead.json>`).

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1; Discord's only op is Effectful, so live posts are reserved for a deliberate example run"]
async fn live_smoke_gate_only() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );
    println!(
        "discord live smoke: no read-only op in this family slice; \
         effectful webhook-post verification is a deliberate example-binary run"
    );
}
