//! Live smoke — env-gated, NEVER part of default CI.
//!
//! The Telegram first slice ships only the Effectful `send_message` op, and
//! live smoke from tests must stay read-only (connector-verification-guide §e):
//! effectful live verification belongs in a dedicated example binary a human
//! runs deliberately. This gated entry point therefore only proves the two
//! gates compose (ignore + LATTICE_LIVE_SMOKE); it sends nothing.
//!
//! Deliberate effectful verification path: run the s17 example against real
//! bindings (`flows run local --example s17_telegram_broadcast --bindings-lock
//! <real-lock.json> --payload <request.json>`).

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1; Telegram's only op is Effectful, so live sends are reserved for a deliberate example run"]
async fn live_smoke_gate_only() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );
    println!(
        "telegram live smoke: no read-only op in this family slice; \
         effectful send verification is a deliberate example-binary run"
    );
}
