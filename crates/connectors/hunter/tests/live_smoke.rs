//! Live smoke — env-gated, NEVER part of default CI.
//!
//! `verify_email` is a ReadOnly op, so a real live smoke against Hunter.io is
//! permissible (connector-verification-guide §e allows read-only live checks).
//! It stays double-gated: it runs only with `#[ignore]` lifted AND
//! `LATTICE_LIVE_SMOKE=1`, and it additionally requires a real endpoint + API
//! key wired through a bindings lock. Without a bound connection this entry
//! point only proves the gates compose; it calls nothing.
//!
//! Deliberate read verification path: run the s24 example against real bindings
//! (`flows run local --example s24_lead_intake_verify --bindings-lock
//! <real-lock.json> --payload <lead.json>`).

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1 and a bound Hunter connection; the s24 example is the deliberate read-verification path"]
async fn live_smoke_gate_only() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );
    println!(
        "hunter live smoke: verify_email is ReadOnly; a real read is exercised \
         through the s24 example against a real bindings lock"
    );
}
