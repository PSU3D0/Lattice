//! Live smoke — env-gated, NEVER part of default CI.
//!
//! connector.http is a generic transport with no fixed live endpoint; the only
//! honest read-only live target is deployment-specific. This gated entry point
//! therefore only proves the gate composes (ignore + LATTICE_LIVE_SMOKE);
//! deliberate end-to-end verification is the local-flow example and the H4
//! example flow run against a real bindings.lock.

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1; connector.http has no fixed live endpoint, so live verification is a deliberate example run"]
async fn live_smoke_gate_only() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );
    println!(
        "connector.http live smoke: no fixed live endpoint; \
         drive the local-flow example or the H4 example flow against a real lock"
    );
}
