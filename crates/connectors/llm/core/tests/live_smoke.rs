//! Live smoke — env-gated, NEVER part of default CI.
//!
//! The LLM family's only op is Effectful (a completion consumes billed
//! provider quota), and live smoke from tests must stay read-only
//! (connector-verification-guide §e): effectful live verification belongs in
//! a deliberate example run. This gated entry point therefore only proves the
//! two gates compose (ignore + LATTICE_LIVE_SMOKE); it completes nothing.
//!
//! Deliberate effectful verification path: run the local-flow example against
//! real bindings (`flows run local --example connector_llm_local_flow
//! --bindings-lock <real-lock.json> --payload '{"model":"...","prompt":"..."}'`).

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1; the LLM family's only op is Effectful (billed), so live completions are reserved for a deliberate example run"]
async fn live_smoke_gate_only() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );
    println!(
        "llm live smoke: no read-only op in this family slice; \
         effectful completion verification is a deliberate example-binary run"
    );
}
