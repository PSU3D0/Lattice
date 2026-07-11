//! Emit the cron proof flow's FlowRequirements manifest (native helper for
//! `npm run render-cron`).
//!
//! The manifest is DERIVED from the same `flow!`-generated IR the worker
//! serves — never hand-written — so `flows deploy render --requirements`
//! renders exactly what the runtime bundle demands (the T3 half of the
//! "generated config is real" gate).

fn main() {
    let out = std::env::args()
        .nth(1)
        .expect("usage: emit-cron-requirements <out.json>");
    let requirements = s1_echo_render_proof::cron_requirements();
    let bytes = serde_json::to_vec_pretty(&requirements).expect("serialize requirements");
    if let Some(parent) = std::path::Path::new(&out).parent() {
        std::fs::create_dir_all(parent).expect("create output directory");
    }
    std::fs::write(&out, bytes).expect("write requirements manifest");
    println!("{out}");
}
