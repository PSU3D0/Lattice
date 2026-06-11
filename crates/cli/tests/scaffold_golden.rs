//! Golden-file test for `flows new` generated output.
//!
//! Renders the tier-a Web template with a fixed name placed under `examples/`
//! and compares each generated file byte-for-byte against checked-in fixtures.
//! Output is deterministic (no timestamps), so a diff here means the templates
//! changed and the fixtures must be regenerated intentionally.
//!
//! Regenerate fixtures: `LATTICE_BLESS_GOLDEN=1 cargo test -p flows-cli --test scaffold_golden`

use std::path::{Path, PathBuf};

// Pull the scaffold module straight from the binary crate source so we can test
// its pure render path without spawning a process. The binary-only entrypoints
// (run_new, print_next_steps, ...) are unused in this test crate.
#[allow(dead_code)]
#[path = "../src/scaffold.rs"]
mod scaffold;

const GOLDEN_NAME: &str = "demo_flow";
// examples/<name> -> back up two levels then into crates.
const REL_PREFIX: &str = "../../crates";

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scaffold_golden")
}

#[test]
fn generated_crate_matches_golden() {
    let names = scaffold::parse_name(GOLDEN_NAME).expect("valid name");
    // Use a relative virtual crate dir so golden paths are stable.
    let crate_dir = Path::new("examples").join(GOLDEN_NAME);
    let generated = scaffold::render_crate(&crate_dir, &names, REL_PREFIX);

    let bless = std::env::var_os("LATTICE_BLESS_GOLDEN").is_some();
    let root = fixture_root();

    for (path, contents) in &generated.files {
        // Fixture path mirrors the in-crate relative path, with a `.golden` suffix.
        let rel = path
            .strip_prefix(&crate_dir)
            .expect("generated path under crate dir");
        let golden_path = root.join(format!("{}.golden", rel.display()));

        if bless {
            std::fs::create_dir_all(golden_path.parent().unwrap()).unwrap();
            std::fs::write(&golden_path, contents).unwrap();
            continue;
        }

        let expected = std::fs::read_to_string(&golden_path).unwrap_or_else(|err| {
            panic!(
                "missing golden fixture {} ({err}); run with LATTICE_BLESS_GOLDEN=1 to create it",
                golden_path.display()
            )
        });
        assert_eq!(
            contents,
            &expected,
            "generated {} drifted from golden {}",
            rel.display(),
            golden_path.display()
        );
    }
}
