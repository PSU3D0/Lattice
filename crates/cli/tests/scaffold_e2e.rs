//! End-to-end smoke test for `flows new`.
//!
//! Generates a real crate named `ztest_scaffold_smoke` under `examples/`,
//! verifies the README's verification commands actually work against it
//! (`cargo check -p`, then `flows graph check` on its emitted IR), then REMOVES
//! the crate and restores the root Cargo.toml byte-for-byte. The test asserts
//! zero residue at the end.
//!
//! This test mutates the shared workspace Cargo.toml, so it is `#[ignore]` by
//! default to avoid colliding with concurrent edits. Run it explicitly:
//!   cargo test -p flows-cli --test scaffold_e2e -- --ignored
//!
//! It is also gated behind LATTICE_RUN_SCAFFOLD_E2E=1 as a second safety latch.

use std::path::{Path, PathBuf};
use std::process::Command;

use assert_cmd::cargo::cargo_bin;

const SMOKE_NAME: &str = "ztest_scaffold_smoke";

fn workspace_root() -> PathBuf {
    // crates/cli -> workspace root.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

struct CargoTomlGuard {
    path: PathBuf,
    original: Vec<u8>,
}

impl CargoTomlGuard {
    fn capture(root: &Path) -> Self {
        let path = root.join("Cargo.toml");
        let original = std::fs::read(&path).expect("read root Cargo.toml");
        Self { path, original }
    }

    fn restore(&self) {
        std::fs::write(&self.path, &self.original).expect("restore root Cargo.toml");
    }
}

impl Drop for CargoTomlGuard {
    fn drop(&mut self) {
        // Best-effort restore even on panic.
        let _ = std::fs::write(&self.path, &self.original);
    }
}

#[test]
#[ignore = "mutates shared workspace Cargo.toml; run with --ignored and LATTICE_RUN_SCAFFOLD_E2E=1"]
fn scaffold_generates_checks_and_cleans_up() {
    if std::env::var_os("LATTICE_RUN_SCAFFOLD_E2E").is_none() {
        eprintln!("skipping: set LATTICE_RUN_SCAFFOLD_E2E=1 to run the scaffold e2e test");
        return;
    }

    let root = workspace_root();
    let crate_dir = root.join("examples").join(SMOKE_NAME);
    let flows_bin = cargo_bin("flows");

    // Guard restores Cargo.toml on drop no matter what.
    let guard = CargoTomlGuard::capture(&root);
    // Pre-clean any stale residue from a previous aborted run.
    let _ = std::fs::remove_dir_all(&crate_dir);

    let result = std::panic::catch_unwind(|| {
        // 1. Generate.
        let generate = Command::new(&flows_bin)
            .current_dir(&root)
            .args(["new", "--name", SMOKE_NAME, "--profile", "web", "--tier", "a"])
            .output()
            .expect("run flows new");
        assert!(
            generate.status.success(),
            "flows new failed:\n{}",
            String::from_utf8_lossy(&generate.stderr)
        );
        assert!(crate_dir.join("Cargo.toml").is_file(), "Cargo.toml not generated");
        assert!(crate_dir.join("src/lib.rs").is_file(), "lib.rs not generated");
        assert!(
            crate_dir.join("payloads/sample.json").is_file(),
            "sample payload not generated"
        );

        // Confirm the workspace edit landed.
        let cargo = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
        assert!(
            cargo.contains(&format!("examples/{SMOKE_NAME}")),
            "workspace member not appended"
        );

        // 2. cargo check -p (README step 1).
        let pkg = format!("example-{}", SMOKE_NAME.replace('_', "-"));
        let check = Command::new(env!("CARGO"))
            .current_dir(&root)
            .args(["check", "-p", &pkg])
            .output()
            .expect("run cargo check");
        assert!(
            check.status.success(),
            "cargo check -p {pkg} failed:\n{}",
            String::from_utf8_lossy(&check.stderr)
        );

        // 3. Emit IR via the dump_ir bin and feed `flows graph check` (README step 2).
        let dump = Command::new(env!("CARGO"))
            .current_dir(&root)
            .args(["run", "-q", "-p", &pkg, "--bin", "dump_ir"])
            .output()
            .expect("run dump_ir");
        assert!(
            dump.status.success(),
            "dump_ir failed:\n{}",
            String::from_utf8_lossy(&dump.stderr)
        );
        let ir_path = std::env::temp_dir().join(format!("{SMOKE_NAME}.ir.json"));
        std::fs::write(&ir_path, &dump.stdout).unwrap();

        let graph = Command::new(&flows_bin)
            .current_dir(&root)
            .args(["graph", "check", "--input"])
            .arg(&ir_path)
            .output()
            .expect("run flows graph check");
        assert!(
            graph.status.success(),
            "flows graph check rejected generated IR:\n{}",
            String::from_utf8_lossy(&graph.stderr)
        );
        assert!(
            String::from_utf8_lossy(&graph.stdout).contains("graph is valid"),
            "unexpected graph check output: {}",
            String::from_utf8_lossy(&graph.stdout)
        );

        // 4. entrypoints check (README step 3).
        let ep = Command::new(&flows_bin)
            .current_dir(&root)
            .args(["entrypoints", "check", "--flow"])
            .arg(&ir_path)
            .args(["--trigger-alias", "trigger", "--capture-alias", "responder"])
            .output()
            .expect("run flows entrypoints check");
        assert!(
            ep.status.success(),
            "flows entrypoints check failed:\n{}",
            String::from_utf8_lossy(&ep.stderr)
        );

        let _ = std::fs::remove_file(&ir_path);
    });

    // Cleanup: remove generated crate, restore Cargo.toml exactly.
    std::fs::remove_dir_all(&crate_dir).expect("remove generated crate dir");
    guard.restore();

    // Prove zero residue.
    assert!(
        !crate_dir.exists(),
        "residual generated crate dir remains: {}",
        crate_dir.display()
    );
    let restored = std::fs::read(root.join("Cargo.toml")).unwrap();
    assert_eq!(
        restored, guard.original,
        "root Cargo.toml not restored byte-for-byte"
    );

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
