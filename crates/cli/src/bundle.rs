use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use flow_bundle::{
    compute_bundle_id, read_manifest_from_custom_section, sha256_prefixed, validate_manifest,
    Manifest,
};

#[derive(clap::Args, Debug)]
pub struct BundleArgs {
    /// Cargo package to build for wasm.
    #[arg(long, short = 'p')]
    pub package: String,
    /// Optional manifest.json path (exported).
    #[arg(long)]
    pub manifest: Option<PathBuf>,
    /// Output directory (default: flow.bundle).
    #[arg(long, default_value = "flow.bundle")]
    pub out_dir: PathBuf,
    /// Release build (runs wasm-opt).
    #[arg(long)]
    pub release: bool,
    /// Development build (skip wasm-opt).
    #[arg(long)]
    pub dev: bool,
}

pub fn run_bundle(args: BundleArgs) -> Result<()> {
    if args.release && args.dev {
        return Err(anyhow!("--release cannot be combined with --dev"));
    }

    run_cargo_build(&args.package, args.release)?;

    let profile = if args.release { "release" } else { "debug" };
    let wasm_path = resolve_wasm_path(&args.package, profile);
    let mut wasm_bytes =
        fs::read(&wasm_path).with_context(|| format!("failed to read {}", wasm_path.display()))?;

    if args.release {
        wasm_bytes = run_wasm_opt(&wasm_bytes)?;
    }

    let mut manifest = if let Some(path) = &args.manifest {
        let data = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        let manifest = serde_json::from_slice::<Manifest>(&data)
            .with_context(|| format!("{} is not valid manifest JSON", path.display()))?;
        validate_manifest(&manifest)
            .map_err(|err| anyhow!(err))
            .with_context(|| format!("{} failed manifest validation", path.display()))?;
        manifest
    } else {
        read_manifest_from_custom_section(&wasm_bytes)
            .context("missing embedded manifest custom section")?
    };

    let code_hash = sha256_prefixed(&wasm_bytes);
    manifest.code.hash = code_hash;
    manifest.code.size_bytes = wasm_bytes.len() as u64;
    manifest.bundle_id = compute_bundle_id(&manifest)?;

    write_bundle(&args.out_dir, &manifest, &wasm_bytes)?;
    println!("{}", args.out_dir.display());
    Ok(())
}

fn run_cargo_build(package: &str, release: bool) -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("-p")
        .arg(package)
        .arg("--target")
        .arg("wasm32-unknown-unknown");
    if release {
        cmd.arg("--release");
    }
    let status = cmd.status().context("failed to run cargo build")?;
    if !status.success() {
        return Err(anyhow!("cargo build failed"));
    }
    Ok(())
}

fn resolve_target_dir() -> PathBuf {
    std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target"))
}

fn wasm_output_name(package: &str) -> String {
    format!("{}.wasm", package.replace('-', "_"))
}

fn resolve_wasm_path(package: &str, profile: &str) -> PathBuf {
    let mut path = resolve_target_dir();
    path.push("wasm32-unknown-unknown");
    path.push(profile);
    path.push(wasm_output_name(package));
    path
}

fn run_wasm_opt(input: &[u8]) -> Result<Vec<u8>> {
    let dir = std::env::temp_dir();
    let input_path = dir.join("lattice.bundle.input.wasm");
    let output_path = dir.join("lattice.bundle.output.wasm");
    fs::write(&input_path, input).context("failed to write wasm-opt input")?;

    let status = Command::new("wasm-opt")
        .arg("-O")
        .arg("-o")
        .arg(&output_path)
        .arg(&input_path)
        .status()
        .context("failed to run wasm-opt")?;
    if !status.success() {
        return Err(anyhow!("wasm-opt failed"));
    }

    let output = fs::read(&output_path).context("failed to read wasm-opt output")?;
    let _ = fs::remove_file(&input_path);
    let _ = fs::remove_file(&output_path);
    Ok(output)
}

fn write_bundle(out_dir: &Path, manifest: &Manifest, wasm_bytes: &[u8]) -> Result<()> {
    if out_dir.exists() {
        fs::remove_dir_all(out_dir)
            .with_context(|| format!("failed to clear {}", out_dir.display()))?;
    }
    fs::create_dir_all(out_dir)
        .with_context(|| format!("failed to create {}", out_dir.display()))?;
    fs::write(out_dir.join("module.wasm"), wasm_bytes)
        .with_context(|| format!("failed to write {}/module.wasm", out_dir.display()))?;
    let json = serde_json::to_vec_pretty(manifest).context("failed to serialize manifest")?;
    fs::write(out_dir.join("manifest.json"), json)
        .with_context(|| format!("failed to write {}/manifest.json", out_dir.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wasm_output_name_normalizes_hyphens() {
        assert_eq!(
            wasm_output_name("example-s6-spill"),
            "example_s6_spill.wasm"
        );
    }

    #[test]
    fn resolve_target_dir_prefers_env() {
        unsafe {
            std::env::set_var("CARGO_TARGET_DIR", "/tmp/lattice-target");
        }
        let dir = resolve_target_dir();
        assert_eq!(dir, std::path::PathBuf::from("/tmp/lattice-target"));
        unsafe {
            std::env::remove_var("CARGO_TARGET_DIR");
        }
    }
}
