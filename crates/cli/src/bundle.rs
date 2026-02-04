use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use flow_bundle::{
    compute_bundle_id, read_manifest_from_custom_section, sha256_prefixed, validate_manifest,
    Manifest,
};

#[derive(clap::Args, Debug)]
pub struct BundleArgs {
    /// Cargo package to build.
    #[arg(long, short = 'p')]
    pub package: String,
    /// Optional manifest.json path (exported).
    #[arg(long)]
    pub manifest: Option<PathBuf>,
    /// Output directory (default: flow.bundle).
    #[arg(long, default_value = "flow.bundle")]
    pub out_dir: PathBuf,
    /// Release build (default; runs wasm-opt).
    #[arg(long)]
    pub release: bool,
    /// Development build (debug profile; skips wasm-opt).
    #[arg(long)]
    pub dev: bool,
    /// Build wasm32-unknown-unknown target.
    #[arg(long)]
    pub wasm: bool,
    /// Build native host target.
    #[arg(long)]
    pub native: bool,
    /// Explicit target triple (repeatable).
    #[arg(long, value_name = "triple")]
    pub target: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BuildProfile {
    Release,
    Debug,
}

impl BuildProfile {
    fn as_str(self) -> &'static str {
        match self {
            BuildProfile::Release => "release",
            BuildProfile::Debug => "debug",
        }
    }

    fn is_release(self) -> bool {
        matches!(self, BuildProfile::Release)
    }
}

pub fn run_bundle(args: BundleArgs) -> Result<()> {
    if args.release && args.dev {
        return Err(anyhow!("--release cannot be combined with --dev"));
    }

    let targets = resolve_targets(&args, None)?;
    let has_wasm = targets.iter().any(|target| is_wasm_target(target));
    if !has_wasm && args.manifest.is_none() {
        return Err(anyhow!(
            "--manifest is required when building without a wasm target (manifest extraction needs wasm)"
        ));
    }

    let mut manifest_from_flag = if let Some(path) = &args.manifest {
        let data = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        let manifest = serde_json::from_slice::<Manifest>(&data)
            .with_context(|| format!("{} is not valid manifest JSON", path.display()))?;
        validate_manifest(&manifest)
            .map_err(|err| anyhow!(err))
            .with_context(|| format!("{} failed manifest validation", path.display()))?;
        Some(manifest)
    } else {
        None
    };

    let profile = resolve_build_profile(&args);
    let mut outputs = Vec::with_capacity(targets.len());
    for target in &targets {
        run_cargo_build(
            &args.package,
            target,
            profile.is_release(),
            is_wasm_target(target),
        )?;
        let artifact_path = resolve_artifact_path(&args.package, target, profile.as_str())?;
        let mut bytes = fs::read(&artifact_path)
            .with_context(|| format!("failed to read {}", artifact_path.display()))?;
        if should_run_wasm_opt(profile, target) {
            bytes = run_wasm_opt(&bytes)?;
        }
        let file_name = output_file_name(target, targets.len() > 1);
        outputs.push(BuiltArtifact {
            target: target.clone(),
            file_name,
            bytes,
        });
    }

    let mut manifest = if let Some(manifest) = manifest_from_flag.take() {
        manifest
    } else {
        let wasm_output = outputs
            .iter()
            .find(|output| is_wasm_target(&output.target))
            .context("missing wasm target for embedded manifest")?;
        read_manifest_from_custom_section(&wasm_output.bytes)
            .context("missing embedded manifest custom section")?
    };

    let primary_index = select_primary_artifact_index(&outputs);
    let primary = &outputs[primary_index];
    let code_hash = sha256_prefixed(&primary.bytes);
    manifest.code.target = primary.target.clone();
    manifest.code.file = primary.file_name.clone();
    manifest.code.hash = code_hash;
    manifest.code.size_bytes = primary.bytes.len() as u64;
    manifest.artifacts = outputs
        .iter()
        .enumerate()
        .filter_map(|(index, output)| {
            if index == primary_index {
                None
            } else {
                Some(flow_bundle::ArtifactDescriptor {
                    target: output.target.clone(),
                    file: output.file_name.clone(),
                    hash: sha256_prefixed(&output.bytes),
                })
            }
        })
        .collect();
    manifest.bundle_id = compute_bundle_id(&manifest)?;

    write_bundle(&args.out_dir, &manifest, &outputs)?;
    println!("{}", args.out_dir.display());
    Ok(())
}

fn run_cargo_build(
    package: &str,
    target: &str,
    release: bool,
    no_default_features: bool,
) -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("-p")
        .arg(package)
        .arg("--target")
        .arg(target);
    if no_default_features {
        cmd.arg("--no-default-features");
    }
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

fn resolve_wasm_path(package: &str, target: &str, profile: &str) -> PathBuf {
    let mut path = resolve_target_dir();
    path.push(target);
    path.push(profile);
    path.push(wasm_output_name(package));
    path
}

fn resolve_native_path(package: &str, target: &str, profile: &str) -> Result<PathBuf> {
    let mut base = resolve_target_dir();
    base.push(target);
    base.push(profile);
    let package_raw = package.to_string();
    let package_underscored = package.replace('-', "_");
    let lib_prefix = format!("lib{}", package_underscored);
    let dylib_ext = if target.contains("windows") {
        "dll"
    } else if target.contains("apple") {
        "dylib"
    } else {
        "so"
    };
    let candidates = [
        package_raw,
        package_underscored,
        format!("{}.{}", lib_prefix, dylib_ext),
        format!("{}.rlib", lib_prefix),
        format!("{}.a", lib_prefix),
    ];
    for candidate in candidates {
        let path = base.join(&candidate);
        if path.exists() {
            return Ok(path);
        }
    }
    Err(anyhow!(
        "no native artifact found for target {target} under {}",
        base.display()
    ))
}

fn resolve_artifact_path(package: &str, target: &str, profile: &str) -> Result<PathBuf> {
    if is_wasm_target(target) {
        Ok(resolve_wasm_path(package, target, profile))
    } else {
        resolve_native_path(package, target, profile)
    }
}

fn is_wasm_target(target: &str) -> bool {
    target.starts_with("wasm32-")
}

fn output_file_name(target: &str, multi_target: bool) -> String {
    if is_wasm_target(target) {
        if multi_target {
            format!("module.{target}.wasm")
        } else {
            "module.wasm".to_string()
        }
    } else {
        format!("module.{target}")
    }
}

fn resolve_targets(args: &BundleArgs, host_override: Option<&str>) -> Result<Vec<String>> {
    let mut targets = Vec::new();
    if args.wasm {
        targets.push("wasm32-unknown-unknown".to_string());
    }
    if args.native {
        targets.push(resolve_host_target(host_override)?);
    }
    targets.extend(args.target.iter().cloned());
    if targets.is_empty() {
        targets.push("wasm32-unknown-unknown".to_string());
    }

    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for target in targets {
        if seen.insert(target.clone()) {
            deduped.push(target);
        }
    }
    Ok(deduped)
}

fn resolve_build_profile(args: &BundleArgs) -> BuildProfile {
    if args.dev {
        BuildProfile::Debug
    } else {
        BuildProfile::Release
    }
}

fn should_run_wasm_opt(profile: BuildProfile, target: &str) -> bool {
    profile.is_release() && is_wasm_target(target)
}

fn resolve_host_target(host_override: Option<&str>) -> Result<String> {
    if let Some(host) = host_override {
        return Ok(host.to_string());
    }
    let output = Command::new("rustc")
        .arg("-vV")
        .output()
        .context("failed to run rustc -vV")?;
    if !output.status.success() {
        return Err(anyhow!("rustc -vV failed"));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if let Some(host) = line.strip_prefix("host: ") {
            return Ok(host.trim().to_string());
        }
    }
    Err(anyhow!("rustc -vV output missing host line"))
}

fn select_primary_artifact_index(outputs: &[BuiltArtifact]) -> usize {
    outputs
        .iter()
        .position(|output| is_wasm_target(&output.target))
        .unwrap_or(0)
}

fn run_wasm_opt(input: &[u8]) -> Result<Vec<u8>> {
    let tempdir = tempfile::tempdir().context("failed to create wasm-opt tempdir")?;
    let mut input_file = tempfile::Builder::new()
        .prefix("lattice.bundle.input.")
        .suffix(".wasm")
        .tempfile_in(tempdir.path())
        .context("failed to create wasm-opt input file")?;
    input_file
        .write_all(input)
        .context("failed to write wasm-opt input")?;
    let input_path = input_file.into_temp_path();
    let output_file = tempfile::Builder::new()
        .prefix("lattice.bundle.output.")
        .suffix(".wasm")
        .tempfile_in(tempdir.path())
        .context("failed to create wasm-opt output file")?;
    let output_path = output_file.path().to_owned();
    drop(output_file);

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

    fs::read(&output_path).context("failed to read wasm-opt output")
}

fn write_bundle(out_dir: &Path, manifest: &Manifest, outputs: &[BuiltArtifact]) -> Result<()> {
    if out_dir.exists() {
        fs::remove_dir_all(out_dir)
            .with_context(|| format!("failed to clear {}", out_dir.display()))?;
    }
    fs::create_dir_all(out_dir)
        .with_context(|| format!("failed to create {}", out_dir.display()))?;
    for output in outputs {
        let path = out_dir.join(&output.file_name);
        fs::write(&path, &output.bytes)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    let json = serde_json::to_vec_pretty(manifest).context("failed to serialize manifest")?;
    fs::write(out_dir.join("manifest.json"), json)
        .with_context(|| format!("failed to write {}/manifest.json", out_dir.display()))?;
    Ok(())
}

struct BuiltArtifact {
    target: String,
    file_name: String,
    bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{OsStr, OsString};
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        key: &'static str,
        prev: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
            let prev = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.prev.take() {
                unsafe {
                    std::env::set_var(self.key, prev);
                }
            } else {
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn wasm_output_name_normalizes_hyphens() {
        assert_eq!(
            wasm_output_name("example-s6-spill"),
            "example_s6_spill.wasm"
        );
    }

    #[test]
    fn resolve_target_dir_prefers_env() {
        let _lock = env_lock().lock().unwrap();
        let tempdir = tempfile::tempdir().expect("tempdir");
        let _guard = EnvGuard::set("CARGO_TARGET_DIR", tempdir.path());
        let dir = resolve_target_dir();
        assert_eq!(dir, tempdir.path());
    }

    #[test]
    fn resolve_build_profile_defaults_to_release() {
        let args = BundleArgs {
            package: "example-s6-spill".to_string(),
            manifest: None,
            out_dir: PathBuf::from("flow.bundle"),
            release: false,
            dev: false,
            wasm: false,
            native: false,
            target: Vec::new(),
        };

        assert_eq!(resolve_build_profile(&args), BuildProfile::Release);
    }

    #[test]
    fn resolve_build_profile_uses_debug_when_dev() {
        let args = BundleArgs {
            package: "example-s6-spill".to_string(),
            manifest: None,
            out_dir: PathBuf::from("flow.bundle"),
            release: false,
            dev: true,
            wasm: false,
            native: false,
            target: Vec::new(),
        };

        assert_eq!(resolve_build_profile(&args), BuildProfile::Debug);
    }

    #[test]
    fn wasm_opt_runs_only_for_release_wasm() {
        assert!(should_run_wasm_opt(
            BuildProfile::Release,
            "wasm32-unknown-unknown"
        ));
        assert!(!should_run_wasm_opt(
            BuildProfile::Debug,
            "wasm32-unknown-unknown"
        ));
        assert!(!should_run_wasm_opt(
            BuildProfile::Release,
            "x86_64-unknown-linux-gnu"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn run_wasm_opt_uses_unique_temp_paths() {
        use std::os::unix::fs::PermissionsExt;

        let _lock = env_lock().lock().unwrap();
        let tempdir = tempfile::tempdir().expect("tempdir");
        let bin_dir = tempdir.path().join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        let wasm_opt_path = bin_dir.join("wasm-opt");
        let script = r#"#!/bin/sh
set -eu
out=""
in=""
next_is_out=0
for arg in "$@"; do
  if [ "$next_is_out" -eq 1 ]; then
    out="$arg"
    next_is_out=0
    continue
  fi
  if [ "$arg" = "-o" ]; then
    next_is_out=1
    continue
  fi
  case "$arg" in
    -*) ;;
    *) in="$arg" ;;
  esac
done
 if [ -z "$out" ] || [ -z "$in" ]; then
  echo "missing args" >&2
  exit 2
fi
 if [ -e "$out" ]; then
   echo "output already exists" >&2
   exit 4
 fi
base_in=$(basename "$in")
base_out=$(basename "$out")
if [ "$base_in" = "lattice.bundle.input.wasm" ] || [ "$base_out" = "lattice.bundle.output.wasm" ]; then
  echo "fixed temp names" >&2
  exit 3
fi
cp "$in" "$out"
"#;
        fs::write(&wasm_opt_path, script).expect("write wasm-opt");
        let mut perms = fs::metadata(&wasm_opt_path)
            .expect("metadata")
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&wasm_opt_path, perms).expect("chmod wasm-opt");

        let existing_path = std::env::var_os("PATH").unwrap_or_default();
        let mut new_path = OsString::from(&bin_dir);
        new_path.push(OsString::from(":"));
        new_path.push(existing_path);
        let _path_guard = EnvGuard::set("PATH", new_path);
        let _tmp_guard = EnvGuard::set("TMPDIR", tempdir.path());

        let payload = b"fake-wasm";
        let optimized = run_wasm_opt(payload).expect("wasm-opt success");
        assert_eq!(optimized, payload);
    }

    #[cfg(not(unix))]
    #[test]
    fn run_wasm_opt_uses_unique_temp_paths() {
        // No-op on non-unix platforms: wasm-opt test relies on /bin/sh and unix permissions.
    }

    #[test]
    fn resolves_default_to_wasm() {
        let args = BundleArgs {
            package: "example-s6-spill".to_string(),
            manifest: None,
            out_dir: PathBuf::from("flow.bundle"),
            release: false,
            dev: false,
            wasm: false,
            native: false,
            target: Vec::new(),
        };

        let targets = resolve_targets(&args, Some("x86_64-unknown-linux-gnu")).unwrap();
        assert_eq!(targets, vec!["wasm32-unknown-unknown".to_string()]);
    }

    #[test]
    fn resolves_wasm_and_native() {
        let args = BundleArgs {
            package: "example-s6-spill".to_string(),
            manifest: None,
            out_dir: PathBuf::from("flow.bundle"),
            release: false,
            dev: false,
            wasm: true,
            native: true,
            target: Vec::new(),
        };

        let targets = resolve_targets(&args, Some("x86_64-unknown-linux-gnu")).unwrap();
        assert_eq!(
            targets,
            vec![
                "wasm32-unknown-unknown".to_string(),
                "x86_64-unknown-linux-gnu".to_string()
            ]
        );
    }
}
