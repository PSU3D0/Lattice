use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Default)]
pub struct HarnessConfig {
    pub default_flow: Option<String>,
    pub flows: Option<Vec<String>>,
}

pub fn write_exporter_crate(
    root: &Path,
    package_path: &Path,
    package_name: &str,
    config: &HarnessConfig,
) -> Result<PathBuf> {
    fs::create_dir_all(root.join("src"))
        .map_err(|err| anyhow!("failed to create {}: {err}", root.display()))?;

    let exporters_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = render_cargo_toml(&exporters_path, package_path, package_name);
    let manifest_path = root.join("Cargo.toml");
    fs::write(&manifest_path, cargo_toml)
        .map_err(|err| anyhow!("failed to write {}: {err}", manifest_path.display()))?;

    let main_rs = render_main_rs(config, package_name);
    let main_path = root.join("src").join("main.rs");
    fs::write(&main_path, main_rs)
        .map_err(|err| anyhow!("failed to write {}: {err}", main_path.display()))?;

    Ok(manifest_path)
}

fn render_cargo_toml(exporters_path: &Path, package_path: &Path, package_name: &str) -> String {
    let exporters_path = exporters_path.display();
    let package_path = package_path.display();
    let package_name = escape_toml_string(package_name);
    format!(
        r#"[package]
name = "flow-exporter"
version = "0.1.0"
edition = "2024"

[dependencies]
exporters = {{ path = "{exporters_path}", features = ["flow-registry"] }}
{package_name} = {{ path = "{package_path}", features = ["flow-registry"] }}
"#
    )
}

fn render_main_rs(config: &HarnessConfig, package_name: &str) -> String {
    let crate_ident = crate_ident(package_name);
    let flows = render_option_vec(&config.flows);
    let default_flow = render_option_string(&config.default_flow);
    format!(
        r#"extern crate {crate_ident} as _;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {{
    let mut args = env::args().skip(1);
    let mut out_dir: Option<PathBuf> = None;
    while let Some(arg) = args.next() {{
        if arg == "--out-dir" {{
            out_dir = args.next().map(PathBuf::from);
        }}
    }}

    let out_dir = out_dir.ok_or("missing --out-dir")?;
    let config = exporters::bundle::BundleConfig {{
        default_flow: {default_flow},
        flows: {flows},
    }};
    let export = exporters::bundle::build_manifest_from_registry(&config)?;
    exporters::bundle::emit_bundle(&out_dir, &export)?;
    Ok(())
}}
"#
    )
}

fn render_option_string(value: &Option<String>) -> String {
    match value {
        Some(value) => format!("Some(\"{}\".to_string())", escape_rust_string(value)),
        None => "None".to_string(),
    }
}

fn render_option_vec(value: &Option<Vec<String>>) -> String {
    match value {
        Some(values) => {
            let rendered = values
                .iter()
                .map(|value| format!("\"{}\".to_string()", escape_rust_string(value)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("Some(vec![{rendered}])")
        }
        None => "None".to_string(),
    }
}

fn escape_rust_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn escape_toml_string(value: &str) -> String {
    value.replace('"', "\\\"")
}

fn crate_ident(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
