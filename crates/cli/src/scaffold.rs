//! `flows new` — scaffold a new example crate modeled on `examples/s1_echo`.
//!
//! Templates live under `crates/cli/templates/flow-web-a/` and are embedded with
//! `include_str!` (no templating engine — just `{{TOKEN}}` substitution). This
//! keeps the CLI dependency-light and the generated output golden-stable.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, ValueEnum};

// --- Embedded templates --------------------------------------------------

const TMPL_CARGO: &str = include_str!("../templates/flow-web-a/Cargo.toml.tmpl");
const TMPL_LIB: &str = include_str!("../templates/flow-web-a/lib.rs.tmpl");
const TMPL_DUMP_IR: &str = include_str!("../templates/flow-web-a/dump_ir.rs.tmpl");
const TMPL_README: &str = include_str!("../templates/flow-web-a/README.md.tmpl");
const TMPL_SAMPLE: &str = include_str!("../templates/flow-web-a/sample.json.tmpl");

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Profile {
    /// Web flow (HTTP trigger + responder). The only profile supported today.
    Web,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Tier {
    /// Tier A — local/basic: crate tests + graph/entrypoint proof.
    A,
}

#[derive(Args, Debug)]
pub struct NewArgs {
    /// snake_case name for the flow (e.g. `lead_intake`).
    #[arg(long)]
    name: String,
    /// Flow profile. Only `web` is supported today.
    #[arg(long, value_enum, default_value = "web")]
    profile: Profile,
    /// Verification tier. Only `a` (basic) works today.
    #[arg(long, value_enum, default_value = "a")]
    tier: Tier,
    /// Directory (relative to the workspace root) to generate the crate into.
    #[arg(long, default_value = "examples/")]
    dir: PathBuf,
    /// Skip appending the new crate to the root Cargo.toml `[workspace] members`.
    #[arg(long)]
    no_workspace_edit: bool,
}

/// Resolved, validated naming derived from `--name`.
pub struct Names {
    pub snake: String,
    pub kebab: String,
    pub pascal: String,
    pub route: String,
}

/// The set of generated files (path + rendered contents), produced purely from
/// inputs so it can be golden-tested without touching the filesystem.
pub struct GeneratedCrate {
    pub files: Vec<(PathBuf, String)>,
}

pub fn run_new(args: NewArgs) -> Result<()> {
    // Tier gate: only 'a' works now. (clap currently rejects other values, but
    // keep an explicit, documented message in case the enum grows.)
    if args.tier != Tier::A {
        bail!(tier_help());
    }
    if args.profile != Profile::Web {
        bail!("only `--profile web` is supported today");
    }

    let names = parse_name(&args.name)?;
    let workspace_root = workspace_root()?;
    let crate_dir = workspace_root.join(&args.dir).join(&names.snake);

    if crate_dir.exists() {
        bail!(
            "target directory already exists: {} (refusing to overwrite)",
            crate_dir.display()
        );
    }

    let rel_prefix = rel_prefix_to_crates(&workspace_root, &crate_dir)?;
    let generated = render_crate(&crate_dir, &names, &rel_prefix);

    // Write files.
    for (path, contents) in &generated.files {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }

    // Workspace membership edit (default on).
    let member_path = workspace_member_path(&workspace_root, &crate_dir)?;
    let mut workspace_edited = false;
    if !args.no_workspace_edit {
        let cargo_path = workspace_root.join("Cargo.toml");
        let original = fs::read_to_string(&cargo_path)
            .with_context(|| format!("failed to read {}", cargo_path.display()))?;
        let updated = insert_workspace_member(&original, &member_path)?;
        if updated != original {
            fs::write(&cargo_path, updated)
                .with_context(|| format!("failed to write {}", cargo_path.display()))?;
            workspace_edited = true;
        }
    }

    print_next_steps(&names, &crate_dir, &member_path, workspace_edited, args.no_workspace_edit);
    Ok(())
}

/// Pure render step — exercised directly by the golden-file test.
pub fn render_crate(crate_dir: &Path, names: &Names, rel_prefix: &str) -> GeneratedCrate {
    let render = |tmpl: &str| -> String {
        tmpl.replace("{{NAME_SNAKE}}", &names.snake)
            .replace("{{NAME_KEBAB}}", &names.kebab)
            .replace("{{NAME_PASCAL}}", &names.pascal)
            .replace("{{ROUTE}}", &names.route)
            .replace("{{REL_PREFIX}}", rel_prefix)
    };

    let files = vec![
        (crate_dir.join("Cargo.toml"), render(TMPL_CARGO)),
        (crate_dir.join("src/lib.rs"), render(TMPL_LIB)),
        (crate_dir.join("src/bin/dump_ir.rs"), render(TMPL_DUMP_IR)),
        (crate_dir.join("README.md"), render(TMPL_README)),
        (crate_dir.join("payloads/sample.json"), render(TMPL_SAMPLE)),
    ];

    GeneratedCrate { files }
}

/// Validate and derive name forms from a snake_case input.
pub fn parse_name(raw: &str) -> Result<Names> {
    if raw.is_empty() {
        bail!("--name must not be empty");
    }
    let valid = raw
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !valid {
        bail!(
            "--name must be snake_case (ascii lowercase, digits, underscores): got `{raw}`"
        );
    }
    if raw.starts_with('_')
        || raw.ends_with('_')
        || raw.contains("__")
        || raw.chars().next().is_some_and(|c| c.is_ascii_digit())
    {
        bail!(
            "--name `{raw}` must start with a letter and contain no leading/trailing/double underscores"
        );
    }

    let snake = raw.to_string();
    let kebab = snake.replace('_', "-");
    let pascal = snake
        .split('_')
        .map(|seg| {
            let mut chars = seg.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<String>();
    let route = kebab.clone();

    Ok(Names {
        snake,
        kebab,
        pascal,
        route,
    })
}

/// Locate the workspace root by walking up for a `Cargo.toml` containing
/// `[workspace]`.
fn workspace_root() -> Result<PathBuf> {
    let mut dir = std::env::current_dir().context("failed to read current directory")?;
    loop {
        let candidate = dir.join("Cargo.toml");
        if candidate.is_file() {
            let contents = fs::read_to_string(&candidate).unwrap_or_default();
            if contents.contains("[workspace]") {
                return Ok(dir);
            }
        }
        if !dir.pop() {
            bail!("could not locate a workspace Cargo.toml above the current directory");
        }
    }
}

/// Relative `path = "..."` prefix from the generated crate dir to `crates/`.
fn rel_prefix_to_crates(workspace_root: &Path, crate_dir: &Path) -> Result<String> {
    // crate_dir is workspace_root/<dir>/<name>. The dep paths point at
    // workspace_root/crates/<crate>. Compute the relative jump.
    let rel = crate_dir
        .strip_prefix(workspace_root)
        .with_context(|| "generated crate must live inside the workspace root")?;
    let depth = rel.components().count();
    if depth == 0 {
        bail!("generated crate cannot be the workspace root itself");
    }
    // `depth` ".." hops back to workspace root, then into `crates`.
    let ups = vec![".."; depth].join("/");
    Ok(format!("{ups}/crates"))
}

/// The `members` entry string (relative to workspace root, forward slashes).
fn workspace_member_path(workspace_root: &Path, crate_dir: &Path) -> Result<String> {
    let rel = crate_dir
        .strip_prefix(workspace_root)
        .with_context(|| "generated crate must live inside the workspace root")?;
    let s = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    Ok(s)
}

/// Insert `member` into the `[workspace] members = [ ... ]` array, preserving
/// formatting and the array's existing indentation. Idempotent: returns the
/// input unchanged if the member is already present. Fails loudly if the file
/// does not look like the expected workspace manifest.
pub fn insert_workspace_member(cargo_toml: &str, member: &str) -> Result<String> {
    let members_start = cargo_toml
        .find("members = [")
        .ok_or_else(|| anyhow!("root Cargo.toml has no `members = [` array (unexpected format)"))?;

    // Find the closing `]` of the members array, starting after `members = [`.
    let array_open = members_start + "members = [".len();
    let rel_close = cargo_toml[array_open..]
        .find(']')
        .ok_or_else(|| anyhow!("root Cargo.toml `members` array is not closed (unexpected format)"))?;
    let close_idx = array_open + rel_close;

    let array_body = &cargo_toml[array_open..close_idx];

    // Already present?
    let needle_q = format!("\"{member}\"");
    if array_body.contains(&needle_q) {
        return Ok(cargo_toml.to_string());
    }

    // Determine the indentation used by existing entries (default two spaces).
    let indent = array_body
        .lines()
        .find_map(|line| {
            let trimmed = line.trim_start();
            if trimmed.starts_with('"') {
                Some(&line[..line.len() - trimmed.len()])
            } else {
                None
            }
        })
        .unwrap_or("  ")
        .to_string();

    // Split the body into significant content and the trailing whitespace that
    // precedes the closing `]` (including the final newline + indentation). The
    // new entry is inserted on its own line just before that trailing block so
    // the closing bracket keeps its original placement.
    let content = array_body.trim_end_matches(['\n', '\r', ' ', '\t']);
    let trailing_ws = &array_body[content.len()..];

    // Ensure the previous last entry has a trailing comma.
    let content = if content.ends_with(',') || content.ends_with('[') {
        content.to_string()
    } else {
        format!("{content},")
    };

    let new_body = format!("{content}\n{indent}\"{member}\",{trailing_ws}");

    let mut out = String::with_capacity(cargo_toml.len() + member.len() + 8);
    out.push_str(&cargo_toml[..array_open]);
    out.push_str(&new_body);
    out.push_str(&cargo_toml[close_idx..]);
    Ok(out)
}

fn tier_help() -> String {
    [
        "unsupported --tier. Only tier `a` works today.",
        "Verification tiers (impl-docs/spec/example-authoring-conventions.md):",
        "  a — local/basic: crate tests + graph/entrypoint proof (supported now)",
        "  b — serveable: + serve roundtrip proof (not yet scaffolded)",
        "  c — bundleable/portable: + wasm no-default-features + bundle proof (not yet)",
        "  d — Workers-ready flagship: + workerd/miniflare proof (not yet)",
    ]
    .join("\n")
}

fn print_next_steps(
    names: &Names,
    crate_dir: &Path,
    member_path: &str,
    workspace_edited: bool,
    no_workspace_edit: bool,
) {
    let snake = &names.snake;
    let kebab = &names.kebab;
    println!("Scaffolded example-{kebab} at {}", crate_dir.display());
    if workspace_edited {
        println!("Added `{member_path}` to [workspace] members in root Cargo.toml.");
    } else if no_workspace_edit {
        println!(
            "Skipped workspace edit (--no-workspace-edit). Add `{member_path}` to [workspace] members manually."
        );
    }
    println!();
    println!("Next steps (tier a verification):");
    println!("  1. cargo check -p example-{kebab}");
    println!("  2. cargo run -q -p example-{kebab} --bin dump_ir | flows graph check");
    println!(
        "  3. cargo run -q -p example-{kebab} --bin dump_ir > /tmp/{snake}.ir.json && \\"
    );
    println!(
        "       flows entrypoints check --flow /tmp/{snake}.ir.json --trigger-alias trigger --capture-alias responder"
    );
    println!("  4. cargo test -p example-{kebab}");
    println!();
    println!("To serve it via `flows run local/serve --example {snake}`, register it in the CLI:");
    println!("  - add `example-{kebab}` path dep to crates/cli/Cargo.toml");
    println!("  - add `use example_{snake} as {snake};` near the other use example_* lines in crates/cli/src/main.rs");
    println!(
        "  - add `\"{snake}\" => ({snake}::bundle(), false),` to load_example(...) in crates/cli/src/main.rs (the match starts near `\"s1_echo\" =>`)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_name_derives_forms() {
        let n = parse_name("lead_intake").expect("valid");
        assert_eq!(n.snake, "lead_intake");
        assert_eq!(n.kebab, "lead-intake");
        assert_eq!(n.pascal, "LeadIntake");
        assert_eq!(n.route, "lead-intake");
    }

    #[test]
    fn parse_name_rejects_bad_input() {
        assert!(parse_name("LeadIntake").is_err());
        assert!(parse_name("lead-intake").is_err());
        assert!(parse_name("_lead").is_err());
        assert!(parse_name("lead__intake").is_err());
        assert!(parse_name("2lead").is_err());
        assert!(parse_name("").is_err());
    }

    #[test]
    fn insert_member_appends_and_is_idempotent() {
        let src = "[workspace]\nmembers = [\n  \"crates/a\",\n  \"crates/b\"\n]\n";
        let once = insert_workspace_member(src, "examples/new_flow").expect("insert");
        assert!(once.contains("\"crates/b\","), "prior last entry gets comma:\n{once}");
        assert!(once.contains("\"examples/new_flow\","));
        // Idempotent.
        let twice = insert_workspace_member(&once, "examples/new_flow").expect("insert");
        assert_eq!(once, twice);
    }

    #[test]
    fn insert_member_preserves_trailing_comma_style() {
        let src = "[workspace]\nmembers = [\n  \"crates/a\",\n]\n";
        let out = insert_workspace_member(src, "examples/x").expect("insert");
        assert!(out.contains("  \"crates/a\",\n  \"examples/x\",\n]"), "got:\n{out}");
    }

    #[test]
    fn insert_member_rejects_unexpected_manifest() {
        assert!(insert_workspace_member("[package]\nname = \"x\"\n", "examples/x").is_err());
    }
}
