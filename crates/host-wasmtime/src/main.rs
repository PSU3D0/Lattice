use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use capabilities::blob::MemoryBlobStore;
use capabilities::ResourceBag;
use host_wasmtime::WasmRuntime;

fn main_usage() -> &'static str {
    "usage: host-wasmtime <guest.wasm> <node_identifier> <input_json>"
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let wasm_path = args.next().ok_or_else(|| anyhow!(main_usage()))?;
    let node_identifier = args.next().ok_or_else(|| anyhow!(main_usage()))?;
    let input_json = args.next().ok_or_else(|| anyhow!(main_usage()))?;
    if args.next().is_some() {
        return Err(anyhow!(main_usage()));
    }

    let wasm_bytes = std::fs::read(&wasm_path)
        .with_context(|| format!("failed to read wasm module at {wasm_path}"))?;
    let resources = ResourceBag::new().with_blob(Arc::new(MemoryBlobStore::new()));
    let runtime = WasmRuntime::new(&wasm_bytes, Arc::new(resources))?;
    let input: serde_json::Value =
        serde_json::from_str(&input_json).context("input_json is not valid JSON")?;
    let output = runtime.invoke_value(&node_identifier, &input)?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
