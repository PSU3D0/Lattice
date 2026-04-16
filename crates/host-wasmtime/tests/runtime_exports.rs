use std::sync::Arc;

use capabilities::ResourceBag;
use flow_bundle::WasmGuestExports;
use host_wasmtime::WasmRuntime;
use serde_json::json;

fn guest_module(alloc: &str, free: &str, invoke: &str) -> Vec<u8> {
    let wat = format!(
        r#"(module
            (memory (export "memory") 1)
            (data (i32.const 0) "\00\7b\7d")
            (func (export "{alloc}") (param i32) (result i32)
                i32.const 1024)
            (func (export "{free}") (param i32 i32))
            (func (export "{invoke}") (param i32 i32 i32 i32) (result i64)
                i64.const 12884901888)
        )"#,
    );
    wat::parse_str(&wat).expect("parse wat")
}

#[test]
fn runtime_uses_legacy_export_names_by_default() {
    let wasm = guest_module("lf_guest_alloc", "lf_guest_free", "lf_invoke_node");
    let runtime = WasmRuntime::new(&wasm, None).expect("runtime");
    let output = runtime
        .invoke_value(
            "node://example",
            &json!({"value": "hello"}),
            Arc::new(ResourceBag::new()),
        )
        .expect("invoke legacy exports");
    assert_eq!(output, json!({}));
}

#[test]
fn runtime_uses_per_flow_export_names_when_provided() {
    let exports = WasmGuestExports {
        alloc: "lf_guest_alloc__custom_flow".to_string(),
        free: "lf_guest_free__custom_flow".to_string(),
        invoke: "lf_invoke_node__custom_flow".to_string(),
    };
    let wasm = guest_module(&exports.alloc, &exports.free, &exports.invoke);
    let runtime = WasmRuntime::new(&wasm, Some(exports)).expect("runtime");
    let output = runtime
        .invoke_value(
            "node://example",
            &json!({"value": "hello"}),
            Arc::new(ResourceBag::new()),
        )
        .expect("invoke custom exports");
    assert_eq!(output, json!({}));
}

#[test]
fn runtime_reports_missing_selected_guest_export_name() {
    let wasm = guest_module("lf_guest_alloc", "lf_guest_free", "lf_invoke_node");
    let runtime = WasmRuntime::new(
        &wasm,
        Some(WasmGuestExports {
            alloc: "lf_guest_alloc__missing".to_string(),
            free: "lf_guest_free__missing".to_string(),
            invoke: "lf_invoke_node__missing".to_string(),
        }),
    )
    .expect("runtime");

    let err = runtime
        .invoke_value("node://example", &json!({}), Arc::new(ResourceBag::new()))
        .expect_err("missing custom export should fail");
    assert!(err.to_string().contains("lf_guest_alloc__missing"));
}
