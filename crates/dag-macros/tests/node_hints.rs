#![allow(dead_code)]

use dag_core::NodeResult;
use dag_macros::{def_node, node};

struct HttpRead;
struct HttpWrite;

#[def_node(
    name = "FetchWebhook",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(http(HttpRead))
)]
async fn fetch_webhook(url: String) -> NodeResult<String> {
    Ok(url)
}

#[def_node(
    name = "PostWebhook",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(http(HttpWrite))
)]
async fn post_webhook(url: String) -> NodeResult<String> {
    Ok(url)
}

#[test]
fn read_node_emits_http_read_hint() {
    let spec = node!(fetch_webhook);
    assert!(
        spec.effect_hints.contains(&"resource::http::read"),
        "expected http read hint, got {:?}",
        spec.effect_hints
    );
    assert!(
        spec.determinism_hints.contains(&"resource::http"),
        "expected http determinism hint, got {:?}",
        spec.determinism_hints
    );
}

#[test]
fn write_node_emits_http_write_hint() {
    let spec = node!(post_webhook);
    assert!(
        spec.effect_hints.contains(&"resource::http::write"),
        "expected http write hint, got {:?}",
        spec.effect_hints
    );
}
