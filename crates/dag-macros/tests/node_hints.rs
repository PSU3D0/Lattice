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

#[def_node(
    name = "SqlLookup",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(sql_read(capabilities::sql::SqlRead))
)]
async fn sql_lookup(key: String) -> NodeResult<String> {
    Ok(key)
}

#[def_node(
    name = "SqlPersist",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(sql_write(capabilities::sql::SqlWrite))
)]
async fn sql_persist(key: String) -> NodeResult<String> {
    Ok(key)
}

#[def_node(
    name = "SqlMigrate",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(sql_admin(capabilities::sql::SqlAdmin))
)]
async fn sql_migrate(key: String) -> NodeResult<String> {
    Ok(key)
}

#[def_node(
    name = "D1Lookup",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(d1_read(capabilities::sql::SqlRead))
)]
async fn d1_lookup(key: String) -> NodeResult<String> {
    Ok(key)
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

#[test]
fn sql_read_node_emits_sql_read_hint() {
    let spec = node!(sql_lookup);
    assert!(
        spec.effect_hints.contains(&"resource::sql::read"),
        "expected sql read hint, got {:?}",
        spec.effect_hints
    );
    assert!(
        spec.determinism_hints.contains(&"resource::sql"),
        "expected sql determinism hint, got {:?}",
        spec.determinism_hints
    );
}

#[test]
fn sql_write_node_emits_sql_write_hint() {
    let spec = node!(sql_persist);
    assert!(
        spec.effect_hints.contains(&"resource::sql::write"),
        "expected sql write hint, got {:?}",
        spec.effect_hints
    );
    assert!(
        spec.determinism_hints.contains(&"resource::sql"),
        "expected sql determinism hint, got {:?}",
        spec.determinism_hints
    );
}

#[test]
fn sql_admin_node_emits_sql_admin_hint() {
    let spec = node!(sql_migrate);
    assert!(
        spec.effect_hints.contains(&"resource::sql::admin"),
        "expected sql admin hint, got {:?}",
        spec.effect_hints
    );
}

#[test]
fn d1_alias_emits_sql_read_hint() {
    let spec = node!(d1_lookup);
    assert!(
        spec.effect_hints.contains(&"resource::sql::read"),
        "expected d1 alias to emit sql read hint, got {:?}",
        spec.effect_hints
    );
}
