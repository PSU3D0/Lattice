//! Canonical graph-node wrappers for the twelve method ops. Each returns the
//! default `json` response mode (`HttpJsonOutput`); typed/text/full modes are
//! reached through the op's `invoke_typed`/`invoke_text`/`invoke_full` (spec
//! §3b) inside custom `def_node`s.
//!
//! These are written out explicitly (not via a local `macro_rules!`) because
//! `def_node` emits a `#[cfg(feature = "host-bundle")]`-gated `_register`
//! function that does not survive being generated from inside another macro.

use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{
    HttpJsonOutput, HttpReadAnyOriginInput, HttpReadInput, HttpWriteAnyOriginInput, HttpWriteInput,
};

// ---- Tier 0 (lock-bound origin) -------------------------------------------

#[def_node(
    name = "HttpGet",
    summary = "HTTP GET against a lock-bound origin",
    identifier = "connector.http.get",
    connector_ops(crate::ops::HttpGet)
)]
pub async fn http_get(input: HttpReadInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpGet::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.get failed: {err}")))
}

#[def_node(
    name = "HttpHead",
    summary = "HTTP HEAD against a lock-bound origin",
    identifier = "connector.http.head",
    connector_ops(crate::ops::HttpHead)
)]
pub async fn http_head(input: HttpReadInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpHead::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.head failed: {err}")))
}

#[def_node(
    name = "HttpPost",
    summary = "HTTP POST against a lock-bound origin",
    identifier = "connector.http.post",
    connector_ops(crate::ops::HttpPost)
)]
pub async fn http_post(input: HttpWriteInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPost::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.post failed: {err}")))
}

#[def_node(
    name = "HttpPut",
    summary = "HTTP PUT against a lock-bound origin",
    identifier = "connector.http.put",
    connector_ops(crate::ops::HttpPut)
)]
pub async fn http_put(input: HttpWriteInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPut::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.put failed: {err}")))
}

#[def_node(
    name = "HttpPatch",
    summary = "HTTP PATCH against a lock-bound origin",
    identifier = "connector.http.patch",
    connector_ops(crate::ops::HttpPatch)
)]
pub async fn http_patch(input: HttpWriteInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPatch::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.patch failed: {err}")))
}

#[def_node(
    name = "HttpDelete",
    summary = "HTTP DELETE against a lock-bound origin",
    identifier = "connector.http.delete",
    connector_ops(crate::ops::HttpDelete)
)]
pub async fn http_delete(input: HttpWriteInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpDelete::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.delete failed: {err}")))
}

// ---- Tier 2 (any-origin) --------------------------------------------------

#[def_node(
    name = "HttpGetAnyOrigin",
    summary = "HTTP GET against a runtime-supplied absolute URL",
    identifier = "connector.http.get_any_origin",
    connector_ops(crate::ops::HttpGetAnyOrigin)
)]
pub async fn http_get_any_origin(input: HttpReadAnyOriginInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpGetAnyOrigin::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.get_any_origin failed: {err}")))
}

#[def_node(
    name = "HttpHeadAnyOrigin",
    summary = "HTTP HEAD against a runtime-supplied absolute URL",
    identifier = "connector.http.head_any_origin",
    connector_ops(crate::ops::HttpHeadAnyOrigin)
)]
pub async fn http_head_any_origin(input: HttpReadAnyOriginInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpHeadAnyOrigin::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.head_any_origin failed: {err}")))
}

#[def_node(
    name = "HttpPostAnyOrigin",
    summary = "HTTP POST against a runtime-supplied absolute URL",
    identifier = "connector.http.post_any_origin",
    connector_ops(crate::ops::HttpPostAnyOrigin)
)]
pub async fn http_post_any_origin(input: HttpWriteAnyOriginInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPostAnyOrigin::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.post_any_origin failed: {err}")))
}

#[def_node(
    name = "HttpPutAnyOrigin",
    summary = "HTTP PUT against a runtime-supplied absolute URL",
    identifier = "connector.http.put_any_origin",
    connector_ops(crate::ops::HttpPutAnyOrigin)
)]
pub async fn http_put_any_origin(input: HttpWriteAnyOriginInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPutAnyOrigin::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.put_any_origin failed: {err}")))
}

#[def_node(
    name = "HttpPatchAnyOrigin",
    summary = "HTTP PATCH against a runtime-supplied absolute URL",
    identifier = "connector.http.patch_any_origin",
    connector_ops(crate::ops::HttpPatchAnyOrigin)
)]
pub async fn http_patch_any_origin(input: HttpWriteAnyOriginInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPatchAnyOrigin::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.patch_any_origin failed: {err}")))
}

#[def_node(
    name = "HttpDeleteAnyOrigin",
    summary = "HTTP DELETE against a runtime-supplied absolute URL",
    identifier = "connector.http.delete_any_origin",
    connector_ops(crate::ops::HttpDeleteAnyOrigin)
)]
pub async fn http_delete_any_origin(input: HttpWriteAnyOriginInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpDeleteAnyOrigin::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.delete_any_origin failed: {err}")))
}
