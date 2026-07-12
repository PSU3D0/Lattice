//! Canonical graph-node wrappers for the byte-plane ops (spec §16.5). Each
//! hoists its op's effect floor via `connector_ops(...)`: `get_binary` →
//! `http_read + workspace_write` + Effectful; `post_multipart`/`put_multipart`
//! → `http_write + workspace_read` + Effectful.

use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use capabilities::Artifact;

use crate::generated::types::{HttpGetBinaryInput, HttpJsonOutput, HttpMultipartInput};

#[def_node(
    name = "HttpGetBinary",
    summary = "HTTP GET whose 2xx body is staged into the run workspace as an Artifact",
    identifier = "connector.http.get_binary",
    connector_ops(crate::ops::HttpGetBinary)
)]
pub async fn http_get_binary(input: HttpGetBinaryInput) -> NodeResult<Artifact> {
    crate::ops::HttpGetBinary::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.get_binary failed: {err}")))
}

#[def_node(
    name = "HttpPostMultipart",
    summary = "HTTP POST of a multipart/form-data body against a lock-bound origin",
    identifier = "connector.http.post_multipart",
    connector_ops(crate::ops::HttpPostMultipart)
)]
pub async fn http_post_multipart(input: HttpMultipartInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPostMultipart::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.post_multipart failed: {err}")))
}

#[def_node(
    name = "HttpPutMultipart",
    summary = "HTTP PUT of a multipart/form-data body against a lock-bound origin",
    identifier = "connector.http.put_multipart",
    connector_ops(crate::ops::HttpPutMultipart)
)]
pub async fn http_put_multipart(input: HttpMultipartInput) -> NodeResult<HttpJsonOutput> {
    crate::ops::HttpPutMultipart::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.http.put_multipart failed: {err}")))
}
