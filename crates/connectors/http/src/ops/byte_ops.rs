//! The connector.http byte-plane ops (spec §16.5, packet H5c-ops, native half):
//! `get_binary` (2xx body → workspace `Artifact`) and
//! `post_multipart`/`put_multipart` (`(field, ByteSource)` parts →
//! `multipart/form-data`). These are the *native* implementations — they hold
//! bytes in-process and reach the run workspace through the handle-only
//! `workspace_read()` / `workspace_write()` VIEWS (spec §16.4), never the raw
//! `Workspace` trait. The wasm composite opcodes are a separate packet.
//!
//! **Effect floors (spec §16.3, review F5).** `get_binary` declares
//! `http_read + workspace_write` and floors **Effectful** (not ReadOnly): a
//! binary GET both reads the remote and mutates run state by staging a file
//! (`HINT_WORKSPACE_WRITE` floors Effectful). Crucially it is
//! *run-local-idempotent* — the staging write targets run-scoped,
//! idempotent-by-path storage (a replay re-downloads and re-stages to the same
//! path), so it requires **no** exactly-once edge / dedupe key, unlike an
//! external §6 write. `post_multipart`/`put_multipart` keep their method's
//! `Effectful` floor and add `workspace_read` (a part *may* deref an artifact).
//!
//! **Tier 0/1 only.** No `*_any_origin` binary variants exist (spec §16.8 F8:
//! a Tier-2 binary GET would stage attacker-chosen bytes under a valid handle).

use dag_core::{
    ConnectorResolutionContract, ConnectorResolutionModeDecl, ConnectorRoleKindDecl,
    ConnectorRoleRequirement,
};

use capabilities::artifact::sha256_hex;
use capabilities::http::HttpMethod;
use capabilities::{Artifact, ByteSource, context};

use crate::generated::profiles::HTTP_TARGET_AUTH_BEARER;
use crate::generated::types::{
    HttpGetBinaryInput, HttpJsonOutput, HttpMultipartInput, MultipartPart,
};
use crate::runtime::errors::HttpConnectorError;
use crate::runtime::http_api::{HttpApi, HttpCall, HttpRawCall};
use crate::runtime::multipart::{ResolvedPart, build_multipart};

/// Byte-op size ceiling (spec §16.9 Q2). The spec's normative cap is
/// `WorkspacePolicy.max_single_file_bytes`, but the policy is **not reachable
/// from a connector op**: the op receives the capability-narrowed
/// `WorkspaceWrite` view (spec §16.4), which by design exposes only
/// `stage_artifact` / `read` and carries no policy handle. So we enforce a sane
/// constant here — over-limit fails before any staging, so no partial artifact
/// is written (§16.5).
//
// H5c: threading `WorkspacePolicy.max_single_file_bytes` into the byte views
// (so the op can honor the host-configured cap instead of this constant) is
// deferred — it needs the policy carried alongside the root key in
// `WorkspaceWrite`, which is a capabilities-layer change beyond this packet.
const MAX_BINARY_BYTES: u64 = 32 * 1024 * 1024;

const RESOLUTION: ConnectorResolutionContract = ConnectorResolutionContract {
    supported_modes: &[ConnectorResolutionModeDecl::BoundConnection],
    default_mode: ConnectorResolutionModeDecl::BoundConnection,
};

/// Tier 0/1 roles — identical to the method ops: a required endpoint profile
/// plus an OPTIONAL outbound-auth role. No Tier-2 (`any_origin`) byte roles.
const TIER0_ROLES: &[ConnectorRoleRequirement] = &[
    ConnectorRoleRequirement {
        kind: ConnectorRoleKindDecl::EndpointProfile,
        name: "http_target",
        expected_handle_kind: "endpoint.profile",
        required: true,
    },
    ConnectorRoleRequirement {
        kind: ConnectorRoleKindDecl::OutboundAuth,
        name: "http_target_auth",
        expected_handle_kind: "http.bearer",
        required: false,
    },
];

// ─────────────────────────────────────────────────────────────────────────
// connector.http.get_binary
// ─────────────────────────────────────────────────────────────────────────

/// `connector.http.get_binary`: GET a lock-bound origin, stage the 2xx body
/// into the run workspace, return an `Artifact`. `http_read + workspace_write`,
/// `Effectful` (run-local-idempotent — see module docs).
pub struct HttpGetBinary;

impl HttpGetBinary {
    pub const META: dag_core::ConnectorOpMetadata = dag_core::ConnectorOpMetadata {
        operation_id: "connector.http.get_binary",
        connector_id: "connector.http",
        summary: "HTTP GET whose 2xx body is staged into the run workspace as an Artifact",
        min_effects: dag_core::Effects::Effectful,
        max_determinism: dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[
            capabilities::http::HINT_HTTP_READ,
            capabilities::workspace::HINT_WORKSPACE_WRITE,
        ],
        roles: TIER0_ROLES,
        resolution: RESOLUTION,
    };

    /// Fetch and stage. Returns the staged `Artifact { handle, content_type,
    /// len, content_hash }`.
    pub async fn invoke(input: &HttpGetBinaryInput) -> Result<Artifact, HttpConnectorError> {
        let api = HttpApi::for_target(Self::META.operation_id).await?;
        let call = HttpCall {
            method: HttpMethod::Get,
            path_or_url: &input.path,
            query: &input.query,
            headers: &input.headers,
            body: None,
            auth: Some(&HTTP_TARGET_AUTH_BEARER),
        };
        // http_read-gated fetch; non-2xx / transport errors behave as §5.
        let (body, content_type) = api.bytes(call).await?;

        // Enforce the size ceiling BEFORE staging — no partial artifact (§16.5).
        let len = body.len() as u64;
        if len > MAX_BINARY_BYTES {
            return Err(HttpConnectorError::ArtifactTooLarge {
                cap: MAX_BINARY_BYTES,
                actual: len,
            });
        }

        let stage_name = input
            .stage_name
            .clone()
            .unwrap_or_else(|| derive_stage_name(&body));

        // Reach the workspace_write() VIEW (spec §16.4) — the handle-only
        // surface, gated on `resource::workspace::write`.
        context::with_current_async(move |resources| async move {
            let writer = resources
                .workspace_write()
                .ok_or(HttpConnectorError::MissingWorkspaceWrite)?;
            let artifact = writer
                .stage_artifact(&stage_name, &body, content_type)
                .await?;
            Ok::<Artifact, HttpConnectorError>(artifact)
        })
        .await
        .ok_or(HttpConnectorError::MissingResourceContext)?
    }
}

/// Deterministic default stage name: `downloads/{sha256(body)[..16]}`
/// (§16.3 run-local-idempotent-by-path).
fn derive_stage_name(body: &[u8]) -> String {
    format!("downloads/{}", &sha256_hex(body)[..16])
}

// ─────────────────────────────────────────────────────────────────────────
// connector.http.post_multipart / .put_multipart
// ─────────────────────────────────────────────────────────────────────────

/// Resolve each part's bytes + content-type through the `workspace_read()`
/// VIEW (spec §16.4) for `Artifact` parts, then assemble the body. Inline parts
/// use their bytes directly. If any part is an `Artifact` and the node lacks
/// the `workspace::read` grant, the view is unreachable and the whole call
/// fails *before* the request is sent (gate 1).
async fn build_body_from_parts(
    parts: &[MultipartPart],
) -> Result<crate::runtime::multipart::BuiltMultipart, HttpConnectorError> {
    let parts = parts.to_vec();
    let resolved = context::with_current_async(move |resources| async move {
        let mut resolved = Vec::with_capacity(parts.len());
        for part in &parts {
            let (bytes, content_type) = match &part.source {
                ByteSource::Inline(bytes) => (
                    bytes.clone(),
                    part.content_type
                        .clone()
                        .unwrap_or_else(|| "application/octet-stream".to_string()),
                ),
                ByteSource::Artifact(artifact) => {
                    let reader = resources
                        .workspace_read()
                        .ok_or(HttpConnectorError::MissingWorkspaceRead)?;
                    let bytes = reader.read(&artifact.handle).await?;
                    let content_type = part
                        .content_type
                        .clone()
                        .unwrap_or_else(|| artifact.content_type.clone());
                    (bytes, content_type)
                }
            };
            resolved.push(ResolvedPart {
                field: part.field.clone(),
                filename: part.filename.clone(),
                content_type,
                bytes,
            });
        }
        Ok::<Vec<ResolvedPart>, HttpConnectorError>(resolved)
    })
    .await
    .ok_or(HttpConnectorError::MissingResourceContext)??;

    Ok(build_multipart(&resolved))
}

async fn invoke_multipart(
    action_id: &'static str,
    method: HttpMethod,
    input: &HttpMultipartInput,
) -> Result<HttpJsonOutput, HttpConnectorError> {
    // Resolve/deref parts (workspace_read-gated) BEFORE opening the transport,
    // so a missing grant denies before any egress.
    let built = build_body_from_parts(&input.parts).await?;

    let api = HttpApi::for_target(action_id).await?;
    let body = api
        .json_raw(HttpRawCall {
            method,
            path_or_url: &input.path,
            query: &input.query,
            headers: &input.headers,
            content_type: &built.content_type,
            body: built.body,
            auth: Some(&HTTP_TARGET_AUTH_BEARER),
        })
        .await?;
    Ok(HttpJsonOutput { body })
}

macro_rules! define_multipart_op {
    ($struct:ident, $op_id:literal, $summary:literal, $method:ident) => {
        pub struct $struct;

        impl $struct {
            pub const META: dag_core::ConnectorOpMetadata = dag_core::ConnectorOpMetadata {
                operation_id: $op_id,
                connector_id: "connector.http",
                summary: $summary,
                min_effects: dag_core::Effects::Effectful,
                max_determinism: dag_core::Determinism::BestEffort,
                determinism_hints: &[capabilities::http::HINT_HTTP],
                effect_hints: &[
                    capabilities::http::HINT_HTTP_WRITE,
                    capabilities::workspace::HINT_WORKSPACE_READ,
                ],
                roles: TIER0_ROLES,
                resolution: RESOLUTION,
            };

            /// Send a `multipart/form-data` body assembled from the input parts
            /// and return the decoded 2xx JSON response (§5 `json` mode).
            pub async fn invoke(
                input: &HttpMultipartInput,
            ) -> Result<HttpJsonOutput, HttpConnectorError> {
                invoke_multipart(Self::META.operation_id, HttpMethod::$method, input).await
            }
        }
    };
}

define_multipart_op!(
    HttpPostMultipart,
    "connector.http.post_multipart",
    "HTTP POST of a multipart/form-data body against a lock-bound origin",
    Post
);
define_multipart_op!(
    HttpPutMultipart,
    "connector.http.put_multipart",
    "HTTP PUT of a multipart/form-data body against a lock-bound origin",
    Put
);
