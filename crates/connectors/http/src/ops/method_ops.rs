//! The twelve connector.http method ops: six Tier-0 (lock-bound origin) and
//! six Tier-2 (`*_any_origin`) variants. Method is static (op identity), so
//! per-op `ConnectorOpMetadata` is honest by construction (spec §4): GET/HEAD
//! declare `http_read` + ReadOnly; POST/PUT/PATCH/DELETE declare `http_write`
//! + Effectful.

use dag_core::{
    ConnectorResolutionContract, ConnectorResolutionModeDecl, ConnectorRoleKindDecl,
    ConnectorRoleRequirement,
};

use capabilities::http::HttpMethod;

use crate::generated::profiles::HTTP_TARGET_AUTH_BEARER;
use crate::generated::types::{
    HttpFullResponse, HttpJsonOutput, HttpReadAnyOriginInput, HttpReadInput, HttpTextOutput,
    HttpWriteAnyOriginInput, HttpWriteInput,
};
use crate::runtime::errors::HttpConnectorError;
use crate::runtime::http_api::{HttpApi, HttpCall};

const RESOLUTION: ConnectorResolutionContract = ConnectorResolutionContract {
    supported_modes: &[ConnectorResolutionModeDecl::BoundConnection],
    default_mode: ConnectorResolutionModeDecl::BoundConnection,
};

/// Tier 0/1 roles: a required endpoint profile plus an OPTIONAL outbound-auth
/// role (a request may carry no auth — spec §7, `required: false`).
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

/// Tier 2 roles: an `endpoint.any_origin` grant and NO outbound-auth role
/// (auth × dynamic host is forbidden — HTTP003, enforced at lock preflight).
const TIER2_ROLES: &[ConnectorRoleRequirement] = &[ConnectorRoleRequirement {
    kind: ConnectorRoleKindDecl::EndpointProfile,
    name: "http_anywhere",
    expected_handle_kind: "endpoint.any_origin",
    required: true,
}];

fn tier0_read_call<'a>(method: HttpMethod, input: &'a HttpReadInput) -> HttpCall<'a> {
    HttpCall {
        method,
        path_or_url: &input.path,
        query: &input.query,
        headers: &input.headers,
        body: None,
        // Default op path applies the dominant (bearer) auth kind, treated as
        // optional. Lock-driven auth-kind selection is packet H3.
        auth: Some(&HTTP_TARGET_AUTH_BEARER),
    }
}

fn tier0_write_call<'a>(method: HttpMethod, input: &'a HttpWriteInput) -> HttpCall<'a> {
    HttpCall {
        method,
        path_or_url: &input.path,
        query: &input.query,
        headers: &input.headers,
        body: input.body.as_ref(),
        auth: Some(&HTTP_TARGET_AUTH_BEARER),
    }
}

fn tier2_read_call<'a>(method: HttpMethod, input: &'a HttpReadAnyOriginInput) -> HttpCall<'a> {
    HttpCall {
        method,
        path_or_url: &input.url,
        query: &input.query,
        headers: &input.headers,
        body: None,
        // Tier 2 binds no lock credential (spec §2/§7).
        auth: None,
    }
}

fn tier2_write_call<'a>(method: HttpMethod, input: &'a HttpWriteAnyOriginInput) -> HttpCall<'a> {
    HttpCall {
        method,
        path_or_url: &input.url,
        query: &input.query,
        headers: &input.headers,
        body: input.body.as_ref(),
        auth: None,
    }
}

macro_rules! define_op {
    (
        $struct:ident, $op_id:literal, $summary:literal, $method:ident,
        effects = $effects:ident, hint = $hint:ident, roles = $roles:ident,
        input = $input:ty, ctor = $ctor:ident, call = $call:ident
    ) => {
        pub struct $struct;

        impl $struct {
            pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
                operation_id: $op_id,
                connector_id: "connector.http",
                summary: $summary,
                min_effects: ::dag_core::Effects::$effects,
                max_determinism: ::dag_core::Determinism::BestEffort,
                determinism_hints: &[capabilities::http::HINT_HTTP],
                effect_hints: &[capabilities::http::$hint],
                broker_contract: None,
                roles: $roles,
                resolution: RESOLUTION,
            };

            /// `json` mode (spec §5): decoded 2xx JSON body.
            pub async fn invoke(input: &$input) -> Result<HttpJsonOutput, HttpConnectorError> {
                let api = HttpApi::$ctor(Self::META.operation_id).await?;
                let body = api.json($call(HttpMethod::$method, input)).await?;
                Ok(HttpJsonOutput { body })
            }

            /// `typed<T>` mode (spec §5): fail-closed serde decode.
            pub async fn invoke_typed<T: ::serde::de::DeserializeOwned>(
                input: &$input,
            ) -> Result<T, HttpConnectorError> {
                let api = HttpApi::$ctor(Self::META.operation_id).await?;
                api.typed($call(HttpMethod::$method, input)).await
            }

            /// `text` mode (spec §5).
            pub async fn invoke_text(input: &$input) -> Result<HttpTextOutput, HttpConnectorError> {
                let api = HttpApi::$ctor(Self::META.operation_id).await?;
                let body = api.text($call(HttpMethod::$method, input)).await?;
                Ok(HttpTextOutput { body })
            }

            /// `full_response` mode (spec §5).
            pub async fn invoke_full(
                input: &$input,
            ) -> Result<HttpFullResponse, HttpConnectorError> {
                let api = HttpApi::$ctor(Self::META.operation_id).await?;
                api.full($call(HttpMethod::$method, input)).await
            }
        }
    };
}

// ---- Tier 0 (lock-bound origin) -------------------------------------------

define_op!(
    HttpGet,
    "connector.http.get",
    "HTTP GET against a lock-bound origin",
    Get,
    effects = ReadOnly,
    hint = HINT_HTTP_READ,
    roles = TIER0_ROLES,
    input = HttpReadInput,
    ctor = for_target,
    call = tier0_read_call
);
define_op!(
    HttpHead,
    "connector.http.head",
    "HTTP HEAD against a lock-bound origin",
    Head,
    effects = ReadOnly,
    hint = HINT_HTTP_READ,
    roles = TIER0_ROLES,
    input = HttpReadInput,
    ctor = for_target,
    call = tier0_read_call
);
define_op!(
    HttpPost,
    "connector.http.post",
    "HTTP POST against a lock-bound origin",
    Post,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER0_ROLES,
    input = HttpWriteInput,
    ctor = for_target,
    call = tier0_write_call
);
define_op!(
    HttpPut,
    "connector.http.put",
    "HTTP PUT against a lock-bound origin",
    Put,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER0_ROLES,
    input = HttpWriteInput,
    ctor = for_target,
    call = tier0_write_call
);
define_op!(
    HttpPatch,
    "connector.http.patch",
    "HTTP PATCH against a lock-bound origin",
    Patch,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER0_ROLES,
    input = HttpWriteInput,
    ctor = for_target,
    call = tier0_write_call
);
define_op!(
    HttpDelete,
    "connector.http.delete",
    "HTTP DELETE against a lock-bound origin",
    Delete,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER0_ROLES,
    input = HttpWriteInput,
    ctor = for_target,
    call = tier0_write_call
);

// ---- Tier 2 (any-origin) --------------------------------------------------

define_op!(
    HttpGetAnyOrigin,
    "connector.http.get_any_origin",
    "HTTP GET against a runtime-supplied absolute URL",
    Get,
    effects = ReadOnly,
    hint = HINT_HTTP_READ,
    roles = TIER2_ROLES,
    input = HttpReadAnyOriginInput,
    ctor = for_any_origin,
    call = tier2_read_call
);
define_op!(
    HttpHeadAnyOrigin,
    "connector.http.head_any_origin",
    "HTTP HEAD against a runtime-supplied absolute URL",
    Head,
    effects = ReadOnly,
    hint = HINT_HTTP_READ,
    roles = TIER2_ROLES,
    input = HttpReadAnyOriginInput,
    ctor = for_any_origin,
    call = tier2_read_call
);
define_op!(
    HttpPostAnyOrigin,
    "connector.http.post_any_origin",
    "HTTP POST against a runtime-supplied absolute URL",
    Post,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER2_ROLES,
    input = HttpWriteAnyOriginInput,
    ctor = for_any_origin,
    call = tier2_write_call
);
define_op!(
    HttpPutAnyOrigin,
    "connector.http.put_any_origin",
    "HTTP PUT against a runtime-supplied absolute URL",
    Put,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER2_ROLES,
    input = HttpWriteAnyOriginInput,
    ctor = for_any_origin,
    call = tier2_write_call
);
define_op!(
    HttpPatchAnyOrigin,
    "connector.http.patch_any_origin",
    "HTTP PATCH against a runtime-supplied absolute URL",
    Patch,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER2_ROLES,
    input = HttpWriteAnyOriginInput,
    ctor = for_any_origin,
    call = tier2_write_call
);
define_op!(
    HttpDeleteAnyOrigin,
    "connector.http.delete_any_origin",
    "HTTP DELETE against a runtime-supplied absolute URL",
    Delete,
    effects = Effectful,
    hint = HINT_HTTP_WRITE,
    roles = TIER2_ROLES,
    input = HttpWriteAnyOriginInput,
    ctor = for_any_origin,
    call = tier2_write_call
);
