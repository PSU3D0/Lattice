use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use connector_spec::{
    ActionSurface, ConnectorManifest, DefaultValue, FieldDecl, FieldKind, OutboundAuthProfile,
    ResourceRequirement, SurfaceDecl, TypeDecl, generated_module_name, paginated_collection_field,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedFile {
    pub relative_path: String,
    pub contents: String,
}

pub fn generate_files(
    manifest: &ConnectorManifest,
    source_yaml: &str,
) -> Result<Vec<GeneratedFile>> {
    manifest
        .validate_for_codegen()
        .map_err(|err| anyhow!(err.to_string()))?;

    let actions = sorted_actions(manifest);
    let mut files = Vec::new();
    files.push(GeneratedFile {
        relative_path: "connector.yaml".to_string(),
        contents: source_yaml.trim().to_string() + "\n",
    });
    files.push(GeneratedFile {
        relative_path: "Cargo.toml".to_string(),
        contents: emit_cargo_toml(manifest),
    });
    files.push(GeneratedFile {
        relative_path: "src/lib.rs".to_string(),
        contents: emit_lib_rs(manifest, &actions),
    });
    files.push(GeneratedFile {
        relative_path: "src/ext.rs".to_string(),
        contents: "// Reserved handwritten extension hooks for connector-specific escapes.\n"
            .to_string(),
    });
    files.push(GeneratedFile {
        relative_path: "src/runtime/mod.rs".to_string(),
        contents: emit_runtime_mod_rs(),
    });
    files.push(GeneratedFile {
        relative_path: "src/runtime/transport.rs".to_string(),
        contents: emit_runtime_transport_rs(),
    });
    files.push(GeneratedFile {
        relative_path: "src/runtime/errors.rs".to_string(),
        contents: "pub use connectors_std::errors::ConnectorRuntimeError;\n".to_string(),
    });
    files.push(GeneratedFile {
        relative_path: "src/runtime/pagination.rs".to_string(),
        contents: "pub use connectors_std::pagination::*;\n".to_string(),
    });
    files.push(GeneratedFile {
        relative_path: "src/generated/mod.rs".to_string(),
        contents: emit_generated_mod_rs(),
    });
    files.push(GeneratedFile {
        relative_path: "src/generated/manifest.rs".to_string(),
        contents: emit_generated_manifest_rs(manifest),
    });
    files.push(GeneratedFile {
        relative_path: "src/generated/types.rs".to_string(),
        contents: emit_generated_types_rs(manifest),
    });
    files.push(GeneratedFile {
        relative_path: "src/generated/profiles.rs".to_string(),
        contents: emit_generated_profiles_rs(manifest),
    });
    files.push(GeneratedFile {
        relative_path: "src/generated/register.rs".to_string(),
        contents: emit_generated_register_rs(&actions),
    });
    files.push(GeneratedFile {
        relative_path: "src/generated/ops/mod.rs".to_string(),
        contents: emit_generated_ops_mod_rs(&actions),
    });
    files.push(GeneratedFile {
        relative_path: "src/generated/actions/mod.rs".to_string(),
        contents: emit_generated_actions_mod_rs(&actions),
    });
    for action in &actions {
        files.push(GeneratedFile {
            relative_path: format!(
                "src/generated/ops/{}.rs",
                generated_module_name(&action.identifier)
            ),
            contents: emit_op_file(manifest, action),
        });
        files.push(GeneratedFile {
            relative_path: format!(
                "src/generated/actions/{}.rs",
                generated_module_name(&action.identifier)
            ),
            contents: emit_action_file(manifest, action),
        });
    }
    files.push(GeneratedFile {
        relative_path: "tests/manifest.rs".to_string(),
        contents: emit_tests_manifest_rs(manifest),
    });
    files.push(GeneratedFile {
        relative_path: "tests/contract.rs".to_string(),
        contents: emit_tests_contract_rs(manifest, &actions),
    });

    Ok(files)
}

pub fn write_generated_files(root: impl AsRef<Path>, files: &[GeneratedFile]) -> Result<()> {
    let root = root.as_ref();
    for file in files {
        let path = root.join(&file.relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create directory {}", parent.display()))?;
        }
        fs::write(&path, &file.contents)
            .with_context(|| format!("write generated file {}", path.display()))?;
    }
    Ok(())
}

fn sorted_actions(manifest: &ConnectorManifest) -> Vec<&ActionSurface> {
    let mut actions = manifest
        .surfaces
        .iter()
        .filter_map(|surface| match surface {
            SurfaceDecl::Action(action) => Some(action),
            _ => None,
        })
        .collect::<Vec<_>>();
    actions.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    actions
}

fn emit_cargo_toml(manifest: &ConnectorManifest) -> String {
    format!(
        r#"[package]
name = "{crate_name}"
version = "{version}"
authors.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
homepage.workspace = true
description = "{description}"

[features]
default = ["host-bundle"]
host-bundle = ["dag-macros/host-bundle"]

[dependencies]
capabilities = {{ path = "../../../capabilities" }}
dag-core = {{ path = "../../../dag-core" }}
dag-macros = {{ path = "../../../dag-macros" }}
kernel-exec = {{ path = "../../../kernel-exec" }}
connectors-std = {{ path = "../../../connectors-std" }}
serde.workspace = true
serde_json.workspace = true

[dev-dependencies]
cap-http-reqwest = {{ path = "../../../cap-http-reqwest" }}
httpmock = "0.7"
tokio = {{ workspace = true, features = ["full"] }}
"#,
        crate_name = manifest.connector.crate_name,
        version = manifest.connector.version,
        description = escape_rust_string(&manifest.connector.summary)
    )
}

fn emit_lib_rs(manifest: &ConnectorManifest, actions: &[&ActionSurface]) -> String {
    let mut out = String::new();
    out.push_str("pub mod ext;\n");
    out.push_str("pub mod generated;\n");
    out.push_str("pub mod runtime;\n\n");
    out.push_str("pub use generated::manifest::*;\n");
    out.push_str("pub use generated::profiles::*;\n");
    out.push_str("pub use generated::types::*;\n");
    out.push_str("pub use generated::actions::*;\n");
    out.push_str("pub mod ops {\n");
    out.push_str("    pub use crate::generated::ops::*;\n");
    out.push_str("}\n");
    out.push_str("#[cfg(feature = \"host-bundle\")]\n");
    out.push_str("pub use generated::register::register_all;\n");
    out.push_str(&format!(
        "\npub const CONNECTOR_FAMILY: &str = \"{}\";\n",
        escape_rust_string(&manifest.connector.id)
    ));
    for action in actions {
        out.push_str(&format!(
            "pub const {}_IDENTIFIER: &str = \"{}\";\n",
            to_upper_snake_case(&action.name),
            escape_rust_string(&action.identifier)
        ));
    }
    out
}

fn emit_runtime_mod_rs() -> String {
    "pub mod errors;\npub mod pagination;\npub mod transport;\n".to_string()
}

fn emit_runtime_transport_rs() -> String {
    r#"pub use connectors_std::ActionDescriptor;
pub use connectors_std::auth::{OutboundAuthKind, OutboundAuthProfileDescriptor};
pub use connectors_std::decode::ResponseDescriptor;
pub use connectors_std::dev::EnvConnectorRuntime;
pub use connectors_std::endpoint::EndpointProfileDescriptor;
pub use connectors_std::http::{FieldBinding, RequestDescriptor, StaticHeaderDescriptor};
pub use connectors_std::pagination::PaginationDescriptor;
pub use connectors_std::run_action_from_current;
"#
    .to_string()
}

fn emit_generated_mod_rs() -> String {
    "pub mod actions;\npub mod manifest;\npub mod ops;\npub mod profiles;\n#[cfg(feature = \"host-bundle\")]\npub mod register;\npub mod types;\n".to_string()
}

fn emit_generated_manifest_rs(manifest: &ConnectorManifest) -> String {
    format!(
        r#"pub const CONNECTOR_ID: &str = "{connector_id}";
pub const CONNECTOR_VENDOR: &str = "{vendor}";
pub const CONNECTOR_FAMILY: &str = "{family}";
pub const CONNECTOR_VERSION: &str = "{version}";
pub const CONNECTOR_CRATE: &str = "{crate_name}";
pub const CONNECTOR_SUMMARY: &str = "{summary}";
pub const CONNECTOR_YAML: &str = include_str!("../../connector.yaml");
"#,
        connector_id = escape_rust_string(&manifest.connector.id),
        vendor = escape_rust_string(&manifest.connector.vendor),
        family = escape_rust_string(&manifest.connector.family),
        version = escape_rust_string(&manifest.connector.version),
        crate_name = escape_rust_string(&manifest.connector.crate_name),
        summary = escape_rust_string(&manifest.connector.summary),
    )
}

fn emit_generated_types_rs(manifest: &ConnectorManifest) -> String {
    let mut out = String::new();
    out.push_str("use serde::{Deserialize, Serialize};\n");
    if manifest_uses_json_field(manifest) {
        out.push_str("use serde_json::Value as JsonValue;\n");
    }
    out.push('\n');
    for (type_name, type_decl) in &manifest.types {
        match type_decl {
            TypeDecl::Enum { variants } => {
                out.push_str("#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\n");
                out.push_str(&format!("pub enum {} {{\n", rust_type_name(type_name)));
                for variant in variants {
                    out.push_str(&format!(
                        "    #[serde(rename = \"{}\")]\n    {},\n",
                        escape_rust_string(variant),
                        rust_enum_variant(variant)
                    ));
                }
                out.push_str("}\n\n");
            }
            TypeDecl::Object { fields } => {
                for (field_name, field_decl) in fields {
                    if let Some(default) = &field_decl.default {
                        out.push_str(&format!(
                            "fn {}() -> {} {{\n    {}\n}}\n\n",
                            default_fn_name(type_name, field_name),
                            rust_type_expression(field_decl),
                            rust_default_expression(default)
                        ));
                    }
                }
                out.push_str("#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]\n");
                out.push_str(&format!("pub struct {} {{\n", rust_type_name(type_name)));
                for (field_name, field_decl) in fields {
                    let attrs = field_attributes(type_name, field_name, field_decl);
                    for attr in attrs {
                        out.push_str("    ");
                        out.push_str(&attr);
                        out.push('\n');
                    }
                    out.push_str(&format!(
                        "    pub {}: {},\n",
                        rust_field_ident(field_name),
                        rust_type_expression(field_decl)
                    ));
                }
                out.push_str("}\n\n");
            }
        }
    }
    out
}

fn emit_generated_profiles_rs(manifest: &ConnectorManifest) -> String {
    let mut out = String::new();
    out.push_str("use crate::runtime::transport::{EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor};\n\n");
    for (profile_name, endpoint) in &manifest.profiles.endpoint_profiles {
        out.push_str(&format!(
            "pub const {}: EndpointProfileDescriptor = EndpointProfileDescriptor {{\n",
            endpoint_const_name(profile_name)
        ));
        out.push_str(&format!(
            "    connector_id: \"{}\",\n",
            escape_rust_string(&manifest.connector.id)
        ));
        out.push_str(&format!(
            "    name: \"{}\",\n",
            escape_rust_string(profile_name)
        ));
        out.push_str(&format!(
            "    env_base_url_var: \"{}\",\n",
            endpoint_env_var(profile_name)
        ));
        out.push_str(&format!(
            "    base_url: \"{}\",\n",
            escape_rust_string(&endpoint.base_url)
        ));
        out.push_str("    default_headers: &[\n");
        for (header, value) in &endpoint.default_headers {
            out.push_str(&format!(
                "        (\"{}\", \"{}\"),\n",
                escape_rust_string(header),
                escape_rust_string(value)
            ));
        }
        out.push_str("    ],\n};\n\n");
    }

    for (profile_name, profile) in &manifest.profiles.outbound_auth {
        out.push_str(&format!(
            "pub const {}: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {{\n",
            auth_const_name(profile_name)
        ));
        out.push_str(&format!(
            "    connector_id: \"{}\",\n",
            escape_rust_string(&manifest.connector.id)
        ));
        out.push_str(&format!(
            "    name: \"{}\",\n",
            escape_rust_string(profile_name)
        ));
        out.push_str(&format!(
            "    env_var: \"{}\",\n",
            auth_env_var(profile_name)
        ));
        out.push_str("    kind: ");
        out.push_str(&emit_outbound_auth_kind(profile));
        out.push_str(",\n};\n\n");
    }
    out
}

fn emit_generated_register_rs(actions: &[&ActionSurface]) -> String {
    let mut out = String::new();
    out.push_str("use kernel_exec::{NodeRegistry, RegistryError};\n\n");
    out.push_str(
        "pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {\n",
    );
    for action in actions {
        out.push_str(&format!(
            "    crate::generated::actions::{}_register(registry)?;\n",
            to_snake_case(&action.name)
        ));
    }
    out.push_str("    Ok(())\n}\n");
    out
}

fn emit_generated_ops_mod_rs(actions: &[&ActionSurface]) -> String {
    let mut out = String::new();
    for action in actions {
        let module_name = generated_module_name(&action.identifier);
        out.push_str(&format!("mod {};\n", module_name));
    }
    out.push('\n');
    for action in actions {
        let module_name = generated_module_name(&action.identifier);
        out.push_str(&format!("pub use {}::*;\n", module_name));
    }
    out
}

fn emit_generated_actions_mod_rs(actions: &[&ActionSurface]) -> String {
    let mut out = String::new();
    for action in actions {
        let module_name = generated_module_name(&action.identifier);
        out.push_str(&format!("mod {};\n", module_name));
    }
    out.push('\n');
    for action in actions {
        let module_name = generated_module_name(&action.identifier);
        out.push_str(&format!("pub use {}::*;\n", module_name));
    }
    out
}

fn emit_op_file(manifest: &ConnectorManifest, action: &ActionSurface) -> String {
    let op_struct = rust_type_name(&action.name);
    let request_const = format!("{}_REQUEST", to_upper_snake_case(&action.name));
    let response_const = format!("{}_RESPONSE", to_upper_snake_case(&action.name));
    let action_const = format!("{}_ACTION", to_upper_snake_case(&action.name));
    let pagination_const = format!("{}_PAGINATION", to_upper_snake_case(&action.name));
    let endpoint_const = endpoint_const_name(&action.endpoint);
    let auth_expr = action
        .auth
        .as_ref()
        .map(|name| {
            format!(
                "Some(&crate::generated::profiles::{})",
                auth_const_name(name)
            )
        })
        .unwrap_or_else(|| "None".to_string());
    let response = action.response();
    let collection_field = paginated_collection_field(manifest, &action.output)
        .map(|field| format!("Some(\"{}\")", escape_rust_string(field)))
        .unwrap_or_else(|| "None".to_string());

    let mut out = String::new();
    out.push_str(&format!(
        "use crate::generated::types::{{{}, {}}};\n",
        rust_type_name(&action.input),
        rust_type_name(&action.output)
    ));
    let mut transport_imports = vec![
        "ActionDescriptor",
        "FieldBinding",
        "RequestDescriptor",
        "ResponseDescriptor",
        "run_action_from_current",
    ];
    if !action.request.headers.is_empty() {
        transport_imports.push("StaticHeaderDescriptor");
    }
    out.push_str(&format!(
        "use crate::runtime::transport::{{{}}};\n",
        transport_imports.join(", ")
    ));
    if action.pagination.is_some() {
        out.push_str("use crate::runtime::transport::PaginationDescriptor;\n");
    }
    out.push('\n');

    out.push_str(&format!(
        "const {}: RequestDescriptor = RequestDescriptor {{\n",
        request_const
    ));
    out.push_str(&format!(
        "    method: capabilities::http::HttpMethod::{},\n",
        rust_http_method_variant(action.request.method.as_str())
    ));
    out.push_str(&format!(
        "    path_template: \"{}\",\n",
        escape_rust_string(&action.request.path_template)
    ));
    out.push_str(&format!(
        "    path_params: &{},\n",
        emit_field_binding_slice(&action.request.path_params)
    ));
    out.push_str(&format!(
        "    query: &{},\n",
        emit_field_binding_slice(&action.request.query)
    ));
    out.push_str(&format!(
        "    body: &{},\n",
        emit_field_binding_slice(&action.request.body)
    ));
    out.push_str(&format!(
        "    headers: &{},\n",
        emit_static_header_slice(&action.request.headers)
    ));
    out.push_str("};\n\n");

    if let Some(pagination) = &action.pagination {
        out.push_str(&format!(
            "const {}: PaginationDescriptor = PaginationDescriptor {{\n",
            pagination_const
        ));
        out.push_str(&format!(
            "    enabled_from: \"{}\",\n",
            escape_rust_string(&pagination.enabled_from)
        ));
        out.push_str(&format!(
            "    page_size_param: \"{}\",\n",
            escape_rust_string(&pagination.page_size_param)
        ));
        out.push_str(&format!("    page_size: {},\n", pagination.page_size));
        out.push_str(&format!(
            "    max_items_from: {},\n",
            pagination
                .max_items_from
                .as_ref()
                .map(|field| format!("Some(\"{}\")", escape_rust_string(field)))
                .unwrap_or_else(|| "None".to_string())
        ));
        out.push_str("};\n\n");
    }

    out.push_str(&format!(
        "const {}: ResponseDescriptor = ResponseDescriptor {{\n",
        response_const
    ));
    out.push_str(&format!(
        "    root_path: \"{}\",\n",
        escape_rust_string(&response.root_path)
    ));
    out.push_str(&format!("    collection_field: {},\n", collection_field));
    out.push_str("};\n\n");

    out.push_str(&format!(
        "const {}: ActionDescriptor = ActionDescriptor {{\n",
        action_const
    ));
    out.push_str(&format!(
        "    identifier: \"{}\",\n",
        escape_rust_string(&action.identifier)
    ));
    out.push_str(&format!(
        "    endpoint: &crate::generated::profiles::{},\n",
        endpoint_const
    ));
    out.push_str(&format!("    auth: {},\n", auth_expr));
    out.push_str(&format!("    request: &{},\n", request_const));
    out.push_str(&format!(
        "    pagination: {},\n",
        action
            .pagination
            .as_ref()
            .map(|_| format!("Some(&{pagination_const})"))
            .unwrap_or_else(|| "None".to_string())
    ));
    out.push_str(&format!("    response: &{},\n", response_const));
    out.push_str("};\n\n");

    out.push_str(&format!("pub struct {};\n\n", op_struct));
    out.push_str(&format!("impl {} {{\n", op_struct));
    out.push_str(
        "    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {\n",
    );
    out.push_str(&format!(
        "        operation_id: \"{}\",\n",
        escape_rust_string(&action.identifier)
    ));
    out.push_str(&format!(
        "        connector_id: \"{}\",\n",
        escape_rust_string(&manifest.connector.id)
    ));
    out.push_str(&format!(
        "        summary: \"{}\",\n",
        escape_rust_string(&action.summary)
    ));
    out.push_str(&format!(
        "        min_effects: ::dag_core::Effects::{},\n",
        action.effects.as_macro_name()
    ));
    out.push_str(&format!(
        "        max_determinism: ::dag_core::Determinism::{},\n",
        action.determinism.as_macro_name()
    ));
    out.push_str(&format!(
        "        determinism_hints: &{},\n",
        emit_connector_determinism_hints(&action.resources)
    ));
    out.push_str(&format!(
        "        effect_hints: &{},\n",
        emit_connector_effect_hints(&action.resources)
    ));
    out.push_str(&format!(
        "        roles: &{},\n",
        emit_connector_role_requirements(manifest, action)
    ));
    out.push_str("    };\n\n");
    out.push_str(&format!(
        "    pub async fn invoke(input: &{}) -> Result<{}, crate::runtime::errors::ConnectorRuntimeError> {{\n",
        rust_type_name(&action.input),
        rust_type_name(&action.output)
    ));
    out.push_str(&format!(
        "        run_action_from_current(input, &{}).await\n",
        action_const
    ));
    out.push_str("    }\n");
    out.push_str("}\n");
    out
}

fn emit_action_file(_manifest: &ConnectorManifest, action: &ActionSurface) -> String {
    let function_name = to_snake_case(&action.name);
    let op_struct = rust_type_name(&action.name);

    let mut out = String::new();
    out.push_str("use dag_core::{NodeError, NodeResult};\n");
    out.push_str("use dag_macros::def_node;\n\n");
    out.push_str(&format!("use crate::generated::ops::{};\n", op_struct));
    out.push_str(&format!(
        "use crate::generated::types::{{{}, {}}};\n\n",
        rust_type_name(&action.input),
        rust_type_name(&action.output)
    ));

    out.push_str("#[def_node(\n");
    out.push_str(&format!(
        "    name = \"{}\",\n",
        escape_rust_string(&action.name)
    ));
    out.push_str(&format!(
        "    summary = \"{}\",\n",
        escape_rust_string(&action.summary)
    ));
    out.push_str(&format!(
        "    identifier = \"{}\",\n",
        escape_rust_string(&action.identifier)
    ));
    out.push_str(&format!(
        "    connector_ops({})\n",
        format!("crate::generated::ops::{}", op_struct)
    ));
    out.push_str(")]\n");
    out.push_str(&format!(
        "pub async fn {}(input: {}) -> NodeResult<{}> {{\n",
        function_name,
        rust_type_name(&action.input),
        rust_type_name(&action.output)
    ));
    out.push_str(&format!("    {}::invoke(&input)\n", op_struct));
    out.push_str("        .await\n");
    out.push_str(&format!(
        "        .map_err(|err| NodeError::new(format!(\"{} failed: {{err}}\")))\n",
        escape_rust_string(&action.identifier)
    ));
    out.push_str("}\n");
    out
}

fn emit_tests_manifest_rs(manifest: &ConnectorManifest) -> String {
    format!(
        r#"use {crate_name}::generated::manifest::{{CONNECTOR_ID, CONNECTOR_YAML}};

#[test]
fn generated_manifest_embeds_source_yaml() {{
    assert_eq!(CONNECTOR_ID, "{connector_id}");
    assert!(CONNECTOR_YAML.contains(CONNECTOR_ID));
}}
"#,
        crate_name = manifest.connector.crate_name,
        connector_id = escape_rust_string(&manifest.connector.id)
    )
}

fn emit_tests_contract_rs(manifest: &ConnectorManifest, actions: &[&ActionSurface]) -> String {
    let mut out = String::new();
    out.push_str(&format!("use {}::*;\n", manifest.connector.crate_name));
    out.push_str(
        "\n#[cfg(feature = \"host-bundle\")]\n#[test]\nfn register_all_binds_all_actions() {\n",
    );
    out.push_str("    let mut registry = kernel_exec::NodeRegistry::new();\n");
    out.push_str("    register_all(&mut registry).expect(\"register nodes\");\n");
    for action in actions {
        out.push_str(&format!(
            "    assert!(registry.handler(\"{}\").is_some());\n",
            escape_rust_string(&action.identifier)
        ));
    }
    out.push_str("}\n");
    out
}

fn emit_outbound_auth_kind(profile: &OutboundAuthProfile) -> String {
    match profile {
        OutboundAuthProfile::Bearer { handle_kind } => format!(
            "OutboundAuthKind::Bearer {{ handle_kind: \"{}\" }}",
            escape_rust_string(handle_kind)
        ),
        OutboundAuthProfile::ApiKeyHeader {
            header_name,
            prefix,
            handle_kind,
        } => format!(
            "OutboundAuthKind::ApiKeyHeader {{ header_name: \"{}\", prefix: {}, handle_kind: \"{}\" }}",
            escape_rust_string(header_name),
            prefix
                .as_ref()
                .map(|value| format!("Some(\"{}\")", escape_rust_string(value)))
                .unwrap_or_else(|| "None".to_string()),
            escape_rust_string(handle_kind)
        ),
        OutboundAuthProfile::ApiKeyQuery {
            query_name,
            handle_kind,
        } => format!(
            "OutboundAuthKind::ApiKeyQuery {{ query_name: \"{}\", handle_kind: \"{}\" }}",
            escape_rust_string(query_name),
            escape_rust_string(handle_kind)
        ),
        other => format!(
            "OutboundAuthKind::Unsupported {{ kind_name: \"{}\", handle_kind: \"{}\" }}",
            other.kind_name(),
            escape_rust_string(other.handle_kind())
        ),
    }
}

fn emit_field_binding_slice(bindings: &std::collections::BTreeMap<String, String>) -> String {
    if bindings.is_empty() {
        return "[]".to_string();
    }
    let mut rendered = String::from("[");
    let mut first = true;
    for (wire_name, input_field) in bindings {
        if !first {
            rendered.push_str(", ");
        }
        first = false;
        rendered.push_str(&format!(
            "FieldBinding {{ wire_name: \"{}\", input_field: \"{}\" }}",
            escape_rust_string(wire_name),
            escape_rust_string(input_field)
        ));
    }
    rendered.push(']');
    rendered
}

fn emit_static_header_slice(
    bindings: &std::collections::BTreeMap<String, connector_spec::StaticHeaderDecl>,
) -> String {
    if bindings.is_empty() {
        return "[]".to_string();
    }
    let mut rendered = String::from("[");
    let mut first = true;
    for (name, value) in bindings {
        if !first {
            rendered.push_str(", ");
        }
        first = false;
        rendered.push_str(&format!(
            "StaticHeaderDescriptor {{ name: \"{}\", value: \"{}\" }}",
            escape_rust_string(name),
            escape_rust_string(&value.const_value)
        ));
    }
    rendered.push(']');
    rendered
}

fn emit_connector_determinism_hints(resources: &[ResourceRequirement]) -> String {
    let mut hints = Vec::new();
    for resource in resources {
        let hint = match resource {
            ResourceRequirement::HttpRead | ResourceRequirement::HttpWrite => {
                "capabilities::http::HINT_HTTP"
            }
        };
        if !hints.contains(&hint) {
            hints.push(hint);
        }
    }
    if hints.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", hints.join(", "))
    }
}

fn emit_connector_effect_hints(resources: &[ResourceRequirement]) -> String {
    let mut hints = Vec::new();
    for resource in resources {
        let hint = match resource {
            ResourceRequirement::HttpRead => "capabilities::http::HINT_HTTP_READ",
            ResourceRequirement::HttpWrite => "capabilities::http::HINT_HTTP_WRITE",
        };
        if !hints.contains(&hint) {
            hints.push(hint);
        }
    }
    if hints.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", hints.join(", "))
    }
}

fn emit_connector_role_requirements(
    manifest: &ConnectorManifest,
    action: &ActionSurface,
) -> String {
    let mut roles = Vec::new();
    roles.push(format!(
        "::dag_core::ConnectorRoleRequirement {{ kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile, name: \"{}\", expected_handle_kind: \"endpoint.profile\" }}",
        escape_rust_string(&action.endpoint)
    ));

    if let Some(auth_name) = &action.auth {
        let profile = manifest
            .profiles
            .outbound_auth
            .get(auth_name)
            .expect("validated manifest outbound auth profile");
        roles.push(format!(
            "::dag_core::ConnectorRoleRequirement {{ kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth, name: \"{}\", expected_handle_kind: \"{}\" }}",
            escape_rust_string(auth_name),
            escape_rust_string(profile.handle_kind())
        ));
    }

    format!("[{}]", roles.join(", "))
}

fn field_attributes(type_name: &str, field_name: &str, field: &FieldDecl) -> Vec<String> {
    let mut attrs = Vec::new();
    attrs.push(format!(
        "#[serde(rename = \"{}\")]",
        escape_rust_string(field_name)
    ));
    if field.default.is_some() {
        attrs.push(format!(
            "#[serde(default = \"{}\")]",
            default_fn_name(type_name, field_name)
        ));
    }
    attrs
}

fn rust_type_expression(field: &FieldDecl) -> String {
    let inner = match field.kind {
        FieldKind::String => "String".to_string(),
        FieldKind::Bool => "bool".to_string(),
        FieldKind::U32 => "u32".to_string(),
        FieldKind::U64 => "u64".to_string(),
        FieldKind::I64 => "i64".to_string(),
        FieldKind::F64 => "f64".to_string(),
        FieldKind::Bytes => "Vec<u8>".to_string(),
        FieldKind::Json => "JsonValue".to_string(),
        FieldKind::ObjectRef | FieldKind::EnumRef => {
            rust_type_name(field.target.as_deref().unwrap_or("Unknown"))
        }
        FieldKind::List => format!(
            "Vec<{}>",
            rust_type_expression(field.item.as_deref().expect("list item"))
        ),
    };
    if field.optional {
        format!("Option<{inner}>")
    } else {
        inner
    }
}

fn rust_default_expression(default: &DefaultValue) -> String {
    match default {
        DefaultValue::Bool(value) => format!("{value}"),
        DefaultValue::U32(value) => format!("{value}"),
        DefaultValue::U64(value) => format!("{value}"),
        DefaultValue::I64(value) => format!("{value}"),
        DefaultValue::F64(value) => format!("{value}"),
        DefaultValue::String(value) => format!("\"{}\".to_string()", escape_rust_string(value)),
    }
}

fn default_fn_name(type_name: &str, field_name: &str) -> String {
    format!(
        "__default_{}_{}",
        to_snake_case(type_name),
        to_snake_case(field_name)
    )
}

fn rust_type_name(name: &str) -> String {
    to_pascal_case(name)
}

fn rust_field_ident(name: &str) -> String {
    escape_keyword(&to_snake_case(name))
}

fn rust_enum_variant(name: &str) -> String {
    to_pascal_case(name)
}

fn endpoint_const_name(profile_name: &str) -> String {
    format!("{}_ENDPOINT_PROFILE", to_upper_snake_case(profile_name))
}

fn auth_const_name(profile_name: &str) -> String {
    format!("{}_OUTBOUND_AUTH", to_upper_snake_case(profile_name))
}

fn endpoint_env_var(profile_name: &str) -> String {
    format!(
        "LATTICE_CONNECTOR_ENDPOINT_{}_BASE_URL",
        to_upper_snake_case(profile_name)
    )
}

fn auth_env_var(profile_name: &str) -> String {
    format!(
        "LATTICE_CONNECTOR_AUTH_{}",
        to_upper_snake_case(profile_name)
    )
}

fn rust_http_method_variant(method: &str) -> &'static str {
    match method {
        "GET" => "Get",
        "HEAD" => "Head",
        "POST" => "Post",
        "PUT" => "Put",
        "PATCH" => "Patch",
        "DELETE" => "Delete",
        _ => "Get",
    }
}

fn manifest_uses_json_field(manifest: &ConnectorManifest) -> bool {
    manifest.types.values().any(type_decl_uses_json_field)
}

fn type_decl_uses_json_field(type_decl: &TypeDecl) -> bool {
    match type_decl {
        TypeDecl::Object { fields } => fields.values().any(field_uses_json),
        TypeDecl::Enum { .. } => false,
    }
}

fn field_uses_json(field: &FieldDecl) -> bool {
    match field.kind {
        FieldKind::Json => true,
        FieldKind::List => field.item.as_deref().is_some_and(field_uses_json),
        _ => false,
    }
}

fn to_pascal_case(value: &str) -> String {
    let snake = to_snake_case(value);
    let mut out = String::new();
    for segment in snake.split('_').filter(|segment| !segment.is_empty()) {
        let mut chars = segment.chars();
        if let Some(first) = chars.next() {
            out.push(first.to_ascii_uppercase());
            for ch in chars {
                out.push(ch.to_ascii_lowercase());
            }
        }
    }
    if out.is_empty() {
        "Generated".to_string()
    } else {
        out
    }
}

fn to_snake_case(value: &str) -> String {
    let mut out = String::new();
    let mut prev_was_sep = false;
    for (index, ch) in value.chars().enumerate() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() && index > 0 && !prev_was_sep {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
            prev_was_sep = false;
        } else if !prev_was_sep && !out.is_empty() {
            out.push('_');
            prev_was_sep = true;
        }
    }
    out.trim_matches('_').to_string()
}

fn to_upper_snake_case(value: &str) -> String {
    to_snake_case(value).to_ascii_uppercase()
}

fn escape_keyword(value: &str) -> String {
    const KEYWORDS: &[&str] = &[
        "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn",
        "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
        "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
        "use", "where", "while", "async", "await", "dyn",
    ];
    if KEYWORDS.contains(&value) {
        format!("r#{value}")
    } else {
        value.to_string()
    }
}

fn escape_rust_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use connector_spec::ConnectorManifest;
    use std::collections::BTreeSet;
    use std::fs;
    use tempfile::tempdir;

    fn fixture_text() -> String {
        let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../connector-spec/tests/fixtures/github_issues.connector.yaml");
        fs::read_to_string(manifest_path).expect("fixture text")
    }

    #[test]
    fn generation_is_deterministic() {
        let text = fixture_text();
        let manifest = ConnectorManifest::from_yaml_str(&text).expect("manifest parses");
        let left = generate_files(&manifest, &text).expect("left generation");
        let right = generate_files(&manifest, &text).expect("right generation");
        assert_eq!(left, right);
    }

    #[test]
    fn generated_crate_writes_expected_action_files() {
        let text = fixture_text();
        let manifest = ConnectorManifest::from_yaml_str(&text).expect("manifest parses");
        let files = generate_files(&manifest, &text).expect("files generated");
        let paths = files
            .iter()
            .map(|file| file.relative_path.as_str())
            .collect::<BTreeSet<_>>();

        assert!(paths.contains("src/generated/ops/get.rs"));
        assert!(paths.contains("src/generated/ops/list.rs"));
        assert!(paths.contains("src/generated/ops/create.rs"));
        assert!(paths.contains("src/generated/actions/get.rs"));
        assert!(paths.contains("src/generated/actions/list.rs"));
        assert!(paths.contains("src/generated/actions/create.rs"));
        assert!(paths.contains("tests/contract.rs"));

        let dir = tempdir().expect("tempdir");
        write_generated_files(dir.path(), &files).expect("files written");
        assert!(dir.path().join("src/generated/types.rs").exists());
    }
}
