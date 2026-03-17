use std::collections::{BTreeMap, BTreeSet};

use crate::diagnostics::{ValidationCode, ValidationError, ValidationErrors};
use crate::model::{
    ActionImplementation, ActionSurface, ConnectorManifest, DefaultValue, FieldDecl, FieldKind,
    PaginationDecl, RequestMapping, ResourceRequirement, SurfaceDecl, TypeDecl,
};

pub fn validate_manifest(manifest: &ConnectorManifest) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrors::new();

    if manifest.connector.id.trim().is_empty() {
        errors.push(ValidationError::new(
            ValidationCode::InvalidTypeReference,
            Some("connector.id".to_string()),
            "connector id must not be empty",
        ));
    }

    if manifest.connector.crate_name.trim().is_empty() {
        errors.push(ValidationError::new(
            ValidationCode::InvalidTypeReference,
            Some("connector.crate".to_string()),
            "connector crate must not be empty",
        ));
    }

    for (type_name, decl) in &manifest.types {
        validate_type_decl(type_name, decl, manifest, &mut errors);
    }

    let mut seen_identifiers = BTreeSet::new();
    let mut seen_modules = BTreeSet::new();
    for (index, surface) in manifest.surfaces.iter().enumerate() {
        let surface_path = format!("surfaces[{index}]");
        if !seen_identifiers.insert(surface.identifier().to_string()) {
            errors.push(ValidationError::new(
                ValidationCode::DuplicateSurfaceIdentifier,
                Some(format!("{surface_path}.identifier")),
                format!("duplicate surface identifier `{}`", surface.identifier()),
            ));
        }

        let module_name = generated_module_name(surface.identifier());
        if !seen_modules.insert(module_name.clone()) {
            errors.push(ValidationError::new(
                ValidationCode::DuplicateGeneratedModuleName,
                Some(format!("{surface_path}.identifier")),
                format!(
                    "surface `{}` collides on generated module name `{module_name}`",
                    surface.identifier()
                ),
            ));
        }

        if let SurfaceDecl::Action(action) = surface {
            validate_action_surface(action, &surface_path, manifest, &mut errors);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn validate_manifest_for_codegen(manifest: &ConnectorManifest) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrors::new();
    if let Err(existing) = validate_manifest(manifest) {
        errors.extend(existing);
    }

    for (index, surface) in manifest.surfaces.iter().enumerate() {
        let surface_path = format!("surfaces[{index}]");
        match surface {
            SurfaceDecl::Action(action) => {
                if action.implementation != ActionImplementation::RequestMapped {
                    errors.push(ValidationError::new(
                        ValidationCode::UnsupportedActionImplementation,
                        Some(format!("{surface_path}.implementation")),
                        format!(
                            "action `{}` uses implementation `{:?}` which is not Phase-B codegen compatible",
                            action.identifier, action.implementation
                        ),
                    ));
                }

                if let Some(auth_name) = &action.auth {
                    if let Some(profile) = manifest.profiles.outbound_auth.get(auth_name) {
                        if !profile.supports_codegen() {
                            errors.push(ValidationError::new(
                                ValidationCode::UnsupportedOutboundAuthKind,
                                Some(format!("{surface_path}.auth")),
                                format!(
                                    "outbound auth profile `{auth_name}` uses unsupported Phase-B kind `{}`",
                                    profile.kind_name()
                                ),
                            ));
                        }
                    }
                }

                if action.pagination.is_some()
                    && paginated_collection_field(manifest, &action.output).is_none()
                {
                    errors.push(ValidationError::new(
                        ValidationCode::UnsupportedPaginatedOutputShape,
                        Some(format!("{surface_path}.output")),
                        format!(
                            "paginated action `{}` requires an object output with exactly one list field",
                            action.identifier
                        ),
                    ));
                }
            }
            other => {
                errors.push(ValidationError::new(
                    ValidationCode::UnsupportedSurfaceKind,
                    Some(format!("{surface_path}.kind")),
                    format!(
                        "surface kind `{}` is reserved in Phase B but not yet runnable",
                        other.kind_name()
                    ),
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn generated_module_name(identifier: &str) -> String {
    identifier
        .split('.')
        .last()
        .unwrap_or(identifier)
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

pub fn paginated_collection_field<'a>(
    manifest: &'a ConnectorManifest,
    output: &str,
) -> Option<&'a str> {
    let fields = manifest.type_decl(output)?.as_object_fields()?;
    if fields.len() != 1 {
        return None;
    }
    let (field_name, field_decl) = fields.iter().next()?;
    if field_decl.kind == FieldKind::List {
        Some(field_name.as_str())
    } else {
        None
    }
}

fn validate_type_decl(
    type_name: &str,
    decl: &TypeDecl,
    manifest: &ConnectorManifest,
    errors: &mut ValidationErrors,
) {
    match decl {
        TypeDecl::Object { fields } => {
            for (field_name, field_decl) in fields {
                validate_field_decl(
                    field_decl,
                    manifest,
                    format!("types.{type_name}.fields.{field_name}"),
                    errors,
                );
            }
        }
        TypeDecl::Enum { variants } => {
            if variants.is_empty() {
                errors.push(ValidationError::new(
                    ValidationCode::InvalidTypeReference,
                    Some(format!("types.{type_name}.variants")),
                    format!("enum type `{type_name}` must declare at least one variant"),
                ));
            }
            let mut seen = BTreeSet::new();
            for (index, variant) in variants.iter().enumerate() {
                if !seen.insert(variant.clone()) {
                    errors.push(ValidationError::new(
                        ValidationCode::InvalidTypeReference,
                        Some(format!("types.{type_name}.variants[{index}]")),
                        format!("enum type `{type_name}` repeats variant `{variant}`"),
                    ));
                }
            }
        }
    }
}

fn validate_field_decl(
    field: &FieldDecl,
    manifest: &ConnectorManifest,
    path: String,
    errors: &mut ValidationErrors,
) {
    match field.kind {
        FieldKind::ObjectRef => match field.target.as_deref() {
            Some(target) => match manifest.type_decl(target) {
                Some(TypeDecl::Object { .. }) => {}
                Some(TypeDecl::Enum { .. }) => errors.push(ValidationError::new(
                    ValidationCode::InvalidTypeReference,
                    Some(format!("{path}.target")),
                    format!("object_ref target `{target}` must refer to an object type"),
                )),
                None => errors.push(ValidationError::new(
                    ValidationCode::InvalidTypeReference,
                    Some(format!("{path}.target")),
                    format!("unknown object_ref target `{target}`"),
                )),
            },
            None => errors.push(ValidationError::new(
                ValidationCode::InvalidTypeReference,
                Some(format!("{path}.target")),
                "object_ref fields must declare `target`",
            )),
        },
        FieldKind::EnumRef => match field.target.as_deref() {
            Some(target) => match manifest.type_decl(target) {
                Some(TypeDecl::Enum { .. }) => {}
                Some(TypeDecl::Object { .. }) => errors.push(ValidationError::new(
                    ValidationCode::InvalidTypeReference,
                    Some(format!("{path}.target")),
                    format!("enum_ref target `{target}` must refer to an enum type"),
                )),
                None => errors.push(ValidationError::new(
                    ValidationCode::InvalidTypeReference,
                    Some(format!("{path}.target")),
                    format!("unknown enum_ref target `{target}`"),
                )),
            },
            None => errors.push(ValidationError::new(
                ValidationCode::InvalidTypeReference,
                Some(format!("{path}.target")),
                "enum_ref fields must declare `target`",
            )),
        },
        FieldKind::List => match field.item.as_deref() {
            Some(item) => validate_field_decl(item, manifest, format!("{path}.item"), errors),
            None => errors.push(ValidationError::new(
                ValidationCode::InvalidTypeReference,
                Some(format!("{path}.item")),
                "list fields must declare `item`",
            )),
        },
        FieldKind::Json => {
            if field
                .escape_hatch_reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
            {
                errors.push(ValidationError::new(
                    ValidationCode::InvalidJsonEscapeHatch,
                    Some(path.clone()),
                    "json fields must declare `escape_hatch_reason`",
                ));
            }
        }
        FieldKind::String
        | FieldKind::Bool
        | FieldKind::U32
        | FieldKind::U64
        | FieldKind::I64
        | FieldKind::F64
        | FieldKind::Bytes => {}
    }

    if let Some(default) = &field.default {
        if !default_matches_field(default, field) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidTypeReference,
                Some(format!("{path}.default")),
                format!(
                    "default value is incompatible with field kind `{:?}`",
                    field.kind
                ),
            ));
        }
    }
}

fn default_matches_field(default: &DefaultValue, field: &FieldDecl) -> bool {
    matches!(
        (default, field.kind),
        (DefaultValue::Bool(_), FieldKind::Bool)
            | (DefaultValue::U32(_), FieldKind::U32)
            | (DefaultValue::U64(_), FieldKind::U64)
            | (DefaultValue::I64(_), FieldKind::I64)
            | (DefaultValue::F64(_), FieldKind::F64)
            | (DefaultValue::String(_), FieldKind::String)
    )
}

fn validate_action_surface(
    action: &ActionSurface,
    surface_path: &str,
    manifest: &ConnectorManifest,
    errors: &mut ValidationErrors,
) {
    let input_decl = match manifest.type_decl(&action.input) {
        Some(decl) => decl,
        None => {
            errors.push(ValidationError::new(
                ValidationCode::InvalidTypeReference,
                Some(format!("{surface_path}.input")),
                format!("unknown input type `{}`", action.input),
            ));
            return;
        }
    };

    let output_decl = match manifest.type_decl(&action.output) {
        Some(decl) => decl,
        None => {
            errors.push(ValidationError::new(
                ValidationCode::InvalidTypeReference,
                Some(format!("{surface_path}.output")),
                format!("unknown output type `{}`", action.output),
            ));
            return;
        }
    };

    let input_fields = match input_decl.as_object_fields() {
        Some(fields) => fields,
        None => {
            errors.push(ValidationError::new(
                ValidationCode::InvalidTypeReference,
                Some(format!("{surface_path}.input")),
                format!("action input type `{}` must be an object", action.input),
            ));
            return;
        }
    };

    if manifest
        .profiles
        .endpoint_profiles
        .get(&action.endpoint)
        .is_none()
    {
        errors.push(ValidationError::new(
            ValidationCode::UnknownEndpointProfile,
            Some(format!("{surface_path}.endpoint")),
            format!("unknown endpoint profile `{}`", action.endpoint),
        ));
    }

    if let Some(auth_name) = &action.auth {
        if manifest.profiles.outbound_auth.get(auth_name).is_none() {
            errors.push(ValidationError::new(
                ValidationCode::UnknownOutboundAuthProfile,
                Some(format!("{surface_path}.auth")),
                format!("unknown outbound auth profile `{auth_name}`"),
            ));
        }
    }

    match action.implementation {
        ActionImplementation::RequestMapped => {
            let request = match action.request() {
                Some(request) => request,
                None => {
                    errors.push(ValidationError::new(
                        ValidationCode::InvalidTypeReference,
                        Some(format!("{surface_path}.request")),
                        "request-mapped actions must declare `request`",
                    ));
                    validate_resource_envelope(action, surface_path, errors);
                    return;
                }
            };

            validate_request_mapping(request, input_fields, surface_path, errors);
            validate_resources_for_request(action, request, surface_path, errors);

            if let Some(pagination) = &action.pagination {
                validate_pagination(pagination, input_fields, surface_path, errors);
            }
        }
        ActionImplementation::HandwrittenSemantic => {
            if let Some(request) = action.request() {
                validate_request_mapping(request, input_fields, surface_path, errors);
            }
            validate_resource_envelope(action, surface_path, errors);
            if action.pagination.is_some() {
                errors.push(ValidationError::new(
                    ValidationCode::UnsupportedPaginatedOutputShape,
                    Some(format!("{surface_path}.pagination")),
                    "handwritten semantic actions do not currently support manifest-level pagination declarations",
                ));
            }
        }
    }

    let response = action.response();
    if response.root_path.trim().is_empty() {
        errors.push(ValidationError::new(
            ValidationCode::InvalidTypeReference,
            Some(format!("{surface_path}.response.root_path")),
            "response root_path must not be empty",
        ));
    }

    if action.pagination.is_some() {
        match output_decl.as_object_fields() {
            Some(fields) if fields.len() == 1 => {
                let (_, only_field) = fields.iter().next().expect("single field checked");
                if only_field.kind != FieldKind::List {
                    errors.push(ValidationError::new(
                        ValidationCode::UnsupportedPaginatedOutputShape,
                        Some(format!("{surface_path}.output")),
                        format!(
                            "paginated output type `{}` must expose exactly one list field",
                            action.output
                        ),
                    ));
                }
            }
            _ => errors.push(ValidationError::new(
                ValidationCode::UnsupportedPaginatedOutputShape,
                Some(format!("{surface_path}.output")),
                format!(
                    "paginated output type `{}` must be an object with exactly one list field",
                    action.output
                ),
            )),
        }
    }
}

fn validate_field_mapping(
    input_fields: &BTreeMap<String, FieldDecl>,
    mapping: &BTreeMap<String, String>,
    path: String,
    errors: &mut ValidationErrors,
) {
    for (parameter_name, field_name) in mapping {
        ensure_input_field_exists(
            input_fields,
            field_name,
            format!("{path}.{parameter_name}"),
            errors,
        );
    }
}

fn validate_request_mapping(
    request: &RequestMapping,
    input_fields: &BTreeMap<String, FieldDecl>,
    surface_path: &str,
    errors: &mut ValidationErrors,
) {
    let placeholders = match extract_placeholders(&request.path_template) {
        Ok(placeholders) => placeholders,
        Err(message) => {
            errors.push(ValidationError::new(
                ValidationCode::InvalidPathTemplate,
                Some(format!("{surface_path}.request.path_template")),
                message,
            ));
            Vec::new()
        }
    };

    let path_params = &request.path_params;
    for placeholder in &placeholders {
        if !path_params.contains_key(placeholder) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidPathTemplate,
                Some(format!("{surface_path}.request.path_params")),
                format!(
                    "path template placeholder `{{{placeholder}}}` is not mapped in `path_params`"
                ),
            ));
        }
    }

    for (placeholder, field_name) in path_params {
        if !placeholders
            .iter()
            .any(|candidate| candidate == placeholder)
        {
            errors.push(ValidationError::new(
                ValidationCode::InvalidPathTemplate,
                Some(format!("{surface_path}.request.path_params.{placeholder}")),
                format!("path_params entry `{placeholder}` is not used by the path template"),
            ));
        }
        ensure_input_field_exists(
            input_fields,
            field_name,
            format!("{surface_path}.request.path_params.{placeholder}"),
            errors,
        );
    }

    validate_field_mapping(
        input_fields,
        &request.query,
        format!("{surface_path}.request.query"),
        errors,
    );
    validate_field_mapping(
        input_fields,
        &request.body,
        format!("{surface_path}.request.body"),
        errors,
    );
}

fn ensure_input_field_exists(
    input_fields: &BTreeMap<String, FieldDecl>,
    field_name: &str,
    path: String,
    errors: &mut ValidationErrors,
) {
    if !input_fields.contains_key(field_name) {
        errors.push(ValidationError::new(
            ValidationCode::InvalidInputFieldReference,
            Some(path),
            format!("references unknown input field `{field_name}`"),
        ));
    }
}

fn validate_resources_for_request(
    action: &ActionSurface,
    request: &RequestMapping,
    surface_path: &str,
    errors: &mut ValidationErrors,
) {
    let required = if request.method.requires_write() {
        ResourceRequirement::HttpWrite
    } else {
        ResourceRequirement::HttpRead
    };

    if !action
        .resources
        .iter()
        .any(|resource| *resource == required)
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidResourceContract,
            Some(format!("{surface_path}.resources")),
            format!(
                "request method `{}` requires resource `{}`",
                request.method.as_str(),
                required.manifest_value()
            ),
        ));
    }

    validate_resource_envelope(action, surface_path, errors);
}

fn validate_resource_envelope(
    action: &ActionSurface,
    surface_path: &str,
    errors: &mut ValidationErrors,
) {
    for resource in &action.resources {
        if !action
            .effects
            .as_dag_core()
            .is_at_least(resource.minimum_effects())
        {
            errors.push(ValidationError::new(
                ValidationCode::InvalidResourceContract,
                Some(format!("{surface_path}.effects")),
                format!(
                    "effects `{}` are weaker than resource `{}` requires",
                    action.effects.as_macro_name(),
                    resource.minimum_effects().as_str()
                ),
            ));
        }

        if !action
            .determinism
            .as_dag_core()
            .is_at_least(resource.minimum_determinism())
        {
            errors.push(ValidationError::new(
                ValidationCode::InvalidResourceContract,
                Some(format!("{surface_path}.determinism")),
                format!(
                    "determinism `{}` is stricter than resource `{}` allows",
                    action.determinism.as_macro_name(),
                    resource.minimum_determinism().as_str()
                ),
            ));
        }
    }
}

fn validate_pagination(
    pagination: &PaginationDecl,
    input_fields: &BTreeMap<String, FieldDecl>,
    surface_path: &str,
    errors: &mut ValidationErrors,
) {
    ensure_input_field_exists(
        input_fields,
        &pagination.enabled_from,
        format!("{surface_path}.pagination.enabled_from"),
        errors,
    );

    if let Some(field) = input_fields.get(&pagination.enabled_from) {
        if field.kind != FieldKind::Bool {
            errors.push(ValidationError::new(
                ValidationCode::InvalidInputFieldReference,
                Some(format!("{surface_path}.pagination.enabled_from")),
                format!(
                    "pagination enabled_from field `{}` must be bool",
                    pagination.enabled_from
                ),
            ));
        }
    }

    if let Some(max_items_from) = &pagination.max_items_from {
        ensure_input_field_exists(
            input_fields,
            max_items_from,
            format!("{surface_path}.pagination.max_items_from"),
            errors,
        );
        if let Some(field) = input_fields.get(max_items_from) {
            if !field.is_numeric() {
                errors.push(ValidationError::new(
                    ValidationCode::InvalidInputFieldReference,
                    Some(format!("{surface_path}.pagination.max_items_from")),
                    format!("pagination max_items_from field `{max_items_from}` must be numeric"),
                ));
            }
        }
    }
}

fn extract_placeholders(path_template: &str) -> Result<Vec<String>, String> {
    let mut placeholders = Vec::new();
    let mut current = String::new();
    let mut in_placeholder = false;
    for ch in path_template.chars() {
        match ch {
            '{' if in_placeholder => {
                return Err("nested `{` is not allowed in path templates".to_string());
            }
            '{' => {
                in_placeholder = true;
                current.clear();
            }
            '}' if !in_placeholder => return Err("unmatched `}` in path template".to_string()),
            '}' => {
                in_placeholder = false;
                if current.trim().is_empty() {
                    return Err("path template placeholders must not be empty".to_string());
                }
                placeholders.push(current.clone());
            }
            _ if in_placeholder => current.push(ch),
            _ => {}
        }
    }

    if in_placeholder {
        return Err("unterminated `{...}` placeholder in path template".to_string());
    }

    Ok(placeholders)
}

#[cfg(test)]
mod tests {
    use super::extract_placeholders;

    #[test]
    fn placeholder_parser_rejects_unclosed_marker() {
        let err = extract_placeholders("/repos/{owner").expect_err("must fail");
        assert!(err.contains("unterminated"));
    }
}
