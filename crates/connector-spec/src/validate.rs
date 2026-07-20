use std::collections::{BTreeMap, BTreeSet};

use crate::diagnostics::{ValidationCode, ValidationError, ValidationErrors};
use crate::model::{
    ActionImplementation, ActionSurface, BrokerRequestPlan, ConnectorManifest, DefaultValue,
    FieldDecl, FieldKind, OperationContract, PaginationDecl, QueryValueDecl, RequestMapping,
    ResourceRequirement, SurfaceDecl, TrustedAdapterPin, TypeDecl,
};

/// Validate the closed, schema-independent security grammar of a generated
/// dispatch descriptor before a host registry loads it.
pub fn validate_broker_dispatch_descriptor(descriptor: &crate::BrokerDispatchDescriptor) -> bool {
    let contract = &descriptor.contract;
    let request = &descriptor.request_plan;
    if !valid_contract_id(&contract.contract_id)
        || contract.broker_abi_version != "0.1"
        || contract.semantic_effect_slots.is_empty()
        || contract.semantic_effect_slots.len() > 1024
        || !is_sorted_unique_ascii(&contract.semantic_effect_slots, true, 128)
        || contract.minimum_scopes.len() > 1024
        || !is_sorted_unique_ascii(&contract.minimum_scopes, false, 1024)
        || descriptor.response_data_policy != contract.response_data_policy
        || descriptor.response_data_policy.max_bytes == 0
        || descriptor.response_data_policy.max_bytes > 256 * 1024
        || !valid_https_origin(&request.origin)
        || validate_broker_path(&request.path_template).is_err()
        || request.placeholders.len() > 256
        || request.query.len() > 256
        || request.static_headers.len() > 128
        || request.body.len() > 1024
        || crate::request_plan_hash(request).ok().as_deref()
            != Some(descriptor.request_plan_hash.as_str())
        || !request.query.iter().all(|(name, declaration)| {
            valid_query_name(name) && valid_query_value(declaration, None)
        })
        || request
            .trusted_adapter
            .as_ref()
            .is_some_and(|pin| !valid_trusted_adapter_pin(pin))
    {
        return false;
    }
    let Ok(placeholders) = extract_placeholders(&request.path_template) else {
        return false;
    };
    if placeholders.len() != request.placeholders.len() {
        return false;
    }
    request.placeholders.iter().all(|(name, declaration)| {
        valid_placeholder_name(name)
            && placeholders.contains(name)
            && match declaration.kind.as_str() {
                "input" => declaration
                    .input_field
                    .as_deref()
                    .is_some_and(|field| !field.is_empty() && field.len() <= 256),
                "idempotency_key" | "timestamp" | "boundary" => declaration.input_field.is_none(),
                _ => false,
            }
    }) && request
        .static_headers
        .iter()
        .all(|(name, value)| valid_static_header(name, value))
}

pub fn validate_manifest(manifest: &ConnectorManifest) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrors::new();

    if manifest.types.len() > 1024 || manifest.surfaces.len() > 1024 {
        errors.push(ValidationError::new(
            ValidationCode::InvalidContractSemantics,
            None,
            "connector manifest exceeds documented descriptor count limits",
        ));
    }

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
    let mut seen_contract_ids = BTreeSet::new();
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
            if let Some(contract) = &action.contract {
                if !seen_contract_ids.insert(contract.contract_id.clone()) {
                    errors.push(ValidationError::new(
                        ValidationCode::DuplicateContractId,
                        Some(format!("{surface_path}.contract.contract_id")),
                        format!("duplicate contract id `{}`", contract.contract_id),
                    ));
                }
            }
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

    match (&action.contract, &action.broker_request) {
        (Some(contract), Some(request)) => {
            validate_operation_contract(contract, action, manifest, surface_path, errors);
            validate_broker_request_plan(request, input_fields, surface_path, errors);
        }
        (Some(_), None) => errors.push(ValidationError::new(
            ValidationCode::InvalidBrokerRequestPlan,
            Some(format!("{surface_path}.broker_request")),
            "contracted actions must declare a broker_request plan",
        )),
        (None, Some(_)) => errors.push(ValidationError::new(
            ValidationCode::InvalidBrokerRequestPlan,
            Some(format!("{surface_path}.broker_request")),
            "broker_request requires an operation contract",
        )),
        (None, None) => {}
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

fn validate_operation_contract(
    contract: &OperationContract,
    action: &ActionSurface,
    manifest: &ConnectorManifest,
    surface_path: &str,
    errors: &mut ValidationErrors,
) {
    let path = format!("{surface_path}.contract");
    if !valid_contract_id(&contract.contract_id)
        || contract.contract_id.split('@').next() != Some(action.identifier.as_str())
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidContractId,
            Some(format!("{path}.contract_id")),
            "contract id must be `<operation_identifier>@<positive-major>` using lowercase ASCII segments",
        ));
    }
    if contract.broker_abi_version != "0.1" {
        errors.push(ValidationError::new(
            ValidationCode::InvalidBrokerAbiVersion,
            Some(format!("{path}.broker_abi_version")),
            "Broker V1 ABI version must be exactly `0.1`",
        ));
    }
    if contract.effect_class != action.effects {
        errors.push(ValidationError::new(
            ValidationCode::InvalidContractSemantics,
            Some(format!("{path}.effect_class")),
            "contract effect class must equal the action effect class",
        ));
    }

    let role_name = contract.auth_role.strip_prefix("outbound_auth.");
    if role_name.is_none()
        || role_name != action.auth.as_deref()
        || role_name.is_some_and(|name| !manifest.profiles.outbound_auth.contains_key(name))
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidContractSemantics,
            Some(format!("{path}.auth_role")),
            "auth_role must name the action's declared `outbound_auth.<profile>` role",
        ));
    }

    if contract.minimum_scopes.len() > 1024
        || !is_sorted_unique_ascii(&contract.minimum_scopes, false, 1024)
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidContractSemantics,
            Some(format!("{path}.minimum_scopes")),
            "minimum scopes must be sorted, unique, non-empty ASCII strings",
        ));
    }
    if contract.semantic_effect_slots.len() > 1024
        || !is_sorted_unique_ascii(&contract.semantic_effect_slots, true, 128)
        || !contract.semantic_effect_slots.iter().all(|slot| {
            slot.bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidSemanticEffectSlots,
            Some(format!("{path}.semantic_effect_slots")),
            "semantic effect slots must be a non-empty sorted unique array of 1-128 byte ASCII identifiers",
        ));
    }

    let policy = &contract.response_data_policy;
    let fields_exist = manifest
        .type_decl(&action.output)
        .and_then(TypeDecl::as_object_fields)
        .is_some_and(|output_fields| {
            policy
                .fields
                .iter()
                .all(|field| output_fields.contains_key(field))
        });
    if policy.max_bytes == 0
        || policy.max_bytes > 256 * 1024
        || policy.fields.len() > 1024
        || !is_sorted_unique_ascii(&policy.fields, true, 256)
        || !fields_exist
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidContractSemantics,
            Some(format!("{path}.response_data_policy")),
            "response projection fields must be sorted unique declared output fields and max_bytes must be 1..=262144",
        ));
    }
}

fn validate_broker_request_plan(
    request: &BrokerRequestPlan,
    input_fields: &BTreeMap<String, FieldDecl>,
    surface_path: &str,
    errors: &mut ValidationErrors,
) {
    let path = format!("{surface_path}.broker_request");
    if request.path_template.len() > 8192
        || request.placeholders.len() > 256
        || request.query.len() > 256
        || request.static_headers.len() > 128
        || request.body.len() > 1024
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidBrokerRequestPlan,
            Some(path.clone()),
            "broker request plan exceeds its documented size or count limits",
        ));
    }
    if !valid_https_origin(&request.origin) {
        errors.push(ValidationError::new(
            ValidationCode::InvalidBrokerRequestOrigin,
            Some(format!("{path}.origin")),
            "origin must be an HTTPS origin without path, query, fragment, userinfo, or wildcard",
        ));
    }
    if let Err(message) = validate_broker_path(&request.path_template) {
        errors.push(ValidationError::new(
            ValidationCode::InvalidBrokerRequestPlan,
            Some(format!("{path}.path_template")),
            message,
        ));
    }

    let placeholders = match extract_placeholders(&request.path_template) {
        Ok(placeholders) => placeholders,
        Err(message) => {
            errors.push(ValidationError::new(
                ValidationCode::InvalidBrokerRequestPlan,
                Some(format!("{path}.path_template")),
                message,
            ));
            Vec::new()
        }
    };
    for name in &placeholders {
        if !request.placeholders.contains_key(name) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidBrokerRequestPlan,
                Some(format!("{path}.placeholders")),
                format!("path placeholder `{{{name}}}` has no typed declaration"),
            ));
        }
    }
    for (name, placeholder) in &request.placeholders {
        if !valid_placeholder_name(name) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidBrokerRequestPlan,
                Some(format!("{path}.placeholders.{name}")),
                "placeholder names must match `[a-z][a-z0-9_]{0,63}`",
            ));
        }
        if !placeholders.contains(name) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidBrokerRequestPlan,
                Some(format!("{path}.placeholders.{name}")),
                "placeholder declaration is not used by the path template",
            ));
        }
        match placeholder.kind.as_str() {
            "input" => match placeholder.input_field.as_deref() {
                Some(field) => ensure_input_field_exists(
                    input_fields,
                    field,
                    format!("{path}.placeholders.{name}.input_field"),
                    errors,
                ),
                None => errors.push(ValidationError::new(
                    ValidationCode::InvalidInputFieldReference,
                    Some(format!("{path}.placeholders.{name}.input_field")),
                    "input placeholder must declare input_field",
                )),
            },
            "idempotency_key" | "timestamp" | "boundary" => {
                if placeholder.input_field.is_some() {
                    errors.push(ValidationError::new(
                        ValidationCode::InvalidBrokerRequestPlan,
                        Some(format!("{path}.placeholders.{name}.input_field")),
                        "broker-filled placeholders must not declare input_field",
                    ));
                }
            }
            _ => errors.push(ValidationError::new(
                ValidationCode::UnknownPlaceholderKind,
                Some(format!("{path}.placeholders.{name}.kind")),
                "placeholder kind is not in the closed Broker V1 vocabulary",
            )),
        }
    }
    for (name, declaration) in &request.query {
        if !valid_query_name(name) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidBrokerRequestPlan,
                Some(format!("{path}.query.{name}")),
                "query names must be non-empty printable ASCII without separators",
            ));
        }
        if !valid_query_value(declaration, Some(input_fields)) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidBrokerRequestPlan,
                Some(format!("{path}.query.{name}")),
                "query values must be a valid static, input, or broker-slot declaration",
            ));
        }
    }
    if request
        .trusted_adapter
        .as_ref()
        .is_some_and(|pin| !valid_trusted_adapter_pin(pin))
    {
        errors.push(ValidationError::new(
            ValidationCode::InvalidBrokerRequestPlan,
            Some(format!("{path}.trusted_adapter")),
            "trusted adapter pin is not in the closed Broker V1 registry",
        ));
    }
    for (name, value) in &request.static_headers {
        if !valid_static_header(name, value) {
            errors.push(ValidationError::new(
                ValidationCode::InvalidBrokerRequestPlan,
                Some(format!("{path}.static_headers.{name}")),
                "static header name/value is invalid or security-sensitive",
            ));
        }
    }
    validate_field_mapping(input_fields, &request.body, format!("{path}.body"), errors);
}

fn valid_contract_id(value: &str) -> bool {
    let Some((name, major)) = value.rsplit_once('@') else {
        return false;
    };
    if major.is_empty()
        || major.starts_with('0')
        || !major.bytes().all(|byte| byte.is_ascii_digit())
    {
        return false;
    }
    let segments = name.split('.').collect::<Vec<_>>();
    segments.len() >= 2
        && segments.iter().all(|segment| {
            segment
                .as_bytes()
                .first()
                .is_some_and(|byte| byte.is_ascii_lowercase())
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        })
}

fn is_sorted_unique_ascii(values: &[String], require_non_empty: bool, max_len: usize) -> bool {
    if require_non_empty && values.is_empty() {
        return false;
    }
    values
        .iter()
        .all(|value| !value.is_empty() && value.len() <= max_len && value.is_ascii())
        && values.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_https_origin(origin: &str) -> bool {
    let Some(authority) = origin.strip_prefix("https://") else {
        return false;
    };
    if authority.is_empty()
        || !authority.is_ascii()
        || authority.bytes().any(|byte| byte.is_ascii_whitespace())
        || authority.contains(['/', '?', '#', '@', '*', '\\'])
    {
        return false;
    }

    let (host, port) = if authority.starts_with('[') {
        let Some(close) = authority.find(']') else {
            return false;
        };
        let literal = &authority[1..close];
        if literal.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        let suffix = &authority[close + 1..];
        let port = if suffix.is_empty() {
            None
        } else {
            let Some(port) = suffix.strip_prefix(':') else {
                return false;
            };
            Some(port)
        };
        (None, port)
    } else {
        let (host, port) = authority
            .rsplit_once(':')
            .map_or((authority, None), |(host, port)| (host, Some(port)));
        if host.is_empty()
            || host.len() > 253
            || host.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        {
            return false;
        }
        (Some(host), port)
    };

    (host.is_some() || authority.starts_with('['))
        && port.is_none_or(|port| {
            !port.is_empty()
                && port.bytes().all(|byte| byte.is_ascii_digit())
                && port.parse::<u16>().is_ok_and(|port| port != 0)
        })
}

fn validate_broker_path(path: &str) -> Result<(), &'static str> {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains(['?', '#', '\\'])
        || path.bytes().any(|byte| byte.is_ascii_control())
        || contains_percent_encoded(path, b'\\')
        || contains_percent_encoded(path, b'/')
    {
        return Err(
            "broker path must be an absolute path without authority, query, fragment, backslash, or controls",
        );
    }
    for segment in path.split('/') {
        let dots = decode_percent_dots(segment);
        if segment == "." || segment == ".." || dots == "." || dots == ".." {
            return Err("broker path must not contain raw or percent-encoded dot segments");
        }
        if segment.contains('{') || segment.contains('}') {
            let placeholder = segment.strip_suffix(":append").unwrap_or(segment);
            let Some(name) = placeholder
                .strip_prefix('{')
                .and_then(|value| value.strip_suffix('}'))
            else {
                return Err(
                    "broker placeholders must occupy a path segment, optionally with the closed `:append` suffix",
                );
            };
            if !valid_placeholder_name(name) || segment.contains('%') {
                return Err("broker placeholder name or encoding is invalid");
            }
        }
    }
    Ok(())
}

fn contains_percent_encoded(value: &str, expected: u8) -> bool {
    value.as_bytes().windows(3).any(|window| {
        window[0] == b'%'
            && std::str::from_utf8(&window[1..])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                == Some(expected)
    })
}

fn decode_percent_dots(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = String::new();
    let mut index = 0;
    while index < bytes.len() {
        if index + 2 < bytes.len() && bytes[index] == b'%' {
            if let Ok(hex) = std::str::from_utf8(&bytes[index + 1..index + 3]) {
                if u8::from_str_radix(hex, 16).ok() == Some(b'.') {
                    out.push('.');
                    index += 3;
                    continue;
                }
            }
        }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
}

fn valid_placeholder_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_query_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_query_value(
    declaration: &QueryValueDecl,
    input_fields: Option<&BTreeMap<String, FieldDecl>>,
) -> bool {
    match declaration.kind.as_str() {
        "static" => {
            declaration.input_field.is_none()
                && declaration.value.as_ref().is_some_and(|value| {
                    !value.is_empty()
                        && value.len() <= 8192
                        && value.is_ascii()
                        && !value.bytes().any(|byte| byte.is_ascii_control())
                })
        }
        "input" => {
            declaration.value.is_none()
                && declaration.input_field.as_ref().is_some_and(|field| {
                    !field.is_empty()
                        && field.len() <= 256
                        && input_fields.is_none_or(|fields| fields.contains_key(field))
                })
        }
        "idempotency_key" | "timestamp" | "boundary" => {
            declaration.input_field.is_none() && declaration.value.is_none()
        }
        _ => false,
    }
}

fn valid_trusted_adapter_pin(pin: &TrustedAdapterPin) -> bool {
    matches!(
        (
            pin.trusted_adapter_id.as_str(),
            pin.implementation_version.as_str(),
            pin.implementation_hash.as_str(),
        ),
        (
            "google.sheets.append_row.v1",
            "1",
            "sha256:54db6603967e1ce4e46ef45ece7bfa947c129564e40a290989fa2ae901a23966"
        ) | (
            "google.gmail.rfc822_message.v1",
            "1",
            "sha256:5a5de77f756b49aac0fb5339bf764f9e53a9437cdc41619c2c978aa5dbb4e3fc"
        )
    )
}

fn valid_static_header(name: &str, value: &str) -> bool {
    const FORBIDDEN: &[&str] = &[
        "authorization",
        "host",
        "cookie",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];
    !name.is_empty()
        && name.len() <= 128
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
        && !FORBIDDEN.contains(&name.to_ascii_lowercase().as_str())
        && value.len() <= 8192
        && !value.contains(['\r', '\n'])
        && value
            .bytes()
            .all(|byte| byte == b'\t' || (byte >= 0x20 && byte != 0x7f))
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
