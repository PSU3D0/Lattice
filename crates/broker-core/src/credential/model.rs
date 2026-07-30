use crate::{BrokerError, canonical};
use jsonschema::{Draft, JSONSchema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::marker::PhantomData;

const CATALOGUE: &str =
    include_str!("../../../../impl-docs/spec/credential-plane-protocol.schema.json");

pub trait ModelTag {
    const SCHEMA: &'static str;
    const MAX_BYTES: usize = 1024 * 1024;
}

pub trait SchemaType: Sized {
    const SCHEMA: &'static str;
    const MAX_BYTES: usize;
    fn value(&self) -> &Value;
}

#[derive(Clone)]
pub struct ParsedV2<T> {
    pub view: T,
    canonical: canonical::CanonicalJson,
}

impl<T> ParsedV2<T> {
    pub fn canonical_bytes(&self) -> &[u8] {
        self.canonical.as_bytes()
    }

    pub fn content_hash(&self) -> String {
        self.canonical.sha256()
    }
}

pub fn parse<T: SchemaType + DeserializeOwned>(source: &[u8]) -> Result<ParsedV2<T>, BrokerError> {
    let canonical = canonical::canonicalize_bounded(source, T::MAX_BYTES)?;
    let view = serde_json::from_slice(canonical.as_bytes()).map_err(|_| BrokerError::Brk004)?;
    Ok(ParsedV2 { view, canonical })
}

pub struct PublicModel<T: ModelTag>(Value, PhantomData<T>);

impl<T: ModelTag> Clone for PublicModel<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}
impl<T: ModelTag> std::fmt::Debug for PublicModel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl<T: ModelTag> PartialEq for PublicModel<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<T: ModelTag> PublicModel<T> {
    pub fn as_value(&self) -> &Value {
        &self.0
    }
}
impl<T: ModelTag> SchemaType for PublicModel<T> {
    const SCHEMA: &'static str = T::SCHEMA;
    const MAX_BYTES: usize = T::MAX_BYTES;
    fn value(&self) -> &Value {
        &self.0
    }
}
impl<'de, T: ModelTag> Deserialize<'de> for PublicModel<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        validate(T::SCHEMA, &value).map_err(serde::de::Error::custom)?;
        Ok(Self(value, PhantomData))
    }
}
impl<T: ModelTag> Serialize for PublicModel<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

pub(crate) struct PrivateModel<T: ModelTag>(Value, PhantomData<T>);

impl<T: ModelTag> PrivateModel<T> {
    pub(crate) fn expose<R>(&self, f: impl FnOnce(&Value) -> R) -> R {
        f(&self.0)
    }
}
impl<T: ModelTag> SchemaType for PrivateModel<T> {
    const SCHEMA: &'static str = T::SCHEMA;
    const MAX_BYTES: usize = T::MAX_BYTES;
    fn value(&self) -> &Value {
        &self.0
    }
}
impl<'de, T: ModelTag> Deserialize<'de> for PrivateModel<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        validate(T::SCHEMA, &value).map_err(serde::de::Error::custom)?;
        Ok(Self(value, PhantomData))
    }
}
impl<T: ModelTag> Drop for PrivateModel<T> {
    fn drop(&mut self) {
        zeroize_json(&mut self.0);
    }
}

fn zeroize_json(value: &mut Value) {
    use zeroize::Zeroize;
    match value {
        Value::String(text) => text.zeroize(),
        Value::Array(values) => values.iter_mut().for_each(zeroize_json),
        Value::Object(values) => values.values_mut().for_each(zeroize_json),
        _ => {}
    }
}

pub(crate) fn validate(schema_name: &str, value: &Value) -> Result<(), BrokerError> {
    let maximum = if matches!(
        schema_name,
        "ExecutionGrant" | "NodeLease" | "InvocationReceipt" | "LS1InvocationReceipt"
    ) {
        64 * 1024
    } else {
        1024 * 1024
    };
    canonical::from_serde(value, maximum)?;
    validate_reference(&format!("#/$defs/{schema_name}"), value)?;
    let catalogue: Value = serde_json::from_str(CATALOGUE).map_err(|_| BrokerError::Brk401)?;
    let defs = catalogue.get("$defs").ok_or(BrokerError::Brk401)?;
    validate_annotations(value, defs, schema_name)?;
    validate_semantics(schema_name, value)
}

pub(crate) fn validate_reference(reference: &str, value: &Value) -> Result<(), BrokerError> {
    let catalogue: Value = serde_json::from_str(CATALOGUE).map_err(|_| BrokerError::Brk401)?;
    let defs = catalogue.get("$defs").cloned().ok_or(BrokerError::Brk401)?;
    let schema = if reference == "#" {
        catalogue
    } else {
        serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": defs,
            "$ref": reference
        })
    };
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema)
        .map_err(|_| BrokerError::Brk401)?;
    if compiled.validate(value).is_err() {
        return Err(BrokerError::Brk004);
    }
    Ok(())
}

fn validate_annotations(value: &Value, defs: &Value, name: &str) -> Result<(), BrokerError> {
    walk_annotations(value, defs.get(name).ok_or(BrokerError::Brk401)?, defs)?;
    if let Some(object) = value.as_object() {
        if let Some(version) = object.get("schema_version")
            && version != "0.2"
        {
            return Err(BrokerError::Brk002);
        }
        if let Some(version) = object.get("private_codec_version")
            && version != "0.2"
        {
            return Err(BrokerError::Brk002);
        }
        validate_critical(value, object)?;
    }
    Ok(())
}

fn walk_annotations(value: &Value, schema: &Value, defs: &Value) -> Result<(), BrokerError> {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let name = reference
            .strip_prefix("#/$defs/")
            .ok_or(BrokerError::Brk401)?;
        return walk_annotations(value, defs.get(name).ok_or(BrokerError::Brk401)?, defs);
    }
    if let Some(branches) = schema.get("oneOf").and_then(Value::as_array) {
        for branch in branches {
            if schema_matches(value, branch, defs) {
                walk_annotations(value, branch, defs)?;
                break;
            }
        }
    }
    if let (Some(items), Some(item_schema)) = (value.as_array(), schema.get("items")) {
        if schema.get("x-lattice-sorted").is_some() {
            let mut previous: Option<Vec<u8>> = None;
            for item in items {
                let bytes =
                    canonical::from_serde(item, canonical::MAX_OPERATION_BYTES)?.into_bytes();
                if previous.as_ref().is_some_and(|old| old >= &bytes) {
                    return Err(BrokerError::Brk004);
                }
                previous = Some(bytes);
            }
        }
        for item in items {
            walk_annotations(item, item_schema, defs)?;
        }
    }
    if let (Some(object), Some(properties)) = (
        value.as_object(),
        schema.get("properties").and_then(Value::as_object),
    ) {
        if object.contains_key("critical_fields") {
            validate_critical(value, object)?;
        }
        if object
            .get("schema_version")
            .is_some_and(|version| version != "0.2")
            || object
                .get("private_codec_version")
                .is_some_and(|version| version != "0.2")
        {
            return Err(BrokerError::Brk002);
        }
        for (key, item) in object {
            if let Some(item_schema) = properties.get(key) {
                walk_annotations(item, item_schema, defs)?;
            }
        }
    }
    for part in schema
        .get("allOf")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        walk_annotations(value, part, defs)?;
    }
    Ok(())
}

fn schema_matches(value: &Value, schema: &Value, defs: &Value) -> bool {
    let target = schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|r| r.strip_prefix("#/$defs/"))
        .and_then(|n| defs.get(n))
        .unwrap_or(schema);
    if let Some(constant) = target.get("const") {
        return constant == value;
    }
    if let Some(properties) = target.get("properties").and_then(Value::as_object) {
        let Some(object) = value.as_object() else {
            return false;
        };
        for key in ["kind", "phase", "status"] {
            if let Some(constant) = properties.get(key).and_then(|p| p.get("const")) {
                return object.get(key) == Some(constant);
            }
        }
    }
    match target.get("type").and_then(Value::as_str) {
        Some("object") => value.is_object(),
        Some("string") => value.is_string(),
        Some("integer") => value.is_i64() || value.is_u64(),
        Some("array") => value.is_array(),
        _ => true,
    }
}

fn validate_critical(root: &Value, object: &Map<String, Value>) -> Result<(), BrokerError> {
    let Some(fields) = object.get("critical_fields").and_then(Value::as_array) else {
        return Ok(());
    };
    let mut previous: Option<&str> = None;
    for field in fields {
        let pointer = field.as_str().ok_or(BrokerError::Brk003)?;
        if !pointer.starts_with('/')
            || invalid_pointer(pointer)
            || root.pointer(pointer).is_none()
            || previous.is_some_and(|old| old >= pointer)
            || pointer.starts_with("/extensions/")
        {
            return Err(BrokerError::Brk003);
        }
        previous = Some(pointer);
    }
    Ok(())
}

fn invalid_pointer(pointer: &str) -> bool {
    pointer.ends_with('~')
        || pointer
            .as_bytes()
            .windows(2)
            .any(|w| w[0] == b'~' && !matches!(w[1], b'0' | b'1'))
}

fn u64_field(object: &Map<String, Value>, field: &str) -> Result<u64, BrokerError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(BrokerError::Brk109)
}

fn validate_semantics(name: &str, value: &Value) -> Result<(), BrokerError> {
    let Some(o) = value.as_object() else {
        return Ok(());
    };
    match name {
        "RegistryDefinition" => {
            if o.get("class") != o.get("class_payload").and_then(|v| v.get("kind")) {
                return Err(BrokerError::Brk004);
            }
        }
        "ConnectionSnapshot" => validate_snapshot(o)?,
        "RotationRecord" => validate_rotation(o, None, None)?,
        "NodeLease" => {
            if o.get("audience").and_then(Value::as_str) != Some("broker-grant-derivation") {
                return Err(BrokerError::Brk107);
            }
            if u64_field(o, "first_activation_ordinal")? > u64_field(o, "last_activation_ordinal")?
                || u64_field(o, "max_logical_effects")? == 0
            {
                return Err(BrokerError::Brk109);
            }
        }
        "ExecutionGrant" => {
            if o.get("audience").and_then(Value::as_str) != Some("broker-execution")
                || o.get("grant_scope")
                    .and_then(|v| v.get("kind"))
                    .and_then(Value::as_str)
                    != Some("logical_effect")
            {
                return Err(BrokerError::Brk107);
            }
            if o.get("canonical_input_commitment")
                != o.get("derivation_evidence")
                    .and_then(|v| v.get("canonical_input_commitment"))
            {
                return Err(BrokerError::Brk203);
            }
            if let Some(d) = o.get("derivation_evidence").and_then(Value::as_object)
                && d.get("kind").and_then(Value::as_str) == Some("node_lease")
                && u64_field(d, "parent_budget_before")?
                    != u64_field(d, "parent_budget_after")?
                        .saturating_add(u64_field(d, "consumed_budget_unit")?)
            {
                return Err(BrokerError::Brk109);
            }
        }
        "InvocationReceipt" => validate_receipt(o)?,
        "CredentialLease" => {
            if u64_field(o, "use_limit")? == 0 {
                return Err(BrokerError::Brk109);
            }
        }
        "CrossVersionCredentialFence" => validate_fence(o)?,
        _ => {}
    }
    Ok(())
}

fn validate_snapshot(o: &Map<String, Value>) -> Result<(), BrokerError> {
    let authority = o
        .get("authority_view")
        .and_then(Value::as_object)
        .ok_or(BrokerError::Brk109)?;
    let epoch = u64_field(authority, "authority_epoch")?;
    let current = u64_field(o, "current_material_generation")?;
    if o.get("active_material")
        .and_then(|v| v.get("generation"))
        .and_then(Value::as_u64)
        != Some(current)
    {
        return Err(BrokerError::Brk109);
    }
    if let Some(rotation) = o
        .get("rotation")
        .filter(|v| !v.is_null())
        .and_then(Value::as_object)
    {
        validate_rotation(
            rotation,
            Some(epoch),
            o.get("authority_view_hash").and_then(Value::as_str),
        )?;
    }
    Ok(())
}

const ROTATION_PHASES: [&str; 9] = [
    "prepared",
    "provider_request_recorded",
    "provider_result_observed",
    "new_material_sealed",
    "authority_reconciled",
    "switched",
    "retirement_enqueued",
    "old_material_destroyed",
    "complete",
];
const ROTATION_FIELDS: [&str; 8] = [
    "provider_request_record_hash",
    "provider_result_commitment",
    "sealed_envelope_hash",
    "reconciliation_record_hash",
    "switch_record_hash",
    "retirement_outbox_hash",
    "destruction_evidence_hash",
    "completion_record_hash",
];

fn validate_rotation(
    o: &Map<String, Value>,
    epoch: Option<u64>,
    view_hash: Option<&str>,
) -> Result<(), BrokerError> {
    let phase = o
        .get("phase")
        .and_then(Value::as_str)
        .ok_or(BrokerError::Brk109)?;
    let index = ROTATION_PHASES
        .iter()
        .position(|p| *p == phase)
        .ok_or(BrokerError::Brk004)?;
    if u64_field(o, "new_generation")?
        != u64_field(o, "old_generation")?
            .checked_add(1)
            .ok_or(BrokerError::Brk109)?
    {
        return Err(BrokerError::Brk109);
    }
    if epoch.is_some_and(|e| o.get("expected_authority_epoch").and_then(Value::as_u64) != Some(e))
        || view_hash
            .is_some_and(|h| o.get("authority_view_hash").and_then(Value::as_str) != Some(h))
    {
        return Err(BrokerError::Brk106);
    }
    for (position, field) in ROTATION_FIELDS.iter().enumerate() {
        if o.contains_key(*field) != (position < index) {
            return Err(BrokerError::Brk109);
        }
    }
    Ok(())
}

fn validate_receipt(o: &Map<String, Value>) -> Result<(), BrokerError> {
    let attempt = u64_field(o, "dispatch_attempt")?;
    let stage = o.get("pre_dispatch_stage");
    let lease_kind = o
        .get("leased_material_generation")
        .and_then(|v| v.get("kind"))
        .and_then(Value::as_str);
    if attempt == 0 {
        let stage = stage.and_then(Value::as_str).ok_or(BrokerError::Brk109)?;
        if lease_kind != Some("not_leased") {
            return Err(BrokerError::Brk109);
        }
        let reason = match stage {
            "pre_planning" => "pre_planning_rejection",
            "post_planning_pre_dispatch" => "no_provider_response",
            _ => return Err(BrokerError::Brk109),
        };
        for field in ["response_firewall_evidence_hash", "response_commitment"] {
            if o.get(field)
                .and_then(|v| v.get("reason"))
                .and_then(Value::as_str)
                != Some(reason)
            {
                return Err(BrokerError::Brk109);
            }
        }
        if stage == "pre_planning" {
            for field in ["request_plan_hash", "authority_facts_hash"] {
                if o.get(field)
                    .and_then(|v| v.get("reason"))
                    .and_then(Value::as_str)
                    != Some(reason)
                {
                    return Err(BrokerError::Brk109);
                }
            }
        }
    } else if attempt > 255 || stage.is_some() || lease_kind != Some("leased") {
        return Err(BrokerError::Brk109);
    }
    Ok(())
}

fn validate_fence(o: &Map<String, Value>) -> Result<(), BrokerError> {
    let phase = o
        .get("phase")
        .and_then(Value::as_str)
        .ok_or(BrokerError::Brk109)?;
    let disabled = o
        .get("v1_leasing_disabled")
        .and_then(Value::as_bool)
        .ok_or(BrokerError::Brk109)?;
    let ever = o
        .get("v2_lease_ever_issued")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || o.get("v2_rotation_ever_started")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    if (phase == "v2_authoritative") != disabled || (ever && phase != "v2_authoritative") {
        return Err(BrokerError::Brk106);
    }
    Ok(())
}
