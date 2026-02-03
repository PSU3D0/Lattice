use std::collections::BTreeMap;

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::Digest;

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("manifest JSON is not an object")]
    ManifestNotObject,
    #[error("manifest missing embedded custom section `{0}`")]
    MissingCustomSection(&'static str),
    #[error("manifest JSON parse error: {0}")]
    ManifestJson(#[from] serde_json::Error),
    #[error("manifest validation error: {0}")]
    ManifestValidation(String),
    #[error("manifest custom section is not valid UTF-8")]
    ManifestUtf8,
    #[error("wasm parse error: {0}")]
    WasmParse(String),
}

pub const BUNDLE_VERSION: &str = "0.1";
pub const DEFAULT_ABI_NAME: &str = "latticeflow.wit";
pub const DEFAULT_ABI_VERSION: &str = "0.1";
pub const MANIFEST_SECTION: &str = "latticeflow.bundle_manifest";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub bundle_version: String,
    pub abi: AbiRef,
    pub bundle_id: String,
    pub code: CodeDescriptor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactDescriptor>,
    pub flow: FlowDescriptor,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_flow_ir"
    )]
    pub flow_ir: Option<FlowIrRef>,
    #[serde(default)]
    pub entrypoints: Vec<Entrypoint>,
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeSpec>,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_subflows"
    )]
    pub subflows: Option<Subflows>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_signing"
    )]
    pub signing: Option<Signing>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbiRef {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeDescriptor {
    pub target: String,
    pub file: String,
    pub hash: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactDescriptor {
    pub target: String,
    pub file: String,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDescriptor {
    pub id: String,
    pub version: String,
    pub profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowIrRef {
    pub artifact: String,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entrypoint {
    pub trigger: String,
    pub capture: String,
    #[serde(default)]
    pub route_aliases: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_deadline_ms"
    )]
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: String,
    pub effects: NodeEffects,
    pub determinism: NodeDeterminism,
    pub durability: DurabilityProfile,
    #[serde(default)]
    pub bindings: Vec<String>,
    pub input_schema: String,
    pub output_schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityProfile {
    pub checkpointable: bool,
    pub replayable: bool,
    pub halts: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Capabilities {
    #[serde(default)]
    pub required: Vec<CapabilityBinding>,
    #[serde(default)]
    pub optional: Vec<CapabilityBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityBinding {
    pub name: String,
    pub kind: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_constraints"
    )]
    pub constraints: Option<JsonMap<String, JsonValue>>,
}

fn deserialize_constraints<'de, D>(
    deserializer: D,
) -> Result<Option<JsonMap<String, JsonValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = JsonValue::deserialize(deserializer)?;
    match value {
        JsonValue::Null => Err(serde::de::Error::custom("constraints cannot be null")),
        JsonValue::Object(map) => Ok(Some(map)),
        _ => Err(serde::de::Error::custom("constraints must be an object")),
    }
}

fn deserialize_non_null_option<'de, D, T>(
    deserializer: D,
    field: &'static str,
) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let value = JsonValue::deserialize(deserializer)?;
    match value {
        JsonValue::Null => Err(serde::de::Error::custom(format!("{field} cannot be null"))),
        other => serde_json::from_value(other)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

fn deserialize_flow_ir<'de, D>(deserializer: D) -> Result<Option<FlowIrRef>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = JsonValue::deserialize(deserializer)?;
    match value {
        JsonValue::Null => Err(serde::de::Error::custom("flow_ir cannot be null")),
        other => serde_json::from_value(other)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

fn deserialize_subflows<'de, D>(deserializer: D) -> Result<Option<Subflows>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_non_null_option(deserializer, "subflows")
}

fn deserialize_signing<'de, D>(deserializer: D) -> Result<Option<Signing>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_non_null_option(deserializer, "signing")
}

fn deserialize_deadline_ms<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_non_null_option(deserializer, "entrypoints[].deadline_ms")
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeEffects {
    Pure,
    ReadOnly,
    Effectful,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeDeterminism {
    Strict,
    Stable,
    BestEffort,
    Nondeterministic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subflows {
    pub mode: SubflowsMode,
    pub entries: Vec<SubflowEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubflowsMode {
    Embedded,
    External,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubflowEntry {
    pub id: String,
    pub bundle_id: String,
    pub entrypoint: String,
    pub embedded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signing {
    pub algorithm: String,
    pub key_id: String,
    pub signed_at: String,
}

pub fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("sha256:{}", hex::encode(digest))
}

pub fn canonical_json(value: &JsonValue) -> String {
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            serde_json::to_string(value).expect("json")
        }
        JsonValue::Array(values) => {
            let mut out = String::from("[");
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json(item));
            }
            out.push(']');
            out
        }
        JsonValue::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();

            let mut out = String::from("{");
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("json key"));
                out.push(':');
                out.push_str(&canonical_json(map.get(key).expect("key present")));
            }
            out.push('}');
            out
        }
    }
}

pub fn compute_bundle_id(manifest: &Manifest) -> Result<String, BundleError> {
    let mut value = serde_json::to_value(manifest)?;
    let Some(obj) = value.as_object_mut() else {
        return Err(BundleError::ManifestNotObject);
    };
    obj.remove("bundle_id");
    obj.remove("signing");
    Ok(sha256_prefixed(canonical_json(&value).as_bytes()))
}

fn is_sha256_prefixed(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("sha256:") else {
        return false;
    };
    rest.len() == 64
        && rest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn validate_manifest(manifest: &Manifest) -> Result<(), BundleError> {
    if manifest.bundle_version != BUNDLE_VERSION {
        return Err(BundleError::ManifestValidation(format!(
            "bundle_version must be {BUNDLE_VERSION}"
        )));
    }
    if !is_sha256_prefixed(&manifest.bundle_id) {
        return Err(BundleError::ManifestValidation(
            "bundle_id must be sha256:<hex>".to_string(),
        ));
    }
    if !is_sha256_prefixed(&manifest.code.hash) {
        return Err(BundleError::ManifestValidation(
            "code.hash must be sha256:<hex>".to_string(),
        ));
    }
    if let Some(flow_ir) = manifest.flow_ir.as_ref() {
        if !is_sha256_prefixed(&flow_ir.hash) {
            return Err(BundleError::ManifestValidation(
                "flow_ir.hash must be sha256:<hex>".to_string(),
            ));
        }
    }
    for artifact in &manifest.artifacts {
        if !is_sha256_prefixed(&artifact.hash) {
            return Err(BundleError::ManifestValidation(
                "artifacts[].hash must be sha256:<hex>".to_string(),
            ));
        }
    }
    if let Some(subflows) = manifest.subflows.as_ref() {
        for entry in &subflows.entries {
            if !is_sha256_prefixed(&entry.bundle_id) {
                return Err(BundleError::ManifestValidation(
                    "subflows.entries[].bundle_id must be sha256:<hex>".to_string(),
                ));
            }
        }
    }
    let expected = compute_bundle_id(manifest)?;
    if manifest.bundle_id != expected {
        return Err(BundleError::ManifestValidation(
            "bundle_id does not match computed manifest hash".to_string(),
        ));
    }
    Ok(())
}

pub fn read_manifest_from_custom_section(bytes: &[u8]) -> Result<Manifest, BundleError> {
    let parser = wasmparser::Parser::new(0);
    for payload in parser.parse_all(bytes) {
        let payload = payload.map_err(|err| BundleError::WasmParse(err.to_string()))?;
        if let wasmparser::Payload::CustomSection(section) = payload {
            if section.name() == MANIFEST_SECTION {
                let json =
                    std::str::from_utf8(section.data()).map_err(|_| BundleError::ManifestUtf8)?;
                let manifest: Manifest = serde_json::from_str(json)?;
                validate_manifest(&manifest)?;
                return Ok(manifest);
            }
        }
    }
    Err(BundleError::MissingCustomSection(MANIFEST_SECTION))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn push_leb_u32(out: &mut Vec<u8>, mut value: u32) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn wasm_with_manifest_json(manifest: &JsonValue) -> Vec<u8> {
        let json = serde_json::to_string(manifest).expect("manifest json");
        let name = MANIFEST_SECTION.as_bytes();
        let mut payload = Vec::new();
        push_leb_u32(&mut payload, name.len() as u32);
        payload.extend_from_slice(name);
        payload.extend_from_slice(json.as_bytes());

        let mut wasm = Vec::new();
        wasm.extend_from_slice(b"\0asm");
        wasm.extend_from_slice(&1u32.to_le_bytes());
        wasm.push(0);
        push_leb_u32(&mut wasm, payload.len() as u32);
        wasm.extend_from_slice(&payload);
        wasm
    }

    fn base_manifest_json() -> JsonValue {
        json!({
            "bundle_version": "0.1",
            "abi": {
                "name": "latticeflow.wit",
                "version": "0.1"
            },
            "bundle_id": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "code": {
                "target": "wasm32-unknown-unknown",
                "file": "flow.wasm",
                "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                "size_bytes": 4
            },
            "flow": {
                "id": "flow://demo",
                "version": "v0.1.0",
                "profile": "wasm"
            }
        })
    }

    #[test]
    fn canonical_json_sorts_object_keys() {
        let value = json!({"b": 2, "a": 1});
        let out = canonical_json(&value);
        assert_eq!(out, "{\"a\":1,\"b\":2}");
    }

    #[test]
    fn bundle_id_is_deterministic() {
        let mut manifest = Manifest {
            bundle_version: "0.1".to_string(),
            abi: AbiRef {
                name: "latticeflow.wit".to_string(),
                version: "0.1".to_string(),
            },
            bundle_id: "".to_string(),
            code: CodeDescriptor {
                target: "wasm32-unknown-unknown".to_string(),
                file: "flow.wasm".to_string(),
                hash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                size_bytes: 4,
            },
            artifacts: Vec::new(),
            flow: FlowDescriptor {
                id: "flow://demo".to_string(),
                version: "v0.1.0".to_string(),
                profile: "wasm".to_string(),
            },
            flow_ir: None,
            entrypoints: Vec::new(),
            nodes: std::collections::BTreeMap::new(),
            capabilities: Capabilities::default(),
            subflows: None,
            signing: None,
        };

        let bundle_id = compute_bundle_id(&manifest).expect("bundle id");
        manifest.bundle_id = bundle_id.clone();
        let bundle_id2 = compute_bundle_id(&manifest).expect("bundle id");
        assert_eq!(bundle_id, bundle_id2);
        assert!(bundle_id.starts_with("sha256:"));
    }

    #[test]
    fn bundle_id_ignores_signing_metadata() {
        let mut manifest = Manifest {
            bundle_version: "0.1".to_string(),
            abi: AbiRef {
                name: "latticeflow.wit".to_string(),
                version: "0.1".to_string(),
            },
            bundle_id: "".to_string(),
            code: CodeDescriptor {
                target: "wasm32-unknown-unknown".to_string(),
                file: "flow.wasm".to_string(),
                hash: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                size_bytes: 4,
            },
            artifacts: Vec::new(),
            flow: FlowDescriptor {
                id: "flow://demo".to_string(),
                version: "v0.1.0".to_string(),
                profile: "wasm".to_string(),
            },
            flow_ir: None,
            entrypoints: Vec::new(),
            nodes: std::collections::BTreeMap::new(),
            capabilities: Capabilities::default(),
            subflows: None,
            signing: None,
        };

        let unsigned_id = compute_bundle_id(&manifest).expect("bundle id");
        manifest.signing = Some(Signing {
            algorithm: "ed25519".to_string(),
            key_id: "key-1".to_string(),
            signed_at: "2026-02-03T00:00:00Z".to_string(),
        });
        let signed_id = compute_bundle_id(&manifest).expect("bundle id");

        assert_eq!(unsigned_id, signed_id);
    }

    #[test]
    fn bundle_id_excludes_bundle_id_field_from_hash() {
        let manifest = Manifest {
            bundle_version: "0.1".to_string(),
            abi: AbiRef {
                name: "latticeflow.wit".to_string(),
                version: "0.1".to_string(),
            },
            bundle_id: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .to_string(),
            code: CodeDescriptor {
                target: "wasm32-unknown-unknown".to_string(),
                file: "flow.wasm".to_string(),
                hash: "sha256:9999999999999999999999999999999999999999999999999999999999999999"
                    .to_string(),
                size_bytes: 4,
            },
            artifacts: Vec::new(),
            flow: FlowDescriptor {
                id: "flow://demo".to_string(),
                version: "v0.1.0".to_string(),
                profile: "wasm".to_string(),
            },
            flow_ir: None,
            entrypoints: Vec::new(),
            nodes: std::collections::BTreeMap::new(),
            capabilities: Capabilities::default(),
            subflows: None,
            signing: None,
        };

        let mut value = serde_json::to_value(&manifest).expect("manifest json");
        let obj = value.as_object_mut().expect("manifest object");
        obj.remove("bundle_id");
        obj.remove("signing");
        let expected = sha256_prefixed(canonical_json(&value).as_bytes());

        let computed = compute_bundle_id(&manifest).expect("bundle id");
        assert_eq!(expected, computed);
    }

    #[test]
    fn manifest_rejects_mismatched_bundle_id() {
        let mut manifest = Manifest {
            bundle_version: "0.1".to_string(),
            abi: AbiRef {
                name: "latticeflow.wit".to_string(),
                version: "0.1".to_string(),
            },
            bundle_id: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .to_string(),
            code: CodeDescriptor {
                target: "wasm32-unknown-unknown".to_string(),
                file: "flow.wasm".to_string(),
                hash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                size_bytes: 4,
            },
            artifacts: Vec::new(),
            flow: FlowDescriptor {
                id: "flow://demo".to_string(),
                version: "v0.1.0".to_string(),
                profile: "wasm".to_string(),
            },
            flow_ir: None,
            entrypoints: Vec::new(),
            nodes: std::collections::BTreeMap::new(),
            capabilities: Capabilities::default(),
            subflows: None,
            signing: None,
        };

        let computed = compute_bundle_id(&manifest).expect("bundle id");
        let mut wrong = computed.clone();
        let replacement = if wrong.ends_with('a') { 'b' } else { 'a' };
        wrong.pop();
        wrong.push(replacement);
        manifest.bundle_id = wrong;
        let result = validate_manifest(&manifest);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_rejects_unknown_entrypoint_fields() {
        let mut manifest = base_manifest_json();
        manifest["entrypoints"] = json!([
            {
                "trigger": "ingress",
                "capture": "out",
                "route_aliases": [],
                "deadline_ms": 0,
                "unexpected": "value"
            }
        ]);

        let result: Result<Manifest, _> = serde_json::from_value(manifest);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_read_rejects_invalid_bundle_version() {
        let mut manifest = base_manifest_json();
        manifest["bundle_version"] = json!("0.2");
        let wasm = wasm_with_manifest_json(&manifest);

        let result = read_manifest_from_custom_section(&wasm);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_read_rejects_invalid_hash_format() {
        let mut manifest = base_manifest_json();
        manifest["code"]["hash"] = json!("not-a-hash");
        let wasm = wasm_with_manifest_json(&manifest);

        let result = read_manifest_from_custom_section(&wasm);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_read_rejects_uppercase_hash() {
        let mut manifest = base_manifest_json();
        manifest["code"]["hash"] =
            json!("sha256:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF");
        let wasm = wasm_with_manifest_json(&manifest);

        let result = read_manifest_from_custom_section(&wasm);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_rejects_non_object_capability_constraints() {
        let mut manifest = base_manifest_json();
        manifest["capabilities"] = json!({
            "required": [
                {
                    "name": "dedupe",
                    "kind": "resource::dedupe",
                    "constraints": "not-an-object"
                }
            ]
        });

        let result: Result<Manifest, _> = serde_json::from_value(manifest);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_rejects_invalid_node_effects() {
        let mut manifest = base_manifest_json();
        manifest["nodes"] = json!({
            "Normalize": {
                "id": "node://normalize",
                "effects": "unstable",
                "determinism": "strict",
                "durability": {
                    "checkpointable": false,
                    "replayable": false,
                    "halts": false
                },
                "input_schema": "opaque",
                "output_schema": "opaque"
            }
        });

        let result: Result<Manifest, _> = serde_json::from_value(manifest);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_rejects_invalid_node_determinism() {
        let mut manifest = base_manifest_json();
        manifest["nodes"] = json!({
            "Normalize": {
                "id": "node://normalize",
                "effects": "pure",
                "determinism": "random",
                "durability": {
                    "checkpointable": false,
                    "replayable": false,
                    "halts": false
                },
                "input_schema": "opaque",
                "output_schema": "opaque"
            }
        });

        let result: Result<Manifest, _> = serde_json::from_value(manifest);
        assert!(result.is_err());
    }

    #[test]
    fn manifest_rejects_invalid_subflows_mode() {
        let mut manifest = base_manifest_json();
        manifest["subflows"] = json!({
            "mode": "local",
            "entries": []
        });

        let result: Result<Manifest, _> = serde_json::from_value(manifest);
        assert!(result.is_err());
    }
}
