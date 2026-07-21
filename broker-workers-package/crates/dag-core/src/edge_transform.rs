use std::fmt;

use serde_json::Value as JsonValue;

use crate::SchemaRef;

/// Deterministic runtime Into coercions currently supported.
///
/// NOTE: this is intentionally conservative. We only allow conversions we can
/// model predictably from serialized JSON values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntoCoercion {
    U32ToU64,
    I32ToI64,
    CharToU32,
}

impl IntoCoercion {
    pub const fn source_schema(self) -> &'static str {
        match self {
            Self::U32ToU64 => "u32",
            Self::I32ToI64 => "i32",
            Self::CharToU32 => "char",
        }
    }

    pub const fn target_schema(self) -> &'static str {
        match self {
            Self::U32ToU64 => "u64",
            Self::I32ToI64 => "i64",
            Self::CharToU32 => "u32",
        }
    }
}

/// Errors returned when runtime coercion fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntoCoercionError {
    InvalidJsonType {
        expected: &'static str,
        actual: &'static str,
    },
    ParseFailed {
        target: &'static str,
        input: String,
    },
    RangeOutOfBounds {
        source: &'static str,
        input: String,
    },
}

impl fmt::Display for IntoCoercionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJsonType { expected, actual } => {
                write!(f, "expected JSON {expected}, got {actual}")
            }
            Self::ParseFailed { target, input } => {
                write!(f, "failed to parse `{input}` as {target}")
            }
            Self::RangeOutOfBounds { source, input } => {
                write!(f, "value `{input}` is outside {source} range")
            }
        }
    }
}

impl std::error::Error for IntoCoercionError {}

/// Check if source and target schemas are already directly compatible.
pub fn schemas_compatible(source: &SchemaRef, target: &SchemaRef) -> bool {
    match (source, target) {
        (SchemaRef::Named { name: a }, SchemaRef::Named { name: b }) => a == b,
        _ => true,
    }
}

/// Resolve deterministic Into coercion support for a schema pair.
pub fn supported_into_coercion(source: &SchemaRef, target: &SchemaRef) -> Option<IntoCoercion> {
    let (Some(source_name), Some(target_name)) = (named_schema(source), named_schema(target))
    else {
        return None;
    };

    match (source_name, target_name) {
        ("u32", "u64") => Some(IntoCoercion::U32ToU64),
        ("i32", "i64") => Some(IntoCoercion::I32ToI64),
        ("char", "u32") => Some(IntoCoercion::CharToU32),
        _ => None,
    }
}

/// Apply a deterministic Into coercion to an outbound payload.
pub fn apply_into_coercion(
    coercion: IntoCoercion,
    payload: JsonValue,
) -> Result<JsonValue, IntoCoercionError> {
    match coercion {
        IntoCoercion::U32ToU64 => {
            let actual = json_type_name(&payload);
            let Some(value) = payload.as_u64() else {
                return Err(IntoCoercionError::InvalidJsonType {
                    expected: "unsigned_integer",
                    actual,
                });
            };
            if value > u32::MAX as u64 {
                return Err(IntoCoercionError::RangeOutOfBounds {
                    source: "u32",
                    input: value.to_string(),
                });
            }
            Ok(JsonValue::from(value))
        }
        IntoCoercion::I32ToI64 => {
            let actual = json_type_name(&payload);
            let Some(value) = payload.as_i64() else {
                return Err(IntoCoercionError::InvalidJsonType {
                    expected: "integer",
                    actual,
                });
            };
            if value < i32::MIN as i64 || value > i32::MAX as i64 {
                return Err(IntoCoercionError::RangeOutOfBounds {
                    source: "i32",
                    input: value.to_string(),
                });
            }
            Ok(JsonValue::from(value))
        }
        IntoCoercion::CharToU32 => {
            let actual = json_type_name(&payload);
            let JsonValue::String(raw) = payload else {
                return Err(IntoCoercionError::InvalidJsonType {
                    expected: "string",
                    actual,
                });
            };
            let mut chars = raw.chars();
            let Some(ch) = chars.next() else {
                return Err(IntoCoercionError::ParseFailed {
                    target: "char",
                    input: raw,
                });
            };
            if chars.next().is_some() {
                return Err(IntoCoercionError::ParseFailed {
                    target: "char",
                    input: raw,
                });
            }
            Ok(JsonValue::from(ch as u32))
        }
    }
}

/// Return a stable JSON type label for diagnostics.
pub fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(number) => {
            if number.is_i64() {
                "integer"
            } else if number.is_u64() {
                "unsigned_integer"
            } else {
                "number"
            }
        }
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn named_schema(schema: &SchemaRef) -> Option<&str> {
    match schema {
        SchemaRef::Named { name } => Some(name.as_str()),
        SchemaRef::Opaque => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_supported_into_coercions() {
        let source = SchemaRef::named("u32");
        let target = SchemaRef::named("u64");
        assert_eq!(
            supported_into_coercion(&source, &target),
            Some(IntoCoercion::U32ToU64)
        );
    }

    #[test]
    fn applies_u32_to_u64() {
        let value = JsonValue::from(42_u32);
        let coerced = apply_into_coercion(IntoCoercion::U32ToU64, value).expect("coerce");
        assert_eq!(coerced, JsonValue::from(42_u64));
    }

    #[test]
    fn char_to_u32_rejects_multi_char_string() {
        let value = JsonValue::String("ab".to_string());
        let err = apply_into_coercion(IntoCoercion::CharToU32, value).expect_err("should fail");
        assert!(matches!(
            err,
            IntoCoercionError::ParseFailed { target: "char", .. }
        ));
    }
}
