use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Canonical diagnostic severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Build or runtime must halt.
    Error,
    /// Action recommended but execution may proceed.
    Warn,
    /// Informational context only.
    Info,
}

/// Structured metadata for a diagnostic emitted by the platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DiagnosticCode {
    /// Stable identifier (e.g. `DAG200`).
    pub code: &'static str,
    /// Primary subsystem or producer of the diagnostic.
    pub subsystem: &'static str,
    /// Default severity when policies do not override the level.
    pub default_severity: Severity,
    /// Short human-readable description.
    pub summary: &'static str,
}

/// Concrete diagnostic emitted during validation or runtime.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    /// Diagnostic code metadata.
    pub code: &'static DiagnosticCode,
    /// Long form message presented to the user.
    pub message: String,
    /// Optional machine-readable location (file span, node id, etc.).
    pub location: Option<String>,
}

impl Diagnostic {
    /// Convenience constructor.
    pub fn new(code: &'static DiagnosticCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            location: None,
        }
    }

    /// Attach location metadata to an existing diagnostic.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }
}

/// Public accessor for the registry.
pub fn diagnostic_codes() -> &'static [DiagnosticCode] {
    &DIAGNOSTIC_CODES
}

/// Canonical diagnostic registry used across the workspace.
pub static DIAGNOSTIC_CODES: Lazy<Vec<DiagnosticCode>> = Lazy::new(|| {
    vec![
        DiagnosticCode {
            code: "DAG001",
            subsystem: "macros",
            default_severity: Severity::Error,
            summary: "Missing or unknown port type on a node definition",
        },
        DiagnosticCode {
            code: "DAG002",
            subsystem: "macros",
            default_severity: Severity::Error,
            summary: "Node parameters could not be reflected into a schema",
        },
        DiagnosticCode {
            code: "DAG003",
            subsystem: "macros",
            default_severity: Severity::Error,
            summary: "Referenced resource or capability is undefined",
        },
        DiagnosticCode {
            code: "DAG004",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Effectful node lacks a valid idempotency declaration",
        },
        DiagnosticCode {
            code: "DAG005",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Node concurrency hints exceed allowed bounds",
        },
        DiagnosticCode {
            code: "DAG006",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Conflicting batch configuration detected on node",
        },
        DiagnosticCode {
            code: "DAG101",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Trigger definition does not expose an output port",
        },
        DiagnosticCode {
            code: "DAG102",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Trigger respond configuration incompatible with profile",
        },
        DiagnosticCode {
            code: "DAG103",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Trigger route or method conflicts with an existing trigger",
        },
        DiagnosticCode {
            code: "DAG200",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Cycle detected in workflow graph",
        },
        DiagnosticCode {
            code: "DAG201",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Port type mismatch between connected nodes",
        },
        DiagnosticCode {
            code: "DAG202",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Referenced workflow variable or alias is undefined",
        },
        DiagnosticCode {
            code: "DAG205",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Duplicate node alias encountered in workflow",
        },
        DiagnosticCode {
            code: "DAG206",
            subsystem: "macros",
            default_severity: Severity::Error,
            summary: "Edge control statement references a missing edge",
        },
        DiagnosticCode {
            code: "DAG207",
            subsystem: "macros",
            default_severity: Severity::Error,
            summary: "Duplicate edge control statement for the same edge",
        },
        DiagnosticCode {
            code: "EXACT001",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Exactly-once delivery requires a dedupe capability binding",
        },
        DiagnosticCode {
            code: "EXACT002",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Exactly-once delivery requires an idempotency key",
        },
        DiagnosticCode {
            code: "EXACT003",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Exactly-once delivery requires a minimum dedupe TTL",
        },
        DiagnosticCode {
            code: "SPILL001",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Spill tiers require a bounded in-memory buffer",
        },
        DiagnosticCode {
            code: "SPILL002",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Spill tiers require a blob capability binding",
        },
        DiagnosticCode {
            code: "EFFECT201",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Declared effects do not match bound capabilities",
        },
        DiagnosticCode {
            code: "DET301",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Determinism claim conflicts with resource usage",
        },
        DiagnosticCode {
            code: "DET302",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Declared determinism is incompatible with referenced resource hints",
        },
        DiagnosticCode {
            code: "CTRL001",
            subsystem: "lint",
            default_severity: Severity::Warn,
            summary: "Control-flow surface hint recommended for branching or loop",
        },
        DiagnosticCode {
            code: "CTRL101",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Edge timeout budget must be positive",
        },
        DiagnosticCode {
            code: "CTRL102",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Edge buffer max_items must be positive",
        },
        DiagnosticCode {
            code: "CTRL110",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Switch control surface config is invalid",
        },
        DiagnosticCode {
            code: "CTRL111",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Switch control surface references a missing edge",
        },
        DiagnosticCode {
            code: "CTRL112",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Multiple switch control surfaces reference the same source node",
        },
        DiagnosticCode {
            code: "CTRL120",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "If control surface config is invalid",
        },
        DiagnosticCode {
            code: "CTRL121",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "If control surface references a missing edge",
        },
        DiagnosticCode {
            code: "CTRL122",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Multiple if control surfaces reference the same source node",
        },
        DiagnosticCode {
            code: "CTRL901",
            subsystem: "runtime",
            default_severity: Severity::Error,
            summary: "Reserved control surface not supported by this host or profile",
        },
        DiagnosticCode {
            code: "CAP101",
            subsystem: "runtime",
            default_severity: Severity::Error,
            summary: "Required capability binding missing from ResourceBag during preflight",
        },
        DiagnosticCode {
            code: "IDEM020",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Effectful sink missing partition key and idempotency key",
        },
        DiagnosticCode {
            code: "IDEM025",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Idempotency key references non-deterministic fields",
        },
        DiagnosticCode {
            code: "CACHE001",
            subsystem: "validation",
            default_severity: Severity::Warn,
            summary: "Strict node missing cache specification",
        },
        DiagnosticCode {
            code: "CACHE002",
            subsystem: "validation",
            default_severity: Severity::Error,
            summary: "Stable node missing pinned inputs or cache policy",
        },
    ]
});
