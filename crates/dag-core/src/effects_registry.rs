use std::sync::RwLock;

use once_cell::sync::Lazy;

use crate::Effects;

/// Registry entry describing the minimum effects level required for a resource hint.
#[derive(Debug, Clone)]
pub struct EffectConstraint {
    /// Resource hint identifier (namespaced using `::`).
    pub hint: &'static str,
    /// Minimum effects level required when this hint is present.
    pub minimum: Effects,
    /// Guidance surfaced alongside diagnostics.
    pub guidance: &'static str,
}

impl EffectConstraint {
    /// Construct a new constraint description.
    pub const fn new(hint: &'static str, minimum: Effects, guidance: &'static str) -> Self {
        Self {
            hint,
            minimum,
            guidance,
        }
    }

    /// Returns true when the provided hint matches this constraint.
    pub fn matches(&self, resource_hint: &str) -> bool {
        resource_hint == self.hint || resource_hint.starts_with(self.hint)
    }
}

static CONSTRAINTS: Lazy<RwLock<Vec<EffectConstraint>>> = Lazy::new(|| {
    RwLock::new(vec![
        EffectConstraint::new(
            "resource::http::read",
            Effects::ReadOnly,
            "HTTP reads interact with external services; declare effects = ReadOnly or wrap in effectful activity.",
        ),
        EffectConstraint::new(
            "resource::http::write",
            Effects::Effectful,
            "HTTP writes are effectful; declare effects = Effectful or refactor to read-only operations.",
        ),
        EffectConstraint::new(
            "resource::db::write",
            Effects::Effectful,
            "Database writes require Effectful; downgrade only for read-only transactions.",
        ),
    ])
});

/// Register an additional effect constraint. Existing hints are replaced.
pub fn register_effect_constraint(constraint: EffectConstraint) {
    let mut guard = CONSTRAINTS
        .write()
        .expect("effect constraint registry poisoned");
    if let Some(existing) = guard.iter_mut().find(|item| item.hint == constraint.hint) {
        *existing = constraint;
    } else {
        guard.push(constraint);
    }
}

/// Look up the constraint that matches the provided hint, if any.
pub fn constraint_for_hint(hint: &str) -> Option<EffectConstraint> {
    let guard = CONSTRAINTS
        .read()
        .expect("effect constraint registry poisoned");
    guard.iter().find(|c| c.matches(hint)).cloned()
}

/// Snapshot all registered effect constraints.
pub fn all_constraints() -> Vec<EffectConstraint> {
    let guard = CONSTRAINTS
        .read()
        .expect("effect constraint registry poisoned");
    guard.clone()
}
