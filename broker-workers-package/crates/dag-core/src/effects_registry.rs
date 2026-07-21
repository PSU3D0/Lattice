use std::sync::RwLock;

use once_cell::sync::Lazy;

use crate::Effects;
use crate::effect_hint::EffectHint;

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

/// Runtime registry for ADDITIONAL (non-canonical) constraints only.
///
/// Packet A1: constraints for every canonical `resource::*` hint are derived
/// exhaustively from [`EffectHint`] and always take precedence, so lookups no
/// longer depend on `ensure_registered()` call order. This registry remains
/// for plugin-defined hint vocabularies that have not been promoted into the
/// enum yet (note that kernel-plan validation rejects unknown hints in Flow
/// IR, so such constraints only apply to out-of-IR consumers).
static CONSTRAINTS: Lazy<RwLock<Vec<EffectConstraint>>> = Lazy::new(|| RwLock::new(Vec::new()));

fn derived_constraint(hint: EffectHint) -> Option<EffectConstraint> {
    hint.effect_constraint()
        .map(|(minimum, guidance)| EffectConstraint::new(hint.as_str(), minimum, guidance))
}

/// Register an additional effect constraint. Existing hints are replaced.
///
/// Constraints for canonical [`EffectHint`] strings are derived from the enum
/// and CANNOT be overridden here; registering one is a no-op for lookups.
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
///
/// Canonical hints resolve via [`EffectHint`] derivation (registration-order
/// independent); anything else falls back to the runtime registry.
pub fn constraint_for_hint(hint: &str) -> Option<EffectConstraint> {
    if let Ok(parsed) = EffectHint::parse(hint) {
        return derived_constraint(parsed);
    }
    let guard = CONSTRAINTS
        .read()
        .expect("effect constraint registry poisoned");
    guard.iter().find(|c| c.matches(hint)).cloned()
}

/// Snapshot all effect constraints: the canonical enum-derived set plus any
/// runtime-registered extras for non-canonical hints.
pub fn all_constraints() -> Vec<EffectConstraint> {
    let mut constraints: Vec<EffectConstraint> = EffectHint::ALL
        .into_iter()
        .filter_map(derived_constraint)
        .collect();
    let guard = CONSTRAINTS
        .read()
        .expect("effect constraint registry poisoned");
    constraints.extend(
        guard
            .iter()
            .filter(|c| EffectHint::parse(c.hint).is_err())
            .cloned(),
    );
    constraints
}
