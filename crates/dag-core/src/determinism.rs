use std::sync::RwLock;

use once_cell::sync::Lazy;

use crate::Determinism;
use crate::effect_hint::EffectHint;

/// Registry entry describing a determinism conflict for a given resource hint.
#[derive(Debug, Clone)]
pub struct DeterminismConstraint {
    /// Resource hint identifier (hierarchical; use `::` separators).
    pub hint: &'static str,
    /// Minimum determinism level required when the resource is present.
    pub minimum: Determinism,
    /// Human-friendly guidance describing the mitigation.
    pub guidance: &'static str,
}

impl DeterminismConstraint {
    /// Construct a new constraint definition.
    pub const fn new(hint: &'static str, minimum: Determinism, guidance: &'static str) -> Self {
        Self {
            hint,
            minimum,
            guidance,
        }
    }

    /// Determine whether the supplied resource hint matches this constraint.
    pub fn matches(&self, resource_hint: &str) -> bool {
        resource_hint == self.hint || resource_hint.starts_with(self.hint)
    }
}

/// Runtime registry for ADDITIONAL (non-canonical) constraints only.
///
/// Packet A1: constraints for every canonical `resource::*` hint are derived
/// exhaustively from [`EffectHint`] (family-wide, matching the historical
/// prefix semantics) and always take precedence, so lookups no longer depend
/// on `ensure_registered()` call order.
static CONSTRAINTS: Lazy<RwLock<Vec<DeterminismConstraint>>> = Lazy::new(|| RwLock::new(Vec::new()));

fn derived_constraint(hint: EffectHint) -> Option<DeterminismConstraint> {
    hint.determinism_constraint().map(|(minimum, guidance)| {
        DeterminismConstraint::new(hint.family().as_str(), minimum, guidance)
    })
}

/// Register an additional determinism constraint. Existing hints are replaced.
///
/// Constraints for canonical [`EffectHint`] strings are derived from the enum
/// and CANNOT be overridden here; registering one is a no-op for lookups.
pub fn register_determinism_constraint(constraint: DeterminismConstraint) {
    let mut guard = CONSTRAINTS
        .write()
        .expect("determinism constraint registry poisoned");
    if let Some(existing) = guard.iter_mut().find(|item| item.hint == constraint.hint) {
        *existing = constraint;
    } else {
        guard.push(constraint);
    }
}

/// Find the constraint matching the provided resource hint.
///
/// Canonical hints resolve via [`EffectHint`] derivation (registration-order
/// independent); anything else falls back to the runtime registry.
pub fn constraint_for_hint(hint: &str) -> Option<DeterminismConstraint> {
    if let Ok(parsed) = EffectHint::parse(hint) {
        return derived_constraint(parsed);
    }
    let guard = CONSTRAINTS
        .read()
        .expect("determinism constraint registry poisoned");
    guard.iter().find(|c| c.matches(hint)).cloned()
}

/// Snapshot all determinism constraints: the canonical enum-derived
/// family-level set plus any runtime-registered extras for non-canonical
/// hints.
pub fn all_constraints() -> Vec<DeterminismConstraint> {
    let mut constraints: Vec<DeterminismConstraint> = Vec::new();
    for hint in EffectHint::ALL {
        // One constraint per family (bare family variants only).
        if hint.family() != hint {
            continue;
        }
        if let Some(constraint) = derived_constraint(hint) {
            constraints.push(constraint);
        }
    }
    let guard = CONSTRAINTS
        .read()
        .expect("determinism constraint registry poisoned");
    constraints.extend(
        guard
            .iter()
            .filter(|c| EffectHint::parse(c.hint).is_err())
            .cloned(),
    );
    constraints
}
