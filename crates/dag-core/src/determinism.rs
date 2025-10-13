use std::sync::RwLock;

use once_cell::sync::Lazy;

use crate::Determinism;

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

static CONSTRAINTS: Lazy<RwLock<Vec<DeterminismConstraint>>> = Lazy::new(|| {
    RwLock::new(vec![
        DeterminismConstraint::new(
            "resource::clock",
            Determinism::BestEffort,
            "Clock access introduces wall-clock time; downgrade determinism to BestEffort or surface the timestamp as an input.",
        ),
        DeterminismConstraint::new(
            "resource::http",
            Determinism::BestEffort,
            "HTTP calls depend on external systems; downgrade determinism or provide cached/pinned responses before claiming Stable.",
        ),
        DeterminismConstraint::new(
            "resource::rng",
            Determinism::BestEffort,
            "Random number generation requires a deterministic seed; downgrade determinism or use a seeded RNG provided via input.",
        ),
    ])
});

/// Register an additional determinism constraint. Existing hints are replaced.
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

/// Find the first constraint matching the provided resource hint.
pub fn constraint_for_hint(hint: &str) -> Option<DeterminismConstraint> {
    let guard = CONSTRAINTS
        .read()
        .expect("determinism constraint registry poisoned");
    guard.iter().find(|c| c.matches(hint)).cloned()
}

/// Snapshot all registered determinism constraints.
pub fn all_constraints() -> Vec<DeterminismConstraint> {
    let guard = CONSTRAINTS
        .read()
        .expect("determinism constraint registry poisoned");
    guard.clone()
}
