//! Shared OpenDAL helpers used across capability crates.
//!
//! This crate centralises feature flags, operator factories, and error
//! translation so individual capability adapters can focus on domain
//! behaviour (blob, kv, queue, etc.) instead of wiring OpenDAL plumbing.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use async_trait::async_trait;
use opendal::raw::{Accessor, Layer};
use opendal::{Operator, Scheme};
use std::sync::Arc;

/// Error translation utilities used by capability crates.
pub mod error;

pub use error::{OpendalError, Result as OpendalResult};

/// Helper trait for building `Operator` instances.
///
/// Capability crates can implement this trait directly or wrap the provided
/// [`SchemeOperatorFactory`] when they need to expose backend-specific
/// configuration options.
#[async_trait]
pub trait OperatorFactory: Send + Sync {
    /// Build an [`Operator`] using the factory configuration.
    async fn build(&self) -> OpendalResult<Operator>;
}

/// Factory that constructs operators from a scheme and key–value configuration.
#[derive(Debug, Clone)]
pub struct SchemeOperatorFactory {
    scheme: Scheme,
    config: Vec<(String, String)>,
}

impl SchemeOperatorFactory {
    /// Create a new factory based on the supplied scheme and configuration.
    pub fn new<I, K, V>(scheme: Scheme, config: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            scheme,
            config: config
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }

    /// Append an additional configuration entry.
    pub fn with_entry(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.push((key.into(), value.into()));
        self
    }

    /// Inspect the configuration entries that will be forwarded to OpenDAL.
    pub fn config(&self) -> &[(String, String)] {
        &self.config
    }
}

#[async_trait]
impl OperatorFactory for SchemeOperatorFactory {
    async fn build(&self) -> OpendalResult<Operator> {
        Operator::via_iter(self.scheme, self.config.clone()).map_err(OpendalError::from_opendal)
    }
}

#[async_trait]
impl<T> OperatorFactory for Arc<T>
where
    T: OperatorFactory + ?Sized,
{
    async fn build(&self) -> OpendalResult<Operator> {
        (**self).build().await
    }
}

/// Extension helpers for layering operators conditionally.
pub trait OperatorLayerExt: Sized {
    /// Attach `layer` when `condition` holds; otherwise return the original operator.
    fn layer_if<L>(self, condition: bool, layer: L) -> Self
    where
        L: Layer<Accessor>;
}

impl OperatorLayerExt for Operator {
    fn layer_if<L>(self, condition: bool, layer: L) -> Self
    where
        L: Layer<Accessor>,
    {
        if condition { self.layer(layer) } else { self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::Error;
    use opendal::ErrorKind;

    #[test]
    fn maps_not_found_error() {
        let err = Error::new(ErrorKind::NotFound, "missing");
        let mapped = OpendalError::from(err);
        assert!(matches!(mapped, OpendalError::NotFound));
    }

    #[cfg(all(feature = "services-memory", feature = "layers-logging"))]
    #[tokio::test]
    async fn builds_memory_operator() {
        let factory = SchemeOperatorFactory::new(Scheme::Memory, [("root", "/tmp")]);
        let operator = factory.build().await.expect("operator");
        let layered = operator.layer_if(false, opendal::layers::LoggingLayer::default());
        // simple smoke check: layering with false shouldn't consume operator
        let _ = layered;
    }
}
