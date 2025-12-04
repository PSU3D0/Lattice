//! OpenDAL-backed blob capability.
//!
//! This crate exposes [`OpendalBlobStore`], an implementation of
//! [`capabilities::blob::BlobStore`] that stores objects via OpenDAL
//! operators. Builders consume any [`cap_opendal_core::OperatorFactory`]
//! implementation, ensuring provider crates can share feature matrices,
//! layer wiring, and error translation logic.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use cap_opendal_core::{OpendalError, OpendalResult, OperatorFactory};
use capabilities::{
    Capability,
    blob::{BlobError, BlobStore},
};
use opendal::Operator;
use opendal::raw::{Accessor, Layer};

/// Operator transform applied after factory construction.
type OperatorTransform = Box<dyn FnOnce(Operator) -> Operator + Send>;

/// OpenDAL-backed blob store implementing the canonical capability trait.
#[derive(Clone)]
pub struct OpendalBlobStore {
    operator: Operator,
    capability_name: &'static str,
}

impl OpendalBlobStore {
    /// Construct a new store from an existing OpenDAL [`Operator`].
    pub fn new(operator: Operator) -> Self {
        Self {
            operator,
            capability_name: DEFAULT_CAPABILITY_NAME,
        }
    }

    /// Set a custom capability name (defaults to `blob.opendal`).
    pub fn with_name(mut self, capability_name: &'static str) -> Self {
        self.capability_name = capability_name;
        self
    }

    /// Access the underlying OpenDAL operator.
    pub fn operator(&self) -> &Operator {
        &self.operator
    }

    /// Create a builder that consumes the supplied factory.
    pub fn builder<F>(factory: F) -> BlobStoreBuilder<F>
    where
        F: OperatorFactory,
    {
        BlobStoreBuilder::new(factory)
    }
}

impl Capability for OpendalBlobStore {
    fn name(&self) -> &'static str {
        self.capability_name
    }
}

#[async_trait::async_trait]
impl BlobStore for OpendalBlobStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, BlobError> {
        match self.operator.read(key).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(err) => match OpendalError::from_opendal(err) {
                OpendalError::NotFound => Ok(None),
                other => Err(map_blob_error(other)),
            },
        }
    }

    async fn put(&self, key: &str, contents: &[u8]) -> Result<(), BlobError> {
        let bytes = contents.to_vec();

        self.operator
            .write(key, bytes)
            .await
            .map(|_| ())
            .map_err(|err| map_blob_error(OpendalError::from_opendal(err)))
    }

    async fn delete(&self, key: &str) -> Result<(), BlobError> {
        if let Err(err) = self.operator.stat(key).await {
            return match OpendalError::from_opendal(err) {
                OpendalError::NotFound => Err(BlobError::NotFound),
                other => Err(map_blob_error(other)),
            };
        }

        match self.operator.delete(key).await {
            Ok(_) => Ok(()),
            Err(err) => match OpendalError::from_opendal(err) {
                OpendalError::NotFound => Err(BlobError::NotFound),
                other => Err(map_blob_error(other)),
            },
        }
    }
}

/// Builder for [`OpendalBlobStore`], wrapping an [`OperatorFactory`].
pub struct BlobStoreBuilder<F> {
    factory: F,
    capability_name: &'static str,
    transforms: Vec<OperatorTransform>,
}

const DEFAULT_CAPABILITY_NAME: &str = "blob.opendal";

impl<F> BlobStoreBuilder<F>
where
    F: OperatorFactory,
{
    /// Create a builder using the provided factory.
    pub fn new(factory: F) -> Self {
        Self {
            factory,
            capability_name: DEFAULT_CAPABILITY_NAME,
            transforms: Vec::new(),
        }
    }

    /// Override the capability name returned by the store.
    pub fn capability_name(mut self, capability_name: &'static str) -> Self {
        self.capability_name = capability_name;
        self
    }

    /// Apply an arbitrary operator transform.
    pub fn map_operator(
        mut self,
        transform: impl FnOnce(Operator) -> Operator + Send + 'static,
    ) -> Self {
        self.transforms.push(Box::new(transform));
        self
    }

    /// Attach an OpenDAL layer to the constructed operator.
    pub fn with_layer<L>(self, layer: L) -> Self
    where
        L: Layer<Accessor> + Send + Sync + 'static,
    {
        self.map_operator(move |operator| operator.layer(layer))
    }

    /// Build the store, constructing the operator through the factory.
    pub async fn build(self) -> OpendalResult<OpendalBlobStore> {
        let factory = self.factory;
        let mut operator = factory.build().await?;
        for transform in self.transforms {
            operator = transform(operator);
        }
        Ok(OpendalBlobStore {
            operator,
            capability_name: self.capability_name,
        })
    }
}

fn map_blob_error(error: OpendalError) -> BlobError {
    match error {
        OpendalError::NotFound => BlobError::NotFound,
        other => BlobError::Other(other.to_string()),
    }
}

#[cfg(all(test, feature = "services-memory"))]
mod tests {
    use super::*;
    use cap_opendal_core::SchemeOperatorFactory;
    use capabilities::blob::BlobStore;
    use opendal::Scheme;

    #[tokio::test]
    async fn round_trip_memory_backend() {
        let factory = SchemeOperatorFactory::new(Scheme::Memory, [("root", "/")]);
        let store = OpendalBlobStore::builder(factory)
            .capability_name("blob.memory")
            .build()
            .await
            .expect("build store");

        assert_eq!(store.get("missing").await.unwrap(), None);

        store.put("key", b"payload").await.expect("put");
        assert_eq!(store.get("key").await.unwrap(), Some(b"payload".to_vec()));

        store.delete("key").await.expect("delete");
        assert_eq!(store.get("key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_missing_reports_not_found() {
        let factory = SchemeOperatorFactory::new(Scheme::Memory, [("root", "/")]);
        let store = OpendalBlobStore::builder(factory)
            .map_operator(|op| op.layer(opendal::layers::LoggingLayer::default()))
            .build()
            .await
            .unwrap();
        let err = store.delete("missing").await.expect_err("delete missing");
        assert!(matches!(err, BlobError::NotFound));
    }
}
