use futures::future::BoxFuture;
use serde_json::Value;

use llm_types::wasm_compat::{WasmCompatSend, WasmCompatSync};

#[derive(Debug, thiserror::Error)]
pub enum VectorStoreError {
    #[error("vector store error: {0}")]
    Message(String),
}

pub type TopNResults = Result<Vec<(f64, String, Value)>, VectorStoreError>;

pub mod request {
    #[derive(Clone, Debug, Default)]
    pub struct VectorSearchRequest {
        pub query: String,
        pub samples: u64,
    }

    #[derive(Default)]
    pub struct VectorSearchRequestBuilder {
        query: Option<String>,
        samples: Option<u64>,
    }

    impl VectorSearchRequest {
        pub fn builder() -> VectorSearchRequestBuilder {
            VectorSearchRequestBuilder::default()
        }
    }

    impl VectorSearchRequestBuilder {
        pub fn query(mut self, query: impl Into<String>) -> Self {
            self.query = Some(query.into());
            self
        }

        pub fn samples(mut self, samples: u64) -> Self {
            self.samples = Some(samples);
            self
        }

        pub fn build(self) -> Result<VectorSearchRequest, &'static str> {
            Ok(VectorSearchRequest {
                query: self.query.ok_or("query is required")?,
                samples: self.samples.unwrap_or(0),
            })
        }
    }
}

pub trait VectorStoreIndexDyn: WasmCompatSend + WasmCompatSync {
    fn top_n<'a>(
        &'a self,
        req: request::VectorSearchRequest,
    ) -> BoxFuture<'a, TopNResults>;
}

impl<T> VectorStoreIndexDyn for T
where
    T: WasmCompatSend + WasmCompatSync + Send + Sync,
{
    fn top_n<'a>(
        &'a self,
        _req: request::VectorSearchRequest,
    ) -> BoxFuture<'a, TopNResults> {
        Box::pin(async { Ok(Vec::new()) })
    }
}
