use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{LlmCompleteInput, LlmCompleteOutput};
use crate::ops::LlmComplete;

#[def_node(
    name = "LlmComplete",
    summary = "Generate one typed completion from a lock-selected LLM provider",
    identifier = "connector.llm.complete",
    connector_ops(crate::ops::LlmComplete)
)]
pub async fn llm_complete(input: LlmCompleteInput) -> NodeResult<LlmCompleteOutput> {
    LlmComplete::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.llm.complete failed: {err}")))
}
