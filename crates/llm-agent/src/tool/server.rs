use std::sync::{Arc, RwLock};

use futures::executor::block_on;
use crate::vector_store::VectorStoreIndexDyn;
use llm_types::completion::ToolDefinition;
use llm_types::tool::{ToolDyn, ToolSet, ToolSetError};

#[derive(Debug, thiserror::Error)]
pub enum ToolServerError {
    #[error("toolset error: {0}")]
    Toolset(#[from] ToolSetError),

    #[error("tool not found: {0}")]
    NotFound(String),

    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Default)]
pub struct ToolServer {
    static_tool_names: Vec<String>,
    toolset: ToolSet,
    dynamic_tools: Vec<(usize, Box<dyn VectorStoreIndexDyn + Send + Sync>)>,
}

impl ToolServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn static_tool_names(mut self, tool_names: Vec<String>) -> Self {
        self.static_tool_names = tool_names;
        self
    }

    pub fn add_tools(mut self, toolset: ToolSet) -> Self {
        self.toolset.add_tools(toolset);
        self
    }

    pub fn add_dynamic_tools(
        mut self,
        dynamic_tools: Vec<(usize, Box<dyn VectorStoreIndexDyn + Send + Sync>)>,
    ) -> Self {
        self.dynamic_tools = dynamic_tools;
        self
    }

    pub fn run(self) -> ToolServerHandle {
        ToolServerHandle(Arc::new(RwLock::new(self)))
    }
}

#[derive(Clone)]
pub struct ToolServerHandle(Arc<RwLock<ToolServer>>);

impl ToolServerHandle {
    pub async fn add_tool(&self, tool: impl ToolDyn + 'static) -> Result<(), ToolServerError> {
        let mut server = self.0.write().map_err(|_| ToolServerError::Poisoned)?;
        server.toolset.add_tool(tool);
        Ok(())
    }

    pub async fn append_toolset(&self, toolset: ToolSet) -> Result<(), ToolServerError> {
        let mut server = self.0.write().map_err(|_| ToolServerError::Poisoned)?;
        server.toolset.add_tools(toolset);
        Ok(())
    }

    pub async fn remove_tool(&self, tool_name: &str) -> Result<(), ToolServerError> {
        let mut server = self.0.write().map_err(|_| ToolServerError::Poisoned)?;
        server.toolset.delete_tool(tool_name);
        Ok(())
    }

    pub async fn call_tool(&self, tool_name: &str, args: &str) -> Result<String, ToolServerError> {
        let server = self.0.read().map_err(|_| ToolServerError::Poisoned)?;
        let result = block_on(server.toolset.call(tool_name, args.to_string()));
        result.map_err(Into::into)
    }

    pub async fn get_tool_defs(
        &self,
        prompt: Option<String>,
    ) -> Result<Vec<ToolDefinition>, ToolServerError> {
        let server = self.0.read().map_err(|_| ToolServerError::Poisoned)?;
        let defs = block_on(server.toolset.get_tool_definitions())?;

        if let Some(prompt) = prompt {
            let _ = prompt;
            let _ = &server.static_tool_names;
            let _ = &server.dynamic_tools;
        }

        Ok(defs)
    }
}
