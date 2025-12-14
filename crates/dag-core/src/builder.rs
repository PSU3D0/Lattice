use std::collections::BTreeMap;

use semver::Version;

use crate::ir::{
    BufferPolicy, Delivery, EdgeIR, FlowIR, FlowId, NodeIR, NodeId, NodeSpec, Profile,
};

/// Errors produced by the flow builder.
#[derive(Debug, thiserror::Error)]
pub enum FlowBuilderError {
    /// Attempted to add a node with a duplicate alias.
    #[error("node alias `{0}` already exists in workflow")]
    DuplicateNode(String),
    /// Attempted to mutate an edge that does not exist.
    #[error("edge `{from}` -> `{to}` does not exist in workflow")]
    UnknownEdge { from: String, to: String },
}

/// Handle referencing a node added to the builder.
#[derive(Debug, Clone)]
pub struct NodeHandle {
    alias: String,
}

impl NodeHandle {
    /// Access node alias.
    pub fn alias(&self) -> &str {
        &self.alias
    }
}

/// Handle referencing an edge created by the builder.
#[derive(Debug, Clone)]
pub struct EdgeHandle {
    pub(crate) from: String,
    pub(crate) to: String,
}

impl EdgeHandle {
    pub fn from(&self) -> &str {
        &self.from
    }

    pub fn to(&self) -> &str {
        &self.to
    }
}

/// Builder used by macros to construct Flow IR instances.
pub struct FlowBuilder {
    flow: FlowIR,
    alias_map: BTreeMap<String, NodeId>,
}

impl FlowBuilder {
    /// Create a new builder for the given workflow.
    pub fn new(name: impl Into<String>, version: Version, profile: Profile) -> Self {
        let name = name.into();
        let id = FlowId::new(&name, &version);
        let flow = FlowIR {
            id,
            name,
            version,
            profile,
            summary: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            control_surfaces: Vec::new(),
            checkpoints: Vec::new(),
            policies: Default::default(),
            metadata: Default::default(),
            artifacts: Vec::new(),
        };
        Self {
            flow,
            alias_map: BTreeMap::new(),
        }
    }

    /// Attach a summary to the workflow.
    pub fn summary(&mut self, summary: Option<impl Into<String>>) {
        self.flow.summary = summary.map(Into::into);
    }

    /// Add a node to the workflow.
    pub fn add_node(
        &mut self,
        alias: impl Into<String>,
        spec: &NodeSpec,
    ) -> Result<NodeHandle, FlowBuilderError> {
        let alias = alias.into();
        if self.alias_map.contains_key(&alias) {
            return Err(FlowBuilderError::DuplicateNode(alias));
        }

        let node_id = NodeId::new(format!("{}::{}", self.flow.id.as_str(), alias));
        let node_ir = NodeIR {
            id: node_id.clone(),
            alias: alias.clone(),
            identifier: spec.identifier.to_string(),
            name: spec.name.to_string(),
            kind: spec.kind,
            summary: spec.summary.map(|s| s.to_string()),
            in_schema: spec.in_schema.into_ref(),
            out_schema: spec.out_schema.into_ref(),
            effects: spec.effects,
            determinism: spec.determinism,
            idempotency: Default::default(),
            determinism_hints: spec
                .determinism_hints
                .iter()
                .map(|hint| hint.to_string())
                .collect(),
            effect_hints: spec
                .effect_hints
                .iter()
                .map(|hint| hint.to_string())
                .collect(),
        };
        self.flow.nodes.push(node_ir);
        self.alias_map.insert(alias.clone(), node_id);
        Ok(NodeHandle { alias })
    }

    /// Create an edge between two node handles.
    pub fn connect(&mut self, from: &NodeHandle, to: &NodeHandle) -> EdgeHandle {
        self.flow.edges.push(EdgeIR {
            from: from.alias.clone(),
            to: to.alias.clone(),
            delivery: Delivery::AtLeastOnce,
            ordering: Default::default(),
            partition_key: None,
            timeout_ms: None,
            buffer: BufferPolicy::default(),
        });
        EdgeHandle {
            from: from.alias.clone(),
            to: to.alias.clone(),
        }
    }

    /// Update the timeout budget for an existing edge.
    pub fn set_edge_timeout_ms(
        &mut self,
        from: &NodeHandle,
        to: &NodeHandle,
        timeout_ms: u64,
    ) -> Result<(), FlowBuilderError> {
        let mut updated = false;
        for edge in &mut self.flow.edges {
            if edge.from == from.alias && edge.to == to.alias {
                edge.timeout_ms = Some(timeout_ms);
                updated = true;
            }
        }
        if updated {
            Ok(())
        } else {
            Err(FlowBuilderError::UnknownEdge {
                from: from.alias.clone(),
                to: to.alias.clone(),
            })
        }
    }

    /// Finalise and return the constructed Flow IR.
    pub fn build(self) -> FlowIR {
        self.flow
    }
}
