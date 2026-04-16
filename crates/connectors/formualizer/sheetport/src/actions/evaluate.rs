use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::ops::SheetPortEvaluate;
use crate::types::{SheetPortEvaluateInput, SheetPortEvaluateOutput};

#[def_node(
    name = "SheetPortEvaluate",
    summary = "Evaluate a SheetPort workbook as a typed semantic function",
    identifier = "connector.formualizer.sheetport.evaluate",
    connector_ops(crate::ops::SheetPortEvaluate)
)]
pub async fn sheetport_evaluate(
    input: SheetPortEvaluateInput,
) -> NodeResult<SheetPortEvaluateOutput> {
    SheetPortEvaluate::invoke(&input).await.map_err(|err| {
        NodeError::new(format!(
            "connector.formualizer.sheetport.evaluate failed: {err}"
        ))
    })
}
