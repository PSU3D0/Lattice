use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{AirtableCreateRecordInput, AirtableCreateRecordOutput};
use crate::ops::AirtableCreateRecord;

#[def_node(
    name = "AirtableCreateRecord",
    summary = "Create one record in an Airtable table",
    identifier = "connector.airtable.create_record",
    connector_ops(crate::ops::AirtableCreateRecord)
)]
pub async fn airtable_create_record(
    input: AirtableCreateRecordInput,
) -> NodeResult<AirtableCreateRecordOutput> {
    AirtableCreateRecord::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.airtable.create_record failed: {err}")))
}
