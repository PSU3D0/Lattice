use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{NotionCreatePageInput, NotionCreatePageOutput};
use crate::ops::NotionCreatePage;

#[def_node(
    name = "NotionCreatePage",
    summary = "Create one page in a Notion database",
    identifier = "connector.notion.create_page",
    connector_ops(crate::ops::NotionCreatePage)
)]
pub async fn notion_create_page(
    input: NotionCreatePageInput,
) -> NodeResult<NotionCreatePageOutput> {
    NotionCreatePage::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.notion.create_page failed: {err}")))
}
