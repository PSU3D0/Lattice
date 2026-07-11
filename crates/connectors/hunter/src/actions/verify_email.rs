use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{HunterVerifyEmailInput, HunterVerifyEmailOutput};
use crate::ops::HunterVerifyEmail;

#[def_node(
    name = "HunterVerifyEmail",
    summary = "Verify the deliverability of one email address via Hunter.io",
    identifier = "connector.hunter.verify_email",
    connector_ops(crate::ops::HunterVerifyEmail)
)]
pub async fn hunter_verify_email(
    input: HunterVerifyEmailInput,
) -> NodeResult<HunterVerifyEmailOutput> {
    HunterVerifyEmail::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.hunter.verify_email failed: {err}")))
}
