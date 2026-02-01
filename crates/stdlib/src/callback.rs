use dag_core::{NodeError, NodeResult};
use dag_macros::node;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::time::Duration;

use capabilities::context;
use capabilities::durability::TokenConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallbackWaitInput {
    #[serde(default, with = "humantime_serde")]
    pub timeout: Option<Duration>,
    #[serde(default)]
    pub context: JsonValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallbackWaitOutput {
    pub resume_token: String,
    pub context: JsonValue,
}

#[node(
    name = "CallbackWait",
    summary = "Pause execution until an external resume signal arrives",
    identifier = "std.callback.wait",
    effects = "Pure",
    determinism = "Nondeterministic",
    halts = true
)]
async fn callback_wait(input: CallbackWaitInput) -> NodeResult<CallbackWaitOutput> {
    let timeout = input.timeout;
    let context_value = input.context;
    let token = context::with_current_async(|resources| async move {
        let source = resources.resume_signal_source().ok_or_else(|| {
            NodeError::new("std.callback.wait requires ResumeSignalSource")
        })?;
        let handle = context::current_checkpoint_handle()
            .ok_or_else(|| NodeError::new("std.callback.wait missing checkpoint handle"))?;
        let metadata = if context_value.is_null() {
            None
        } else {
            Some(context_value)
        };
        source
            .create_token(
                &handle,
                TokenConfig {
                    ttl: timeout,
                    single_use: true,
                    metadata,
                },
            )
            .await
            .map_err(|err| NodeError::new(format!("std.callback.wait token error: {err}")))
    })
    .await
    .ok_or_else(|| NodeError::new("std.callback.wait missing ResourceAccess context"))??;

    Ok(CallbackWaitOutput {
        resume_token: token.0,
        context: input.context,
    })
}
