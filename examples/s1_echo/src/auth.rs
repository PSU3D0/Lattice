use std::cell::RefCell;
use std::sync::Arc;

use host_inproc::{EnvironmentPlugin, InvocationMetadata};
use serde::{Deserialize, Serialize};

thread_local! {
    static AUTH_CONTEXT: RefCell<Option<AuthUser>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthUser {
    pub sub: String,
    pub email: Option<String>,
}

pub fn current_user() -> Option<AuthUser> {
    AUTH_CONTEXT.with(|ctx| ctx.borrow().clone())
}

fn set_current_user(user: Option<AuthUser>) {
    AUTH_CONTEXT.with(|ctx| *ctx.borrow_mut() = user);
}

#[derive(Default)]
pub struct AuthEnvironmentPlugin;

impl EnvironmentPlugin for AuthEnvironmentPlugin {
    fn before_execute(&self, metadata: &InvocationMetadata) {
        let user = metadata
            .extensions()
            .get("auth.user")
            .and_then(|value| serde_json::from_value::<AuthUser>(value.clone()).ok());
        set_current_user(user);
    }

    fn after_execute(
        &self,
        _metadata: &InvocationMetadata,
        _outcome: Result<&host_inproc::HostExecutionResult, &host_inproc::HostExecutionError>,
    ) {
        set_current_user(None);
    }
}

pub fn environment_plugins() -> Vec<Arc<dyn EnvironmentPlugin>> {
    vec![Arc::new(AuthEnvironmentPlugin)]
}
