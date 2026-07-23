use broker_auth::{
    ActivationEngine, ActivationRequest, NextAction, NonceSource, OAuthCallback, TokenService,
};
use broker_core::BrokerError;

/// Provider-neutral worker activation coordinator. Durable adapters persist the
/// opaque action and public snapshot; all profile semantics remain in the
/// approved registry held by the engine.
pub struct ActivationCoordinator {
    engine: ActivationEngine,
}

impl ActivationCoordinator {
    pub fn new(engine: ActivationEngine) -> Self {
        Self { engine }
    }
    pub fn create(
        &mut self,
        request: ActivationRequest,
        now: i64,
        nonces: &mut dyn NonceSource,
    ) -> Result<NextAction, BrokerError> {
        self.engine.create(request, now, nonces)
    }
    pub fn callback(
        &mut self,
        callback: OAuthCallback,
        now: i64,
        token_service: &dyn TokenService,
    ) -> Result<NextAction, BrokerError> {
        self.engine.universal_callback(callback, now, token_service)
    }
    pub fn engine(&self) -> &ActivationEngine {
        &self.engine
    }
    pub fn engine_mut(&mut self) -> &mut ActivationEngine {
        &mut self.engine
    }
}
