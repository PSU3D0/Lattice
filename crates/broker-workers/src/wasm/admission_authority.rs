use super::*;
use crate::admission_authority::{
    ADMISSION_AUTHORITY_STORAGE_KEY, AdmissionAuthorityCommand, AdmissionAuthorityError,
    AdmissionAuthorityReply, AdmissionAuthorityState, transition,
};
use broker_core::signing::BrokerSigner;
use serde::Serialize;
use worker::durable_object;

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum AdmissionAuthorityEnvelopeReply {
    Accepted { reply: AdmissionAuthorityReply },
    Rejected { error: AdmissionAuthorityError },
}

#[durable_object]
pub struct AdmissionAuthorityDurableObject {
    state: State,
    env: Env,
}

impl worker::DurableObject for AdmissionAuthorityDurableObject {
    fn new(state: State, env: Env) -> Self {
        Self { state, env }
    }

    async fn fetch(&self, mut request: Request) -> worker::Result<Response> {
        let command: AdmissionAuthorityCommand =
            match bounded_json(&mut request, MAX_INVOKE_BODY).await {
                Ok(command) => command,
                Err(_) => {
                    return Response::from_json(&AdmissionAuthorityEnvelopeReply::Rejected {
                        error: AdmissionAuthorityError::InvalidInput,
                    })
                    .map(|response| response.with_status(400));
                }
            };
        let current = self
            .state
            .storage()
            .get::<AdmissionAuthorityState>(ADMISSION_AUTHORITY_STORAGE_KEY)
            .await?;
        let signer = BrokerSigner::from_seed(
            self.env.var("ADMISSION_AUTHORITY_KEY_ID")?.to_string(),
            secret_32(&self.env, "ADMISSION_AUTHORITY_SIGNING_SEED")?,
        );
        match transition(current.as_ref(), command, &signer) {
            Ok(result) => {
                if result.mutated {
                    self.state
                        .storage()
                        .put(
                            ADMISSION_AUTHORITY_STORAGE_KEY,
                            result
                                .state
                                .as_ref()
                                .ok_or_else(|| worker_rust_error("admission authority state"))?,
                        )
                        .await?;
                }
                Response::from_json(&AdmissionAuthorityEnvelopeReply::Accepted {
                    reply: result.reply,
                })
            }
            Err(error) => Response::from_json(&AdmissionAuthorityEnvelopeReply::Rejected { error })
                .map(|response| response.with_status(409)),
        }
    }
}
