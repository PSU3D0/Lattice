use dag_core::{
    ConnectorOpMetadata, ConnectorRoleKindDecl, ConnectorRoleRequirement, Determinism, Effects,
    FlowIR, NodeError, NodeResult,
};
use dag_macros::{def_node, flow_struct};
use kernel_plan::{ValidatedIR, validate};
use llm_agent::image_generation::ImageGenerationModel as _;
use llm_agent::prelude::{CompletionClient, ImageGenerationClient, TypedPrompt};
use llm_lattice::LatticeHttpClient;
use llm_provider_openai::{Client as OpenAIClient, GPT_5_4_MINI, GPT_IMAGE_1_5};

use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};
use connectors_std::openai::{OpenAiFallback, env_or_default, resolve_openai_settings};

const DEFAULT_OPENAI_API_KEY: &str = "test-key";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_OPENAI_TEXT_MODEL: &str = GPT_5_4_MINI;
const DEFAULT_OPENAI_IMAGE_MODEL: &str = GPT_IMAGE_1_5;
const OPENAI_CONNECTOR_ID: &str = "connector.openai";

const OPENAI_ENDPOINT_PROFILE: EndpointProfileDescriptor = EndpointProfileDescriptor {
    connector_id: OPENAI_CONNECTOR_ID,
    name: "default_api",
    env_base_url_var: "OPENAI_BASE_URL",
    base_url: DEFAULT_OPENAI_BASE_URL,
    default_headers: &[],
};

const OPENAI_AUTH_PROFILE: OutboundAuthProfileDescriptor = OutboundAuthProfileDescriptor {
    connector_id: OPENAI_CONNECTOR_ID,
    name: "default_auth",
    env_var: "OPENAI_API_KEY",
    kind: OutboundAuthKind::Bearer {
        handle_kind: "http.bearer",
    },
};

const OPENAI_FALLBACK: OpenAiFallback = OpenAiFallback {
    env_api_key_var: "OPENAI_API_KEY",
    default_api_key: DEFAULT_OPENAI_API_KEY,
    env_base_url_var: "OPENAI_BASE_URL",
    default_base_url: DEFAULT_OPENAI_BASE_URL,
};

struct OpenAiStructuredExtractOp;
impl OpenAiStructuredExtractOp {
    pub const META: ConnectorOpMetadata = ConnectorOpMetadata {
        operation_id: "connector.openai.extract_structured",
        connector_id: OPENAI_CONNECTOR_ID,
        summary: "Extract structured data with OpenAI structured outputs",
        min_effects: Effects::Effectful,
        max_determinism: Determinism::Nondeterministic,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::EndpointProfile,
                name: OPENAI_ENDPOINT_PROFILE.name,
                expected_handle_kind: "endpoint.profile",
            },
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: OPENAI_AUTH_PROFILE.name,
                expected_handle_kind: "http.bearer",
            },
        ],
        resolution: dag_core::ConnectorResolutionContract {
            supported_modes: &[dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };
}

struct OpenAiDraftOp;
impl OpenAiDraftOp {
    pub const META: ConnectorOpMetadata = ConnectorOpMetadata {
        operation_id: "connector.openai.complete",
        connector_id: OPENAI_CONNECTOR_ID,
        summary: "Generate typed text completions with OpenAI",
        min_effects: Effects::Effectful,
        max_determinism: Determinism::Nondeterministic,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::EndpointProfile,
                name: OPENAI_ENDPOINT_PROFILE.name,
                expected_handle_kind: "endpoint.profile",
            },
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: OPENAI_AUTH_PROFILE.name,
                expected_handle_kind: "http.bearer",
            },
        ],
        resolution: dag_core::ConnectorResolutionContract {
            supported_modes: &[dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };
}

struct OpenAiImageGenOp;
impl OpenAiImageGenOp {
    pub const META: ConnectorOpMetadata = ConnectorOpMetadata {
        operation_id: "connector.openai.generate_image",
        connector_id: OPENAI_CONNECTOR_ID,
        summary: "Generate an image with OpenAI image models",
        min_effects: Effects::Effectful,
        max_determinism: Determinism::Nondeterministic,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::EndpointProfile,
                name: OPENAI_ENDPOINT_PROFILE.name,
                expected_handle_kind: "endpoint.profile",
            },
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: OPENAI_AUTH_PROFILE.name,
                expected_handle_kind: "http.bearer",
            },
        ],
        resolution: dag_core::ConnectorResolutionContract {
            supported_modes: &[dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };
}

#[flow_struct]
pub struct LeadSubmission {
    pub name: String,
    pub email: String,
    pub message: String,
}

#[flow_struct]
#[derive(PartialEq, Eq)]
pub struct LeadInfo {
    pub name: String,
    pub email: String,
    pub priority: Priority,
    pub product_interest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeline: Option<String>,
    pub summary: String,
}

#[flow_struct]
#[derive(Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    High,
    Medium,
    Low,
}

#[flow_struct]
#[derive(PartialEq, Eq)]
pub struct OutreachDraft {
    pub subject: String,
    pub body: String,
    pub tone: String,
}

#[flow_struct]
#[derive(PartialEq, Eq)]
pub struct DraftedLead {
    pub lead: LeadInfo,
    pub draft: OutreachDraft,
}

#[flow_struct]
#[derive(PartialEq, Eq)]
pub struct HighPriorityLeadImage {
    pub lead: LeadInfo,
    pub draft: OutreachDraft,
    pub image_bytes: Vec<u8>,
}

#[flow_struct]
#[derive(PartialEq, Eq)]
pub struct StoredLeadPackage {
    pub lead: LeadInfo,
    pub draft: OutreachDraft,
    pub image_artifact_path: String,
}

#[flow_struct]
#[derive(PartialEq, Eq)]
pub struct EmailPackage {
    pub to: String,
    pub subject: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_artifact_path: Option<String>,
    pub priority: Priority,
}

#[def_node(
    trigger,
    name = "LeadSubmissionTrigger",
    summary = "Ingress trigger for the lead intake flow",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn lead_submission_trigger(input: LeadSubmission) -> NodeResult<LeadSubmission> {
    Ok(input)
}

#[def_node(
    name = "ExtractLead",
    summary = "Extract structured lead details from the inbound submission",
    effects = "Effectful",
    determinism = "Nondeterministic",
    resources(http_write(capabilities::http::HttpWrite)),
    connector_ops(OpenAiStructuredExtractOp)
)]
async fn extract_lead(submission: LeadSubmission) -> NodeResult<LeadInfo> {
    let client = openai_client().await?;
    let submission_text = format!(
        "Lead submission:\nname: {}\nemail: {}\nmessage: {}",
        submission.name, submission.email, submission.message
    );

    let agent = client
        .agent(openai_text_model())
        .preamble(
            "Extract a structured lead record. Preserve the contact fields and decide the priority.\n\
             Mark the lead high priority when the message signals urgency, budget, strong intent, or a\n\
             meaningful seat count. Return only the structured fields requested by the schema.",
        )
        .build();

    agent
        .prompt_typed::<LeadInfo>(submission_text)
        .await
        .map_err(node_error)
}

#[def_node(
    name = "DraftOutreach",
    summary = "Draft a personalized outreach email for a high-priority lead",
    effects = "Effectful",
    determinism = "Nondeterministic",
    resources(http_write(capabilities::http::HttpWrite)),
    connector_ops(OpenAiDraftOp)
)]
async fn draft_outreach(lead: LeadInfo) -> NodeResult<DraftedLead> {
    let client = openai_client().await?;
    let prompt = format!(
        "Write a concise outreach email for the following lead.\n\n{}",
        serde_json::to_string_pretty(&lead)
            .map_err(|err| NodeError::new(format!("serialize lead for prompt: {err}")))?
    );

    let agent = client
        .agent(openai_text_model())
        .preamble(
            "You write concise, warm outreach emails for sales follow-up. Return only the structured\n             fields requested by the schema.",
        )
        .build();

    let draft = agent
        .prompt_typed::<OutreachDraft>(prompt)
        .await
        .map_err(node_error)?;

    Ok(DraftedLead { lead, draft })
}

#[def_node(
    name = "GenerateImage",
    summary = "Generate a hero image for the high-priority lead",
    effects = "Effectful",
    determinism = "Nondeterministic",
    resources(http_write(capabilities::http::HttpWrite)),
    connector_ops(OpenAiImageGenOp)
)]
async fn generate_image(input: DraftedLead) -> NodeResult<HighPriorityLeadImage> {
    let client = openai_client().await?;
    let prompt = format!(
        "Create a polished hero image for a high-priority lead.\n\nLead:\n{}\n\nDraft subject: {}\nDraft tone: {}",
        input.lead.name, input.draft.subject, input.draft.tone
    );

    let image = client
        .image_generation_model(openai_image_model())
        .image_generation_request()
        .prompt(&prompt)
        .width(1024)
        .height(1024)
        .send()
        .await
        .map_err(node_error)?;

    Ok(HighPriorityLeadImage {
        lead: input.lead,
        draft: input.draft,
        image_bytes: image.image,
    })
}

#[def_node(
    name = "StoreImage",
    summary = "Persist the generated image bytes into the run-scoped workspace",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_write(capabilities::workspace::Workspace))
)]
async fn store_image(input: HighPriorityLeadImage) -> NodeResult<StoredLeadPackage> {
    let image_path = workspace_image_path(&input.lead);
    let image_bytes = input.image_bytes;
    let written = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("store_image requires Workspace"))?;
        workspace
            .write_normalized(
                &image_path,
                &image_bytes,
                capabilities::workspace::WorkspaceWriteOptions::default(),
            )
            .await
            .map_err(node_error)
    })
    .await
    .ok_or_else(|| NodeError::new("store_image missing ResourceAccess context"))??;

    Ok(StoredLeadPackage {
        lead: input.lead,
        draft: input.draft,
        image_artifact_path: written.path,
    })
}

#[def_node(
    name = "ComposeEmail",
    summary = "Assemble the final email package for the high-priority branch",
    effects = "Pure",
    determinism = "Strict"
)]
async fn compose_email(input: StoredLeadPackage) -> NodeResult<EmailPackage> {
    Ok(EmailPackage {
        to: input.lead.email,
        subject: input.draft.subject,
        body: input.draft.body,
        image_artifact_path: Some(input.image_artifact_path),
        priority: input.lead.priority,
    })
}

#[def_node(
    name = "TemplateResponse",
    summary = "Create the low-priority/default response package",
    effects = "Pure",
    determinism = "Strict"
)]
async fn template_response(lead: LeadInfo) -> NodeResult<EmailPackage> {
    Ok(EmailPackage {
        to: lead.email.clone(),
        subject: format!("Thanks for reaching out, {}", lead.name),
        body: format!(
            "Thanks for reaching out about {}. We received your note: {}. We'll follow up soon.",
            lead.product_interest, lead.summary
        ),
        image_artifact_path: None,
        priority: lead.priority,
    })
}

#[def_node(
    name = "Capture",
    summary = "Capture the final email package",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(package: EmailPackage) -> NodeResult<EmailPackage> {
    Ok(package)
}

mod bundle_def {
    use dag_macros::node;

    dag_macros::flow! {
        name: s11_lead_intake_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Explicit lead intake AI flow with structured extraction, branching, and workspace artifact storage";

        let trigger = node!(lead_submission_trigger);
        let extract = node!(extract_lead);
        let draft = node!(draft_outreach);
        let generate_image = node!(generate_image);
        let store_image = node!(store_image);
        let compose_email = node!(compose_email);
        let template_response = node!(template_response);
        let capture = node!(capture);

        connect!(trigger -> extract);
        connect!(extract -> draft);
        connect!(extract -> template_response);

        switch!(
            source = extract,
            selector_pointer = "/priority",
            cases = { "high" => draft },
            default = template_response
        );

        connect!(draft -> generate_image);
        connect!(generate_image -> store_image);
        connect!(store_image -> compose_email);
        connect!(compose_email -> capture);
        connect!(template_response -> capture);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route_aliases: ["/leads"],
            method: "POST",
            deadline_ms: 5_000,
        });
    }
}

pub fn flow() -> FlowIR {
    bundle_def::flow()
}

pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("s11 lead intake flow should validate")
}

/// Node registration is derived by `flow!` from its `node!(...)` bindings —
/// there is no manual register list to drift from the flow definition.
#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
pub fn bundle() -> host_inproc::FlowBundle {
    bundle_def::bundle()
}

async fn openai_client() -> NodeResult<OpenAIClient<LatticeHttpClient>> {
    let settings = resolve_openai_settings(
        OpenAiDraftOp::META.operation_id,
        &OPENAI_ENDPOINT_PROFILE,
        &OPENAI_AUTH_PROFILE,
        OPENAI_FALLBACK,
    )
    .await?;
    OpenAIClient::<LatticeHttpClient>::builder()
        .base_url(settings.base_url)
        .api_key(settings.api_key)
        .http_client(LatticeHttpClient::from_current_resources().unwrap_or_default())
        .build()
        .map_err(node_error)
}

fn openai_text_model() -> String {
    env_or_default("OPENAI_TEXT_MODEL", DEFAULT_OPENAI_TEXT_MODEL)
}

fn openai_image_model() -> String {
    env_or_default("OPENAI_IMAGE_MODEL", DEFAULT_OPENAI_IMAGE_MODEL)
}

fn workspace_image_path(lead: &LeadInfo) -> String {
    format!(
        "artifacts/lead-intake/{}/hero.png",
        sanitize_path_segment(&lead.email)
    )
}

fn sanitize_path_segment(value: &str) -> String {
    let mut segment = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();

    while segment.contains("--") {
        segment = segment.replace("--", "-");
    }

    let trimmed = segment.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "lead".to_string()
    } else {
        trimmed
    }
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use base64::Engine;
    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::context;
    use capabilities::workspace::{
        Workspace, WorkspaceDeleteResult, WorkspaceEntry, WorkspaceListOptions,
        WorkspaceReadResult, WorkspaceWriteOptions, WorkspaceWriteResult,
    };
    use capabilities::{Capability, ResourceBag};
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Default)]
    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(previous) => unsafe {
                    std::env::set_var(self.key, previous);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    #[derive(Default)]
    struct MemoryWorkspace {
        files: Mutex<BTreeMap<String, Vec<u8>>>,
        clock: Mutex<u64>,
    }

    impl Capability for MemoryWorkspace {
        fn name(&self) -> &'static str {
            "workspace.memory"
        }
    }

    #[async_trait]
    impl Workspace for MemoryWorkspace {
        async fn read_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<Option<WorkspaceReadResult>, capabilities::workspace::WorkspaceError> {
            Ok(self
                .files
                .lock()
                .expect("workspace lock")
                .get(normalized_path)
                .cloned()
                .map(WorkspaceReadResult::Bytes))
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, capabilities::workspace::WorkspaceError> {
            self.files
                .lock()
                .expect("workspace lock")
                .insert(normalized_path.to_string(), data.to_vec());
            let mut clock = self.clock.lock().expect("clock lock");
            *clock += 1;
            Ok(WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: *clock,
            })
        }

        async fn list_normalized(
            &self,
            options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, capabilities::workspace::WorkspaceError> {
            let prefix = options.prefix;
            let files = self.files.lock().expect("workspace lock");
            Ok(files
                .iter()
                .filter(|(path, _)| {
                    prefix
                        .as_deref()
                        .map(|prefix| path.starts_with(prefix))
                        .unwrap_or(true)
                })
                .enumerate()
                .map(|(index, (path, bytes))| WorkspaceEntry {
                    path: path.clone(),
                    size_bytes: bytes.len() as u64,
                    updated_at_ms: index as u64,
                    content_hash: Some(format!("sha256:{}", bytes.len())),
                })
                .collect())
        }

        async fn delete_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<WorkspaceDeleteResult, capabilities::workspace::WorkspaceError> {
            let deleted = self
                .files
                .lock()
                .expect("workspace lock")
                .remove(normalized_path)
                .is_some();
            Ok(WorkspaceDeleteResult { deleted })
        }
    }

    fn resource_bag(workspace: Arc<MemoryWorkspace>) -> ResourceBag {
        let http = Arc::new(ReqwestHttpClient::default());
        ResourceBag::new()
            .with_http_read(Arc::clone(&http))
            .with_http_write(http)
            .with_workspace(workspace)
    }

    async fn execute_flow(input: LeadSubmission, workspace: Arc<MemoryWorkspace>) -> EmailPackage {
        let bundle = bundle();
        let payload = serde_json::to_value(&input).expect("serialize input");
        let output = bundle
            .executor()
            .with_resource_bag(resource_bag(workspace))
            .run_once(&bundle.validated_ir, "trigger", payload, "capture", None)
            .await
            .expect("flow execution should succeed");

        match output {
            host_inproc::HostExecutionResult::Value(value) => {
                serde_json::from_value(value).expect("decode email package")
            }
            host_inproc::HostExecutionResult::Stream(_) => panic!("expected value output"),
            host_inproc::HostExecutionResult::Halt { alias, .. } => {
                panic!("unexpected halt at {alias}")
            }
        }
    }

    fn lead_submission(name: &str, email: &str, message: &str) -> LeadSubmission {
        LeadSubmission {
            name: name.to_string(),
            email: email.to_string(),
            message: message.to_string(),
        }
    }

    fn high_priority_lead() -> LeadInfo {
        LeadInfo {
            name: "Ada Lovelace".to_string(),
            email: "ada@example.test".to_string(),
            priority: Priority::High,
            product_interest: "workflow automation".to_string(),
            seat_count: Some(24),
            timeline: Some("this quarter".to_string()),
            summary: "Needs a fast follow-up for a procurement review.".to_string(),
        }
    }

    fn low_priority_lead() -> LeadInfo {
        LeadInfo {
            name: "Grace Hopper".to_string(),
            email: "grace@example.test".to_string(),
            priority: Priority::Low,
            product_interest: "newsletter updates".to_string(),
            seat_count: None,
            timeline: None,
            summary: "Just exploring the product.".to_string(),
        }
    }

    fn openai_response_for_extraction(lead: &LeadInfo) -> serde_json::Value {
        let content = serde_json::to_string(lead).expect("serialize lead response");
        json!({
            "id": "chatcmpl-extract",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5.4-mini",
            "system_fingerprint": null,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": []
                },
                "logprobs": null,
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 7,
                "total_tokens": 19,
                "prompt_tokens_details": { "cached_tokens": 0 }
            }
        })
    }

    fn openai_response_for_draft(draft: &OutreachDraft) -> serde_json::Value {
        let content = serde_json::to_string(draft).expect("serialize draft response");
        json!({
            "id": "chatcmpl-draft",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5.4-mini",
            "system_fingerprint": null,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": []
                },
                "logprobs": null,
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 16,
                "completion_tokens": 8,
                "total_tokens": 24,
                "prompt_tokens_details": { "cached_tokens": 0 }
            }
        })
    }

    fn mock_openai_routes(
        server: &MockServer,
        lead: &LeadInfo,
        draft: &OutreachDraft,
        image_bytes: &[u8],
    ) {
        let lead_response = openai_response_for_extraction(lead);
        let draft_response = openai_response_for_draft(draft);
        let image_response = json!({
            "created": 1,
            "data": [{
                "b64_json": base64::engine::general_purpose::STANDARD.encode(image_bytes)
            }]
        });

        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .header("authorization", "Bearer test-key")
                .body_contains("LeadInfo");
            then.status(200).json_body(lead_response.clone());
        });

        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .header("authorization", "Bearer test-key")
                .body_contains("OutreachDraft");
            then.status(200).json_body(draft_response.clone());
        });

        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/images/generations")
                .header("authorization", "Bearer test-key")
                .body_contains("gpt-image-1.5");
            then.status(200).json_body(image_response.clone());
        });
    }

    #[tokio::test]
    async fn extract_lead_parses_high_priority_submission() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _base = EnvGuard::set("OPENAI_BASE_URL", &format!("{}/v1", server.base_url()));

        let mock_lead = high_priority_lead();
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .header("authorization", "Bearer test-key")
                .body_contains("LeadInfo");
            then.status(200)
                .json_body(openai_response_for_extraction(&mock_lead));
        });

        let workspace = Arc::new(MemoryWorkspace::default());
        let lead = context::with_resources(Arc::new(resource_bag(workspace)), async {
            extract_lead(lead_submission(
                "Ada Lovelace",
                "ada@example.test",
                "We need help with workflow automation and can move this quarter.",
            ))
            .await
            .expect("lead extraction")
        })
        .await;

        assert_eq!(lead.priority, Priority::High);
        assert_eq!(lead.email, "ada@example.test");
        assert_eq!(lead.product_interest, "workflow automation");
        assert_eq!(lead.seat_count, Some(24));
    }

    #[tokio::test]
    async fn low_priority_default_path_returns_template_email_package() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _base = EnvGuard::set("OPENAI_BASE_URL", &format!("{}/v1", server.base_url()));

        let low_lead = low_priority_lead();
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .header("authorization", "Bearer test-key")
                .body_contains("LeadInfo");
            then.status(200)
                .json_body(openai_response_for_extraction(&low_lead));
        });

        let workspace = Arc::new(MemoryWorkspace::default());
        let output = execute_flow(
            lead_submission(
                "Grace Hopper",
                "grace@example.test",
                "Just exploring the product right now.",
            ),
            workspace,
        )
        .await;

        assert_eq!(output.priority, Priority::Low);
        assert_eq!(output.to, "grace@example.test");
        assert!(output.image_artifact_path.is_none());
        assert!(output.subject.contains("Grace Hopper"));
        assert!(output.body.contains("Just exploring"));
    }

    #[tokio::test]
    async fn high_priority_path_writes_image_to_workspace_and_builds_email_package() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _base = EnvGuard::set("OPENAI_BASE_URL", &format!("{}/v1", server.base_url()));

        let lead = high_priority_lead();
        let draft = OutreachDraft {
            subject: "Fast follow-up for workflow automation".to_string(),
            body: "Hi Ada, we can move quickly and help with the workflow automation review."
                .to_string(),
            tone: "warm".to_string(),
        };
        let image_bytes = b"mock image bytes".to_vec();
        mock_openai_routes(&server, &lead, &draft, &image_bytes);

        let workspace = Arc::new(MemoryWorkspace::default());
        let output = execute_flow(
            lead_submission(
                "Ada Lovelace",
                "ada@example.test",
                "We need help with workflow automation and can move this quarter.",
            ),
            Arc::clone(&workspace),
        )
        .await;

        assert_eq!(output.priority, Priority::High);
        assert_eq!(output.to, "ada@example.test");
        assert_eq!(output.subject, draft.subject);
        assert_eq!(output.body, draft.body);
        assert_eq!(
            output.image_artifact_path.as_deref(),
            Some("artifacts/lead-intake/ada-example-test/hero.png")
        );

        let stored = workspace
            .files
            .lock()
            .expect("workspace lock")
            .get("artifacts/lead-intake/ada-example-test/hero.png")
            .cloned()
            .expect("image stored in workspace");
        assert_eq!(stored, image_bytes);
    }

    #[tokio::test]
    async fn compose_email_builds_the_final_email_package() {
        let package = compose_email(StoredLeadPackage {
            lead: high_priority_lead(),
            draft: OutreachDraft {
                subject: "Fast follow-up for workflow automation".to_string(),
                body: "Hi Ada, we can move quickly and help with the workflow automation review."
                    .to_string(),
                tone: "warm".to_string(),
            },
            image_artifact_path: "artifacts/lead-intake/ada-example-test/hero.png".to_string(),
        })
        .await
        .expect("compose email");

        assert_eq!(package.to, "ada@example.test");
        assert_eq!(package.priority, Priority::High);
        assert_eq!(
            package.image_artifact_path.as_deref(),
            Some("artifacts/lead-intake/ada-example-test/hero.png")
        );
        assert_eq!(package.subject, "Fast follow-up for workflow automation");
    }
}
