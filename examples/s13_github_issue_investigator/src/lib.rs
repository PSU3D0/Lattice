use capabilities::connector::{
    EndpointProfileDescriptor, OutboundAuthKind, OutboundAuthProfileDescriptor,
};
use capabilities::context;
use capabilities::durability::TokenConfig;
use connectors_std::openai::{OpenAiFallback, env_or_default, resolve_openai_settings};
use dag_core::{
    ConnectorOpMetadata, ConnectorRoleKindDecl, ConnectorRoleRequirement, Determinism, Effects,
    NodeError, NodeResult,
};
use dag_macros::{def_node, node};
use dispatch_backend::{
    DispatchBackend, DispatchReceipt, DispatchRequest, HttpDispatchBackend,
    ResourceAccessDispatchHost, TrackingMode,
};
use llm_agent::prelude::{CompletionClient, TypedPrompt};
use llm_lattice::LatticeHttpClient;
use llm_provider_openai::{Client as OpenAIClient, GPT_5_4_MINI};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const DEFAULT_OPENAI_API_KEY: &str = "test-key";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_OPENAI_TEXT_MODEL: &str = GPT_5_4_MINI;
const OPENAI_CONNECTOR_ID: &str = "connector.openai";
const DEFAULT_SANDBOX_DISPATCH_URL: &str = "https://sandbox.invalid/jobs/investigate";
const DEFAULT_RESUME_CALLBACK_URL: &str = "https://host.invalid/__lattice/resume";

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

struct OpenAiIssueTriageOp;
impl OpenAiIssueTriageOp {
    pub const META: ConnectorOpMetadata = ConnectorOpMetadata {
        operation_id: "connector.openai.issue_triage",
        connector_id: OPENAI_CONNECTOR_ID,
        summary: "Classify a GitHub issue into a typed triage decision",
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

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct IssueComment {
    pub author: String,
    pub body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct GithubIssueInput {
    pub owner: String,
    pub repo: String,
    pub issue_number: u64,
    pub title: String,
    pub body: String,
    #[serde(default)]
    pub comments: Vec<IssueComment>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IssueCategory {
    Bug,
    FeatureRequest,
    Question,
    Docs,
    NeedsInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IssueSeverity {
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct TriageDecision {
    pub category: IssueCategory,
    pub severity: IssueSeverity,
    pub needs_investigation: bool,
    #[serde(default)]
    pub suggested_labels: Vec<String>,
    pub rationale: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct TriagedIssue {
    pub issue: GithubIssueInput,
    pub triage: TriageDecision,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationPolicy {
    pub max_steps: u32,
    pub allow_shell: bool,
    pub allow_test_runs: bool,
    pub allow_patch_proposal: bool,
    pub max_wall_clock_seconds: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationPlan {
    pub issue: GithubIssueInput,
    pub triage: TriageDecision,
    pub policy: InvestigationPolicy,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SandboxRepoRef {
    pub owner: String,
    pub name: String,
    pub r#ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SandboxCallbackAuth {
    pub kind: String,
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SandboxCallbackTarget {
    pub url: String,
    pub auth: SandboxCallbackAuth,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    pub source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SandboxInvestigationRequest {
    pub repo: SandboxRepoRef,
    pub issue: GithubIssueInput,
    pub triage: TriageDecision,
    pub policy: InvestigationPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationDispatchEnvelope {
    pub contract_version: String,
    pub job_kind: String,
    pub job_id: String,
    pub request: SandboxInvestigationRequest,
    pub callback: SandboxCallbackTarget,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationFinding {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ProposedAction {
    pub kind: String,
    pub body: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationArtifactRef {
    pub kind: String,
    pub uri: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationResult {
    pub summary: String,
    pub confidence: f32,
    #[serde(default)]
    pub findings: Vec<InvestigationFinding>,
    #[serde(default)]
    pub proposed_actions: Vec<ProposedAction>,
    #[serde(default)]
    pub artifacts: Vec<InvestigationArtifactRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationDispatchState {
    pub plan: InvestigationPlan,
    pub job_id: String,
    pub dispatch: InvestigationDispatchEnvelope,
    pub dispatch_receipt: DispatchReceipt,
    pub resume_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InvestigationResumeState {
    pub plan: InvestigationPlan,
    pub job_id: String,
    pub result: InvestigationResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum InvestigationAwaitOutput {
    Waiting(InvestigationDispatchState),
    Completed(InvestigationResumeState),
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionKind {
    InvestigationCompleted,
    RequestMoreInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct IssueInvestigatorOutcome {
    pub issue_number: u64,
    pub owner: String,
    pub repo: String,
    pub triage: TriageDecision,
    pub resolution: ResolutionKind,
    pub note: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub investigation: Option<InvestigationResult>,
}

async fn openai_client() -> NodeResult<OpenAIClient<LatticeHttpClient>> {
    let settings = resolve_openai_settings(
        OpenAiIssueTriageOp::META.operation_id,
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

fn sandbox_dispatch_url() -> String {
    #[cfg(target_arch = "wasm32")]
    {
        DEFAULT_SANDBOX_DISPATCH_URL.to_string()
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var("LATTICE_SANDBOX_DISPATCH_URL")
            .unwrap_or_else(|_| DEFAULT_SANDBOX_DISPATCH_URL.to_string())
    }
}

fn resume_callback_url() -> String {
    #[cfg(target_arch = "wasm32")]
    {
        DEFAULT_RESUME_CALLBACK_URL.to_string()
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var("LATTICE_RESUME_CALLBACK_URL")
            .unwrap_or_else(|_| DEFAULT_RESUME_CALLBACK_URL.to_string())
    }
}

fn triage_prompt(issue: &GithubIssueInput) -> String {
    let comments = if issue.comments.is_empty() {
        "- no comments yet".to_string()
    } else {
        issue
            .comments
            .iter()
            .map(|comment| format!("- {}: {}", comment.author, comment.body))
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "Classify this GitHub issue for workflow triage.\n\nReturn only the structured fields requested by the schema.\n\nRepository: {}/{}\nIssue #: {}\nTitle: {}\nBody: {}\nComments:\n{}",
        issue.owner, issue.repo, issue.issue_number, issue.title, issue.body, comments
    )
}

fn default_investigation_policy() -> InvestigationPolicy {
    InvestigationPolicy {
        max_steps: 8,
        allow_shell: true,
        allow_test_runs: true,
        allow_patch_proposal: true,
        max_wall_clock_seconds: 900,
    }
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

#[def_node(
    trigger,
    name = "GitHubIssueTrigger",
    summary = "Ingress trigger for a GitHub-issue-like payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn github_issue_trigger(input: GithubIssueInput) -> NodeResult<GithubIssueInput> {
    Ok(input)
}

#[def_node(
    name = "TriageIssueAgent",
    summary = "Produce a typed triage decision for an incoming issue",
    effects = "Effectful",
    determinism = "Nondeterministic",
    resources(http_write(capabilities::http::HttpWrite)),
    connector_ops(OpenAiIssueTriageOp)
)]
async fn triage_issue_agent(issue: GithubIssueInput) -> NodeResult<TriagedIssue> {
    let client = openai_client().await?;
    let prompt = triage_prompt(&issue);

    let triage = client
        .agent(openai_text_model())
        .preamble(
            "You are a GitHub issue triage assistant. Classify the issue, estimate severity, and decide whether it deserves deeper repo-local investigation. Return only the structured fields requested by the schema.",
        )
        .build()
        .prompt_typed::<TriageDecision>(prompt)
        .await
        .map_err(node_error)?;

    Ok(TriagedIssue { issue, triage })
}

#[def_node(
    name = "PrepareInvestigationRequest",
    summary = "Convert the triaged issue into an external investigation plan",
    effects = "Pure",
    determinism = "Strict"
)]
async fn prepare_investigation_request(input: TriagedIssue) -> NodeResult<InvestigationPlan> {
    Ok(InvestigationPlan {
        idempotency_key: format!(
            "{}:{}:{}",
            input.issue.owner, input.issue.repo, input.issue.issue_number
        ),
        issue: input.issue,
        triage: input.triage,
        policy: default_investigation_policy(),
    })
}

#[def_node(
    name = "DispatchInvestigationJob",
    summary = "Dispatch an external sandbox job and halt awaiting callback resume",
    effects = "Effectful",
    determinism = "Nondeterministic",
    idempotency(key = "idempotency_key", scope = "Node", ttl_ms = 900_000),
    resources(http_write(capabilities::http::HttpWrite)),
    halts = true
)]
async fn dispatch_investigation_job(
    plan: InvestigationPlan,
) -> NodeResult<InvestigationAwaitOutput> {
    let timeout = std::time::Duration::from_secs(plan.policy.max_wall_clock_seconds as u64);
    let metadata = serde_json::json!({
        "purpose": "external_sandbox_callback",
        "job_kind": "github_issue_investigation",
        "issue_number": plan.issue.issue_number,
        "repo": format!("{}/{}", plan.issue.owner, plan.issue.repo),
    });

    context::with_current_async(|resources| async move {
        let signal_source = resources.resume_signal_source().ok_or_else(|| {
            NodeError::new("dispatch_investigation_job requires ResumeSignalSource")
        })?;
        let handle = context::current_checkpoint_handle().ok_or_else(|| {
            NodeError::new("dispatch_investigation_job missing checkpoint handle")
        })?;
        let token = signal_source
            .create_token(
                &handle,
                TokenConfig {
                    ttl: Some(timeout),
                    single_use: true,
                    metadata: Some(metadata),
                },
            )
            .await
            .map_err(|err| NodeError::new(format!("dispatch token error: {err}")))?;

        let job_id = format!("issue-investigation-{}", handle.checkpoint_id);
        let envelope = InvestigationDispatchEnvelope {
            contract_version: "0.1".to_string(),
            job_kind: "github_issue_investigation".to_string(),
            job_id: job_id.clone(),
            request: SandboxInvestigationRequest {
                repo: SandboxRepoRef {
                    owner: plan.issue.owner.clone(),
                    name: plan.issue.repo.clone(),
                    r#ref: "main".to_string(),
                },
                issue: plan.issue.clone(),
                triage: plan.triage.clone(),
                policy: plan.policy.clone(),
            },
            callback: SandboxCallbackTarget {
                url: resume_callback_url(),
                auth: SandboxCallbackAuth {
                    kind: "bearer_resume_token".to_string(),
                    token: token.0.clone(),
                },
                expires_at: None,
                source: "sandbox_dispatch".to_string(),
            },
        };

        let backend = HttpDispatchBackend::new(
            "sandbox_http",
            sandbox_dispatch_url(),
            TrackingMode::CallbackOnly,
        );
        let host = ResourceAccessDispatchHost::new(resources.as_ref());
        let dispatch_receipt = backend
            .dispatch(
                &host,
                DispatchRequest {
                    job_kind: "github_issue_investigation".to_string(),
                    job_id: job_id.clone(),
                    payload: serde_json::to_value(&envelope).map_err(|err| {
                        NodeError::new(format!("serialize dispatch payload: {err}"))
                    })?,
                },
            )
            .await
            .map_err(node_error)?;

        Ok(InvestigationAwaitOutput::Waiting(
            InvestigationDispatchState {
                plan,
                job_id,
                dispatch: envelope,
                dispatch_receipt,
                resume_token: token.0,
            },
        ))
    })
    .await
    .ok_or_else(|| NodeError::new("dispatch_investigation_job missing ResourceAccess context"))?
}

#[def_node(
    name = "ReviewInvestigationResult",
    summary = "Review the resumed sandbox result and convert it to final typed output",
    effects = "Pure",
    determinism = "Strict"
)]
async fn review_investigation_result(
    wait: InvestigationAwaitOutput,
) -> NodeResult<IssueInvestigatorOutcome> {
    let completed = match wait {
        InvestigationAwaitOutput::Completed(completed) => completed,
        InvestigationAwaitOutput::Waiting(_) => {
            return Err(NodeError::new(
                "review_investigation_result expected a completed investigation payload",
            ));
        }
    };

    Ok(IssueInvestigatorOutcome {
        issue_number: completed.plan.issue.issue_number,
        owner: completed.plan.issue.owner,
        repo: completed.plan.issue.repo,
        triage: completed.plan.triage,
        resolution: ResolutionKind::InvestigationCompleted,
        note: "External sandbox investigation completed and resumed the workflow.".to_string(),
        investigation: Some(completed.result),
    })
}

#[def_node(
    name = "RequestMoreInfo",
    summary = "Choose the lighter path when the issue does not yet warrant deep investigation",
    effects = "Pure",
    determinism = "Strict"
)]
async fn request_more_info(input: TriagedIssue) -> NodeResult<IssueInvestigatorOutcome> {
    Ok(IssueInvestigatorOutcome {
        issue_number: input.issue.issue_number,
        owner: input.issue.owner,
        repo: input.issue.repo,
        triage: input.triage,
        resolution: ResolutionKind::RequestMoreInfo,
        note: "Ask for more detail or route to lightweight human review before spending sandbox capacity.".to_string(),
        investigation: None,
    })
}

#[def_node(
    name = "Capture",
    summary = "Capture the example's final issue-investigator outcome",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(output: IssueInvestigatorOutcome) -> NodeResult<IssueInvestigatorOutcome> {
    Ok(output)
}

dag_macros::flow! {
    name: s13_github_issue_investigator_flow,
    version: "1.0.0",
    profile: Web,
    summary: "GitHub issue investigator with typed AI triage, explicit external dispatch, and callback-resume-ready orchestration";

    let trigger = node!(github_issue_trigger);
    let triage = node!(triage_issue_agent);
    let prepare_investigation_request = node!(prepare_investigation_request);
    let dispatch_investigation_job = node!(dispatch_investigation_job);
    let review_investigation_result = node!(review_investigation_result);
    let request_more_info = node!(request_more_info);
    let capture = node!(capture);

    connect!(trigger -> triage);
    connect!(triage -> prepare_investigation_request);
    connect!(triage -> request_more_info);

    if_!(
        source = triage,
        selector_pointer = "/triage/needs_investigation",
        then = prepare_investigation_request,
        else = request_more_info
    );

    connect!(prepare_investigation_request -> dispatch_investigation_job);
    connect!(dispatch_investigation_job -> review_investigation_result);
    connect!(review_investigation_result -> capture);
    connect!(request_more_info -> capture);

    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/github/issues"],
        method: "POST",
        deadline_ms: 5_000,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    };
    use std::time::Duration;

    use async_trait::async_trait;
    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::Capability;
    use capabilities::connector::{
        ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, ResolvedEndpointProfile,
    };
    use capabilities::durability::{
        CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, Lease,
        ResumeSignalSource, ResumeToken, TokenError,
    };
    use capabilities::http::HttpRequest;
    use capabilities::{ResourceBag, context};
    use host_inproc::{HostExecutionResult, HostRuntime, Invocation};
    use httpmock::{Method::POST, Mock, MockServer};
    use serde_json::json;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    struct TestConnectorRuntime;

    #[async_trait]
    impl ConnectorRuntime for TestConnectorRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &capabilities::connector::OutboundAuthProfileDescriptor,
            request: &mut HttpRequest,
        ) -> Result<(), ConnectorRuntimeError> {
            let token = std::env::var(profile.env_var)
                .unwrap_or_else(|_| DEFAULT_OPENAI_API_KEY.to_string());
            request
                .headers
                .insert("authorization".to_string(), format!("Bearer {token}"));
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &capabilities::connector::EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
            Ok(ResolvedEndpointProfile {
                base_url: std::env::var(profile.env_base_url_var)
                    .unwrap_or_else(|_| profile.base_url.to_string()),
                default_headers: profile
                    .default_headers
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
            })
        }
    }

    #[derive(Default)]
    struct TestCheckpointStore {
        records: Mutex<BTreeMap<String, CheckpointRecord>>,
    }

    impl Capability for TestCheckpointStore {
        fn name(&self) -> &'static str {
            "test-checkpoint-store"
        }
    }

    #[async_trait]
    impl capabilities::durability::CheckpointStore for TestCheckpointStore {
        async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError> {
            let handle = CheckpointHandle {
                checkpoint_id: record.checkpoint_id.clone(),
                flow_id: record.flow_id.clone(),
                run_id: record.run_id.clone(),
            };
            self.records
                .lock()
                .expect("checkpoint records")
                .insert(record.checkpoint_id.clone(), record);
            Ok(handle)
        }

        async fn get(
            &self,
            handle: &CheckpointHandle,
        ) -> Result<CheckpointRecord, CheckpointError> {
            self.records
                .lock()
                .expect("checkpoint records")
                .get(&handle.checkpoint_id)
                .cloned()
                .ok_or(CheckpointError::NotFound)
        }

        async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError> {
            self.records
                .lock()
                .expect("checkpoint records")
                .remove(&handle.checkpoint_id)
                .map(|_| ())
                .ok_or(CheckpointError::NotFound)
        }

        async fn lease(
            &self,
            handle: &CheckpointHandle,
            ttl: Duration,
        ) -> Result<Lease, CheckpointError> {
            Ok(Lease {
                lease_id: format!("lease:{}", handle.checkpoint_id),
                expires_at_ms: ttl.as_millis().try_into().unwrap_or(u64::MAX),
            })
        }

        async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn list(
            &self,
            filter: CheckpointFilter,
        ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
            let records = self.records.lock().expect("checkpoint records");
            Ok(records
                .values()
                .filter(|record| {
                    filter
                        .flow_id
                        .as_ref()
                        .map(|flow_id| &record.flow_id == flow_id)
                        .unwrap_or(true)
                        && filter
                            .run_id
                            .as_ref()
                            .map(|run_id| &record.run_id == run_id)
                            .unwrap_or(true)
                })
                .map(|record| CheckpointHandle {
                    checkpoint_id: record.checkpoint_id.clone(),
                    flow_id: record.flow_id.clone(),
                    run_id: record.run_id.clone(),
                })
                .collect())
        }
    }

    #[derive(Default)]
    struct TestResumeSignalSource {
        counter: AtomicU64,
        tokens: Mutex<BTreeMap<String, CheckpointHandle>>,
    }

    impl Capability for TestResumeSignalSource {
        fn name(&self) -> &'static str {
            "test-resume-signal-source"
        }
    }

    #[async_trait]
    impl ResumeSignalSource for TestResumeSignalSource {
        async fn create_token(
            &self,
            handle: &CheckpointHandle,
            _config: TokenConfig,
        ) -> Result<ResumeToken, TokenError> {
            let id = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
            let token = format!("resume-token-{id}");
            self.tokens
                .lock()
                .expect("resume token map")
                .insert(token.clone(), handle.clone());
            Ok(ResumeToken(token))
        }

        async fn resolve_token(&self, token: &ResumeToken) -> Result<CheckpointHandle, TokenError> {
            self.tokens
                .lock()
                .expect("resume token map")
                .get(&token.0)
                .cloned()
                .ok_or(TokenError::NotFound)
        }

        async fn revoke_token(&self, token: &ResumeToken) -> Result<(), TokenError> {
            self.tokens
                .lock()
                .expect("resume token map")
                .remove(&token.0)
                .map(|_| ())
                .ok_or(TokenError::NotFound)
        }
    }

    fn resource_bag(
        checkpoint_store: Arc<TestCheckpointStore>,
        resume_source: Arc<TestResumeSignalSource>,
    ) -> ResourceBag {
        let http = Arc::new(ReqwestHttpClient::default());
        ResourceBag::new()
            .with_http_read(Arc::clone(&http))
            .with_http_write(http)
            .with_checkpoint_store(checkpoint_store)
            .with_resume_signal_source(resume_source)
            .with_connector_runtime(Arc::new(TestConnectorRuntime))
    }

    fn runtime(
        checkpoint_store: Arc<TestCheckpointStore>,
        resume_source: Arc<TestResumeSignalSource>,
    ) -> HostRuntime {
        let bundle = bundle();
        HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir))
            .with_resource_bag(resource_bag(checkpoint_store, resume_source))
    }

    fn sample_issue() -> GithubIssueInput {
        GithubIssueInput {
            owner: "PSU3D0".to_string(),
            repo: "Lattice".to_string(),
            issue_number: 417,
            title: "panic when config file is missing".to_string(),
            body: "The CLI panics when a config file is absent instead of returning a typed error."
                .to_string(),
            comments: vec![IssueComment {
                author: "maintainer".to_string(),
                body: "Please confirm whether this reproduces on main.".to_string(),
            }],
        }
    }

    fn investigation_triage() -> TriageDecision {
        TriageDecision {
            category: IssueCategory::Bug,
            severity: IssueSeverity::High,
            needs_investigation: true,
            suggested_labels: vec!["bug".to_string(), "investigate".to_string()],
            rationale: "This appears reproducible, likely actionable, and worth deeper repo-local analysis.".to_string(),
        }
    }

    fn needs_info_triage() -> TriageDecision {
        TriageDecision {
            category: IssueCategory::NeedsInfo,
            severity: IssueSeverity::Low,
            needs_investigation: false,
            suggested_labels: vec!["needs-info".to_string()],
            rationale: "The report lacks enough concrete reproduction detail to justify sandbox investigation yet.".to_string(),
        }
    }

    fn sample_investigation_result() -> InvestigationResult {
        InvestigationResult {
            summary: "Likely null dereference in config loader when the file is absent."
                .to_string(),
            confidence: 0.83,
            findings: vec![InvestigationFinding {
                kind: "root_cause".to_string(),
                path: Some("src/config/loader.rs".to_string()),
                detail: "Unchecked unwrap after optional file read path.".to_string(),
            }],
            proposed_actions: vec![ProposedAction {
                kind: "comment".to_string(),
                body: "I investigated and found an unchecked unwrap in the config loader."
                    .to_string(),
            }],
            artifacts: vec![InvestigationArtifactRef {
                kind: "report".to_string(),
                uri: "blob://reports/issue-417.json".to_string(),
            }],
        }
    }

    fn openai_response_for_triage(triage: &TriageDecision) -> serde_json::Value {
        let content = serde_json::to_string(triage).expect("serialize triage response");
        json!({
            "id": "chatcmpl-triage",
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
                "prompt_tokens": 14,
                "completion_tokens": 9,
                "total_tokens": 23,
                "prompt_tokens_details": { "cached_tokens": 0 }
            }
        })
    }

    fn mock_openai_triage<'a>(server: &'a MockServer, triage: &'a TriageDecision) -> Mock<'a> {
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .header("authorization", "Bearer test-key")
                .body_contains("TriageDecision");
            then.status(200)
                .json_body(openai_response_for_triage(triage));
        })
    }

    fn mock_sandbox_dispatch(server: &MockServer) -> Mock<'_> {
        server.mock(|when, then| {
            when.method(POST)
                .path("/jobs/investigate")
                .header("content-type", "application/json")
                .body_contains("github_issue_investigation")
                .body_contains("bearer_resume_token")
                .body_contains("/__lattice/resume");
            then.status(202).json_body(json!({ "accepted": true }));
        })
    }

    fn invocation(input: &GithubIssueInput) -> Invocation {
        Invocation::new(
            "trigger",
            "capture",
            serde_json::to_value(input).expect("serialize issue input"),
        )
    }

    #[test]
    fn flow_contains_phase_two_nodes_and_branch_surface() {
        let ir = flow();
        let aliases: Vec<_> = ir.nodes.iter().map(|node| node.alias.as_str()).collect();
        assert!(aliases.contains(&"dispatch_investigation_job"));
        assert!(aliases.contains(&"review_investigation_result"));
        assert_eq!(ir.control_surfaces.len(), 1);
    }

    #[tokio::test]
    async fn triage_agent_marks_issue_for_investigation() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _base = EnvGuard::set("OPENAI_BASE_URL", &format!("{}/v1", server.base_url()));

        let _mock = mock_openai_triage(&server, &investigation_triage());
        let checkpoint_store = Arc::new(TestCheckpointStore::default());
        let resume_source = Arc::new(TestResumeSignalSource::default());

        let triaged = context::with_resources(
            Arc::new(resource_bag(checkpoint_store, resume_source)),
            async {
                triage_issue_agent(sample_issue())
                    .await
                    .expect("triage should succeed")
            },
        )
        .await;

        assert!(triaged.triage.needs_investigation);
        assert_eq!(triaged.triage.category, IssueCategory::Bug);
    }

    #[tokio::test]
    async fn flow_routes_to_request_more_info_without_halting() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let openai = MockServer::start();
        let sandbox = MockServer::start();
        let _base = EnvGuard::set("OPENAI_BASE_URL", &format!("{}/v1", openai.base_url()));
        let _dispatch = EnvGuard::set(
            "LATTICE_SANDBOX_DISPATCH_URL",
            &format!("{}/jobs/investigate", sandbox.base_url()),
        );
        let _callback = EnvGuard::set("LATTICE_RESUME_CALLBACK_URL", DEFAULT_RESUME_CALLBACK_URL);

        let _mock_openai = mock_openai_triage(&openai, &needs_info_triage());
        let checkpoint_store = Arc::new(TestCheckpointStore::default());
        let resume_source = Arc::new(TestResumeSignalSource::default());
        let runtime = runtime(checkpoint_store, resume_source);

        let outcome = runtime
            .execute(invocation(&sample_issue()))
            .await
            .expect("run ok");
        match outcome {
            HostExecutionResult::Value(value) => {
                let outcome: IssueInvestigatorOutcome =
                    serde_json::from_value(value).expect("decode outcome");
                assert_eq!(outcome.resolution, ResolutionKind::RequestMoreInfo);
                assert!(outcome.investigation.is_none());
            }
            _ => panic!("unexpected non-value execution result"),
        }
    }

    #[tokio::test]
    async fn investigation_path_dispatches_and_halts_with_resume_token() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let openai = MockServer::start();
        let sandbox = MockServer::start();
        let _base = EnvGuard::set("OPENAI_BASE_URL", &format!("{}/v1", openai.base_url()));
        let _dispatch = EnvGuard::set(
            "LATTICE_SANDBOX_DISPATCH_URL",
            &format!("{}/jobs/investigate", sandbox.base_url()),
        );
        let _callback = EnvGuard::set("LATTICE_RESUME_CALLBACK_URL", DEFAULT_RESUME_CALLBACK_URL);

        let _mock_openai = mock_openai_triage(&openai, &investigation_triage());
        let sandbox_mock = mock_sandbox_dispatch(&sandbox);
        let checkpoint_store = Arc::new(TestCheckpointStore::default());
        let resume_source = Arc::new(TestResumeSignalSource::default());
        let runtime = runtime(Arc::clone(&checkpoint_store), Arc::clone(&resume_source));

        let halted = runtime
            .execute(invocation(&sample_issue()))
            .await
            .expect("run ok");
        let wait = match halted {
            HostExecutionResult::Halt { alias, payload } => {
                assert_eq!(alias, "dispatch_investigation_job");
                serde_json::from_value::<InvestigationAwaitOutput>(payload)
                    .expect("decode halt payload")
            }
            _ => panic!("expected halt result"),
        };

        sandbox_mock.assert_hits(1);
        let waiting = match wait {
            InvestigationAwaitOutput::Waiting(waiting) => waiting,
            InvestigationAwaitOutput::Completed(_) => panic!("expected waiting state"),
        };
        assert!(waiting.resume_token.starts_with("resume-token-"));
        assert_eq!(waiting.dispatch_receipt.backend_kind, "sandbox_http");
    }

    #[tokio::test]
    async fn investigation_path_resumes_with_callback_result() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let openai = MockServer::start();
        let sandbox = MockServer::start();
        let _base = EnvGuard::set("OPENAI_BASE_URL", &format!("{}/v1", openai.base_url()));
        let _dispatch = EnvGuard::set(
            "LATTICE_SANDBOX_DISPATCH_URL",
            &format!("{}/jobs/investigate", sandbox.base_url()),
        );
        let _callback = EnvGuard::set("LATTICE_RESUME_CALLBACK_URL", DEFAULT_RESUME_CALLBACK_URL);

        let _mock_openai = mock_openai_triage(&openai, &investigation_triage());
        let _mock_sandbox = mock_sandbox_dispatch(&sandbox);
        let checkpoint_store = Arc::new(TestCheckpointStore::default());
        let resume_source = Arc::new(TestResumeSignalSource::default());
        let runtime = runtime(Arc::clone(&checkpoint_store), Arc::clone(&resume_source));

        let halted = runtime
            .execute(invocation(&sample_issue()))
            .await
            .expect("run ok");
        let (wait, checkpoint_id) = match halted {
            HostExecutionResult::Halt { alias, payload } => {
                assert_eq!(alias, "dispatch_investigation_job");
                let checkpoint_id = payload
                    .get("checkpoint_id")
                    .and_then(|value| value.as_str())
                    .expect("checkpoint_id in halt payload")
                    .to_string();
                let wait = serde_json::from_value::<InvestigationAwaitOutput>(payload)
                    .expect("decode halt payload");
                (wait, checkpoint_id)
            }
            _ => panic!("expected halt result"),
        };

        let waiting = match wait {
            InvestigationAwaitOutput::Waiting(waiting) => waiting,
            InvestigationAwaitOutput::Completed(_) => panic!("expected waiting state"),
        };

        let token = waiting.resume_token.clone();
        let resolved = resume_source
            .resolve_token(&ResumeToken(token))
            .await
            .expect("resolve token");
        assert_eq!(resolved.checkpoint_id, checkpoint_id);

        let resumed_payload = InvestigationAwaitOutput::Completed(InvestigationResumeState {
            plan: waiting.plan.clone(),
            job_id: waiting.job_id.clone(),
            result: sample_investigation_result(),
        });

        let resumed = runtime
            .resume_with_payload(
                &resolved.checkpoint_id,
                serde_json::to_value(resumed_payload).expect("serialize resumed payload"),
            )
            .await
            .expect("resume ok");

        match resumed {
            HostExecutionResult::Value(value) => {
                let outcome: IssueInvestigatorOutcome =
                    serde_json::from_value(value).expect("decode resumed outcome");
                assert_eq!(outcome.resolution, ResolutionKind::InvestigationCompleted);
                let result = outcome.investigation.expect("investigation result");
                assert!(result.summary.contains("config loader"));
                assert_eq!(result.findings.len(), 1);
            }
            _ => panic!("expected resumed value"),
        }
    }
}
