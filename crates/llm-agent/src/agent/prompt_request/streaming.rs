use crate::{
    compat::{MessageRagTextExt, PromptErrorExt},
    OneOrMany,
    agent::completion::{DynamicContextStore, build_completion_request},
    agent::prompt_request::{HookAction, hooks::PromptHook},
    completion::{Document, GetTokenUsage},
    json_utils,
    message::{AssistantContent, ToolChoice, ToolResult, ToolResultContent, UserContent},
    streaming::{StreamedAssistantContent, StreamedUserContent},
    tool::server::ToolServerHandle,
    wasm_compat::{WasmBoxedFuture, WasmCompatSend},
};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{pin::Pin, sync::Arc};
use tracing::info_span;
use tracing_futures::Instrument;

use super::ToolCallHookAction;
use crate::{
    agent::Agent,
    completion::{CompletionError, CompletionModel, PromptError},
    message::{Message, Text},
    tool::ToolSetError,
};

#[cfg(not(target_arch = "wasm32"))]
pub type StreamingResult<R> =
    Pin<Box<dyn Stream<Item = Result<MultiTurnStreamItem<R>, StreamingError>> + Send>>;

#[cfg(target_arch = "wasm32")]
pub type StreamingResult<R> =
    Pin<Box<dyn Stream<Item = Result<MultiTurnStreamItem<R>, StreamingError>>>>;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
#[non_exhaustive]
pub enum MultiTurnStreamItem<R> {
    /// A streamed assistant content item.
    StreamAssistantItem(StreamedAssistantContent<R>),
    /// A streamed user content item (mostly for tool results).
    StreamUserItem(StreamedUserContent),
    /// The final result from the stream.
    FinalResponse(FinalResponse),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FinalResponse {
    response: String,
    aggregated_usage: crate::completion::Usage,
    #[serde(skip_serializing_if = "Option::is_none")]
    history: Option<Vec<Message>>,
}

impl FinalResponse {
    pub fn empty() -> Self {
        Self {
            response: String::new(),
            aggregated_usage: crate::completion::Usage::new(),
            history: None,
        }
    }

    pub fn response(&self) -> &str {
        &self.response
    }

    pub fn usage(&self) -> crate::completion::Usage {
        self.aggregated_usage
    }

    pub fn history(&self) -> Option<&[Message]> {
        self.history.as_deref()
    }
}

impl<R> MultiTurnStreamItem<R> {
    pub(crate) fn stream_item(item: StreamedAssistantContent<R>) -> Self {
        Self::StreamAssistantItem(item)
    }

    pub fn final_response(response: &str, aggregated_usage: crate::completion::Usage) -> Self {
        Self::FinalResponse(FinalResponse {
            response: response.to_string(),
            aggregated_usage,
            history: None,
        })
    }

    pub fn final_response_with_history(
        response: &str,
        aggregated_usage: crate::completion::Usage,
        history: Option<Vec<Message>>,
    ) -> Self {
        Self::FinalResponse(FinalResponse {
            response: response.to_string(),
            aggregated_usage,
            history,
        })
    }
}

fn merge_reasoning_blocks(
    accumulated_reasoning: &mut Vec<crate::message::Reasoning>,
    incoming: &crate::message::Reasoning,
) {
    let ids_match = |existing: &crate::message::Reasoning| {
        matches!(
            (&existing.id, &incoming.id),
            (Some(existing_id), Some(incoming_id)) if existing_id == incoming_id
        )
    };

    if let Some(existing) = accumulated_reasoning
        .iter_mut()
        .rev()
        .find(|existing| ids_match(existing))
    {
        existing.content.extend(incoming.content.clone());
    } else {
        accumulated_reasoning.push(incoming.clone());
    }
}

/// Build full history for error reporting (input + new messages).
fn build_full_history(
    chat_history: Option<&[Message]>,
    new_messages: Vec<Message>,
) -> Vec<Message> {
    let input = chat_history.unwrap_or(&[]);
    input.iter().cloned().chain(new_messages).collect()
}

/// Combine input history with new messages for building completion requests.
fn build_history_for_request(
    chat_history: Option<&[Message]>,
    new_messages: &[Message],
) -> Vec<Message> {
    let input = chat_history.unwrap_or(&[]);
    input.iter().chain(new_messages.iter()).cloned().collect()
}

async fn cancelled_prompt_error(
    chat_history: Option<&[Message]>,
    new_messages: Vec<Message>,
    reason: String,
) -> StreamingError {
    StreamingError::Prompt(
        PromptError::prompt_cancelled(build_full_history(chat_history, new_messages), reason)
            .into(),
    )
}

fn tool_result_to_user_message(
    id: String,
    call_id: Option<String>,
    tool_result: String,
) -> Message {
    let content = OneOrMany::one(ToolResultContent::text(tool_result));
    let user_content = match call_id {
        Some(call_id) => UserContent::tool_result_with_call_id(id, call_id, content),
        None => UserContent::tool_result(id, content),
    };

    Message::User {
        content: OneOrMany::one(user_content),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StreamingError {
    #[error("CompletionError: {0}")]
    Completion(#[from] CompletionError),
    #[error("PromptError: {0}")]
    Prompt(#[from] Box<PromptError>),
    #[error("ToolSetError: {0}")]
    Tool(#[from] ToolSetError),
}

const UNKNOWN_AGENT_NAME: &str = "Unnamed Agent";

/// A builder for creating prompt requests with customizable options.
/// Uses generics to track which options have been set during the build process.
///
/// If you expect to continuously call tools, you will want to ensure you use the `.multi_turn()`
/// argument to add more turns as by default, it is 0 (meaning only 1 tool round-trip). Otherwise,
/// attempting to await (which will send the prompt request) can potentially return
/// [`crate::completion::request::PromptError::MaxTurnsError`] if the agent decides to call tools
/// back to back.
pub struct StreamingPromptRequest<M, P>
where
    M: CompletionModel,
    P: PromptHook<M> + 'static,
{
    /// The prompt message to send to the model
    prompt: Message,
    /// Optional chat history provided by the caller.
    chat_history: Option<Vec<Message>>,
    /// Maximum Turns for multi-turn conversations (0 means no multi-turn)
    max_turns: usize,

    // Agent data (cloned from agent to allow hook type transitions):
    /// The completion model
    model: Arc<M>,
    /// Agent name for logging
    agent_name: Option<String>,
    /// System prompt
    preamble: Option<String>,
    /// Static context documents
    static_context: Vec<Document>,
    /// Temperature setting
    temperature: Option<f64>,
    /// Max tokens setting
    max_tokens: Option<u64>,
    /// Additional model parameters
    additional_params: Option<serde_json::Value>,
    /// Tool server handle for tool execution
    tool_server_handle: ToolServerHandle,
    /// Dynamic context store
    dynamic_context: DynamicContextStore,
    /// Tool choice setting
    tool_choice: Option<ToolChoice>,
    /// Optional JSON Schema for structured output
    output_schema: Option<schemars::Schema>,
    /// Optional per-request hook for events
    hook: Option<P>,
}

impl<M, P> StreamingPromptRequest<M, P>
where
    M: CompletionModel + 'static,
    <M as CompletionModel>::StreamingResponse: WasmCompatSend + GetTokenUsage,
    P: PromptHook<M>,
{
    /// Create a new StreamingPromptRequest with the given prompt and model.
    /// Note: This creates a request without an agent hook. Use `from_agent` to include the agent's hook.
    pub fn new(agent: Arc<Agent<M>>, prompt: impl Into<Message>) -> StreamingPromptRequest<M, ()> {
        StreamingPromptRequest {
            prompt: prompt.into(),
            chat_history: None,
            max_turns: agent.default_max_turns.unwrap_or_default(),
            model: agent.model.clone(),
            agent_name: agent.name.clone(),
            preamble: agent.preamble.clone(),
            static_context: agent.static_context.clone(),
            temperature: agent.temperature,
            max_tokens: agent.max_tokens,
            additional_params: agent.additional_params.clone(),
            tool_server_handle: agent.tool_server_handle.clone(),
            dynamic_context: agent.dynamic_context.clone(),
            tool_choice: agent.tool_choice.clone(),
            output_schema: agent.output_schema.clone(),
            hook: None,
        }
    }

    /// Create a new StreamingPromptRequest from an agent, cloning the agent's data and default hook.
    pub fn from_agent<P2>(
        agent: &Agent<M, P2>,
        prompt: impl Into<Message>,
    ) -> StreamingPromptRequest<M, P2>
    where
        P2: PromptHook<M>,
    {
        StreamingPromptRequest {
            prompt: prompt.into(),
            chat_history: None,
            max_turns: agent.default_max_turns.unwrap_or_default(),
            model: agent.model.clone(),
            agent_name: agent.name.clone(),
            preamble: agent.preamble.clone(),
            static_context: agent.static_context.clone(),
            temperature: agent.temperature,
            max_tokens: agent.max_tokens,
            additional_params: agent.additional_params.clone(),
            tool_server_handle: agent.tool_server_handle.clone(),
            dynamic_context: agent.dynamic_context.clone(),
            tool_choice: agent.tool_choice.clone(),
            output_schema: agent.output_schema.clone(),
            hook: agent.hook.clone(),
        }
    }

    fn agent_name(&self) -> &str {
        self.agent_name.as_deref().unwrap_or(UNKNOWN_AGENT_NAME)
    }

    /// Set the maximum Turns for multi-turn conversations (ie, the maximum number of turns an LLM can have calling tools before writing a text response).
    /// If the maximum turn number is exceeded, it will return a [`crate::completion::request::PromptError::MaxTurnsError`].
    pub fn multi_turn(mut self, turns: usize) -> Self {
        self.max_turns = turns;
        self
    }

    /// Add chat history to the prompt request.
    ///
    /// When history is provided, the final [`FinalResponse`] will include the
    /// updated chat history (original messages + new user prompt + assistant response).
    /// ```ignore
    /// let mut stream = agent
    ///     .stream_prompt("Hello")
    ///     .with_history(vec![])
    ///     .await;
    /// // ... consume stream ...
    /// // Access updated history from FinalResponse::history()
    /// ```
    pub fn with_history<I, T>(mut self, history: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Message>,
    {
        self.chat_history = Some(history.into_iter().map(Into::into).collect());
        self
    }

    /// Attach a per-request hook for tool call events.
    /// This overrides any default hook set on the agent.
    pub fn with_hook<P2>(self, hook: P2) -> StreamingPromptRequest<M, P2>
    where
        P2: PromptHook<M>,
    {
        StreamingPromptRequest {
            prompt: self.prompt,
            chat_history: self.chat_history,
            max_turns: self.max_turns,
            model: self.model,
            agent_name: self.agent_name,
            preamble: self.preamble,
            static_context: self.static_context,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            additional_params: self.additional_params,
            tool_server_handle: self.tool_server_handle,
            dynamic_context: self.dynamic_context,
            tool_choice: self.tool_choice,
            output_schema: self.output_schema,
            hook: Some(hook),
        }
    }

    async fn send(self) -> StreamingResult<M::StreamingResponse> {
        let agent_span = if tracing::Span::current().is_disabled() {
            info_span!(
                "invoke_agent",
                gen_ai.operation.name = "invoke_agent",
                gen_ai.agent.name = self.agent_name(),
                gen_ai.system_instructions = self.preamble,
                gen_ai.prompt = tracing::field::Empty,
                gen_ai.completion = tracing::field::Empty,
                gen_ai.usage.input_tokens = tracing::field::Empty,
                gen_ai.usage.output_tokens = tracing::field::Empty,
                gen_ai.usage.cache_read.input_tokens = tracing::field::Empty,
                gen_ai.usage.cache_creation.input_tokens = tracing::field::Empty,
            )
        } else {
            tracing::Span::current()
        };

        let prompt = self.prompt;
        if let Some(text) = prompt.rag_text() {
            agent_span.record("gen_ai.prompt", text);
        }

        // Clone fields needed inside the stream
        let model = self.model.clone();
        let preamble = self.preamble.clone();
        let static_context = self.static_context.clone();
        let temperature = self.temperature;
        let max_tokens = self.max_tokens;
        let additional_params = self.additional_params.clone();
        let tool_server_handle = self.tool_server_handle.clone();
        let dynamic_context = self.dynamic_context.clone();
        let tool_choice = self.tool_choice.clone();
        let agent_name = self.agent_name.clone();
        let has_history = self.chat_history.is_some();
        let chat_history = self.chat_history;
        let mut new_messages: Vec<Message> = vec![prompt.clone()];

        let mut current_max_turns = 0;
        let mut last_prompt_error = String::new();

        let mut last_text_response = String::new();
        let mut is_text_response = false;
        let mut max_turns_reached = false;
        let output_schema = self.output_schema;

        let mut aggregated_usage = crate::completion::Usage::new();

        // NOTE: We use .instrument(agent_span) instead of span.enter() to avoid
        // span context leaking to other concurrent tasks. Using span.enter() inside
        // async_stream::stream! holds the guard across yield points, which causes
        // thread-local span context to leak when other tasks run on the same thread.
        // See: https://docs.rs/tracing/latest/tracing/span/struct.Span.html#in-asynchronous-code
        // See also: https://github.com/rust-lang/rust-clippy/issues/8722
        let stream = async_stream::stream! {
            let mut current_prompt = prompt.clone();

            'outer: loop {
                if current_max_turns > self.max_turns + 1 {
                    last_prompt_error = current_prompt.rag_text().unwrap_or_default();
                    max_turns_reached = true;
                    break;
                }

                current_max_turns += 1;

                if self.max_turns > 1 {
                    tracing::info!(
                        "Current conversation Turns: {}/{}",
                        current_max_turns,
                        self.max_turns
                    );
                }

                if let Some(ref hook) = self.hook {
                    let history_snapshot: Vec<Message> = build_history_for_request(chat_history.as_deref(), &new_messages[..new_messages.len().saturating_sub(1)]);
                    if let HookAction::Terminate { reason } = hook.on_completion_call(&current_prompt, &history_snapshot)
                        .await {
                        yield Err(cancelled_prompt_error(chat_history.as_deref(), new_messages.clone(), reason).await);
                        break 'outer;
                    }
                }

                let chat_stream_span = info_span!(
                    target: "rig::agent_chat",
                    parent: tracing::Span::current(),
                    "chat_streaming",
                    gen_ai.operation.name = "chat",
                    gen_ai.agent.name = agent_name.as_deref().unwrap_or(UNKNOWN_AGENT_NAME),
                    gen_ai.system_instructions = preamble,
                    gen_ai.provider.name = tracing::field::Empty,
                    gen_ai.request.model = tracing::field::Empty,
                    gen_ai.response.id = tracing::field::Empty,
                    gen_ai.response.model = tracing::field::Empty,
                    gen_ai.usage.output_tokens = tracing::field::Empty,
                    gen_ai.usage.input_tokens = tracing::field::Empty,
                    gen_ai.usage.cache_read.input_tokens = tracing::field::Empty,
                    gen_ai.usage.cache_creation.input_tokens = tracing::field::Empty,
                    gen_ai.input.messages = tracing::field::Empty,
                    gen_ai.output.messages = tracing::field::Empty,
                );

                let history_snapshot: Vec<Message> = build_history_for_request(chat_history.as_deref(), &new_messages[..new_messages.len().saturating_sub(1)]);
                let mut stream = tracing::Instrument::instrument(
                    build_completion_request(
                        &model,
                        current_prompt.clone(),
                        &history_snapshot,
                        preamble.as_deref(),
                        &static_context,
                        temperature,
                        max_tokens,
                        additional_params.as_ref(),
                        tool_choice.as_ref(),
                        &tool_server_handle,
                        &dynamic_context,
                        output_schema.as_ref(),
                    )
                    .await?
                    .stream(), chat_stream_span
                )

                .await?;

                new_messages.push(current_prompt.clone());

                let mut tool_calls = vec![];
                let mut tool_results = vec![];
                let mut accumulated_reasoning: Vec<crate::message::Reasoning> = vec![];
                // Kept separate from accumulated_reasoning so providers requiring
                // signatures (e.g. Anthropic) never see unsigned blocks.
                let mut pending_reasoning_delta_text = String::new();
                let mut pending_reasoning_delta_id: Option<String> = None;
                let mut saw_tool_call_this_turn = false;

                while let Some(content) = stream.next().await {
                    match content {
                        Ok(StreamedAssistantContent::Text(text)) => {
                            if !is_text_response {
                                last_text_response = String::new();
                                is_text_response = true;
                            }
                            last_text_response.push_str(&text.text);
                            if let Some(ref hook) = self.hook &&
                                let HookAction::Terminate { reason } = hook.on_text_delta(&text.text, &last_text_response).await {
                                    yield Err(cancelled_prompt_error(chat_history.as_deref(), new_messages.clone(), reason).await);
                                    break 'outer;
                            }

                            yield Ok(MultiTurnStreamItem::stream_item(StreamedAssistantContent::Text(text)));
                        },
                        Ok(StreamedAssistantContent::ToolCall { tool_call, internal_call_id }) => {
                            let tool_span = info_span!(
                                parent: tracing::Span::current(),
                                "execute_tool",
                                gen_ai.operation.name = "execute_tool",
                                gen_ai.tool.type = "function",
                                gen_ai.tool.name = tracing::field::Empty,
                                gen_ai.tool.call.id = tracing::field::Empty,
                                gen_ai.tool.call.arguments = tracing::field::Empty,
                                gen_ai.tool.call.result = tracing::field::Empty
                            );

                            yield Ok(MultiTurnStreamItem::stream_item(StreamedAssistantContent::ToolCall { tool_call: tool_call.clone(), internal_call_id: internal_call_id.clone() }));

                            let tc_result = async {
                                let tool_span = tracing::Span::current();
                                let tool_args = json_utils::value_to_json_string(&tool_call.function.arguments);
                                if let Some(ref hook) = self.hook {
                                    let action = hook
                                        .on_tool_call(&tool_call.function.name, tool_call.call_id.clone(), &internal_call_id, &tool_args)
                                        .await;

                                    if let ToolCallHookAction::Terminate { reason } = action {
                                        return Err(cancelled_prompt_error(chat_history.as_deref(), new_messages.clone(), reason).await);
                                    }

                                    if let ToolCallHookAction::Skip { reason } = action {
                                        // Tool execution rejected, return rejection message as tool result
                                        tracing::info!(
                                            tool_name = tool_call.function.name.as_str(),
                                            reason = reason,
                                            "Tool call rejected"
                                        );
                                        let tool_call_msg = AssistantContent::ToolCall(tool_call.clone());
                                        tool_calls.push(tool_call_msg);
                                        tool_results.push((tool_call.id.clone(), tool_call.call_id.clone(), reason.clone()));
                                        saw_tool_call_this_turn = true;
                                        return Ok(reason);
                                    }
                                }

                                tool_span.record("gen_ai.tool.name", &tool_call.function.name);
                                tool_span.record("gen_ai.tool.call.arguments", &tool_args);

                                let tool_result = match
                                tool_server_handle.call_tool(&tool_call.function.name, &tool_args).await {
                                    Ok(thing) => thing,
                                    Err(e) => {
                                        tracing::warn!("Error while calling tool: {e}");
                                        e.to_string()
                                    }
                                };

                                tool_span.record("gen_ai.tool.call.result", &tool_result);

                                if let Some(ref hook) = self.hook &&
                                    let HookAction::Terminate { reason } =
                                    hook.on_tool_result(
                                        &tool_call.function.name,
                                        tool_call.call_id.clone(),
                                        &internal_call_id,
                                        &tool_args,
                                        &tool_result.to_string()
                                    )
                                    .await {
                                        return Err(cancelled_prompt_error(chat_history.as_deref(), new_messages.clone(), reason).await);
                                    }

                                let tool_call_msg = AssistantContent::ToolCall(tool_call.clone());

                                tool_calls.push(tool_call_msg);
                                tool_results.push((tool_call.id.clone(), tool_call.call_id.clone(), tool_result.clone()));

                                saw_tool_call_this_turn = true;
                                Ok(tool_result)
                            }.instrument(tool_span).await;

                            match tc_result {
                                Ok(text) => {
                                    let tr = ToolResult { id: tool_call.id, call_id: tool_call.call_id, content: ToolResultContent::from_tool_output(text) };
                                    yield Ok(MultiTurnStreamItem::StreamUserItem(StreamedUserContent::ToolResult{ tool_result: tr, internal_call_id }));
                                }
                                Err(e) => {
                                    yield Err(e);
                                    break 'outer;
                                }
                            }
                        },
                        Ok(StreamedAssistantContent::ToolCallDelta { id, internal_call_id, content }) => {
                            if let Some(ref hook) = self.hook {
                                let (name, delta) = match &content {
                                    crate::streaming::ToolCallDeltaContent::Name(n) => (Some(n.as_str()), ""),
                                    crate::streaming::ToolCallDeltaContent::Delta(d) => (None, d.as_str()),
                                };

                                if let HookAction::Terminate { reason } = hook.on_tool_call_delta(&id, &internal_call_id, name, delta)
                                .await {
                                    yield Err(cancelled_prompt_error(chat_history.as_deref(), new_messages.clone(), reason).await);
                                    break 'outer;
                                }
                            }
                        }
                        Ok(StreamedAssistantContent::Reasoning(reasoning)) => {
                            // Accumulate reasoning for inclusion in chat history with tool calls.
                            // OpenAI Responses API requires reasoning items to be sent back
                            // alongside function_call items in multi-turn conversations.
                            merge_reasoning_blocks(&mut accumulated_reasoning, &reasoning);
                            yield Ok(MultiTurnStreamItem::stream_item(StreamedAssistantContent::Reasoning(reasoning)));
                        },
                        Ok(StreamedAssistantContent::ReasoningDelta { reasoning, id }) => {
                            // Deltas lack signatures/encrypted content that full
                            // blocks carry; mixing them into accumulated_reasoning
                            // causes Anthropic to reject with "signature required".
                            pending_reasoning_delta_text.push_str(&reasoning);
                            if pending_reasoning_delta_id.is_none() {
                                pending_reasoning_delta_id = id.clone();
                            }
                            yield Ok(MultiTurnStreamItem::stream_item(StreamedAssistantContent::ReasoningDelta { reasoning, id }));
                        },
                        Ok(StreamedAssistantContent::Final(final_resp)) => {
                            if let Some(usage) = final_resp.token_usage() { aggregated_usage += usage; };
                            if is_text_response {
                                if let Some(ref hook) = self.hook &&
                                     let HookAction::Terminate { reason } = hook.on_stream_completion_response_finish(&prompt, &final_resp).await {
                                        yield Err(cancelled_prompt_error(chat_history.as_deref(), new_messages.clone(), reason).await);
                                        break 'outer;
                                    }

                                tracing::Span::current().record("gen_ai.completion", &last_text_response);
                                yield Ok(MultiTurnStreamItem::stream_item(StreamedAssistantContent::Final(final_resp)));
                                is_text_response = false;
                            }
                        }
                        Err(e) => {
                            yield Err(e.into());
                            break 'outer;
                        }
                    }
                }

                // Providers like Gemini emit thinking as incremental deltas
                // without signatures; assemble into a single block so
                // reasoning survives into the next turn's chat history.
                if accumulated_reasoning.is_empty() && !pending_reasoning_delta_text.is_empty() {
                    let mut assembled = crate::message::Reasoning::new(&pending_reasoning_delta_text);
                    if let Some(id) = pending_reasoning_delta_id.take() {
                        assembled = assembled.with_id(id);
                    }
                    accumulated_reasoning.push(assembled);
                }

                // Add text, reasoning, and tool calls to chat history.
                // OpenAI Responses API requires reasoning items to precede function_call items.
                if !tool_calls.is_empty() || !accumulated_reasoning.is_empty() {
                    let mut content_items: Vec<crate::message::AssistantContent> = vec![];

                    // Text before tool calls so the model sees its own prior output
                    if !last_text_response.is_empty() {
                        content_items.push(crate::message::AssistantContent::text(&last_text_response));
                        last_text_response.clear();
                    }

                    // Reasoning must come before tool calls (OpenAI requirement)
                    for reasoning in accumulated_reasoning.drain(..) {
                        content_items.push(crate::message::AssistantContent::Reasoning(reasoning));
                    }

                    content_items.extend(tool_calls.clone());

                    if !content_items.is_empty() {
                        new_messages.push(Message::Assistant {
                            id: stream.message_id.clone(),
                            content: OneOrMany::many(content_items).expect("Should have at least one item"),
                        });
                    }
                }

                for (id, call_id, tool_result) in tool_results {
                    new_messages.push(tool_result_to_user_message(id, call_id, tool_result));
                }

                // Set the current prompt to the last message in new_messages
                current_prompt = match new_messages.pop() {
                    Some(prompt) => prompt,
                    None => unreachable!("New messages should never be empty at this point"),
                };

                if !saw_tool_call_this_turn {
                    // Add user message and assistant response to history before finishing
                    new_messages.push(current_prompt.clone());
                    if !last_text_response.is_empty() {
                        new_messages.push(Message::assistant(&last_text_response));
                    }

                    let current_span = tracing::Span::current();
                    current_span.record("gen_ai.usage.input_tokens", aggregated_usage.input_tokens);
                    current_span.record("gen_ai.usage.output_tokens", aggregated_usage.output_tokens);
                    current_span.record("gen_ai.usage.cache_read.input_tokens", aggregated_usage.cached_input_tokens);
                    current_span.record("gen_ai.usage.cache_creation.input_tokens", aggregated_usage.cache_creation_input_tokens);
                    tracing::info!("Agent multi-turn stream finished");
                    let final_messages: Option<Vec<Message>> = if has_history {
                        Some(new_messages.clone())
                    } else {
                        None
                    };
                    yield Ok(MultiTurnStreamItem::final_response_with_history(
                        &last_text_response,
                        aggregated_usage,
                        final_messages,
                    ));
                    break;
                }
            }

            if max_turns_reached {
                yield Err(Box::new(PromptError::MaxTurnsError {
                    max_turns: self.max_turns,
                    chat_history: build_full_history(chat_history.as_deref(), new_messages.clone()).into(),
                    prompt: Box::new(last_prompt_error.clone().into()),
                }).into());
            }
        };

        Box::pin(stream)
    }
}

impl<M, P> IntoFuture for StreamingPromptRequest<M, P>
where
    M: CompletionModel + 'static,
    <M as CompletionModel>::StreamingResponse: WasmCompatSend,
    P: PromptHook<M> + 'static,
{
    type Output = StreamingResult<M::StreamingResponse>; // what `.await` returns
    type IntoFuture = WasmBoxedFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        // Wrap send() in a future, because send() returns a stream immediately
        Box::pin(async move { self.send().await })
    }
}

/// Helper function to stream a completion request to stdout.
pub async fn stream_to_stdout<R>(
    stream: &mut StreamingResult<R>,
) -> Result<FinalResponse, std::io::Error> {
    let mut final_res = FinalResponse::empty();
    print!("Response: ");
    while let Some(content) = stream.next().await {
        match content {
            Ok(MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::Text(
                Text { text },
            ))) => {
                print!("{text}");
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
            Ok(MultiTurnStreamItem::StreamAssistantItem(StreamedAssistantContent::Reasoning(
                reasoning,
            ))) => {
                let reasoning = reasoning.display_text();
                print!("{reasoning}");
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
            Ok(MultiTurnStreamItem::FinalResponse(res)) => {
                final_res = res;
            }
            Err(err) => {
                eprintln!("Error: {err}");
            }
            _ => {}
        }
    }

    Ok(final_res)
}
