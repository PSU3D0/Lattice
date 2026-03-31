use futures::{stream, StreamExt};
use llm_provider_anthropic::completion as anthropic;
use llm_provider_anthropic::streaming::{ContentDelta, StreamingEvent};
use llm_types::completion as core;
use llm_types::message::ToolChoice as CoreToolChoice;
use llm_types::OneOrMany;
use serde_json::json;

#[test]
fn request_serialization_matches_anthropic_messages_api() {
    let request = core::CompletionRequest {
        model: Some("claude-3-5-sonnet-latest".to_string()),
        preamble: Some("You are a helpful assistant.".to_string()),
        chat_history: OneOrMany::one(core::Message::user("Hello")),
        documents: vec![],
        tools: vec![core::ToolDefinition {
            name: "lookup_weather".to_string(),
            description: "Look up weather by location".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "location": { "type": "string" }
                },
                "required": ["location"]
            }),
        }],
        temperature: Some(0.2),
        max_tokens: Some(256),
        tool_choice: Some(CoreToolChoice::Required),
        additional_params: Some(json!({"top_p": 0.9})),
        output_schema: None,
    };

    let anthropic_request = anthropic::AnthropicCompletionRequest::try_from(
        anthropic::AnthropicRequestParams {
            model: anthropic::CLAUDE_3_5_SONNET,
            request,
            prompt_caching: false,
            automatic_caching: false,
            automatic_caching_ttl: None,
        },
    )
    .expect("request conversion should succeed");

    let value = serde_json::to_value(anthropic_request).expect("serialization should succeed");

    assert_eq!(value["model"], "claude-3-5-sonnet-latest");
    assert_eq!(value["max_tokens"], 256);
    assert_eq!(value["temperature"], 0.2);
    assert_eq!(value["system"][0]["type"], "text");
    assert_eq!(value["system"][0]["text"], "You are a helpful assistant.");
    assert_eq!(value["messages"][0]["role"], "user");
    assert_eq!(value["messages"][0]["content"][0]["type"], "text");
    assert_eq!(value["messages"][0]["content"][0]["text"], "Hello");
    assert_eq!(value["tools"][0]["name"], "lookup_weather");
    assert_eq!(value["tools"][0]["description"], "Look up weather by location");
    assert_eq!(value["tools"][0]["input_schema"]["properties"]["location"]["type"], "string");
    assert_eq!(value["tool_choice"]["type"], "any");
    assert_eq!(value["top_p"], 0.9);
}

#[test]
fn response_deserializes_messages_api_payload() {
    let json = r#"{
        "id": "msg_123",
        "type": "message",
        "role": "assistant",
        "model": "claude-3-5-sonnet-latest",
        "content": [
            {"type": "text", "text": "Hello there", "cache_control": null}
        ],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 12,
            "cache_read_input_tokens": 4,
            "cache_creation_input_tokens": 2,
            "output_tokens": 15
        }
    }"#;

    let response: anthropic::CompletionResponse = serde_json::from_str(json).unwrap();

    assert_eq!(response.id, "msg_123");
    assert_eq!(response.model, "claude-3-5-sonnet-latest");
    assert_eq!(response.role, "assistant");
    assert_eq!(response.content.len(), 1);
    match &response.content[0] {
        anthropic::Content::Text { text, .. } => assert_eq!(text, "Hello there"),
        other => panic!("expected text content, got {other:?}"),
    }
    assert_eq!(response.usage.input_tokens, 12);
    assert_eq!(response.usage.cache_read_input_tokens, Some(4));
    assert_eq!(response.usage.cache_creation_input_tokens, Some(2));
    assert_eq!(response.usage.output_tokens, 15);
}

#[test]
fn streaming_event_deserializes_through_sse_decoder() {
    let payload = concat!(
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"First, I need to think.\"}}\n",
        "\n"
    );

    let events = llm_provider_anthropic::decoders::sse::iter_sse_messages(stream::iter(vec![
        Ok::<Vec<u8>, std::io::Error>(payload.as_bytes().to_vec()),
    ]));
    futures::pin_mut!(events);

    let event = futures::executor::block_on(async { events.next().await.unwrap().unwrap() });
    assert_eq!(event.event.as_deref(), Some("content_block_delta"));

    let parsed: StreamingEvent = serde_json::from_str(&event.data).unwrap();
    match parsed {
        StreamingEvent::ContentBlockDelta { index, delta } => {
            assert_eq!(index, 0);
            match delta {
                ContentDelta::ThinkingDelta { thinking } => {
                    assert_eq!(thinking, "First, I need to think.");
                }
                other => panic!("expected thinking delta, got {other:?}"),
            }
        }
        other => panic!("expected content_block_delta event, got {other:?}"),
    }
}

#[test]
fn tool_definition_serializes_correctly() {
    let tool = anthropic::ToolDefinition {
        name: "lookup_weather".to_string(),
        description: Some("Look up weather by location".to_string()),
        input_schema: json!({
            "type": "object",
            "properties": {
                "location": { "type": "string" }
            },
            "required": ["location"]
        }),
    };

    let value = serde_json::to_value(tool).unwrap();
    assert_eq!(value["name"], "lookup_weather");
    assert_eq!(value["description"], "Look up weather by location");
    assert_eq!(value["input_schema"]["type"], "object");
    assert_eq!(value["input_schema"]["properties"]["location"]["type"], "string");
}

#[test]
fn model_constants_exist() {
    assert_eq!(anthropic::CLAUDE_4_OPUS, "claude-opus-4-0");
    assert_eq!(anthropic::CLAUDE_4_SONNET, "claude-sonnet-4-0");
    assert_eq!(anthropic::CLAUDE_3_7_SONNET, "claude-3-7-sonnet-latest");
    assert_eq!(anthropic::CLAUDE_3_5_SONNET, "claude-3-5-sonnet-latest");
    assert_eq!(anthropic::CLAUDE_3_5_HAIKU, "claude-3-5-haiku-latest");
}
