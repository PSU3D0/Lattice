use llm_provider_openai::completion as openai;
use llm_provider_openai::completion::{FunctionDefinition, ToolChoice, ToolDefinition};
use llm_provider_openai::{
    DALL_E_3, GPT_4O, GPT_4O_MINI, GPT_5_4, GPT_5_4_MINI, GPT_IMAGE_1_5, TEXT_EMBEDDING_3_SMALL,
    TTS_1, WHISPER_1,
};
use llm_types::OneOrMany;
use llm_types::completion as core;
use llm_types::message::ToolChoice as CoreToolChoice;
use schemars::{JsonSchema, schema_for};
use serde_json::json;

#[test]
fn request_serialization_matches_openai_format() {
    let request = core::CompletionRequest {
        model: Some("gpt-4o-mini".to_string()),
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

    let openai_request = openai::CompletionRequest::try_from(openai::OpenAIRequestParams {
        model: "gpt-4o-mini".to_string(),
        request,
        strict_tools: true,
        tool_result_array_content: false,
    })
    .expect("request conversion should succeed");

    let value = serde_json::to_value(openai_request).expect("serialization should succeed");

    assert_eq!(value["model"], "gpt-4o-mini");
    assert_eq!(value["messages"][0]["role"], "system");
    assert_eq!(value["messages"][1]["role"], "user");
    assert_eq!(value["messages"][1]["content"], "Hello");
    assert_eq!(value["tools"][0]["type"], "function");
    assert_eq!(value["tools"][0]["function"]["name"], "lookup_weather");
    assert_eq!(value["tools"][0]["function"]["strict"], true);
    assert_eq!(value["tool_choice"], "required");
    assert_eq!(value["max_tokens"], 256);
    assert_eq!(value["temperature"], 0.2);
    assert_eq!(value["top_p"], 0.9);
}

#[test]
fn response_deserializes_chat_completion() {
    let json = r#"{
        "id": "chatcmpl-123",
        "object": "chat.completion",
        "created": 1234567890,
        "model": "gpt-4o-mini",
        "system_fingerprint": null,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Hello there"
            },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 12,
            "total_tokens": 15,
            "prompt_tokens_details": { "cached_tokens": 4 }
        }
    }"#;

    let response: openai::CompletionResponse = serde_json::from_str(json).unwrap();

    assert_eq!(response.id, "chatcmpl-123");
    assert_eq!(response.model, "gpt-4o-mini");
    assert_eq!(response.choices.len(), 1);
    assert_eq!(response.choices[0].finish_reason, "stop");
    match &response.choices[0].message {
        openai::Message::Assistant { content, .. } => {
            assert_eq!(
                content[0],
                openai::AssistantContent::Text {
                    text: "Hello there".to_string()
                }
            );
        }
        other => panic!("expected assistant message, got {other:?}"),
    }
    assert_eq!(response.usage.unwrap().prompt_tokens, 12);
}

#[test]
fn tool_choice_serializes_correctly() {
    assert_eq!(
        serde_json::to_value(ToolChoice::Auto).unwrap(),
        json!("auto")
    );
    assert_eq!(
        serde_json::to_value(ToolChoice::Required).unwrap(),
        json!("required")
    );
}

#[test]
fn tool_definition_serializes_with_function_wrapper() {
    let tool = ToolDefinition {
        r#type: "function".to_string(),
        function: FunctionDefinition {
            name: "lookup_weather".to_string(),
            description: "Look up weather by location".to_string(),
            parameters: json!({"type": "object", "properties": {"location": {"type": "string"}}}),
            strict: Some(true),
        },
    };

    let value = serde_json::to_value(tool).unwrap();
    assert_eq!(value["type"], "function");
    assert_eq!(value["function"]["name"], "lookup_weather");
    assert_eq!(value["function"]["strict"], true);
}

#[allow(dead_code)]
#[derive(JsonSchema)]
struct StructuredOutput {
    answer: String,
    confidence: f32,
}

#[test]
fn model_constants_exist() {
    assert_eq!(GPT_4O, "gpt-4o");
    assert_eq!(GPT_4O_MINI, "gpt-4o-mini");
    assert_eq!(GPT_5_4, "gpt-5.4");
    assert_eq!(GPT_5_4_MINI, "gpt-5.4-mini");
    assert_eq!(TEXT_EMBEDDING_3_SMALL, "text-embedding-3-small");
    assert_eq!(WHISPER_1, "whisper-1");
    assert_eq!(TTS_1, "tts-1");
    assert_eq!(DALL_E_3, "dall-e-3");
    assert_eq!(GPT_IMAGE_1_5, "gpt-image-1.5");
}

#[test]
fn output_schema_maps_to_strict_json_schema_response_format() {
    let request = core::CompletionRequest {
        model: Some("gpt-4o-mini".to_string()),
        preamble: None,
        chat_history: OneOrMany::one(core::Message::user("Hello")),
        documents: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        tool_choice: None,
        additional_params: None,
        output_schema: Some(schema_for!(StructuredOutput)),
    };

    let openai_request = openai::CompletionRequest::try_from(openai::OpenAIRequestParams {
        model: "gpt-4o-mini".to_string(),
        request,
        strict_tools: false,
        tool_result_array_content: false,
    })
    .expect("request conversion should succeed");

    let value = serde_json::to_value(openai_request).expect("serialization should succeed");
    let response_format = &value["response_format"];

    assert_eq!(response_format["type"], "json_schema");
    assert_eq!(response_format["json_schema"]["name"], "StructuredOutput");
    assert_eq!(response_format["json_schema"]["strict"], true);

    let schema = &response_format["json_schema"]["schema"];
    assert_eq!(schema["additionalProperties"], json!(false));
    assert!(
        schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("answer"))
    );
    assert!(
        schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("confidence"))
    );
}
