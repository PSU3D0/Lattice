const fallbackCounts = { llm: 0, sheetsRead: 0, sheetsAppend: 0, gmail: 0 };

function json(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function handle(request, env = {}, storage) {
  const url = new URL(request.url);
  if (
    typeof env.MOCK_ADMIN_BEARER !== "string"
    || typeof env.MOCK_LLM_BEARER !== "string"
    || typeof env.MOCK_GOOGLE_BEARER !== "string"
  ) {
    return json({ error: "unconfigured" }, 503);
  }
  let counts = storage === undefined
    ? fallbackCounts
    : (await storage.get("counts")) ?? { llm: 0, sheetsRead: 0, sheetsAppend: 0, gmail: 0 };
  const adminBearer = env.MOCK_ADMIN_BEARER;
  const adminAuthorized = request.headers.get("authorization") === `Bearer ${adminBearer}`;
  if (url.pathname === "/__counts") {
    return adminAuthorized ? json(counts) : json({ error: "unauthorized" }, 401);
  }
  if (url.pathname === "/__reset" && request.method === "POST") {
    if (!adminAuthorized) return json({ error: "unauthorized" }, 401);
    counts = { llm: 0, sheetsRead: 0, sheetsAppend: 0, gmail: 0 };
    if (storage === undefined) Object.assign(fallbackCounts, counts);
    else await storage.put("counts", counts);
    return json({ reset: true });
  }

  const authorization = request.headers.get("authorization");
  const llmBearer = env.MOCK_LLM_BEARER;
  const googleBearer = env.MOCK_GOOGLE_BEARER;
  if (url.pathname.includes("/chat/completions") && request.method === "POST") {
    if (authorization !== `Bearer ${llmBearer}`) return json({ error: "unauthorized" }, 401);
    counts.llm += 1;
    await storage?.put("counts", counts);
    const requestBody = await request.text();
    if (requestBody.includes("reflect-provider")) {
      return json({ error: `reflected provider body: ${requestBody}` }, 500);
    }
    return json({
      id: "chatcmpl-w4",
      object: "chat.completion",
      created: 1,
      model: "gpt-5.4-mini",
      system_fingerprint: null,
      choices: [{
        index: 0,
        message: {
          role: "assistant",
          content: "Rating: 9/10. Strong match. Recommend an interview.",
          tool_calls: [],
        },
        logprobs: null,
        finish_reason: "stop",
      }],
      usage: { prompt_tokens: 10, completion_tokens: 10, total_tokens: 20 },
    });
  }
  if (url.pathname.includes("/values/") && request.method === "GET") {
    if (authorization !== `Bearer ${googleBearer}`) return json({ error: "unauthorized" }, 401);
    counts.sheetsRead += 1;
    await storage?.put("counts", counts);
    return json({
      values: [["full_name", "email", "expectation", "linkedin", "cv_filename", "ai_rating"]],
    });
  }
  if (url.pathname.includes("append") && request.method === "POST") {
    if (authorization !== `Bearer ${googleBearer}`) return json({ error: "unauthorized" }, 401);
    counts.sheetsAppend += 1;
    await storage?.put("counts", counts);
    return json({ updates: { updatedRange: "'Candidates'!A2:F2" } });
  }
  if (url.pathname === "/gmail/v1/users/me/messages/send" && request.method === "POST") {
    if (authorization !== `Bearer ${googleBearer}`) return json({ error: "unauthorized" }, 401);
    counts.gmail += 1;
    await storage?.put("counts", counts);
    return json({
      id: `msg-w4-${counts.gmail}`,
      threadId: `thread-w4-${counts.gmail}`,
      labelIds: ["SENT"],
    });
  }
  return json({ error: "not_found" }, 404);
}

export class MockProviderState {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    return handle(request, this.env, this.state.storage);
  }
}

export default {
  async fetch(request, env) {
    if (env?.MOCK_PROVIDER_STATE !== undefined) {
      const id = env.MOCK_PROVIDER_STATE.idFromName("singleton");
      return env.MOCK_PROVIDER_STATE.get(id).fetch(request);
    }
    return handle(request, env);
  },
};
