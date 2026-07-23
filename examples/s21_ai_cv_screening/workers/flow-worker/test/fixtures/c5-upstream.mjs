const state = { llm: 0, token: 0, userinfo: 0, sheets: 0, gmail: 0, requests: [] };

function record(request, body) {
  state.requests.push({ method: request.method, url: request.url, authorization: request.headers.get("authorization"), body });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__state") return Response.json(state);
    const body = request.method === "POST" ? await request.clone().text() : "";
    record(request, body);
    if (url.pathname.includes("/chat/completions")) {
      state.llm += 1;
      if (request.headers.get("authorization") !== "Bearer c5-llm-key") return Response.json({ error: "unauthorized" }, { status: 401 });
      return Response.json({ id: "chatcmpl-c5", object: "chat.completion", created: 1, model: "gpt-5.4-mini", system_fingerprint: null,
        choices: [{ index: 0, message: { role: "assistant", content: "Rating: 9/10. Strong match.", tool_calls: [] }, logprobs: null, finish_reason: "stop" }],
        usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } });
    }
    if (request.url === "https://oauth2.googleapis.com/token") {
      state.token += 1;
      return Response.json({ access_token: "owned-access-token-private", refresh_token: "owned-refresh-token-private", expires_in: 3600,
        scope: "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/gmail.send", token_type: "Bearer" });
    }
    if (url.origin === "https://oauth2.googleapis.com" && url.pathname === "/tokeninfo") {
      state.userinfo += 1;
      return Response.json({ user_id: "owned-google-subject", scope: "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/gmail.send" });
    }
    if (url.origin === "https://sheets.googleapis.com") {
      state.sheets += 1;
      return Response.json({ updates: { updatedCells: 6, updatedColumns: 6, updatedRange: "Candidates!A2:F2", updatedRows: 1 } }, { headers: { "x-request-id": "sheets-c5" } });
    }
    if (url.origin === "https://gmail.googleapis.com") {
      state.gmail += 1;
      return Response.json({ id: `message-c5-${state.gmail}`, threadId: `thread-c5-${state.gmail}` }, { headers: { "x-request-id": `gmail-c5-${state.gmail}` } });
    }
    return Response.json({ error: "unexpected_target" }, { status: 599 });
  },
};
