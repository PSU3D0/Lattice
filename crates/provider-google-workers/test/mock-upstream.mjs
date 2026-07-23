const calls = [];

function tokenResponse(form) {
  const common = {
    access_token: "access-token-private-value",
    expires_in: 3600,
    scope: "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/gmail.send",
    token_type: "Bearer",
  };
  if (form.get("grant_type") === "authorization_code") return { ...common, refresh_token: "refresh-token-private-value" };
  return common;
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__calls") return Response.json(calls);
    const body = request.method === "POST" ? await request.clone().text() : "";
    calls.push({ url: request.url, method: request.method, headers: Object.fromEntries(request.headers), body });
    if (request.url === "https://oauth2.googleapis.com/token") {
      return Response.json(tokenResponse(new URLSearchParams(body)));
    }
    if (new URL(request.url).pathname === "/tokeninfo") {
      return Response.json({ user_id: "google-subject-123456", scope: "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/gmail.send" });
    }
    if (request.url === "https://oauth2.googleapis.com/revoke") {
      return new Response(null, { status: 200 });
    }
    if (url.origin === "https://gmail.googleapis.com") {
      return Response.json({ id: "message-1", threadId: "thread-1" }, { headers: { "x-request-id": "gmail-request-1" } });
    }
    if (url.origin === "https://sheets.googleapis.com") {
      return Response.json({ updates: { updatedCells: 1, updatedColumns: 1, updatedRange: "Sheet1!A2", updatedRows: 1 } }, { headers: { "x-request-id": "sheets-request-1" } });
    }
    return Response.json({ error: "unexpected_mock_target" }, { status: 599 });
  },
};
