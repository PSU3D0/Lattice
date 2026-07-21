const counts = { token: 0, provider: 0 };
const exchangeCache = new Map();
let oversizeNext = false;

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__counts") return Response.json(counts);
    if (url.pathname === "/__oversize-next") {
      oversizeNext = true;
      return Response.json({ armed: true });
    }
    if (["/exchange", "/refresh"].includes(url.pathname) && request.method === "POST") {
      counts.token += 1;
      const input = await request.json();
      const common = {
        access_token: "mock-access-never-log",
        expires_in: 3600,
        scopes: [
          "https://www.googleapis.com/auth/gmail.send",
          "https://www.googleapis.com/auth/spreadsheets",
        ],
      };
      if (url.pathname === "/exchange") {
        if (input.code === "exchange-ambiguous") {
          return Response.json({ error: "ambiguous" }, { status: 503 });
        }
        if (exchangeCache.has(input.intent_ref)) {
          return Response.json(exchangeCache.get(input.intent_ref));
        }
        const exchange = {
          ...common,
          refresh_token: "mock-refresh-never-log",
          account_id: input.code === "missing-account"
            ? ""
            : input.code === "wrong-account"
              ? "not-a-normalized-provider-account"
              : "workspace-user@example.com",
        };
        if (input.code === "missing-scopes") exchange.scopes = [common.scopes[0]];
        if (input.code === "added-scope") exchange.scopes = [...common.scopes, "openid"];
        if (input.code === "precomputed-commitment") {
          exchange.account_commitment = "hmac-sha256:caller-controlled";
        }
        exchangeCache.set(input.intent_ref, exchange);
        return Response.json(exchange);
      }
      return Response.json(common);
    }
    counts.provider += 1;
    if (
      request.method !== "POST" ||
      request.headers.get("accept") !== "application/json" ||
      request.headers.get("content-type") !== "application/json"
    ) {
      return Response.json({ error: "descriptor_plan_mismatch" }, { status: 422 });
    }
    if (oversizeNext) {
      oversizeNext = false;
      return new Response("x".repeat(70 * 1024), {
        headers: { "content-type": "application/json" },
      });
    }
    if (url.pathname.includes("/spreadsheets/")) {
      return Response.json({
        updates: {
          updatedRange: "Sheet1!A2",
          updatedRows: 1,
          updatedColumns: 1,
          updatedCells: 1,
        },
      }, { headers: { "x-request-id": "mock-sheets-1" } });
    }
    if (url.pathname === "/gmail/v1/users/me/messages/send") {
      return Response.json(
        { id: "msg-1", threadId: "thread-1" },
        { headers: { "x-request-id": "mock-gmail-1" } },
      );
    }
    return Response.json({ error: "not_found" }, { status: 404 });
  },
};
