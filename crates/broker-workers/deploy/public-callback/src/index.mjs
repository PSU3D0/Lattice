const ALLOWED_EXACT = new Map([
  ["GET /health", true],
  ["GET /ready", true],
  ["POST /v0.2/sessions", true],
  ["GET /v0.2/trust/receipts", true],
  ["POST /v0.2/connection-intents", true],
  ["GET /v0.2/credential-callback", true],
  ["POST /v0.2/bindings", true],
]);

function allowed(request) {
  const path = new URL(request.url).pathname;
  if (ALLOWED_EXACT.has(`${request.method} ${path}`)) return true;
  if (["GET", "DELETE"].includes(request.method) && path.startsWith("/v0.2/connections/")) {
    return true;
  }
  return request.method === "GET" && path.startsWith("/v0.2/receipts/");
}

export default {
  async fetch(request, env) {
    if (!allowed(request) || env.BROKER_PRIVATE === undefined) {
      return Response.json(
        { error: { code: "BRK001", message: "broker request rejected" } },
        { status: 404 },
      );
    }
    return env.BROKER_PRIVATE.fetch(request);
  },
};
