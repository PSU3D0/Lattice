const counts = { token: 0, provider: 0 };
const exchangeCache = new Map();
let oversizeNext = false;
let revokeFailOnce = false;

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.hostname === "auth-driver-upstream.test") {
      const kind = url.pathname.slice(1);
      let valid = false;
      let dispatch = request.method === "POST" && ["header", "query", "bearer", "basic"].includes(kind);
      if (kind === "header") valid = request.headers.get("x-api-key") === "secret-header";
      if (kind === "query") valid = url.searchParams.get("api_key") === "secret-query";
      if (kind === "bearer") valid = request.headers.get("authorization") === "Bearer secret-bearer";
      if (kind === "basic") valid = request.headers.get("authorization") === `Basic ${btoa("fixture-user:fixture-password")}`;
      if (kind === "workload") { const input = await request.json(); dispatch = input.assertion === undefined; valid = dispatch || (input.audience === "lattice-workload" && typeof input.assertion === "string"); }
      let destroy = false;
      if (kind === "external") { const input = await request.json(); destroy = input.action === "destroy" && typeof input.material_or_remote_proof === "string"; dispatch = input.action === "authorize_and_dispatch" && typeof input.remote_handle === "string"; valid = destroy || dispatch || (input.custodian_ref === "auth-driver.fixture" && typeof input.remote_proof === "string"); }
      if (valid && dispatch) return Response.json({ updates: { updatedRange: "Sheet1!A2", updatedRows: 1, updatedColumns: 1, updatedCells: 1 }, remote_dispatch_proof: `sha256:${"a".repeat(64)}` }, { headers: { "x-request-id": `wire-${kind}` } });
      return valid ? Response.json({ account_subject: `verified:${kind}`, remote_destruction_proof: destroy ? "signed-remote-destruction" : undefined, material_b64u: btoa(`exchanged:${kind}`).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", ""), remote_proof: kind === "external" ? btoa("remote-authorized").replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "") : null }) : Response.json({ error: "auth_driver_rejected" }, { status: 401 });
    }
    if (url.pathname === "/__counts") return Response.json(counts);
    if (url.pathname === "/__oversize-next") {
      oversizeNext = true;
      return Response.json({ armed: true });
    }
    if (url.pathname === "/__revoke-fail-once") {
      revokeFailOnce = true;
      return Response.json({ armed: true });
    }
    if (url.pathname === "/authorize" && request.method === "POST") {
      counts.token += 1;
      const input = await request.json();
      const authorization = new URL("https://accounts.google.com/o/oauth2/v2/auth");
      authorization.search = new URLSearchParams({
        client_id: "test-client-id-12345",
        redirect_uri: input.redirect_uri,
        scope: [...input.scopes].sort().join(" "),
        state: input.state,
        response_type: "code",
        code_challenge: input.code_challenge,
        code_challenge_method: "S256",
      });
      return Response.json({ authorization_url: authorization.toString(), exact_scopes: [...input.scopes].sort() });
    }
    if (url.pathname === "/revoke" && request.method === "POST") {
      counts.token += 1;
      if (revokeFailOnce) { revokeFailOnce = false; return Response.json({ error: "temporary" }, { status: 503 }); }
      const input = await request.json();
      const valid = request.headers.has("x-lattice-auth-driver-service-auth")
        ? typeof input.material_or_remote_proof === "string" && input.material_or_remote_proof.length > 0
        : typeof input.token === "string" && input.token.length > 0;
      return valid ? Response.json({ revoked: true, remote_destruction_proof: "signed" })
        : Response.json({ error: "invalid" }, { status: 400 });
    }
    if (["/validate", "/token-exchange", "/authorize-and-dispatch"].includes(url.pathname) && request.method === "POST") {
      const input = await request.json();
      const submission = input.submission;
      const opaque = submission.material_b64u ?? submission.assertion_b64u ?? submission.remote_proof;
      if (typeof opaque !== "string" || opaque.length < 4) return Response.json({ error: "invalid" }, { status: 400 });
      return Response.json({ material_b64u: url.pathname === "/authorize-and-dispatch" ? null : opaque, account_subject: `subject:${url.pathname}`, claims: input.expected_claims, remote_proof: url.pathname === "/authorize-and-dispatch" ? opaque : null });
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
          "openid",
        ],
      };
      if (url.pathname === "/exchange") {
        if (input.code === "exchange-ambiguous") {
          return Response.json({ error: "ambiguous" }, { status: 503 });
        }
        const idempotencyKey = request.headers.get("x-lattice-idempotency-key");
        if (exchangeCache.has(idempotencyKey)) {
          return Response.json(exchangeCache.get(idempotencyKey));
        }
        const exchange = {
          ...common,
          refresh_token: "mock-refresh-never-log",
          account_subject: input.code === "missing-account"
            ? ""
            : input.code === "wrong-account"
              ? "not a normalized provider subject"
              : "google-subject-123456",
        };
        if (input.code === "missing-scopes") exchange.scopes = [common.scopes[0]];
        if (input.code === "added-scope") exchange.scopes = [
          ...common.scopes,
          "https://www.googleapis.com/auth/drive",
        ];
        if (input.code === "precomputed-commitment") {
          exchange.account_commitment = "hmac-sha256:caller-controlled";
        }
        exchangeCache.set(idempotencyKey, exchange);
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
      }, { headers: { "x-request-id": "mock-sheets-1", "x-lattice-remote-dispatch-proof": `sha256:${"a".repeat(64)}` } });
    }
    if (url.pathname === "/gmail/v1/users/me/messages/send") {
      return Response.json(
        { id: "msg-1", threadId: "thread-1" },
        { headers: { "x-request-id": "mock-gmail-1", "x-lattice-remote-dispatch-proof": `sha256:${"a".repeat(64)}` } },
      );
    }
    return Response.json({ error: "not_found" }, { status: 404 });
  },
};
