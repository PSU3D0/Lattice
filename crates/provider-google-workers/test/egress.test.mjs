import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";

const AUTH = "local-shared-egress-auth-value-123456789";
const mf = new Miniflare({
  workers: [
    {
      name: "token-egress", scriptPath: "src/token-worker.mjs", modules: true,
      compatibilityDate: "2026-07-15",
      bindings: {
        GOOGLE_EGRESS_SERVICE_AUTH: AUTH,
        GOOGLE_OAUTH_CLIENT_ID: "google-client-id-private",
        GOOGLE_OAUTH_CLIENT_SECRET: "google-client-secret-private",
        GOOGLE_OAUTH_REDIRECT_URI: "https://broker.example/v0.2/credential-callback",
        GOOGLE_TOKEN_RESULT_KEY: "33".repeat(32),
      },
      durableObjects: { GOOGLE_TOKEN_IDEMPOTENCY: { className: "GoogleTokenIdempotency", useSQLite: true } },
      serviceBindings: { GOOGLE_UPSTREAM: "mock-upstream" },
    },
    {
      name: "provider-egress", scriptPath: "src/provider-worker.mjs", modules: true,
      compatibilityDate: "2026-07-15", bindings: { GOOGLE_EGRESS_SERVICE_AUTH: AUTH },
      durableObjects: { GOOGLE_PROVIDER_IDEMPOTENCY: { className: "GoogleProviderIdempotency", useSQLite: true } },
      serviceBindings: { GOOGLE_UPSTREAM: "mock-upstream" },
    },
    { name: "mock-upstream", scriptPath: "test/mock-upstream.mjs", modules: true, compatibilityDate: "2026-07-15" },
  ],
});
let token;
let provider;
let upstream;
let sequence = 0;

function headers(prefix) {
  sequence += 1;
  return {
    "content-type": "application/json",
    "x-lattice-egress-auth": AUTH,
    "x-lattice-correlation-id": `${prefix}-correlation-${String(sequence).padStart(8, "0")}`,
    "x-lattice-idempotency-key": `${prefix}-idempotency-${String(sequence).padStart(8, "0")}`,
  };
}

beforeAll(async () => {
  token = await mf.getWorker("token-egress");
  provider = await mf.getWorker("provider-egress");
  upstream = await mf.getWorker("mock-upstream");
});
afterAll(async () => mf.dispose());

async function calls() { return (await (await upstream.fetch("http://mock/__calls")).json()); }

async function exchangeCode(code, prefix) {
  return token.fetch("http://token.internal/exchange", {
    method: "POST", headers: headers(prefix),
    body: JSON.stringify({
      code, code_verifier: "v".repeat(43), grant_type: "authorization_code",
      redirect_uri: "https://broker.example/v0.2/credential-callback",
    }),
  });
}

describe("private Google token and account egress", () => {
  it("constructs authorization only from secret client identity and the exact callback/scopes", async () => {
    const response = await token.fetch("http://token.internal/authorize", {
      method: "POST", headers: headers("authorize"),
      body: JSON.stringify({
        code_challenge: "c".repeat(43),
        redirect_uri: "https://broker.example/v0.2/credential-callback",
        response_type: "code",
        scopes: [
          "https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/gmail.send",
        ],
        state: "oauth-state-value-123456789",
      }),
    });
    expect(response.status).toBe(200);
    const value = await response.json();
    const url = new URL(value.authorization_url);
    expect(`${url.origin}${url.pathname}`).toBe("https://accounts.google.com/o/oauth2/v2/auth");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      client_id: "google-client-id-private",
      redirect_uri: "https://broker.example/v0.2/credential-callback",
      scope: "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/spreadsheets",
      state: "oauth-state-value-123456789",
      response_type: "code",
      code_challenge: "c".repeat(43),
      code_challenge_method: "S256",
      access_type: "offline",
      prompt: "consent",
    });
    expect(JSON.stringify(value)).not.toContain("google-client-secret-private");
  });

  it("performs auth-code PKCE, exact scope normalization, subject discovery, and idempotent replay", async () => {
    const h = headers("exchange");
    const body = JSON.stringify({
      code: "4/authorization-code-value", code_verifier: "v".repeat(43), grant_type: "authorization_code",
      redirect_uri: "https://broker.example/v0.2/credential-callback",
    });
    const first = await token.fetch("http://token.internal/exchange", { method: "POST", headers: h, body });
    const firstText = await first.text();
    expect(first.status).toBe(200);
    const value = JSON.parse(firstText);
    expect(value.account_subject).toBe("google-subject-123456");
    expect(value.scopes).toEqual([
      "https://www.googleapis.com/auth/gmail.send", "https://www.googleapis.com/auth/spreadsheets",
    ]);
    const before = (await calls()).length;
    const replay = await token.fetch("http://token.internal/exchange", { method: "POST", headers: h, body });
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual(value);
    expect((await calls()).length).toBe(before);
    const tokenCall = (await calls()).find((call) => call.url === "https://oauth2.googleapis.com/token");
    expect(tokenCall.method).toBe("POST");
    const form = new URLSearchParams(tokenCall.body);
    expect(form.get("client_id")).toBe("google-client-id-private");
    expect(form.get("client_secret")).toBe("google-client-secret-private");
    expect(JSON.stringify(value)).not.toContain("google-client-secret-private");
  });

  it.each([
    ["missing-subject", "a response missing both subject fields"],
    ["wrong-audience", "a token minted for another OAuth client"],
    ["unknown-claim", "a genuinely unknown tokeninfo field"],
    ["wrong-scope", "a scope mismatch"],
  ])("fails closed for %s (%s)", async (code) => {
    const response = await exchangeCode(code, `tokeninfo-${code}`);
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "provider_response_invalid" });
  });

  it("accepts legacy user_id only when the modern sub field is absent", async () => {
    const response = await exchangeCode("legacy-subject", "tokeninfo-legacy-subject");
    expect(response.status).toBe(200);
    expect((await response.json()).account_subject).toBe("legacy-google-subject-654321");
  });

  it("fails closed when an authorization-code exchange omits the required refresh token", async () => {
    const response = await exchangeCode("missing-refresh-token", "exchange-missing-refresh");
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "provider_response_invalid" });
  });

  it("revokes only through the pinned lifecycle route with idempotent custody", async () => {
    const h = headers("revoke");
    const body = JSON.stringify({ token: "refresh-token-private-value" });
    const response = await token.fetch("http://token.internal/revoke", { method: "POST", headers: h, body });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ revoked: true });
    expect((await token.fetch("http://token.internal/revoke", { method: "POST", headers: h, body })).status).toBe(200);
    expect((await calls()).filter((call) => call.url === "https://oauth2.googleapis.com/revoke")).toHaveLength(1);
  });

  it("accepts Google's normal refresh response without a replacement refresh token", async () => {
    const response = await token.fetch("http://token.internal/refresh", {
      method: "POST", headers: headers("refresh"),
      body: JSON.stringify({ grant_type: "refresh_token", refresh_token: "refresh-token-private-value" }),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      access_token: "access-token-private-value",
      expires_in: 3600,
      scopes: [
        "https://www.googleapis.com/auth/gmail.send",
        "https://www.googleapis.com/auth/spreadsheets",
      ],
    });
  });

  it("uses the unchanged exact scopes when Google omits scope on refresh", async () => {
    const response = await token.fetch("http://token.internal/refresh", {
      method: "POST", headers: headers("refresh-no-scope"),
      body: JSON.stringify({ grant_type: "refresh_token", refresh_token: "refresh-without-scope" }),
    });
    expect(response.status).toBe(200);
    expect((await response.json()).scopes).toEqual([
      "https://www.googleapis.com/auth/gmail.send",
      "https://www.googleapis.com/auth/spreadsheets",
    ]);
  });

  it.each(["refresh-missing-access-token", "refresh-missing-expires-in", "refresh-wrong-scope"])(
    "fails closed for an invalid refresh response (%s)",
    async (refreshToken) => {
      const response = await token.fetch("http://token.internal/refresh", {
        method: "POST", headers: headers(refreshToken),
        body: JSON.stringify({ grant_type: "refresh_token", refresh_token: refreshToken }),
      });
      expect(response.status).toBe(503);
      expect(await response.json()).toEqual({ error: "provider_response_invalid" });
    },
  );

  it.each(["refresh-lowercase-bearer", "refresh-with-rotation"])(
    "accepts a documented refresh response variant (%s)",
    async (refreshToken) => {
      const response = await token.fetch("http://token.internal/refresh", {
        method: "POST", headers: headers(refreshToken),
        body: JSON.stringify({ grant_type: "refresh_token", refresh_token: refreshToken }),
      });
      expect(response.status).toBe(200);
    },
  );

  it("rejects caller credentials and wrong redirects", async () => {
    for (const body of [
      { grant_type: "refresh_token", refresh_token: "refresh-token-private-value", client_secret: "caller" },
      { code: "authorization-code-value", code_verifier: "v".repeat(43), grant_type: "authorization_code", redirect_uri: "https://evil.example/callback" },
    ]) {
      const rejected = await token.fetch(`http://token.internal/${body.code ? "exchange" : "refresh"}`, {
        method: "POST", headers: headers("reject"), body: JSON.stringify(body),
      });
      expect(rejected.status).toBe(400);
    }
    expect((await token.fetch("http://token.internal/refresh", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })).status).toBe(401);
  });
});

describe("private exact Google provider egress", () => {
  it("dispatches only exact Gmail send and Sheets append plans and suppresses duplicate dispatch", async () => {
    const gmailHeaders = { ...headers("gmail"), accept: "application/json", authorization: "Bearer access-token-private-value" };
    const gmailBody = JSON.stringify({ raw: "SGVsbG8" });
    const gmail = await provider.fetch("http://provider.internal/gmail/v1/users/me/messages/send", {
      method: "POST", headers: gmailHeaders, body: gmailBody,
    });
    expect(gmail.status).toBe(200);
    expect(gmail.headers.get("x-request-id")).toBe("gmail-request-1");
    const before = (await calls()).length;
    expect((await provider.fetch("http://provider.internal/gmail/v1/users/me/messages/send", {
      method: "POST", headers: gmailHeaders, body: gmailBody,
    })).status).toBe(200);
    expect((await calls()).length).toBe(before);

    const sheets = await provider.fetch("http://provider.internal/v4/spreadsheets/sheet_1/values/Sheet1:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS", {
      method: "POST",
      headers: { ...headers("sheets"), accept: "application/json", authorization: "Bearer access-token-private-value" },
      body: JSON.stringify({ values: [["one", 2]] }),
    });
    expect(sheets.status).toBe(200);
    expect(sheets.headers.get("x-request-id")).toBe("sheets-request-1");
    const providerCalls = (await calls()).filter((call) => call.url.includes("googleapis.com"));
    expect(providerCalls.some((call) => call.url.startsWith("https://gmail.googleapis.com/gmail/v1/users/me/messages/send"))).toBe(true);
    expect(providerCalls.some((call) => call.url.startsWith("https://sheets.googleapis.com/v4/spreadsheets/sheet_1/values/Sheet1:append"))).toBe(true);
  });

  it("has no arbitrary proxy surface and rejects method, origin-shaped path, query, headers, and bounds", async () => {
    const before = (await calls()).length;
    const baseHeaders = { ...headers("negative"), accept: "application/json", authorization: "Bearer access-token-private-value" };
    for (const [url, method, body, additions = {}] of [
      ["http://provider.internal/https://evil.example", "POST", "{}"],
      ["http://provider.internal/gmail/v1/users/me/messages/send?alt=json", "POST", JSON.stringify({ raw: "QQ" })],
      ["http://provider.internal/gmail/v1/users/me/messages/send", "GET", undefined],
      ["http://provider.internal/v4/spreadsheets/s/values/r:append?valueInputOption=RAW", "POST", JSON.stringify({ values: [[1]] })],
      ["http://provider.internal/gmail/v1/users/me/messages/send", "POST", JSON.stringify({ raw: "not+/base64", extra: true })],
    ]) {
      const response = await provider.fetch(url, { method, headers: { ...baseHeaders, ...additions }, body });
      expect([400, 404]).toContain(response.status);
    }
    expect((await calls()).length).toBe(before);
  });
});
