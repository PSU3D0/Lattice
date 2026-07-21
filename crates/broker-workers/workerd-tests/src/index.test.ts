import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";
import { readFile } from "node:fs/promises";
import { createHash, createHmac, generateKeyPairSync, randomBytes, sign } from "node:crypto";

const mf = new Miniflare({
  workers: [
    {
      name: "broker-private",
      scriptPath: "./build/index.js",
      compatibilityDate: "2026-07-15",
      modules: true,
      modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true }],
      bindings: {
        KEY_HASH_PEPPER: "local-pepper-not-a-production-secret",
        INVOKE_SERVICE_AUTH: "local-service-auth-not-production",
        RECEIPT_SIGNING_SEED: "1111111111111111111111111111111111111111111111111111111111111111",
        COMMITMENT_KEY: "2222222222222222222222222222222222222222222222222222222222222222",
        BINDING_SIGNING_SEED: "4444444444444444444444444444444444444444444444444444444444444444",
        CUSTODY_ROOT_KEY: "3333333333333333333333333333333333333333333333333333333333333333",
        LOCAL_TEST_MODE: "true",
        PUBLIC_CALLBACK_BASE: "https://broker-public.example",
        OAUTH_REDIRECT_URI: "https://broker-public.example/v1/oauth/callback",
        GOOGLE_AUTHORIZE_ENDPOINT: "https://accounts.example/authorize",
        GOOGLE_OAUTH_CLIENT_ID: "test-client-id-12345",
        DEPLOYMENT_BOOTSTRAP_AUTH: "local-bootstrap-auth-not-production",
      },
      d1Databases: { BROKER_DB: "broker-test-db" },
      durableObjects: {
        BROKER_LEDGER_DO: { className: "BrokerLedgerDurableObject", useSQLite: true },
        CONNECTION_REFRESH_DO: { className: "ConnectionRefreshDurableObject", useSQLite: true },
      },
      serviceBindings: {
        GOOGLE_TOKEN_SERVICE: "mock-services",
        GOOGLE_PROVIDER_SERVICE: "mock-services",
      },
    },
    {
      name: "broker-public",
      scriptPath: "./build/public.mjs",
      compatibilityDate: "2026-07-15",
      modules: true,
      serviceBindings: { BROKER_PRIVATE: "broker-private" },
    },
    {
      name: "mock-services",
      scriptPath: "src/mock-provider.mjs",
      compatibilityDate: "2026-07-15",
      modules: true,
    },
  ],
});

const productionMf = new Miniflare({
  workers: [{
    name: "broker-production",
    scriptPath: "./build-production/index.js",
    compatibilityDate: "2026-07-15",
    modules: true,
    modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true }],
    bindings: {
      KEY_HASH_PEPPER: "production-route-test-pepper",
      INVOKE_SERVICE_AUTH: "production-route-test-service-auth",
      RECEIPT_SIGNING_SEED: "1".repeat(64), COMMITMENT_KEY: "2".repeat(64),
      BINDING_SIGNING_SEED: "4".repeat(64), CUSTODY_ROOT_KEY: "3".repeat(64),
      PUBLIC_CALLBACK_BASE: "https://production.example",
      OAUTH_REDIRECT_URI: "https://production.example/v1/oauth/callback",
      GOOGLE_AUTHORIZE_ENDPOINT: "https://accounts.example/authorize",
      GOOGLE_OAUTH_CLIENT_ID: "production-client-id",
      DEPLOYMENT_BOOTSTRAP_AUTH: "production-route-test-bootstrap",
    },
    d1Databases: { BROKER_DB: "broker-production-route-db" },
    durableObjects: {
      BROKER_LEDGER_DO: { className: "BrokerLedgerDurableObject", useSQLite: true },
      CONNECTION_REFRESH_DO: { className: "ConnectionRefreshDurableObject", useSQLite: true },
    },
  }],
});

let publicWorker: Fetcher;
let privateWorker: Fetcher;
const sessionKeys = new Map<string, ReturnType<typeof generateKeyPairSync>["privateKey"]>();
let requestJti = 0;

function framed(domain: string, fields: (string | Buffer)[]) {
  const chunks = [Buffer.from(domain), Buffer.from([0])];
  for (const field of fields) {
    const bytes = Buffer.isBuffer(field) ? field : Buffer.from(field);
    const length = Buffer.alloc(4);
    length.writeUInt32BE(bytes.length);
    chunks.push(length, bytes);
  }
  return Buffer.concat(chunks);
}

function requestHeaders(
  sessionRef: string,
  method: string,
  path: string,
  body: Buffer,
  override: { timestamp?: number; jti?: string; privateKey?: ReturnType<typeof generateKeyPairSync>["privateKey"] } = {},
) {
  const privateKey = override.privateKey ?? sessionKeys.get(sessionRef);
  if (privateKey === undefined) throw new Error("unknown test session");
  const timestamp = override.timestamp ?? Math.floor(Date.now() / 1000);
  const jti = override.jti ?? `request-jti-${String(++requestJti).padStart(24, "0")}`;
  const url = new URL(`http://broker${path}`);
  const requestTarget = url.search.length === 0
    ? url.pathname
    : `${url.pathname}${url.search}`;
  const bodyHash = `sha256:${createHash("sha256").update(body).digest("hex")}`;
  const transcript = framed("lattice.authenticated-request.ed25519.v1", [
    sessionRef, "lattice-broker", method, requestTarget, bodyHash, String(timestamp), jti,
  ]);
  return {
    authorization: `Session ${sessionRef}`,
    "x-lattice-pop-jti": jti,
    "x-lattice-pop-timestamp": String(timestamp),
    "x-lattice-pop-signature": sign(null, transcript, privateKey).toString("base64url"),
  };
}
const fixtureKey = `lbk_${"a".repeat(64)}`;

async function jsonRequest(
  worker: Fetcher,
  path: string,
  body: unknown,
  headers: Record<string, string> = {},
) {
  const exactBody = Buffer.from(JSON.stringify(body));
  const sessionRef = headers.authorization?.replace(/^Session /, "");
  const pop = sessionRef === undefined ? {} : requestHeaders(sessionRef, "POST", path, exactBody);
  return worker.fetch(`http://broker${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers, ...pop },
    body: exactBody,
  });
}

beforeAll(async () => {
  publicWorker = await mf.getWorker("broker-public");
  privateWorker = await mf.getWorker("broker-private");
  const db = await mf.getD1Database("BROKER_DB", "broker-private");
  const migration = await readFile("../migrations/0001_broker.sql", "utf8");
  for (const statement of migration.split(";").map((value) => value.trim()).filter(Boolean)) {
    await db.prepare(statement).run();
  }
  const fixture = await jsonRequest(privateWorker, "/__test/fixtures", {
    fixture: "google-semantic-broker-v1",
  });
  expect(fixture.status).toBe(201);
});

afterAll(async () => {
  await mf.dispose();
  await productionMf.dispose();
});

async function session(key = fixtureKey) {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  const jwk = publicKey.export({ format: "jwk" });
  if (jwk.x === undefined) throw new Error("missing Ed25519 public key");
  const timestamp = Math.floor(Date.now() / 1000);
  const nonce = `exchange-${randomBytes(24).toString("base64url")}`;
  const keyHash = createHmac("sha256", "local-pepper-not-a-production-secret")
    .update("deployment-key").update(Buffer.from([0])).update(key).digest("hex");
  const audience = "lattice-broker-session";
  const transcript = framed("lattice.session-exchange.ed25519.v1", [
    `sha256:${keyHash}`, jwk.x, nonce, String(timestamp), audience,
  ]);
  const response = await jsonRequest(publicWorker, "/v1/sessions", {
    deployment_key: key,
    client_public_key: jwk.x,
    client_nonce: nonce,
    timestamp,
    audience,
    signature: sign(null, transcript, privateKey).toString("base64url"),
  });
  const body = await response.json() as any;
  if (response.status === 201) sessionKeys.set(body.session_ref, privateKey);
  return { response, body };
}

function sessionHeaders(sessionRef: string) {
  return { authorization: `Session ${sessionRef}` };
}

async function authenticatedFetch(worker: Fetcher, path: string, sessionRef: string, method = "GET") {
  const headers = requestHeaders(sessionRef, method, path, Buffer.alloc(0));
  return worker.fetch(`http://broker${path}`, { method, headers });
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right));
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function authorityFixture() {
  const contractId = "connector.google.sheets.append_row@1";
  const contractHash = "sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999";
  const flow = {
    id: "flow-installed",
    name: "flow-installed",
    version: "1.0.0",
    profile: "dev",
    nodes: [{
      id: "node-installed",
      alias: "sheets",
      identifier: "fixture::sheets",
      name: "Sheets",
      kind: "activity",
      in_schema: { kind: "opaque" },
      out_schema: { kind: "opaque" },
      effects: "effectful",
      determinism: "best_effort",
      idempotency: {},
      determinismHints: [],
      effectHints: [],
      connectorOps: [{
        operation_id: "connector.google.sheets.append_row",
        connector_id: "connector.google.sheets",
        roles: [],
        default_resolution_mode: "bound_connection",
        selected_resolution_mode: "bound_connection",
        supported_resolution_modes: ["bound_connection"],
      }],
      broker_authority: {
        operation_budgets: [{
          contract_id: contractId,
          semantic_effect_slots: ["append_row"],
          max_logical_calls: 2,
          max_dispatch_attempts_per_call: 1,
          connection_aggregate_key: "workspace",
        }],
        flow_aggregate_max_logical_calls: 2,
        connection_aggregate_max_logical_calls: { workspace: 2 },
      },
    }],
    edges: [],
    control_surfaces: [],
    checkpoints: [],
    policies: { lint: { require_control_hints: false } },
    metadata: { tags: [] },
    artifacts: [],
  };
  const flowIrJson = canonicalJson(flow);
  const flowIrHash = `sha256:${createHash("sha256").update(flowIrJson).digest("hex")}`;
  const manifest = {
    schema_version: "0.1",
    critical_fields: [],
    org_id: "org-fixture",
    principal: { kind: "deployment", id: "deployment-fixture" },
    flow_ir_hash: flowIrHash,
    nodes: {
      sheets: {
        node_id: "node-installed",
        operations: [{
          contract_id: contractId,
          contract_hash: contractHash,
          call_budget: {
            max_logical_calls: 2,
            max_dispatch_attempts_per_call: 1,
          },
          minimum_assurance: "brokered_count",
          required_attenuations: [],
          connection_aggregate_key: "workspace",
        }],
      },
    },
    aggregate_ceilings: {
      flow: { max_logical_calls: 2 },
      connections: { workspace: { max_logical_calls: 2 } },
    },
  };
  return {
    flowId: flow.id,
    nodeId: "node-installed",
    nodeAlias: "sheets",
    contractId,
    flowIrJson,
    flowIrHash,
    authorityManifestJson: canonicalJson(manifest),
  };
}

function effectId(runId: string, nodeId: string, ordinal: bigint, slot: string) {
  const fields: Buffer[] = [];
  for (const value of [runId, nodeId]) {
    const bytes = Buffer.from(value);
    const length = Buffer.alloc(4);
    length.writeUInt32BE(bytes.length);
    fields.push(length, bytes);
  }
  const ordinalBytes = Buffer.alloc(8);
  ordinalBytes.writeBigUInt64BE(ordinal);
  fields.push(ordinalBytes);
  const slotBytes = Buffer.from(slot);
  const slotLength = Buffer.alloc(4);
  slotLength.writeUInt32BE(slotBytes.length);
  fields.push(slotLength, slotBytes);
  const digest = createHash("sha256")
    .update(Buffer.from("lattice.logical-effect.v0.1\0"))
    .update(Buffer.concat(fields))
    .digest("hex");
  return `sha256:${digest}`;
}

describe("broker Worker management and topology", () => {
  it("keeps fixture code absent from the production WASM route table", async () => {
    const production = await productionMf.getWorker("broker-production");
    const response = await production.fetch("http://broker/__test/fixtures", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ fixture: "google-semantic-broker-v1" }),
    });
    expect(response.status).toBe(404);
  });

  it("keeps fixture and invoke routes off the public facade", async () => {
    const fixture = await jsonRequest(publicWorker, "/__test/fixtures", {
      fixture: "google-semantic-broker-v1",
    });
    expect(fixture.status).toBe(404);
    const invoke = await jsonRequest(publicWorker, "/internal/v1/invoke", {});
    expect(invoke.status).toBe(404);
  });

  it("exchanges a hashed deployment key for an expiring PoP-bound session", async () => {
    const good = await session();
    expect(good.response.status).toBe(201);
    expect(good.body.session_ref).toMatch(/^session_/);
    expect(good.body).not.toHaveProperty("deployment_key");

    const wrong = await session(`lbk_${"c".repeat(64)}`);
    expect(wrong.response.status).toBe(401);
    expect(JSON.stringify(wrong.body)).not.toContain(fixtureKey);
  });

  it("fails unknown lane/custody/profile and caller provider configuration closed", async () => {
    const { body } = await session();
    const valid = {
      connector_ref: "connector.google.workspace@1",
      auth_profile_ref: "auth.google.workspace.oauth2@1",
      execution_lane: "semantic_broker",
      custody: "hosted_broker",
    };
    for (const mutation of [
      { execution_lane: "onecli" },
      { custody: "local_vault" },
      { auth_profile_ref: "unknown" },
      { token_url: "https://evil.example/token" },
      { scopes: ["admin"] },
    ]) {
      const response = await jsonRequest(
        publicWorker,
        "/v1/connection-intents",
        { ...valid, ...mutation },
        sessionHeaders(body.session_ref),
      );
      expect(response.status).toBe(400);
    }
  });

  it("uses one provider-neutral callback and rejects state replay", async () => {
    const { body: auth } = await session();
    const intentResponse = await jsonRequest(
      publicWorker,
      "/v1/connection-intents",
      {
        connector_ref: "connector.google.workspace@1",
        auth_profile_ref: "auth.google.workspace.oauth2@1",
        execution_lane: "semantic_broker",
        custody: "hosted_broker",
      },
      sessionHeaders(auth.session_ref),
    );
    expect(intentResponse.status).toBe(201);
    const intent = await intentResponse.json() as any;
    const callback = new URL(intent.next_action.url);
    expect(callback.origin).toBe("https://accounts.example");
    expect(callback.pathname).toBe("/authorize");
    expect(callback.searchParams.get("client_id")).toBe("test-client-id-12345");
    expect(callback.searchParams.get("redirect_uri")).toBe("https://broker-public.example/v1/oauth/callback");
    expect(callback.searchParams.get("code_challenge_method")).toBe("S256");
    expect(callback.searchParams.get("code_challenge")).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(callback.searchParams.get("scope")?.split(" ").sort()).toEqual([
      "https://www.googleapis.com/auth/gmail.send",
      "https://www.googleapis.com/auth/spreadsheets",
    ]);
    const state = callback.searchParams.get("state")!;
    const callbackRequest = new URL("http://broker/v1/oauth/callback");
    callbackRequest.searchParams.set("state", state);
    callbackRequest.searchParams.set("code", "mock-oauth-code-never-log");
    const first = await publicWorker.fetch(callbackRequest);
    expect(first.status).toBe(200);
    const firstText = await first.text();
    expect(firstText).not.toContain("mock-oauth-code-never-log");
    expect(firstText).not.toContain("mock-access-never-log");
    const connection = JSON.parse(firstText);
    const authority = authorityFixture();
    const validBinding = {
      connection_ref: connection.connection_ref,
      deployment_id: "deployment-fixture",
      bundle_id: "bundle-installed",
      flow_ir_hash: authority.flowIrHash,
      binding_lock_hash: `sha256:${"3".repeat(64)}`,
      flow_id: authority.flowId,
      flow_ir_json: authority.flowIrJson,
      authority_manifest_json: authority.authorityManifestJson,
      contracts: [authority.contractId],
    };
    const tamperedManifest = JSON.parse(authority.authorityManifestJson);
    tamperedManifest.nodes.sheets.operations[0].call_budget.max_logical_calls = 1;
    const tampered = await jsonRequest(
      publicWorker,
      "/v1/bindings",
      { ...validBinding, authority_manifest_json: canonicalJson(tamperedManifest) },
      sessionHeaders(auth.session_ref),
    );
    expect(tampered.status).toBe(400);
    const wrongHash = await jsonRequest(
      publicWorker,
      "/v1/bindings",
      { ...validBinding, flow_ir_hash: `sha256:${"9".repeat(64)}` },
      sessionHeaders(auth.session_ref),
    );
    expect(wrongHash.status).toBe(400);
    const bindingResponse = await jsonRequest(
      publicWorker,
      "/v1/bindings",
      validBinding,
      sessionHeaders(auth.session_ref),
    );
    expect(bindingResponse.status).toBe(201);
    const binding = await bindingResponse.json() as any;
    expect(binding.attestation.lane).toBe("semantic_broker");
    expect(binding.attestation.signature.value.length).toBeGreaterThan(32);
    const validGrant = {
      org_id: "org-fixture",
      deployment_id: "deployment-fixture",
      session_ref: auth.session_ref,
      binding_ref: binding.binding_ref,
      bundle_id: "bundle-installed",
      flow_ir_hash: authority.flowIrHash,
      binding_lock_hash: `sha256:${"3".repeat(64)}`,
      flow_id: authority.flowId,
      node_id: authority.nodeId,
      node_alias: authority.nodeAlias,
      run_id: "run-installed",
      operation_contract: authority.contractId,
    };
    for (const mutation of [
      { node_id: "wrong-node" },
      { operation_contract: "connector.google.gmail.send_message@1" },
      { binding_ref: "binding_wrong_connection" },
      { connection_ref: "caller-selected-connection" },
    ]) {
      const rejected = await jsonRequest(
        privateWorker,
        "/internal/v1/grants",
        { ...validGrant, ...mutation },
        { "x-lattice-service-auth": "local-service-auth-not-production" },
      );
      expect([400, 403]).toContain(rejected.status);
    }
    const grant = await jsonRequest(
      privateWorker,
      "/internal/v1/grants",
      validGrant,
      { "x-lattice-service-auth": "local-service-auth-not-production" },
    );
    expect(grant.status).toBe(201);
    const grantBody = await grant.json() as any;
    expect(grantBody.grant_ref).toMatch(/^grant_/);
    const db = await mf.getD1Database("BROKER_DB", "broker-private");
    const storedGrant = await db.prepare("SELECT canonical_grant FROM grants WHERE grant_ref = ?")
      .bind(grantBody.grant_ref).first<{ canonical_grant: string }>();
    expect(JSON.parse(storedGrant!.canonical_grant).budgets).toEqual({
      logical_calls: 2,
      dispatch_attempts_per_call: 1,
    });
    const exhaustedGrant = await jsonRequest(
      privateWorker,
      "/internal/v1/grants",
      {
        org_id: "org-fixture",
        deployment_id: "deployment-fixture",
        session_ref: auth.session_ref,
        binding_ref: binding.binding_ref,
        bundle_id: "bundle-installed",
        flow_ir_hash: authority.flowIrHash,
        binding_lock_hash: `sha256:${"3".repeat(64)}`,
        flow_id: authority.flowId,
        node_id: authority.nodeId,
        node_alias: authority.nodeAlias,
        run_id: "run-installed-2",
        operation_contract: authority.contractId,
      },
      { "x-lattice-service-auth": "local-service-auth-not-production" },
    );
    expect(exhaustedGrant.status).toBe(409);
    const replay = await publicWorker.fetch(callbackRequest);
    expect(replay.status).toBe(400);
  });

  it("resumes OAuth activation idempotently at every credential saga boundary", async () => {
    const { body: auth } = await session();
    const db = await mf.getD1Database("BROKER_DB", "broker-private");
    for (const phase of [
      "route_reserved",
      "credential_registered_unrecorded",
      "credential_registered",
      "connection_inserted",
    ]) {
      const intentResponse = await jsonRequest(
        publicWorker,
        "/v1/connection-intents",
        {
          connector_ref: "connector.google.workspace@1",
          auth_profile_ref: "auth.google.workspace.oauth2@1",
          execution_lane: "semantic_broker",
          custody: "hosted_broker",
        },
        sessionHeaders(auth.session_ref),
      );
      expect(intentResponse.status).toBe(201);
      const intent = await intentResponse.json() as any;
      const authorization = new URL(intent.next_action.url);
      const callback = new URL("http://broker/v1/oauth/callback");
      callback.searchParams.set("state", authorization.searchParams.get("state")!);
      callback.searchParams.set("code", `saga-${phase}`);
      const crashed = await publicWorker.fetch(callback, {
        headers: { "x-lattice-test-activation-crash": phase },
      });
      expect(crashed.status).toBe(599);
      const interrupted = await db.prepare(
        "SELECT status, activation_phase FROM connection_intents WHERE intent_ref = ?",
      ).bind(intent.intent_ref).first<any>();
      expect(interrupted?.status).toBe("activating");
      const resumed = await publicWorker.fetch(callback);
      const resumedText = await resumed.text();
      expect(resumed.status, `${phase}: ${resumedText}`).toBe(200);
      const connection = JSON.parse(resumedText) as any;
      const completed = await db.prepare(
        "SELECT status FROM connection_intents WHERE intent_ref = ?",
      ).bind(intent.intent_ref).first<any>();
      expect(completed?.status).toBe("ready");
      const active = await db.prepare(
        "SELECT COUNT(*) AS count FROM connections WHERE intent_ref = ? AND connection_ref = ? AND status = 'active'",
      ).bind(intent.intent_ref, connection.connection_ref).first<any>();
      expect(active?.count).toBe(1);
    }
  });

  it("recovers claimed and exchange crashes and durably retries cleanup", async () => {
    const { body: auth } = await session();
    const db = await mf.getD1Database("BROKER_DB", "broker-private");
    const makeCallback = async (code: string) => {
      const response = await jsonRequest(publicWorker, "/v1/connection-intents", {
        connector_ref: "connector.google.workspace@1",
        auth_profile_ref: "auth.google.workspace.oauth2@1",
        execution_lane: "semantic_broker",
        custody: "hosted_broker",
      }, sessionHeaders(auth.session_ref));
      const intent = await response.json() as any;
      const authorization = new URL(intent.next_action.url);
      const callback = new URL("http://broker/v1/oauth/callback");
      callback.searchParams.set("state", authorization.searchParams.get("state")!);
      callback.searchParams.set("code", code);
      return { callback, intentRef: intent.intent_ref };
    };
    for (const phase of ["claimed", "exchange_inflight", "before_route_reserved"]) {
      const attempt = await makeCallback(`recover-${phase}`);
      const crashed = await publicWorker.fetch(attempt.callback, {
        headers: { "x-lattice-test-activation-crash": phase },
      });
      expect(crashed.status).toBe(599);
      const interrupted = await db.prepare(
        "SELECT status, activation_phase FROM connection_intents WHERE intent_ref = ?",
      ).bind(attempt.intentRef).first<any>();
      expect(interrupted?.status).toBe("claimed");
      expect(["exchange_pending", "exchange_inflight"]).toContain(interrupted?.activation_phase);
      const orphan = await db.prepare("SELECT COUNT(*) AS count FROM connections WHERE intent_ref = ?")
        .bind(attempt.intentRef).first<any>();
      expect(orphan?.count).toBe(0);
      expect((await publicWorker.fetch(attempt.callback)).status).toBe(200);
    }

    const ambiguous = await makeCallback("exchange-ambiguous");
    expect((await publicWorker.fetch(ambiguous.callback)).status).toBe(400);
    const ambiguousState = await db.prepare(
      "SELECT status, failure_code FROM connection_intents WHERE intent_ref = ?",
    ).bind(ambiguous.intentRef).first<any>();
    expect(ambiguousState).toEqual({ status: "restart_required", failure_code: "exchange_ambiguous" });
    expect((await db.prepare("SELECT COUNT(*) AS count FROM connections WHERE intent_ref = ?")
      .bind(ambiguous.intentRef).first<any>())?.count).toBe(0);

    const cleanup = await makeCallback("cleanup-retry");
    const failedCleanup = await publicWorker.fetch(cleanup.callback, {
      headers: { "x-lattice-test-activation-crash": "cleanup_revoke_failure" },
    });
    expect(failedCleanup.status).toBe(503);
    const pending = await db.prepare(
      "SELECT status, activation_phase FROM connection_intents WHERE intent_ref = ?",
    ).bind(cleanup.intentRef).first<any>();
    expect(pending).toEqual({ status: "cleanup_pending", activation_phase: "cleanup_pending" });
    expect((await db.prepare("SELECT COUNT(*) AS count FROM connections WHERE intent_ref = ? AND status = 'active'")
      .bind(cleanup.intentRef).first<any>())?.count).toBe(0);
    expect((await publicWorker.fetch(cleanup.callback)).status).toBe(400);
    const cleaned = await db.prepare("SELECT status, failure_code FROM connection_intents WHERE intent_ref = ?")
      .bind(cleanup.intentRef).first<any>();
    expect(cleaned).toEqual({ status: "restart_required", failure_code: "activation_cleanup_completed" });
    expect((await db.prepare("SELECT COUNT(*) AS count FROM connections WHERE intent_ref = ?")
      .bind(cleanup.intentRef).first<any>())?.count).toBe(0);
  });

  it("fails expired callback state and actual missing scopes/account or precomputed commitments", async () => {
    const { body: auth } = await session();
    const intentBody = {
      connector_ref: "connector.google.workspace@1",
      auth_profile_ref: "auth.google.workspace.oauth2@1",
      execution_lane: "semantic_broker",
      custody: "hosted_broker",
    };
    const makeCallback = async (code: string) => {
      const response = await jsonRequest(
        publicWorker,
        "/v1/connection-intents",
        intentBody,
        sessionHeaders(auth.session_ref),
      );
      expect(response.status).toBe(201);
      const intent = await response.json() as any;
      const authorization = new URL(intent.next_action.url);
      const callback = new URL("http://broker/v1/oauth/callback");
      callback.searchParams.set("state", authorization.searchParams.get("state")!);
      callback.searchParams.set("code", code);
      return { callback, intentRef: intent.intent_ref };
    };
    const db = await mf.getD1Database("BROKER_DB", "broker-private");
    const expired = await makeCallback("honest-code");
    await db.prepare("UPDATE connection_intents SET expires_at = ? WHERE intent_ref = ?")
      .bind(Math.floor(Date.now() / 1000) - 1, expired.intentRef).run();
    expect((await publicWorker.fetch(expired.callback)).status).toBe(400);
    for (const code of ["missing-scopes", "added-scope", "missing-account", "wrong-account", "precomputed-commitment"]) {
      const attempt = await makeCallback(code);
      const response = await publicWorker.fetch(attempt.callback);
      expect([400, 503]).toContain(response.status);
      const row = await db.prepare("SELECT status, failure_code FROM connection_intents WHERE intent_ref = ?")
        .bind(attempt.intentRef).first<{ status: string; failure_code: string }>();
      expect(row?.status).toBe("restart_required");
      expect(row?.failure_code).toMatch(/^[a-z_]+$/);
    }
  });

  it("rejects wrong PoP before creating an intent", async () => {
    const { body } = await session();
    const requestBody = Buffer.from(JSON.stringify({
      connector_ref: "connector.google.workspace@1",
      auth_profile_ref: "auth.google.workspace.oauth2@1",
      execution_lane: "semantic_broker",
      custody: "hosted_broker",
    }));
    const headers = requestHeaders(body.session_ref, "POST", "/v1/connection-intents", requestBody);
    headers["x-lattice-pop-signature"] = Buffer.alloc(64, 9).toString("base64url");
    const response = await publicWorker.fetch("http://broker/v1/connection-intents", {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body: requestBody,
    });
    expect(response.status).toBe(401);
  });

  it("rejects replay, JTI reuse, stale time, wrong key, and body/path/method mutation before effects", async () => {
    const { body: auth } = await session();
    const payload = Buffer.from(JSON.stringify({
      connector_ref: "connector.google.workspace@1",
      auth_profile_ref: "auth.google.workspace.oauth2@1",
      execution_lane: "semantic_broker",
      custody: "hosted_broker",
    }));
    const path = "/v1/connection-intents";
    const captured = requestHeaders(auth.session_ref, "POST", path, payload);
    const send = (target: string, method: string, body: Buffer, headers: Record<string, string>) =>
      publicWorker.fetch(`http://broker${target}`, {
        method,
        headers: { "content-type": "application/json", ...headers },
        body: method === "GET" ? undefined : body,
      });
    expect((await send(path, "POST", payload, captured)).status).toBe(201);
    expect((await send(path, "POST", payload, captured)).status).toBe(401);

    const bodyHeaders = requestHeaders(auth.session_ref, "POST", path, payload);
    expect((await send(path, "POST", Buffer.from(`${payload.toString()} `), bodyHeaders)).status).toBe(401);
    const pathHeaders = requestHeaders(auth.session_ref, "POST", path, payload);
    expect((await send("/v1/bindings", "POST", payload, pathHeaders)).status).toBe(401);
    const methodHeaders = requestHeaders(auth.session_ref, "POST", "/v1/connections/missing", Buffer.alloc(0));
    expect((await send("/v1/connections/missing", "GET", Buffer.alloc(0), methodHeaders)).status).toBe(401);
    const stale = requestHeaders(auth.session_ref, "POST", path, payload, {
      timestamp: Math.floor(Date.now() / 1000) - 61,
    });
    expect((await send(path, "POST", payload, stale)).status).toBe(401);
    const rawQuery = "/v1/connections/missing?a=1+2&a=%2f";
    const rawQueryHeaders = requestHeaders(auth.session_ref, "GET", rawQuery, Buffer.alloc(0));
    expect((await send("/v1/connections/missing?a=1+2&a=%2F", "GET", Buffer.alloc(0), rawQueryHeaders)).status).toBe(401);
    const deletePath = "/v1/connections/missing";
    const deleteHeaders = requestHeaders(auth.session_ref, "DELETE", deletePath, Buffer.alloc(0));
    const forbiddenBody = await publicWorker.fetch(`http://broker${deletePath}`, {
      method: "DELETE",
      headers: deleteHeaders,
      body: "forbidden",
    });
    expect(forbiddenBody.status).toBe(400);
    const wrong = generateKeyPairSync("ed25519").privateKey;
    const wrongHeaders = requestHeaders(auth.session_ref, "POST", path, payload, { privateKey: wrong });
    expect((await send(path, "POST", payload, wrongHeaders)).status).toBe(401);
    const reusedJti = `forced-jti-${"x".repeat(24)}`;
    const firstJti = requestHeaders(auth.session_ref, "POST", path, payload, { jti: reusedJti });
    expect((await send(path, "POST", payload, firstJti)).status).toBe(201);
    const secondJti = requestHeaders(auth.session_ref, "POST", path, payload, { jti: reusedJti });
    expect((await send(path, "POST", payload, secondJti)).status).toBe(401);
  });

  it("executes an exact Google descriptor through the private binding and redelivers the receipt", async () => {
    const { body: auth } = await session();
    const fixtureResponse = await jsonRequest(privateWorker, "/__test/fixtures", {
      fixture: "google-semantic-broker-v1",
      session_ref: auth.session_ref,
    });
    expect(fixtureResponse.status).toBe(201);
    const fixture = await fixtureResponse.json() as any;
    const logicalEffectId = effectId("run-fixture", "node-sheets", 1n, "append_row");
    const invocation = {
      session_ref: auth.session_ref,
      grant_ref: fixture.grants.sheets,
      bundle_id: "bundle-fixture",
      flow_ir_hash: `sha256:${"2".repeat(64)}`,
      binding_lock_hash: `sha256:${"3".repeat(64)}`,
      flow_id: "flow-fixture",
      node_id: "node-sheets",
      node_alias: "sheets",
      run_id: "run-fixture",
      activation_ordinal: 1,
      logical_effect_id: logicalEffectId,
      input: {
        spreadsheet_id: "sheet-1",
        sheet: "Sheet1",
        row: { column: "one" },
        value_input_option: "raw",
      },
    };
    const headers = {
      ...sessionHeaders(auth.session_ref),
      "x-lattice-service-auth": "local-service-auth-not-production",
    };
    const first = await jsonRequest(privateWorker, "/internal/v1/invoke", invocation, headers);
    const firstBody = await first.json() as any;
    expect(first.status, JSON.stringify(firstBody)).toBe(200);
    expect(firstBody.redelivery).toBe(false);
    expect(firstBody.receipt.contract_hash).toBe(
      "sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999",
    );
    expect(firstBody.receipt.claims.provider_dispatch_observed).toBe(true);
    const second = await jsonRequest(privateWorker, "/internal/v1/invoke", invocation, headers);
    expect(second.status).toBe(200);
    const secondBody = await second.json() as any;
    expect(secondBody.redelivery).toBe(true);
    expect(secondBody.receipt).toEqual(firstBody.receipt);

    const mockServices = await mf.getWorker("mock-services");
    await mockServices.fetch("http://mock/__oversize-next");
    const oversizedEffect = effectId("run-fixture", "node-sheets", 2n, "append_row");
    const oversized = await jsonRequest(
      privateWorker,
      "/internal/v1/invoke",
      {
        ...invocation,
        activation_ordinal: 2,
        logical_effect_id: oversizedEffect,
      },
      headers,
    );
    expect(oversized.status).toBe(200);
    expect((await oversized.json() as any).receipt.outcome).toBe("ambiguous");

    const publicText = JSON.stringify(firstBody);
    for (const secret of [fixtureKey, "fixture-access-never-log", "one"]) {
      expect(publicText).not.toContain(secret);
    }

    const gmailEffect = effectId("run-fixture", "node-gmail", 1n, "send_message");
    const gmail = await jsonRequest(
      privateWorker,
      "/internal/v1/invoke",
      {
        ...invocation,
        grant_ref: fixture.grants.gmail,
        node_id: "node-gmail",
        node_alias: "gmail",
        logical_effect_id: gmailEffect,
        input: {
          to: "test@example.com",
          subject: "Hello",
          text_body: "rfc822-body-never-receipt",
        },
      },
      headers,
    );
    expect(gmail.status).toBe(200);
    const gmailBody = await gmail.json() as any;
    expect(gmailBody.receipt.contract_hash).toBe(
      "sha256:8fbdd2dbb63877b92004b7b5e6a7dc665a0ec5788850e4a466c0b6200639de2a",
    );
    expect(JSON.stringify(gmailBody)).not.toContain("rfc822-body-never-receipt");

    const before = await (await (await mf.getWorker("mock-services")).fetch("http://mock/__counts")).json() as any;
    const hostile = await jsonRequest(
      privateWorker,
      "/internal/v1/invoke",
      { ...invocation, node_id: "wrong-node" },
      headers,
    );
    expect(hostile.status).toBe(403);
    const after = await (await (await mf.getWorker("mock-services")).fetch("http://mock/__counts")).json() as any;
    expect(after.provider).toBe(before.provider);
  });

  it("isolates receipt lookup by session tenant", async () => {
    const db = await mf.getD1Database("BROKER_DB", "broker-private");
    const row = await db.prepare("SELECT receipt_ref FROM receipts WHERE org_id = ? LIMIT 1")
      .bind("org-fixture")
      .first<{ receipt_ref: string }>();
    expect(row?.receipt_ref).toMatch(/^receipt_/);
    const otherSession = "session_other_000000000000000000000001";
    const { privateKey, publicKey } = generateKeyPairSync("ed25519");
    const jwk = publicKey.export({ format: "jwk" });
    const publicBytes = Buffer.from(jwk.x!, "base64url");
    await db.prepare(
      "INSERT INTO sessions (session_ref, org_id, deployment_id, pop_key_thumbprint, pop_public_key, expires_at, revoked) VALUES (?, ?, ?, ?, ?, ?, 0)",
    ).bind(
      otherSession,
      "org-other",
      "deployment-other",
      `sha256:${createHash("sha256").update(publicBytes).digest("hex")}`,
      jwk.x,
      Math.floor(Date.now() / 1000) + 300,
    ).run();
    sessionKeys.set(otherSession, privateKey);
    const response = await authenticatedFetch(
      publicWorker,
      `/v1/receipts/${row!.receipt_ref}`,
      otherSession,
    );
    expect(response.status).toBe(404);

    const crossDeploymentSession = "session_cross_deployment_00000000000001";
    const crossKeys = generateKeyPairSync("ed25519");
    const crossJwk = crossKeys.publicKey.export({ format: "jwk" });
    const crossBytes = Buffer.from(crossJwk.x!, "base64url");
    await db.prepare(
      "INSERT INTO sessions (session_ref, org_id, deployment_id, pop_key_thumbprint, pop_public_key, expires_at, revoked) VALUES (?, ?, ?, ?, ?, ?, 0)",
    ).bind(
      crossDeploymentSession,
      "org-fixture",
      "deployment-other",
      `sha256:${createHash("sha256").update(crossBytes).digest("hex")}`,
      crossJwk.x,
      Math.floor(Date.now() / 1000) + 300,
    ).run();
    sessionKeys.set(crossDeploymentSession, crossKeys.privateKey);
    const crossDeployment = await authenticatedFetch(
      publicWorker,
      `/v1/receipts/${row!.receipt_ref}`,
      crossDeploymentSession,
    );
    expect(crossDeployment.status).toBe(404);
  });

  it("fails missing service authentication without touching a provider", async () => {
    const { body } = await session();
    const mockWorker = await mf.getWorker("mock-services");
    const before = await (await mockWorker.fetch("http://mock/__counts")).json();
    const response = await jsonRequest(
      privateWorker,
      "/internal/v1/invoke",
      {},
      sessionHeaders(body.session_ref),
    );
    expect(response.status).toBe(401);
    const after = await (await mockWorker.fetch("http://mock/__counts")).json();
    expect(after).toEqual(before);
  });

  it("bootstraps one hashed deployment key only on the private service path", async () => {
    const db = await mf.getD1Database("BROKER_DB", "broker-private");
    await db.prepare("DELETE FROM deployment_keys").run();
    const body = {
      org_id: "org-bootstrap",
      deployment_id: "deployment-bootstrap",
      expires_at: Math.floor(Date.now() / 1000) + 3600,
    };
    const publicAttempt = await jsonRequest(publicWorker, "/internal/v1/bootstrap", body, {
      "x-lattice-bootstrap-auth": "local-bootstrap-auth-not-production",
    });
    expect(publicAttempt.status).toBe(404);
    const first = await jsonRequest(privateWorker, "/internal/v1/bootstrap", body, {
      "x-lattice-bootstrap-auth": "local-bootstrap-auth-not-production",
    });
    expect(first.status).toBe(201);
    const firstBody = await first.json() as any;
    expect(firstBody.deployment_key).toMatch(/^lbk_[A-Za-z0-9_-]{64}$/);
    const stored = await db.prepare("SELECT key_hash FROM deployment_keys LIMIT 1").first<{ key_hash: string }>();
    expect(stored?.key_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(JSON.stringify(stored)).not.toContain(firstBody.deployment_key);
    const repeat = await jsonRequest(privateWorker, "/internal/v1/bootstrap", body, {
      "x-lattice-bootstrap-auth": "local-bootstrap-auth-not-production",
    });
    expect(repeat.status).toBe(409);
  });
});

describe("broker ledger Durable Object", () => {
  it("persists exact replay and BRK203 conflict", async () => {
    const namespace = await mf.getDurableObjectNamespace("BROKER_LEDGER_DO", "broker-private");
    const authority = { org_id: "org-a", grant_ref: "grant-a" };
    const id = namespace.idFromName("authority-test");
    const stub = namespace.get(id);
    const key = {
      org_id: "org-a",
      deployment_id: "deployment-a",
      flow_ir_hash: "flow-a",
      run_id: "run-a",
      node_id: "node-a",
      logical_effect_id: "effect-a",
      operation_contract: "contract@1",
      connection_ref: "connection-a",
    };
    const reserve = (canonical_input: number[]) => ({
      authority,
      command: {
        op: "reserve",
        request: {
          key,
          canonical_input,
          max_logical_calls: 1,
          max_dispatch_attempts: 1,
          lease_deadline: 30,
        },
        now: 0,
      },
    });
    const send = (body: unknown) => stub.fetch("http://do/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const first = await send(reserve([123, 125]));
    expect(first.status).toBe(200);
    expect((await first.json() as any).acquired).toBe(true);
    const replay = await send(reserve([123, 125]));
    expect((await replay.json() as any).acquired).toBe(false);
    const altered = await send(reserve([91, 93]));
    expect(altered.status).toBe(409);
    expect(await altered.json()).toEqual({ error: "BRK203" });
  });

  it("compacts DO history without returning events and continues after the threshold", async () => {
    const namespace = await mf.getDurableObjectNamespace("BROKER_LEDGER_DO", "broker-private");
    const authority = { org_id: "org-compact", grant_ref: "grant-compact" };
    const stub = namespace.get(namespace.idFromName("authority-compaction"));
    const command = (index: number) => ({
      authority,
      command: {
        op: "reserve",
        request: {
          key: {
            org_id: "org-compact",
            deployment_id: "deployment-compact",
            flow_ir_hash: "flow-compact",
            run_id: "run-compact",
            node_id: "node-compact",
            logical_effect_id: `effect-${String(index).padStart(4, "0")}`,
            operation_contract: "contract@1",
            connection_ref: "connection-compact",
          },
          canonical_input: [123, 125],
          max_logical_calls: 301,
          max_dispatch_attempts: 1,
          lease_deadline: 30,
        },
        now: 0,
      },
    });
    for (let index = 0; index < 300; index++) {
      const response = await stub.fetch("http://do/", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(command(index)),
      });
      expect(response.status).toBe(200);
      expect(await response.json()).not.toHaveProperty("records");
    }
    const replay = await stub.fetch("http://do/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(command(0)),
    });
    expect((await replay.json() as any).acquired).toBe(false);
    const final = await stub.fetch("http://do/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(command(300)),
    });
    expect(final.status).toBe(200);
  });

  it("recovers expired, dispatched, terminal-outbox, failed, and concurrent states durably", async () => {
    const namespace = await mf.getDurableObjectNamespace("BROKER_LEDGER_DO", "broker-private");
    const authority = { org_id: "org-crash", grant_ref: "grant-crash" };
    const stub = namespace.get(namespace.idFromName("authority-crash-boundaries"));
    const key = (effect: string) => ({
      org_id: "org-crash",
      deployment_id: "deployment-crash",
      flow_ir_hash: "flow-crash",
      run_id: "run-crash",
      node_id: "node-crash",
      logical_effect_id: effect,
      operation_contract: "contract@1",
      connection_ref: "connection-crash",
    });
    const send = async (command: unknown) => {
      const response = await stub.fetch("http://do/", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ authority, command }),
      });
      return { response, body: await response.json() as any };
    };
    const reserve = (effect: string, deadline = 10) => ({
      op: "reserve",
      request: {
        key: key(effect),
        canonical_input: [123, 125],
        max_logical_calls: 5,
        max_dispatch_attempts: 1,
        lease_deadline: deadline,
      },
      now: 0,
    });
    const expired = await send(reserve("expired"));
    const token = expired.body.snapshot.lease_token;
    await send({
      op: "plan", key: key("expired"), lease_token: token, now: 0,
      data: {
        request_plan_hash: `sha256:${"1".repeat(64)}`,
        authority_facts_hash: `sha256:${"2".repeat(64)}`,
        implementation: "impl@1", endpoint: "https://provider.example", next_attempt: 0,
      },
    });
    const released = await send({ op: "release", key: key("expired"), lease_token: token, now: 10, expired: true });
    expect(released.body.snapshot.budget_after).toBe(5);
    const releasedAgain = await send({ op: "release", key: key("expired"), lease_token: token, now: 10, expired: true });
    expect(releasedAgain.body.snapshot.budget_after).toBe(5);
    const releaseReceipt = [123, 34, 111, 117, 116, 99, 111, 109, 101, 34, 58, 34, 114, 101, 106, 101, 99, 116, 101, 100, 34, 125];
    const releaseIssued = await send({ op: "issue_released_receipt", key: key("expired"), receipt: releaseReceipt });
    expect(releaseIssued.body.snapshot.state.ReceiptIssued.receipt).toEqual(releaseReceipt);

    const dispatched = await send(reserve("dispatched", 30));
    const dispatchedToken = dispatched.body.snapshot.lease_token;
    await send({
      op: "plan", key: key("dispatched"), lease_token: dispatchedToken, now: 0,
      data: {
        request_plan_hash: `sha256:${"1".repeat(64)}`,
        authority_facts_hash: `sha256:${"2".repeat(64)}`,
        implementation: "impl@1", endpoint: "https://provider.example", next_attempt: 0,
      },
    });
    await send({ op: "mark_dispatched", key: key("dispatched"), lease_token: dispatchedToken, now: 0 });
    const redelivery = await send(reserve("dispatched", 30));
    expect(redelivery.body.snapshot.state.Dispatched).toBeDefined();
    const ambiguousReceipt = [123, 34, 111, 117, 116, 99, 111, 109, 101, 34, 58, 34, 97, 109, 98, 105, 103, 117, 111, 117, 115, 34, 125];
    await send({
      op: "finish", key: key("dispatched"), outcome: "Ambiguous",
      receipt: ambiguousReceipt, response_projection: [], provider_request_id: null,
    });
    const issued = await send({ op: "issue_receipt", key: key("dispatched") });
    expect(issued.body.snapshot.state.ReceiptIssued.receipt).toEqual(ambiguousReceipt);
    const exact = await send(reserve("dispatched", 30));
    expect(exact.body.snapshot.state.ReceiptIssued.receipt).toEqual(ambiguousReceipt);

    const failed = await send(reserve("failed", 30));
    const failedToken = failed.body.snapshot.lease_token;
    await send({
      op: "plan", key: key("failed"), lease_token: failedToken, now: 0,
      data: {
        request_plan_hash: `sha256:${"1".repeat(64)}`,
        authority_facts_hash: `sha256:${"2".repeat(64)}`,
        implementation: "impl@1", endpoint: "https://provider.example", next_attempt: 0,
      },
    });
    await send({ op: "mark_dispatched", key: key("failed"), lease_token: failedToken, now: 0 });
    const failedReceipt = [123, 34, 111, 117, 116, 99, 111, 109, 101, 34, 58, 34, 102, 97, 105, 108, 101, 100, 34, 125];
    await send({
      op: "finish", key: key("failed"), outcome: "Failed",
      receipt: failedReceipt, response_projection: [], provider_request_id: null,
    });
    await send({ op: "issue_receipt", key: key("failed") });
    const failedRedelivery = await send(reserve("failed", 30));
    expect(failedRedelivery.body.snapshot.state.ReceiptIssued.receipt).toEqual(failedReceipt);

    const concurrent = await Promise.all(["a", "b", "c"].map((effect) => send(reserve(effect, 30))));
    expect(concurrent.filter(({ response }) => response.status === 200).length).toBe(3);
    const exhausted = await send(reserve("budget-exhausted", 30));
    expect(exhausted.response.status).toBe(409);
    expect(exhausted.body).toEqual({ error: "BRK201" });
  });
});
