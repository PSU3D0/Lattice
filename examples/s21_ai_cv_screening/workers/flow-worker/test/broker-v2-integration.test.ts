import { File } from "node:buffer";
import { createHash, createHmac, createPrivateKey, createPublicKey, generateKeyPairSync, sign } from "node:crypto";
import { readFile } from "node:fs/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { FormData } from "undici";
import { Log, LogLevel, Miniflare } from "miniflare";
import { makeOperatorBundleFixture } from "../../../../../crates/broker-workers/workerd-tests/src/operator-bundle-fixture";

const runtimeLogs: string[] = [];
class MemoryLog extends Log { protected log(message: string) { runtimeLogs.push(message); } }
const memoryLog = new MemoryLog(LogLevel.DEBUG);

const deploymentKey = `lbk_${"c".repeat(64)}`;
const pepper = "c5-combined-production-pepper";
const serviceAuth = "c5-combined-service-auth-private";
const egressAuth = "c5-combined-egress-auth-private-123456";
const popSeed = Buffer.alloc(32, 7).toString("base64url");
const receiptSeed = Buffer.alloc(32, 0x11);
const receiptPrivateKey = createPrivateKey({
  key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), receiptSeed]),
  format: "der", type: "pkcs8",
});
const receiptPublicKey = createPublicKey(receiptPrivateKey).export({ format: "der", type: "spki" }).subarray(-32);
const receiptPublicKeyHash = `sha256:${createHash("sha256").update(receiptPublicKey).digest("hex")}`;
const DEPLOYMENT_AUTHORITY_PUBLIC_KEY_B64U = "aFWAEo9WCNNHHWc-OD5cUfopZ5TpCUpsVay617fsa4A"
const DEPLOYMENT_CONTRACT_SET_JCS = "{\"connector_ref\":\"connector.google.workspace@1\",\"contract_set_ref\":\"contract-set-fixture\",\"contracts\":[{\"claim_requirement_hash\":\"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"contract_hash\":\"sha256:8fbdd2dbb63877b92004b7b5e6a7dc665a0ec5788850e4a466c0b6200639de2a\",\"contract_id\":\"connector.google.gmail.send_message@1\",\"credential_response_policy\":{\"kind\":\"forbidden\",\"sensitive_headers\":[],\"sensitive_json_pointers\":[]}},{\"claim_requirement_hash\":\"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"contract_hash\":\"sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999\",\"contract_id\":\"connector.google.sheets.append_row@1\",\"credential_response_policy\":{\"kind\":\"forbidden\",\"sensitive_headers\":[],\"sensitive_json_pointers\":[]}}],\"critical_fields\":[],\"deployment_id\":\"deployment-c5\",\"extensions\":{},\"issuer\":\"operator-fixture\",\"key_id\":\"operator-authority-fixture\",\"org_id\":\"org-c5\",\"schema_version\":\"0.2\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"operator-authority-fixture\",\"value\":\"CeS4FKTG1iZ7BeAzm5oZo4q2aLzVLeLW7goO8yoPvnZ6mUj0m3jgFYMSNEhaPVZgBD48VS1deYy0G2OkcdMoCw\"}}"
const DEPLOYMENT_STANDING_AUTHORITY_JCS = "{\"connector_ref\":\"connector.google.workspace@1\",\"contract_set_hash\":\"sha256:f3a30b338c87daec46251145630938a0d2d0e96f5a77e571245133dc23bbfb88\",\"contract_set_ref\":\"contract-set-fixture\",\"critical_fields\":[],\"deployment_id\":\"deployment-c5\",\"expires_at\":\"2030-01-01T00:00:00Z\",\"extensions\":{},\"issuer\":\"operator-fixture\",\"key_id\":\"operator-authority-fixture\",\"maximum_budgets\":{\"connection_logical_calls\":3,\"dispatch_attempts_per_call\":1,\"flow_logical_calls\":3,\"logical_calls\":3,\"node_logical_calls\":1},\"not_before\":\"2026-01-01T00:00:00Z\",\"operator_policy_hash\":\"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"org_id\":\"org-c5\",\"required_assurance_predicates\":[{\"kind\":\"brokered_count\",\"predicate_id\":\"brokered-count-v2\",\"required_kernel_controls\":[\"durable_before_dispatch\",\"exact_redelivery\",\"pop_bound\"]}],\"schema_version\":\"0.2\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"operator-authority-fixture\",\"value\":\"2me_J8x7uA8BA2TvwSov4HL6---2Nv5PskZKXUi_nZhrcjr5iieaCYWXae392EUoTrVrbgx3desnw5BLatfYDg\"},\"standing_authority_ref\":\"standing-fixture\"}"
const OPERATOR_BUNDLE_KEYS = generateKeyPairSync("ed25519");
const OPERATOR_BUNDLE_KEY_ID = "operator-bundle-c5";
const OPERATOR_BUNDLE_PUBLIC_KEY_B64U = (OPERATOR_BUNDLE_KEYS.publicKey.export({ format: "jwk" }) as JsonWebKey).x!;
const GENERIC_ACTIVATION_RECIPIENT_KEY_ID = "private-channel-c5";
const GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U = Buffer.alloc(32, 9).toString("base64url");
const PROTOCOL_VECTORS = JSON.parse(await readFile("../../../../impl-docs/spec/credential-plane-protocol-vectors.json", "utf8"));
const OPERATOR_ARTIFACT_BUNDLE_JCS = makeOperatorBundleFixture({ protocolVectors: PROTOCOL_VECTORS, privateKey: OPERATOR_BUNDLE_KEYS.privateKey, keyId: OPERATOR_BUNDLE_KEY_ID, publicKeyB64u: OPERATOR_BUNDLE_PUBLIC_KEY_B64U, activationRecipientKeyId: GENERIC_ACTIVATION_RECIPIENT_KEY_ID, activationRecipientPublicKeyB64u: GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U });
const OPERATOR_ARTIFACT_BUNDLE_SHA256 = `sha256:${createHash("sha256").update(OPERATOR_ARTIFACT_BUNDLE_JCS).digest("hex")}`;
const flow = JSON.parse(await readFile("test/fixtures/s21-flow-ir.json", "utf8"));
const brokerModules: any[] = [
  { type: "ESModule", path: "index.js", contents: await readFile("../../../../crates/broker-workers/workerd-tests/build-production/index.js", "utf8") },
  { type: "CompiledWasm", path: "index_bg.wasm", contents: await readFile("../../../../crates/broker-workers/workerd-tests/build-production/index_bg.wasm") },
];
const sharedEgress = await readFile("../../../../crates/provider-google-workers/src/shared.mjs", "utf8");
const tokenModules: any[] = [
  { type: "ESModule", path: "token-worker.mjs", contents: await readFile("../../../../crates/provider-google-workers/src/token-worker.mjs", "utf8") },
  { type: "ESModule", path: "shared.mjs", contents: sharedEgress },
];
const providerModules: any[] = [
  { type: "ESModule", path: "provider-worker.mjs", contents: await readFile("../../../../crates/provider-google-workers/src/provider-worker.mjs", "utf8") },
  { type: "ESModule", path: "shared.mjs", contents: sharedEgress },
];

function canonical(value: any): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

const flowJson = canonical(flow);
const flowHash = `sha256:${createHash("sha256").update(flowJson).digest("hex")}`;
const lockHash = `sha256:${"5".repeat(64)}`;
const googleNodes = ["record_candidate", "confirm_candidate", "notify_hr"].map((alias) => {
  const node = flow.nodes.find((candidate: any) => candidate.alias === alias);
  if (node === undefined) throw new Error(`missing ${alias}`);
  const sheets = alias === "record_candidate";
  return {
    alias,
    id: node.id,
    contract: sheets ? "connector.google.sheets.append_row@1" : "connector.google.gmail.send_message@1",
    contractHash: sheets ? "sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999" : "sha256:8fbdd2dbb63877b92004b7b5e6a7dc665a0ec5788850e4a466c0b6200639de2a",
    slot: sheets ? "append_row" : "send_message",
  };
});
const authority = {
  schema_version: "0.1", critical_fields: [], org_id: "org-c5", principal: { kind: "deployment", id: "deployment-c5" }, flow_ir_hash: flowHash,
  nodes: Object.fromEntries(googleNodes.map((node) => [node.alias, { node_id: node.id, operations: [{ contract_id: node.contract, contract_hash: node.contractHash,
    call_budget: { max_logical_calls: 1, max_dispatch_attempts_per_call: 1 }, minimum_assurance: "brokered_count", required_attenuations: [], connection_aggregate_key: "google-workspace" }] }])),
  aggregate_ceilings: { flow: { max_logical_calls: 3 }, connections: { "google-workspace": { max_logical_calls: 3 } } },
};

function baseWorkers() {
  return [
    { name: "broker-c5", compatibilityDate: "2026-07-15", modules: brokerModules,
      bindings: { KEY_HASH_PEPPER: pepper, INVOKE_SERVICE_AUTH: serviceAuth, GOOGLE_EGRESS_SERVICE_AUTH: egressAuth,
        RECEIPT_SIGNING_SEED: "1".repeat(64), COMMITMENT_KEY: "2".repeat(64), BINDING_SIGNING_SEED: "4".repeat(64), BROKER_WORKER_WASM_SHA256: `sha256:${"e".repeat(64)}`, AUTH_DRIVER_WORKER_SHA256: `sha256:${"d".repeat(64)}`, GOOGLE_TOKEN_WORKER_SHA256: `sha256:${"c".repeat(64)}`, GOOGLE_PROVIDER_WORKER_SHA256: `sha256:${"b".repeat(64)}`, OPERATOR_ARTIFACT_BUNDLE_SHA256, OPERATOR_BUNDLE_KEY_ID, OPERATOR_BUNDLE_PUBLIC_KEY_B64U, GENERIC_ACTIVATION_RECIPIENT_KEY_ID, GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U, CUSTODY_ROOT_KEY: "3".repeat(64),
        PUBLIC_CALLBACK_BASE: "https://c5.example", OAUTH_REDIRECT_URI: "https://c5.example/v0.2/credential-callback", DEPLOYMENT_BOOTSTRAP_AUTH: "unused-bootstrap", DEPLOYMENT_AUTHORITY_PUBLIC_KEY_B64U, DEPLOYMENT_AUTHORITY_KEY_ID: "operator-authority-fixture", DEPLOYMENT_CONTRACT_SET_JCS, DEPLOYMENT_STANDING_AUTHORITY_JCS },
      d1Databases: { BROKER_DB: "c5-combined-db" }, durableObjects: {
        CONNECTION_REFRESH_DO: { className: "ConnectionRefreshDurableObject", useSQLite: true },
        CREDENTIAL_STATE_V2_DO: { className: "CredentialStateDurableObject", useSQLite: true },
        V2_AUTHORITY_DO: { className: "V2AuthorityDurableObject", useSQLite: true },
      }, serviceBindings: { GOOGLE_TOKEN_SERVICE: "token-egress-c5", GOOGLE_PROVIDER_SERVICE: "provider-egress-c5", AUTH_DRIVER_SERVICE: "c5-upstream" } },
    { name: "token-egress-c5", compatibilityDate: "2026-07-15", modules: tokenModules,
      bindings: { GOOGLE_EGRESS_SERVICE_AUTH: egressAuth, GOOGLE_OAUTH_CLIENT_ID: "owned-client-id", GOOGLE_OAUTH_CLIENT_SECRET: "owned-client-secret-private", GOOGLE_OAUTH_REDIRECT_URI: "https://c5.example/v0.2/credential-callback", GOOGLE_TOKEN_RESULT_KEY: "6".repeat(64) },
      durableObjects: { GOOGLE_TOKEN_IDEMPOTENCY: { className: "GoogleTokenIdempotency", useSQLite: true } }, serviceBindings: { GOOGLE_UPSTREAM: "c5-upstream" } },
    { name: "provider-egress-c5", compatibilityDate: "2026-07-15", modules: providerModules,
      bindings: { GOOGLE_EGRESS_SERVICE_AUTH: egressAuth }, durableObjects: { GOOGLE_PROVIDER_IDEMPOTENCY: { className: "GoogleProviderIdempotency", useSQLite: true } },
      serviceBindings: { GOOGLE_UPSTREAM: "c5-upstream" } },
    { name: "c5-upstream", scriptPath: "test/fixtures/c5-upstream.mjs", compatibilityDate: "2026-07-15", modules: true },
    { name: "s21-w4-pdf-extract", scriptPath: "deploy/extraction-worker/dist/index.mjs", compatibilityDate: "2026-07-15", modules: true,
      modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true }, { type: "ESModule", include: ["**/*.mjs"], fallthrough: true }] },
  ];
}

function options(bindingRef?: string): any {
  const workers: any[] = baseWorkers();
  if (bindingRef !== undefined) workers.push({ name: "s21-c5-flow", scriptPath: "deploy/flow-worker/build/worker/shim.mjs", compatibilityDate: "2024-09-23", modules: true,
    modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true }, { type: "ESModule", include: ["**/*.js", "**/*.mjs"], fallthrough: true }],
    durableObjects: { FLOW_DO: { className: "FlowDurableObject", useSQLite: true }, WORKSPACE_DO: { className: "WorkspaceDurableObject", useSQLite: true } },
    r2Buckets: ["WORKSPACE_BUCKET"], kvNamespaces: ["FLOW_KV"], serviceBindings: { LATTICE_EXTRACT_PDF: "s21-w4-pdf-extract", LATTICE_BROKER_PRIVATE: "broker-c5", LATTICE_S21_PROVIDER: "c5-upstream" },
    bindings: { LATTICE_S21_HTTP_MODE: "broker_v2", LATTICE_BROKER_BINDING_REF: bindingRef, LATTICE_BROKER_BUNDLE_ID: "bundle-s21-c5", LATTICE_BROKER_FLOW_IR_HASH: flowHash,
      LATTICE_BROKER_BINDING_LOCK_HASH: lockHash, LATTICE_BROKER_FLOW_ID: flow.id, LATTICE_BROKER_DEPLOYMENT_KEY: deploymentKey, LATTICE_BROKER_POP_SEED_B64U: popSeed,
      LATTICE_BROKER_RECEIPT_PUBLIC_KEY_B64U: receiptPublicKey.toString("base64url"), LATTICE_BROKER_RECEIPT_PUBLIC_KEY_HASH: receiptPublicKeyHash,
      LATTICE_BROKER_SERVICE_AUTH: serviceAuth, LATTICE_CONNECTOR_AUTH_LLM_API_KEY: "c5-llm-key", LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL: "https://gateway.ai.cloudflare.com/v1/c5/openai",
      LATTICE_MULTIPART_ARTIFACT_FIELD: "cv", LATTICE_MULTIPART_EXPECTED_CONTENT_TYPE: "application/pdf", LATTICE_MULTIPART_FILENAME_METADATA_FIELD: "cv_filename", LATTICE_MULTIPART_FILE_FIELD: "cv",
      LATTICE_MULTIPART_MAX_FILE_BYTES: "8388608", LATTICE_MULTIPART_MAX_TEXT_BYTES: "65536", LATTICE_MULTIPART_MAX_TOTAL_BYTES: "10485760", LATTICE_MULTIPART_REQUIRED_MAGIC: "%PDF-" } });
  return { workers, log: memoryLog };
}

const mf = new Miniflare(options());
let bindingRef = "";
let activationCounts: any;
const keys = generateKeyPairSync("ed25519");
let sessionRef = "";
let jti = 0;

function framed(domain: string, fields: (string | Buffer)[]) {
  const chunks = [Buffer.from(domain), Buffer.from([0])];
  for (const field of fields) { const bytes = Buffer.isBuffer(field) ? field : Buffer.from(field); const length = Buffer.alloc(4); length.writeUInt32BE(bytes.length); chunks.push(length, bytes); }
  return Buffer.concat(chunks);
}
function popHeaders(path: string, body: Buffer) {
  const timestamp = Math.floor(Date.now() / 1000); const requestJti = `c5-pop-${String(++jti).padStart(24, "0")}`;
  const transcript = framed("lattice.authenticated-request.ed25519.v1", [sessionRef, "lattice-broker", "POST", path, `sha256:${createHash("sha256").update(body).digest("hex")}`, String(timestamp), requestJti]);
  return { authorization: `Session ${sessionRef}`, "x-lattice-pop-jti": requestJti, "x-lattice-pop-timestamp": String(timestamp), "x-lattice-pop-signature": sign(null, transcript, keys.privateKey).toString("base64url") };
}
async function post(path: string, value: unknown, authenticated = true) {
  const body = Buffer.from(JSON.stringify(value));
  return (await mf.getWorker("broker-c5")).fetch(`http://broker${path}`, { method: "POST", headers: { "content-type": "application/json", ...(authenticated ? popHeaders(path, body) : {}) }, body });
}

beforeAll(async () => {
  await mf.ready;
  const db = await mf.getD1Database("BROKER_DB", "broker-c5");
  for (const name of ["0001_broker.sql", "0002_credential_plane_v2.sql", "0003_production_v2_cutover.sql", "0004_operator_artifact_bundle.sql"]) {
    const migration = await readFile(`../../../../crates/broker-workers/migrations/${name}`, "utf8");
    for (const statement of migration.split(";").map((item) => item.trim()).filter(Boolean)) await db.prepare(statement).run();
  }
  await db.prepare("INSERT INTO operator_artifact_bundles(deployment_id,bundle_hash,canonical_bundle_jcs,seeded_at) VALUES(?,?,?,?)")
    .bind("deployment-c5", OPERATOR_ARTIFACT_BUNDLE_SHA256, Buffer.from(OPERATOR_ARTIFACT_BUNDLE_JCS), Math.floor(Date.now() / 1000)).run();
  const keyHash = createHmac("sha256", pepper).update("deployment-key").update(Buffer.from([0])).update(deploymentKey).digest("hex");
  await db.prepare("INSERT INTO deployment_keys(org_id,deployment_id,key_hash,expires_at,revoked) VALUES(?,?,?,?,0)").bind("org-c5", "deployment-c5", keyHash, Math.floor(Date.now() / 1000) + 3600).run();
  const publicJwk = keys.publicKey.export({ format: "jwk" });
  const timestamp = Math.floor(Date.now() / 1000); const nonce = "c5-exchange-nonce-0000000000000001"; const audience = "lattice-broker-session";
  const deploymentId = createHash("sha256").update("deployment-key-id\0").update(deploymentKey).digest("hex");
  const transcript = framed("lattice.session-exchange.ed25519.v1", [`sha256:${deploymentId}`, publicJwk.x!, nonce, String(timestamp), audience]);
  const session = await post("/v0.2/sessions", { deployment_key: deploymentKey, client_public_key: publicJwk.x, client_nonce: nonce, timestamp, audience, signature: sign(null, transcript, keys.privateKey).toString("base64url") }, false);
  expect(session.status, await session.clone().text()).toBe(201); sessionRef = ((await session.json()) as any).session_ref;
  const intent = await post("/v0.2/connection-intents", { connector_ref: "connector.google.workspace@1", auth_profile_ref: "auth.google.workspace.oauth2@1", execution_lane: "semantic_broker", custody: "hosted_broker" });
  expect(intent.status, await intent.clone().text()).toBe(201); const intentBody = await intent.json() as any;
  const authUrl = new URL(intentBody.next_action.url); const callback = new URL("http://broker/v0.2/credential-callback"); callback.searchParams.set("state", authUrl.searchParams.get("state")!); callback.searchParams.set("code", "4/owned-c5-code");
  const activated = await (await mf.getWorker("broker-c5")).fetch(callback); const intentState = await db.prepare("SELECT status,activation_phase FROM connection_intents WHERE intent_ref=?").bind(intentBody.intent_ref).first<any>(); expect(activated.status, `${await activated.clone().text()} state=${JSON.stringify(intentState)}`).toBe(200); const connection = await activated.json() as any;
  activationCounts = await upstream();
  const bound = await post("/v0.2/bindings", { connection_ref: connection.connection_ref, deployment_id: "deployment-c5", bundle_id: "bundle-s21-c5", flow_ir_hash: flowHash,
    binding_lock_hash: lockHash, flow_id: flow.id, flow_ir_json: flowJson, authority_manifest_json: canonical(authority), contracts: [...new Set(googleNodes.map((node) => node.contract))] });
  expect(bound.status, await bound.clone().text()).toBe(201); bindingRef = ((await bound.json()) as any).binding_ref;
  await mf.setOptions(options(bindingRef)); await mf.ready;
}, 40_000);

afterAll(async () => { await mf.dispose(); });

function pdf(text: string) {
  const encoder = new TextEncoder();
  const escaped = text.replaceAll("\\", "\\\\").replaceAll("(", "\\(").replaceAll(")", "\\)");
  const stream = `BT /F1 12 Tf 72 720 Td (${escaped}) Tj ET`;
  const objects = ["<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>", `<< /Length ${encoder.encode(stream).length} >>\nstream\n${stream}\nendstream`, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"];
  let value = "%PDF-1.4\n% c5 combined fixture\n"; const offsets = [0];
  for (let index = 0; index < objects.length; index += 1) { offsets.push(encoder.encode(value).length); value += `${index + 1} 0 obj\n${objects[index]}\nendobj\n`; }
  const xref = encoder.encode(value).length; value += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (const offset of offsets.slice(1)) value += `${String(offset).padStart(10, "0")} 00000 n \n`;
  value += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  return encoder.encode(value);
}
function form(email: string, name = "Ada Lovelace") { const value = new FormData(); value.set("full_name", name); value.set("email", email); value.set("expectation", "Build reliable systems"); value.set("linkedin", "https://example.test/ada"); value.set("cv", new File([pdf(`${name} builds reliable systems`)], "resume.pdf", { type: "application/pdf" })); return value; }
async function upstream() { return (await (await mf.getWorker("c5-upstream")).fetch("http://upstream/__state")).json() as any; }


describe("compiled S21 -> production V2 broker -> owned egress", () => {
  it("executes exact effects, receipts and fail-closed replay budgets without exposing authority", async () => {
    const first = await (await mf.getWorker("s21-c5-flow")).fetch("http://s21-c5-flow/cv-screening", { method: "POST", body: form("ada@example.test") });
    const firstText = await first.text();
    const db = await mf.getD1Database("BROKER_DB", "broker-c5");
    const diagnostics = await db.prepare("SELECT artifact_kind,COUNT(*) AS count FROM v2_host_records GROUP BY artifact_kind").all<any>();
    expect(first.status, `${firstText} records=${JSON.stringify(diagnostics.results)}`).toBe(200); expect(JSON.parse(firstText).stored).toBe(true);
    let calls = await upstream();
    expect({ token: activationCounts.token, userinfo: activationCounts.userinfo }).toEqual({ token: 1, userinfo: 1 });
    expect({ llm: calls.llm, sheets: calls.sheets, gmail: calls.gmail }).toEqual({ llm: 1, sheets: 1, gmail: 2 });
    const receipts = await db.prepare("SELECT artifact_ref,canonical_artifact_json FROM v2_host_records WHERE artifact_kind='invocation_receipt'").all<any>();
    expect(receipts.results).toHaveLength(3); for (const row of receipts.results) expect(JSON.parse(row.canonical_artifact_json).schema_version).toBe("0.2");
    const second = await (await mf.getWorker("s21-c5-flow")).fetch("http://s21-c5-flow/cv-screening", { method: "POST", body: form("ada@example.test") });
    expect(second.status).toBe(200); expect((await second.json() as any).stored).toBe(false); expect((await upstream()).requests).toHaveLength(calls.requests.length);
    const kv = await mf.getKVNamespace("FLOW_KV", "s21-c5-flow");
    const terminalKey = "s21_ai_cv_screening_flow:screening_trigger:ada@example.test";
    await kv.delete(terminalKey);
    const brokerReplay = await (await mf.getWorker("s21-c5-flow")).fetch("http://s21-c5-flow/cv-screening", { method: "POST", body: form("ada@example.test") });
    expect(brokerReplay.status).toBe(200); expect((await brokerReplay.json() as any).stored).toBe(true);
    calls = await upstream(); expect({ sheets: calls.sheets, gmail: calls.gmail }).toEqual({ sheets: 1, gmail: 2 });
    await kv.delete(terminalKey);
    const altered = await (await mf.getWorker("s21-c5-flow")).fetch("http://s21-c5-flow/cv-screening", { method: "POST", body: form("ada@example.test", "Altered Ada") });
    expect(altered.status).toBe(500); calls = await upstream(); expect({ sheets: calls.sheets, gmail: calls.gmail }).toEqual({ sheets: 1, gmail: 2 });
    expect((await db.prepare("SELECT COUNT(*) AS count FROM v2_invocation_outbox").first<any>())?.count).toBe(3);
    const exposed = `${flowJson}\n${firstText}\n${await altered.text()}\n${runtimeLogs.join("\n")}`;
    for (const secret of [deploymentKey, serviceAuth, popSeed, "owned-access-token-private", "owned-refresh-token-private"]) expect(exposed).not.toContain(secret);
    expect(exposed).not.toMatch(/grant_[A-Za-z0-9_-]+|lbk_[A-Za-z0-9_-]+|Bearer\s+[A-Za-z0-9_-]+/);
    expect((await db.prepare("SELECT COUNT(*) AS count FROM sessions").first<any>())?.count).toBeGreaterThan(0);
    expect((await db.prepare("SELECT COUNT(*) AS count FROM v2_host_records WHERE artifact_kind='node_lease'").first<any>())?.count).toBe(3);
  }, 40_000);
});
