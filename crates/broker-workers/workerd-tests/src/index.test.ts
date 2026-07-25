import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";
import { readFile } from "node:fs/promises";
import { createCipheriv, createHash, createHmac, createPrivateKey, createPublicKey, diffieHellman, generateKeyPairSync, randomBytes, sign, verify } from "node:crypto";
import { makeOperatorBundleFixture } from "./operator-bundle-fixture";

const DEPLOYMENT_AUTHORITY_PUBLIC_KEY_B64U = "G58A2khwWXCYU56xPu4z-vjh7zzLWW0mmoOVT6v27WA";
const GENERIC_PROFILE_KEYS = generateKeyPairSync("ed25519");
const GENERIC_PROFILE_PUBLIC_KEY_B64U = (GENERIC_PROFILE_KEYS.publicKey.export({ format: "jwk" }) as JsonWebKey).x!;
const GENERIC_ACTIVATION_KEYS = generateKeyPairSync("x25519");
const GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U = (GENERIC_ACTIVATION_KEYS.privateKey.export({ format: "jwk" }) as JsonWebKey).d!;
const GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U = (GENERIC_ACTIVATION_KEYS.publicKey.export({ format: "jwk" }) as JsonWebKey).x!;
const OPERATOR_BUNDLE_KEYS=generateKeyPairSync("ed25519");
const OPERATOR_BUNDLE_KEY_ID="operator-bundle-fixture";
const OPERATOR_BUNDLE_PUBLIC_KEY_B64U=(OPERATOR_BUNDLE_KEYS.publicKey.export({format:"jwk"}) as JsonWebKey).x!;
const PROTOCOL_VECTORS=JSON.parse(await readFile(new URL("../../../../impl-docs/spec/credential-plane-protocol-vectors.json",import.meta.url),"utf8"));
const OPERATOR_ARTIFACT_BUNDLE_JCS=makeOperatorBundleFixture({protocolVectors:PROTOCOL_VECTORS,privateKey:OPERATOR_BUNDLE_KEYS.privateKey,keyId:OPERATOR_BUNDLE_KEY_ID,publicKeyB64u:OPERATOR_BUNDLE_PUBLIC_KEY_B64U,activationRecipientKeyId:"private-channel-fixture",activationRecipientPublicKeyB64u:GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U});
const OPERATOR_ARTIFACT_BUNDLE_SHA256=`sha256:${createHash("sha256").update(OPERATOR_ARTIFACT_BUNDLE_JCS).digest("hex")}`;
const AUTH_DRIVER_SCRIPT = await readFile(new URL("../../deploy/auth-driver/src/index.mjs", import.meta.url), "utf8");
const DEPLOYMENT_CONTRACT_SET_JCS = "{\"connector_ref\":\"connector.google.workspace@1\",\"contract_set_ref\":\"contract-set-fixture\",\"contracts\":[{\"claim_requirement_hash\":\"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"contract_hash\":\"sha256:8fbdd2dbb63877b92004b7b5e6a7dc665a0ec5788850e4a466c0b6200639de2a\",\"contract_id\":\"connector.google.gmail.send_message@1\",\"credential_response_policy\":{\"kind\":\"forbidden\",\"sensitive_headers\":[],\"sensitive_json_pointers\":[]}},{\"claim_requirement_hash\":\"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"contract_hash\":\"sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999\",\"contract_id\":\"connector.google.sheets.append_row@1\",\"credential_response_policy\":{\"kind\":\"forbidden\",\"sensitive_headers\":[],\"sensitive_json_pointers\":[]}}],\"critical_fields\":[],\"deployment_id\":\"deployment-fixture\",\"extensions\":{},\"issuer\":\"operator-fixture\",\"key_id\":\"operator-authority-fixture\",\"org_id\":\"org-fixture\",\"schema_version\":\"0.2\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"operator-authority-fixture\",\"value\":\"SjqV-jvV1vRUR0XoBTfySXSfeMI7mXnPh4qwNOHQiUQUnGpVX3oMN7skullqE3ApcqyTrYeYOkupmFPEL3DyBQ\"}}";
const DEPLOYMENT_STANDING_AUTHORITY_JCS = "{\"connector_ref\":\"connector.google.workspace@1\",\"contract_set_hash\":\"sha256:6e1c48b18913e42a9cc017ec9b473906ad3ea3daa5b75c6f6dd118e73a93a5c5\",\"contract_set_ref\":\"contract-set-fixture\",\"critical_fields\":[],\"deployment_id\":\"deployment-fixture\",\"expires_at\":\"2030-01-01T00:00:00Z\",\"extensions\":{},\"issuer\":\"operator-fixture\",\"key_id\":\"operator-authority-fixture\",\"maximum_budgets\":{\"connection_logical_calls\":3,\"dispatch_attempts_per_call\":1,\"flow_logical_calls\":3,\"logical_calls\":3,\"node_logical_calls\":1},\"not_before\":\"2026-01-01T00:00:00Z\",\"operator_policy_hash\":\"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\",\"org_id\":\"org-fixture\",\"required_assurance_predicates\":[{\"kind\":\"brokered_count\",\"predicate_id\":\"brokered-count-v2\",\"required_kernel_controls\":[\"durable_before_dispatch\",\"exact_redelivery\",\"pop_bound\"]}],\"schema_version\":\"0.2\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"operator-authority-fixture\",\"value\":\"MWQaydDSoDkyVZTLiq-3OyRBRieUdI0SYj-CcTTW1kVrzooVWutT6twJrdxt-xv55lt7IRA9OFkH3_XxpK9UAQ\"},\"standing_authority_ref\":\"standing-fixture\"}";
const HISTORICAL_ARCHIVE_AUTHORITY_PUBLIC_KEY_B64U = "oJql9HpnWYAv-VX43C0qFKXJnSO-l_hkEn_5ODRVpPA";
const HISTORICAL_RECEIPT_KEY_ARCHIVE_JCS = "{\"algorithm\":\"ed25519\",\"archive_authority\":\"archive-root\",\"archive_key_id\":\"archive-root-fixture\",\"archive_ref\":\"receipt-v1-final\",\"critical_fields\":[],\"extensions\":{},\"issuer\":\"broker-production\",\"key_id\":\"broker-receipt-v1\",\"public_key_base64url\":\"0EqyMnQrtKs6E2i9RhXk5tAiSrcaAWuvhSCjMsl3hzc\",\"public_key_encoding\":\"raw_base64url\",\"revocation_evidence\":{\"evidence_authority\":\"archive-root\",\"evidence_key_id\":\"archive-root-fixture\",\"issuer\":\"broker-production\",\"key_id\":\"broker-receipt-v1\",\"observed_through\":\"2030-01-01T00:00:00Z\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"archive-root-fixture\",\"value\":\"2b0UzLlQMfTt8pFYfGiLYPJx-dw5gomhc1uQpPU2wTHEk5gvWV1gw1x-4T4wqodEOjzQBf5F6llw04cYNXFrAA\"},\"status\":\"not_revoked_through\"},\"schema_version\":\"0.2\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"archive-root-fixture\",\"value\":\"cvrrp3Ur955d4OOcRspB-LxvtWjIlCh_vOcPtLcwiLQP6lozVzel_SRaK7AgUpPcEzUbd8oVTr6TTqA2BXGUDw\"},\"valid_from\":\"2020-01-01T00:00:00Z\",\"valid_until\":\"2030-01-01T00:00:00Z\",\"validity_evidence\":{\"algorithm\":\"ed25519\",\"evidence_authority\":\"archive-root\",\"evidence_key_id\":\"archive-root-fixture\",\"issuer\":\"broker-production\",\"key_id\":\"broker-receipt-v1\",\"observed_at\":\"2026-07-21T00:00:00Z\",\"public_key_base64url\":\"0EqyMnQrtKs6E2i9RhXk5tAiSrcaAWuvhSCjMsl3hzc\",\"public_key_encoding\":\"raw_base64url\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"archive-root-fixture\",\"value\":\"GuC9QJGz9xZltiXnmKq0ReC_ZjJJOaCIBBm8E6yxk8MllApF__c9S_yOb4pPsb05K-k1bp--X7v61vwZvI82Bg\"},\"valid_from\":\"2020-01-01T00:00:00Z\",\"valid_until\":\"2030-01-01T00:00:00Z\"}}";
const LEGACY_INVENTORY_JCS = "{\"created_at\":\"2026-01-01T00:00:00Z\",\"critical_fields\":[],\"expires_at\":\"2030-01-01T00:00:00Z\",\"extensions\":{},\"inventory_ref\":\"legacy.production.inventory.final\",\"issuer\":\"archive-root\",\"items\":[{\"account_commitment\":{\"connection_ref\":\"legacy-restart-0\",\"context\":[\"broker-production\",\"legacy-restart-0\",\"account_commitment\"],\"issuer\":\"broker-production\",\"kind\":\"legacy_v0_1\",\"original_envelope\":{\"alg\":\"hmac-sha256\",\"key_id\":\"legacy\",\"value\":\"hmac-sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"}},\"actual_scopes_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"authority_manifest_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"binding_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"broker_issuer\":\"broker-production\",\"broker_key_id\":\"broker-receipt-v1\",\"broker_public_key_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"connection_ref\":\"legacy-restart-0\",\"contract_hashes\":[\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"],\"deployment_id\":\"deployment-fixture\",\"grants\":[],\"lane\":\"semantic_broker\",\"maximum_binding_expiry\":\"2029-01-01T00:00:00Z\",\"org_id\":\"org-fixture\",\"provider\":\"google\",\"required_scopes_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"revocation_epoch\":0,\"roles_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"},{\"account_commitment\":{\"connection_ref\":\"legacy-restart-1\",\"context\":[\"broker-production\",\"legacy-restart-1\",\"account_commitment\"],\"issuer\":\"broker-production\",\"kind\":\"legacy_v0_1\",\"original_envelope\":{\"alg\":\"hmac-sha256\",\"key_id\":\"legacy\",\"value\":\"hmac-sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"}},\"actual_scopes_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"authority_manifest_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"binding_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"broker_issuer\":\"broker-production\",\"broker_key_id\":\"broker-receipt-v1\",\"broker_public_key_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"connection_ref\":\"legacy-restart-1\",\"contract_hashes\":[\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"],\"deployment_id\":\"deployment-fixture\",\"grants\":[],\"lane\":\"semantic_broker\",\"maximum_binding_expiry\":\"2029-01-01T00:00:00Z\",\"org_id\":\"org-fixture\",\"provider\":\"google\",\"required_scopes_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"revocation_epoch\":0,\"roles_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"},{\"account_commitment\":{\"connection_ref\":\"legacy-restart-2\",\"context\":[\"broker-production\",\"legacy-restart-2\",\"account_commitment\"],\"issuer\":\"broker-production\",\"kind\":\"legacy_v0_1\",\"original_envelope\":{\"alg\":\"hmac-sha256\",\"key_id\":\"legacy\",\"value\":\"hmac-sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"}},\"actual_scopes_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"authority_manifest_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"binding_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"broker_issuer\":\"broker-production\",\"broker_key_id\":\"broker-receipt-v1\",\"broker_public_key_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"connection_ref\":\"legacy-restart-2\",\"contract_hashes\":[\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"],\"deployment_id\":\"deployment-fixture\",\"grants\":[],\"lane\":\"semantic_broker\",\"maximum_binding_expiry\":\"2029-01-01T00:00:00Z\",\"org_id\":\"org-fixture\",\"provider\":\"google\",\"required_scopes_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"revocation_epoch\":0,\"roles_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"}],\"key_id\":\"archive-root-fixture\",\"schema_version\":\"0.2\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"archive-root-fixture\",\"value\":\"aMk7Q7_cwK06Vpli_JEkYq9NwxmP2IBFBO0v9XmtdAlRJogNnl8zbsJXzF44GPIDGGY_xmyrue-6LMCQ5TlvAA\"}}";
const LEGACY_INVENTORY_DECISION_JCS = "{\"approval_epoch\":1,\"critical_fields\":[],\"decision_authority\":\"archive-root\",\"decision_key_id\":\"archive-root-fixture\",\"effective_at\":\"2020-01-01T00:00:00Z\",\"expires_at\":\"2030-01-01T00:00:00Z\",\"extensions\":{},\"inventory_definition\":{\"approval_epoch\":1,\"definition_hash\":\"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\",\"entry_ref\":\"legacy.production.inventory.final\",\"revocation_epoch\":0,\"version\":\"1\"},\"inventory_hash\":\"sha256:91a202f21048e40ff86c16b2b52ea1f6c34c762b6381e841523f7cec60b7114c\",\"inventory_ref\":\"legacy.production.inventory.final\",\"revocation_epoch\":0,\"schema_version\":\"0.2\",\"signature\":{\"alg\":\"Ed25519\",\"key_id\":\"archive-root-fixture\",\"value\":\"SsLbxSmUSIHuZNNNKV1b5voxzMF5AE5nioO-lK2d7LNkUMQN6EbcwNOFqA2vVkQ-0HiiIJG-AmJBwRw-4hRFCQ\"},\"status\":\"approved\"}";

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
        GOOGLE_EGRESS_SERVICE_AUTH: "local-egress-service-auth-not-production-123456",
        RECEIPT_SIGNING_SEED: "1111111111111111111111111111111111111111111111111111111111111111",
        COMMITMENT_KEY: "2222222222222222222222222222222222222222222222222222222222222222",
        BINDING_SIGNING_SEED: "4444444444444444444444444444444444444444444444444444444444444444",
        BROKER_WORKER_WASM_SHA256: `sha256:${"e".repeat(64)}`, AUTH_DRIVER_WORKER_SHA256: `sha256:${"d".repeat(64)}`, GOOGLE_TOKEN_WORKER_SHA256: `sha256:${"c".repeat(64)}`, GOOGLE_PROVIDER_WORKER_SHA256: `sha256:${"b".repeat(64)}`, OPERATOR_ARTIFACT_BUNDLE_SHA256, OPERATOR_BUNDLE_KEY_ID, OPERATOR_BUNDLE_PUBLIC_KEY_B64U,
        CUSTODY_ROOT_KEY: "3333333333333333333333333333333333333333333333333333333333333333",
        LOCAL_TEST_MODE: "true",
        PUBLIC_CALLBACK_BASE: "https://broker-public.example",
        OAUTH_REDIRECT_URI: "https://broker-public.example/v0.2/credential-callback",
        DEPLOYMENT_BOOTSTRAP_AUTH: "local-bootstrap-auth-not-production",
        DEPLOYMENT_AUTHORITY_PUBLIC_KEY_B64U, DEPLOYMENT_AUTHORITY_KEY_ID: "operator-authority-fixture",
        DEPLOYMENT_CONTRACT_SET_JCS, DEPLOYMENT_STANDING_AUTHORITY_JCS,
        HISTORICAL_ARCHIVE_AUTHORITY_KEY_ID: "archive-root-fixture", HISTORICAL_ARCHIVE_AUTHORITY_PUBLIC_KEY_B64U,
        LEGACY_CUTOVER_AUTHORITY_KEY_ID: "archive-root-fixture", LEGACY_CUTOVER_AUTHORITY_PUBLIC_KEY_B64U: HISTORICAL_ARCHIVE_AUTHORITY_PUBLIC_KEY_B64U,
        GENERIC_PROFILE_AUTHORITY_PUBLIC_KEY_B64U: GENERIC_PROFILE_PUBLIC_KEY_B64U,
        GENERIC_ACTIVATION_RECIPIENT_KEY_ID: "private-channel-fixture",
        GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U,
        GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U,
        ACTIVATION_SERVICE_AUTH: "private-activation-service-auth-fixture",
        AUTH_DRIVER_SERVICE_AUTH: "private-auth-driver-service-auth-fixture",
      },
      d1Databases: { BROKER_DB: "broker-test-db" },
      durableObjects: {
        BROKER_LEDGER_DO: { className: "BrokerLedgerDurableObject", useSQLite: true },
        CONNECTION_REFRESH_DO: { className: "ConnectionRefreshDurableObject", useSQLite: true },
        CREDENTIAL_STATE_V2_DO: { className: "CredentialStateDurableObject", useSQLite: true },
        V2_AUTHORITY_DO: { className: "V2AuthorityDurableObject", useSQLite: true },
      },
      serviceBindings: {
        GOOGLE_TOKEN_SERVICE: "mock-services",
        GOOGLE_PROVIDER_SERVICE: "mock-services",
        AUTH_DRIVER_SERVICE: "mock-services",
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
      GOOGLE_EGRESS_SERVICE_AUTH: "production-egress-service-auth-value-123456",
      RECEIPT_SIGNING_SEED: "1".repeat(64), COMMITMENT_KEY: "2".repeat(64),
      BINDING_SIGNING_SEED: "4".repeat(64), BROKER_WORKER_WASM_SHA256: `sha256:${"e".repeat(64)}`, AUTH_DRIVER_WORKER_SHA256: `sha256:${"d".repeat(64)}`, GOOGLE_TOKEN_WORKER_SHA256: `sha256:${"c".repeat(64)}`, GOOGLE_PROVIDER_WORKER_SHA256: `sha256:${"b".repeat(64)}`, OPERATOR_ARTIFACT_BUNDLE_SHA256, OPERATOR_BUNDLE_KEY_ID, OPERATOR_BUNDLE_PUBLIC_KEY_B64U, CUSTODY_ROOT_KEY: "3".repeat(64),
      PUBLIC_CALLBACK_BASE: "https://production.example",
      OAUTH_REDIRECT_URI: "https://production.example/v0.2/credential-callback",
      DEPLOYMENT_BOOTSTRAP_AUTH: "production-route-test-bootstrap",
      DEPLOYMENT_AUTHORITY_PUBLIC_KEY_B64U, DEPLOYMENT_AUTHORITY_KEY_ID: "operator-authority-fixture",
      DEPLOYMENT_CONTRACT_SET_JCS, DEPLOYMENT_STANDING_AUTHORITY_JCS,
        HISTORICAL_ARCHIVE_AUTHORITY_KEY_ID: "archive-root-fixture", HISTORICAL_ARCHIVE_AUTHORITY_PUBLIC_KEY_B64U,
        LEGACY_CUTOVER_AUTHORITY_KEY_ID: "archive-root-fixture", LEGACY_CUTOVER_AUTHORITY_PUBLIC_KEY_B64U: HISTORICAL_ARCHIVE_AUTHORITY_PUBLIC_KEY_B64U,
        GENERIC_PROFILE_AUTHORITY_PUBLIC_KEY_B64U: GENERIC_PROFILE_PUBLIC_KEY_B64U,
        GENERIC_ACTIVATION_RECIPIENT_KEY_ID: "private-channel-fixture",
        GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U,
        GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U,
        ACTIVATION_SERVICE_AUTH: "private-activation-service-auth-fixture",
        AUTH_DRIVER_SERVICE_AUTH: "private-auth-driver-service-auth-fixture",
    },
    d1Databases: { BROKER_DB: "broker-production-route-db" },
    durableObjects: {
      CONNECTION_REFRESH_DO: { className: "ConnectionRefreshDurableObject", useSQLite: true },
      CREDENTIAL_STATE_V2_DO: { className: "CredentialStateDurableObject", useSQLite: true },
      V2_AUTHORITY_DO: { className: "V2AuthorityDurableObject", useSQLite: true },
    },
    serviceBindings: {
      GOOGLE_TOKEN_SERVICE: "production-mock-services",
      GOOGLE_PROVIDER_SERVICE: "production-mock-services",
      AUTH_DRIVER_SERVICE: "production-auth-driver",
    },
  }, {
    name: "production-mock-services",
    scriptPath: "src/mock-provider.mjs",
    compatibilityDate: "2026-07-15",
    modules: true,
  }, {
    name: "production-auth-driver",
    script: AUTH_DRIVER_SCRIPT,
    compatibilityDate: "2026-07-15",
    modules: true,
    outboundService: "production-mock-services",
    bindings: { AUTH_DRIVER_SERVICE_AUTH: "private-auth-driver-service-auth-fixture" },
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

function sealV2Submission(activationRef: string, jti: string, value: unknown) {
  const key = Buffer.from("33".repeat(32), "hex");
  const nonce = createHash("sha256").update(`${activationRef}\0${jti}`).digest().subarray(0, 12);
  const cipher = createCipheriv("chacha20-poly1305", key, nonce, { authTagLength: 16 });
  cipher.setAAD(Buffer.from(`${activationRef}\0${jti}\0private-material-v2`));
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(value)), cipher.final(), cipher.getAuthTag()]);
  return { nonce_b64u: nonce.toString("base64url"), ciphertext_b64u: ciphertext.toString("base64url") };
}

async function v2(body: unknown) {
  const response = await jsonRequest(privateWorker, "/__test/v2-activation", body);
  const text = await response.text();
  let parsed: any = {};
  try { parsed = text.length === 0 ? {} : JSON.parse(text); } catch { parsed = { raw: text }; }
  return { response, body: parsed };
}

beforeAll(async () => {
  publicWorker = await mf.getWorker("broker-public");
  privateWorker = await mf.getWorker("broker-private");
  for (const [runtime, workerName, pepper] of [
    [mf, "broker-private", "local-pepper-not-a-production-secret"],
    [productionMf, "broker-production", "production-route-test-pepper"],
  ] as const) {
    const db = await runtime.getD1Database("BROKER_DB", workerName);
    for (const name of ["0001_broker.sql", "0002_credential_plane_v2.sql", "0003_production_v2_cutover.sql", "0004_operator_artifact_bundle.sql"]) {
      const migration = await readFile(`../migrations/${name}`, "utf8");
      for (const statement of migration.split(";").map((value) => value.trim()).filter(Boolean)) {
        await db.prepare(statement).run();
      }
    }
    await db.prepare("INSERT INTO operator_artifact_bundles(deployment_id,bundle_hash,canonical_bundle_jcs,seeded_at) VALUES(?,?,?,?)")
      .bind("deployment-fixture", OPERATOR_ARTIFACT_BUNDLE_SHA256, Buffer.from(OPERATOR_ARTIFACT_BUNDLE_JCS), Math.floor(Date.now() / 1000)).run();
    const keyHash = createHmac("sha256", pepper)
      .update("deployment-key").update(Buffer.from([0])).update(fixtureKey).digest("hex");
    await db.prepare("INSERT INTO deployment_keys(org_id,deployment_id,key_hash,expires_at,revoked) VALUES(?,?,?,?,0)")
      .bind("org-fixture", "deployment-fixture", keyHash, Math.floor(Date.now() / 1000) + 3600).run();
  }
});

afterAll(async () => {
  await mf.dispose();
  await productionMf.dispose();
});

async function sessionOn(worker: Fetcher, pepper: string, key = fixtureKey) {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  const jwk = publicKey.export({ format: "jwk" });
  if (jwk.x === undefined) throw new Error("missing Ed25519 public key");
  const timestamp = Math.floor(Date.now() / 1000);
  const nonce = `exchange-${randomBytes(24).toString("base64url")}`;
  const keyHash = createHmac("sha256", pepper)
    .update("deployment-key").update(Buffer.from([0])).update(key).digest("hex");
  const audience = "lattice-broker-session";
  const deploymentKeyId = createHash("sha256").update("deployment-key-id\0").update(key).digest("hex");
  const transcript = framed("lattice.session-exchange.ed25519.v1", [
    `sha256:${deploymentKeyId}`, jwk.x, nonce, String(timestamp), audience,
  ]);
  const response = await jsonRequest(worker, "/v0.2/sessions", {
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

async function session(key = fixtureKey) {
  return sessionOn(publicWorker, "local-pepper-not-a-production-secret", key);
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

function hpkeExtract(salt: Buffer, ikm: Buffer) { return createHmac("sha256", salt).update(ikm).digest(); }
function hpkeExpand(prk: Buffer, info: Buffer, length: number) { let out=Buffer.alloc(0),previous=Buffer.alloc(0); for(let i=1;out.length<length;i++){previous=createHmac("sha256",prk).update(Buffer.concat([previous,info,Buffer.from([i])])).digest();out=Buffer.concat([out,previous]);}return out.subarray(0,length); }
function hpkeLabeledExtract(salt:Buffer,suite:Buffer,label:string,ikm:Buffer){return hpkeExtract(salt,Buffer.concat([Buffer.from("HPKE-v1"),suite,Buffer.from(label),ikm]));}
function hpkeLabeledExpand(prk:Buffer,suite:Buffer,label:string,info:Buffer,length:number){return hpkeExpand(prk,Buffer.concat([Buffer.from([length>>8,length&255]),Buffer.from("HPKE-v1"),suite,Buffer.from(label),info]),length);}
function sealGenericActivation(action:any, plaintext:any, requestJti:string, overrides:any={}) {
  const ephemeral=generateKeyPairSync("x25519");
  const ephemeralPublic=(ephemeral.publicKey.export({format:"jwk"}) as JsonWebKey).x!;
  const recipient=createPublicKey({key:{kty:"OKP",crv:"X25519",x:overrides.publicKey??action.recipient_public_key_b64u},format:"jwk"});
  const dh=diffieHellman({privateKey:ephemeral.privateKey,publicKey:recipient});
  const enc=Buffer.from(ephemeralPublic,"base64url"),recipientRaw=Buffer.from(overrides.publicKey??action.recipient_public_key_b64u,"base64url");
  const kemSuite=Buffer.concat([Buffer.from("KEM"),Buffer.from([0,0x20])]);
  const eae=hpkeLabeledExtract(Buffer.alloc(0),kemSuite,"eae_prk",dh);
  const shared=hpkeLabeledExpand(eae,kemSuite,"shared_secret",Buffer.concat([enc,recipientRaw]),32);
  const suite=Buffer.concat([Buffer.from("HPKE"),Buffer.from([0,0x20,0,1,0,2])]);
  const psk=hpkeLabeledExtract(Buffer.alloc(0),suite,"psk_id_hash",Buffer.alloc(0));
  const info=hpkeLabeledExtract(Buffer.alloc(0),suite,"info_hash",Buffer.from("lattice.generic-activation-envelope.v0.2"));
  const context=Buffer.concat([Buffer.from([0]),psk,info]);
  const secret=hpkeLabeledExtract(shared,suite,"secret",Buffer.alloc(0));
  const key=hpkeLabeledExpand(secret,suite,"key",context,32),nonce=hpkeLabeledExpand(secret,suite,"base_nonce",context,12);
  const issuedAt=overrides.issuedAt??Math.floor(Date.now()/1000),expiresAt=overrides.expiresAt??action.expires_at;
  const correlation=plaintext.channel_ref??plaintext.challenge;
  const aad=canonicalJson({activation_ref:action.activation_ref,correlation_hash:`sha256:${createHash("sha256").update(correlation).digest("hex")}`,deployment_id:overrides.deploymentId??"deployment-fixture",domain:"lattice.generic-activation.hpke.v1",expires_at:expiresAt,issued_at:issuedAt,org_id:"org-fixture",recipient_key_id:overrides.keyId??action.recipient_key_id,request_jti:requestJti,service_identity:overrides.serviceIdentity??"lattice-broker-private.generic-activation"});
  const cipher=createCipheriv("aes-256-gcm",key,nonce);cipher.setAAD(Buffer.from(aad));const ciphertext=Buffer.concat([cipher.update(canonicalJson(plaintext)),cipher.final(),cipher.getAuthTag()]);
  return {request_jti:requestJti,issued_at:issuedAt,expires_at:expiresAt,recipient_key_id:overrides.keyId??action.recipient_key_id,encapsulated_key_b64u:enc.toString("base64url"),ciphertext_b64u:ciphertext.toString("base64url")};
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

describe("production route exclusion", () => {
  it("keeps fixture code absent from the production WASM route table", async () => {
    const production = await productionMf.getWorker("broker-production");
    const response = await production.fetch("http://broker/__test/fixtures", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ fixture: "google-semantic-broker-v1" }),
    });
    expect(response.status).toBe(404);
    const credentialState = await production.fetch(
      "http://broker/__test/credential-state-v2",
      { method: "POST", headers: { "content-type": "application/json" }, body: "{}" },
    );
    expect(credentialState.status).toBe(404);
    const activation = await production.fetch("http://broker/__test/v2-activation", {
      method: "POST", headers: { "content-type": "application/json" }, body: "{}",
    });
    expect(activation.status).toBe(404);
    for (const legacy of [
      ["POST", "/v1/sessions"],
      ["POST", "/internal/v1/grants"],
      ["POST", "/internal/v1/invoke"],
      ["GET", "/v1/receipts/historical"],
    ]) {
      expect((await production.fetch(`http://broker${legacy[1]}`, {
        method: legacy[0],
        headers: { "content-type": "application/json" },
        body: legacy[0] === "GET" ? undefined : "{}",
      })).status).toBe(404);
    }
  });

});

describe.skip("archived V1 packet regression fixtures", () => {
  it("seals and reads back V2 prepared state behind the fixture-only route", async () => {
    const fence = {
      schema_version: "0.2", critical_fields: [], extensions: {}, phase: "v1_authoritative",
      fence_generation: 0, v2_lease_ever_issued: false, v2_rotation_ever_started: false,
      v1_leasing_disabled: false, active_v2_generation: null, cas_version: 0,
    };
    const base = { org_id: "org-v2-fixture", connection_ref: "connection-v2-fixture" };
    const initialized = await jsonRequest(privateWorker, "/__test/credential-state-v2", {
      ...base,
      command: { op: "initialize", fence_json: [...Buffer.from(JSON.stringify(fence))] },
    });
    expect(initialized.status).toBe(200);
    const sealed = await jsonRequest(privateWorker, "/__test/credential-state-v2", {
      ...base,
      command: { op: "seal_material", generation: 1, sealed_envelope: [...Buffer.from("sealed-v2-fixture")] },
    });
    expect(sealed.status).toBe(200);
    const sealedBody = await sealed.json() as any;
    expect(sealedBody.material_generations).toHaveLength(1);
    expect(JSON.stringify(sealedBody)).not.toContain("sealed-v2-fixture");
    const readback = await jsonRequest(privateWorker, "/__test/credential-state-v2", {
      ...base, command: { op: "read" },
    });
    expect(readback.status).toBe(200);
    expect((await readback.json() as any).material_generations).toEqual(sealedBody.material_generations);

    const prepared = { ...fence, phase: "v2_prepared", fence_generation: 1, cas_version: 1 };
    expect((await jsonRequest(privateWorker, "/__test/credential-state-v2", {
      ...base, command: { op: "advance_fence", canonical_json: [...Buffer.from(JSON.stringify(prepared))] },
    })).status).toBe(200);
    const authoritative = {
      ...prepared, phase: "v2_authoritative", fence_generation: 2, cas_version: 2,
      v2_lease_ever_issued: true, v2_rotation_ever_started: true,
      v1_leasing_disabled: true, active_v2_generation: 1,
    };
    expect((await jsonRequest(privateWorker, "/__test/credential-state-v2", {
      ...base, command: { op: "activate_v2_for_test", canonical_json: [...Buffer.from(JSON.stringify(authoritative))] },
    })).status).toBe(200);
    const rollback = { ...fence, fence_generation: 3, cas_version: 3 };
    expect((await jsonRequest(privateWorker, "/__test/credential-state-v2", {
      ...base, command: { op: "advance_fence", canonical_json: [...Buffer.from(JSON.stringify(rollback))] },
    })).status).toBe(409);
  });

  it("persists and dispatches all generic V2 activation schemes behind the test fence", async () => {
    let sequence = 0;
    const externalPrivateKeys = new Map<string, any>();
    const activate = async (profile: any, material?: unknown) => {
      sequence += 1;
      const org_id = "org-v2-generic";
      const activation_ref = `activation-v2-${sequence}`;
      expect((await v2({ op: "install_profile", org_id, profile, standing_authority_ref: `standing-${sequence}`, trusted_source_refs: ["source.trusted"] })).response.status).toBe(201);
      const created = await v2({ op: "create", org_id, activation_ref, profile_ref: profile.profile_ref, request_jti: `create-${sequence}` });
      expect(created.response.status).toBe(201);
      if (profile.activation_kind === "oauth") {
        const crashed = await v2({ op: "oauth_callback", org_id, activation_ref, state: created.body.action_nonce, code: "synthetic-code", crash_phase: "after_claim" });
        expect(crashed.response.status).toBe(599);
        expect((await v2({ op: "oauth_callback", org_id, activation_ref, state: created.body.action_nonce, code: "synthetic-code" })).body.kind).toBe("complete");
      } else if (profile.activation_kind === "external") {
        const privateKey = externalPrivateKeys.get(profile.profile_ref);
        const challengeHash = `sha256:${createHash("sha256").update(created.body.action_nonce).digest("hex")}`;
        const signature = sign(null, Buffer.from(challengeHash), privateKey).toString("base64url");
        expect((await v2({ op: "external_bind", org_id, activation_ref, public_key_b64u: profile.external_public_key_b64u, signature_b64u: signature })).body.kind).toBe("complete");
      } else {
        const submitted = profile.activation_kind === "workload"
          ? { iss: profile.workload_issuer, aud: profile.workload_audience, nonce: created.body.action_nonce }
          : material;
        const sealed = sealV2Submission(activation_ref, `submission-${sequence}`, submitted);
        expect((await v2({ op: "submit_private", org_id, activation_ref, submission_jti: `submission-${sequence}`, ...sealed })).body.kind).toBe("complete");
        expect((await v2({ op: "submit_private", org_id, activation_ref, submission_jti: `submission-${sequence}`, ...sealed })).response.status).toBe(409);
      }
      expect((await v2({ op: "activate_fence", org_id, activation_ref })).response.status).toBe(200);
      const dispatched = await v2({ op: "dispatch", org_id, activation_ref, request_ref: `dispatch-${sequence}`, response: { ok: true } });
      expect(dispatched.response.status).toBe(200);
      expect(JSON.stringify(dispatched.body)).not.toMatch(/synthetic-code|static-secret|password-value|workload-token|custodian-reference/);
      return { org_id, activation_ref, created, dispatched };
    };

    const oauth = await activate({ profile_ref: "auth.synthetic.oauth", activation_kind: "oauth", scheme_kind: "oauth", endpoint: "https://synthetic.invalid/api" });
    expect(oauth.dispatched.body.auth_evidence.header_names).toContain("Authorization");
    const staticCases = [
      [{ profile_ref: "auth.synthetic.header", activation_kind: "secret", scheme_kind: "header", endpoint: "https://synthetic.invalid/api", placement_name: "x-api-key", prefix: "Key" }, { secret: "static-secret" }, "x-api-key", "header_names"],
      [{ profile_ref: "auth.synthetic.query", activation_kind: "secret", scheme_kind: "query", endpoint: "https://synthetic.invalid/api", placement_name: "api_key" }, { secret: "static-secret" }, "api_key", "query_names"],
      [{ profile_ref: "auth.synthetic.basic", activation_kind: "secret", scheme_kind: "basic", endpoint: "https://synthetic.invalid/api" }, { username: "user", password: "password-value" }, "Authorization", "header_names"],
      [{ profile_ref: "auth.synthetic.bearer", activation_kind: "secret", scheme_kind: "bearer", endpoint: "https://synthetic.invalid/api" }, { access_token: "static-secret" }, "Authorization", "header_names"],
    ] as const;
    let rotating: any;
    for (const [profile, material, placement, list] of staticCases) {
      const active = await activate(profile, material);
      expect(active.dispatched.body.auth_evidence[list]).toContain(placement);
      rotating ??= active;
    }
    const workload = await activate({ profile_ref: "auth.synthetic.workload", activation_kind: "workload", scheme_kind: "workload", endpoint: "https://synthetic.invalid/api", workload_issuer: "https://issuer.invalid", workload_audience: "lattice" });
    expect(workload.dispatched.body.auth_evidence.header_names).toContain("Authorization");
    const externalKeys = generateKeyPairSync("ed25519");
    const externalJwk = externalKeys.publicKey.export({ format: "jwk" }) as JsonWebKey;
    externalPrivateKeys.set("auth.synthetic.external", externalKeys.privateKey);
    const external = await activate({ profile_ref: "auth.synthetic.external", activation_kind: "external", scheme_kind: "external", endpoint: "https://synthetic.invalid/api", external_public_key_b64u: externalJwk.x });
    expect(external.dispatched.body.auth_evidence.header_names).toContain("Authorization");

    const read = await v2({ op: "read", org_id: rotating.org_id, activation_ref: rotating.activation_ref });
    const rotationJti = "rotation-jti";
    const rotated = sealV2Submission(rotating.activation_ref, rotationJti, { secret: "rotated-secret" });
    expect((await v2({ op: "rotate", org_id: rotating.org_id, activation_ref: rotating.activation_ref, expected_cas: read.body.cas_version, submission_jti: rotationJti, ...rotated, crash_phase: "before_seal" })).response.status).toBe(599);
    const completed = await v2({ op: "rotate", org_id: rotating.org_id, activation_ref: rotating.activation_ref, expected_cas: read.body.cas_version, submission_jti: rotationJti, ...rotated });
    expect(completed.body.material_generation).toBe(2);
    expect((await v2({ op: "rotate", org_id: rotating.org_id, activation_ref: rotating.activation_ref, expected_cas: read.body.cas_version, submission_jti: "stale", ...rotated })).response.status).toBe(409);

    const ambiguous = await v2({ op: "dispatch", org_id: rotating.org_id, activation_ref: rotating.activation_ref, request_ref: "dispatch-ambiguous", response: { ok: true }, crash_phase: "after_dispatch" });
    expect(ambiguous.response.status).toBe(599);
    expect((await v2({ op: "dispatch", org_id: rotating.org_id, activation_ref: rotating.activation_ref, request_ref: "dispatch-ambiguous", response: { ok: true } })).body.outcome).toBe("ambiguous");
    const smuggled = await v2({ op: "dispatch", org_id: rotating.org_id, activation_ref: rotating.activation_ref, request_ref: "dispatch-smuggled", response: { access_token: "must-not-project" } });
    expect(smuggled.response.status).toBe(409);
  });

  it("fails trusted semantic policy references closed while retaining source records", async () => {
    const org_id = "org-v2-policy";
    const profile = { profile_ref: "auth.synthetic.policy", activation_kind: "secret", scheme_kind: "bearer", endpoint: "https://synthetic.invalid/api" };
    expect((await v2({ op: "install_profile", org_id, profile, standing_authority_ref: "standing-policy", trusted_source_refs: ["source.trusted"], policy_refs: ["policy.unsupported"] })).response.status).toBe(201);
    expect((await v2({ op: "create", org_id, activation_ref: "activation-policy", profile_ref: profile.profile_ref, request_jti: "policy-jti" })).response.status).toBe(409);
  });

  it("keeps fixture and invoke routes off the public facade", async () => {
    const fixture = await jsonRequest(publicWorker, "/__test/fixtures", {
      fixture: "google-semantic-broker-v1",
    });
    expect(fixture.status).toBe(404);
    expect((await jsonRequest(publicWorker, "/__test/v2-activation", {})).status).toBe(404);
    const invoke = await jsonRequest(publicWorker, "/internal/v0.2/invoke", {});
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
        "/v0.2/connection-intents",
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
      "/v0.2/connection-intents",
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
    expect(callback.origin).toBe("https://accounts.google.com");
    expect(callback.pathname).toBe("/o/oauth2/v2/auth");
    expect(callback.searchParams.get("client_id")).toBe("test-client-id-12345");
    expect(callback.searchParams.get("redirect_uri")).toBe("https://broker-public.example/v0.2/credential-callback");
    expect(callback.searchParams.get("code_challenge_method")).toBe("S256");
    expect(callback.searchParams.get("code_challenge")).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(callback.searchParams.get("scope")?.split(" ").sort()).toEqual([
      "https://www.googleapis.com/auth/gmail.send",
      "https://www.googleapis.com/auth/spreadsheets",
      "openid",
    ]);
    const state = callback.searchParams.get("state")!;
    const callbackRequest = new URL("http://broker/v0.2/credential-callback");
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
      "/v0.2/bindings",
      { ...validBinding, authority_manifest_json: canonicalJson(tamperedManifest) },
      sessionHeaders(auth.session_ref),
    );
    expect(tampered.status).toBe(400);
    const wrongHash = await jsonRequest(
      publicWorker,
      "/v0.2/bindings",
      { ...validBinding, flow_ir_hash: `sha256:${"9".repeat(64)}` },
      sessionHeaders(auth.session_ref),
    );
    expect(wrongHash.status).toBe(400);
    const bindingResponse = await jsonRequest(
      publicWorker,
      "/v0.2/bindings",
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
        "/internal/v0.2/grants",
        { ...validGrant, ...mutation },
        { "x-lattice-service-auth": "local-service-auth-not-production" },
      );
      expect([400, 403]).toContain(rejected.status);
    }
    const grant = await jsonRequest(
      privateWorker,
      "/internal/v0.2/grants",
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
      "/internal/v0.2/grants",
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
        "/v0.2/connection-intents",
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
      const callback = new URL("http://broker/v0.2/credential-callback");
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
      const response = await jsonRequest(publicWorker, "/v0.2/connection-intents", {
        connector_ref: "connector.google.workspace@1",
        auth_profile_ref: "auth.google.workspace.oauth2@1",
        execution_lane: "semantic_broker",
        custody: "hosted_broker",
      }, sessionHeaders(auth.session_ref));
      const intent = await response.json() as any;
      const authorization = new URL(intent.next_action.url);
      const callback = new URL("http://broker/v0.2/credential-callback");
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
        "/v0.2/connection-intents",
        intentBody,
        sessionHeaders(auth.session_ref),
      );
      expect(response.status).toBe(201);
      const intent = await response.json() as any;
      const authorization = new URL(intent.next_action.url);
      const callback = new URL("http://broker/v0.2/credential-callback");
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
    const headers = requestHeaders(body.session_ref, "POST", "/v0.2/connection-intents", requestBody);
    headers["x-lattice-pop-signature"] = Buffer.alloc(64, 9).toString("base64url");
    const response = await publicWorker.fetch("http://broker/v0.2/connection-intents", {
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
    const path = "/v0.2/connection-intents";
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
    expect((await send("/v0.2/bindings", "POST", payload, pathHeaders)).status).toBe(401);
    const methodHeaders = requestHeaders(auth.session_ref, "POST", "/v0.2/connections/missing", Buffer.alloc(0));
    expect((await send("/v0.2/connections/missing", "GET", Buffer.alloc(0), methodHeaders)).status).toBe(401);
    const stale = requestHeaders(auth.session_ref, "POST", path, payload, {
      timestamp: Math.floor(Date.now() / 1000) - 61,
    });
    expect((await send(path, "POST", payload, stale)).status).toBe(401);
    const rawQuery = "/v0.2/connections/missing?a=1+2&a=%2f";
    const rawQueryHeaders = requestHeaders(auth.session_ref, "GET", rawQuery, Buffer.alloc(0));
    expect((await send("/v0.2/connections/missing?a=1+2&a=%2F", "GET", Buffer.alloc(0), rawQueryHeaders)).status).toBe(401);
    const deletePath = "/v0.2/connections/missing";
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
    const first = await jsonRequest(privateWorker, "/internal/v0.2/invoke", invocation, headers);
    const firstBody = await first.json() as any;
    expect(first.status, JSON.stringify(firstBody)).toBe(200);
    expect(firstBody.redelivery).toBe(false);
    expect(firstBody.receipt.contract_hash).toBe(
      "sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999",
    );
    expect(firstBody.receipt.claims.provider_dispatch_observed).toBe(true);
    const second = await jsonRequest(privateWorker, "/internal/v0.2/invoke", invocation, headers);
    expect(second.status).toBe(200);
    const secondBody = await second.json() as any;
    expect(secondBody.redelivery).toBe(true);
    expect(secondBody.receipt).toEqual(firstBody.receipt);

    const mockServices = await mf.getWorker("mock-services");
    await mockServices.fetch("http://mock/__oversize-next");
    const oversizedEffect = effectId("run-fixture", "node-sheets", 2n, "append_row");
    const oversized = await jsonRequest(
      privateWorker,
      "/internal/v0.2/invoke",
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
      "/internal/v0.2/invoke",
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
      "/internal/v0.2/invoke",
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
      `/v0.2/receipts/${row!.receipt_ref}`,
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
      `/v0.2/receipts/${row!.receipt_ref}`,
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
      "/internal/v0.2/invoke",
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
    const publicAttempt = await jsonRequest(publicWorker, "/internal/v0.2/bootstrap", body, {
      "x-lattice-bootstrap-auth": "local-bootstrap-auth-not-production",
    });
    expect(publicAttempt.status).toBe(404);
    const first = await jsonRequest(privateWorker, "/internal/v0.2/bootstrap", body, {
      "x-lattice-bootstrap-auth": "local-bootstrap-auth-not-production",
    });
    expect(first.status).toBe(201);
    const firstBody = await first.json() as any;
    expect(firstBody.deployment_key).toMatch(/^lbk_[A-Za-z0-9_-]{64}$/);
    const stored = await db.prepare("SELECT key_hash FROM deployment_keys LIMIT 1").first<{ key_hash: string }>();
    expect(stored?.key_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(JSON.stringify(stored)).not.toContain(firstBody.deployment_key);
    const repeat = await jsonRequest(privateWorker, "/internal/v0.2/bootstrap", body, {
      "x-lattice-bootstrap-auth": "local-bootstrap-auth-not-production",
    });
    expect(repeat.status).toBe(409);
  });
});

function v2FlowAuthorityFixture() {
  const sheets = {
    alias: "sheets", nodeId: "node-sheets", slot: "append_row",
    contract: "connector.google.sheets.append_row@1",
    contractHash: "sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999",
    operationId: "connector.google.sheets.append_row", connectorId: "connector.google.sheets",
  };
  const gmail = {
    alias: "gmail", nodeId: "node-gmail", slot: "send_message",
    contract: "connector.google.gmail.send_message@1",
    contractHash: "sha256:8fbdd2dbb63877b92004b7b5e6a7dc665a0ec5788850e4a466c0b6200639de2a",
    operationId: "connector.google.gmail.send_message", connectorId: "connector.google.gmail",
  };
  const nodes = [sheets, gmail].map((node) => ({
    id: node.nodeId, alias: node.alias, identifier: `fixture::${node.alias}`, name: node.alias,
    kind: "activity", in_schema: { kind: "opaque" }, out_schema: { kind: "opaque" },
    effects: "effectful", determinism: "best_effort", idempotency: {}, determinismHints: [], effectHints: [],
    connectorOps: [{ operation_id: node.operationId, connector_id: node.connectorId, roles: [],
      default_resolution_mode: "bound_connection", selected_resolution_mode: "bound_connection",
      supported_resolution_modes: ["bound_connection"] }],
    broker_authority: { operation_budgets: [{ contract_id: node.contract, semantic_effect_slots: [node.slot],
      max_logical_calls: 1, max_dispatch_attempts_per_call: 1, connection_aggregate_key: "workspace" }],
      flow_aggregate_max_logical_calls: 2, connection_aggregate_max_logical_calls: { workspace: 2 } },
  }));
  const flow = { id: "flow-v2", name: "flow-v2", version: "1.0.0", profile: "dev", nodes,
    edges: [], control_surfaces: [], checkpoints: [], policies: { lint: { require_control_hints: false } },
    metadata: { tags: [] }, artifacts: [] };
  const flowIrJson = canonicalJson(flow);
  const flowIrHash = `sha256:${createHash("sha256").update(flowIrJson).digest("hex")}`;
  const manifestNodes = Object.fromEntries([sheets, gmail].map((node) => [node.alias, {
    node_id: node.nodeId,
    operations: [{ contract_id: node.contract, contract_hash: node.contractHash,
      call_budget: { max_logical_calls: 1, max_dispatch_attempts_per_call: 1 },
      minimum_assurance: "brokered_count", required_attenuations: [], connection_aggregate_key: "workspace" }],
  }]));
  const manifest = { schema_version: "0.1", critical_fields: [], org_id: "org-fixture",
    principal: { kind: "deployment", id: "deployment-fixture" }, flow_ir_hash: flowIrHash,
    nodes: manifestNodes, aggregate_ceilings: { flow: { max_logical_calls: 2 },
      connections: { workspace: { max_logical_calls: 2 } } } };
  return { sheets, gmail, flowIrJson, flowIrHash, authorityManifestJson: canonicalJson(manifest) };
}

function genericDescriptor(name: string, kind: "header" | "query" | "basic" | "bearer" | "workload" | "external") {
  const h = `sha256:${"a".repeat(64)}`;
  const pin = { entry_ref: "auth-driver.fixture", version: "1", definition_hash: h, approval_epoch: 1, revocation_epoch: 0 };
  const secret = kind === "header"
    ? { kind: "api_key_header", header_name: "X-API-Key", prefix: "", redact_authenticated_header: true, secret_schema_hash: h }
    : kind === "query"
      ? { kind: "api_key_query", query_name: "api_key", encoding: "rfc3986", redact_authenticated_query: true, secret_schema_hash: h }
      : kind === "basic"
        ? { kind: "http_basic", username_schema_hash: h, password_schema_hash: h, charset: "UTF-8", colon_rule: "username_forbids_colon", header_name: "Authorization" }
        : { kind: "generic_bearer", header_name: "Authorization", prefix: "Bearer", whitespace_rule: "single_space", secret_schema_hash: h };
  const activation = kind === "workload"
    ? { kind: "workload_binding", assertion_schema_hash: h, maximum_assertion_age_seconds: 600 }
    : kind === "external"
      ? { kind: "external_custodian_binding", proof_method: "private_service_binding" }
      : { kind: "secret_submission", submission_schema_hash: h };
  const scheme = kind === "workload" ? {
    kind: "oauth_token_exchange_workload_oidc", trusted_issuer_set_ref: "issuers.fixture", trusted_issuer_set_hash: h,
    audience: "lattice-workload", subject_token_type: "urn:ietf:params:oauth:token-type:id_token",
    requested_token_type: "urn:ietf:params:oauth:token-type:access_token", exchange_endpoint_key: "exchange",
    maximum_assertion_age_seconds: 600, nonce_required: true, proof_method: "private_submission",
    token_endpoint_auth_method: "none", claim_mapping_schema_hash: h, response_firewall_policy_hash: h,
  } : kind === "external" ? {
    kind: "external_custodian_reference", allowed_custodians: [pin], allowed_transports: [pin], codec: "remote_custody_v1",
    service_identity_policy_hash: h, destruction_evidence_kind: "signed_remote_proof",
  } : secret;
  return { schema_version: "0.2", critical_fields: [], extensions: {}, profile_ref: `auth.fixture.${name}`,
    version: "1", scheme_ref: `credential.fixture.${kind}@1`, activation_kind: activation, scheme_config: scheme,
    public_config_schema_hash: h, authorization_claims_schema_hash: h, public_claims_projection_policy: { kind: "none" },
    trusted_auth_driver: pin, claim_normalizer: { ...pin, entry_ref: "normalizer.fixture" }, endpoint_policy_schema_hash: h,
    lifecycle_capabilities: kind === "workload" ? ["activate", "destroy", "exchange", "workload_bind"] : ["activate", "destroy"] };
}
async function exerciseGenericConnection(worker: Fetcher, sessionRef: string, connectionRef: string, suffix: string) {
  const authority = v2FlowAuthorityFixture();
  const lock = `sha256:${createHash("sha256").update(`generic-lock-${suffix}`).digest("hex")}`;
  const bundle = `bundle-generic-${suffix}`;
  const bindingResponse = await jsonRequest(worker, "/v0.2/bindings", { connection_ref: connectionRef, deployment_id: "deployment-fixture", bundle_id: bundle, flow_ir_hash: authority.flowIrHash, binding_lock_hash: lock, flow_id: "flow-v2", flow_ir_json: authority.flowIrJson, authority_manifest_json: authority.authorityManifestJson, contracts: [authority.gmail.contract, authority.sheets.contract] }, sessionHeaders(sessionRef));
  const bindingText = await bindingResponse.text(); expect(bindingResponse.status, bindingText).toBe(201); const binding = JSON.parse(bindingText);
  const common = { session_ref: sessionRef, binding_ref: binding.binding_ref, bundle_id: bundle, flow_ir_hash: authority.flowIrHash, binding_lock_hash: lock, flow_id: "flow-v2", run_id: `run-${suffix}`, node_id: authority.sheets.nodeId, node_alias: authority.sheets.alias, activation_ordinal: 1 };
  const leaseResponse = await jsonRequest(worker, "/internal/v0.2/node-leases", { ...common, operation_contract: authority.sheets.contract }, { ...sessionHeaders(sessionRef), "x-lattice-service-auth": "production-route-test-service-auth" });
  const leaseText = await leaseResponse.text(); expect(leaseResponse.status, leaseText).toBe(201); const lease = JSON.parse(leaseText);
  const input = { spreadsheet_id: "sheet-1", sheet: "Sheet1", row: { column: suffix }, value_input_option: "raw" };
  const grantResponse = await jsonRequest(worker, "/internal/v0.2/grants", { ...common, node_lease_ref: lease.node_lease_ref, semantic_effect_slot: authority.sheets.slot, expected_cas_version: 0, input }, { ...sessionHeaders(sessionRef), "x-lattice-service-auth": "production-route-test-service-auth" });
  const grantText = await grantResponse.text(); expect(grantResponse.status, grantText).toBe(201); const grant = JSON.parse(grantText);
  const stateRoute = `sha256:${createHash("sha256").update(`lattice.credential-state.v2\0org-fixture\0${connectionRef}`).digest("hex")}`;
  const stateNs = await productionMf.getDurableObjectNamespace("CREDENTIAL_STATE_V2_DO", "broker-production");
  const leaseProbe = await stateNs.get(stateNs.idFromName(stateRoute)).fetch("http://state/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ org_id: "org-fixture", connection_ref: connectionRef, command: { op: "lease_material_for_dispatch", generation: 1 } }) });
  expect(leaseProbe.status, `${suffix} state: ${await leaseProbe.clone().text()}`).toBe(suffix === "external" ? 409 : 200);
  const invoke = await jsonRequest(worker, "/internal/v0.2/invoke", { session_ref: sessionRef, grant_ref: grant.grant_ref, input }, { ...sessionHeaders(sessionRef), "x-lattice-service-auth": "production-route-test-service-auth" });
  const invokeText = await invoke.text(); expect(invoke.status, `${suffix}: ${invokeText}`).toBe(200); return JSON.parse(invokeText);
}

function signedGenericProfile(descriptor: any) {
  const driver_config = { endpoint: `https://auth-driver-upstream.test/${descriptor.profile_ref.split(".").at(-1)}`, custodian: { ...descriptor.trusted_auth_driver, entry_ref: `custodian.fixture.${descriptor.profile_ref}` }, transport: { ...descriptor.trusted_auth_driver, entry_ref: `transport.fixture.${descriptor.profile_ref}` }, ...(descriptor.activation_kind.kind === "workload_binding" ? { trusted_issuers: ["issuer.fixture"] } : {}) };
  const unsigned = { deployment_id: "deployment-fixture", descriptor, driver_config, org_id: "org-fixture" };
  return { ...unsigned, signature_b64u: sign(null, Buffer.from(canonicalJson(unsigned)), GENERIC_PROFILE_KEYS.privateKey).toString("base64url") };
}

describe("production V2 broker routes", () => {
  it("denies executable V1 and performs binding, leases, exact grants, two effects and redelivery", async () => {
    const worker = await productionMf.getWorker("broker-production");
    const auth = await sessionOn(worker, "production-route-test-pepper");
    expect(auth.response.status).toBe(201);
    const intent = await jsonRequest(worker, "/v0.2/connection-intents", {
      connector_ref: "connector.google.workspace@1",
      auth_profile_ref: "auth.google.workspace.oauth2@1",
      execution_lane: "semantic_broker", custody: "hosted_broker",
    }, sessionHeaders(auth.body.session_ref));
    expect(intent.status).toBe(201);
    const intentBody = await intent.json() as any;
    const authorization = new URL(intentBody.next_action.url);
    const callback = new URL("http://broker/v0.2/credential-callback");
    callback.searchParams.set("state", authorization.searchParams.get("state")!);
    callback.searchParams.set("code", "4/mock-production-code");
    const activated = await worker.fetch(callback);
    expect(activated.status, await activated.clone().text()).toBe(200);
    const connection = await activated.json() as any;
    expect(connection.status).toBe("v2_prepared");

    const authority = v2FlowAuthorityFixture();
    const bindingLockHash = `sha256:${"3".repeat(64)}`;
    const bindingRequest = {
      connection_ref: connection.connection_ref, deployment_id: "deployment-fixture",
      bundle_id: "bundle-v2", flow_ir_hash: authority.flowIrHash, binding_lock_hash: bindingLockHash,
      flow_id: "flow-v2", flow_ir_json: authority.flowIrJson,
      authority_manifest_json: authority.authorityManifestJson,
      contracts: [authority.gmail.contract, authority.sheets.contract],
    };
    const bindingResponse = await jsonRequest(worker, "/v0.2/bindings", bindingRequest, sessionHeaders(auth.body.session_ref));
    const bindingText = await bindingResponse.text();
    expect(bindingResponse.status, bindingText).toBe(201);
    const binding = JSON.parse(bindingText);
    expect(binding.binding.schema_version).toBe("0.2");
    const productionDb = await productionMf.getD1Database("BROKER_DB", "broker-production");
    const cutover = await productionDb.prepare("SELECT phase,legacy_destruction_evidence_hash FROM credential_cutover_state_v2 WHERE org_id=? AND connection_ref=?")
      .bind("org-fixture", connection.connection_ref).first<any>();
    expect(cutover?.phase).toBe("complete");
    expect(cutover?.legacy_destruction_evidence_hash).toMatch(/^sha256:[0-9a-f]{64}$/);
    const freshEvents = await productionDb.prepare("SELECT phase FROM credential_cutover_events_v2 WHERE org_id=? AND connection_ref=? ORDER BY event_sequence")
      .bind("org-fixture", connection.connection_ref).all<any>();
    expect(freshEvents.results.map((row: any) => row.phase)).toEqual(["registry_verified", "binding_verified", "fence_switched", "legacy_material_destroyed", "complete"]);
    const bindingReplay = await jsonRequest(worker, "/v0.2/bindings", bindingRequest, sessionHeaders(auth.body.session_ref));
    expect([200, 201]).toContain(bindingReplay.status);
    expect((await productionDb.prepare("SELECT COUNT(*) AS count FROM credential_cutover_events_v2 WHERE org_id=? AND connection_ref=?")
      .bind("org-fixture", connection.connection_ref).first<any>())?.count).toBe(5);

    const grants: Record<string, { grant_ref: string; input: unknown }> = {};
    for (const [index, node] of [authority.sheets, authority.gmail].entries()) {
      const common = { session_ref: auth.body.session_ref, binding_ref: binding.binding_ref,
        bundle_id: "bundle-v2", flow_ir_hash: authority.flowIrHash, binding_lock_hash: bindingLockHash,
        flow_id: "flow-v2", run_id: "run-v2", node_id: node.nodeId, node_alias: node.alias,
        activation_ordinal: index + 1 };
      const leaseRequest = { ...common, operation_contract: node.contract };
      const leaseResponse = await jsonRequest(worker, "/internal/v0.2/node-leases", leaseRequest,
        { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
      const leaseText = await leaseResponse.text();
      expect(leaseResponse.status, leaseText).toBe(201);
      const lease = JSON.parse(leaseText);
      expect(lease.node_lease.audience).toBe("broker-grant-derivation");
      expect((Date.parse(lease.node_lease.expires_at) - Date.parse(lease.node_lease.not_before)) / 1000).toBeLessThanOrEqual(301);
      const leaseReplay = await jsonRequest(worker, "/internal/v0.2/node-leases", leaseRequest,
        { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
      expect(leaseReplay.status).toBe(200);
      expect((await leaseReplay.json() as any).redelivery).toBe(true);
      const input = node.alias === "sheets"
        ? { spreadsheet_id: "sheet-1", sheet: "Sheet1", row: { column: "one" }, value_input_option: "raw" }
        : { to: "test@example.com", subject: "Hello", text_body: "body" };
      const grantResponse = await jsonRequest(worker, "/internal/v0.2/grants", {
        ...common, node_lease_ref: lease.node_lease_ref, semantic_effect_slot: node.slot,
        expected_cas_version: 0, input,
      }, { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
      const grantText = await grantResponse.text();
      expect(grantResponse.status, grantText).toBe(201);
      const grant = JSON.parse(grantText);
      expect(grant.grant.audience).toBe("broker-execution");
      expect(grant.grant.budgets.logical_calls).toBe(1);
      grants[node.alias] = { grant_ref: grant.grant_ref, input };
    }

    for (const node of [authority.sheets, authority.gmail]) {
      const invoke = await jsonRequest(worker, "/internal/v0.2/invoke", {
        session_ref: auth.body.session_ref, grant_ref: grants[node.alias].grant_ref,
        input: grants[node.alias].input,
      }, { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
      const invokeText = await invoke.text();
      expect(invoke.status, invokeText).toBe(200);
      const result = JSON.parse(invokeText);
      expect(result.receipt.schema_version).toBe("0.2");
      expect(result.receipt.dispatch_attempt).toBe(1);
      expect(result.receipt.claims.provider_dispatch_observed).toBe(true);
      const receiptLookup = await authenticatedFetch(worker, `/v0.2/receipts/${result.receipt_ref}`, auth.body.session_ref);
      expect(receiptLookup.status).toBe(200);
      expect((await receiptLookup.json() as any).receipt.schema_version).toBe("0.2");
      const replay = await jsonRequest(worker, "/internal/v0.2/invoke", {
        session_ref: auth.body.session_ref, grant_ref: grants[node.alias].grant_ref,
        input: grants[node.alias].input,
      }, { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
      expect(replay.status).toBe(200);
      expect((await replay.json() as any).redelivery).toBe(true);
    }
    const altered = await jsonRequest(worker, "/internal/v0.2/invoke", {
      session_ref: auth.body.session_ref, grant_ref: grants.sheets.grant_ref,
      input: { ...(grants.sheets.input as any), spreadsheet_id: "altered" },
    }, { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
    expect(altered.status).toBe(409);
    const counts = await (await productionMf.getWorker("production-mock-services")).fetch("http://provider/__counts");
    expect((await counts.json() as any).provider).toBe(2);
    const db = await productionMf.getD1Database("BROKER_DB", "broker-production");
    const connectionRow = await db.prepare("SELECT connection_ref,material_do_route FROM connections_v2 WHERE org_id=? AND status='active'").bind("org-fixture").first<any>();
    const stateRoute = `sha256:${createHash("sha256").update(`lattice.credential-state.v2\0org-fixture\0${connectionRow.connection_ref}`).digest("hex")}`;
    const stateNamespace = await productionMf.getDurableObjectNamespace("CREDENTIAL_STATE_V2_DO", "broker-production");
    const stateReply = await stateNamespace.get(stateNamespace.idFromName(stateRoute)).fetch("http://state/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ org_id: "org-fixture", connection_ref: connectionRow.connection_ref, command: { op: "read" } }) });
    const state = await stateReply.json() as any;
    expect(JSON.parse(new TextDecoder().decode(new Uint8Array(state.fence_json))).v2_lease_ever_issued).toBe(true);
    const legacyNamespace = await productionMf.getDurableObjectNamespace("CONNECTION_REFRESH_DO", "broker-production");
    expect((await legacyNamespace.get(legacyNamespace.idFromName(connectionRow.material_do_route)).fetch("http://legacy/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ op: "metadata" }) })).status).toBe(409);
    for (const path of ["/v1/sessions", "/internal/v1/grants", "/internal/v1/invoke"]) {
      expect((await worker.fetch(`http://broker${path}`, { method: "POST", body: "{}" })).status).toBe(404);
    }
    await (await productionMf.getWorker("production-mock-services")).fetch("http://provider/__revoke-fail-once");
    const interrupted = await authenticatedFetch(worker, `/v0.2/connections/${connection.connection_ref}`, auth.body.session_ref, "DELETE");
    expect(interrupted.status).toBe(503);
    expect((await db.prepare("SELECT status FROM connections_v2 WHERE org_id=? AND connection_ref=?").bind("org-fixture", connection.connection_ref).first<any>())?.status).toBe("cleanup_pending");
    const revoked = await authenticatedFetch(worker, `/v0.2/connections/${connection.connection_ref}`, auth.body.session_ref, "DELETE");
    expect(revoked.status, await revoked.clone().text()).toBe(200);
    expect((await revoked.json() as any).status).toBe("revoked");
    const revokedReplay = await authenticatedFetch(worker, `/v0.2/connections/${connection.connection_ref}`, auth.body.session_ref, "DELETE");
    expect(revokedReplay.status).toBe(200);
    expect((await revokedReplay.json() as any).redelivery).toBe(true);
    const destroyedReply = await stateNamespace.get(stateNamespace.idFromName(stateRoute)).fetch("http://state/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ org_id: "org-fixture", connection_ref: connectionRow.connection_ref, command: { op: "read" } }) });
    const destroyed = await destroyedReply.json() as any;
    expect(destroyed.material_generations).toEqual([]);
    expect(destroyed.destroyed_material_generations).toEqual([1]);
    expect(destroyed.revocation_evidence_hash).toMatch(/^sha256:[0-9a-f]{64}$/);
    const forbiddenLease = await stateNamespace.get(stateNamespace.idFromName(stateRoute)).fetch("http://state/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ org_id: "org-fixture", connection_ref: connectionRow.connection_ref, command: { op: "lease_material_for_dispatch", generation: 1 } }) });
    expect(forbiddenLease.status).toBe(409);
  });

  it("executes signed generic private activation drivers with one-time channels", async () => {
    const worker = await productionMf.getWorker("broker-production");
    expect((await worker.fetch("http://broker/ready")).status).toBe(200);
    const auth = await sessionOn(worker, "production-route-test-pepper");
    const service = { ...sessionHeaders(auth.body.session_ref), "x-lattice-activation-auth": "private-activation-service-auth-fixture" };
    let externalConnection = "";
    for (const kind of ["header", "query", "basic", "bearer", "workload", "external"] as const) {
      const descriptor = genericDescriptor(kind, kind);
      const created = await jsonRequest(worker, "/internal/v0.2/activations", {
        operator_id: "operator-fixture", request_jti: `generic-${kind}-${randomBytes(8).toString("hex")}`,
        contract_ids: ["connector.google.gmail.send_message@1", "connector.google.sheets.append_row@1"], signed_profile: signedGenericProfile(descriptor),
      }, service);
      const createdText = await created.text();
      expect(created.status, `${kind}: ${createdText}`).toBe(201);
      const action = JSON.parse(createdText);
      const submission: any = {};
      if (["header", "query", "basic", "bearer"].includes(kind)) {
        Object.assign(submission, { channel_ref: action.channel_ref, material_b64u: Buffer.from(kind === "basic" ? JSON.stringify({ username: "fixture-user", password: "fixture-password" }) : `secret-${kind}`).toString("base64url") });
      } else if (kind === "workload") {
        Object.assign(submission, { channel_ref: action.channel_ref, assertion_b64u: Buffer.from("signed-workload-assertion").toString("base64url"), issuer: "issuer.fixture", audience: "lattice-workload", nonce: action.nonce, issued_at: Math.floor(Date.now() / 1000) });
      } else {
        Object.assign(submission, { challenge: action.challenge, custodian_ref: "auth-driver.fixture", remote_proof: Buffer.from("signed-remote-proof-fixture").toString("base64url") });
      }
      const path = `/internal/v0.2/activations/${action.activation_ref}/submit`;
      const envelope=sealGenericActivation(action,submission,`submit-${kind}-${randomBytes(6).toString("hex")}`);
      expect(JSON.stringify(envelope)).not.toContain("secret-");
      if(kind==="header"){
        const tampered={...envelope,ciphertext_b64u:Buffer.concat([Buffer.from(envelope.ciphertext_b64u,"base64url").subarray(0,-1),Buffer.from([Buffer.from(envelope.ciphertext_b64u,"base64url").at(-1)!^1])]).toString("base64url")};
        expect((await jsonRequest(worker,path,tampered,service)).status).toBe(409);
        expect((await jsonRequest(worker,path,sealGenericActivation(action,submission,"wrong-aad-jti",{serviceIdentity:"wrong-service"}),service)).status).toBe(409);
        const wrong=generateKeyPairSync("x25519"); const wrongPublic=(wrong.publicKey.export({format:"jwk"}) as JsonWebKey).x!;
        expect((await jsonRequest(worker,path,sealGenericActivation(action,submission,"wrong-key-jti",{publicKey:wrongPublic}),service)).status).toBe(409);
        expect((await jsonRequest(worker,path,sealGenericActivation(action,submission,"expired-jti",{expiresAt:Math.floor(Date.now()/1000)-1,issuedAt:Math.floor(Date.now()/1000)-2}),service)).status).toBe(409);
      }
      const completed = await jsonRequest(worker, path, envelope, service);
      const completedText = await completed.text();
      expect(completed.status, `${kind}: ${completedText}`).toBe(200);
      expect(JSON.parse(completedText).status).toBe("active");
      const completedBody = JSON.parse(completedText);
      if (kind === "external") externalConnection = completedBody.connection_ref;
      const invoked = await exerciseGenericConnection(worker, auth.body.session_ref, completedBody.connection_ref, kind);
      expect(invoked.receipt.claims.provider_dispatch_observed).toBe(true);
      expect(invoked.receipt.claims.remote_durable_state_proven,kind).toBe(false);
      expect((await jsonRequest(worker, path, envelope, service)).status).toBe(409);
      expect((await worker.fetch(`http://broker/v0.2/activations/${action.activation_ref}/submit`, { method: "POST", body: "{}" })).status).toBe(404);
    }
    const genericDb = await productionMf.getD1Database("BROKER_DB", "broker-production");
    const storedActivation=JSON.stringify((await genericDb.prepare("SELECT canonical_profile_json,driver_config_json,expected_claims_json FROM generic_activation_intents_v2 WHERE org_id=?").bind("org-fixture").all()).results);
    expect(storedActivation).not.toContain("secret-header");
    expect(storedActivation).not.toContain("signed-workload-assertion");
    expect(storedActivation).not.toContain("signed-remote-proof-fixture");
    expect((await genericDb.prepare("SELECT material_mode FROM connections_v2 WHERE org_id=? AND connection_ref=?").bind("org-fixture", externalConnection).first<any>())?.material_mode).toBe("remote_external");
    const externalRoute = `sha256:${createHash("sha256").update(`lattice.credential-state.v2\0org-fixture\0${externalConnection}`).digest("hex")}`;
    const genericStateNs = await productionMf.getDurableObjectNamespace("CREDENTIAL_STATE_V2_DO", "broker-production");
    const externalState = await (await genericStateNs.get(genericStateNs.idFromName(externalRoute)).fetch("http://state/", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ org_id: "org-fixture", connection_ref: externalConnection, command: { op: "read" } }) })).json() as any;
    expect(externalState.material_generations).toEqual([]);
    expect(externalState.remote_custodian_binding_hash).toMatch(/^sha256:[0-9a-f]{64}$/);
    const remoteRevoked = await authenticatedFetch(worker, `/v0.2/connections/${externalConnection}`, auth.body.session_ref, "DELETE");
    expect(remoteRevoked.status, await remoteRevoked.clone().text()).toBe(200);
    const expiringDescriptor = genericDescriptor("expired", "header");
    const expiring = await jsonRequest(worker, "/internal/v0.2/activations", { operator_id: "operator-fixture", request_jti: `expired-${randomBytes(8).toString("hex")}`, contract_ids: ["connector.google.sheets.append_row@1"], signed_profile: signedGenericProfile(expiringDescriptor) }, service);
    expect(expiring.status).toBe(201);
    const expiringAction = await expiring.json() as any;
    const activationDb = await productionMf.getD1Database("BROKER_DB", "broker-production");
    await activationDb.prepare("UPDATE generic_activation_intents_v2 SET expires_at=? WHERE org_id=? AND activation_ref=?").bind(Math.floor(Date.now() / 1000) - 1, "org-fixture", expiringAction.activation_ref).run();
    const expiredAt=Math.floor(Date.now()/1000)-1;
    const expiredPlaintext={channel_ref:expiringAction.channel_ref,material_b64u:Buffer.from("expired-secret").toString("base64url")};
    const expiredEnvelope=sealGenericActivation(expiringAction,expiredPlaintext,"expired-submit",{expiresAt:expiredAt,issuedAt:expiredAt-60});
    expect((await jsonRequest(worker, `/internal/v0.2/activations/${expiringAction.activation_ref}/submit`, expiredEnvelope, service)).status).toBe(409);
    const unknown = genericDescriptor("unknown", "bearer");
    (unknown.activation_kind as any).kind = "device_authorization";
    const denied = await jsonRequest(worker, "/internal/v0.2/activations", { operator_id: "operator-fixture", request_jti: "unknown-device", contract_ids: ["connector.google.sheets.append_row@1"], signed_profile: signedGenericProfile(unknown) }, service);
    expect(denied.status).toBe(409);
  }, 60_000);

  it("executes signed-inventory legacy reconciliation across journal restarts and preserves historical verification", async () => {
    const worker = await productionMf.getWorker("broker-production");
    const auth = await sessionOn(worker, "production-route-test-pepper");
    expect(auth.response.status).toBe(201);
    const db = await productionMf.getD1Database("BROKER_DB", "broker-production");
    await db.prepare("INSERT INTO legacy_admission_inventories_v2(org_id,inventory_ref,inventory_hash,canonical_inventory_json) VALUES(?,?,?,?)")
      .bind("org-fixture", "legacy.production.inventory.final", `sha256:${createHash("sha256").update(LEGACY_INVENTORY_JCS).digest("hex")}`, LEGACY_INVENTORY_JCS).run();
    await db.prepare("INSERT INTO legacy_inventory_decisions_v2(org_id,inventory_ref,inventory_hash,canonical_decision_json) VALUES(?,?,?,?)")
      .bind("org-fixture", "legacy.production.inventory.final", `sha256:${createHash("sha256").update(LEGACY_INVENTORY_JCS).digest("hex")}`, LEGACY_INVENTORY_DECISION_JCS).run();
    await db.prepare("INSERT INTO connections_v2(org_id,connection_ref,profile_ref,profile_version,active_material_generation,status) VALUES(?,?,?,?,?,?)")
      .bind("org-fixture", "replacement-restart", "auth.google.workspace.oauth2", "1", 1, "active").run();
    for (const [index, phase] of ["inventoried", "binding_verified", "legacy_material_destroyed"].entries()) {
      const legacy = `legacy-restart-${index}`;
      await db.prepare("INSERT INTO connections_v1_quarantine(org_id,connection_ref,intent_ref,connector_ref,auth_profile_ref,execution_lane,custody,account_commitment,actual_scopes_json,refresh_do_route,revocation_epoch,status) VALUES(?,?,?,?,?,'semantic_broker','hosted_broker',?,?,?,0,'active')")
        .bind("org-fixture", legacy, `intent-${legacy}`, "connector.google.workspace@1", "auth.google.workspace.oauth2@1", "sha256:legacy-account", "[]", `refresh-${legacy}`).run();
      await db.prepare("INSERT INTO connections_v2(org_id,connection_ref,profile_ref,profile_version,status) VALUES(?,?,?,'legacy-import','reconciling')")
        .bind("org-fixture", legacy, "auth.google.workspace.oauth2").run();
      const cas = index + 1;
      await db.prepare("INSERT INTO credential_cutover_state_v2(org_id,connection_ref,phase,cas_version) VALUES(?,?,?,?)").bind("org-fixture", legacy, phase, cas).run();
      const fenceJson = canonicalJson({ schema_version: "0.2", phase: "v2_prepared", fence_generation: 1, v2_lease_ever_issued: true,
        v2_rotation_ever_started: false, v1_leasing_disabled: false, active_v2_generation: null, cas_version: cas });
      await db.prepare("INSERT INTO credential_fences_v2(org_id,connection_ref,phase,fence_generation,v2_lease_ever_issued,v2_rotation_ever_started,v1_leasing_disabled,active_v2_generation,cas_version,canonical_fence_json) VALUES(?,?,'v2_prepared',1,1,0,0,NULL,?,?)")
        .bind("org-fixture", legacy, cas, fenceJson).run();
      const body = { session_ref: auth.body.session_ref, legacy_connection_ref: legacy, replacement_connection_ref: "replacement-restart", expected_cas_version: cas };
      const stale = await jsonRequest(worker, "/internal/v0.2/cutover/reconcile", { ...body, expected_cas_version: cas - 1 },
        { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
      expect(stale.status).toBe(409);
      const reconciled = await jsonRequest(worker, "/internal/v0.2/cutover/reconcile", body,
        { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" });
      expect(reconciled.status, await reconciled.clone().text()).toBe(200);
      const state = await db.prepare("SELECT phase,cas_version,legacy_destruction_evidence_hash FROM credential_cutover_state_v2 WHERE org_id=? AND connection_ref=?").bind("org-fixture", legacy).first<any>();
      expect(state?.phase).toBe("complete"); expect(state?.cas_version).toBe(cas + 1); expect(state?.legacy_destruction_evidence_hash).toMatch(/^sha256:[0-9a-f]{64}$/);
      const fence = await db.prepare("SELECT phase,v2_lease_ever_issued,v1_leasing_disabled FROM credential_fences_v2 WHERE org_id=? AND connection_ref=?").bind("org-fixture", legacy).first<any>();
      expect(fence).toEqual({ phase: "v2_authoritative", v2_lease_ever_issued: 1, v1_leasing_disabled: 1 });
      expect((await jsonRequest(worker, "/internal/v0.2/cutover/reconcile", body,
        { ...sessionHeaders(auth.body.session_ref), "x-lattice-service-auth": "production-route-test-service-auth" })).status).toBe(409);
    }
    const historicalUnsigned: any = { schema_version: "0.1", critical_fields: [], org_id: "org-fixture", principal: { kind: "broker", id: "broker-production" },
      issuer: "broker-production", broker_key_id: "broker-receipt-v1", grant_hash: `sha256:${"1".repeat(64)}`, policy_hash: `sha256:${"2".repeat(64)}`,
      contract_hash: `sha256:${"3".repeat(64)}`, plugin_module_sha256: `sha256:${"4".repeat(64)}`, plugin_trust_tier: "lattice_first_party",
      bundle_id: "archived-bundle", flow_ir_hash: `sha256:${"5".repeat(64)}`, binding_lock_hash: `sha256:${"6".repeat(64)}`, flow_id: "archived-flow",
      run_id: "archived-run", node_id: "archived-node", node_alias: "archived", logical_effect_id: `sha256:${"7".repeat(64)}`, dispatch_attempt: 0,
      connection_commitment: { alg: "hmac-sha256", key_id: "archive", verification_tier: "broker_only", value: `hmac-sha256:${"8".repeat(64)}` },
      canonical_input_commitment: { alg: "hmac-sha256", key_id: "archive", verification_tier: "broker_only", value: `hmac-sha256:${"9".repeat(64)}` },
      request_plan_hash: null, authority_facts_hash: null, budget_before: 1, budget_after: 1, provider_request_id: null,
      response_commitment: { alg: "hmac-sha256", key_id: "archive", verification_tier: "broker_only", value: `hmac-sha256:${"a".repeat(64)}` },
      outcome: "rejected", claims: { trusted_host_scope_authenticated: true, broker_admission_enforced: true, provider_dispatch_observed: false, remote_durable_state_proven: false, verifiable_execution_proven: false },
      issued_at: "2026-07-20T00:00:00Z" };
    const receiptPreimage = Buffer.concat([Buffer.from("lattice.invocation-receipt.v0.1\0"), Buffer.from(canonicalJson(historicalUnsigned))]);
    const receiptKey = createPrivateKey({ key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), Buffer.from("11".repeat(32), "hex")]), format: "der", type: "pkcs8" });
    const historicalReceipt = canonicalJson({ ...historicalUnsigned, signature: { alg: "Ed25519", key_id: "broker-receipt-v1", value: sign(null, receiptPreimage, receiptKey).toString("base64url") } });
    await db.prepare("INSERT INTO historical_verification_keys_v2(org_id,issuer,key_id,archive_hash,canonical_archive_json) VALUES(?,?,?,?,?)").bind("org-fixture", "broker-production", "broker-receipt-v1", `sha256:${createHash("sha256").update(HISTORICAL_RECEIPT_KEY_ARCHIVE_JCS).digest("hex")}`, HISTORICAL_RECEIPT_KEY_ARCHIVE_JCS).run();
    await db.prepare("INSERT INTO receipts_v1_history(org_id,deployment_id,receipt_ref,grant_ref,reservation_identity_hash,receipt_json) VALUES(?,?,?,?,?,?)")
      .bind("org-fixture", "deployment-fixture", "historical-cutover-receipt", "archived-grant", "sha256:archived", historicalReceipt).run();
    const historical = await authenticatedFetch(worker, "/v0.2/receipts/historical-cutover-receipt", auth.body.session_ref);
    expect(historical.status, await historical.clone().text()).toBe(200);
    expect((await historical.json() as any).historical_protocol).toBe("0.1");
    for (const path of ["/v1/sessions", "/internal/v1/grants", "/internal/v1/invoke"]) {
      expect((await worker.fetch(`http://broker${path}`, { method: "POST", body: "{}" })).status).toBe(404);
    }
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
