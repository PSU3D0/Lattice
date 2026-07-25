import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { access, chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { executeApply, redactedPlan, REQUIRED_SECRET_NAMES } from "./deploy-lib.mjs";
import {
  D1_ID_SENTINEL,
  renderD1DatabaseId,
  validateD1Id,
} from "./cloudflare-identifiers.mjs";
import {
  classifyDeploymentsListResult,
  deployPrivateWorker,
  loadPrivateWorkerSecrets,
} from "./private-worker-deploy-lib.mjs";
import {
  PRIVATE_DIGEST_VARS,
  replaceJsonStringPlaceholdersOnce,
  replacePlaceholdersOnce,
  validateRenderedConfig,
  withRenderedDeployConfigs,
  writeRenderedConfigEvidence,
} from "./rendered-config.mjs";
import { validatePublicCallbackBase } from "./workers-subdomain.mjs";
import { renderWorkerUploadedSourceDigests } from "./uploaded-source.mjs";

const hash = "c".repeat(64);
const kvNamespaceDeleteError = `✘ [ERROR] A request to the Cloudflare API (/accounts/<acct>/storage/kv/namespaces) failed.
Authentication error [code: 10000]`;
const d1Id = "c1a61a80-5d61-4500-aae8-4db034897753";
const deploymentIds = {
  auth: "11111111-1111-4111-8111-111111111111",
  provider: "22222222-2222-4222-8222-222222222222",
  token: "33333333-3333-4333-8333-333333333333",
  private: "44444444-4444-4444-8444-444444444444",
  public: "55555555-5555-4555-8555-555555555555",
};
const versionIds = {
  auth: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1",
  provider: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2",
  token: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3",
  private: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4",
  public: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5",
};
const runStartedAt = "2026-07-25T02:35:40Z";
function cloudflareDeployment(id, versionId, createdOn = "2026-07-25T02:35:42Z", triggeredBy = "upload") {
  return {
    id,
    source: "wrangler",
    strategy: "percentage",
    annotations: { "workers/triggered_by": triggeredBy },
    versions: [{ version_id: versionId, percentage: 100 }],
    created_on: createdOn,
  };
}
const completeSecrets = {
  "auth-driver": { AUTH_DRIVER_SERVICE_AUTH: "shared-auth-driver-value" },
  "google-token-egress": {
    GOOGLE_EGRESS_SERVICE_AUTH: "shared-egress-value",
    GOOGLE_OAUTH_CLIENT_ID: "oauth-client-id",
    GOOGLE_OAUTH_CLIENT_SECRET: "oauth-client-secret",
    GOOGLE_TOKEN_RESULT_KEY: "1".repeat(64),
  },
  "google-provider-egress": { GOOGLE_EGRESS_SERVICE_AUTH: "shared-egress-value" },
  "broker-private": {
    ACTIVATION_SERVICE_AUTH: "activation-value",
    AI_GATEWAY_AUTHORIZATION: "gateway-value",
    AUTH_DRIVER_SERVICE_AUTH: "shared-auth-driver-value",
    BINDING_SIGNING_SEED: "2".repeat(64),
    COMMITMENT_KEY: "3".repeat(64),
    CUSTODY_ROOT_KEY: "4".repeat(64),
    DEPLOYMENT_BOOTSTRAP_AUTH: "bootstrap-value",
    GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U: Buffer.alloc(32, 5).toString("base64url"),
    GOOGLE_EGRESS_SERVICE_AUTH: "shared-egress-value",
    INVOKE_SERVICE_AUTH: "invoke-value",
    KEY_HASH_PEPPER: "pepper-value",
    RECEIPT_SIGNING_SEED: "6".repeat(64),
  },
  "broker-public": {},
};
const context = {
  accountId: "a".repeat(32), prefix: "lattice-b5-test123", d1Id,
  d1Name: "lattice-b5-test123-broker",
  privateName: "lattice-b5-test123-broker-private",
  publicName: "lattice-b5-test123-broker-public",
  workersSubdomain: "frankie-colson",
  publicCallbackBase: "https://lattice-b5-test123-broker-public.frankie-colson.workers.dev",
  googleOauthRedirectUri: "https://lattice-b5-test123-broker-public.frankie-colson.workers.dev/v0.2/credential-callback",
  cloudflareApiToken: "test-token-not-a-credential",
  fetchImpl: async () => ({ ok: true, json: async () => ({ success: true, result: { subdomain: "frankie-colson" } }) }),
  authDriverService: "approved-auth-driver",
  googleProviderService: "approved-google-provider",
  googleTokenService: "approved-google-token",
  approvedDependencies: {
    schema_version: "3", account_id: "a".repeat(32), prefix: "lattice-b5-test123",
    d1_database_id: d1Id,
    artifacts: {
      deployment_authority_key_id: "operator-key", deployment_authority_public_key_b64u: "operator-public-key",
      deployment_contract_set_jcs: "{}", deployment_standing_authority_jcs: "{}",
      historical_archive_authority_key_id: "archive-key", historical_archive_authority_public_key_b64u: "archive-public-key",
      legacy_cutover_authority_key_id: "cutover-key", legacy_cutover_authority_public_key_b64u: "cutover-public-key",
      generic_profile_authority_public_key_b64u: "profile-public-key", generic_profile_registry_jcs: "{}",
      generic_activation_recipient_key_id: "recipient-key",
    },
    services: {
      AUTH_DRIVER_SERVICE: { name: "approved-auth-driver", account_id: "a".repeat(32), deployment_id: deploymentIds.auth, version_id: versionIds.auth, uploaded_source_sha256: hash },
      GOOGLE_PROVIDER_SERVICE: { name: "approved-google-provider", account_id: "a".repeat(32), deployment_id: deploymentIds.provider, version_id: versionIds.provider, uploaded_source_sha256: hash },
      GOOGLE_TOKEN_SERVICE: { name: "approved-google-token", account_id: "a".repeat(32), deployment_id: deploymentIds.token, version_id: versionIds.token, uploaded_source_sha256: hash },
    },
    workers: {},
  },
  privateConfig: '{"workers_dev": false}', publicConfig: '{"workers_dev": true}',
  privateConfigPath: "/evidence/private.jsonc", publicConfigPath: "/evidence/public.jsonc",
  privateUploadedSourceSha256: hash, publicUploadedSourceSha256: hash, runStartedAt,
  privateSecrets: completeSecrets["broker-private"],
  publicSecrets: completeSecrets["broker-public"],
};

class FakeRunner {
  constructor(overrides = {}) { this.overrides = overrides; this.calls = []; this.inputs = []; }
  async run(step, command, options = {}) {
    this.calls.push({ step, command });
    if (options.input !== undefined) this.inputs.push({ step, input: options.input });
    if (this.overrides[step] !== undefined) return this.overrides[step];
    if (step === "private_worker:deploy" && this.overrides.deploy_private !== undefined) return this.overrides.deploy_private;
    if (step === "public_worker:deploy" && this.overrides.deploy_public !== undefined) return this.overrides.deploy_public;
    if (step === "auth") return { status: 0, stdout: JSON.stringify({ loggedIn: true, account_id: context.accountId }) };
    if (step.startsWith("target:")) return { status: 0, stdout: "[]" };
    if (step === "private_worker:absence") return this.overrides["target:private"] ?? { status: 0, stdout: "[]" };
    if (step === "public_worker:absence") return this.overrides["target:public"] ?? { status: 0, stdout: "[]" };
    if (step === "dependency:AUTH_DRIVER_SERVICE") return { status: 0, stdout: JSON.stringify([cloudflareDeployment(deploymentIds.auth, versionIds.auth)]) };
    if (step === "dependency:GOOGLE_PROVIDER_SERVICE") return { status: 0, stdout: JSON.stringify([cloudflareDeployment(deploymentIds.provider, versionIds.provider)]) };
    if (step === "dependency:GOOGLE_TOKEN_SERVICE") return { status: 0, stdout: JSON.stringify([cloudflareDeployment(deploymentIds.token, versionIds.token)]) };
    if (step === "d1") return { status: 0, stdout: JSON.stringify([{ uuid: context.d1Id }]) };
    if (step === "private_worker:secrets") return { status: 0, stdout: JSON.stringify(REQUIRED_SECRET_NAMES.map((name) => ({ name }))) };
    if (step === "public_worker:secrets") return { status: 0, stdout: "[]" };
    if (step === "private_worker:capture") return { status: 0, stdout: JSON.stringify([cloudflareDeployment(deploymentIds.private, versionIds.private)]) };
    if (step === "public_worker:capture") return { status: 0, stdout: JSON.stringify([cloudflareDeployment(deploymentIds.public, versionIds.public)]) };
    if (step === "readiness:oauth_callback_400") return { status: 0, stdout: "400" };
    if (step === "smoke") return { status: 0, stdout: JSON.stringify({ status: "ok" }) };
    if (step.endsWith(":verify")) return { status: 0, stdout: "[]" };
    return { status: 0, stdout: "{}" };
  }
}

function ownedContext(kind) {
  const name = kind === "private" ? context.privateName : context.publicName;
  return {
    ...context,
    approvedDependencies: {
      ...context.approvedDependencies,
      workers: { [kind]: {
        name, account_id: context.accountId, prefix: context.prefix,
        deployment_id: deploymentIds[kind], version_id: versionIds[kind], uploaded_source_sha256: hash,
      } },
    },
  };
}
const live = (kind) => ({ status: 0, stdout: JSON.stringify([
  cloudflareDeployment(deploymentIds[kind], versionIds[kind], runStartedAt),
]) });

function mutatingCalls(runner) {
  return runner.calls.filter(({ command }) =>
    command[0] === "npx" && command[1] === "wrangler" && (
      ["deploy", "delete", "secret"].includes(command[2]) ||
      (command[2] === "d1" && command[3] === "migrations")
    )
  );
}

function validRenderedPrivateConfig() {
  const bundleJcs = '{"schema_version":"1","artifact":"operator"}';
  const bundleHash = `sha256:${createHash("sha256").update(bundleJcs).digest("hex")}`;
  return JSON.stringify({
    workers_dev: false,
    vars: {
      BROKER_WORKER_WASM_SHA256: `sha256:${hash}`,
      AUTH_DRIVER_WORKER_SHA256: `sha256:${hash}`,
      GOOGLE_TOKEN_WORKER_SHA256: `sha256:${hash}`,
      GOOGLE_PROVIDER_WORKER_SHA256: `sha256:${hash}`,
      OPERATOR_ARTIFACT_BUNDLE_JCS: bundleJcs,
      OPERATOR_ARTIFACT_BUNDLE_SHA256: bundleHash,
    },
  });
}

async function assertFileMissing(path) {
  await assert.rejects(access(path), (error) => error?.code === "ENOENT");
}

async function withCloudflareAccountId(value, callback) {
  const previous = process.env.CLOUDFLARE_ACCOUNT_ID;
  if (value === undefined) delete process.env.CLOUDFLARE_ACCOUNT_ID;
  else process.env.CLOUDFLARE_ACCOUNT_ID = value;
  try {
    return await callback();
  } finally {
    if (previous === undefined) delete process.env.CLOUDFLARE_ACCOUNT_ID;
    else process.env.CLOUDFLARE_ACCOUNT_ID = previous;
  }
}

async function assertAccountRejected(stdout, expectedError, cloudflareAccountId) {
  await withCloudflareAccountId(cloudflareAccountId, async () => {
    const runner = new FakeRunner({ auth: { status: 0, stdout } });
    await assert.rejects(executeApply(context, runner), expectedError);
    assert.equal(mutatingCalls(runner).length, 0);
  });
}

test("canonical lowercase dashed D1 UUID is accepted", () => {
  assert.equal(validateD1Id(d1Id), d1Id);
});

test("undashed and uppercase D1 ids are rejected", () => {
  assert.throws(() => validateD1Id(d1Id.replaceAll("-", "")), /canonical lowercase UUID with dashes/);
  assert.throws(() => validateD1Id(d1Id.toUpperCase()), /canonical lowercase UUID with dashes/);
});

test("malformed, short, and long D1 ids are rejected", () => {
  for (const invalid of [
    `{${d1Id}}`, ` ${d1Id}`, `${d1Id} `,
    d1Id.slice(0, -1), `${d1Id}0`, "not-a-d1-id",
  ]) {
    assert.throws(() => validateD1Id(invalid), /canonical lowercase UUID with dashes/);
  }
});

test("D1 config rendering installs the exact UUID and removes the sentinel", async () => {
  const template = await readFile(resolve(new URL("../../wrangler.jsonc", import.meta.url).pathname), "utf8");
  const rendered = renderD1DatabaseId(template, d1Id);
  assert.equal(rendered.includes(`"database_id": "${d1Id}"`), true);
  assert.equal(rendered.includes(D1_ID_SENTINEL), false);
});

test("prefix-overlapping operator bundle placeholders render exactly and preserve the bundle hash invariant", () => {
  const bundleJcs = '{"schema_version":"1","artifact":"operator"}';
  const bundleHash = `sha256:${createHash("sha256").update(bundleJcs).digest("hex")}`;
  const template = JSON.stringify({
    vars: {
      BROKER_WORKER_WASM_SHA256: `sha256:${hash}`,
      AUTH_DRIVER_WORKER_SHA256: `sha256:${hash}`,
      GOOGLE_TOKEN_WORKER_SHA256: `sha256:${hash}`,
      GOOGLE_PROVIDER_WORKER_SHA256: `sha256:${hash}`,
      OPERATOR_ARTIFACT_BUNDLE_JCS: "REPLACE_WITH_OPERATOR_ARTIFACT_BUNDLE",
      OPERATOR_ARTIFACT_BUNDLE_SHA256: "REPLACE_WITH_OPERATOR_ARTIFACT_BUNDLE_HASH",
    },
  });
  const rendered = replaceJsonStringPlaceholdersOnce(template, {
    REPLACE_WITH_OPERATOR_ARTIFACT_BUNDLE: bundleJcs,
    REPLACE_WITH_OPERATOR_ARTIFACT_BUNDLE_HASH: bundleHash,
  });
  const parsed = validateRenderedConfig(rendered, {
    requiredDigestVars: PRIVATE_DIGEST_VARS,
    requireBundleHashInvariant: true,
  });
  assert.equal(parsed.vars.OPERATOR_ARTIFACT_BUNDLE_JCS, bundleJcs);
  assert.equal(parsed.vars.OPERATOR_ARTIFACT_BUNDLE_SHA256, bundleHash);
});

test("single-pass substitution neither truncates a longer token nor rescans replacement values", () => {
  const rendered = replacePlaceholdersOnce(
    "REPLACE_WITH_COLLISION_LONG|REPLACE_WITH_COLLISION",
    {
      REPLACE_WITH_COLLISION: "short:REPLACE_WITH_COLLISION_LONG",
      REPLACE_WITH_COLLISION_LONG: "long",
    },
  );
  assert.equal(rendered, "long|short:REPLACE_WITH_COLLISION_LONG");
});

test("post-render validation fails closed before mutation", () => {
  const fixtures = [
    [validRenderedPrivateConfig().replace('"workers_dev":false', '"leftover":"REPLACE_WITH_VALUE","workers_dev":false'), /REPLACE_WITH placeholder/],
    [validRenderedPrivateConfig().replace(`sha256:${hash}`, "not-a-digest"), /digest invalid/],
    [validRenderedPrivateConfig().replace(/"OPERATOR_ARTIFACT_BUNDLE_SHA256":"sha256:[0-9a-f]{64}"/, `"OPERATOR_ARTIFACT_BUNDLE_SHA256":"sha256:${"d".repeat(64)}"`), /bundle hash mismatch/],
  ];
  for (const [fixture, expected] of fixtures) {
    const runner = new FakeRunner();
    assert.throws(() => validateRenderedConfig(fixture, {
      requiredDigestVars: PRIVATE_DIGEST_VARS,
      requireBundleHashInvariant: true,
    }), expected);
    assert.equal(mutatingCalls(runner).length, 0);
  }
});

test("valid Workers subdomain callback form is accepted", () => {
  const callback = validatePublicCallbackBase(
    context.prefix,
    "frankie-colson",
    context.publicCallbackBase,
  );
  assert.equal(callback.googleOauthRedirectUri, context.googleOauthRedirectUri);
});
test("bare workers.dev callback form is rejected explicitly", () => {
  assert.throws(
    () => validatePublicCallbackBase(context.prefix, "frankie-colson", `https://${context.publicName}.workers.dev`),
    /missing the required account Workers subdomain label/,
  );
});
test("malformed Workers subdomain labels are rejected", () => {
  for (const label of ["Frankie", "two.labels", "-leading", "trailing-"]) {
    assert.throws(() => validatePublicCallbackBase(context.prefix, label, context.publicCallbackBase), /lowercase DNS label/);
  }
});
test("oversized Workers subdomain and total hostname are rejected", () => {
  const oversizedLabel = "a".repeat(64);
  assert.throws(() => validatePublicCallbackBase(context.prefix, oversizedLabel, context.publicCallbackBase), /lowercase DNS label/);
  assert.throws(
    () => validatePublicCallbackBase("a".repeat(240), "valid", `https://${"a".repeat(240)}-broker-public.valid.workers.dev`),
    /DNS name limit|invalid DNS label/,
  );
});
test("redacted dry-run performs no command and names the clean preflight", () => {
  const runner = new FakeRunner(); const plan = redactedPlan(context);
  assert.equal(runner.calls.length, 0);
  assert.deepEqual(plan.worker_secrets[context.privateName], REQUIRED_SECRET_NAMES.map((name) => ({ name, install_status: "planned" })));
  assert.equal(plan.steps[0], "clean_hermetic_production_preflight");
  assert.equal(plan.workers_subdomain, "frankie-colson");
  assert.equal(plan.google_oauth_redirect_uri, context.googleOauthRedirectUri);
});
test("rendered configs deploy from template sibling paths, leave evidence copies, and are removed", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lattice-rendered-config-"));
  const publicDirectory = join(directory, "deploy", "public-callback");
  const evidenceDir = join(directory, "evidence");
  const privateTemplatePath = join(directory, "wrangler.jsonc");
  const publicTemplatePath = join(publicDirectory, "wrangler.jsonc");
  await mkdir(publicDirectory, { recursive: true });
  await mkdir(evidenceDir);
  await writeFile(privateTemplatePath, "{}\n");
  await writeFile(publicTemplatePath, "{}\n");
  const privateConfig = validRenderedPrivateConfig();
  const publicConfig = '{"workers_dev":true}';
  try {
    await writeRenderedConfigEvidence({ evidenceDir, privateConfig, publicConfig });
    let deployedPaths;
    await withRenderedDeployConfigs({
      privateTemplatePath, publicTemplatePath, privateConfig, publicConfig,
    }, async (paths) => {
      deployedPaths = paths;
      const runner = new FakeRunner();
      await executeApply({ ...context, ...paths }, runner);
      const privateDeploy = runner.calls.find(({ step }) => step === "private_worker:deploy");
      const publicDeploy = runner.calls.find(({ step }) => step === "public_worker:deploy");
      const migrations = runner.calls.find(({ step }) => step === "migrations");
      assert.equal(privateDeploy.command[privateDeploy.command.indexOf("--config") + 1], paths.privateConfigPath);
      assert.equal(publicDeploy.command[publicDeploy.command.indexOf("--config") + 1], paths.publicConfigPath);
      assert.equal(migrations.command[migrations.command.indexOf("--config") + 1], paths.privateConfigPath);
      assert.equal(dirname(paths.privateConfigPath), dirname(privateTemplatePath));
      assert.equal(dirname(paths.publicConfigPath), dirname(publicTemplatePath));
      await access(paths.privateConfigPath);
      await access(paths.publicConfigPath);
    });
    await assertFileMissing(deployedPaths.privateConfigPath);
    await assertFileMissing(deployedPaths.publicConfigPath);
    assert.equal(await readFile(join(evidenceDir, "wrangler.private.jsonc"), "utf8"), privateConfig);
    assert.equal(await readFile(join(evidenceDir, "wrangler.public.jsonc"), "utf8"), publicConfig);
    const record = JSON.parse(await readFile(join(evidenceDir, "rendered-config-record.json"), "utf8"));
    assert.equal(record.private.purpose, "evidence_record_only_not_deployed");
    assert.equal(record.public.purpose, "evidence_record_only_not_deployed");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("rendered sibling configs are removed when deployment fails", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lattice-rendered-config-failure-"));
  const publicDirectory = join(directory, "public");
  const privateTemplatePath = join(directory, "wrangler.jsonc");
  const publicTemplatePath = join(publicDirectory, "wrangler.jsonc");
  await mkdir(publicDirectory);
  let deployedPaths;
  try {
    await assert.rejects(withRenderedDeployConfigs({
      privateTemplatePath,
      publicTemplatePath,
      privateConfig: validRenderedPrivateConfig(),
      publicConfig: '{"workers_dev":true}',
    }, async (paths) => {
      deployedPaths = paths;
      const runner = new FakeRunner({ deploy_private: { status: 1, stdout: "", stderr: "synthetic" } });
      await executeApply({ ...context, ...paths }, runner);
    }), /private_worker_deploy_failed/);
    await assertFileMissing(deployedPaths.privateConfigPath);
    await assertFileMissing(deployedPaths.publicConfigPath);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("hermetic preflight failure occurs before every remote command", async () => {
  const runner = new FakeRunner({ hermetic_preflight: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(context, runner), /hermetic_preflight_failed/);
  assert.deepEqual(runner.calls.map((call) => call.step), ["hermetic_preflight"]);
});
test("apply rejects a live Workers subdomain mismatch before any mutating command", async () => {
  const fetchCalls = [];
  const runner = new FakeRunner();
  const mismatch = {
    ...context,
    fetchImpl: async (...request) => {
      fetchCalls.push(request);
      return { ok: true, json: async () => ({ success: true, result: { subdomain: "different-account" } }) };
    },
  };
  await assert.rejects(executeApply(mismatch, runner), /does not match the live Cloudflare account/);
  assert.equal(fetchCalls.length, 1);
  assert.equal(fetchCalls[0][0], `https://api.cloudflare.com/client/v4/accounts/${context.accountId}/workers/subdomain`);
  assert.equal(fetchCalls[0][1].method, "GET");
  assert.deepEqual(runner.calls.map((call) => call.step), ["hermetic_preflight"]);
});
test("OAuth whoami shape is accepted", async () => {
  await withCloudflareAccountId(undefined, async () => {
    const runner = new FakeRunner({
      auth: { status: 0, stdout: JSON.stringify({ loggedIn: true, account_id: context.accountId }) },
    });
    const evidence = await executeApply(context, runner);
    assert.equal(evidence.status, "qualified");
    assert.equal(mutatingCalls(runner).length > 0, true);
  });
});

test("API-token single-account whoami shape is accepted", async () => {
  await withCloudflareAccountId(undefined, async () => {
    const runner = new FakeRunner({
      auth: { status: 0, stdout: JSON.stringify({ loggedIn: true, accounts: [{ id: context.accountId }] }) },
    });
    const evidence = await executeApply(context, runner);
    assert.equal(evidence.status, "qualified");
    assert.equal(mutatingCalls(runner).length > 0, true);
  });
});

test("multi-account identity without CLOUDFLARE_ACCOUNT_ID aborts before mutation", async () => {
  await assertAccountRejected(
    JSON.stringify({ loggedIn: true, accounts: [{ id: context.accountId }, { id: "b".repeat(32) }] }),
    /multiple_accounts_require_matching_CLOUDFLARE_ACCOUNT_ID/,
    undefined,
  );
});

test("multi-account identity with matching CLOUDFLARE_ACCOUNT_ID is accepted", async () => {
  await withCloudflareAccountId(context.accountId, async () => {
    const runner = new FakeRunner({
      auth: { status: 0, stdout: JSON.stringify({ loggedIn: true, accounts: [{ id: "b".repeat(32) }, { id: context.accountId }] }) },
    });
    const evidence = await executeApply(context, runner);
    assert.equal(evidence.status, "qualified");
    assert.equal(mutatingCalls(runner).length > 0, true);
  });
});

test("multi-account identity with mismatched CLOUDFLARE_ACCOUNT_ID aborts before mutation", async () => {
  await assertAccountRejected(
    JSON.stringify({ loggedIn: true, accounts: [{ id: context.accountId }, { id: "b".repeat(32) }] }),
    /multiple_accounts_require_matching_CLOUDFLARE_ACCOUNT_ID/,
    "b".repeat(32),
  );
});

test("accounts not containing the expected account abort before mutation", async () => {
  await assertAccountRejected(
    JSON.stringify({ loggedIn: true, accounts: [{ id: "b".repeat(32) }] }),
    /account_mismatch/,
    undefined,
  );
});

test("empty accounts array aborts before mutation", async () => {
  await assertAccountRejected(
    JSON.stringify({ loggedIn: true, accounts: [] }),
    /account_accounts_empty/,
    undefined,
  );
});

test("malformed whoami JSON aborts before mutation", async () => {
  await assertAccountRejected("{not-json", /account_identity_invalid_json/, undefined);
});

test("logged-out whoami identity aborts before mutation", async () => {
  await assertAccountRejected(
    JSON.stringify({ loggedIn: false, accounts: [{ id: context.accountId }] }),
    /account_not_authenticated/,
    undefined,
  );
});

test("whoami identity missing loggedIn aborts before mutation", async () => {
  await assertAccountRejected(
    JSON.stringify({ accounts: [{ id: context.accountId }] }),
    /account_not_authenticated/,
    undefined,
  );
});

test("D1 existence proof requires a byte-exact UUID and aborts before mutation", async () => {
  for (const listed of [[], [{ uuid: d1Id.toUpperCase() }]]) {
    const runner = new FakeRunner({ d1: { status: 0, stdout: JSON.stringify(listed) } });
    await assert.rejects(executeApply(context, runner), /d1_missing/);
    assert.equal(runner.calls.at(-1).step, "d1");
    assert.equal(runner.calls.some((call) =>
      call.step.startsWith("private_worker:") || call.step.startsWith("public_worker:") || call.step === "migrations"
    ), false);
  }
});

test("pre-existing unowned target fails before mutation", async () => {
  const runner = new FakeRunner({ "target:private": { status: 0, stdout: JSON.stringify([cloudflareDeployment(deploymentIds.private, versionIds.private)]) } });
  await assert.rejects(executeApply(context, runner), /target_private_unowned/);
  assert.equal(runner.calls.some((call) => call.step === "migrations"), false);
});
test("pre-existing owned update is never deleted after later failure", async () => {
  const owned = ownedContext("private");
  const runner = new FakeRunner({ "target:private": live("private"), deploy_public: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(owned, runner), /public_worker_deploy_failed/);
  assert.equal(runner.calls.some((call) => call.step === "cleanup_private"), false);
});
test("newly created fence-aware private resource is preserved after migrated public failure", async () => {
  const runner = new FakeRunner({ deploy_public: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(context, runner), /public_worker_deploy_failed/);
  assert.deepEqual(runner.calls.filter((call) => call.step.startsWith("cleanup_")).map((call) => call.step), []);
});
test("post-migration failure deletes only the created public facade and requires a forward fix", async () => {
  const runner = new FakeRunner({ "readiness:/ready": { status: 1, stdout: "" } });
  let failure;
  await assert.rejects(executeApply(context, runner), (error) => {
    failure = error;
    return /readiness:\/ready_failed/.test(error.message);
  });
  assert.equal(failure.evidence.migration_state, "forward_fix_required");
  assert.deepEqual(failure.evidence.cleanup[0], {
    resource: context.publicName,
    delete_exit_status: 0,
    post_delete_state: "absent",
    status: "deleted",
  });
  assert.deepEqual(runner.calls.filter((call) => call.command.includes("delete")).map((call) => call.command), [
    ["npx", "wrangler", "delete", "--name", context.publicName, "--force"],
  ]);
});
test("apply rollback trusts verified absence after Wrangler KV auth failure", async () => {
  const runner = new FakeRunner({
    "readiness:/ready": { status: 1, stdout: "" },
    cleanup_public: { status: 1, stdout: "", stderr: kvNamespaceDeleteError },
    "cleanup_public:verify": { status: 0, stdout: "[]" },
  });
  let failure;
  await assert.rejects(executeApply(context, runner), (error) => {
    failure = error;
    return error.message === "readiness:/ready_failed";
  });
  assert.deepEqual(failure.evidence.cleanup[0], {
    resource: context.publicName, delete_exit_status: 1, post_delete_state: "absent", status: "deleted",
  });
  assert.equal(runner.calls.filter((call) => call.command[2] === "delete").length, 1);
  assert.equal(runner.calls.filter((call) => call.command[2] === "delete")[0].step, "cleanup_public");
});
test("immutable dependency deployment mismatch fails before mutation", async () => {
  const runner = new FakeRunner({ "dependency:GOOGLE_PROVIDER_SERVICE": { status: 0, stdout: JSON.stringify([
    cloudflareDeployment("99999999-9999-4999-8999-999999999999", versionIds.provider),
  ]) } });
  await assert.rejects(executeApply(context, runner), /dependency_pin_mismatch/);
  assert.equal(runner.calls.some((call) => call.step === "migrations"), false);
});
test("missing secret and migration failures stop deploy", async () => {
  const missing = new FakeRunner({ "private_worker:secrets": { status: 0, stdout: "[]" } });
  await assert.rejects(executeApply(context, missing), /private_worker_secret_verification_failed/);
  const migration = new FakeRunner({ migrations: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(context, migration), /migrations_failed/);
  assert.equal(migration.calls.some((call) => call.step === "private_worker:deploy"), true);
  assert.equal(migration.calls.some((call) => call.step === "cleanup_private"), false);
});
test("broker-private remains unreachable and is deleted if secret installation fails before migration", async () => {
  const firstSecret = REQUIRED_SECRET_NAMES[0];
  const runner = new FakeRunner({ [`private_worker:secret:${firstSecret}`]: { status: 1, stdout: "", stderr: "redacted" } });
  await assert.rejects(executeApply(context, runner), /private_worker_secret_install_failed/);
  const config = JSON.parse((await readFile(resolve(new URL("../../wrangler.jsonc", import.meta.url).pathname), "utf8")));
  assert.equal(config.workers_dev, false);
  assert.equal(Object.hasOwn(config, "routes"), false);
  assert.equal(runner.calls.some((call) => call.step === "migrations" || call.step.startsWith("public_worker:")), false);
  assert.deepEqual(runner.calls.filter((call) => call.step.endsWith(":cleanup")).map((call) => call.command), [
    ["npx", "wrangler", "delete", "--name", context.privateName, "--force"],
  ]);
});

test("broker-private deploy installs and verifies secrets before migration", async () => {
  const runner = new FakeRunner();
  await executeApply(context, runner);
  const privateSteps = runner.calls
    .map((call) => call.step)
    .filter((step) => step.startsWith("private_worker:") || step === "migrations");
  assert.deepEqual(privateSteps, [
    "private_worker:absence",
    "private_worker:deploy",
    ...REQUIRED_SECRET_NAMES.map((name) => `private_worker:secret:${name}`),
    "private_worker:secrets",
    "private_worker:capture",
    "migrations",
  ]);
});

test("broker secret values use stdin and never enter argv or evidence", async () => {
  const runner = new FakeRunner();
  const evidence = await executeApply(context, runner);
  const serializedCalls = JSON.stringify(runner.calls);
  const serializedEvidence = JSON.stringify(evidence);
  for (const value of Object.values(context.privateSecrets)) {
    assert.equal(serializedCalls.includes(value), false);
    assert.equal(serializedEvidence.includes(value), false);
  }
  assert.deepEqual(runner.inputs.map(({ step }) => step), REQUIRED_SECRET_NAMES.map((name) => `private_worker:secret:${name}`));
});

test("OAuth callback readiness requires exact safe 400", async () => {
  const runner = new FakeRunner({ "readiness:oauth_callback_400": { status: 0, stdout: "200" } });
  await assert.rejects(executeApply(context, runner), /oauth_callback_status_mismatch/);
  assert.deepEqual(runner.calls.filter((call) => call.command.includes("delete")).map((call) => call.command), [
    ["npx", "wrangler", "delete", "--name", context.publicName, "--force"],
  ]);
});

const brokerPrivateSecrets = Object.fromEntries(REQUIRED_SECRET_NAMES.map((name) => [name, `broker-${name.toLowerCase()}`]));
for (const name of ["BINDING_SIGNING_SEED", "COMMITMENT_KEY", "CUSTODY_ROOT_KEY", "RECEIPT_SIGNING_SEED"]) brokerPrivateSecrets[name] = "2".repeat(64);
brokerPrivateSecrets.GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U = Buffer.alloc(32, 3).toString("base64url");
brokerPrivateSecrets.AUTH_DRIVER_SERVICE_AUTH = "auth-driver-value";
brokerPrivateSecrets.GOOGLE_EGRESS_SERVICE_AUTH = "shared-egress-value";

const privateSecrets = {
  "auth-driver": { AUTH_DRIVER_SERVICE_AUTH: "auth-driver-value" },
  "google-token-egress": {
    GOOGLE_EGRESS_SERVICE_AUTH: "shared-egress-value",
    GOOGLE_OAUTH_CLIENT_ID: "oauth-client-id",
    GOOGLE_OAUTH_CLIENT_SECRET: "oauth-client-secret",
    GOOGLE_TOKEN_RESULT_KEY: "1".repeat(64),
  },
  "google-provider-egress": { GOOGLE_EGRESS_SERVICE_AUTH: "shared-egress-value" },
  "broker-private": brokerPrivateSecrets,
  "broker-public": {},
};

class PrivateWorkerRunner {
  constructor(overrides = {}) { this.overrides = overrides; this.calls = []; this.inputs = []; this.deployed = false; }
  async run(step, command, options = {}) {
    this.calls.push({ step, command });
    if (options.input !== undefined) this.inputs.push({ step, input: options.input });
    if (this.overrides[step] !== undefined) return this.overrides[step];
    if (step.endsWith(":absence")) return { status: 1, stdout: "", stderr: "workers.api.error.script_not_found [code: 10007]" };
    if (step.endsWith(":deploy")) { this.deployed = true; return { status: 0, stdout: "" }; }
    if (step.endsWith(":secrets")) return { status: 0, stdout: JSON.stringify([{ name: "AUTH_DRIVER_SERVICE_AUTH" }]) };
    if (step.endsWith(":capture")) return {
      status: 0,
      stdout: JSON.stringify([cloudflareDeployment(deploymentIds.auth, versionIds.auth)]),
    };
    if (step.endsWith(":verify")) return { status: 0, stdout: "[]" };
    return { status: 0, stdout: "" };
  }
}

async function withSecretsFile(value, callback) {
  const directory = await mkdtemp(join(tmpdir(), "lattice-private-secrets-"));
  const path = join(directory, "secrets.json");
  await writeFile(path, JSON.stringify(value), { mode: 0o600 });
  await chmod(path, 0o600);
  try { return await callback(path, directory); } finally { await rm(directory, { recursive: true, force: true }); }
}

test("deployments list classifies absent, present, and unknown without trusting ambiguous failures", () => {
  assert.equal(classifyDeploymentsListResult({ status: 1, stderr: "workers.api.error.script_not_found (10007)" }).state, "absent");
  assert.equal(classifyDeploymentsListResult({ status: 0, stdout: "[]" }).state, "absent");
  assert.equal(classifyDeploymentsListResult({ status: 0, stdout: '[{"id":"one"}]' }).state, "present");
  assert.equal(classifyDeploymentsListResult({ status: 1, stderr: "authentication failed" }).state, "unknown");
  assert.equal(classifyDeploymentsListResult({ status: 0, stdout: "not-json" }).state, "unknown");
});

test("unknown private target state aborts before every mutation", async () => {
  const runner = new PrivateWorkerRunner({
    "auth_driver:absence": { status: 1, stdout: "", stderr: "ambiguous API failure" },
  });
  await assert.rejects(deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  }), /target_unknown/);
  assert.deepEqual(runner.calls.map((call) => call.step), ["auth_driver:absence"]);
});

test("private Worker apply proves absence, deploys, installs and verifies secrets, then captures", async () => {
  const runner = new PrivateWorkerRunner();
  const evidence = await deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  });
  assert.deepEqual(runner.calls.map((call) => call.step), [
    "auth_driver:absence", "auth_driver:deploy", "auth_driver:secret:AUTH_DRIVER_SERVICE_AUTH",
    "auth_driver:secrets", "auth_driver:capture",
  ]);
  assert.deepEqual(evidence.secrets, [{ name: "AUTH_DRIVER_SERVICE_AUTH", install_status: "installed_and_verified" }]);
});

test("real Cloudflare upload and secret deployments are accepted without source_hash", async () => {
  const upload = cloudflareDeployment(
    "9651fa32-1111-4111-8111-111111111111",
    "dabcfe1d-1111-4111-8111-111111111111",
    "2026-07-25T02:35:42Z",
    "upload",
  );
  upload.annotations["workers/message"] = "Automatic deployment on upload.";
  const secret = cloudflareDeployment(
    "721b76ed-2222-4222-8222-222222222222",
    "5aa3982d-2222-4222-8222-222222222222",
    "2026-07-25T02:35:44Z",
    "secret",
  );
  const runner = new PrivateWorkerRunner({
    "auth_driver:capture": { status: 0, stdout: JSON.stringify([upload, secret]) },
  });
  const evidence = await deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  });
  assert.equal(evidence.deployment_id, secret.id);
  assert.equal(evidence.version_id, secret.versions[0].version_id);
  assert.equal(evidence.deployment_count, 2);
  assert.deepEqual(evidence.triggered_by_annotations.map((entry) => entry.triggered_by), ["upload", "secret"]);
  assert.equal(evidence.uploaded_source_sha256, hash);
  assert.equal(Object.hasOwn(evidence, "source_hash"), false);
});

test("newest deployment is selected when Cloudflare entries are out of order", async () => {
  const newest = cloudflareDeployment(
    "77777777-7777-4777-8777-777777777777",
    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    "2026-07-25T02:35:49Z",
    "secret",
  );
  const runner = new PrivateWorkerRunner({
    "auth_driver:capture": { status: 0, stdout: JSON.stringify([
      cloudflareDeployment(deploymentIds.auth, versionIds.auth, "2026-07-25T02:35:42Z"),
      newest,
      cloudflareDeployment(deploymentIds.private, versionIds.private, "2026-07-25T02:35:45Z", "secret"),
    ]) },
  });
  const evidence = await deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  });
  assert.equal(evidence.deployment_id, newest.id);
  assert.equal(evidence.version_id, newest.versions[0].version_id);
});

test("invalid deployment identity, stale creation, and non-wrangler source are rejected", async () => {
  const invalidFixtures = [
    { ...cloudflareDeployment(deploymentIds.auth, versionIds.auth), id: "not-a-uuid" },
    cloudflareDeployment(deploymentIds.auth, "not-a-uuid"),
    cloudflareDeployment(deploymentIds.auth, versionIds.auth, "2026-07-25T02:35:39Z"),
    { ...cloudflareDeployment(deploymentIds.auth, versionIds.auth), source: "api" },
  ];
  for (const fixture of invalidFixtures) {
    const runner = new PrivateWorkerRunner({
      "auth_driver:capture": { status: 0, stdout: JSON.stringify([fixture]) },
    });
    await assert.rejects(deployPrivateWorker({
      runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
      config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
      uploadedSourceSha256: hash, runStartedAt,
    }), /deployment_evidence_invalid/);
    assert.equal(runner.calls.some((call) => call.step === "auth_driver:cleanup"), false);
    assert.equal(runner.calls.some((call) => call.step === "migrations"), false);
    assert.equal(runner.calls.at(-1).step, "auth_driver:capture");
  }
});

test("operator-computed uploaded source digests are threaded into rendered config", () => {
  const template = [
    "REPLACE_WITH_AUTH_DRIVER_SOURCE_SHA256",
    "REPLACE_WITH_GOOGLE_TOKEN_SOURCE_SHA256",
    "REPLACE_WITH_GOOGLE_PROVIDER_SOURCE_SHA256",
  ].join("|");
  const rendered = renderWorkerUploadedSourceDigests(template, context.approvedDependencies.services);
  assert.equal(rendered, [`sha256:${hash}`, `sha256:${hash}`, `sha256:${hash}`].join("|"));
  assert.throws(() => renderWorkerUploadedSourceDigests(template, {
    ...context.approvedDependencies.services,
    AUTH_DRIVER_SERVICE: {
      ...context.approvedDependencies.services.AUTH_DRIVER_SERVICE,
      uploaded_source_sha256: "A".repeat(64),
    },
  }), /uploaded source digest invalid/);
});

test("uploaded source digest is required lowercase SHA-256 and is recorded", async () => {
  for (const invalid of [undefined, "a".repeat(63), "A".repeat(64)]) {
    const runner = new PrivateWorkerRunner();
    await assert.rejects(deployPrivateWorker({
      runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
      config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"], uploadedSourceSha256: invalid,
    }), /uploaded_source_sha256_invalid/);
    assert.equal(runner.calls.length, 0);
  }
  const runner = new PrivateWorkerRunner();
  const evidence = await deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  });
  assert.equal(evidence.uploaded_source_sha256, hash);
});

test("nonzero delete with verified absence is successful rollback and preserves the original error", async () => {
  const runner = new PrivateWorkerRunner({
    "auth_driver:secret:AUTH_DRIVER_SERVICE_AUTH": { status: 1, stdout: "", stderr: "redacted" },
    "auth_driver:cleanup": { status: 1, stdout: "", stderr: kvNamespaceDeleteError },
    "auth_driver:cleanup:verify": { status: 0, stdout: "[]", stderr: "" },
  });
  let failure;
  await assert.rejects(deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  }), (error) => {
    failure = error;
    return error.message.startsWith("auth_driver_secret_install_failed:AUTH_DRIVER_SERVICE_AUTH");
  });
  assert.deepEqual(failure.cleanupEvidence, {
    resource: "fresh-auth-driver", delete_exit_status: 1, post_delete_state: "absent", status: "deleted",
  });
  assert.equal(runner.calls.filter((call) => call.command[2] === "delete").length, 1);
  assert.equal(runner.calls.at(-1).step, "auth_driver:cleanup:verify");
});

test("zero delete with target still present reports cleanup failure", async () => {
  const runner = new PrivateWorkerRunner({
    "auth_driver:secret:AUTH_DRIVER_SERVICE_AUTH": { status: 1, stdout: "", stderr: "redacted" },
    "auth_driver:cleanup": { status: 0, stdout: "", stderr: "" },
    "auth_driver:cleanup:verify": { status: 0, stdout: JSON.stringify([
      cloudflareDeployment(deploymentIds.auth, versionIds.auth),
    ]) },
  });
  let failure;
  await assert.rejects(deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  }), (error) => {
    failure = error;
    return error.message.startsWith("auth_driver_secret_install_failed:AUTH_DRIVER_SERVICE_AUTH")
      && error.message.endsWith("; auth_driver_cleanup_failed");
  });
  assert.equal(failure.cleanupEvidence.post_delete_state, "present");
  assert.equal(failure.cleanupEvidence.delete_exit_status, 0);
  assert.equal(runner.calls.filter((call) => call.command[2] === "delete").length, 1);
  assert.equal(runner.calls.at(-1).step, "auth_driver:cleanup:verify");
});

test("nonzero delete with unknown post-check fails closed without extra deletion", async () => {
  const runner = new PrivateWorkerRunner({
    "auth_driver:secret:AUTH_DRIVER_SERVICE_AUTH": { status: 1, stdout: "", stderr: "redacted" },
    "auth_driver:cleanup": { status: 1, stdout: "", stderr: kvNamespaceDeleteError },
    "auth_driver:cleanup:verify": { status: 1, stdout: "", stderr: kvNamespaceDeleteError },
  });
  let failure;
  await assert.rejects(deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  }), (error) => {
    failure = error;
    return error.message.startsWith("auth_driver_secret_install_failed:AUTH_DRIVER_SERVICE_AUTH")
      && error.message.endsWith("; auth_driver_cleanup_failed");
  });
  assert.equal(failure.cleanupEvidence.post_delete_state, "unknown");
  assert.equal(failure.cleanupEvidence.status, "cleanup_failed");
  assert.equal(runner.calls.filter((call) => call.command[2] === "delete").length, 1);
  assert.equal(runner.calls.at(-1).step, "auth_driver:cleanup:verify");
});

test("missing or extra secrets-file names abort during local validation", async () => {
  for (const invalid of [
    { ...privateSecrets, "auth-driver": {} },
    { ...privateSecrets, "auth-driver": { ...privateSecrets["auth-driver"], EXTRA: "not-allowed" } },
  ]) {
    await withSecretsFile(invalid, async (path) => {
      await assert.rejects(loadPrivateWorkerSecrets(path, resolve(new URL("../../../..", import.meta.url).pathname)), /secrets_file_names_invalid/);
    });
  }
});

test("malformed GOOGLE_TOKEN_RESULT_KEY aborts during local validation", async () => {
  const invalid = structuredClone(privateSecrets);
  invalid["google-token-egress"].GOOGLE_TOKEN_RESULT_KEY = "A".repeat(64);
  await withSecretsFile(invalid, async (path) => {
    await assert.rejects(loadPrivateWorkerSecrets(path, resolve(new URL("../../../..", import.meta.url).pathname)), /secrets_file_value_invalid/);
  });
});

test("malformed broker key shapes and mismatched shared auth abort locally", async () => {
  const invalidValues = [];
  const malformedHex = structuredClone(privateSecrets);
  malformedHex["broker-private"].CUSTODY_ROOT_KEY = "F".repeat(64);
  invalidValues.push(malformedHex);
  const malformedB64u = structuredClone(privateSecrets);
  malformedB64u["broker-private"].GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U = "not-a-32-byte-key";
  invalidValues.push(malformedB64u);
  const mismatchedAuth = structuredClone(privateSecrets);
  mismatchedAuth["broker-private"].AUTH_DRIVER_SERVICE_AUTH = "different";
  invalidValues.push(mismatchedAuth);
  for (const invalid of invalidValues) {
    await withSecretsFile(invalid, async (path) => {
      await assert.rejects(loadPrivateWorkerSecrets(path, resolve(new URL("../../../..", import.meta.url).pathname)), /secrets_file_value_invalid/);
    });
  }
});

test("secret-install failure deletes exactly the Worker created by this run", async () => {
  const runner = new PrivateWorkerRunner({
    "auth_driver:secret:AUTH_DRIVER_SERVICE_AUTH": { status: 1, stdout: "", stderr: "redacted" },
  });
  await assert.rejects(deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  }), /secret_install_failed/);
  assert.deepEqual(runner.calls.filter((call) => call.step.endsWith(":cleanup")).map((call) => call.command), [
    ["npx", "wrangler", "delete", "--name", "fresh-auth-driver", "--force"],
  ]);
});

test("secret values travel only over stdin and never appear in argv or evidence", async () => {
  const runner = new PrivateWorkerRunner();
  const evidence = await deployPrivateWorker({
    runner, step: "auth_driver", name: "fresh-auth-driver", accountId: context.accountId,
    config: "auth-driver.jsonc", secrets: privateSecrets["auth-driver"],
    uploadedSourceSha256: hash, runStartedAt,
  });
  const secretValue = privateSecrets["auth-driver"].AUTH_DRIVER_SERVICE_AUTH;
  assert.equal(JSON.stringify(runner.calls).includes(secretValue), false);
  assert.equal(JSON.stringify(evidence).includes(secretValue), false);
  assert.deepEqual(runner.inputs, [{ step: "auth_driver:secret:AUTH_DRIVER_SERVICE_AUTH", input: `${secretValue}\n` }]);
  await withSecretsFile(privateSecrets, async (path, directory) => {
    const evidencePath = join(directory, "evidence.json");
    await writeFile(evidencePath, JSON.stringify(evidence), { mode: 0o600 });
    assert.equal((await readFile(evidencePath, "utf8")).includes(secretValue), false);
  });
});
