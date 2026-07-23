import test from "node:test";
import assert from "node:assert/strict";
import { executeApply, redactedPlan, REQUIRED_SECRET_NAMES } from "./deploy-lib.mjs";

const hash = "c".repeat(64);
const context = {
  accountId: "a".repeat(32), prefix: "lattice-b5-test123", d1Id: "b".repeat(32),
  d1Name: "lattice-b5-test123-broker",
  privateName: "lattice-b5-test123-broker-private",
  publicName: "lattice-b5-test123-broker-public",
  publicCallbackBase: "https://lattice-b5-test123-broker-public.workers.dev",
  authDriverService: "approved-auth-driver",
  googleProviderService: "approved-google-provider",
  googleTokenService: "approved-google-token",
  approvedDependencies: {
    schema_version: "2", account_id: "a".repeat(32), prefix: "lattice-b5-test123",
    d1_database_id: "b".repeat(32),
    artifacts: {
      deployment_authority_key_id: "operator-key", deployment_authority_public_key_b64u: "operator-public-key",
      deployment_contract_set_jcs: "{}", deployment_standing_authority_jcs: "{}",
      historical_archive_authority_key_id: "archive-key", historical_archive_authority_public_key_b64u: "archive-public-key",
      legacy_cutover_authority_key_id: "cutover-key", legacy_cutover_authority_public_key_b64u: "cutover-public-key",
      generic_profile_authority_public_key_b64u: "profile-public-key", generic_profile_registry_jcs: "{}",
      generic_activation_recipient_key_id: "recipient-key",
    },
    services: {
      AUTH_DRIVER_SERVICE: { name: "approved-auth-driver", account_id: "a".repeat(32), deployment_id: "auth-driver-deployment", source_hash: hash },
      GOOGLE_PROVIDER_SERVICE: { name: "approved-google-provider", account_id: "a".repeat(32), deployment_id: "provider-deployment", source_hash: hash },
      GOOGLE_TOKEN_SERVICE: { name: "approved-google-token", account_id: "a".repeat(32), deployment_id: "token-deployment", source_hash: hash },
    },
    workers: {},
  },
  privateConfig: "private-redacted", publicConfig: "public-redacted",
  privateConfigPath: "/evidence/private.jsonc", publicConfigPath: "/evidence/public.jsonc",
};

class FakeRunner {
  constructor(overrides = {}) { this.overrides = overrides; this.calls = []; }
  async run(step, command) {
    this.calls.push({ step, command });
    if (this.overrides[step] !== undefined) return this.overrides[step];
    if (step === "auth") return { status: 0, stdout: JSON.stringify({ account_id: context.accountId }) };
    if (step.startsWith("target:")) return { status: 0, stdout: "[]" };
    if (step === "dependency:AUTH_DRIVER_SERVICE") return { status: 0, stdout: JSON.stringify([{ id: "auth-driver-deployment", source_hash: hash }]) };
    if (step === "dependency:GOOGLE_PROVIDER_SERVICE") return { status: 0, stdout: JSON.stringify([{ id: "provider-deployment", source_hash: hash }]) };
    if (step === "dependency:GOOGLE_TOKEN_SERVICE") return { status: 0, stdout: JSON.stringify([{ id: "token-deployment", source_hash: hash }]) };
    if (step === "d1") return { status: 0, stdout: JSON.stringify([{ uuid: context.d1Id }]) };
    if (step === "secrets") return { status: 0, stdout: JSON.stringify(REQUIRED_SECRET_NAMES.map((name) => ({ name }))) };
    if (step === "readiness:oauth_callback_400") return { status: 0, stdout: "400" };
    if (step === "smoke") return { status: 0, stdout: JSON.stringify({ status: "ok" }) };
    return { status: 0, stdout: "{}" };
  }
}

function ownedContext(kind) {
  const name = kind === "private" ? context.privateName : context.publicName;
  return {
    ...context,
    approvedDependencies: {
      ...context.approvedDependencies,
      workers: { [kind]: { name, account_id: context.accountId, prefix: context.prefix, deployment_id: `${kind}-deployment`, source_hash: hash } },
    },
  };
}
const live = (kind) => ({ status: 0, stdout: JSON.stringify([{ id: `${kind}-deployment`, source_hash: hash }]) });

test("redacted dry-run performs no command and names the clean preflight", () => {
  const runner = new FakeRunner(); const plan = redactedPlan(context);
  assert.equal(runner.calls.length, 0); assert.deepEqual(plan.required_secret_names, REQUIRED_SECRET_NAMES);
  assert.equal(plan.steps[0], "clean_hermetic_production_preflight");
});
test("hermetic preflight failure occurs before every remote command", async () => {
  const runner = new FakeRunner({ hermetic_preflight: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(context, runner), /hermetic_preflight_failed/);
  assert.deepEqual(runner.calls.map((call) => call.step), ["hermetic_preflight"]);
});
test("pre-existing unowned target fails before mutation", async () => {
  const runner = new FakeRunner({ "target:private": { status: 0, stdout: JSON.stringify([{ id: "foreign", source_hash: hash }]) } });
  await assert.rejects(executeApply(context, runner), /target_private_unowned/);
  assert.equal(runner.calls.some((call) => call.step === "migrations"), false);
});
test("pre-existing owned update is never deleted after later failure", async () => {
  const owned = ownedContext("private");
  const runner = new FakeRunner({ "target:private": live("private"), deploy_public: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(owned, runner), /deploy_public_failed/);
  assert.equal(runner.calls.some((call) => call.step === "cleanup_private"), false);
});
test("newly created fence-aware private resource is preserved after migrated public failure", async () => {
  const runner = new FakeRunner({ deploy_public: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(context, runner), /deploy_public_failed/);
  assert.deepEqual(runner.calls.filter((call) => call.step.startsWith("cleanup_")).map((call) => call.step), []);
});
test("post-migration failure cleans public but preserves the fence authority", async () => {
  const runner = new FakeRunner({ "readiness:/ready": { status: 1, stdout: "" } });
  await assert.rejects(executeApply(context, runner), /readiness:\/ready_failed/);
  assert.deepEqual(runner.calls.filter((call) => call.step.startsWith("cleanup_")).map((call) => call.step), ["cleanup_public"]);
});
test("immutable dependency hash mismatch fails before mutation", async () => {
  const runner = new FakeRunner({ "dependency:GOOGLE_PROVIDER_SERVICE": { status: 0, stdout: JSON.stringify([{ id: "provider-deployment", source_hash: "d".repeat(64) }]) } });
  await assert.rejects(executeApply(context, runner), /dependency_pin_mismatch/);
  assert.equal(runner.calls.some((call) => call.step === "migrations"), false);
});
test("missing secret and migration failures stop deploy", async () => {
  const missing = new FakeRunner({ secrets: { status: 0, stdout: "[]" } });
  await assert.rejects(executeApply(context, missing), /secret_missing/);
  const migration = new FakeRunner({ migrations: { status: 1, stdout: "" } });
  await assert.rejects(executeApply(context, migration), /migrations_failed/);
  assert.equal(migration.calls.some((call) => call.step === "deploy_private"), true);
  assert.equal(migration.calls.some((call) => call.step === "cleanup_private"), false);
});
test("OAuth callback readiness requires exact safe 400", async () => {
  const runner = new FakeRunner({ "readiness:oauth_callback_400": { status: 0, stdout: "200" } });
  await assert.rejects(executeApply(context, runner), /oauth_callback_status_mismatch/);
  assert.deepEqual(runner.calls.filter((call) => call.step.startsWith("cleanup_")).map((call) => call.step), ["cleanup_public"]);
});
