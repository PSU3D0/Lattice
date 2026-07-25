import test from "node:test";
import assert from "node:assert/strict";
import { executeStandaloneCleanup, manifestHash } from "./cleanup-lib.mjs";

const account = "a".repeat(32);
const prefix = "lattice-b5-test123";
const source = "b".repeat(64);
const kvNamespaceDeleteError = `✘ [ERROR] A request to the Cloudflare API (/accounts/<acct>/storage/kv/namespaces) failed.
Authentication error [code: 10000]`;
function worker(kind) {
  const name = `${prefix}-broker-${kind}`;
  const deployment_id = kind === "private"
    ? "11111111-1111-4111-8111-111111111111"
    : "22222222-2222-4222-8222-222222222222";
  const version_id = kind === "private"
    ? "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
    : "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2";
  const ownership = {
    owner: "lattice-broker-b5", account_id: account, prefix,
    worker_name: name, deployment_id, version_id, uploaded_source_sha256: source,
  };
  return {
    kind, name, account_id: account, prefix, created_by_run: true,
    deployment_id, version_id, uploaded_source_sha256: source, ownership,
    ownership_manifest_hash: manifestHash(ownership),
  };
}
const state = {
  schema_version: "0.3", owner: "lattice-broker-b5", account_id: account, prefix,
  resources: { workers: [worker("private"), worker("public")], d1_database: { owned: false } },
};
class Runner {
  constructor(overrides = {}) { this.overrides = overrides; this.calls = []; }
  async run(step) {
    this.calls.push(step);
    if (this.overrides[step]) return this.overrides[step];
    const kind = step.split(":").at(-1);
    const pin = state.resources.workers.find((value) => value.kind === kind);
    if (step.startsWith("cleanup:deployments:")) return { status: 0, stdout: JSON.stringify([{
      id: pin.deployment_id,
      source: "wrangler",
      versions: [{ version_id: pin.version_id, percentage: 100 }],
      created_on: "2026-07-25T02:35:42Z",
    }]) };
    if (step.startsWith("cleanup:verify:")) return { status: 0, stdout: "[]" };
    return { status: 0, stdout: "{}" };
  }
}

test("standalone cleanup refuses pre-existing unowned state before queries", async () => {
  const forged = structuredClone(state);
  forged.resources.workers[0].created_by_run = false;
  const runner = new Runner();
  await assert.rejects(executeStandaloneCleanup(forged, runner), /ownership_manifest_invalid/);
  assert.equal(runner.calls.length, 0);
});
test("standalone cleanup refuses immutable deployment identity mismatch without deleting", async () => {
  const runner = new Runner({
    "cleanup:deployments:private": { status: 0, stdout: JSON.stringify([{
      id: "99999999-9999-4999-8999-999999999999",
      source: "wrangler",
      versions: [{ version_id: state.resources.workers[0].version_id, percentage: 100 }],
      created_on: "2026-07-25T02:35:42Z",
    }]) },
  });
  await assert.rejects(executeStandaloneCleanup(state, runner), /deployment_mismatch/);
  assert.equal(runner.calls.some((step) => step.startsWith("cleanup:delete:")), false);
});
test("standalone cleanup refuses missing live deployment without deleting", async () => {
  const runner = new Runner({ "cleanup:deployments:private": { status: 0, stdout: "[]" } });
  await assert.rejects(executeStandaloneCleanup(state, runner), /missing_deployment/);
  assert.equal(runner.calls.some((step) => step.startsWith("cleanup:delete:")), false);
});
test("standalone cleanup refuses local ownership digest mismatch without querying", async () => {
  const forged = structuredClone(state);
  forged.resources.workers[0].ownership.uploaded_source_sha256 = "c".repeat(64);
  const runner = new Runner();
  await assert.rejects(executeStandaloneCleanup(forged, runner), /ownership_manifest_invalid/);
  assert.equal(runner.calls.length, 0);
});
test("standalone cleanup accepts nonzero delete only after verified absence", async () => {
  const publicName = `${prefix}-broker-public`;
  const runner = new Runner({
    [`cleanup:delete:${publicName}`]: { status: 1, stdout: "", stderr: kvNamespaceDeleteError },
    [`cleanup:verify:${publicName}`]: { status: 0, stdout: "[]" },
  });
  const evidence = await executeStandaloneCleanup(state, runner);
  assert.equal(evidence.deleted.includes(publicName), true);
  assert.deepEqual(evidence.cleanup[0], {
    resource: publicName, delete_exit_status: 1, post_delete_state: "absent", status: "deleted",
  });
  assert.equal(runner.calls.filter((step) => step === `cleanup:delete:${publicName}`).length, 1);
});
test("standalone cleanup rejects zero delete when target remains present and stops mutations", async () => {
  const publicWorker = state.resources.workers.find(({ kind }) => kind === "public");
  const publicName = publicWorker.name;
  const runner = new Runner({
    [`cleanup:verify:${publicName}`]: { status: 0, stdout: JSON.stringify([{
      id: publicWorker.deployment_id,
      source: "wrangler",
      versions: [{ version_id: publicWorker.version_id, percentage: 100 }],
      created_on: "2026-07-25T02:35:42Z",
    }]) },
  });
  let failure;
  await assert.rejects(executeStandaloneCleanup(state, runner), (error) => {
    failure = error;
    return error.message === `cleanup_delete_failed:${publicName}`;
  });
  assert.equal(failure.evidence.cleanup[0].post_delete_state, "present");
  assert.deepEqual(runner.calls.filter((step) => step.startsWith("cleanup:delete:")), [`cleanup:delete:${publicName}`]);
});
test("standalone cleanup rejects unknown post-delete state and stops mutations", async () => {
  const publicName = `${prefix}-broker-public`;
  const runner = new Runner({
    [`cleanup:delete:${publicName}`]: { status: 1, stdout: "", stderr: kvNamespaceDeleteError },
    [`cleanup:verify:${publicName}`]: { status: 1, stdout: "", stderr: kvNamespaceDeleteError },
  });
  let failure;
  await assert.rejects(executeStandaloneCleanup(state, runner), (error) => {
    failure = error;
    return error.message === `cleanup_delete_failed:${publicName}`;
  });
  assert.equal(failure.evidence.cleanup[0].post_delete_state, "unknown");
  assert.deepEqual(runner.calls.filter((step) => step.startsWith("cleanup:delete:")), [`cleanup:delete:${publicName}`]);
});
test("standalone cleanup deletes only both exact live-verified resources", async () => {
  const runner = new Runner();
  const evidence = await executeStandaloneCleanup(state, runner);
  assert.deepEqual(evidence.deleted, [`${prefix}-broker-public`, `${prefix}-broker-private`]);
  assert.deepEqual(evidence.cleanup.map(({ delete_exit_status, post_delete_state, status }) => ({
    delete_exit_status, post_delete_state, status,
  })), [
    { delete_exit_status: 0, post_delete_state: "absent", status: "deleted" },
    { delete_exit_status: 0, post_delete_state: "absent", status: "deleted" },
  ]);
  assert.deepEqual(runner.calls.filter((step) => step.startsWith("cleanup:delete:")), [
    `cleanup:delete:${prefix}-broker-public`, `cleanup:delete:${prefix}-broker-private`,
  ]);
  assert.deepEqual(runner.calls.filter((step) => step.startsWith("cleanup:verify:")), [
    `cleanup:verify:${prefix}-broker-public`, `cleanup:verify:${prefix}-broker-private`,
  ]);
});
