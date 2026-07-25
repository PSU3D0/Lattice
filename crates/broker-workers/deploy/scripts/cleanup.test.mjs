import test from "node:test";
import assert from "node:assert/strict";
import { executeStandaloneCleanup, manifestHash } from "./cleanup-lib.mjs";

const account = "a".repeat(32);
const prefix = "lattice-b5-test123";
const source = "b".repeat(64);
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
test("standalone cleanup deletes only both exact live-verified resources", async () => {
  const runner = new Runner();
  const evidence = await executeStandaloneCleanup(state, runner);
  assert.deepEqual(evidence.deleted, [`${prefix}-broker-public`, `${prefix}-broker-private`]);
  assert.deepEqual(runner.calls.filter((step) => step.startsWith("cleanup:delete:")), [
    `cleanup:delete:${prefix}-broker-public`, `cleanup:delete:${prefix}-broker-private`,
  ]);
});
