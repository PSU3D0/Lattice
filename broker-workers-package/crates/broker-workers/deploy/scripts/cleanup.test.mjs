import test from "node:test";
import assert from "node:assert/strict";
import { executeStandaloneCleanup, manifestHash } from "./cleanup-lib.mjs";

const account = "a".repeat(32);
const prefix = "lattice-b5-test123";
const source = "b".repeat(64);
function worker(kind) {
  const name = `${prefix}-broker-${kind}`;
  const deployment_id = `${kind}-immutable-deployment`;
  const ownership = {
    owner: "lattice-broker-b5", account_id: account, prefix,
    worker_name: name, deployment_id, source_hash: source,
  };
  return {
    kind, name, account_id: account, prefix, created_by_run: true,
    deployment_id, source_hash: source, ownership,
    ownership_manifest_hash: manifestHash(ownership),
  };
}
const state = {
  schema_version: "0.2", owner: "lattice-broker-b5", account_id: account, prefix,
  resources: { workers: [worker("private"), worker("public")], d1_database: { owned: false } },
};
class Runner {
  constructor(overrides = {}) { this.overrides = overrides; this.calls = []; }
  async run(step) {
    this.calls.push(step);
    if (this.overrides[step]) return this.overrides[step];
    const kind = step.split(":").at(-1);
    const pin = state.resources.workers.find((value) => value.kind === kind);
    if (step.startsWith("cleanup:deployments:")) return { status: 0, stdout: JSON.stringify([{ id: pin.deployment_id, source_hash: pin.source_hash }]) };
    if (step.startsWith("cleanup:metadata:")) return { status: 0, stdout: JSON.stringify({ id: pin.deployment_id, account_id: account, source_hash: pin.source_hash, metadata: { source_hash: pin.source_hash, ownership: pin.ownership, ownership_manifest_hash: pin.ownership_manifest_hash } }) };
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
test("standalone cleanup refuses immutable deployment hash mismatch without deleting", async () => {
  const runner = new Runner({
    "cleanup:deployments:private": { status: 0, stdout: JSON.stringify([{ id: "private-immutable-deployment", source_hash: "c".repeat(64) }]) },
  });
  await assert.rejects(executeStandaloneCleanup(state, runner), /deployment_mismatch/);
  assert.equal(runner.calls.some((step) => step.startsWith("cleanup:delete:")), false);
});
test("standalone cleanup refuses missing live metadata without deleting", async () => {
  const runner = new Runner({ "cleanup:metadata:private": { status: 1, stdout: "" } });
  await assert.rejects(executeStandaloneCleanup(state, runner), /cleanup_metadata_private_failed/);
  assert.equal(runner.calls.some((step) => step.startsWith("cleanup:delete:")), false);
});
test("standalone cleanup refuses metadata ownership mismatch without deleting", async () => {
  const pin = state.resources.workers[0];
  const runner = new Runner({
    "cleanup:metadata:private": { status: 0, stdout: JSON.stringify({ id: pin.deployment_id, account_id: account, source_hash: pin.source_hash, metadata: { ownership: { ...pin.ownership, prefix: "foreign" }, ownership_manifest_hash: pin.ownership_manifest_hash } }) },
  });
  await assert.rejects(executeStandaloneCleanup(state, runner), /metadata_mismatch/);
  assert.equal(runner.calls.some((step) => step.startsWith("cleanup:delete:")), false);
});
test("standalone cleanup deletes only both exact live-verified resources", async () => {
  const runner = new Runner();
  const evidence = await executeStandaloneCleanup(state, runner);
  assert.deepEqual(evidence.deleted, [`${prefix}-broker-public`, `${prefix}-broker-private`]);
  assert.deepEqual(runner.calls.filter((step) => step.startsWith("cleanup:delete:")), [
    `cleanup:delete:${prefix}-broker-public`, `cleanup:delete:${prefix}-broker-private`,
  ]);
});
