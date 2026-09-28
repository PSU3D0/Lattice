import { afterAll, expect, it } from "vitest";
import { Miniflare } from "miniflare";
import { createHash } from "node:crypto";

const hash = (value: string) => `sha256:${createHash("sha256").update(value).digest("hex")}`;
const account = `hmac-sha256:${"a".repeat(64)}`;
const actor = `hmac-sha256:${"1".repeat(64)}`;
const input = `hmac-sha256:${"b".repeat(64)}`;
const recordCommitment = `hmac-sha256:${"d".repeat(64)}`;

const mf = new Miniflare({
  workers: [{
    name: "admission-authority-test",
    scriptPath: "./build/index.js",
    compatibilityDate: "2026-07-15",
    modules: true,
    modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true }],
    bindings: {
      ADMISSION_AUTHORITY_KEY_ID: "admission-key",
      ADMISSION_AUTHORITY_SIGNING_SEED: "09".repeat(32),
    },
    durableObjects: {
      ADMISSION_AUTHORITY_DO: {
        className: "AdmissionAuthorityDurableObject",
        useSQLite: true,
      },
    },
  }],
});

afterAll(async () => mf.dispose());

async function command(stub: DurableObjectStub, body: unknown) {
  return stub.fetch("http://admission-authority.internal/", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

it("persists one shared reservation and redelivers it without a production route", async () => {
  const namespace = await mf.getDurableObjectNamespace(
    "ADMISSION_AUTHORITY_DO",
    "admission-authority-test",
  );
  const stub = namespace.get(namespace.idFromName("tenant-a/deployment-a"));
  const artifactHead = (reference: string, epoch: number) => ({
    reference,
    hash: hash(reference),
    epoch,
  });
  const bootstrap = {
    tenant_id: "tenant-a",
    deployment_id: "deployment-a",
    cutover: artifactHead("cutover-1", 1),
    control_epoch: 1,
    heads: {
      standing: artifactHead("standing-1", 1),
      contract_set: artifactHead("contracts-1", 1),
      policy: artifactHead("policy-1", 1),
      registry_vector: artifactHead("registry-1", 1),
    },
    provider_grants: [{
      provider_grant_lineage_ref: "lineage-a",
      provider_grant_version_ref: "grant-a1",
      provider_grant_version_hash: hash("grant-a1"),
      account_subject_commitment: account,
      authority_epoch: 1,
      fence_epoch: 0,
      status: "current",
    }],
    acls: [{
      key: {
        actor_subject_commitment: actor,
        provider_grant_version_ref: "grant-a1",
      },
      account_subject_commitment: account,
      acl_epoch: 1,
      selector_hash: hash("selector"),
      record_commitment: recordCommitment,
      record_hash: hash("acl-record"),
      status: "active",
    }],
    legacy_inventory: {
      inventory_ref: "legacy-1",
      inventory_hash: hash("legacy-1"),
      sealed: true,
    },
    trusted_broker_partitions: ["broker-a", "broker-b"],
    default_ceilings: {
      flow_max: 1,
      node_max: 1,
      connection_lineage_max: 1,
      account_partition_max: 1,
    },
    control_key_id: "control-key",
    control_public_key: [234, 74, 108, 99, 226, 156, 82, 10, 190, 245, 80, 123, 19, 46, 197, 249, 149, 71, 118, 174, 190, 190, 123, 146, 66, 30, 234, 105, 20, 70, 210, 44],
    admission_key_id: "admission-key",
  };
  const initialized = await command(stub, { command: "initialize", body: bootstrap });
  expect(initialized.status, await initialized.clone().text()).toBe(200);

  const context = {
    broker_partition: "broker-a",
    run_id: "run-a",
    node_id: "node-a",
    logical_effect_id: hash("effect-a"),
    canonical_input_commitment: input,
    binding_ref: "binding-a",
    binding_hash: hash("binding-a"),
    provider_grant_version_ref: "grant-a1",
    provider_grant_version_hash: hash("grant-a1"),
    provider_grant_lineage_ref: "lineage-a",
    account_partition: {
      provider: "google",
      auth_profile_ref: "auth.google.workspace.oauth2",
      auth_profile_version: "1",
      account_subject_commitment: account,
    },
    actor_subject_commitment: actor,
    acl_epoch: 1,
    acl_selector_hash: hash("selector"),
    acl_record_commitment: recordCommitment,
    acl_record_hash: hash("acl-record"),
    contract_id: "connector.google.test@1",
    contract_hash: hash("contract"),
    registry_vector_ref: "registry-1",
    registry_vector_hash: hash("registry-1"),
    registry_vector_epoch: 1,
  };
  const reserveBody = {
    command: "reserve_effect",
    body: { expected_state_version: 1, expected_control_epoch: 1, context },
  };
  const first = await command(stub, reserveBody);
  expect(first.status, await first.clone().text()).toBe(200);
  const firstBody = await first.json() as any;
  expect(firstBody.reply.reply).toBe("reservation");
  expect(firstBody.reply.body.redelivery).toBe(false);

  const repeated = await command(stub, reserveBody);
  expect(repeated.status, await repeated.clone().text()).toBe(200);
  const repeatedBody = await repeated.json() as any;
  expect(repeatedBody.reply.reply).toBe("reservation");
  expect(repeatedBody.reply.body.redelivery).toBe(true);
  expect(repeatedBody.reply.body.reservation_hash)
    .toBe(firstBody.reply.body.reservation_hash);

  const read = await command(stub, { command: "read" });
  const readBody = await read.json() as any;
  expect(readBody.reply.reply).toBe("state");
  expect(readBody.reply.body.runs["run-a"].flow.reserved).toBe(1);
});
