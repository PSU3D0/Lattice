import test from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { chmod, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { verifyBundle } from "./operator-artifacts.mjs";
import { parsePredicates } from "./operator-input.mjs";

const generator = new URL("./operator-input.mjs", import.meta.url).pathname;
const artifactsTool = new URL("./operator-artifacts.mjs", import.meta.url).pathname;
const digest = (character) => character.repeat(64);
const timestamp = (milliseconds) => new Date(Math.floor(milliseconds / 1000) * 1000).toISOString().replace(".000Z", "Z");
const validPredicates = JSON.stringify([{
  kind: "brokered_count",
  predicate_id: "durable-budget-and-dispatch",
  required_kernel_controls: ["durable_before_dispatch", "exact_redelivery", "pop_bound"],
}]);

function baseArguments(directory, overrides = {}) {
  const now = Date.now();
  const values = {
    "--org-id": "org-disposable",
    "--deployment-id": "deployment-disposable-1",
    "--prefix": "lattice-c5-test123",
    "--not-before": timestamp(now - 60_000),
    "--expires-at": timestamp(now + 86_400_000),
    "--spend-limit-usd": "5.00",
    "--rate-limit-per-minute": "12",
    "--required-assurance-predicates": validPredicates,
    "--activation-recipient-key-id": "activation-disposable-1",
    "--activation-recipient-public-key-b64u": Buffer.alloc(32, 9).toString("base64url"),
    "--key-id": "operator-disposable-1",
    "--broker-wasm-sha256": digest("1"),
    "--auth-driver-sha256": digest("2"),
    "--google-token-sha256": digest("3"),
    "--google-provider-sha256": digest("4"),
    "--output": join(directory, "operator-artifact-input.json"),
    ...overrides,
  };
  return Object.entries(values).flat();
}

function run(script, args) {
  return spawnSync(process.execPath, [script, ...args], { encoding: "utf8" });
}

test("generator derives, signs, and passes JS plus Rust bundle verification", async () => {
  const directory = await mkdtemp(join(tmpdir(), "operator-input-"));
  const configPath = join(directory, "operator-artifact-input.json");
  const bundlePath = join(directory, "operator-artifact-bundle.json");
  const trustPath = join(directory, "operator-trust-root.json");
  const keyPath = join(directory, "operator.pem");
  const generatedKey = generateKeyPairSync("ed25519");
  await writeFile(keyPath, generatedKey.privateKey.export({ format: "pem", type: "pkcs8" }), { mode: 0o600 });
  await chmod(keyPath, 0o600);

  const generated = run(generator, baseArguments(directory));
  assert.equal(generated.status, 0, generated.stderr);
  assert.equal((await stat(configPath)).mode & 0o777, 0o600);
  const config = JSON.parse(await readFile(configPath, "utf8"));
  assert.equal(config.artifacts.historical_inventory[0].value.items.length, 0);
  const contracts = config.artifacts.deployment_contract_set[0].value.contracts;
  assert.deepEqual(contracts.map(({ contract_id }) => contract_id).sort(), [
    "connector.google.gmail.send_message@1",
    "connector.google.sheets.append_row@1",
    "connector.google.sheets.create_spreadsheet@1",
  ]);
  // Each planner/projector must support EXACTLY its own operation. Granting a
  // family-wide set let the append_row planner claim create_spreadsheet.
  const hashFor = (id) => contracts.find(({ contract_id }) => contract_id === id).contract_hash;
  const expectedByEntry = {
    "google.sheets.append_row.v1": hashFor("connector.google.sheets.append_row@1"),
    "projector.connector.google.sheets.append_row@1": hashFor("connector.google.sheets.append_row@1"),
    "google.sheets.create_spreadsheet.v1": hashFor("connector.google.sheets.create_spreadsheet@1"),
    "projector.connector.google.sheets.create_spreadsheet@1": hashFor("connector.google.sheets.create_spreadsheet@1"),
    "google.gmail.rfc822_message.v1": hashFor("connector.google.gmail.send_message@1"),
    "projector.connector.google.gmail.send_message@1": hashFor("connector.google.gmail.send_message@1"),
  };
  const implementations = config.artifacts.registry_definitions
    .map(({ value }) => value)
    .filter(({ class: kind }) => ["capsule_planner", "response_projector"].includes(kind));
  assert.equal(implementations.length, 6);
  for (const implementation of implementations) {
    assert.deepEqual(implementation.class_payload.supported_contract_hashes,
      [expectedByEntry[implementation.entry_ref]],
      `wrong contracts for ${implementation.entry_ref}`);
  }
  assert.equal(config.artifacts.registry_definitions.some(({ value }) => value.class_payload?.implementation_digest === `sha256:${digest("1")}`), true);
  assert.equal(config.artifacts.registry_definitions.some(({ value }) => value.class_payload?.implementation_digest === `sha256:${digest("2")}`), true);
  assert.equal(config.artifacts.registry_definitions.some(({ value }) => value.class_payload?.implementation_digest === `sha256:${digest("3")}`), true);
  assert.equal(config.artifacts.registry_definitions.some(({ value }) => value.class_payload?.implementation_digest === `sha256:${digest("4")}`), true);

  const built = run(artifactsTool, ["build", "--config", configPath, "--key-file", keyPath, "--output", bundlePath]);
  assert.equal(built.status, 0, built.stderr);
  assert.equal((await stat(bundlePath)).mode & 0o777, 0o600);
  const bundle = JSON.parse(await readFile(bundlePath, "utf8"));
  const trust = { key_id: bundle.key_id, public_key_b64u: bundle.public_key_b64u };
  await writeFile(trustPath, JSON.stringify(trust), { mode: 0o600 });
  assert.equal(verifyBundle(bundle, trust), true);
  assert.equal(JSON.parse(bundle.artifacts.deployment_standing_authority[0].canonical_jcs).contract_set_hash, bundle.artifacts.deployment_contract_set[0].hash);

  const verified = run(artifactsTool, ["verify", "--bundle", bundlePath, "--trust-root", trustPath]);
  assert.equal(verified.status, 0, verified.stderr);
  assert.match(verified.stdout, /"bundle_hash":"sha256:[0-9a-f]{64}"/);

  const malformedDigest = run(generator, baseArguments(directory, { "--broker-wasm-sha256": digest("A") }));
  assert.notEqual(malformedDigest.status, 0);
  assert.match(malformedDigest.stderr, /64 lowercase hexadecimal/);

  const first = { kind: "brokered_count", predicate_id: "z-last", required_kernel_controls: ["control"] };
  const second = { kind: "brokered_count", predicate_id: "a-first", required_kernel_controls: ["control"] };
  const unsorted = run(generator, baseArguments(directory, { "--required-assurance-predicates": JSON.stringify([first, second]) }));
  assert.notEqual(unsorted.status, 0);
  assert.match(unsorted.stderr, /JCS-lexically sorted/);
  const duplicate = run(generator, baseArguments(directory, { "--required-assurance-predicates": JSON.stringify([first, first]) }));
  assert.notEqual(duplicate.status, 0);
  assert.match(duplicate.stderr, /unique/);

  const expired = run(generator, baseArguments(directory, {
    "--not-before": "2020-01-01T00:00:00Z",
    "--expires-at": "2020-01-02T00:00:00Z",
  }));
  assert.notEqual(expired.status, 0);
  assert.match(expired.stderr, /expired/);

  config.artifacts.deployment_standing_authority[0].value.contract_set_hash = `sha256:${digest("f")}`;
  await writeFile(configPath, JSON.stringify(config), { mode: 0o600 });
  const mismatched = run(artifactsTool, ["build", "--config", configPath, "--key-file", keyPath, "--output", join(directory, "mismatch.json")]);
  assert.notEqual(mismatched.status, 0);
  assert.match(mismatched.stderr, /contract_set_hash mismatch/);
});

test("rejects required kernel controls the broker does not implement", () => {
  // Regression: a standing authority demanding controls outside the broker's
  // advertised set deploys cleanly and then fails every binding with Brk108.
  const predicates = JSON.stringify([{
    kind: "brokered_count",
    predicate_id: "durable-budget-and-dispatch",
    required_kernel_controls: ["durable_budget_ledger", "persisted_dispatch_boundary"],
  }]);
  assert.throws(() => parsePredicates(predicates), /unsupported required kernel controls/);
});

test("accepts the exact control set the broker implements", () => {
  const predicates = JSON.stringify([{
    kind: "brokered_count",
    predicate_id: "brokered-count-v2",
    required_kernel_controls: ["durable_before_dispatch", "exact_redelivery", "pop_bound"],
  }]);
  assert.deepEqual(parsePredicates(predicates)[0].required_kernel_controls,
    ["durable_before_dispatch", "exact_redelivery", "pop_bound"]);
});
