import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const workspace = resolve(new URL("../../../..", import.meta.url).pathname);
const packageRoot = resolve(workspace, "broker-workers-package");
const excluded = /(^|\/)(node_modules|target|build-test|build-production|build-production-tmp|workerd-tests)(\/|$)/;
const sentinels = [
  "/__test/", "fixture-pop-public", "fixture-deployment-key-value", "google-semantic-broker-v1",
  "lbk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "fixture-refresh-never-log", "fixture-access-never-log", "x-lattice-test-activation-crash",
  "LOCAL_TEST_MODE",
];

test("production package excludes every test/cache path and sentinel from every file", async () => {
  const manifest = JSON.parse(await readFile(resolve(packageRoot, "build-manifest.json"), "utf8"));
  assert.equal(manifest.files.some((file) => excluded.test(file.path)), false);
  assert.equal(manifest.files.some((file) => file.path.endsWith("/src/wasm/test_fixtures.rs")), false);
  for (const file of manifest.files) {
    const bytes = await readFile(resolve(packageRoot, file.path));
    for (const sentinel of sentinels) assert.equal(bytes.includes(Buffer.from(sentinel)), false, `${file.path}: ${sentinel}`);
  }
});

test("package lock and production WASM are byte/hash identical to repository outputs", async () => {
  const packageLock = await readFile(resolve(packageRoot, "Cargo.lock"));
  const repositoryLock = await readFile(resolve(workspace, "Cargo.lock"));
  assert.equal(packageLock.equals(repositoryLock), true);
  const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
  const packageWasm = await readFile(resolve(packageRoot, "crates/broker-workers/build/index_bg.wasm"));
  const rootWasm = await readFile(resolve(workspace, "crates/broker-workers/build/index_bg.wasm"));
  assert.equal(digest(packageWasm), digest(rootWasm));
});
