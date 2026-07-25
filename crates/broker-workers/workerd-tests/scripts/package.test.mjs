import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { resolveProductionWasmRustflags } from "./production-preflight.mjs";

const workspace = resolve(new URL("../../../..", import.meta.url).pathname);
const packageRoot = resolve(workspace, "broker-workers-package");
const excluded = /(^|\/)(node_modules|target|build-test|build-production|build-production-tmp|workerd-tests)(\/|$)/;
const sentinels = [
  "/__test/", "fixture-pop-public", "fixture-deployment-key-value", "google-semantic-broker-v1",
  "lbk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "fixture-refresh-never-log", "fixture-access-never-log", "x-lattice-test-activation-crash",
  "LOCAL_TEST_MODE",
];

test("production preflight and build.sh resolve identical wasm32 rustflags", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lattice-rustflags-"));
  const cargo = join(directory, "cargo");
  await writeFile(cargo, "#!/usr/bin/env bash\nprintf '%s' \"$CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS\"\n");
  await chmod(cargo, 0o755);
  const crate = resolve(workspace, "crates/broker-workers");
  const environment = {
    ...process.env,
    PATH: `${directory}:${process.env.PATH}`,
    CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS: "--cfg existing_backend",
  };
  try {
    const build = spawnSync("bash", ["scripts/build.sh"], { cwd: crate, encoding: "utf8", env: environment });
    assert.equal(build.status, 0, build.stderr);
    assert.equal(build.stdout, await resolveProductionWasmRustflags(environment));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

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
