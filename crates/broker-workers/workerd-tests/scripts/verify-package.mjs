import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { join, relative, resolve } from "node:path";

const root = resolve(new URL("../../../../broker-workers-package", import.meta.url).pathname);
const manifest = JSON.parse(await readFile(join(root, "build-manifest.json"), "utf8"));
if (
  manifest.schema_version !== "0.2" || manifest.package !== "broker-workers" ||
  manifest.package_version !== "0.1.0" || !/^[0-9a-f]{64}$/.test(manifest.wasm_sha256) ||
  manifest.root_production_wasm_sha256 !== manifest.wasm_sha256
) throw new Error("unexpected production build manifest");
const attested = new Set();
for (const file of manifest.files) {
  if (attested.has(file.path) || file.path.includes("..") || file.path.startsWith("/") || !/^[0-9a-f]{64}$/.test(file.sha256)) {
    throw new Error(`invalid manifest path/hash: ${file.path}`);
  }
  if (
    /(^|\/)(node_modules|target|build-test|build-production|build-production-tmp|workerd-tests)(\/|$)/.test(file.path) ||
    file.path.endsWith("/src/wasm/test_fixtures.rs")
  ) {
    throw new Error(`excluded test/cache path in package: ${file.path}`);
  }
  attested.add(file.path);
  const bytes = await readFile(join(root, file.path));
  for (const sentinel of [
    "/__test/", "fixture-pop-public", "fixture-deployment-key-value", "google-semantic-broker-v1",
    "lbk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "fixture-refresh-never-log", "fixture-access-never-log", "x-lattice-test-activation-crash",
    "LOCAL_TEST_MODE",
  ]) {
    if (bytes.includes(Buffer.from(sentinel))) throw new Error(`fixture sentinel in ${file.path}`);
  }
  const digest = createHash("sha256").update(bytes).digest("hex");
  if (digest !== file.sha256 || bytes.length !== file.size_bytes) throw new Error(`manifest mismatch: ${file.path}`);
}
async function walk(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await walk(path));
    else if (entry.isFile()) files.push(relative(root, path).replaceAll("\\", "/"));
    else throw new Error(`non-regular package entry: ${path}`);
  }
  return files;
}
const packaged = (await walk(root)).filter((path) => path !== "build-manifest.json");
if (packaged.some((path) => !attested.has(path)) || packaged.length !== attested.size) throw new Error("package contains unattested or absent files");
const requirements = JSON.parse(await readFile(join(root, "requirements.json"), "utf8"));
if (
  requirements.schema_version !== "0.2" || requirements.compatibility_date !== "2026-07-15" ||
  requirements.ai_gateway_policy?.payload_logging !== "disabled" || requirements.ai_gateway_policy?.applied_locally !== false ||
  requirements.feature_flags?.production?.length !== 0
) throw new Error("requirements do not preserve production evidence honesty");
const packagedLock = await readFile(join(root, "Cargo.lock"));
const repositoryLock = await readFile(resolve(root, "../Cargo.lock"));
const lockHash = createHash("sha256").update(packagedLock).digest("hex");
if (lockHash !== manifest.cargo_lock_sha256 || !attested.has("Cargo.lock") || !packagedLock.equals(repositoryLock)) {
  throw new Error("Cargo.lock is absent or differs from repository Cargo.lock");
}
for (const file of ["crates/broker-workers/build/index.js", "crates/broker-workers/build/index_bg.wasm", "crates/broker-workers/build/worker/shim.mjs"]) {
  const bytes = await readFile(join(root, file));
  for (const sentinel of ["/__test/", "fixture-pop-public", "fixture-deployment-key-value", "google-semantic-broker-v1"]) {
    if (bytes.includes(Buffer.from(sentinel))) throw new Error(`production fixture sentinel in ${file}`);
  }
}
console.log(`verified ${attested.size} production broker package files`);
