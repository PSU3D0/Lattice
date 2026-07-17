import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { join, relative } from "node:path";

const root = new URL("../deploy/", import.meta.url).pathname;
const manifest = JSON.parse(await readFile(join(root, "deploy-manifest.json"), "utf8"));
if (manifest.schema_version !== "0.1" || manifest.implementation_backends?.length !== 1) {
  throw new Error("unexpected deployment manifest shape");
}
const backend = manifest.implementation_backends[0];
if (
  backend.module_sha256 !== "048f650aec8502659633289a4ace493c56a7bc6e95c8da3d4a34e293e96d4e96" ||
  backend.abi_version !== "lattice.transform.v1" ||
  backend.compatibility_date !== "2026-07-15" ||
  backend.cpu_ms !== 30000 ||
  backend.guest_memory_bytes !== 64 * 1024 * 1024 ||
  backend.isolate_memory_bytes !== 128 * 1024 * 1024 ||
  backend.concurrency !== 1 ||
  backend.instance_model !== "fresh_per_invocation"
) {
  throw new Error("deployment backend policy does not match the pinned W4 contract");
}

const attested = new Set();
for (const file of manifest.files) {
  if (attested.has(file.path) || file.path.includes("..") || file.path.startsWith("/")) {
    throw new Error(`invalid attested path: ${file.path}`);
  }
  attested.add(file.path);
  const bytes = await readFile(join(root, file.path));
  const digest = createHash("sha256").update(bytes).digest("hex");
  if (digest !== file.sha256 || bytes.length !== file.size_bytes) {
    throw new Error(`attestation mismatch: ${file.path}`);
  }
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
const packaged = (await walk(root)).filter((path) => path !== "deploy-manifest.json");
for (const path of packaged) {
  if (!attested.has(path)) throw new Error(`unattested package file: ${path}`);
}
if (attested.size !== packaged.length) throw new Error("manifest references an absent package file");
console.log(`verified ${attested.size} W4 package attestations`);
