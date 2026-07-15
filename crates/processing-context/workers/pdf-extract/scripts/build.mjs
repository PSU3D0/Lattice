import { createHash } from "node:crypto";
import { cp, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const guest = path.resolve(root, "..", "..", "guests", "pdf-extract");
const modulePath = path.join(guest, "pdf_extract.wasm");
const manifestPath = path.join(guest, "pdf_extract.manifest.json");
const inspectorPath = path.join(guest, "inspect_wasm.py");
const dist = path.join(root, "dist");

const [moduleBytes, manifestText, moduleStat] = await Promise.all([
  readFile(modulePath),
  readFile(manifestPath, "utf8"),
  stat(modulePath),
]);
const manifest = JSON.parse(manifestText);
const actualSha256 = createHash("sha256").update(moduleBytes).digest("hex");

if (actualSha256 !== manifest.module.sha256) {
  throw new Error("checked PDF transform hash does not match its manifest");
}
if (moduleStat.size !== manifest.module.size_bytes) {
  throw new Error("checked PDF transform size does not match its manifest");
}
if (manifest.build.max_memory_bytes !== 64 * 1024 * 1024) {
  throw new Error("checked PDF transform is not bounded to the W0 64 MiB ceiling");
}
if (
  manifest.transform_id !== "lattice.pdf.extract_text.v1" ||
  manifest.abi_version !== "lattice.transform.v1"
) {
  throw new Error("checked PDF transform identity or ABI is not pinned");
}

const inspection = spawnSync("uv", ["run", "python3", inspectorPath, modulePath], {
  cwd: root,
  encoding: "utf8",
});
if (inspection.status !== 0) {
  throw new Error("checked PDF transform failed exact no-import ABI admission");
}

await rm(dist, { recursive: true, force: true });
await mkdir(dist, { recursive: true });
await Promise.all([
  cp(path.join(root, "src", "index.mjs"), path.join(dist, "index.mjs")),
  cp(path.join(root, "src", "runtime.mjs"), path.join(dist, "runtime.mjs")),
  cp(modulePath, path.join(dist, "pdf_extract.wasm")),
]);
await writeFile(
  path.join(dist, "attestation.mjs"),
  [
    `export const TRANSFORM_ID = ${JSON.stringify(manifest.transform_id)};`,
    `export const ABI_VERSION = ${JSON.stringify(manifest.abi_version)};`,
    `export const MODULE_SHA256_ATTESTATION = ${JSON.stringify(actualSha256)};`,
    "",
  ].join("\n"),
);

console.log(`checked PDF transform build attestation: ${actualSha256}`);
