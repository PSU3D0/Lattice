import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";

const files = [
  "README.md",
  "package-lock.json",
  "package.json",
  "scripts/cleanup.mjs",
  "scripts/deploy.mjs",
  "scripts/package.mjs",
  "src/provider-worker.mjs",
  "src/shared.mjs",
  "src/token-worker.mjs",
  "wrangler.provider.jsonc",
  "wrangler.token.jsonc",
].sort();
const records = [];
for (const path of files) {
  const bytes = await readFile(path);
  records.push({ path, sha256: createHash("sha256").update(bytes).digest("hex"), size_bytes: bytes.length });
}
const sourceHash = createHash("sha256");
for (const record of records) sourceHash.update(`${record.path}\0${record.sha256}\0${record.size_bytes}\n`);
const manifest = {
  schema_version: "0.2",
  package: "provider-google-workers",
  package_version: "0.1.0",
  services: {
    token: { entrypoint: "src/token-worker.mjs", wrangler: "wrangler.token.jsonc" },
    provider: { entrypoint: "src/provider-worker.mjs", wrangler: "wrangler.provider.jsonc" },
  },
  source_hash: sourceHash.digest("hex"),
  files: records,
};
await writeFile("build-manifest.json", `${JSON.stringify(manifest, null, 2)}\n`);
console.log(manifest.source_hash);
