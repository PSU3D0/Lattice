import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";

const path = "deploy/wrangler.toml";
let config = await readFile(path, "utf8");
config = config
  .replace('main = "build/worker/shim.mjs"', 'main = "build/index.js"')
  .replace(/\n\[build\]\n(?:#.*\n)?command = .*\n/, "\n");
await mkdir("deploy/build", { recursive: true });
for (const file of ["index.js", "index_bg.wasm", "package.json"]) {
  await copyFile(`build/${file}`, `deploy/build/${file}`);
}
const vars = [
  'LATTICE_BROKER_BINDING_REF = "REPLACE_WITH_V2_BINDING_REF"',
  'LATTICE_BROKER_BUNDLE_ID = "REPLACE_WITH_ATTESTED_BUNDLE_ID"',
  'LATTICE_BROKER_FLOW_IR_HASH = "sha256:8205e5bbdc1e1332ea09cab4eaf0cf31b5e1ce5fb2faf5c827ee389d6e2fbb4b"',
  'LATTICE_BROKER_BINDING_LOCK_HASH = "REPLACE_WITH_SHA256_BINDING_LOCK"',
  'LATTICE_BROKER_FLOW_ID = "afe6290b-d88a-5337-bdf2-a4d0372cd0f7"',
  'LATTICE_BROKER_RECEIPT_PUBLIC_KEY_B64U = "REPLACE_WITH_OPERATOR_APPROVED_ED25519_KEY"',
  'LATTICE_BROKER_RECEIPT_PUBLIC_KEY_HASH = "REPLACE_WITH_SHA256_RECEIPT_PUBLIC_KEY"',
].join("\n");
if (config.includes("[vars]\n")) {
  config = config.replace("[vars]\n", `[vars]\n${vars}\n`);
} else {
  config += `\n[vars]\n${vars}\n`;
}
config += `\n# Private V2 credential broker; Google requests never use ambient fetch.\n[[services]]\nbinding = "LATTICE_BROKER_PRIVATE"\nservice = "REPLACE_WITH_PRIVATE_BROKER_WORKER"\n`;
config += `\n# Required host-only secrets (set with wrangler secret put):\n# LATTICE_BROKER_DEPLOYMENT_KEY, LATTICE_BROKER_POP_SEED_B64U,\n# LATTICE_BROKER_SERVICE_AUTH\n`;
await writeFile(path, config);
const manifestPath = "deploy/deploy-manifest.json";
try {
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  const entry = manifest.files.find((file) => file.path === "wrangler.toml");
  if (entry === undefined) throw new Error("deployment manifest has no wrangler entry");
  const bytes = Buffer.from(config);
  entry.sha256 = createHash("sha256").update(bytes).digest("hex");
  entry.size_bytes = bytes.length;
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}
