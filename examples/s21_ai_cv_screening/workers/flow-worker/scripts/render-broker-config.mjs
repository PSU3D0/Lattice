import { readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";

const path = "deploy/wrangler.toml";
let config = await readFile(path, "utf8");
const vars = [
  'LATTICE_S21_HTTP_MODE = "broker_v2"',
  'LATTICE_BROKER_BINDING_REF = "REPLACE_WITH_V2_BINDING_REF"',
  'LATTICE_BROKER_BUNDLE_ID = "REPLACE_WITH_ATTESTED_BUNDLE_ID"',
  'LATTICE_BROKER_FLOW_IR_HASH = "REPLACE_WITH_SHA256_FLOW_IR"',
  'LATTICE_BROKER_BINDING_LOCK_HASH = "REPLACE_WITH_SHA256_BINDING_LOCK"',
  'LATTICE_BROKER_FLOW_ID = "1f82ed0f-708f-5fda-b289-338382a74a78"',
  'LATTICE_BROKER_RECEIPT_PUBLIC_KEY_B64U = "REPLACE_WITH_OPERATOR_APPROVED_ED25519_KEY"',
  'LATTICE_BROKER_RECEIPT_PUBLIC_KEY_HASH = "REPLACE_WITH_SHA256_RECEIPT_PUBLIC_KEY"',
].join("\n");
if (!config.includes("[vars]\n")) throw new Error("rendered Worker config has no vars table");
config = config.replace("[vars]\n", `[vars]\n${vars}\n`);
config += `\n# Private V2 credential broker. Google requests never use ambient fetch.\n[[services]]\nbinding = "LATTICE_BROKER_PRIVATE"\nservice = "REPLACE_WITH_PRIVATE_BROKER_WORKER"\n\n# Exact private connector/provider transport binding required by the host.\n[[services]]\nbinding = "LATTICE_S21_PROVIDER"\nservice = "REPLACE_WITH_S21_PROVIDER_SERVICE"\n`;
config += `\n# Required host-only secrets (set with wrangler secret put):\n# LATTICE_BROKER_DEPLOYMENT_KEY, LATTICE_BROKER_POP_SEED_B64U,\n# LATTICE_BROKER_SERVICE_AUTH, LATTICE_CONNECTOR_AUTH_LLM_API_KEY\n`;
await writeFile(path, config);
const manifestPath = "deploy/deploy-manifest.json";
const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
const entry = manifest.files.find((file) => file.path === "wrangler.toml");
if (entry === undefined) throw new Error("deployment manifest has no wrangler entry");
const bytes = Buffer.from(config);
entry.sha256 = createHash("sha256").update(bytes).digest("hex");
entry.size_bytes = bytes.length;
await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
