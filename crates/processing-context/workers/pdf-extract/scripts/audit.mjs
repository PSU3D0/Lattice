import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { unstable_readConfig } from "wrangler";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const runtime = await readFile(path.join(root, "src", "runtime.mjs"), "utf8");
const index = await readFile(path.join(root, "src", "index.mjs"), "utf8");
const config = unstable_readConfig({ config: path.join(root, "wrangler.toml") });

const fetchTokens = runtime.match(/\bfetch\b/g) ?? [];
if (fetchTokens.length !== 1 || !runtime.includes("async fetch(request)")) {
  throw new Error("extraction runtime must not reference ambient fetch");
}
if (!index.includes('from "./pdf_extract.wasm"')) {
  throw new Error("extraction entrypoint must use the checked precompiled wasm binding");
}
if (config.workers_dev !== false || config.preview_urls !== false) {
  throw new Error("extraction Worker must have no workers.dev or preview URL");
}
for (const key of ["routes", "kv_namespaces", "r2_buckets", "d1_databases", "services"]) {
  if ((config[key] ?? []).length !== 0) {
    throw new Error(`extraction Worker must not declare ${key}`);
  }
}
if ((config.durable_objects?.bindings ?? []).length !== 0) {
  throw new Error("extraction Worker must not declare durable object bindings");
}
if (
  (config.queues?.producers ?? []).length !== 0 ||
  (config.queues?.consumers ?? []).length !== 0
) {
  throw new Error("extraction Worker must not declare queue bindings");
}
if (Object.keys(config.vars ?? {}).length !== 0) {
  throw new Error("extraction Worker must not declare variables");
}
if (config.limits?.cpu_ms !== 30_000) {
  throw new Error("extraction Worker CPU policy drifted from the checked 30000 ms budget");
}

console.log("extraction Worker static containment audit: OK");
