import { readFile, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { createCloudflareApi } from "./cloudflare-api.mjs";
import { ownedDurableNamespaces, ownedKvNamespace } from "./cloud-ownership.mjs";

const argument = (name) => {
  const index = process.argv.indexOf(name);
  return index < 0 ? undefined : process.argv[index + 1];
};
const statePath = argument("--state");
if (!statePath || !process.argv.includes("--approve-cleanup")) {
  throw new Error("--state and --approve-cleanup are required");
}
const token = process.env.CLOUDFLARE_API_TOKEN;
if (!token) throw new Error("CLOUDFLARE_API_TOKEN is required");
const state = JSON.parse(await readFile(statePath, "utf8"));
const accountId = state.account_id;
const prefix = state.prefix;
if (state.schema_version !== "0.1" || !/^[0-9a-f]{32}$/.test(accountId) || !/^lattice-w4-[a-z0-9-]{6,30}$/.test(prefix)) {
  throw new Error("resource state identity is invalid");
}
const expectedNames = {
  flow: `${prefix}-flow`, extraction: `${prefix}-extract`, provider: `${prefix}-provider`,
  kv: `${prefix}-kv`, bucket: `${prefix}-workspace`,
};
if (JSON.stringify(state.names) !== JSON.stringify(expectedNames)) {
  throw new Error("resource state names do not derive from its W4 prefix");
}
const expectedScripts = [expectedNames.provider, expectedNames.extraction, expectedNames.flow];
if (!Array.isArray(state.scripts) || JSON.stringify(state.scripts) !== JSON.stringify(expectedScripts)) {
  throw new Error("resource state Worker targets are invalid");
}
if (state.kv_namespace_id !== null && !/^[0-9a-f]{32}$/.test(state.kv_namespace_id)) {
  throw new Error("resource state KV id is invalid");
}
for (const id of [...(state.durable_object_baseline_ids ?? []), ...(state.durable_object_namespace_ids ?? [])]) {
  if (!/^[0-9a-f]{32}$/.test(id)) throw new Error("resource state Durable Object id is invalid");
}
if (process.env.CLOUDFLARE_ACCOUNT_ID && process.env.CLOUDFLARE_ACCOUNT_ID !== accountId) {
  throw new Error("CLOUDFLARE_ACCOUNT_ID does not match resource state");
}
const { request: api, listPaged, listBuckets } = createCloudflareApi(token);
const root = resolve(new URL("..", import.meta.url).pathname);
const removeWorker = (name) => {
  const result = spawnSync("npx", ["wrangler", "delete", name, "--force"], {
    cwd: root,
    stdio: "inherit",
    env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId },
  });
  if (result.status !== 0) throw new Error(`failed to delete Worker ${name}`);
};
const errors = [];
const scripts = await listPaged(`/accounts/${accountId}/workers/scripts`);
const existingScripts = new Set(scripts.map((script) => script.id));
for (const name of [...state.scripts].reverse()) {
  if (!existingScripts.has(name)) continue;
  try { removeWorker(name); } catch { errors.push(`worker:${name}`); }
}
const durable = await listPaged(`/accounts/${accountId}/workers/durable_objects/namespaces`);
const existingDurable = new Set(durable.map((namespace) => namespace.id));
const targetDurableIds = ownedDurableNamespaces(state.names, durable)
  .map((namespace) => namespace.id);
state.durable_object_namespace_ids = targetDurableIds;
for (const id of targetDurableIds) {
  if (!existingDurable.has(id)) continue;
  try { await api(`/accounts/${accountId}/workers/durable_objects/namespaces/${id}`, { method: "DELETE" }); }
  catch { errors.push(`durable-object:${id}`); }
}
try {
  const kv = await listPaged(`/accounts/${accountId}/storage/kv/namespaces`);
  const ownedKv = ownedKvNamespace(state.names, state.kv_namespace_id, kv);
  if (ownedKv) {
    state.kv_namespace_id = ownedKv.id;
    await api(`/accounts/${accountId}/storage/kv/namespaces/${ownedKv.id}`, { method: "DELETE" });
  }
} catch { errors.push("kv"); }
if (state.r2_created) {
  const r2 = await listBuckets(accountId);
  if (r2.some((bucket) => bucket.name === state.names.bucket)) {
    try { await api(`/accounts/${accountId}/r2/buckets/${state.names.bucket}`, { method: "DELETE" }); }
    catch { errors.push("r2"); }
  }
}
const [remainingScripts, remainingDurable, remainingKv, remainingR2] = await Promise.all([
  listPaged(`/accounts/${accountId}/workers/scripts`),
  listPaged(`/accounts/${accountId}/workers/durable_objects/namespaces`),
  listPaged(`/accounts/${accountId}/storage/kv/namespaces`),
  listBuckets(accountId),
]);
if (remainingScripts.some((script) => state.scripts.includes(script.id))) errors.push("workers:still-present");
if (ownedDurableNamespaces(state.names, remainingDurable).length > 0) errors.push("durable-object:still-present");
if (remainingKv.some((namespace) => namespace.id === state.kv_namespace_id)) errors.push("kv:still-present");
if (remainingR2.some((bucket) => bucket.name === state.names.bucket)) errors.push("r2:still-present");
state.cleanup_attempted = true;
state.cleanup_verified = errors.length === 0;
state.cleanup_errors = errors;
await writeFile(statePath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
if (errors.length) throw new Error(`cleanup incomplete: ${errors.join(", ")}`);
console.log(`verified cleanup for ${state.prefix}`);
