import { readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { spawnSync } from "node:child_process";
import { assertAuthenticatedAccount } from "../../broker-workers/deploy/scripts/cloudflare-identifiers.mjs";
import { validatePublicCallbackBase } from "../../broker-workers/deploy/scripts/workers-subdomain.mjs";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) args.set(process.argv[index], process.argv[index + 1]);
const statePath = args.get("--ownership-state");
if (!isAbsolute(statePath ?? "")) throw new Error("ownership state must be absolute");
const state = JSON.parse(await readFile(statePath, "utf8"));
const manifest = JSON.parse(await readFile("build-manifest.json", "utf8"));
if (state.schema_version !== "0.3" || state.owner !== "lattice-provider-google-workers" ||
    state.created_by_run !== true || state.operator_computed_uploaded_source_sha256 !== manifest.source_hash ||
    !/^[0-9a-f]{32}$/.test(state.account_id ?? "") || !/^lattice-c5-[a-z0-9]{6,20}$/.test(state.prefix ?? "") ||
    !Array.isArray(state.workers) || state.workers.length !== 2) throw new Error("ownership state invalid");
const callback = validatePublicCallbackBase(state.prefix, state.workers_subdomain, state.public_callback_base);
if (state.callback_uri !== callback.googleOauthRedirectUri) throw new Error("ownership callback state invalid");
const expected = new Map([
  ["token", `${state.prefix}-google-token-egress`],
  ["provider", `${state.prefix}-google-provider-egress`],
]);
for (const worker of state.workers) {
  if (
    expected.get(worker.kind) !== worker.name ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(worker.deployment_id ?? "") ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(worker.version_id ?? "") ||
    worker.uploaded_source_sha256 !== manifest.source_hash
  ) throw new Error("worker ownership mismatch");
}
const plan = {
  schema_version: "0.3",
  account_id: state.account_id,
  operator_computed_uploaded_source_sha256: state.operator_computed_uploaded_source_sha256,
  delete: [...expected.values()].reverse(),
};
if (args.get("--mode") !== "apply") {
  console.log(JSON.stringify(plan, null, 2));
  console.log("cleanup dry-run complete; zero remote commands executed");
  process.exit(0);
}
if (args.get("--approve-cleanup") !== "yes" || !process.env.CLOUDFLARE_API_TOKEN) throw new Error("cleanup approval and API token required");
const run = (command) => {
  const result = spawnSync(command[0], command.slice(1), { encoding: "utf8", env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: state.account_id } });
  if (result.status !== 0) throw new Error("remote command failed");
  return result.stdout;
};
assertAuthenticatedAccount(run(["npx", "wrangler", "whoami", "--json"]), state.account_id);
for (const name of plan.delete) {
  const worker = state.workers.find((candidate) => candidate.name === name);
  const deployments = JSON.parse(run(["npx", "wrangler", "deployments", "list", "--name", name, "--json"]));
  if (!Array.isArray(deployments) || !deployments.some((deployment) =>
    deployment?.source === "wrangler" && deployment.id === worker.deployment_id &&
    deployment.versions?.some((version) => version?.version_id === worker.version_id)
  )) throw new Error("owned deployment missing");
}
for (const name of plan.delete) run(["npx", "wrangler", "delete", "--name", name, "--force"]);
console.log("private Google egress cleanup complete");
