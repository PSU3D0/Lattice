import { createHash } from "node:crypto";

function canonical(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
}

function manifestHash(ownership) {
  return createHash("sha256").update(canonical(ownership)).digest("hex");
}

function parseJson(result, step) {
  if (result.status !== 0) throw new Error(`${step}_failed`);
  try { return JSON.parse(result.stdout); } catch { throw new Error(`${step}_invalid_json`); }
}

function validateState(state) {
  if (
    state.schema_version !== "0.3" || state.owner !== "lattice-broker-b5" ||
    !/^[0-9a-f]{32}$/.test(state.account_id ?? "") ||
    !/^lattice-b5-[a-z0-9]{6,20}$/.test(state.prefix ?? "") ||
    !Array.isArray(state.resources?.workers) || state.resources.workers.length !== 2 ||
    state.resources?.d1_database?.owned !== false
  ) throw new Error("resource_state_invalid");
}

async function verifyWorker(state, runner, worker) {
  const expectedName = `${state.prefix}-broker-${worker.kind}`;
  const ownership = worker.ownership;
  if (
    worker.name !== expectedName || worker.account_id !== state.account_id || worker.prefix !== state.prefix ||
    worker.created_by_run !== true ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(worker.deployment_id ?? "") ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(worker.version_id ?? "") ||
    !/^[0-9a-f]{64}$/.test(worker.uploaded_source_sha256 ?? "") ||
    ownership?.owner !== state.owner || ownership?.account_id !== state.account_id ||
    ownership?.prefix !== state.prefix || ownership?.worker_name !== worker.name ||
    ownership?.deployment_id !== worker.deployment_id || ownership?.version_id !== worker.version_id ||
    ownership?.uploaded_source_sha256 !== worker.uploaded_source_sha256 ||
    manifestHash(ownership) !== worker.ownership_manifest_hash
  ) throw new Error(`cleanup_${worker.kind}_ownership_manifest_invalid`);

  const deployments = parseJson(await runner.run(`cleanup:deployments:${worker.kind}`, [
    "npx", "wrangler", "deployments", "list", "--name", worker.name, "--json",
  ]), `cleanup_deployments_${worker.kind}`);
  if (!Array.isArray(deployments) || deployments.length === 0) throw new Error(`cleanup_${worker.kind}_missing_deployment`);
  const pinned = deployments.filter((entry) =>
    entry?.source === "wrangler" && entry.id === worker.deployment_id &&
    entry.versions?.some((version) => version?.version_id === worker.version_id)
  );
  if (pinned.length !== 1) throw new Error(`cleanup_${worker.kind}_deployment_mismatch`);
  return worker.name;
}

export async function executeStandaloneCleanup(state, runner) {
  validateState(state);
  // Query and verify every target before deleting any target.
  const verified = [];
  for (const kind of ["private", "public"]) {
    const matches = state.resources.workers.filter((worker) => worker.kind === kind);
    if (matches.length !== 1) throw new Error(`cleanup_${kind}_ambiguous`);
    verified.push(await verifyWorker(state, runner, matches[0]));
  }
  const deleted = [];
  for (const name of [...verified].reverse()) {
    const result = await runner.run(`cleanup:delete:${name}`, [
      "npx", "wrangler", "delete", "--name", name, "--force",
    ]);
    if (result.status !== 0) throw new Error(`cleanup_delete_failed:${name}`);
    deleted.push(name);
  }
  return { schema_version: "0.3", account_id: state.account_id, deleted };
}

export { canonical, manifestHash };
