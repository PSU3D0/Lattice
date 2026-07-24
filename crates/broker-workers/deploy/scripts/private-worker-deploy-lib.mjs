import { readFile, realpath, stat } from "node:fs/promises";
import { isAbsolute, relative } from "node:path";

export const BROKER_PRIVATE_SECRET_NAMES = Object.freeze([
  "ACTIVATION_SERVICE_AUTH",
  "AI_GATEWAY_AUTHORIZATION",
  "AUTH_DRIVER_SERVICE_AUTH",
  "BINDING_SIGNING_SEED",
  "COMMITMENT_KEY",
  "CUSTODY_ROOT_KEY",
  "DEPLOYMENT_BOOTSTRAP_AUTH",
  "GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U",
  "GOOGLE_EGRESS_SERVICE_AUTH",
  "INVOKE_SERVICE_AUTH",
  "KEY_HASH_PEPPER",
  "RECEIPT_SIGNING_SEED",
]);

export const WORKER_SECRET_NAMES = Object.freeze({
  "auth-driver": Object.freeze(["AUTH_DRIVER_SERVICE_AUTH"]),
  "google-token-egress": Object.freeze([
    "GOOGLE_EGRESS_SERVICE_AUTH",
    "GOOGLE_OAUTH_CLIENT_ID",
    "GOOGLE_OAUTH_CLIENT_SECRET",
    "GOOGLE_TOKEN_RESULT_KEY",
  ]),
  "google-provider-egress": Object.freeze(["GOOGLE_EGRESS_SERVICE_AUTH"]),
  "broker-private": BROKER_PRIVATE_SECRET_NAMES,
  "broker-public": Object.freeze([]),
});

export const PRIVATE_WORKER_SECRET_NAMES = WORKER_SECRET_NAMES;

const HEX_32_BROKER_SECRET_NAMES = Object.freeze([
  "BINDING_SIGNING_SEED",
  "COMMITMENT_KEY",
  "CUSTODY_ROOT_KEY",
  "RECEIPT_SIGNING_SEED",
]);

const NOT_FOUND_PATTERNS = [
  /workers\.api\.error\.script_not_found/i,
  /script[_ -]not[_ -]found/i,
  /(?:error|code)["'\s:=-]*10007\b/i,
];

export function classifyDeploymentsListResult(result) {
  if (result?.status === 0) {
    try {
      const deployments = JSON.parse(result.stdout || "null");
      if (!Array.isArray(deployments)) return { state: "unknown" };
      return { state: deployments.length === 0 ? "absent" : "present", deployments };
    } catch {
      return { state: "unknown" };
    }
  }
  const output = `${result?.stdout ?? ""}\n${result?.stderr ?? ""}`;
  if (NOT_FOUND_PATTERNS.some((pattern) => pattern.test(output))) return { state: "absent", deployments: [] };
  return { state: "unknown" };
}

function exactNames(actual, expected) {
  const left = [...actual].sort();
  const right = [...expected].sort();
  return left.length === right.length && left.every((name, index) => name === right[index]);
}

function inside(path, root) {
  const value = relative(root, path);
  return value === "" || (!value.startsWith("..") && !isAbsolute(value));
}

export async function loadPrivateWorkerSecrets(path, repositoryRoot) {
  if (!isAbsolute(path ?? "") || !isAbsolute(repositoryRoot ?? "")) throw new Error("secrets_file_invalid");
  let resolved;
  let metadata;
  let parsed;
  try {
    resolved = await realpath(path);
    const resolvedRoot = await realpath(repositoryRoot);
    if (inside(resolved, resolvedRoot)) throw new Error("invalid");
    metadata = await stat(resolved);
    if (!metadata.isFile() || (metadata.mode & 0o777) !== 0o600) throw new Error("invalid");
    parsed = JSON.parse(await readFile(resolved, "utf8"));
  } catch {
    throw new Error("secrets_file_invalid");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed) || !exactNames(Object.keys(parsed), Object.keys(WORKER_SECRET_NAMES))) {
    throw new Error("secrets_file_names_invalid");
  }
  for (const [worker, requiredNames] of Object.entries(WORKER_SECRET_NAMES)) {
    const values = parsed[worker];
    if (!values || typeof values !== "object" || Array.isArray(values) || !exactNames(Object.keys(values), requiredNames)) {
      throw new Error("secrets_file_names_invalid");
    }
    for (const name of requiredNames) {
      if (typeof values[name] !== "string" || values[name].length === 0 || values[name].includes("\u0000")) {
        throw new Error("secrets_file_value_invalid");
      }
    }
  }
  if (!/^[0-9a-f]{64}$/.test(parsed["google-token-egress"].GOOGLE_TOKEN_RESULT_KEY)) {
    throw new Error("secrets_file_value_invalid");
  }
  for (const name of HEX_32_BROKER_SECRET_NAMES) {
    if (!/^[0-9a-f]{64}$/.test(parsed["broker-private"][name])) throw new Error("secrets_file_value_invalid");
  }
  const activationPrivateKey = parsed["broker-private"].GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U;
  if (!/^[A-Za-z0-9_-]{43}$/.test(activationPrivateKey) || Buffer.from(activationPrivateKey, "base64url").length !== 32) {
    throw new Error("secrets_file_value_invalid");
  }
  if (
    parsed["google-token-egress"].GOOGLE_EGRESS_SERVICE_AUTH !== parsed["google-provider-egress"].GOOGLE_EGRESS_SERVICE_AUTH ||
    parsed["google-token-egress"].GOOGLE_EGRESS_SERVICE_AUTH !== parsed["broker-private"].GOOGLE_EGRESS_SERVICE_AUTH ||
    parsed["auth-driver"].AUTH_DRIVER_SERVICE_AUTH !== parsed["broker-private"].AUTH_DRIVER_SERVICE_AUTH
  ) {
    throw new Error("secrets_file_value_invalid");
  }
  return parsed;
}

function validDeployment(deployment) {
  const sourceHash = deployment?.source_hash ?? deployment?.metadata?.source_hash;
  return /^[A-Za-z0-9._:-]{6,256}$/.test(deployment?.id ?? "") && /^[0-9a-f]{64}$/.test(sourceHash ?? "");
}

function ownedDeploymentPresent(deployments, ownership, name, accountId) {
  if (
    ownership?.name !== name || ownership?.account_id !== accountId ||
    !/^[A-Za-z0-9._:-]{6,256}$/.test(ownership?.deployment_id ?? "") ||
    !/^[0-9a-f]{64}$/.test(ownership?.source_hash ?? "")
  ) return false;
  return deployments.some((deployment) =>
    deployment.id === ownership.deployment_id &&
    (deployment.source_hash ?? deployment.metadata?.source_hash) === ownership.source_hash
  );
}

export async function qualifyWorkerTarget({ runner, step, name, accountId, ownedUpdate = false, ownership }) {
  const result = await runner.run(`${step}:absence`, ["npx", "wrangler", "deployments", "list", "--name", name, "--json"]);
  const classified = classifyDeploymentsListResult(result);
  if (classified.state === "unknown") throw new Error(`${step}_target_unknown`);
  if (classified.state === "absent") return { existed: false, created: false };
  if (!ownedUpdate || !ownedDeploymentPresent(classified.deployments, ownership, name, accountId)) {
    throw new Error(`${step}_target_already_exists`);
  }
  return { existed: true, created: false };
}

export async function deployPrivateWorker({
  runner, step, name, accountId, config, secrets, derivedSecrets = {}, ownedUpdate = false, ownership,
  rollbackCreatedOnFailure = true,
}) {
  const resource = await qualifyWorkerTarget({ runner, step, name, accountId, ownedUpdate, ownership });
  const suppliedNames = Object.keys(secrets);
  const derivedNames = Object.keys(derivedSecrets);
  const requiredNames = [...suppliedNames, ...derivedNames];
  let created = false;
  try {
    const deployed = await runner.run(`${step}:deploy`, ["npx", "wrangler", "deploy", "--config", config, "--name", name]);
    if (deployed.status !== 0) throw new Error(`${step}_deploy_failed`);
    created = !resource.existed;
    for (const [secretName, value] of [...Object.entries(secrets), ...Object.entries(derivedSecrets)]) {
      const installed = await runner.run(
        `${step}:secret:${secretName}`,
        ["npx", "wrangler", "secret", "put", secretName, "--name", name],
        { input: `${value}\n` },
      );
      if (installed.status !== 0) throw new Error(`${step}_secret_install_failed`);
    }
    const listed = await runner.run(`${step}:secrets`, ["npx", "wrangler", "secret", "list", "--name", name, "--format", "json"]);
    let installedSecretNames;
    try {
      installedSecretNames = listed.status === 0 ? JSON.parse(listed.stdout || "null") : null;
    } catch {
      installedSecretNames = null;
    }
    if (!Array.isArray(installedSecretNames)) throw new Error(`${step}_secret_verification_failed`);
    const names = new Set(installedSecretNames.map((item) => item?.name).filter((value) => typeof value === "string"));
    if (requiredNames.some((name) => !names.has(name))) throw new Error(`${step}_secret_verification_failed`);
    const deploymentResult = await runner.run(`${step}:capture`, ["npx", "wrangler", "deployments", "list", "--name", name, "--json"]);
    const classified = classifyDeploymentsListResult(deploymentResult);
    if (classified.state !== "present" || classified.deployments.length !== 1 || !validDeployment(classified.deployments[0])) {
      throw new Error(`${step}_deployment_evidence_invalid`);
    }
    const deployment = classified.deployments[0];
    return {
      name,
      account_id: accountId,
      deployment_id: deployment.id,
      source_hash: deployment.source_hash ?? deployment.metadata.source_hash,
      created_by_run: created,
      secrets: requiredNames.map((secretName) => ({ name: secretName, install_status: "installed_and_verified" })),
    };
  } catch (error) {
    if (created && rollbackCreatedOnFailure) {
      const cleanup = await runner.run(`${step}:cleanup`, ["npx", "wrangler", "delete", "--name", name, "--force"]);
      if (cleanup.status !== 0) throw new Error(`${step}_failed_cleanup_failed`);
    }
    throw error;
  }
}
