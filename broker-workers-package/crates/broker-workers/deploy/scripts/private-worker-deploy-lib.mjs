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

export async function deleteWorkerAndVerifyAbsent({
  runner, name, deleteStep, verifyStep = `${deleteStep}:verify`, failureMessage = `${deleteStep}_failed`,
}) {
  const deletion = await runner.run(deleteStep, ["npx", "wrangler", "delete", "--name", name, "--force"]);
  const verified = classifyDeploymentsListResult(await runner.run(
    verifyStep,
    ["npx", "wrangler", "deployments", "list", "--name", name, "--json"],
  ));
  const evidence = {
    resource: name,
    delete_exit_status: deletion.status,
    post_delete_state: verified.state,
    status: verified.state === "absent" ? "deleted" : "cleanup_failed",
  };
  if (verified.state !== "absent") {
    const error = new Error(failureMessage);
    error.cleanupEvidence = evidence;
    throw error;
  }
  return evidence;
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

const CANONICAL_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const LOWERCASE_SHA256 = /^[0-9a-f]{64}$/;

export function isCanonicalUuid(value) {
  return CANONICAL_UUID.test(value ?? "");
}

export function isValidCloudflareDeployment(deployment) {
  return isCanonicalUuid(deployment?.id) &&
    deployment?.source === "wrangler" &&
    typeof deployment?.created_on === "string" &&
    Number.isFinite(Date.parse(deployment.created_on)) &&
    Array.isArray(deployment?.versions) && deployment.versions.length > 0 &&
    deployment.versions.every((version) => isCanonicalUuid(version?.version_id));
}

function recordedDeployment(deployment) {
  const version = [...deployment.versions].sort((left, right) =>
    (Number(right?.percentage) || 0) - (Number(left?.percentage) || 0)
  )[0];
  return {
    deployment_id: deployment.id,
    version_id: version.version_id,
    created_on: deployment.created_on,
    triggered_by: deployment.annotations?.["workers/triggered_by"] ?? null,
  };
}

function ownedDeploymentPresent(deployments, ownership, name, accountId) {
  if (
    ownership?.name !== name || ownership?.account_id !== accountId ||
    !isCanonicalUuid(ownership?.deployment_id) ||
    !isCanonicalUuid(ownership?.version_id) ||
    !LOWERCASE_SHA256.test(ownership?.uploaded_source_sha256 ?? "")
  ) return false;
  return deployments.every(isValidCloudflareDeployment) && deployments.some((deployment) =>
    deployment.id === ownership.deployment_id &&
    deployment.versions.some((version) => version.version_id === ownership.version_id)
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
  rollbackCreatedOnFailure = true, uploadedSourceSha256, runStartedAt,
}) {
  if (!LOWERCASE_SHA256.test(uploadedSourceSha256 ?? "")) throw new Error(`${step}_uploaded_source_sha256_invalid`);
  const resource = await qualifyWorkerTarget({ runner, step, name, accountId, ownedUpdate, ownership });
  const capturedRunStartedAt = runStartedAt ?? new Date(Math.floor(Date.now() / 1000) * 1000).toISOString();
  const runStartedAtMillis = Date.parse(capturedRunStartedAt);
  if (!Number.isFinite(runStartedAtMillis)) throw new Error(`${step}_run_started_at_invalid`);
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
    if (
      classified.state !== "present" ||
      classified.deployments.some((deployment) =>
        !isValidCloudflareDeployment(deployment) || Date.parse(deployment.created_on) < runStartedAtMillis
      )
    ) {
      const evidenceError = new Error(`${step}_deployment_evidence_invalid`);
      evidenceError.creationOwnershipUncertain = true;
      throw evidenceError;
    }
    const history = classified.deployments.map(recordedDeployment);
    const deployment = history.reduce((newest, candidate) =>
      Date.parse(candidate.created_on) > Date.parse(newest.created_on) ? candidate : newest
    );
    return {
      name,
      account_id: accountId,
      deployment_id: deployment.deployment_id,
      version_id: deployment.version_id,
      deployment_count: history.length,
      triggered_by_annotations: history.map(({ deployment_id, version_id, created_on, triggered_by }) => ({
        deployment_id, version_id, created_on, triggered_by,
      })),
      run_started_at: capturedRunStartedAt,
      uploaded_source_sha256: uploadedSourceSha256,
      created_by_run: created,
      secrets: requiredNames.map((secretName) => ({ name: secretName, install_status: "installed_and_verified" })),
    };
  } catch (error) {
    if (created && rollbackCreatedOnFailure && error?.creationOwnershipUncertain !== true) {
      try {
        const cleanupEvidence = await deleteWorkerAndVerifyAbsent({
          runner, name, deleteStep: `${step}:cleanup`, failureMessage: `${step}_cleanup_failed`,
        });
        if (error && typeof error === "object") error.cleanupEvidence = cleanupEvidence;
      } catch (cleanupError) {
        const original = error instanceof Error ? error.message : String(error);
        const combined = new Error(`${original}; ${step}_cleanup_failed`, { cause: error });
        combined.cleanupEvidence = cleanupError.cleanupEvidence;
        throw combined;
      }
    }
    throw error;
  }
}
