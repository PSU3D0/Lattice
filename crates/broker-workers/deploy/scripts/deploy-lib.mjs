import { createHash } from "node:crypto";
import { assertAuthenticatedAccount } from "./cloudflare-identifiers.mjs";
import { verifyLiveWorkersSubdomain } from "./workers-subdomain.mjs";
import {
  BROKER_PRIVATE_SECRET_NAMES,
  classifyDeploymentsListResult,
  deleteWorkerAndVerifyAbsent,
  deployPrivateWorker,
  isCanonicalUuid,
  isValidCloudflareDeployment,
} from "./private-worker-deploy-lib.mjs";

export const REQUIRED_SECRET_NAMES = BROKER_PRIVATE_SECRET_NAMES;

export function redactedPlan(context) {
  return {
    schema_version: "3",
    account_id: context.accountId,
    prefix: context.prefix,
    resources: {
      private_worker: context.privateName,
      public_worker: context.publicName,
      d1_database_id: context.d1Id,
      durable_objects: ["ConnectionRefreshDurableObject", "CredentialStateDurableObject", "V2AuthorityDurableObject"],
      approved_services: [context.authDriverService, context.googleProviderService, context.googleTokenService],
    },
    workers_subdomain: context.workersSubdomain,
    callback_base: context.publicCallbackBase,
    google_oauth_redirect_uri: context.googleOauthRedirectUri,
    required_secret_names: REQUIRED_SECRET_NAMES,
    worker_secrets: {
      [context.privateName]: REQUIRED_SECRET_NAMES.map((name) => ({ name, install_status: "planned" })),
      [context.publicName]: [],
    },
    steps: [
      "clean_hermetic_production_preflight",
      "verify_live_workers_subdomain",
      "qualify_auth_account",
      "prove_target_absent_or_owned",
      "verify_immutable_dependency_pins",
      "qualify_exact_secrets_file_before_mutation",
      "prove_deploy_install_verify_capture_fence_aware_private",
      "apply_0003_production_v2_cutover",
      "prove_deploy_verify_capture_public_last",
      "health_ready_callback_400_smoke",
      "write_sanitized_evidence",
    ],
  };
}

function parseJson(result, step) {
  if (result.status !== 0) throw new Error(`${step}_failed`);
  try {
    return JSON.parse(result.stdout || "null");
  } catch {
    throw new Error(`${step}_invalid_json`);
  }
}

async function checked(runner, step, command) {
  const result = await runner.run(step, command);
  if (result.status !== 0) throw new Error(`${step}_failed`);
  return result;
}

function exactDeployment(deployments, pin) {
  return Array.isArray(deployments) && deployments.every(isValidCloudflareDeployment) && deployments.some((deployment) =>
    deployment?.source === "wrangler" &&
    deployment.id === pin.deployment_id &&
    deployment.versions?.some((version) => version?.version_id === pin.version_id)
  );
}

function validateServicePin(binding, expectedName, pin, accountId) {
  if (
    pin?.name !== expectedName || pin?.account_id !== accountId ||
    !isCanonicalUuid(pin?.deployment_id) ||
    !isCanonicalUuid(pin?.version_id) ||
    !/^[0-9a-f]{64}$/.test(pin?.uploaded_source_sha256 ?? "")
  ) throw new Error(`dependency_pin_invalid:${binding}`);
}

async function qualifyTarget(context, runner, kind, name) {
  const result = classifyDeploymentsListResult(
    await runner.run(`target:${kind}`, ["npx", "wrangler", "deployments", "list", "--name", name, "--json"]),
  );
  if (result.state === "unknown") throw new Error(`target_${kind}_unknown`);
  if (result.state === "absent") return { existed: false, created: false };
  const deployments = result.deployments;
  const pin = context.approvedDependencies.workers?.[kind];
  if (
    pin?.name !== name || pin?.account_id !== context.accountId || pin?.prefix !== context.prefix ||
    !isCanonicalUuid(pin?.deployment_id) || !isCanonicalUuid(pin?.version_id) ||
    !/^[0-9a-f]{64}$/.test(pin?.uploaded_source_sha256 ?? "") ||
    !exactDeployment(deployments, pin)
  ) throw new Error(`target_${kind}_unowned`);
  return { existed: true, created: false };
}

export async function executeApply(context, runner) {
  if (
    !context.privateConfig.includes('"workers_dev": false') ||
    /"routes?"\s*:/.test(context.privateConfig)
  ) throw new Error("private_worker_not_fail_closed");
  if (
    !/^[0-9a-f]{64}$/.test(context.privateUploadedSourceSha256 ?? "") ||
    !/^[0-9a-f]{64}$/.test(context.publicUploadedSourceSha256 ?? "")
  ) throw new Error("uploaded_source_sha256_invalid");
  const runStartedAt = context.runStartedAt ?? new Date(Math.floor(Date.now() / 1000) * 1000).toISOString();
  const resources = {
    private: { existed: false, created: false },
    public: { existed: false, created: false },
  };
  const evidence = {
    schema_version: "3",
    account_id: context.accountId,
    prefix: context.prefix,
    workers_subdomain: context.workersSubdomain,
    callback_base: context.publicCallbackBase,
    google_oauth_redirect_uri: context.googleOauthRedirectUri,
    checks: [],
    created_resources: [],
    cleanup: [],
  };
  try {
    // This local clean build/package/lock/sentinel/hash gate is deliberately
    // first. No Wrangler/curl command is allowed before it succeeds.
    await checked(runner, "hermetic_preflight", context.preflightCommand ?? [
      "node", "workerd-tests/scripts/production-preflight.mjs",
    ]);
    evidence.checks.push("hermetic_production_preflight");

    await verifyLiveWorkersSubdomain({
      accountId: context.accountId,
      workersSubdomain: context.workersSubdomain,
      apiToken: context.cloudflareApiToken,
      fetchImpl: context.fetchImpl,
    });
    evidence.checks.push("live_workers_subdomain");

    const whoami = await runner.run("auth", ["npx", "wrangler", "whoami", "--json"]);
    if (whoami.status !== 0) throw new Error("auth_failed");
    assertAuthenticatedAccount(whoami.stdout, context.accountId);
    evidence.checks.push("auth_account");

    const ownership = context.approvedDependencies;
    if (
      ownership.schema_version !== "3" || ownership.account_id !== context.accountId ||
      ownership.prefix !== context.prefix || ownership.d1_database_id !== context.d1Id
    ) throw new Error("ownership_mismatch");

    resources.private = await qualifyTarget(context, runner, "private", context.privateName);
    resources.public = await qualifyTarget(context, runner, "public", context.publicName);
    evidence.checks.push("target_absence_or_ownership");

    const expectedServices = {
      AUTH_DRIVER_SERVICE: context.authDriverService,
      GOOGLE_PROVIDER_SERVICE: context.googleProviderService,
      GOOGLE_TOKEN_SERVICE: context.googleTokenService,
    };
    for (const [binding, service] of Object.entries(expectedServices)) {
      const pin = ownership.services?.[binding];
      validateServicePin(binding, service, pin, context.accountId);
      const deployments = parseJson(
        await runner.run(`dependency:${binding}`, ["npx", "wrangler", "deployments", "list", "--name", service, "--json"]),
        `dependency:${binding}`,
      );
      if (!exactDeployment(deployments, pin)) throw new Error(`dependency_pin_mismatch:${binding}`);
    }
    evidence.checks.push("immutable_dependency_pins");

    const d1 = parseJson(await runner.run("d1", ["npx", "wrangler", "d1", "list", "--json"]), "d1");
    if (!Array.isArray(d1) || !d1.some((db) => db.uuid === context.d1Id)) throw new Error("d1_missing");
    evidence.checks.push("d1_qualified");

    const privateDeployment = await deployPrivateWorker({
      runner,
      step: "private_worker",
      name: context.privateName,
      accountId: context.accountId,
      config: context.privateConfigPath,
      secrets: context.privateSecrets,
      ownedUpdate: resources.private.existed,
      ownership: ownership.workers?.private,
      uploadedSourceSha256: context.privateUploadedSourceSha256,
      runStartedAt,
    });
    resources.private = { existed: resources.private.existed, created: privateDeployment.created_by_run };
    if (resources.private.created) evidence.created_resources.push(context.privateName);
    evidence.worker_secrets = { [context.privateName]: privateDeployment.secrets };
    evidence.deployments = { [context.privateName]: {
      deployment_id: privateDeployment.deployment_id,
      version_id: privateDeployment.version_id,
      deployment_count: privateDeployment.deployment_count,
      triggered_by_annotations: privateDeployment.triggered_by_annotations,
      run_started_at: privateDeployment.run_started_at,
      uploaded_source_sha256: privateDeployment.uploaded_source_sha256,
    } };
    evidence.checks.push("fence_aware_private_deployed_secrets_verified_and_captured");
    evidence.migration_state = "attempting_forward_only";
    await checked(runner, "migrations", [
      "npx", "wrangler", "d1", "migrations", "apply", context.d1Name,
      "--remote", "--config", context.privateConfigPath,
    ]);
    evidence.migration_state = "applied_forward_only";
    evidence.checks.push("0003_production_v2_cutover_applied");
    const publicDeployment = await deployPrivateWorker({
      runner,
      step: "public_worker",
      name: context.publicName,
      accountId: context.accountId,
      config: context.publicConfigPath,
      secrets: context.publicSecrets ?? {},
      ownedUpdate: resources.public.existed,
      ownership: ownership.workers?.public,
      uploadedSourceSha256: context.publicUploadedSourceSha256,
      runStartedAt,
    });
    resources.public = { existed: resources.public.existed, created: publicDeployment.created_by_run };
    if (resources.public.created) evidence.created_resources.push(context.publicName);
    evidence.worker_secrets[context.publicName] = publicDeployment.secrets;
    evidence.deployments[context.publicName] = {
      deployment_id: publicDeployment.deployment_id,
      version_id: publicDeployment.version_id,
      deployment_count: publicDeployment.deployment_count,
      triggered_by_annotations: publicDeployment.triggered_by_annotations,
      run_started_at: publicDeployment.run_started_at,
      uploaded_source_sha256: publicDeployment.uploaded_source_sha256,
    };
    evidence.checks.push("public_deployed_last_and_captured");

    for (const route of ["/health", "/ready"]) {
      await checked(runner, `readiness:${route}`, [
        "curl", "--fail", "--silent", "--show-error", `${context.publicCallbackBase}${route}`,
      ]);
    }
    const callback = await checked(runner, "readiness:oauth_callback_400", [
      "curl", "--silent", "--output", "/dev/null", "--write-out", "%{http_code}",
      context.googleOauthRedirectUri,
    ]);
    if (callback.stdout.trim() !== "400") throw new Error("oauth_callback_status_mismatch");
    const smoke = parseJson(await runner.run("smoke", [
      "curl", "--fail", "--silent", "--show-error", `${context.publicCallbackBase}/health`,
    ]), "smoke");
    if (smoke.status !== "ok") throw new Error("smoke_failed");
    evidence.checks.push("health_ready_callback_400_smoke");
    evidence.config_sha256 = createHash("sha256").update(context.privateConfig).digest("hex");
    evidence.public_config_sha256 = createHash("sha256").update(context.publicConfig).digest("hex");
    evidence.status = "qualified";
    return evidence;
  } catch (error) {
    // Default rollback is creation-scoped. Updating an owned pre-existing
    // Worker never makes it deletion-owned by this run. After migration,
    // preserve D1 and broker-private while removing only a created public facade.
    let cleanupFailure;
    const cleanupCreatedWorker = async (name, step) => {
      try {
        evidence.cleanup.push(await deleteWorkerAndVerifyAbsent({
          runner, name, deleteStep: step, failureMessage: `${step}_failed`,
        }));
      } catch (cleanupError) {
        evidence.cleanup.push(cleanupError.cleanupEvidence);
        cleanupFailure ??= cleanupError;
      }
    };
    if (resources.public.created) await cleanupCreatedWorker(context.publicName, "cleanup_public");
    if (resources.private.created && evidence.migration_state === undefined) {
      await cleanupCreatedWorker(context.privateName, "cleanup_private");
    }
    if (evidence.migration_state !== undefined) {
      evidence.migration_state = "forward_fix_required";
      evidence.cleanup.push({ resource: context.d1Name, status: "preserved_forward_only" });
      evidence.cleanup.push({ resource: context.privateName, status: "preserved_fence_authority" });
    }
    evidence.status = "failed";
    evidence.failure = error instanceof Error ? error.message : "unknown_failure";
    if (cleanupFailure) {
      const combined = new Error(`${evidence.failure}; ${cleanupFailure.message}`, { cause: error });
      combined.evidence = evidence;
      throw combined;
    }
    error.evidence = evidence;
    throw error;
  }
}
