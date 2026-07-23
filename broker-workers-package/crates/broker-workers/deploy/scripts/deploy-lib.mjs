import { createHash } from "node:crypto";

export const REQUIRED_SECRET_NAMES = [
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
];

export function redactedPlan(context) {
  return {
    schema_version: "2",
    account_id: context.accountId,
    prefix: context.prefix,
    resources: {
      private_worker: context.privateName,
      public_worker: context.publicName,
      d1_database_id: context.d1Id,
      durable_objects: ["ConnectionRefreshDurableObject", "CredentialStateDurableObject", "V2AuthorityDurableObject"],
      approved_services: [context.authDriverService, context.googleProviderService, context.googleTokenService],
    },
    callback_base: context.publicCallbackBase,
    required_secret_names: REQUIRED_SECRET_NAMES,
    steps: [
      "clean_hermetic_production_preflight",
      "qualify_auth_account",
      "prove_target_absent_or_owned",
      "verify_immutable_dependency_pins",
      "qualify_secret_names",
      "deploy_fence_aware_private",
      "apply_checked_in_migrations",
      "deploy_public",
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
  return Array.isArray(deployments) && deployments.some((deployment) =>
    deployment.id === pin.deployment_id &&
    (deployment.source_hash ?? deployment.metadata?.source_hash) === pin.source_hash
  );
}

function validateServicePin(binding, expectedName, pin, accountId) {
  if (
    pin?.name !== expectedName || pin?.account_id !== accountId ||
    !/^[A-Za-z0-9._:-]{6,256}$/.test(pin?.deployment_id ?? "") ||
    !/^[0-9a-f]{64}$/.test(pin?.source_hash ?? "")
  ) throw new Error(`dependency_pin_invalid:${binding}`);
}

async function qualifyTarget(context, runner, kind, name) {
  const deployments = parseJson(
    await runner.run(`target:${kind}`, ["npx", "wrangler", "deployments", "list", "--name", name, "--json"]),
    `target:${kind}`,
  );
  if (!Array.isArray(deployments)) throw new Error(`target_${kind}_invalid`);
  if (deployments.length === 0) return { existed: false, created: false };
  const pin = context.approvedDependencies.workers?.[kind];
  if (
    pin?.name !== name || pin?.account_id !== context.accountId || pin?.prefix !== context.prefix ||
    !exactDeployment(deployments, pin)
  ) throw new Error(`target_${kind}_unowned`);
  return { existed: true, created: false };
}

export async function executeApply(context, runner) {
  const resources = {
    private: { existed: false, created: false },
    public: { existed: false, created: false },
  };
  const evidence = {
    schema_version: "2",
    account_id: context.accountId,
    prefix: context.prefix,
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

    const whoami = parseJson(await runner.run("auth", ["npx", "wrangler", "whoami", "--json"]), "auth");
    if (whoami.account_id !== context.accountId) throw new Error("account_mismatch");
    evidence.checks.push("auth_account");

    const ownership = context.approvedDependencies;
    if (
      ownership.schema_version !== "2" || ownership.account_id !== context.accountId ||
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

    const secrets = parseJson(await runner.run("secrets", [
      "npx", "wrangler", "secret", "list", "--name", context.privateName, "--format", "json",
    ]), "secrets");
    const names = new Set((Array.isArray(secrets) ? secrets : []).map((item) => item.name));
    if (REQUIRED_SECRET_NAMES.some((name) => !names.has(name))) throw new Error("secret_missing");
    evidence.checks.push("secret_names");

    await checked(runner, "deploy_private", ["npx", "wrangler", "deploy", "--config", context.privateConfigPath]);
    if (!resources.private.existed) {
      resources.private.created = true;
      evidence.created_resources.push(context.privateName);
    }
    evidence.checks.push("fence_aware_private_deployed");
    evidence.migration_state = "attempting_forward_only";
    await checked(runner, "migrations", [
      "npx", "wrangler", "d1", "migrations", "apply", context.d1Name,
      "--remote", "--config", context.privateConfigPath,
    ]);
    evidence.migration_state = "applied_forward_only";
    evidence.checks.push("migrations_applied");
    await checked(runner, "deploy_public", ["npx", "wrangler", "deploy", "--config", context.publicConfigPath]);
    if (!resources.public.existed) {
      resources.public.created = true;
      evidence.created_resources.push(context.publicName);
    }

    for (const route of ["/health", "/ready"]) {
      await checked(runner, `readiness:${route}`, [
        "curl", "--fail", "--silent", "--show-error", `${context.publicCallbackBase}${route}`,
      ]);
    }
    const callback = await checked(runner, "readiness:oauth_callback_400", [
      "curl", "--silent", "--output", "/dev/null", "--write-out", "%{http_code}",
      `${context.publicCallbackBase}/v0.2/credential-callback`,
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
    // Worker never makes it deletion-owned by this run.
    if (resources.public.created) {
      const result = await runner.run("cleanup_public", ["npx", "wrangler", "delete", "--name", context.publicName, "--force"]);
      evidence.cleanup.push({ resource: context.publicName, status: result.status });
    }
    if (resources.private.created && evidence.migration_state === undefined) {
      const result = await runner.run("cleanup_private", ["npx", "wrangler", "delete", "--name", context.privateName, "--force"]);
      evidence.cleanup.push({ resource: context.privateName, status: result.status });
    }
    if (evidence.migration_state !== undefined) {
      evidence.migration_state = "forward_fix_required";
      evidence.cleanup.push({ resource: context.d1Name, status: "preserved_forward_only" });
      evidence.cleanup.push({ resource: context.privateName, status: "preserved_fence_authority" });
    }
    evidence.status = "failed";
    evidence.failure = error instanceof Error ? error.message : "unknown_failure";
    error.evidence = evidence;
    throw error;
  }
}
