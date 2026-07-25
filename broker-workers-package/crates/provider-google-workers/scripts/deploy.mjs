import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { assertAuthenticatedAccount } from "../../broker-workers/deploy/scripts/cloudflare-identifiers.mjs";
import { validatePublicCallbackBase, verifyLiveWorkersSubdomain } from "../../broker-workers/deploy/scripts/workers-subdomain.mjs";
import {
  deleteWorkerAndVerifyAbsent,
  deployPrivateWorker,
  loadPrivateWorkerSecrets,
} from "../../broker-workers/deploy/scripts/private-worker-deploy-lib.mjs";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const name = process.argv[index];
  if (!name?.startsWith("--")) throw new Error("arguments must be explicit --name value pairs");
  args.set(name, process.argv[index + 1]);
}
for (const name of ["--account-id", "--prefix", "--workers-subdomain", "--callback-uri", "--evidence-dir", "--secrets-file"]) {
  if (!args.has(name)) throw new Error(`missing ${name}`);
}
const accountId = args.get("--account-id");
const prefix = args.get("--prefix");
const workersSubdomain = args.get("--workers-subdomain");
const callbackUri = args.get("--callback-uri");
const evidenceDir = args.get("--evidence-dir");
if (!/^[0-9a-f]{32}$/.test(accountId)) throw new Error("invalid account id");
if (!/^lattice-c5-[a-z0-9]{6,20}$/.test(prefix)) throw new Error("invalid C5 ownership prefix");
if (!isAbsolute(evidenceDir)) throw new Error("evidence directory must be absolute");
const expected = validatePublicCallbackBase(
  prefix,
  workersSubdomain,
  `https://${prefix}-broker-public.${workersSubdomain}.workers.dev`,
);
const obsoleteCallback = `https://${prefix}-broker-public.workers.dev/v0.2/credential-callback`;
if (callbackUri === obsoleteCallback) throw new Error("callback URI is missing the required account Workers subdomain label");
if (callbackUri !== expected.googleOauthRedirectUri) throw new Error("invalid exact callback URI");
const repositoryRoot = resolve(new URL("../../..", import.meta.url).pathname);
const secretValues = await loadPrivateWorkerSecrets(args.get("--secrets-file"), repositoryRoot);
const manifest = JSON.parse(await readFile("build-manifest.json", "utf8"));
if (!/^[0-9a-f]{64}$/.test(manifest.source_hash ?? "")) throw new Error("source manifest invalid");
const tokenName = `${prefix}-google-token-egress`;
const providerName = `${prefix}-google-provider-egress`;
const plan = {
  schema_version: "0.3", account_id: accountId, prefix,
  operator_computed_uploaded_source_sha256: manifest.source_hash,
  workers_subdomain: workersSubdomain,
  public_callback_base: expected.publicCallbackBase,
  callback_uri: callbackUri,
  workers: [
    { kind: "token", name: tokenName, config: "wrangler.token.jsonc", secrets: [...Object.keys(secretValues["google-token-egress"]), "GOOGLE_OAUTH_REDIRECT_URI"].map((name) => ({ name, install_status: "planned" })) },
    { kind: "provider", name: providerName, config: "wrangler.provider.jsonc", secrets: Object.keys(secretValues["google-provider-egress"]).map((name) => ({ name, install_status: "planned" })) },
  ],
  steps: [
    "verify_live_workers_subdomain", "qualify_account",
    "prove_token_absent", "deploy_token_private", "install_token_secrets", "verify_token_secret_names", "capture_token_deployment",
    "prove_provider_absent", "deploy_provider_private", "install_provider_secrets", "verify_provider_secret_names", "capture_provider_deployment",
    "write_ownership_state",
  ],
};
await mkdir(evidenceDir, { recursive: true, mode: 0o700 });
await writeFile(join(evidenceDir, "google-egress-plan.json"), `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
if (args.get("--mode") !== "apply") {
  console.log(JSON.stringify(plan, null, 2));
  console.log("dry-run complete; zero remote commands executed");
  process.exit(0);
}
if (args.get("--approve-private-deploy") !== "yes" || !process.env.CLOUDFLARE_API_TOKEN) throw new Error("apply approval and API token required");
await verifyLiveWorkersSubdomain({ accountId, workersSubdomain, apiToken: process.env.CLOUDFLARE_API_TOKEN });
const runner = {
  async run(_step, command, options = {}) {
    const result = spawnSync(command[0], command.slice(1), {
      encoding: "utf8",
      input: options.input,
      env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId },
    });
    return { status: result.status ?? 1, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
  },
};
const whoami = await runner.run("auth", ["npx", "wrangler", "whoami", "--json"]);
if (whoami.status !== 0) throw new Error("account authentication failed");
assertAuthenticatedAccount(whoami.stdout, accountId);
const created = [];
const runStartedAt = new Date(Math.floor(Date.now() / 1000) * 1000).toISOString();
try {
  const token = await deployPrivateWorker({
    runner, step: "token", name: tokenName, accountId, config: "wrangler.token.jsonc",
    secrets: secretValues["google-token-egress"],
    derivedSecrets: { GOOGLE_OAUTH_REDIRECT_URI: callbackUri },
    uploadedSourceSha256: manifest.source_hash,
    runStartedAt,
  });
  created.push(token);
  const provider = await deployPrivateWorker({
    runner, step: "provider", name: providerName, accountId, config: "wrangler.provider.jsonc",
    secrets: secretValues["google-provider-egress"],
    uploadedSourceSha256: manifest.source_hash,
    runStartedAt,
  });
  created.push(provider);
  const workers = [
    { ...plan.workers[0], ...token, secrets: token.secrets },
    { ...plan.workers[1], ...provider, secrets: provider.secrets },
  ];
  const state = { ...plan, owner: "lattice-provider-google-workers", created_by_run: true, workers };
  await writeFile(join(evidenceDir, "google-egress-ownership.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  console.log("private Google egress deployed");
} catch (error) {
  let cleanupFailure;
  const cleanup = [];
  for (const worker of created.reverse()) {
    if (!worker.created_by_run) continue;
    try {
      cleanup.push(await deleteWorkerAndVerifyAbsent({
        runner,
        name: worker.name,
        deleteStep: `${worker.name}:cleanup`,
        failureMessage: "private_egress_cleanup_failed",
      }));
    } catch (cleanupError) {
      cleanup.push(cleanupError.cleanupEvidence);
      cleanupFailure = cleanupError;
      break;
    }
  }
  if (cleanupFailure) {
    const original = error instanceof Error ? error.message : String(error);
    const combined = new Error(`${original}; private_egress_cleanup_failed`, { cause: error });
    combined.cleanupEvidence = cleanup;
    throw combined;
  }
  if (error && typeof error === "object") error.cleanupEvidence = cleanup;
  throw error;
}
