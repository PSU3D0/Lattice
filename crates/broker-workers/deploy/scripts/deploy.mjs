import { mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { executeApply, redactedPlan } from "./deploy-lib.mjs";
import { renderD1DatabaseId, validateD1Id } from "./cloudflare-identifiers.mjs";
import { loadPrivateWorkerSecrets } from "./private-worker-deploy-lib.mjs";
import { verifyBundle } from "./operator-artifacts.mjs";
import {
  PRIVATE_DIGEST_VARS,
  replaceJsonStringPlaceholdersOnce,
  validateRenderedConfig,
  withRenderedDeployConfigs,
  writeRenderedConfigEvidence,
} from "./rendered-config.mjs";
import { validatePublicCallbackBase, validateWorkersSubdomain } from "./workers-subdomain.mjs";
import { computeUploadedSourceSha256 } from "./uploaded-source.mjs";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const name = process.argv[index];
  if (!name?.startsWith("--")) throw new Error("arguments must be explicit --name value pairs");
  args.set(name, process.argv[index + 1]);
}
const required = [
  "--account-id", "--prefix", "--evidence-dir", "--d1-id", "--approved-dependencies", "--secrets-file",
  "--google-provider-service", "--google-token-service", "--auth-driver-service", "--workers-subdomain", "--public-callback-base",
  "--spend-limit-usd", "--rate-limit-per-minute",
];
for (const name of required) if (!args.has(name)) throw new Error(`missing ${name}`);
const accountId = args.get("--account-id");
const prefix = args.get("--prefix");
const evidenceInput = args.get("--evidence-dir");
const d1Id = args.get("--d1-id");
const dependencyPath = args.get("--approved-dependencies");
const workersSubdomain = validateWorkersSubdomain(args.get("--workers-subdomain"));
if (!/^[0-9a-f]{32}$/.test(accountId)) throw new Error("account id must be exact 32 lowercase hex");
if (!/^lattice-(?:b5|c5)-[a-z0-9]{6,20}$/.test(prefix)) throw new Error("prefix is outside the disposable broker namespace");
validateD1Id(d1Id);
if (!isAbsolute(evidenceInput) || !isAbsolute(dependencyPath)) throw new Error("evidence and dependency paths must be absolute");
if (!/^\d+(\.\d{1,2})?$/.test(args.get("--spend-limit-usd"))) throw new Error("invalid spend limit");
if (!/^[1-9]\d{0,5}$/.test(args.get("--rate-limit-per-minute"))) throw new Error("invalid rate limit");
for (const name of [args.get("--google-provider-service"), args.get("--google-token-service"), args.get("--auth-driver-service")]) {
  if (!/^[a-z0-9-]{6,63}$/.test(name)) throw new Error("invalid approved service name");
}
const apply = args.get("--mode") === "apply";
if (apply && (args.get("--approve-create-disposable") !== "yes" || args.get("--approve-cleanup") !== "yes")) {
  throw new Error("apply requires exact create and cleanup approvals");
}
if (apply && !process.env.CLOUDFLARE_API_TOKEN) throw new Error("apply requires CLOUDFLARE_API_TOKEN");

await mkdir(evidenceInput, { recursive: true, mode: 0o700 });
const evidenceDir = await realpath(evidenceInput);
const root = resolve(new URL("../..", import.meta.url).pathname);
if (evidenceDir.startsWith(`${root}/`)) throw new Error("evidence directory must be outside the repository");
const secretValues = await loadPrivateWorkerSecrets(args.get("--secrets-file"), resolve(root, "../.."));
const approvedDependencies = JSON.parse(await readFile(dependencyPath, "utf8"));
const artifacts = approvedDependencies.artifacts;
const operatorBundleJcs=approvedDependencies.operator_bundle_jcs;
const operatorTrustRoot=approvedDependencies.operator_trust_root;
if(typeof operatorBundleJcs!=="string"||!operatorTrustRoot)throw new Error("operator signed bundle and trust root are required");
const operatorBundle=JSON.parse(operatorBundleJcs);
verifyBundle(operatorBundle,operatorTrustRoot);
const operatorBundleHash=`sha256:${createHash("sha256").update(operatorBundleJcs).digest("hex")}`;
if(approvedDependencies.operator_bundle_hash!==operatorBundleHash)throw new Error("operator bundle hash mismatch");
const verifiedBundlePath=join(evidenceDir,"operator-artifact-bundle.json"),verifiedTrustPath=join(evidenceDir,"operator-trust-root.json");
await writeFile(verifiedBundlePath,operatorBundleJcs,{mode:0o600});await writeFile(verifiedTrustPath,JSON.stringify(operatorTrustRoot),{mode:0o600});
const rustVerification=spawnSync("cargo",["run","--quiet","--bin","broker-artifact-verifier","--","--bundle",verifiedBundlePath,"--trust-root",verifiedTrustPath,"--now",new Date().toISOString().replace(/\.\d{3}Z$/,"Z")],{cwd:root,encoding:"utf8"});
if(rustVerification.status!==0)throw new Error("shared Rust operator artifact verification failed");
const artifactNames = ["deployment_authority_key_id","deployment_authority_public_key_b64u","deployment_contract_set_jcs","deployment_standing_authority_jcs","historical_archive_authority_key_id","historical_archive_authority_public_key_b64u","legacy_cutover_authority_key_id","legacy_cutover_authority_public_key_b64u","generic_profile_authority_public_key_b64u","generic_profile_registry_jcs","generic_activation_recipient_key_id","generic_activation_recipient_public_key_b64u"];
if (!artifacts || artifactNames.some((name) => typeof artifacts[name] !== "string" || artifacts[name].length === 0 || artifacts[name].includes("REPLACE_"))) throw new Error("signed artifact bundle missing or placeholder");
if(artifacts.deployment_authority_key_id!==operatorTrustRoot.key_id||artifacts.deployment_authority_public_key_b64u!==operatorTrustRoot.public_key_b64u)throw new Error("deployment authority does not match operator bundle trust root");
if(operatorBundle.activation_recipient?.key_id!==artifacts.generic_activation_recipient_key_id||operatorBundle.activation_recipient?.public_key_b64u!==artifacts.generic_activation_recipient_public_key_b64u)throw new Error("activation recipient pin does not match signed operator bundle");
if(artifacts.generic_profile_registry_jcs!==operatorBundleJcs)throw new Error("installed registry bytes must be the exact verified operator bundle");
for (const name of ["deployment_contract_set_jcs","deployment_standing_authority_jcs","generic_profile_registry_jcs"]) JSON.parse(artifacts[name]);
const bundledStanding=operatorBundle.artifacts.deployment_standing_authority?.map(entry=>entry.canonical_jcs)??[];
const bundledContracts=operatorBundle.artifacts.deployment_contract_set?.map(entry=>entry.canonical_jcs)??[];
if(!bundledStanding.includes(artifacts.deployment_standing_authority_jcs)||!bundledContracts.includes(artifacts.deployment_contract_set_jcs))throw new Error("installed deployment authority is not the exact verified bundle artifact");
const brokerManifest = JSON.parse(await readFile(resolve(root, "../../broker-workers-package/build-manifest.json"), "utf8"));
if (!/^[0-9a-f]{64}$/.test(brokerManifest.wasm_sha256 ?? "")) throw new Error("broker WASM hash missing");
const publicUploadedSourceSha256 = await computeUploadedSourceSha256([{
  path: "deploy/public-callback/src/index.mjs",
  file: join(root, "deploy/public-callback/src/index.mjs"),
}]);
const privateName = `${prefix}-broker-private`;
const publicName = `${prefix}-broker-public`;
const d1Name = `${prefix}-broker`;
const { publicCallbackBase, googleOauthRedirectUri } = validatePublicCallbackBase(
  prefix,
  workersSubdomain,
  args.get("--public-callback-base"),
);
const artifactReplacements = {
  REPLACE_WITH_PACKAGED_PRODUCTION_WASM_SHA256: `sha256:${brokerManifest.wasm_sha256}`,
  REPLACE_WITH_OPERATOR_KEY_ID: artifacts.deployment_authority_key_id,
  REPLACE_WITH_OPERATOR_ED25519_KEY: artifacts.deployment_authority_public_key_b64u,
  REPLACE_WITH_SIGNED_CANONICAL_CONTRACT_SET: artifacts.deployment_contract_set_jcs,
  REPLACE_WITH_SIGNED_CANONICAL_STANDING_AUTHORITY: artifacts.deployment_standing_authority_jcs,
  REPLACE_WITH_ARCHIVE_ROOT_KEY_ID: artifacts.historical_archive_authority_key_id,
  REPLACE_WITH_ARCHIVE_ROOT_ED25519_KEY: artifacts.historical_archive_authority_public_key_b64u,
  REPLACE_WITH_CUTOVER_ROOT_KEY_ID: artifacts.legacy_cutover_authority_key_id,
  REPLACE_WITH_CUTOVER_ROOT_ED25519_KEY: artifacts.legacy_cutover_authority_public_key_b64u,
  REPLACE_WITH_PROFILE_AUTHORITY_ED25519_KEY: artifacts.generic_profile_authority_public_key_b64u,
  REPLACE_WITH_SIGNED_PROFILE_REGISTRY: artifacts.generic_profile_registry_jcs,
  REPLACE_WITH_PRIVATE_CHANNEL_KEY_ID: artifacts.generic_activation_recipient_key_id,
  REPLACE_WITH_PRIVATE_CHANNEL_X25519_PUBLIC_KEY: artifacts.generic_activation_recipient_public_key_b64u,
  REPLACE_WITH_OPERATOR_ARTIFACT_BUNDLE: operatorBundleJcs,
  REPLACE_WITH_OPERATOR_ARTIFACT_BUNDLE_HASH: operatorBundleHash,
  REPLACE_WITH_OPERATOR_BUNDLE_KEY_ID: operatorTrustRoot.key_id,
  REPLACE_WITH_OPERATOR_BUNDLE_PUBLIC_KEY: operatorTrustRoot.public_key_b64u,
};
const uploadedSourceReplacements = {
  REPLACE_WITH_AUTH_DRIVER_SOURCE_SHA256: approvedDependencies.services?.AUTH_DRIVER_SERVICE?.uploaded_source_sha256,
  REPLACE_WITH_GOOGLE_TOKEN_SOURCE_SHA256: approvedDependencies.services?.GOOGLE_TOKEN_SERVICE?.uploaded_source_sha256,
  REPLACE_WITH_GOOGLE_PROVIDER_SOURCE_SHA256: approvedDependencies.services?.GOOGLE_PROVIDER_SERVICE?.uploaded_source_sha256,
};
for (const [placeholder, digest] of Object.entries(uploadedSourceReplacements)) {
  if (!/^[0-9a-f]{64}$/.test(digest ?? "")) throw new Error(`approved dependency uploaded source digest invalid:${placeholder}`);
  uploadedSourceReplacements[placeholder] = `sha256:${digest}`;
}
const privateTemplatePath = join(root, "wrangler.jsonc");
let privateConfig = renderD1DatabaseId(await readFile(privateTemplatePath, "utf8"), d1Id);
privateConfig = replaceJsonStringPlaceholdersOnce(privateConfig, {
  "lattice-broker-template-google-provider": args.get("--google-provider-service"),
  "lattice-broker-template-google-token": args.get("--google-token-service"),
  "lattice-broker-template-auth-driver": args.get("--auth-driver-service"),
  "lattice-broker-template-private": privateName,
  "lattice-broker-template": d1Name,
  "https://invalid.example": publicCallbackBase,
  ...uploadedSourceReplacements,
  ...artifactReplacements,
})
  .replace('"AI_GATEWAY_SPEND_LIMIT_USD": "0"', `"AI_GATEWAY_SPEND_LIMIT_USD": "${args.get("--spend-limit-usd")}"`)
  .replace('"AI_GATEWAY_RATE_LIMIT_PER_MINUTE": "0"', `"AI_GATEWAY_RATE_LIMIT_PER_MINUTE": "${args.get("--rate-limit-per-minute")}"`);
validateRenderedConfig(privateConfig, {
  requiredDigestVars: PRIVATE_DIGEST_VARS,
  requireBundleHashInvariant: true,
});
if (privateConfig.includes("lattice-broker-template") || privateConfig.includes("invalid.example")) throw new Error("rendered private config retains a template value");
for (const requiredBinding of [
  '"CONNECTION_REFRESH_DO"', '"CREDENTIAL_STATE_V2_DO"', '"V2_AUTHORITY_DO"', '"BROKER_DB"',
  `"${args.get("--google-provider-service")}"`, `"${args.get("--google-token-service")}"`, `"${args.get("--auth-driver-service")}"`,
]) {
  if (!privateConfig.includes(requiredBinding)) throw new Error("rendered private config is missing an exact required resource");
}
const publicTemplatePath = join(root, "deploy/public-callback/wrangler.jsonc");
let publicConfig = replaceJsonStringPlaceholdersOnce(await readFile(publicTemplatePath, "utf8"), {
  "lattice-broker-template-public": publicName,
  "lattice-broker-template-private": privateName,
})
  .replace('"workers_dev": false', '"workers_dev": true');
validateRenderedConfig(publicConfig);
if (publicConfig.includes("lattice-broker-template")) throw new Error("rendered public config retains a template value");
await writeRenderedConfigEvidence({ evidenceDir, privateConfig, publicConfig });
const baseContext = {
  accountId, prefix, d1Id, d1Name, privateName, publicName, workersSubdomain,
  publicCallbackBase, googleOauthRedirectUri,
  cloudflareApiToken: process.env.CLOUDFLARE_API_TOKEN,
  googleProviderService: args.get("--google-provider-service"),
  googleTokenService: args.get("--google-token-service"),
  authDriverService: args.get("--auth-driver-service"),
  approvedDependencies, privateConfig, publicConfig,
  privateUploadedSourceSha256: brokerManifest.wasm_sha256,
  publicUploadedSourceSha256,
  privateSecrets: secretValues["broker-private"], publicSecrets: secretValues["broker-public"],
};
if (!apply) {
  const plan = redactedPlan(baseContext);
  await writeFile(join(evidenceDir, "dry-run-plan.json"), `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
  console.log(JSON.stringify(plan, null, 2));
  console.log("dry-run complete; zero remote commands executed");
  process.exit(0);
}
const runner = {
  async run(_step, command, options = {}) {
    const result = spawnSync(command[0], command.slice(1), {
      cwd: root,
      encoding: "utf8",
      input: options.input,
      env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId },
    });
    return { status: result.status ?? 1, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
  },
};
await withRenderedDeployConfigs({
  privateTemplatePath,
  publicTemplatePath,
  privateConfig,
  publicConfig,
}, async ({ privateConfigPath, publicConfigPath }) => {
  const context = { ...baseContext, privateConfigPath, publicConfigPath };
  try {
    const evidence = await executeApply(context, runner);
    await writeFile(join(evidenceDir, "qualification-evidence.json"), `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
    console.log("B5 approved resources qualified and deployed");
  } catch (error) {
    const evidence = error?.evidence ?? { schema_version: "1", status: "failed", failure: "qualification_failed" };
    await writeFile(join(evidenceDir, "qualification-evidence.json"), `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
    throw error;
  }
});
