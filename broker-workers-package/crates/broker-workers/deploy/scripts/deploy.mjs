import { mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { executeApply, redactedPlan } from "./deploy-lib.mjs";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const name = process.argv[index];
  if (!name?.startsWith("--")) throw new Error("arguments must be explicit --name value pairs");
  args.set(name, process.argv[index + 1]);
}
const required = [
  "--account-id", "--prefix", "--evidence-dir", "--d1-id", "--approved-dependencies",
  "--google-provider-service", "--google-token-service", "--public-callback-base",
  "--google-oauth-client-id", "--spend-limit-usd", "--rate-limit-per-minute",
];
for (const name of required) if (!args.has(name)) throw new Error(`missing ${name}`);
const accountId = args.get("--account-id");
const prefix = args.get("--prefix");
const evidenceInput = args.get("--evidence-dir");
const d1Id = args.get("--d1-id");
const dependencyPath = args.get("--approved-dependencies");
if (!/^[0-9a-f]{32}$/.test(accountId)) throw new Error("account id must be exact 32 lowercase hex");
if (!/^lattice-b5-[a-z0-9]{6,20}$/.test(prefix)) throw new Error("prefix is outside the disposable B5 namespace");
if (!/^[0-9a-f]{32}$/.test(d1Id)) throw new Error("D1 id must be exact 32 lowercase hex");
if (!isAbsolute(evidenceInput) || !isAbsolute(dependencyPath)) throw new Error("evidence and dependency paths must be absolute");
if (!/^\d+(\.\d{1,2})?$/.test(args.get("--spend-limit-usd"))) throw new Error("invalid spend limit");
if (!/^[1-9]\d{0,5}$/.test(args.get("--rate-limit-per-minute"))) throw new Error("invalid rate limit");
if (!/^[A-Za-z0-9._-]{8,512}$/.test(args.get("--google-oauth-client-id"))) throw new Error("invalid OAuth client id");
for (const name of [args.get("--google-provider-service"), args.get("--google-token-service")]) {
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
const approvedDependencies = JSON.parse(await readFile(dependencyPath, "utf8"));
const privateName = `${prefix}-broker-private`;
const publicName = `${prefix}-broker-public`;
const d1Name = `${prefix}-broker`;
const publicCallbackBase = args.get("--public-callback-base");
if (publicCallbackBase !== `https://${publicName}.workers.dev`) {
  throw new Error("public callback base must be the explicitly owned disposable workers.dev hostname");
}
let privateConfig = (await readFile(join(root, "wrangler.jsonc"), "utf8"))
  .replaceAll("lattice-broker-template-private", privateName)
  .replaceAll("lattice-broker-template-google-provider", args.get("--google-provider-service"))
  .replaceAll("lattice-broker-template-google-token", args.get("--google-token-service"))
  .replaceAll("lattice-broker-template", d1Name)
  .replaceAll("00000000000000000000000000000000", d1Id)
  .replaceAll("https://invalid.example", publicCallbackBase)
  .replaceAll("configure-at-deploy.invalid", args.get("--google-oauth-client-id"))
  .replace('"AI_GATEWAY_SPEND_LIMIT_USD": "0"', `"AI_GATEWAY_SPEND_LIMIT_USD": "${args.get("--spend-limit-usd")}"`)
  .replace('"AI_GATEWAY_RATE_LIMIT_PER_MINUTE": "0"', `"AI_GATEWAY_RATE_LIMIT_PER_MINUTE": "${args.get("--rate-limit-per-minute")}"`);
if (privateConfig.includes("lattice-broker-template") || privateConfig.includes("invalid.example")) throw new Error("rendered private config retains a template value");
for (const requiredBinding of [
  '"BROKER_LEDGER_DO"', '"CONNECTION_REFRESH_DO"', '"BROKER_DB"',
  `"${args.get("--google-provider-service")}"`, `"${args.get("--google-token-service")}"`,
]) {
  if (!privateConfig.includes(requiredBinding)) throw new Error("rendered private config is missing an exact required resource");
}
let publicConfig = (await readFile(join(root, "deploy/public-callback/wrangler.jsonc"), "utf8"))
  .replaceAll("lattice-broker-template-public", publicName)
  .replaceAll("lattice-broker-template-private", privateName)
  .replace('"workers_dev": false', '"workers_dev": true');
if (publicConfig.includes("lattice-broker-template")) throw new Error("rendered public config retains a template value");
const privateConfigPath = join(evidenceDir, "wrangler.private.jsonc");
const publicConfigPath = join(evidenceDir, "wrangler.public.jsonc");
await writeFile(privateConfigPath, privateConfig, { mode: 0o600 });
await writeFile(publicConfigPath, publicConfig, { mode: 0o600 });
const context = {
  accountId, prefix, d1Id, d1Name, privateName, publicName, publicCallbackBase,
  googleProviderService: args.get("--google-provider-service"),
  googleTokenService: args.get("--google-token-service"),
  approvedDependencies, privateConfig, publicConfig, privateConfigPath, publicConfigPath,
};
if (!apply) {
  const plan = redactedPlan(context);
  await writeFile(join(evidenceDir, "dry-run-plan.json"), `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
  console.log(JSON.stringify(plan, null, 2));
  console.log("dry-run complete; zero remote commands executed");
  process.exit(0);
}
const runner = {
  async run(_step, command) {
    const result = spawnSync(command[0], command.slice(1), {
      cwd: root,
      encoding: "utf8",
      env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId },
    });
    return { status: result.status ?? 1, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
  },
};
try {
  const evidence = await executeApply(context, runner);
  await writeFile(join(evidenceDir, "qualification-evidence.json"), `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
  console.log("B5 approved resources qualified and deployed");
} catch (error) {
  const evidence = error?.evidence ?? { schema_version: "1", status: "failed", failure: "qualification_failed" };
  await writeFile(join(evidenceDir, "qualification-evidence.json"), `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
  throw error;
}
