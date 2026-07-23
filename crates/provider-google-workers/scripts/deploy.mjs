import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { spawnSync } from "node:child_process";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const name = process.argv[index];
  if (!name?.startsWith("--")) throw new Error("arguments must be explicit --name value pairs");
  args.set(name, process.argv[index + 1]);
}
for (const name of ["--account-id", "--prefix", "--callback-uri", "--evidence-dir"]) {
  if (!args.has(name)) throw new Error(`missing ${name}`);
}
const accountId = args.get("--account-id");
const prefix = args.get("--prefix");
const callbackUri = args.get("--callback-uri");
const evidenceDir = args.get("--evidence-dir");
if (!/^[0-9a-f]{32}$/.test(accountId)) throw new Error("invalid account id");
if (!/^lattice-c5-[a-z0-9]{6,20}$/.test(prefix)) throw new Error("invalid C5 ownership prefix");
if (!isAbsolute(evidenceDir)) throw new Error("evidence directory must be absolute");
const callback = new URL(callbackUri);
if (callback.protocol !== "https:" || callback.username || callback.password || callback.search || callback.hash ||
    callback.pathname !== "/v0.2/credential-callback") throw new Error("invalid exact callback URI");
const manifest = JSON.parse(await readFile("build-manifest.json", "utf8"));
if (!/^[0-9a-f]{64}$/.test(manifest.source_hash ?? "")) throw new Error("source manifest invalid");
const tokenName = `${prefix}-google-token-egress`;
const providerName = `${prefix}-google-provider-egress`;
const plan = {
  schema_version: "0.2", account_id: accountId, prefix, source_hash: manifest.source_hash,
  callback_uri: callbackUri,
  workers: [
    { kind: "token", name: tokenName, config: "wrangler.token.jsonc", required_secrets: ["GOOGLE_EGRESS_SERVICE_AUTH", "GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET", "GOOGLE_OAUTH_REDIRECT_URI", "GOOGLE_TOKEN_RESULT_KEY"] },
    { kind: "provider", name: providerName, config: "wrangler.provider.jsonc", required_secrets: ["GOOGLE_EGRESS_SERVICE_AUTH"] },
  ],
  steps: ["qualify_account", "qualify_secret_names", "deploy_token_private", "deploy_provider_private", "write_ownership_state"],
};
await mkdir(evidenceDir, { recursive: true, mode: 0o700 });
await writeFile(join(evidenceDir, "google-egress-plan.json"), `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
if (args.get("--mode") !== "apply") {
  console.log(JSON.stringify(plan, null, 2));
  console.log("dry-run complete; zero remote commands executed");
  process.exit(0);
}
if (args.get("--approve-private-deploy") !== "yes" || !process.env.CLOUDFLARE_API_TOKEN) throw new Error("apply approval and API token required");
const run = (command) => {
  const result = spawnSync(command[0], command.slice(1), { encoding: "utf8", env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId } });
  if (result.status !== 0) throw new Error("remote command failed");
  return result.stdout;
};
const whoami = JSON.parse(run(["npx", "wrangler", "whoami", "--json"]));
if (whoami.account_id !== accountId) throw new Error("account mismatch");
for (const worker of plan.workers) {
  const secrets = JSON.parse(run(["npx", "wrangler", "secret", "list", "--name", worker.name, "--format", "json"]));
  const names = new Set(secrets.map((item) => item.name));
  if (worker.required_secrets.some((name) => !names.has(name))) throw new Error(`secret missing:${worker.kind}`);
}
run(["npx", "wrangler", "deploy", "--config", "wrangler.token.jsonc", "--name", tokenName]);
try {
  run(["npx", "wrangler", "deploy", "--config", "wrangler.provider.jsonc", "--name", providerName]);
} catch (error) {
  run(["npx", "wrangler", "delete", "--name", tokenName, "--force"]);
  throw error;
}
const state = { ...plan, owner: "lattice-provider-google-workers", created_by_run: true };
await writeFile(join(evidenceDir, "google-egress-ownership.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
console.log("private Google egress deployed");
