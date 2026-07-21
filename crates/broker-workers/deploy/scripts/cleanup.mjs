import { readFile } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { executeStandaloneCleanup } from "./cleanup-lib.mjs";

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const name = process.argv[index];
  if (!name?.startsWith("--")) throw new Error("arguments must be explicit --name value pairs");
  args.set(name, process.argv[index + 1]);
}
const statePath = args.get("--state");
if (!statePath || !isAbsolute(statePath)) throw new Error("--state must be an absolute path");
const state = JSON.parse(await readFile(statePath, "utf8"));
const apply = args.get("--approve-cleanup") === "yes";
if (!apply) {
  console.log(JSON.stringify({ mode: "dry-run", state: statePath, remote_queries: 0, deletes: 0 }, null, 2));
  process.exit(0);
}
if (!process.env.CLOUDFLARE_API_TOKEN) throw new Error("cleanup requires CLOUDFLARE_API_TOKEN");
const root = resolve(new URL("../..", import.meta.url).pathname);
const runner = {
  async run(_step, command) {
    const result = spawnSync(command[0], command.slice(1), {
      cwd: root,
      encoding: "utf8",
      env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: state.account_id },
    });
    return { status: result.status ?? 1, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
  },
};
const evidence = await executeStandaloneCleanup(state, runner);
console.log(JSON.stringify(evidence, null, 2));
