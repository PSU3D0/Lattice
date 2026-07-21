import { cp, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

const crate = resolve(new URL("../..", import.meta.url).pathname);
const workspace = resolve(crate, "../..");
const packageRoot = resolve(workspace, "broker-workers-package");
const wrangler = resolve(crate, "workerd-tests/node_modules/.bin/wrangler");
const target = "/tmp/lattice-broker-production-preflight-target";
const checkPackage = "/tmp/lattice-broker-production-preflight-package";
function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd, encoding: "utf8", stdio: "pipe", env: { ...process.env, CARGO_TARGET_DIR: target },
  });
  if (result.status !== 0) throw new Error(`${command} failed: ${(result.stderr || "").slice(-2000)}`);
}
await rm(target, { recursive: true, force: true });
await rm(checkPackage, { recursive: true, force: true });
run("node", ["workerd-tests/scripts/package.mjs"], crate);
run("node", ["workerd-tests/scripts/verify-package.mjs"], crate);
await cp(packageRoot, checkPackage, { recursive: true });
run("cargo", ["check", "--offline", "--target", "wasm32-unknown-unknown", "-p", "broker-workers"], checkPackage);
run(wrangler, ["deploy", "--dry-run", "--config", "crates/broker-workers/wrangler.jsonc", "--outdir", "/tmp/lattice-broker-preflight-private"], checkPackage);
run(wrangler, ["deploy", "--dry-run", "--config", "crates/broker-workers/deploy/public-callback/wrangler.jsonc", "--outdir", "/tmp/lattice-broker-preflight-public"], checkPackage);
run("node", ["workerd-tests/scripts/verify-package.mjs"], crate);
await rm(target, { recursive: true, force: true });
await rm(checkPackage, { recursive: true, force: true });
console.log("clean production package/build/hash preflight passed");
