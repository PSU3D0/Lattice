import { cp, readFile, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const crate = resolve(new URL("../..", import.meta.url).pathname);
const workspace = resolve(crate, "../..");
const packageRoot = resolve(workspace, "broker-workers-package");
const wrangler = resolve(crate, "workerd-tests/node_modules/.bin/wrangler");
const target = "/tmp/lattice-broker-production-preflight-target";
const checkPackage = "/tmp/lattice-broker-production-preflight-package";
const rustflagsPath = resolve(crate, "scripts/wasm32-unknown-unknown-rustflags.txt");

export async function resolveProductionWasmRustflags(environment = process.env) {
  const required = (await readFile(rustflagsPath, "utf8")).trim();
  const existing = environment.CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS;
  return existing ? `${existing} ${required}` : required;
}

function run(command, args, cwd, env) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", stdio: "pipe", env });
  if (result.status !== 0) {
    const rendered = [command, ...args].join(" ");
    throw new Error(`command failed (${rendered}): ${(result.stderr || "").slice(-2000)}`);
  }
}

export async function main() {
  const env = { ...process.env, CARGO_TARGET_DIR: target };
  const cargoCheckEnv = {
    ...env,
    CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS: await resolveProductionWasmRustflags(env),
  };
  await rm(target, { recursive: true, force: true });
  await rm(checkPackage, { recursive: true, force: true });
  run("node", ["workerd-tests/scripts/package.mjs"], crate, env);
  run("node", ["workerd-tests/scripts/verify-package.mjs"], crate, env);
  await cp(packageRoot, checkPackage, { recursive: true });
  run("cargo", ["check", "--offline", "--target", "wasm32-unknown-unknown", "-p", "broker-workers"], checkPackage, cargoCheckEnv);
  run(wrangler, ["deploy", "--dry-run", "--config", "crates/broker-workers/wrangler.jsonc", "--outdir", "/tmp/lattice-broker-preflight-private"], checkPackage, env);
  run(wrangler, ["deploy", "--dry-run", "--config", "crates/broker-workers/deploy/public-callback/wrangler.jsonc", "--outdir", "/tmp/lattice-broker-preflight-public"], checkPackage, env);
  run("node", ["workerd-tests/scripts/verify-package.mjs"], crate, env);
  await rm(target, { recursive: true, force: true });
  await rm(checkPackage, { recursive: true, force: true });
  console.log("clean production package/build/hash preflight passed");
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) await main();
