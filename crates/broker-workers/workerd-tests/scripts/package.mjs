import { createHash } from "node:crypto";
import { cp, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { basename, dirname, join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";

const crate = resolve(new URL("../..", import.meta.url).pathname);
const workspace = resolve(crate, "../..");
const finalOutput = join(workspace, "broker-workers-package");
const output = join(tmpdir(), `lattice-broker-package-${process.pid}`);
const workspacePackages = [
  "crates/broker-core", "crates/broker-host", "crates/broker-workers",
  "crates/jcs-canonical", "crates/connector-spec", "crates/dag-core",
  "crates/kernel-plan", "crates/connectors/google/platform", "crates/custodian-google",
];
const descriptors = [
  "crates/connectors/google/sheets/broker/operations/append_row.json",
  "crates/connectors/google/gmail/broker/operations/send_message.json",
];
const sentinels = [
  "/__test/", "fixture-pop-public", "fixture-deployment-key-value", "google-semantic-broker-v1",
  "lbk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "fixture-refresh-never-log", "fixture-access-never-log", "x-lattice-test-activation-crash",
  "LOCAL_TEST_MODE",
];

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", stdio: "pipe" });
  if (result.status !== 0) throw new Error(`${command} failed: ${(result.stderr || "").slice(-2000)}`);
}
const excluded = (source) => {
  const normalized = source.replaceAll("\\", "/");
  return !normalized.includes("/target/") && !normalized.endsWith("/target")
    && !normalized.includes("/node_modules/") && !normalized.includes("/workerd-tests/")
    && !/(^|\/)(build-test|build-production|build-production-tmp)(\/|$)/.test(normalized)
    && !normalized.includes("/deploy/package/") && !normalized.endsWith("/deploy/package")
    && !normalized.includes("/crates/broker-workers/build/")
    && !normalized.endsWith("/crates/broker-workers/build")
    && !normalized.endsWith("/crates/broker-workers/src/wasm/test_fixtures.rs")
    && !normalized.includes("/tests/") && !normalized.endsWith("/tests");
};
async function sha(path) {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}
async function assertNoSentinels(root) {
  for (const file of await walk(root)) {
    const bytes = await readFile(file);
    for (const sentinel of sentinels) {
      if (bytes.includes(Buffer.from(sentinel))) {
        throw new Error(`production sentinel ${sentinel} in ${relative(root, file)}`);
      }
    }
  }
}

async function assertProductionAssets(build) {
  await assertNoSentinels(build);
}

await rm(output, { recursive: true, force: true });
await mkdir(output, { recursive: true });
// Establish a clean root production artifact first. Fixture builds are never
// an input to packaging and are written to a separately named directory.
await rm(join(crate, "build"), { recursive: true, force: true });
run("bash", ["scripts/build.sh"], crate);
await assertProductionAssets(join(crate, "build"));
const rootProductionHash = await sha(join(crate, "build/index_bg.wasm"));

for (const packagePath of workspacePackages) {
  await cp(join(workspace, packagePath), join(output, packagePath), { recursive: true, filter: excluded });
  const manifestPath = join(output, packagePath, "Cargo.toml");
  const manifest = await readFile(manifestPath, "utf8");
  await writeFile(
    manifestPath,
    manifest.replace(/\n\[dev-dependencies\][\s\S]*?(?=\n\[|$)/g, ""),
  );
}
for (const descriptor of descriptors) {
  await mkdir(dirname(join(output, descriptor)), { recursive: true });
  await cp(join(workspace, descriptor), join(output, descriptor));
}
const packagedWrangler = join(output, "crates/broker-workers/wrangler.jsonc");
await writeFile(packagedWrangler, (await readFile(packagedWrangler, "utf8")).replace(
  'bash \\"$(git rev-parse --show-toplevel)/crates/broker-workers/scripts/build.sh\\"',
  'bash crates/broker-workers/scripts/build.sh',
));
let rootCargo = await readFile(join(workspace, "Cargo.toml"), "utf8");
rootCargo = rootCargo.replace(
  /members = \[[\s\S]*?\]\n\n\[workspace\.package\]/,
  `members = [\n${workspacePackages.map((path) => `  "${path}",`).join("\n")}\n]\n\n[workspace.package]`,
);
await writeFile(join(output, "Cargo.toml"), rootCargo);
await cp(join(workspace, "Cargo.lock"), join(output, "Cargo.lock"));
await cp(join(workspace, "schemas"), join(output, "schemas"), { recursive: true });

const requirements = {
  schema_version: "0.2", package: "broker-workers", package_version: "0.1.0",
  compatibility_date: "2026-07-15", rust_is_sole_state_writer: true,
  public_entrypoint: "crates/broker-workers/deploy/public-callback/src/index.mjs",
  private_entrypoint: "crates/broker-workers/build/worker/shim.mjs",
  build_command: "cargo run --locked --offline --manifest-path crates/broker-workers/tools/worker-build-0.8.1/Cargo.toml --bin worker-build -- --release crates/broker-workers",
  feature_flags: { production: [], workerd_tests_separate_artifact: ["test-fixtures"] },
  required_secret_bindings: [
    "AI_GATEWAY_AUTHORIZATION", "BINDING_SIGNING_SEED", "COMMITMENT_KEY", "CUSTODY_ROOT_KEY",
    "DEPLOYMENT_BOOTSTRAP_AUTH", "INVOKE_SERVICE_AUTH", "KEY_HASH_PEPPER", "RECEIPT_SIGNING_SEED",
  ],
  required_variable_bindings: ["GOOGLE_AUTHORIZE_ENDPOINT", "GOOGLE_OAUTH_CLIENT_ID", "OAUTH_REDIRECT_URI", "PUBLIC_CALLBACK_BASE"],
  required_service_bindings: ["GOOGLE_PROVIDER_SERVICE", "GOOGLE_TOKEN_SERVICE"],
  ai_gateway_policy: { payload_logging: "disabled", spend_limit_usd: "required-at-deploy", rate_limit_per_minute: "required-at-deploy", applied_locally: false },
};
await writeFile(join(output, "requirements.json"), `${JSON.stringify(requirements, null, 2)}\n`);

// Build from the copied dependency closure, not ambient root build output.
run("bash", ["crates/broker-workers/scripts/build.sh"], output);
await assertProductionAssets(join(output, "crates/broker-workers/build"));
const packageProductionHash = await sha(join(output, "crates/broker-workers/build/index_bg.wasm"));
if (packageProductionHash !== rootProductionHash) throw new Error("root/package production WASM hash mismatch");
// Remove build caches after the hermetic build; only the production Worker
// output and dependency sources remain.
await rm(join(output, "target"), { recursive: true, force: true });
await rm(join(output, "crates/broker-workers/tools/worker-build-0.8.1/target"), { recursive: true, force: true });
// Cargo prunes a lock when resolving the reduced deploy-only workspace.
// Restore the repository lock as the package attestation input, then compare
// bytes and fail if the final package root ever differs.
await cp(join(workspace, "Cargo.lock"), join(output, "Cargo.lock"));
const repositoryLock = await readFile(join(workspace, "Cargo.lock"));
const packagedLock = await readFile(join(output, "Cargo.lock"));
if (!repositoryLock.equals(packagedLock)) throw new Error("packaged Cargo.lock differs from repository Cargo.lock");
await assertNoSentinels(output);

async function walk(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await walk(path));
    else if (entry.isFile()) files.push(path);
    else throw new Error(`non-regular package entry: ${path}`);
  }
  return files;
}
const files = (await walk(output)).filter((path) => basename(path) !== "build-manifest.json").sort((a, b) => a.localeCompare(b));
const manifestFiles = [];
for (const path of files) {
  const bytes = await readFile(path);
  manifestFiles.push({ path: relative(output, path).replaceAll("\\", "/"), sha256: createHash("sha256").update(bytes).digest("hex"), size_bytes: (await stat(path)).size });
}
const findHash = (path) => manifestFiles.find((file) => file.path === path)?.sha256;
const manifest = {
  schema_version: "0.2", package: "broker-workers", package_version: "0.1.0",
  requirements_sha256: findHash("requirements.json"), cargo_lock_sha256: findHash("Cargo.lock"),
  wasm_sha256: findHash("crates/broker-workers/build/index_bg.wasm"),
  root_production_wasm_sha256: rootProductionHash, feature_flags: requirements.feature_flags,
  files: manifestFiles,
};
if (!manifest.requirements_sha256 || !manifest.cargo_lock_sha256 || manifest.wasm_sha256 !== rootProductionHash) throw new Error("incomplete production manifest");
await writeFile(join(output, "build-manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
await rm(finalOutput, { recursive: true, force: true });
await cp(output, finalOutput, { recursive: true });
await rm(output, { recursive: true, force: true });
console.log(`packaged ${manifestFiles.length} production broker files ${rootProductionHash}`);
