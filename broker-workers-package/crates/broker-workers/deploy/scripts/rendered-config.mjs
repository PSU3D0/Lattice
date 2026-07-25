import { randomUUID } from "node:crypto";
import { rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

export const PRIVATE_DIGEST_VARS = [
  "BROKER_WORKER_WASM_SHA256",
  "AUTH_DRIVER_WORKER_SHA256",
  "GOOGLE_TOKEN_WORKER_SHA256",
  "GOOGLE_PROVIDER_WORKER_SHA256",
  "OPERATOR_ARTIFACT_BUNDLE_SHA256",
];

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function replacePlaceholdersOnce(template, replacements) {
  const entries = Object.entries(replacements);
  if (entries.length === 0) return template;
  const values = new Map(entries.map(([token, value]) => [token, String(value)]));
  const pattern = entries
    .map(([token]) => token)
    .sort((left, right) => right.length - left.length)
    .map(escapeRegExp)
    .join("|");
  return template.replace(new RegExp(pattern, "g"), (token) => values.get(token));
}

export function replaceJsonStringPlaceholdersOnce(template, replacements) {
  return replacePlaceholdersOnce(template, Object.fromEntries(
    Object.entries(replacements).map(([token, value]) => [token, JSON.stringify(String(value)).slice(1, -1)]),
  ));
}

function assertNoOrphanDigestFragments(value) {
  if (typeof value === "string" && /_(?:HASH|SHA256)$/.test(value)) {
    throw new Error("rendered config retains an orphan digest fragment");
  }
  if (Array.isArray(value)) {
    for (const entry of value) assertNoOrphanDigestFragments(entry);
  } else if (value && typeof value === "object") {
    for (const entry of Object.values(value)) assertNoOrphanDigestFragments(entry);
  }
}

export function validateRenderedConfig(config, { requiredDigestVars = [] } = {}) {
  if (/REPLACE_WITH_[A-Z0-9_]*/.test(config)) {
    throw new Error("rendered config retains a REPLACE_WITH placeholder");
  }
  let parsed;
  try {
    parsed = JSON.parse(config);
  } catch {
    throw new Error("rendered config is not valid JSON");
  }
  assertNoOrphanDigestFragments(parsed);
  const vars = parsed.vars ?? {};
  const digestVars = new Set([
    ...requiredDigestVars,
    ...PRIVATE_DIGEST_VARS.filter((name) => Object.hasOwn(vars, name)),
    ...Object.keys(vars).filter((name) => /_(?:HASH|SHA256)$/.test(name)),
  ]);
  for (const name of digestVars) {
    if (!/^sha256:[0-9a-f]{64}$/.test(vars[name] ?? "")) {
      throw new Error(`rendered config digest invalid:${name}`);
    }
  }
  for (const removed of ["OPERATOR_ARTIFACT_BUNDLE_JCS", "GENERIC_PROFILE_REGISTRY_JCS"]) {
    if (Object.hasOwn(vars, removed)) {
      throw new Error(`rendered config contains removed oversized binding:${removed}`);
    }
  }
  return parsed;
}

export async function writeRenderedConfigEvidence({ evidenceDir, privateConfig, publicConfig }) {
  const privateRecordPath = join(evidenceDir, "wrangler.private.jsonc");
  const publicRecordPath = join(evidenceDir, "wrangler.public.jsonc");
  await writeFile(privateRecordPath, privateConfig, { mode: 0o600 });
  await writeFile(publicRecordPath, publicConfig, { mode: 0o600 });
  await writeFile(join(evidenceDir, "rendered-config-record.json"), `${JSON.stringify({
    schema_version: "1",
    private: { file: "wrangler.private.jsonc", purpose: "evidence_record_only_not_deployed" },
    public: { file: "wrangler.public.jsonc", purpose: "evidence_record_only_not_deployed" },
  }, null, 2)}\n`, { mode: 0o600 });
  return { privateRecordPath, publicRecordPath };
}

async function createExclusiveConfig(directory, kind, config, createSuffix) {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    const suffix = createSuffix();
    const path = join(directory, `.wrangler.${kind}.generated-${process.pid}-${suffix}.jsonc`);
    try {
      await writeFile(path, config, { mode: 0o600, flag: "wx" });
      return path;
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
    }
  }
  throw new Error(`unable to allocate unique rendered ${kind} config`);
}

export async function withRenderedDeployConfigs({
  privateTemplatePath,
  publicTemplatePath,
  privateConfig,
  publicConfig,
  createSuffix = randomUUID,
}, callback) {
  let privateConfigPath;
  let publicConfigPath;
  try {
    privateConfigPath = await createExclusiveConfig(dirname(privateTemplatePath), "private", privateConfig, createSuffix);
    publicConfigPath = await createExclusiveConfig(dirname(publicTemplatePath), "public", publicConfig, createSuffix);
    return await callback({ privateConfigPath, publicConfigPath });
  } finally {
    await Promise.all([
      publicConfigPath && rm(publicConfigPath, { force: true }),
      privateConfigPath && rm(privateConfigPath, { force: true }),
    ].filter(Boolean));
  }
}
