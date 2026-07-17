import { randomBytes } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { join, resolve } from "node:path";
import { createCloudflareApi } from "./cloudflare-api.mjs";
import { materializeCloudConfigs } from "./cloud-config.mjs";
import { ownedDurableNamespaces, ownedKvNamespace } from "./cloud-ownership.mjs";

function fail(message) { throw new Error(message); }
function arg(name) {
  const index = process.argv.indexOf(name);
  return index < 0 ? undefined : process.argv[index + 1];
}
function approved(name) { return process.argv.includes(name); }

const accountId = arg("--account-id");
const prefix = arg("--prefix");
const evidenceDir = arg("--evidence-dir");
if (!accountId || !/^[0-9a-f]{32}$/.test(accountId)) fail("--account-id must be an exact 32-hex disposable account id");
if (!prefix || !/^lattice-w4-[a-z0-9-]{6,30}$/.test(prefix)) fail("--prefix must match lattice-w4-[a-z0-9-]{6,30}");
if (!evidenceDir) fail("--evidence-dir is required");
if (!approved("--approve-create-disposable") || !approved("--approve-cleanup")) {
  fail("both --approve-create-disposable and --approve-cleanup are required");
}
const token = process.env.CLOUDFLARE_API_TOKEN;
if (!token) fail("CLOUDFLARE_API_TOKEN is required");
if (process.env.CLOUDFLARE_ACCOUNT_ID && process.env.CLOUDFLARE_ACCOUNT_ID !== accountId) {
  fail("CLOUDFLARE_ACCOUNT_ID does not match --account-id");
}

const names = {
  flow: `${prefix}-flow`, extraction: `${prefix}-extract`, provider: `${prefix}-provider`,
  kv: `${prefix}-kv`, bucket: `${prefix}-workspace`,
};
for (const [kind, name] of Object.entries(names)) if (name.length > 63) fail(`${kind} name exceeds 63 bytes`);
const root = resolve(new URL("..", import.meta.url).pathname);
const repository = resolve(root, "../../../..");
const evidence = resolve(evidenceDir);
if (evidence === repository || evidence.startsWith(`${repository}/`)) {
  fail("--evidence-dir must be outside the source repository");
}
const generated = join(evidence, "generated-config");
await mkdir(generated, { recursive: true, mode: 0o700 });
const state = { schema_version: "0.1", account_id: accountId, prefix, names, kv_namespace_id: null, scripts: [] };
await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });

const { request: api, listPaged, listBuckets } = createCloudflareApi(token);
const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", stdio: options.capture ? "pipe" : "inherit", env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId } });
  if (result.status !== 0) fail(`${command} ${args.join(" ")} failed`);
  return result.stdout ?? "";
};
const secret = (config, name, value) => {
  const result = spawnSync("npx", ["wrangler", "secret", "put", name, "--config", config], {
    cwd: root, input: `${value}\n`, encoding: "utf8", stdio: ["pipe", "inherit", "inherit"],
    env: { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId },
  });
  if (result.status !== 0) fail(`failed to install ${name}`);
};

run("bash", ["scripts/qualify.sh"]);

let created = false;
try {
  const accounts = await listPaged("/accounts");
  if (!accounts.some((account) => account.id === accountId)) fail("approved account is not visible to this token");
  const scripts = await listPaged(`/accounts/${accountId}/workers/scripts`);
  const scriptNames = new Set(scripts.map((script) => script.id));
  for (const name of [names.flow, names.extraction, names.provider]) {
    if (scriptNames.has(name)) fail(`refusing to overwrite existing Worker ${name}`);
  }
  const namespaces = await listPaged(`/accounts/${accountId}/storage/kv/namespaces`);
  if (namespaces.some((namespace) => namespace.title === names.kv)) {
    fail(`refusing to reuse existing KV namespace ${names.kv}`);
  }
  const buckets = await listBuckets(accountId);
  if (buckets.some((bucket) => bucket.name === names.bucket)) {
    fail(`refusing to reuse existing R2 bucket ${names.bucket}`);
  }
  const durableBaseline = await listPaged(`/accounts/${accountId}/workers/durable_objects/namespaces`);
  if (ownedDurableNamespaces(names, durableBaseline).length > 0) {
    fail("refusing to reuse existing Durable Object namespaces for the requested Worker names");
  }
  state.durable_object_baseline_ids = durableBaseline.map((namespace) => namespace.id);
  state.scripts = [names.provider, names.extraction, names.flow];
  await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });

  const kv = await api(`/accounts/${accountId}/storage/kv/namespaces`, {
    method: "POST", body: JSON.stringify({ title: names.kv }),
  });
  state.kv_namespace_id = kv.id;
  await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  created = true;
  await api(`/accounts/${accountId}/r2/buckets/${names.bucket}`, { method: "PUT", body: "{}" });
  state.r2_created = true;
  await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });

  const cloudConfigs = await materializeCloudConfigs({
    root,
    output: generated,
    names,
    kvNamespaceId: kv.id,
    bucketName: names.bucket,
  });
  const flowConfig = cloudConfigs.paths.flow;
  const extractionConfig = cloudConfigs.paths.extraction;
  const providerConfig = cloudConfigs.paths.provider;

  run("npx", ["wrangler", "deploy", "--config", providerConfig]);
  state.provider_deployed = true;
  await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  const llmBearer = randomBytes(24).toString("hex");
  const googleBearer = randomBytes(24).toString("hex");
  const adminBearer = randomBytes(24).toString("hex");
  secret(providerConfig, "MOCK_LLM_BEARER", llmBearer);
  secret(providerConfig, "MOCK_GOOGLE_BEARER", googleBearer);
  secret(providerConfig, "MOCK_ADMIN_BEARER", adminBearer);
  await writeFile(providerConfig, cloudConfigs.publicProvider);
  run("npx", ["wrangler", "deploy", "--config", providerConfig]);
  run("npx", ["wrangler", "deploy", "--config", extractionConfig]);
  state.extraction_deployed = true;
  await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  run("npx", ["wrangler", "deploy", "--config", flowConfig]);
  state.flow_deployed = true;
  await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  secret(flowConfig, "LATTICE_CONNECTOR_AUTH_LLM_API_KEY", llmBearer);
  secret(flowConfig, "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH", googleBearer);
  await writeFile(flowConfig, cloudConfigs.publicFlow);
  run("npx", ["wrangler", "deploy", "--config", flowConfig]);
  const durableAfterDeploy = await listPaged(`/accounts/${accountId}/workers/durable_objects/namespaces`);
  const durableBaselineIds = new Set(state.durable_object_baseline_ids);
  state.durable_object_namespace_ids = ownedDurableNamespaces(names, durableAfterDeploy)
    .filter((namespace) => !durableBaselineIds.has(namespace.id))
    .map((namespace) => namespace.id);
  state.expected_durable_object_namespaces = 3;
  await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  if (state.durable_object_namespace_ids.length !== state.expected_durable_object_namespaces) {
    fail("could not identify the three deployment-owned Durable Object namespaces");
  }

  const subdomain = (await api(`/accounts/${accountId}/workers/subdomain`)).subdomain;
  const flowUrl = `https://${names.flow}.${subdomain}.workers.dev`;
  const providerUrl = `https://${names.provider}.${subdomain}.workers.dev`;
  const adminHeaders = { authorization: `Bearer ${adminBearer}` };
  await fetch(`${providerUrl}/__reset`, { method: "POST", headers: adminHeaders });
  const makePdf = (text) => {
    const encoder = new TextEncoder();
    const stream = `BT /F1 12 Tf 72 720 Td (${text}) Tj ET`;
    const objects = [
      "<< /Type /Catalog /Pages 2 0 R >>",
      "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
      "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
      `<< /Length ${encoder.encode(stream).length} >>\nstream\n${stream}\nendstream`,
      "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ];
    let pdf = "%PDF-1.4\n% W4 fixture\n";
    const offsets = [0];
    for (let index = 0; index < objects.length; index += 1) {
      offsets.push(encoder.encode(pdf).length);
      pdf += `${index + 1} 0 obj\n${objects[index]}\nendobj\n`;
    }
    const xref = encoder.encode(pdf).length;
    pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
    for (const offset of offsets.slice(1)) pdf += `${String(offset).padStart(10, "0")} 00000 n \n`;
    pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
    return encoder.encode(pdf);
  };
  const syntheticPdf = makePdf("W4 cloud proof");
  const invoke = async (pdf, email) => {
    const form = new FormData();
    form.set("full_name", "W4 Fixture"); form.set("email", email);
    form.set("expectation", "bounded proof"); form.set("linkedin", "https://example.invalid/w4");
    form.set("cv", new Blob([pdf], { type: "application/pdf" }), "fixture.pdf");
    const response = await fetch(`${flowUrl}/cv-screening`, { method: "POST", body: form });
    return { status: response.status, body: await response.json().catch(() => ({})) };
  };
  const counts = async () => (await fetch(`${providerUrl}/__counts`, { headers: adminHeaders })).json();
  const first = await invoke(syntheticPdf, "w4-fixture-1@example.invalid");
  if (first.status !== 200 || first.body.stored !== true) fail("first cloud invocation failed");
  const afterFirst = await counts();
  if (JSON.stringify(afterFirst) !== JSON.stringify({ llm: 1, sheetsRead: 1, sheetsAppend: 1, gmail: 2 })) fail("unexpected first-call provider counts");
  const second = await invoke(syntheticPdf, "w4-fixture-1@example.invalid");
  if (second.status !== 200 || second.body.stored !== false) fail("sequential redelivery proof failed");
  const afterSecond = await counts();
  if (JSON.stringify(afterSecond) !== JSON.stringify(afterFirst)) fail("redelivery repeated provider effects");
  const hostile = await invoke(new TextEncoder().encode("%PDF-private-hostile-sentinel"), "w4-hostile@example.invalid");
  const hostileText = JSON.stringify(hostile.body);
  if (hostile.status !== 500 || hostileText.includes("private-hostile-sentinel")) fail("hostile-input sanitization proof failed");
  if (JSON.stringify(await counts()) !== JSON.stringify(afterFirst)) fail("hostile input reached providers");

  const manifest = JSON.parse(await readFile(join(root, "deploy/deploy-manifest.json"), "utf8"));
  const report = {
    schema_version: "0.1", account_id: accountId, names,
    module_sha256: manifest.implementation_backends[0].module_sha256,
    compatibility_date: manifest.implementation_backends[0].compatibility_date,
    cpu_ms_policy: manifest.implementation_backends[0].cpu_ms,
    first: { status: first.status, stored: first.body.stored === true },
    redelivery: { status: second.status, stored: second.body.stored === true },
    hostile: { status: hostile.status, sanitized: !hostileText.includes("private-hostile-sentinel") },
    provider_counts: afterFirst,
    claims: {
      containment_configuration_verified: true,
      platform_cpu_policy_deployed: true,
      platform_termination_observed: false,
      native_metering_parity: false,
      cpu_vs_memory_attribution: false,
    },
  };
  await writeFile(join(evidence, "sanitized-proof.json"), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
} finally {
  if (created) {
    const cleanupErrors = [];
    let existingDurable = [];
    try { existingDurable = await listPaged(`/accounts/${accountId}/workers/durable_objects/namespaces`); }
    catch { cleanupErrors.push("durable-object:list-before-delete"); }
    const existingDurableIds = new Set(existingDurable.map((namespace) => namespace.id));
    state.durable_object_namespace_ids = ownedDurableNamespaces(names, existingDurable)
      .map((namespace) => namespace.id);
    let existingScripts = [];
    try { existingScripts = await listPaged(`/accounts/${accountId}/workers/scripts`); }
    catch { cleanupErrors.push("workers:list-before-delete"); }
    const existingScriptNames = new Set(existingScripts.map((script) => script.id));
    for (const script of [...state.scripts].reverse()) {
      if (!existingScriptNames.has(script)) continue;
      try { run("npx", ["wrangler", "delete", script, "--force"]); }
      catch { cleanupErrors.push(`worker:${script}`); }
    }
    for (const namespaceId of state.durable_object_namespace_ids) {
      if (!existingDurableIds.has(namespaceId)) continue;
      try { await api(`/accounts/${accountId}/workers/durable_objects/namespaces/${namespaceId}`, { method: "DELETE" }); }
      catch { cleanupErrors.push(`durable-object:${namespaceId}`); }
    }
    try {
      const existingKv = await listPaged(`/accounts/${accountId}/storage/kv/namespaces`);
      const ownedKv = ownedKvNamespace(names, state.kv_namespace_id, existingKv);
      if (ownedKv) {
        state.kv_namespace_id = ownedKv.id;
        await api(`/accounts/${accountId}/storage/kv/namespaces/${ownedKv.id}`, { method: "DELETE" });
      }
    } catch { cleanupErrors.push("kv"); }
    if (state.r2_created) {
      try { await api(`/accounts/${accountId}/r2/buckets/${names.bucket}`, { method: "DELETE" }); }
      catch { cleanupErrors.push("r2"); }
    }
    try {
      const remainingScripts = await listPaged(`/accounts/${accountId}/workers/scripts`);
      if (remainingScripts.some((script) => state.scripts.includes(script.id))) cleanupErrors.push("workers:still-present");
      const remainingKv = await listPaged(`/accounts/${accountId}/storage/kv/namespaces`);
      if (remainingKv.some((namespace) => namespace.id === state.kv_namespace_id)) cleanupErrors.push("kv:still-present");
      const remainingBuckets = await listBuckets(accountId);
      if (remainingBuckets.some((bucket) => bucket.name === names.bucket)) cleanupErrors.push("r2:still-present");
      const remainingDurable = await listPaged(`/accounts/${accountId}/workers/durable_objects/namespaces`);
      if (ownedDurableNamespaces(names, remainingDurable).length > 0) {
        cleanupErrors.push("durable-object:still-present");
      }
    } catch { cleanupErrors.push("post-cleanup-verification"); }
    state.cleanup_attempted = true;
    state.cleanup_verified = cleanupErrors.length === 0;
    state.cleanup_errors = cleanupErrors;
    await writeFile(join(evidence, "resource-state.json"), `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
    if (cleanupErrors.length > 0) fail(`cleanup incomplete: ${cleanupErrors.join(", ")}`);
  }
}
