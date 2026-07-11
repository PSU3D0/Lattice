// Cron render→run proof (packet T3 of ops/phase1-clone-engine-plan-2026-06-12.md).
//
// The scheduled() half of "the generated config is real": the worker under
// test is stood up from the wrangler.toml RENDERED by `flows deploy render
// --requirements cron/requirements.json` (npm run render-cron), where the
// requirements manifest is itself DERIVED from the flow by
// `emit-cron-requirements` — nothing is hand-written. Every binding AND every
// cron string the test dispatches is read from the rendered file via
// wrangler's own config parser, so the byte-equal routing contract
// (impl-docs/spec/schedule-trigger.md §7a/§7c) is proven end to end:
// manifest -> [triggers].crons -> scheduled event -> entrypoint.
//
// Coverage:
// - a cron fire executes the flow (observable KV record keyed on the
//   scheduled time), with durability preflight satisfied from the rendered
//   FLOW_DO binding;
// - scoped capabilities (CAP110) are in force during cron fires: the
//   smuggle arm's UNDECLARED KV access is denied, failing the invocation and
//   leaving no side effect;
// - an unroutable cron (config drift) errors loudly, never a silent drop.
//
// Run: `npm test` (renders both configs, builds the worker, runs vitest).

import { describe, it, expect, afterAll } from "vitest";
import { Miniflare } from "miniflare";
import { unstable_readConfig } from "wrangler";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));
const configPath = path.resolve(here, "..", "cron", "wrangler.toml");

if (!existsSync(configPath)) {
  throw new Error(
    `${configPath} not found — it is RENDERED, not hand-written. ` +
      "Run `npm test` (or `npm run render-cron`) so `flows deploy render " +
      "--requirements cron/requirements.json` generates it first.",
  );
}

// Parse the RENDERED config with wrangler's own parser: what wrangler would
// deploy is what we serve.
const cfg = unstable_readConfig({ config: configPath });

// --- Derive the ENTIRE miniflare configuration from the rendered config. ---
const sqliteClasses = new Set(
  (cfg.migrations ?? []).flatMap((m) => m.new_sqlite_classes ?? []),
);
const durableObjects = Object.fromEntries(
  (cfg.durable_objects?.bindings ?? []).map((b) => [
    b.name,
    { className: b.class_name, useSQLite: sqliteClasses.has(b.class_name) },
  ]),
);
const kvNamespaces = (cfg.kv_namespaces ?? []).map((ns) => ns.binding);
const r2Buckets = (cfg.r2_buckets ?? []).map((b) => b.binding);
// The crons the deploy would configure — and therefore the only strings CF
// would ever echo into scheduled(). The test dispatches EXACTLY these.
const crons = cfg.triggers?.crons ?? [];

const mf = new Miniflare({
  workers: [
    {
      name: cfg.name,
      // The renderer does not scaffold the worker entry crate (documented
      // NOTES line); the shared crate build provides the module the rendered
      // `main` names. Asserted below to match cfg.main's shape.
      scriptPath: path.resolve(here, "..", "build", "worker", "shim.mjs"),
      compatibilityDate: cfg.compatibility_date ?? undefined,
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
        { type: "ESModule", include: ["**/*.js", "**/*.mjs"], fallthrough: true },
      ],
      durableObjects,
      kvNamespaces,
      r2Buckets,
      bindings: { ...(cfg.vars ?? {}) },
    },
  ],
});

const mfUrl = await mf.ready;

afterAll(async () => {
  await mf.dispose();
});

/** Dispatch a scheduled event the way miniflare exposes it (the
 * /cdn-cgi/mf/scheduled test route; `time` is the scheduled epoch ms the
 * event carries). Returns the raw response: 200 "ok" on success, 500
 * otherwise. */
function dispatchScheduled(cron: string, timeMs: number) {
  const url =
    `${mfUrl}cdn-cgi/mf/scheduled?cron=${encodeURIComponent(cron)}` +
    `&time=${timeMs}`;
  return mf.dispatchFetch(url);
}

describe("rendered cron wrangler.toml shape", () => {
  it("carries the [triggers].crons union, byte-verbatim and sorted", () => {
    expect(cfg.name).toBe("cron-render-proof-flow");
    expect(cfg.main).toMatch(/build\/worker\/shim\.mjs$/);
    expect(crons).toEqual(["*/5 * * * *", "*/9 * * * *"]);
  });

  it("provisions exactly the flow's demands: FLOW_DO (durability) + FLOW_KV (declared kv write)", () => {
    expect(cfg.durable_objects?.bindings).toEqual([
      { name: "FLOW_DO", class_name: "FlowDurableObject" },
    ]);
    expect(kvNamespaces).toEqual(["FLOW_KV"]);
    expect(cfg.d1_databases ?? []).toEqual([]);
    expect(r2Buckets).toEqual([]);
  });

  it("carries no unresolved placeholders (bindings.lock supplied the kv id)", async () => {
    const { readFile } = await import("node:fs/promises");
    const raw = await readFile(configPath, "utf8");
    const valueLines = raw
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("#"));
    expect(valueLines.join("\n")).not.toContain("REPLACE_WITH_");
  });
});

describe("cron render→run proof: scheduled events drive the flow", () => {
  // A fixed scheduled time makes the KV key deterministic and proves the
  // payload carries the *scheduled* time, not the observed wall clock.
  const scheduledTimeMs = 1720000000000;

  it("a cron fire routed by byte equality executes the flow (KV record observable)", async () => {
    // crons[0] is "*/5 * * * *" -> tick -> record. Durability preflight
    // (checkpoint store from the rendered FLOW_DO binding) must also pass or
    // this run fails closed.
    const response = await dispatchScheduled(crons[0], scheduledTimeMs);
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("ok");

    const kv = await mf.getKVNamespace("FLOW_KV");
    const recorded = await kv.get(`tick:${scheduledTimeMs}`, "json");
    // The trigger received the typed ScheduledEvent: scheduled time + the
    // cron string byte-identical to the rendered/authored one.
    expect(recorded).toEqual({
      scheduled_time_ms: scheduledTimeMs,
      cron: "*/5 * * * *",
    });
  });

  it("scoped capabilities (CAP110) hold during cron fires: undeclared KV access fails the run, no side effect", async () => {
    // crons[1] is "*/9 * * * *" -> smuggle_tick -> smuggle, whose node
    // declares NO capabilities but attempts a KV write. ScopedResources must
    // deny it: the invocation errors (500 outcome) and nothing is written.
    const response = await dispatchScheduled(crons[1], scheduledTimeMs);
    expect(response.status).toBe(500);

    const kv = await mf.getKVNamespace("FLOW_KV");
    expect(await kv.get(`smuggled:${scheduledTimeMs}`)).toBeNull();
  });

  it("an unroutable cron (config drift) errors loudly — never fires a fallback flow", async () => {
    // A valid cron that appears nowhere in the bundle: zero byte-equal
    // matches must be an explicit error, not a silent drop and not a
    // best-guess dispatch to the only flow.
    const response = await dispatchScheduled("0 0 1 1 *", scheduledTimeMs + 60000);
    expect(response.status).toBe(500);

    const kv = await mf.getKVNamespace("FLOW_KV");
    expect(await kv.get(`tick:${scheduledTimeMs + 60000}`)).toBeNull();
  });

  it("redelivery of the same fire is idempotent by construction (same scheduled_time key)", async () => {
    // At-least-once semantics: replay the same scheduled event; the record
    // key is derived from scheduled_time_ms, so the redelivery overwrites
    // the same key instead of fanning out new state.
    const response = await dispatchScheduled(crons[0], scheduledTimeMs);
    expect(response.status).toBe(200);

    const kv = await mf.getKVNamespace("FLOW_KV");
    const keys = (await kv.list({ prefix: "tick:" })).keys.map((k) => k.name);
    expect(keys).toEqual([`tick:${scheduledTimeMs}`]);
  });
});
