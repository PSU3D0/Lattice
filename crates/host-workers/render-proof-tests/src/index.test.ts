// Render→run proof (packet W2 of ops/phase1-clone-engine-plan-2026-06-12.md).
//
// The gate that says "the generated config is real": the worker under test is
// stood up from the wrangler.toml RENDERED by `flows deploy render --example
// s1_echo` (npm run render) — not from a hand-written config, and not from a
// hand-coded miniflare binding list. Every binding miniflare gets below is
// derived from the rendered file via wrangler's own config parser
// (unstable_readConfig), so a renderer regression (wrong binding name, wrong
// entry, missing durability DO) turns this suite red.
//
// Run: `npm test` (renders the config, builds the worker via the
// ensure-worker-build guard, then runs vitest).

import { describe, it, expect, afterAll } from "vitest";
import { Miniflare } from "miniflare";
import { unstable_readConfig } from "wrangler";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));
const configPath = path.resolve(here, "..", "wrangler.toml");

if (!existsSync(configPath)) {
  throw new Error(
    `${configPath} not found — it is RENDERED, not hand-written. ` +
      "Run `npm test` (or `npm run render`) so `flows deploy render " +
      "--example s1_echo` generates it first.",
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

const mf = new Miniflare({
  workers: [
    {
      name: cfg.name,
      // cfg.main is resolved by unstable_readConfig relative to the config
      // file: the renderer's `build/worker/shim.mjs` worker-build entry.
      scriptPath: cfg.main!,
      compatibilityDate: cfg.compatibility_date ?? undefined,
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
        // worker-build output is ESM (build/package.json `"type": "module"`,
        // which wrangler honors); miniflare's default rules would parse the
        // shim's `../index.js` import as CommonJS without this.
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

describe("rendered wrangler.toml shape (s1_echo: http-only, near-empty bindings)", () => {
  it("names the worker after the flow and uses the worker-build entry", () => {
    expect(cfg.name).toBe("s1-echo-flow");
    expect(cfg.main).toMatch(/build\/worker\/shim\.mjs$/);
    expect(cfg.compatibility_date).toBe("2024-09-23");
  });

  it("provisions exactly the durability checkpoint DO and nothing else", () => {
    // FLOW_DO/FlowDurableObject: the renderer default AND the host-workers
    // runtime name — if either side drifts, this fails.
    expect(cfg.durable_objects?.bindings).toEqual([
      { name: "FLOW_DO", class_name: "FlowDurableObject" },
    ]);
    expect(sqliteClasses.has("FlowDurableObject")).toBe(true);
    // Near-empty bindings: an http-only pure flow must not drag in storage.
    expect(cfg.kv_namespaces ?? []).toEqual([]);
    expect(cfg.r2_buckets ?? []).toEqual([]);
    expect(cfg.d1_databases ?? []).toEqual([]);
    expect(cfg.services ?? []).toEqual([]);
    expect(Object.keys(cfg.vars ?? {})).toEqual([]);
  });

  it("carries no unresolved placeholders (nothing to replace before deploy)", async () => {
    const { readFile } = await import("node:fs/promises");
    const raw = await readFile(configPath, "utf8");
    // The renderer's fixed header mentions REPLACE_WITH_* generically; only
    // non-comment (value) lines may not carry an actual placeholder.
    const valueLines = raw
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("#"));
    expect(valueLines.join("\n")).not.toContain("REPLACE_WITH_");
  });
});

describe("render→run proof: the rendered config drives the worker", () => {
  it("serves the flow's POST /echo entrypoint end-to-end", async () => {
    const response = await mf.dispatchFetch(`${mfUrl}echo`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value: "  HeLLo World  " }),
    });

    expect(response.status).toBe(200);
    // s1_echo semantics: Normalize trims + lowercases; Responder attaches no
    // user without auth metadata (skip-serialized when None).
    const body = await response.json();
    expect(body).toEqual({ value: "hello world" });
  });

  it("normalization is the flow executing, not an echo of the input", async () => {
    const response = await mf.dispatchFetch(`${mfUrl}echo`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value: "ALREADY-LOWER?  NO." }),
    });

    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body).toEqual({ value: "already-lower?  no." });
  });

  it("serves ONLY the rendered flow's entrypoints (single-flow bundle, not the multi-flow fixture)", async () => {
    // /health belongs to the workerd-tests multi-flow fixture, not to
    // s1_echo. Its absence proves this worker is the single-flow bundle the
    // rendered config describes.
    const health = await mf.dispatchFetch(`${mfUrl}health`);
    expect(health.status).toBe(404);

    // Method must match the rendered entrypoint (POST /echo).
    const get = await mf.dispatchFetch(`${mfUrl}echo`, { method: "GET" });
    expect(get.status).toBe(404);
  });
});
