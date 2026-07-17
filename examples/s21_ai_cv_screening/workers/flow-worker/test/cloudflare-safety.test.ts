import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createCloudflareApi } from "../scripts/cloudflare-api.mjs";
import { ownedDurableNamespaces, ownedKvNamespace } from "../scripts/cloud-ownership.mjs";

describe("guarded Cloudflare lifecycle helpers", () => {
  it("exhausts page and cursor pagination", async () => {
    const calls: string[] = [];
    const fetchMock = async (input: string | URL | Request) => {
      const url = new URL(String(input));
      calls.push(url.toString());
      if (url.pathname.endsWith("/workers/scripts")) {
        const page = Number(url.searchParams.get("page"));
        return Response.json({
          success: true,
          result: [{ id: `worker-${page}` }],
          result_info: { page, total_pages: 2 },
        });
      }
      if (url.pathname.endsWith("/r2/buckets")) {
        const cursor = url.searchParams.get("cursor");
        return Response.json({
          success: true,
          result: { buckets: [{ name: cursor ? "bucket-2" : "bucket-1" }] },
          result_info: cursor ? {} : { cursor: "next-page" },
        });
      }
      throw new Error(`unexpected URL ${url}`);
    };
    const api = createCloudflareApi("test-token", fetchMock as typeof fetch);
    expect(await api.listPaged("/accounts/a/workers/scripts")).toEqual([
      { id: "worker-1" },
      { id: "worker-2" },
    ]);
    expect(await api.listBuckets("a")).toEqual([
      { name: "bucket-1" },
      { name: "bucket-2" },
    ]);
    expect(calls.some((url) => url.includes("page=2"))).toBe(true);
    expect(calls.some((url) => url.includes("cursor=next-page"))).toBe(true);
  });

  it("selects only live script/class-owned DO and exact-title KV resources", () => {
    const names = {
      flow: "lattice-w4-safety1-flow",
      provider: "lattice-w4-safety1-provider",
      extraction: "lattice-w4-safety1-extract",
      kv: "lattice-w4-safety1-kv",
      bucket: "lattice-w4-safety1-workspace",
    };
    const namespaces = [
      { id: "1", script: names.flow, class: "FlowDurableObject" },
      { id: "2", script: names.flow, class: "UnrelatedClass" },
      { id: "3", script: "production-worker", class: "FlowDurableObject" },
    ];
    expect(ownedDurableNamespaces(names, namespaces)).toEqual([namespaces[0]]);
    expect(ownedKvNamespace(names, "a".repeat(32), [
      { id: "a".repeat(32), title: names.kv },
    ])?.title).toBe(names.kv);
    expect(() => ownedKvNamespace(names, "b".repeat(32), [
      { id: "b".repeat(32), title: "production-kv" },
      { id: "c".repeat(32), title: names.kv },
    ])).toThrow(/different namespace title/);
  });

  it("rejects tampered cleanup targets before any Cloudflare request", async () => {
    const directory = await mkdtemp(join(tmpdir(), "lattice-w4-cleanup-test-"));
    try {
      const statePath = join(directory, "state.json");
      await writeFile(statePath, JSON.stringify({
        schema_version: "0.1",
        account_id: "a".repeat(32),
        prefix: "lattice-w4-safety1",
        names: {
          flow: "lattice-w4-safety1-flow",
          extraction: "lattice-w4-safety1-extract",
          provider: "lattice-w4-safety1-provider",
          kv: "lattice-w4-safety1-kv",
          bucket: "lattice-w4-safety1-workspace",
        },
        scripts: ["unrelated-production-worker"],
        kv_namespace_id: "b".repeat(32),
      }));
      const result = spawnSync(
        process.execPath,
        ["scripts/cloudflare-w4-cleanup.mjs", "--state", statePath, "--approve-cleanup"],
        {
          cwd: new URL("..", import.meta.url).pathname,
          encoding: "utf8",
          env: { ...process.env, CLOUDFLARE_API_TOKEN: "not-used" },
        },
      );
      expect(result.status).not.toBe(0);
      expect(result.stderr).toContain("resource state Worker targets are invalid");
      expect(await readFile(statePath, "utf8")).toContain("unrelated-production-worker");
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
