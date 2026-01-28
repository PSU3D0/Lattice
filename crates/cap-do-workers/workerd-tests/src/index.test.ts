import { describe, it, expect, afterAll } from "vitest";
import { Miniflare } from "miniflare";

const mf = new Miniflare({
  workers: [
    {
      scriptPath: "./build/index.js",
      compatibilityDate: "2024-09-23",
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
      ],
      durableObjects: {
        FLOW_DO: {
          className: "FlowDurableObject",
          useSQLite: true,
        },
      },
    },
  ],
});

const mfUrl = await mf.ready;

async function dedupePut(key: string, ttlSeconds = 60) {
  const response = await mf.dispatchFetch(`${mfUrl}dedupe/put_if_absent`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key, ttl_seconds: ttlSeconds }),
  });
  return response.json();
}

async function dedupeForget(key: string) {
  const response = await mf.dispatchFetch(`${mfUrl}dedupe/forget`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key }),
  });
  return response.json();
}

async function storagePut(key: string, value: unknown, ttlSeconds?: number) {
  const response = await mf.dispatchFetch(`${mfUrl}storage/put`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key, value, ttl_seconds: ttlSeconds }),
  });
  return response.json();
}

async function storageGet(key: string) {
  const response = await mf.dispatchFetch(`${mfUrl}storage/get?key=${encodeURIComponent(key)}`);
  return response.json();
}

async function storageDelete(key: string) {
  const response = await mf.dispatchFetch(
    `${mfUrl}storage/delete?key=${encodeURIComponent(key)}`,
    { method: "DELETE" }
  );
  return response.json();
}

async function storageList(options?: { prefix?: string; start?: string; limit?: number }) {
  const params = new URLSearchParams();
  if (options?.prefix) params.set("prefix", options.prefix);
  if (options?.start) params.set("start", options.start);
  if (options?.limit) params.set("limit", options.limit.toString());
  const url = `${mfUrl}storage/list${params.toString() ? "?" + params.toString() : ""}`;
  const response = await mf.dispatchFetch(url);
  return response.json();
}

async function alarmGet() {
  const response = await mf.dispatchFetch(`${mfUrl}alarm/get`);
  return response.json();
}

async function alarmSet(scheduledMs: number) {
  const response = await mf.dispatchFetch(`${mfUrl}alarm/set`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scheduled_ms: scheduledMs }),
  });
  return response.json();
}

async function alarmDelete() {
  const response = await mf.dispatchFetch(`${mfUrl}alarm/delete`, {
    method: "DELETE",
  });
  return response.json();
}

async function sqlExec(query: string, bindings?: unknown[], mode?: "json" | "raw") {
  const response = await mf.dispatchFetch(`${mfUrl}sql/exec`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, bindings: bindings ?? [], mode: mode ?? "json" }),
  });
  return response.json();
}

afterAll(async () => {
  await mf.dispose();
});

describe("cap-do-workers E2E", () => {
  it("responds to health check", async () => {
    const response = await mf.dispatchFetch(`${mfUrl}health`);
    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body).toEqual({ status: "ok" });
  });

  it("dedupe put_if_absent then forget", async () => {
    const first = await dedupePut("alpha");
    expect(first.inserted).toBe(true);

    const second = await dedupePut("alpha");
    expect(second.inserted).toBe(false);

    const forget = await dedupeForget("alpha");
    expect(forget.success).toBe(true);

    const third = await dedupePut("alpha");
    expect(third.inserted).toBe(true);
  });

  it("dedupe is single-writer per key", async () => {
    const results = await Promise.all(
      Array.from({ length: 12 }, () => dedupePut("race-key"))
    );
    const inserted = results.filter((r) => r.inserted).length;
    expect(inserted).toBe(1);
  });

  it("storage CRUD", async () => {
    const put = await storagePut("item:1", { name: "alpha", count: 2 });
    expect(put.success).toBe(true);

    const get = await storageGet("item:1");
    expect(get.found).toBe(true);
    expect(get.value).toEqual({ name: "alpha", count: 2 });

    const deleted = await storageDelete("item:1");
    expect(deleted.deleted).toBe(true);

    const missing = await storageGet("item:1");
    expect(missing.found).toBe(false);
  });

  it("storage list supports prefix/start/limit", async () => {
    await storagePut("list/a", { n: 1 });
    await storagePut("list/b", { n: 2 });
    await storagePut("list/c", { n: 3 });

    const all = await storageList({ prefix: "list/" });
    expect(all.keys.length).toBeGreaterThanOrEqual(3);

    const limited = await storageList({ prefix: "list/", limit: 2 });
    expect(limited.keys.length).toBe(2);

    const start = limited.keys[1];
    const next = await storageList({ prefix: "list/", start });
    expect(next.keys.length).toBeGreaterThan(0);
  });

  it("alarm set/get/delete", async () => {
    const scheduledMs = Date.now() + 5000;
    const set = await alarmSet(scheduledMs);
    expect(set.success).toBe(true);

    const get = await alarmGet();
    expect(typeof get.alarm_ms).toBe("number");
    expect(get.alarm_ms).toBeGreaterThanOrEqual(scheduledMs);

    const del = await alarmDelete();
    expect(del.success).toBe(true);

    const cleared = await alarmGet();
    expect(cleared.alarm_ms).toBeNull();
  });

  it("sqlite exec json and raw", async () => {
    await sqlExec("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)");
    await sqlExec("DELETE FROM items");

    await sqlExec("INSERT INTO items (id, name) VALUES (?, ?)", [
      { type: "integer", value: 1 },
      { type: "string", value: "alpha" },
    ]);
    await sqlExec("INSERT INTO items (id, name) VALUES (?, ?)", [
      { type: "integer", value: 2 },
      { type: "string", value: "beta" },
    ]);

    const jsonRows = await sqlExec("SELECT id, name FROM items ORDER BY id");
    expect(jsonRows.rows.length).toBe(2);
    expect(jsonRows.rows[0].name).toBe("alpha");
    expect(jsonRows.rows[1].name).toBe("beta");

    const rawRows = await sqlExec("SELECT name FROM items ORDER BY id", [], "raw");
    expect(rawRows.rows.length).toBe(2);
    expect(rawRows.rows[0][0]).toEqual({ type: "string", value: "alpha" });
    expect(rawRows.rows[1][0]).toEqual({ type: "string", value: "beta" });
  });
});
