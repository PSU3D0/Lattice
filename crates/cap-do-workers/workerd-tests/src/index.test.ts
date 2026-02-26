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

async function checkpointPut(
  checkpointId: string,
  flowId = "flow/test",
  runId = "run/test",
  ttlMs?: number
) {
  const response = await mf.dispatchFetch(`${mfUrl}checkpoint/put`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      checkpoint_id: checkpointId,
      flow_id: flowId,
      run_id: runId,
      ttl_ms: ttlMs,
      created_at_ms: Date.now(),
    }),
  });
  return response.json();
}

async function checkpointGet(checkpointId: string, flowId = "flow/test", runId = "run/test") {
  const params = new URLSearchParams({
    checkpoint_id: checkpointId,
    flow_id: flowId,
    run_id: runId,
  });
  const response = await mf.dispatchFetch(`${mfUrl}checkpoint/get?${params.toString()}`);
  return response.json();
}

async function checkpointLease(
  checkpointId: string,
  flowId = "flow/test",
  runId = "run/test",
  ttlSeconds = 30
) {
  const response = await mf.dispatchFetch(`${mfUrl}checkpoint/lease`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      checkpoint_id: checkpointId,
      flow_id: flowId,
      run_id: runId,
      ttl_seconds: ttlSeconds,
    }),
  });
  return response.json();
}

async function checkpointRelease(leaseId: string, expiresAtMs: number) {
  const response = await mf.dispatchFetch(`${mfUrl}checkpoint/release`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lease_id: leaseId, expires_at_ms: expiresAtMs }),
  });
  return response.json();
}

async function checkpointAck(checkpointId: string, flowId = "flow/test", runId = "run/test") {
  const params = new URLSearchParams({
    checkpoint_id: checkpointId,
    flow_id: flowId,
    run_id: runId,
  });
  const response = await mf.dispatchFetch(`${mfUrl}checkpoint/ack?${params.toString()}`, {
    method: "DELETE",
  });
  return response.json();
}

async function checkpointList(flowId = "flow/test") {
  const params = new URLSearchParams({ flow_id: flowId });
  const response = await mf.dispatchFetch(`${mfUrl}checkpoint/list?${params.toString()}`);
  return response.json();
}

async function scheduleAfter(
  checkpointId: string,
  flowId = "flow/test",
  runId = "run/test",
  delayMs = 25
) {
  const response = await mf.dispatchFetch(`${mfUrl}resume/schedule_after`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      checkpoint_id: checkpointId,
      flow_id: flowId,
      run_id: runId,
      delay_ms: delayMs,
    }),
  });
  return response.json();
}

async function scheduleStatus(scheduleId: string) {
  const response = await mf.dispatchFetch(`${mfUrl}resume/schedule_status`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ schedule_id: scheduleId }),
  });
  return response.json();
}

async function scheduleCancel(scheduleId: string) {
  const response = await mf.dispatchFetch(`${mfUrl}resume/schedule_cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ schedule_id: scheduleId }),
  });
  return response.json();
}

async function tokenCreate(
  checkpointId: string,
  flowId = "flow/test",
  runId = "run/test",
  ttlSeconds = 60,
  singleUse = true
) {
  const response = await mf.dispatchFetch(`${mfUrl}resume/token_create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      checkpoint_id: checkpointId,
      flow_id: flowId,
      run_id: runId,
      ttl_seconds: ttlSeconds,
      single_use: singleUse,
    }),
  });
  return response.json();
}

async function tokenResolve(token: string) {
  const response = await mf.dispatchFetch(`${mfUrl}resume/token_resolve`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token }),
  });
  return response.json();
}

async function tokenRevoke(token: string) {
  const response = await mf.dispatchFetch(`${mfUrl}resume/token_revoke`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token }),
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

  it("checkpoint store roundtrip + lease semantics", async () => {
    const id = "ckpt-e2e-1";
    await checkpointPut(id, "flow/e2e", "run/e2e", 60_000);

    const got = await checkpointGet(id, "flow/e2e", "run/e2e");
    expect(got.checkpoint_id).toBe(id);
    expect(got.flow_id).toBe("flow/e2e");

    const lease1 = await checkpointLease(id, "flow/e2e", "run/e2e", 30);
    expect(lease1.lease_id).toContain(id);

    const lease2 = await checkpointLease(id, "flow/e2e", "run/e2e", 30);
    expect(String(lease2.error)).toContain("lease conflict");

    const release = await checkpointRelease(lease1.lease_id, lease1.expires_at_ms);
    expect(release.success).toBe(true);

    const lease3 = await checkpointLease(id, "flow/e2e", "run/e2e", 30);
    expect(lease3.lease_id).toContain(id);

    const listed = await checkpointList("flow/e2e");
    expect(Array.isArray(listed.checkpoints)).toBe(true);
    expect(listed.checkpoints.some((c: any) => c.checkpoint_id === id)).toBe(true);

    const acked = await checkpointAck(id, "flow/e2e", "run/e2e");
    expect(acked.success).toBe(true);
  });

  it("resume scheduler status/cancel and token single-use", async () => {
    const checkpointId = "ckpt-e2e-2";
    await checkpointPut(checkpointId, "flow/e2e", "run/e2e", 60_000);

    const scheduled = await scheduleAfter(checkpointId, "flow/e2e", "run/e2e", 5);
    expect(typeof scheduled.schedule_id).toBe("string");

    await new Promise((r) => setTimeout(r, 15));

    const status1 = await scheduleStatus(scheduled.schedule_id);
    expect(status1.status).toContain("Fired");

    const cancelled = await scheduleCancel(scheduled.schedule_id);
    expect(cancelled.success).toBe(true);

    const status2 = await scheduleStatus(scheduled.schedule_id);
    expect(status2.status).toContain("Cancelled");

    const token = await tokenCreate(checkpointId, "flow/e2e", "run/e2e", 60, true);
    expect(typeof token.token).toBe("string");

    const resolved1 = await tokenResolve(token.token);
    expect(resolved1.checkpoint_id).toBe(checkpointId);

    const resolved2 = await tokenResolve(token.token);
    expect(String(resolved2.error)).toContain("already used");

    const revoke = await tokenRevoke(token.token);
    expect(revoke.success).toBe(true);
  });
});
