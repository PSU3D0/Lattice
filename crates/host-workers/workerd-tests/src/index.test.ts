import { describe, it, expect, afterAll } from "vitest";
import { Miniflare, kCurrentWorker } from "miniflare";

// Initialize Miniflare with our test worker
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
        WORKSPACE_DO: {
          className: "WorkspaceDurableObject",
          useSQLite: true,
        },
      },
      r2Buckets: ["WORKSPACE_BUCKET"],
      serviceBindings: {
        LATTICE_RESUME_SERVICE: kCurrentWorker,
      },
      bindings: {
        LATTICE_RESUME_SERVICE_BINDING: "LATTICE_RESUME_SERVICE",
        LATTICE_INTERNAL_RESUME_TOKEN: "test-resume-token",
        LATTICE_WORKSPACE_MAX_TOTAL_BYTES: "64",
        LATTICE_WORKSPACE_MAX_FILE_COUNT: "4",
        LATTICE_WORKSPACE_MAX_SINGLE_FILE_BYTES: "32",
      },
    },
  ],
});

const mfUrl = await mf.ready;

afterAll(async () => {
  await mf.dispose();
});

describe("host-workers E2E", () => {
  describe("/health endpoint", () => {
    it("should return 200 OK with status JSON", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}health`);

      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body).toEqual({ status: "ok" });
    });

    it("should have correct content-type header", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}health`);

      expect(response.headers.get("content-type")).toContain("application/json");
    });
  });

  describe("/echo endpoint", () => {
    it("should echo back the request body", async () => {
      const payload = { message: "hello", count: 42 };
      const response = await mf.dispatchFetch(`${mfUrl}echo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body).toEqual({ echoed: payload });
    });

    it("should handle empty body", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}echo`, {
        method: "POST",
      });

      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body).toEqual({ echoed: null });
    });

    it("should echo nested objects", async () => {
      const payload = {
        user: { name: "test", id: 123 },
        items: [1, 2, 3],
        nested: { deep: { value: true } },
      };
      const response = await mf.dispatchFetch(`${mfUrl}echo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body).toEqual({ echoed: payload });
    });
  });

  describe("/stream endpoint", () => {
    it("should return SSE content-type", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ count: 1 }),
      });

      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toBe("text/event-stream");
    });

    it("should stream the default number of chunks (3)", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}stream`, {
        method: "POST",
      });

      expect(response.status).toBe(200);

      const text = await response.text();
      const events = parseSSE(text);

      expect(events.length).toBe(3);
      expect(events[0]).toEqual({ index: 0, message: "chunk 0" });
      expect(events[1]).toEqual({ index: 1, message: "chunk 1" });
      expect(events[2]).toEqual({ index: 2, message: "chunk 2" });
    });

    it("should stream custom number of chunks", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ count: 5 }),
      });

      expect(response.status).toBe(200);

      const text = await response.text();
      const events = parseSSE(text);

      expect(events.length).toBe(5);
      for (let i = 0; i < 5; i++) {
        expect(events[i]).toEqual({ index: i, message: `chunk ${i}` });
      }
    });

    it("should handle count of 0", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ count: 0 }),
      });

      expect(response.status).toBe(200);

      const text = await response.text();
      const events = parseSSE(text);

      expect(events.length).toBe(0);
    });
  });

  describe("/cancel endpoint", () => {
    it("should handle abort signal", async () => {
      const controller = new AbortController();

      const fetchPromise = mf.dispatchFetch(`${mfUrl}cancel`, {
        method: "POST",
        signal: controller.signal,
      });

      setTimeout(() => controller.abort(), 100);

      try {
        const response = await fetchPromise;
        expect(response.status).toBe(503);
        const body = await response.json();
        expect(body).toEqual({ error: "execution cancelled" });
      } catch (error) {
        expect(error).toBeDefined();
      }
    });
  });

  describe("durability + alarm-driven resume", () => {
    it("rejects internal resume route without token", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}__lattice/resume`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ checkpoint_id: "cp-missing" }),
      });
      expect(response.status).toBe(401);
    });

    it("resumes timer checkpoint via DO alarm dispatch path", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}timer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          duration: "50ms",
          payload: { hello: "world" },
        }),
      });

      expect(response.status).toBe(202);
      const halted = await response.json();
      expect(halted.halted).toBe(true);
      expect(halted.node).toBe("timer_wait");

      const checkpointId = halted?.payload?.checkpoint_id;
      expect(typeof checkpointId).toBe("string");
      expect(checkpointId.length).toBeGreaterThan(0);

      await waitFor(async () => {
        await triggerAlarmTick();
        return !(await checkpointFound(checkpointId));
      }, {
        timeoutMs: 8000,
        intervalMs: 100,
      });

      const finalFound = await checkpointFound(checkpointId);
      expect(finalFound).toBe(false);
    });
  });

  describe("workspace backend", () => {
    it("round-trips workspace artifacts and cleans them up on terminal success", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          content: "hello workspace",
          prefix: "artifacts",
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.original).toBe("hello workspace");
      expect(body.upper).toBe("HELLO WORKSPACE");
      expect(body.missing_read).toBe(false);
      expect(body.missing_delete).toBe(false);
      expect(body.deleted_upper).toBe(true);
      expect(body.listed_paths_before_delete).toEqual([
        "artifacts/original.txt",
        "artifacts/upper.txt",
      ]);
      expect(body.listed_paths_after_delete).toEqual(["artifacts/original.txt"]);

      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("preserves workspace state across resume and cleans it up after completion", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-resume`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          duration: "10s",
          content: "resume workspace",
        }),
      });

      expect(response.status).toBe(202);
      const halted = await response.json();
      expect(halted.halted).toBe(true);

      const checkpointId = halted?.payload?.checkpoint_id;
      expect(typeof checkpointId).toBe("string");
      expect(await checkpointFound(checkpointId)).toBe(true);

      await waitFor(async () => {
        const keys = await listWorkspaceObjects();
        return keys.some((key) => key.endsWith("resume/input.txt"));
      }, {
        timeoutMs: 3000,
        intervalMs: 100,
      });

      const resumeResponse = await mf.dispatchFetch(`${mfUrl}__lattice/resume`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-lattice-internal-token": "test-resume-token",
        },
        body: JSON.stringify({ checkpoint_id: checkpointId }),
      });

      expect(resumeResponse.status).toBe(200);
      const resumed = await resumeResponse.json();
      expect(resumed.resumed).toBe(true);
      expect(resumed.result?.resumed).toBe(true);
      expect(resumed.result?.content).toBe("resume workspace");
      expect(resumed.result?.listed_paths).toEqual(["resume/input.txt"]);

      expect(await checkpointFound(checkpointId)).toBe(false);
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("enforces single-file workspace quota in the workers backend", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-quota`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "single_file" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("max_single_file_bytes");
    });

    it("enforces total-bytes workspace quota in the workers backend", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-quota`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "total_bytes" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("max_total_bytes");
    });

    it("enforces file-count workspace quota in the workers backend", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-quota`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "file_count" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("max_file_count");
    });

    it("rejects traversal paths before reaching the backend write path", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-invalid-path`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "write_traversal" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("path traversal");
    });

    it("retains workspace objects until the retained cleanup path runs", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-retained`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          content: "keep me around",
          prefix: "retained-artifacts",
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.content).toBe("keep me around");
      expect(body.listed_paths).toEqual(["retained-artifacts/artifact.txt"]);

      const keys = await listWorkspaceObjects();
      expect(keys).toHaveLength(1);
      expect(keys[0]).toContain("retained-artifacts/artifact.txt");

      await runWorkspaceRetainedCleanup(keys[0], Date.now() + 60_000);
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("rejects traversal prefixes before reaching the backend list path", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-invalid-path`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "list_traversal" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("path traversal");
    });

    it("keeps overwrite quota accounting delta-based", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-mutation`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "overwrite_delta" }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.ok).toBe(true);
      expect(body.listed_paths).toEqual(["mutation/artifact.txt"]);
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("allows delete and rewrite without counter drift", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-mutation`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "delete_rewrite" }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.ok).toBe(true);
      expect(body.listed_paths).toEqual(["mutation/second.txt"]);
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("rejects blocked prefixes by host policy", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-blocked-prefix`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "write_blocked" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("blocked by host policy");
    });

    it("rejects blocked prefixes for prefix listing", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-blocked-prefix`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "list_blocked" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("blocked by host policy");
    });

    it("rejects overly deep paths by host policy", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-blocked-prefix`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "max_depth" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("max depth");
    });

    it("rejects overly long paths by host policy", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-blocked-prefix`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ kind: "max_length" }),
      });

      expect(response.status).toBe(500);
      const body = await response.json();
      expect(String(body.error)).toContain("max length");
    });

    it("executes stdlib workspace write against the workers backend", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-stdlib-write`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          path: "stdlib/write.txt",
          content: "hello stdlib",
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.path).toBe("stdlib/write.txt");
      expect(body.size_bytes).toBe(12);
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("executes stdlib workspace read against the workers backend", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-stdlib-read`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          path: "stdlib/read.txt",
          content: "hello stdlib",
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.path).toBe("stdlib/read.txt");
      expect(body.found).toBe(true);
      expect(body.value).toEqual({
        kind: "bytes",
        bytes: Array.from(Buffer.from("hello stdlib", "utf8")),
      });
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("executes stdlib workspace list against the workers backend", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-stdlib-list`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prefix: "stdlib/list" }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.paths).toEqual(["stdlib/list/a.txt", "stdlib/list/b.txt"]);
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("executes stdlib workspace delete against the workers backend", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}workspace-stdlib-delete`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          path: "stdlib/delete.txt",
          content: "delete me",
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.path).toBe("stdlib/delete.txt");
      expect(body.deleted).toBe(true);
      expect(await listWorkspaceObjects()).toEqual([]);
    });
  });

  describe("error handling", () => {
    it("should return 404 for unknown routes", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}unknown`);

      expect(response.status).toBe(404);
    });

    it("should return 404 for wrong HTTP method", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}echo`, {
        method: "GET",
      });

      expect(response.status).toBe(404);
    });
  });
});

async function checkpointFound(checkpointId: string): Promise<boolean> {
  const response = await mf.dispatchFetch(
    `${mfUrl}__test/checkpoint?checkpoint_id=${encodeURIComponent(checkpointId)}`,
    { method: "GET" }
  );
  expect(response.status).toBe(200);
  const body = await response.json();
  return Boolean(body?.found);
}

// Miniflare does not always auto-fire DO alarms deterministically in unit tests,
// so this endpoint exercises the same DO alarm dispatch code path explicitly.
async function triggerAlarmTick(): Promise<void> {
  const response = await mf.dispatchFetch(`${mfUrl}__test/alarm/tick`, {
    method: "POST",
  });
  expect(response.status).toBe(200);
}

async function waitFor(
  fn: () => Promise<boolean>,
  opts: { timeoutMs: number; intervalMs: number }
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < opts.timeoutMs) {
    if (await fn()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, opts.intervalMs));
  }
  throw new Error(`waitFor timeout after ${opts.timeoutMs}ms`);
}

/**
 * Parse SSE event stream into array of JSON objects
 */
function parseSSE(text: string): unknown[] {
  const events: unknown[] = [];
  const lines = text.split("\n");

  for (const line of lines) {
    if (line.startsWith("data: ")) {
      const data = line.slice(6);
      try {
        events.push(JSON.parse(data));
      } catch {
        // Skip non-JSON lines
      }
    }
  }

  return events;
}

async function listWorkspaceObjects(prefix = "workspace/"): Promise<string[]> {
  const response = await mf.dispatchFetch(
    `${mfUrl}__test/workspace/objects?prefix=${encodeURIComponent(prefix)}`
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { keys: string[] };
  return body.keys;
}

async function runWorkspaceRetainedCleanup(
  objectKey: string,
  nowMs: number
): Promise<void> {
  const response = await mf.dispatchFetch(
    `${mfUrl}__test/workspace/run-retained-cleanup`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ object_key: objectKey, now_ms: nowMs }),
    }
  );
  expect(response.status).toBe(200);
}
