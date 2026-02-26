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
      },
      serviceBindings: {
        LATTICE_RESUME_SERVICE: kCurrentWorker,
      },
      bindings: {
        LATTICE_RESUME_SERVICE_BINDING: "LATTICE_RESUME_SERVICE",
        LATTICE_INTERNAL_RESUME_TOKEN: "test-resume-token",
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
