import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";
import http from "node:http";

const tenant = "acme";
const stream = "orders";

type OtelRequest = {
  headers: Record<string, string | string[]>;
  body: string;
};

async function startOtelServer() {
  const received: OtelRequest[] = [];
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      received.push({
        headers: req.headers,
        body: Buffer.concat(chunks).toString("utf-8"),
      });
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("ok");
    });
  });

  const url = await new Promise<string>((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (typeof address === "object" && address) {
        resolve(`http://127.0.0.1:${address.port}/v1/traces`);
      }
    });
  });

  return {
    url,
    received,
    close: () => server.close(),
  };
}

let mf: Miniflare;
let mfUrl: string;
let otel: Awaited<ReturnType<typeof startOtelServer>>;

beforeAll(async () => {
  otel = await startOtelServer();
  mf = new Miniflare({
    workers: [
      {
        scriptPath: "./build/index.js",
        compatibilityDate: "2024-09-23",
        modules: true,
        modulesRules: [
          { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
        ],
        kvNamespaces: ["EVENTS_KV"],
        durableObjects: {
          DEDUP_DO: {
            className: "FlowDurableObject",
            useSQLite: true,
          },
        },
        bindings: {
          OTEL_EXPORTER_OTLP_ENDPOINT: otel.url,
          OTEL_EXPORTER_OTLP_HEADERS: "authorization=Bearer test-token",
          SNAPSHOT_INTERVAL: "2",
          LIST_LIMIT: "2",
          STREAM_PAGE_LIMIT: "2",
          STREAM_MAX_PAGES: "2",
          STREAM_PAGE_DELAY_MS: "5",
        },
      },
    ],
  });

  mfUrl = await mf.ready;
});

afterAll(async () => {
  await mf.dispose();
  otel.close();
});

async function ingest(idempotencyKey: string, payload: number[]) {
  const response = await mf.dispatchFetch(`${mfUrl}ingest`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      tenant,
      stream,
      idempotency_key: idempotencyKey,
      payload,
      metadata: { source: "test" },
    }),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`ingest failed: ${response.status} ${text}`);
  }
  return response.json();
}

async function listEvents(cursor?: string, limit?: number) {
  const params = new URLSearchParams({ tenant, stream });
  if (cursor) params.set("cursor", cursor);
  if (limit) params.set("limit", `${limit}`);
  const response = await mf.dispatchFetch(`${mfUrl}events?${params.toString()}`);
  expect(response.status).toBe(200);
  return response.json();
}

async function getSnapshot() {
  const params = new URLSearchParams({ tenant, stream });
  const response = await mf.dispatchFetch(`${mfUrl}snapshot?${params.toString()}`);
  return response;
}

async function streamEvents(limit = 2) {
  const params = new URLSearchParams({ tenant, stream, limit: `${limit}` });
  const response = await mf.dispatchFetch(`${mfUrl}stream?${params.toString()}`, {
    method: "GET",
    headers: { Accept: "text/event-stream" },
  });
  expect(response.status).toBe(200);
  return response.text();
}

describe("cloudflare idem ingest example", () => {
  it("dedupes ingest by idempotency key", async () => {
    const first = await ingest("idem-1", [1, 2, 3]);
    const second = await ingest("idem-1", [1, 2, 3]);

    expect(first.inserted).toBe(true);
    expect(second.inserted).toBe(false);
    expect(second.seq).toBe(first.seq);

    const listed = await listEvents();
    expect(listed.events.length).toBe(1);
  });

  it("preserves sequence ordering", async () => {
    await Promise.all([
      ingest("idem-2", [4, 5, 6]),
      ingest("idem-3", [7, 8, 9]),
      ingest("idem-4", [10, 11, 12]),
    ]);

    const listed = await listEvents();
    const seqs = listed.events.map((event: { seq: number }) => event.seq);
    const sorted = [...seqs].sort((a, b) => a - b);
    expect(seqs).toEqual(sorted);
  });

  it("paginates replay with cursor", async () => {
    const first = await listEvents(undefined, 2);
    expect(first.events.length).toBe(2);
    expect(first.cursor).toBeDefined();

    const second = await listEvents(first.cursor, 2);
    expect(second.events.length).toBeGreaterThanOrEqual(1);
  });

  it("serves snapshots from KV", async () => {
    const response = await getSnapshot();
    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body.seq).toBeGreaterThanOrEqual(2);
  });

  it("streams SSE events", async () => {
    const text = await streamEvents(2);
    const events = parseSSE(text);
    expect(events.length).toBeGreaterThan(0);
  });

  it("dispatches OTLP/HTTP payloads", async () => {
    const hasIngest = otel.received.some((req) =>
      req.body.includes("\"name\":\"ingest\"")
    );
    const hasReplay = otel.received.some((req) =>
      req.body.includes("\"name\":\"replay_page\"")
    );

    expect(hasIngest).toBe(true);
    expect(hasReplay).toBe(true);

    const authHeader = otel.received[0]?.headers["authorization"];
    const authValue = Array.isArray(authHeader) ? authHeader[0] : authHeader;
    expect(authValue).toBe("Bearer test-token");
  });
});

function parseSSE(text: string): unknown[] {
  const events: unknown[] = [];
  const lines = text.split("\n");

  for (const line of lines) {
    if (line.startsWith("data: ")) {
      const data = line.slice(6);
      try {
        events.push(JSON.parse(data));
      } catch {
        // ignore
      }
    }
  }

  return events;
}
