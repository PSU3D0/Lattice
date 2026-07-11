import { describe, test, expect, afterAll, beforeEach } from "vitest";
import { Miniflare } from "miniflare";
import http from "node:http";
import { AddressInfo } from "node:net";

// Packet H2a definition of done (Workers provider path).
//
// NOTE: this deliberately does NOT use miniflare's `fetchMock`. With
// `fetchMock`, miniflare routes every outbound subrequest through a Node
// loopback that reconstructs the request WITHOUT its redirect mode and
// dispatches it via `undici.fetch` (redirect: "follow"), so the mock layer
// follows 3xx itself and the test could never observe whether workerd
// honored `redirect: manual`. Instead we run a real local HTTP server and
// let workerd perform the outbound fetch — the actual runtime behavior.

let hopHits = 0;
let targetHits = 0;

const server = http.createServer((req, res) => {
  if (req.url === "/hop") {
    hopHits++;
    res.writeHead(302, { location: `http://${req.headers.host}/target` });
    res.end();
  } else if (req.url === "/target") {
    targetHits++;
    res.writeHead(200, { "content-type": "text/plain" });
    res.end("followed");
  } else {
    res.writeHead(404);
    res.end();
  }
});
await new Promise<void>((resolve) =>
  server.listen(0, "127.0.0.1", () => resolve()),
);
const port = (server.address() as AddressInfo).port;
const base = `http://127.0.0.1:${port}`;

const mf = new Miniflare({
  workers: [
    {
      scriptPath: "./build/index.js",
      compatibilityDate: "2024-09-23",
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
      ],
    },
  ],
});
const mfUrl = await mf.ready;

afterAll(async () => {
  await mf.dispose();
  server.close();
});

beforeEach(() => {
  hopHits = 0;
  targetHits = 0;
});

describe("WorkersHttpClient redirect handling (packet H2a)", () => {
  test("redirect Off surfaces the 3xx (status + Location) and never follows", async () => {
    const resp = await mf.dispatchFetch(
      `${mfUrl}redirect-off?base=${encodeURIComponent(base)}`,
    );
    const text = await resp.text();

    expect(resp.status).toBe(200);
    expect(text).toBe(`status=302 location=${base}/target body=`);
    // The hop was requested exactly once and the target NEVER fetched.
    expect(hopHits).toBe(1);
    expect(targetHits).toBe(0);
  });

  test("default (serde-default Follow) keeps the historical follow behavior", async () => {
    const resp = await mf.dispatchFetch(
      `${mfUrl}redirect-follow?base=${encodeURIComponent(base)}`,
    );
    const text = await resp.text();

    expect(resp.status).toBe(200);
    expect(text).toBe("status=200 body=followed");
    expect(hopHits).toBe(1);
    expect(targetHits).toBe(1);
  });
});
