import { describe, test, expect, afterAll } from "vitest";
import { mf, mfUrl } from "./mf";

afterAll(async () => {
  await mf.dispose();
});

describe("WorkersHttpClient timeout", () => {
  test("returns Timeout error when request exceeds timeout_ms", async () => {
    const resp = await mf.dispatchFetch(`${mfUrl}timeout`);
    const text = await resp.text();

    expect(resp.status).toBe(200);
    expect(text).toBe("Timeout(50)");
  });

  test("health check works", async () => {
    const resp = await mf.dispatchFetch(`${mfUrl}health`);
    expect(resp.status).toBe(200);
    expect(await resp.text()).toBe("ok");
  });
});
