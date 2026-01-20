import { Miniflare } from "miniflare";
import { MockAgent } from "undici";

const mockAgent = new MockAgent();

// Mock a slow endpoint that delays 500ms - longer than our 50ms timeout
mockAgent
  .get("https://miniflare.mocks")
  .intercept({ path: "/slow" })
  .reply(200, "delayed response")
  .delay(500);

const mf = new Miniflare({
  workers: [
    {
      scriptPath: "./build/index.js",
      compatibilityDate: "2024-09-23",
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
      ],
      fetchMock: mockAgent,
    },
  ],
});

export { mf };
export const mfUrl = await mf.ready;
