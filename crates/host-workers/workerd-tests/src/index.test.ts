import { File } from "node:buffer";
import { describe, it, expect, afterAll } from "vitest";
import { FormData } from "undici";
import { Miniflare, kCurrentWorker } from "miniflare";

// Initialize Miniflare with our test worker
const mf = new Miniflare({
  workers: [
    {
      name: "host-workers-test",
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
        LATTICE_EXTRACT_PDF: "lattice-pdf-extract-test",
      },
      bindings: {
        LATTICE_RESUME_SERVICE_BINDING: "LATTICE_RESUME_SERVICE",
        LATTICE_INTERNAL_RESUME_TOKEN: "test-resume-token",
        LATTICE_WORKSPACE_MAX_TOTAL_BYTES: "64",
        LATTICE_WORKSPACE_MAX_FILE_COUNT: "4",
        LATTICE_WORKSPACE_MAX_SINGLE_FILE_BYTES: "32",
        LATTICE_MULTIPART_FILE_FIELD: "cv",
        LATTICE_MULTIPART_ARTIFACT_FIELD: "artifact",
        LATTICE_MULTIPART_FILENAME_METADATA_FIELD: "cv_filename",
        LATTICE_MULTIPART_EXPECTED_CONTENT_TYPE: "application/pdf",
        LATTICE_MULTIPART_REQUIRED_MAGIC: "%PDF-",
        LATTICE_MULTIPART_MAX_TOTAL_BYTES: String(10 * 1024 * 1024),
        LATTICE_MULTIPART_MAX_FILE_BYTES: String(8 * 1024 * 1024),
        LATTICE_MULTIPART_MAX_TEXT_BYTES: String(64 * 1024),
      },
    },
    {
      name: "lattice-pdf-extract-test",
      scriptPath: "./src/extraction-fixture.mjs",
      compatibilityDate: "2026-07-15",
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
        { type: "ESModule", include: ["**/*.mjs"], fallthrough: true },
      ],
    },
  ],
});

const mfUrl = await mf.ready;
const extractionFetcher = await mf.getWorker("lattice-pdf-extract-test");

function syntheticPdf(text: string): Uint8Array {
  const encoder = new TextEncoder();
  const escaped = text.replaceAll("\\", "\\\\").replaceAll("(", "\\(").replaceAll(")", "\\)");
  const stream = `BT /F1 12 Tf 72 720 Td (${escaped}) Tj ET`;
  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
    `<< /Length ${encoder.encode(stream).length} >>\nstream\n${stream}\nendstream`,
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
  ];
  let pdf = "%PDF-1.4\n% independent host-workers fixture\n";
  const offsets = [0];
  for (let index = 0; index < objects.length; index += 1) {
    offsets.push(encoder.encode(pdf).length);
    pdf += `${index + 1} 0 obj\n${objects[index]}\nendobj\n`;
  }
  const xref = encoder.encode(pdf).length;
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (const offset of offsets.slice(1)) {
    pdf += `${String(offset).padStart(10, "0")} 00000 n \n`;
  }
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  return encoder.encode(pdf);
}

function pdfForm(pdf: Uint8Array): FormData {
  const form = new FormData();
  form.set("full_name", "Ada Lovelace");
  form.set("cv", new File([pdf], "resume.pdf", { type: "application/pdf" }));
  return form;
}

async function invalidMultipartLimitResponse(
  value: string,
): Promise<{ status: number; body: unknown }> {
  const isolated = new Miniflare({
    workers: [
      {
        name: "host-workers-invalid-config",
        scriptPath: "./build/index.js",
        compatibilityDate: "2024-09-23",
        modules: true,
        modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true }],
        durableObjects: {
          FLOW_DO: { className: "FlowDurableObject", useSQLite: true },
          WORKSPACE_DO: { className: "WorkspaceDurableObject", useSQLite: true },
        },
        r2Buckets: ["WORKSPACE_BUCKET"],
        serviceBindings: {
          LATTICE_RESUME_SERVICE: kCurrentWorker,
          LATTICE_EXTRACT_PDF: "lattice-pdf-extract-invalid-config",
        },
        bindings: {
          LATTICE_MULTIPART_FILE_FIELD: "cv",
          LATTICE_MULTIPART_MAX_TOTAL_BYTES: value,
        },
      },
      {
        name: "lattice-pdf-extract-invalid-config",
        scriptPath: "./src/extraction-fixture.mjs",
        compatibilityDate: "2026-07-15",
        modules: true,
        modulesRules: [
          { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
          { type: "ESModule", include: ["**/*.mjs"], fallthrough: true },
        ],
      },
    ],
  });
  try {
    const url = await isolated.ready;
    const response = await isolated.dispatchFetch(`${url}pdf-extract`, {
      method: "POST",
      body: pdfForm(syntheticPdf("invalid config must fail closed")),
    });
    return { status: response.status, body: await response.json() };
  } finally {
    await isolated.dispose();
  }
}

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
      expect(typeof body.updated_at_ms).toBe("number");
      expect(body.updated_at_ms).toBeGreaterThan(0);
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
      expect(body.entries.map((entry: any) => entry.path)).toEqual([
        "stdlib/list/a.txt",
        "stdlib/list/b.txt",
      ]);
      expect(body.entries[0].size_bytes).toBe(1);
      expect(body.entries[1].size_bytes).toBe(3);
      expect(typeof body.entries[0].updated_at_ms).toBe("number");
      expect(typeof body.entries[1].updated_at_ms).toBe("number");
      expect(body.entries[0]).toHaveProperty("content_hash");
      expect(body.entries[1]).toHaveProperty("content_hash");
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

  describe("s11 lead intake example", () => {
    it("executes the lead intake flow and persists the generated hero image in workspace", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}leads`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "Ada Lovelace",
          email: "ada@example.test",
          message:
            "We need help with workflow automation and can move this quarter.",
        }),
      });

      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body).toMatchObject({
        to: "ada@example.test",
        subject: "Fast follow-up for workflow automation",
        body: "Hi Ada, we can move quickly and help with the workflow automation review.",
        image_artifact_path: "artifacts/lead-intake/ada-example-test/hero.png",
        priority: "high",
      });

      const keys = await listWorkspaceObjects();
      const imageKey = keys.find((key) =>
        key.endsWith("artifacts/lead-intake/ada-example-test/hero.png")
      );
      expect(imageKey).toBeDefined();

      await completeWorkspaceObject(imageKey!);
      await waitFor(async () => (await listWorkspaceObjects()).length === 0, {
        timeoutMs: 5000,
        intervalMs: 100,
      });
    });
  });

  describe("s13 GitHub issue investigator", () => {
    it("halts on dispatch and resumes through the internal resume route with a typed result", async () => {
      const initial = await mf.dispatchFetch(`${mfUrl}github/issues`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-lattice-test-bundle": "s13",
        },
        body: JSON.stringify({
          owner: "PSU3D0",
          repo: "Lattice",
          issue_number: 417,
          title: "panic when config file is missing",
          body: "The CLI panics when a config file is absent instead of returning a typed error.",
          comments: [
            {
              author: "maintainer",
              body: "Please confirm whether this reproduces on main.",
            },
          ],
        }),
      });

      expect(initial.status).toBe(202);
      const halted = await initial.json();
      expect(halted.halted).toBe(true);
      expect(halted.node).toBe("dispatch_investigation_job");
      expect(halted.payload?.state).toBe("waiting");
      expect(typeof halted.payload?.resume_token).toBe("string");
      expect(halted.payload?.resume_token.length).toBeGreaterThan(0);
      expect(halted.payload?.dispatch_receipt?.backend_kind).toBe("sandbox_http");
      expect(halted.payload?.dispatch_receipt?.metadata?.http_status).toBe(202);

      const checkpointId = halted?.payload?.checkpoint_id;
      expect(typeof checkpointId).toBe("string");
      expect(await checkpointFound(checkpointId)).toBe(true);

      const resumeResponse = await mf.dispatchFetch(`${mfUrl}__lattice/resume`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-lattice-internal-token": "test-resume-token",
          "x-lattice-test-bundle": "s13",
        },
        body: JSON.stringify({
          token: halted.payload.resume_token,
          payload: {
            state: "completed",
            plan: halted.payload.plan,
            job_id: halted.payload.job_id,
            result: {
              summary: "Likely null dereference in config loader when the file is absent.",
              confidence: 0.83,
              findings: [
                {
                  kind: "root_cause",
                  path: "src/config.rs",
                  detail: "Missing config path falls through to unwrap() instead of a typed error.",
                },
              ],
              proposed_actions: [
                {
                  kind: "comment",
                  body: "I reproduced the panic and traced it to the config loader unwrap path.",
                },
              ],
              artifacts: [
                {
                  kind: "report",
                  uri: "blob://reports/issue-417.json",
                },
              ],
            },
          },
        }),
      });

      expect(resumeResponse.status).toBe(200);
      const resumed = await resumeResponse.json();
      expect(resumed.resumed).toBe(true);
      expect(resumed.result?.resolution).toBe("investigation_completed");
      expect(resumed.result?.issue_number).toBe(417);
      expect(resumed.result?.triage?.needs_investigation).toBe(true);
      expect(resumed.result?.investigation?.summary).toContain("config loader");
      expect(await checkpointFound(checkpointId)).toBe(false);
    });
  });

  describe("service-bound PDF extraction", () => {
    it("fails closed on malformed, overflowing, or above-cap multipart limits", async () => {
      for (const value of ["not-a-number", "999999999999999999999999", "10485761"]) {
        const response = await invalidMultipartLimitResponse(value);
        expect(response).toEqual({
          status: 500,
          body: { error: "multipart configuration is invalid" },
        });
      }
    });

    it("rejects transform policy drift from the pinned deployment contract", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}__test/transform/policy-drift`, {
        method: "POST",
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ rejected: true });
    });

    it("rejects an oversized R2 read from the bounded prefix without fallback", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}__test/workspace/bounded-read`, {
        method: "POST",
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        code: "CAP-WS-008",
        message: "bounded workspace entry exceeds 64 bytes",
      });
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("exposes exactly one run-scoped ingress object while the bound transform is blocked", async () => {
      const block = await extractionFetcher.fetch("http://extract/__test/block", {
        method: "POST",
      });
      expect(block.status).toBe(200);
      const pdf = syntheticPdf("observable R2 staging");
      const firstPromise = mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(pdf),
      });

      await waitFor(async () => (await workspaceSnapshot()).entries.length === 1, {
        timeoutMs: 2_000,
        intervalMs: 10,
      });
      const staged = await workspaceSnapshot();
      expect(staged.entries).toHaveLength(1);
      expect(staged.entries[0].key).toContain("/ingress/0000-");
      expect(staged.entries[0].size).toBe(pdf.byteLength);

      const second = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(syntheticPdf("must fail before bounded R2 read")),
      });
      expect(second.status).toBe(500);
      expect(await second.text()).toContain("[busy]");
      expect((await workspaceSnapshot()).entries).toEqual(staged.entries);

      const release = await extractionFetcher.fetch("http://extract/__test/release", {
        method: "POST",
      });
      expect(release.status).toBe(200);
      const first = await firstPromise;
      expect(first.status).toBe(200);
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("finishes run-scoped cleanup after a client abort observed post-staging", async () => {
      await extractionFetcher.fetch("http://extract/__test/block", { method: "POST" });
      const controller = new AbortController();
      const pending = mf
        .dispatchFetch(`${mfUrl}pdf-extract`, {
          method: "POST",
          body: pdfForm(syntheticPdf("abort cleanup sentinel")),
          signal: controller.signal,
        })
        .catch(() => undefined);
      await waitFor(async () => (await workspaceSnapshot()).entries.length === 1, {
        timeoutMs: 2_000,
        intervalMs: 10,
      });
      controller.abort();
      await extractionFetcher.fetch("http://extract/__test/release", { method: "POST" });
      await pending.catch(() => undefined);
      await waitFor(async () => (await workspaceSnapshot()).entries.length === 0, {
        timeoutMs: 2_000,
        intervalMs: 10,
      });
    });

    it("stages multipart bytes in R2, bounded-reads them, and returns checked text", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(syntheticPdf("Workers multipart extraction")),
      });
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.page_count).toBe(1);
      expect(body.resume_text).toContain("Workers multipart extraction");
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("rejects oversized files before workspace staging", async () => {
      const oversized = new Uint8Array(8 * 1024 * 1024 + 1);
      oversized.set(new TextEncoder().encode("%PDF-"));
      const response = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(oversized),
      });
      expect(response.status).toBe(413);
      expect(await response.json()).toEqual({
        error: "multipart file exceeds limit",
        class: "payload_too_large",
      });
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("stream-counts the total multipart ceiling without trusting Content-Length", async () => {
      const boundary = "lattice-total-over-limit";
      const prefix = new TextEncoder().encode(
        `--${boundary}\r\nContent-Disposition: form-data; name="cv"; filename="oversized.pdf"\r\nContent-Type: application/pdf\r\n\r\n%PDF-`,
      );
      let emitted = 0;
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          if (emitted === 0) controller.enqueue(prefix);
          if (emitted < 11) {
            controller.enqueue(new Uint8Array(1024 * 1024));
            emitted += 1;
          } else {
            controller.close();
          }
        },
      });
      const response = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        headers: { "content-type": `multipart/form-data; boundary=${boundary}` },
        body,
        duplex: "half",
      });
      expect(response.status).toBe(413);
      expect(await response.json()).toEqual({
        error: "multipart request exceeds limit",
        class: "payload_too_large",
      });
      expect(await listWorkspaceObjects()).toEqual([]);

      const recovery = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(syntheticPdf("recovered after total limit")),
      });
      expect(recovery.status).toBe(200);
    });

    it("bounds aggregate text and rejects duplicate file fields", async () => {
      const textHeavy = pdfForm(syntheticPdf("text ceiling"));
      textHeavy.set("notes", "x".repeat(64 * 1024 + 1));
      const textResponse = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: textHeavy,
      });
      expect(textResponse.status).toBe(413);
      expect(await textResponse.json()).toEqual({
        error: "multipart text fields exceed limit",
        class: "payload_too_large",
      });

      const duplicate = new FormData();
      duplicate.append(
        "cv",
        new File([syntheticPdf("first")], "first.pdf", { type: "application/pdf" }),
      );
      duplicate.append(
        "cv",
        new File([syntheticPdf("second")], "second.pdf", { type: "application/pdf" }),
      );
      const duplicateResponse = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: duplicate,
      });
      expect(duplicateResponse.status).toBe(400);
      expect(await duplicateResponse.json()).toEqual({
        error: "multipart field is duplicated or invalid",
        class: "bad_request",
      });
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("rejects MIME and magic mismatches before workspace staging", async () => {
      const wrongMime = new FormData();
      wrongMime.set(
        "cv",
        new File([syntheticPdf("wrong MIME")], "resume.pdf", { type: "text/plain" }),
      );
      const mimeResponse = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: wrongMime,
      });
      expect(mimeResponse.status).toBe(415);
      expect(await mimeResponse.json()).toEqual({
        error: "multipart file has unsupported media type",
        class: "unsupported_media_type",
      });

      const magicResponse = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(new TextEncoder().encode("not a PDF")),
      });
      expect(magicResponse.status).toBe(415);
      expect(await magicResponse.json()).toEqual({
        error: "multipart file does not match required magic",
        class: "unsupported_media_type",
      });
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("rejects unattested failures and preserves generic platform termination", async () => {
      await extractionFetcher.fetch("http://extract/__test/mode?value=wrong-attestation", {
        method: "POST",
      });
      const unattested = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(syntheticPdf("unattested response")),
      });
      expect(unattested.status).toBe(500);
      expect(await unattested.text()).toContain("[invalid_output]");

      await extractionFetcher.fetch("http://extract/__test/mode?value=platform-terminated", {
        method: "POST",
      });
      const terminated = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(syntheticPdf("generic termination")),
      });
      expect(terminated.status).toBe(500);
      expect(await terminated.text()).toContain("[platform_terminated]");
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("sanitizes hostile PDFs and cleans the run-scoped artifact", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(new TextEncoder().encode("%PDF-hostile private parser sentinel")),
      });
      expect(response.status).toBe(500);
      const text = await response.text();
      expect(text).toContain("unsupported_document");
      expect(text).not.toContain("private parser sentinel");
      expect(await listWorkspaceObjects()).toEqual([]);
    });

    it("enforces one isolate-local multipart request with no waiter queue", async () => {
      const boundary = "lattice-workers-pending";
      let controller!: ReadableStreamDefaultController<Uint8Array>;
      const body = new ReadableStream<Uint8Array>({
        start(value) {
          controller = value;
          value.enqueue(
            new TextEncoder().encode(
              `--${boundary}\r\nContent-Disposition: form-data; name="cv"; filename="pending.pdf"\r\nContent-Type: application/pdf\r\n\r\n%PDF-`,
            ),
          );
        },
      });
      const firstPromise = mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        headers: { "content-type": `multipart/form-data; boundary=${boundary}` },
        body,
        duplex: "half",
      });
      await new Promise((resolve) => setTimeout(resolve, 25));

      const second = await mf.dispatchFetch(`${mfUrl}pdf-extract`, {
        method: "POST",
        body: pdfForm(syntheticPdf("must not queue")),
      });
      expect(second.status).toBe(503);
      expect(await second.json()).toEqual({ error: "busy" });

      controller.enqueue(new TextEncoder().encode(`\r\n--${boundary}--\r\n`));
      controller.close();
      const first = await firstPromise;
      expect(first.status).toBe(500);
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

async function workspaceSnapshot(prefix = "workspace/"): Promise<{
  keys: string[];
  entries: Array<{ key: string; size: number }>;
}> {
  const response = await mf.dispatchFetch(
    `${mfUrl}__test/workspace/objects?prefix=${encodeURIComponent(prefix)}`
  );
  expect(response.status).toBe(200);
  return (await response.json()) as {
    keys: string[];
    entries: Array<{ key: string; size: number }>;
  };
}

async function listWorkspaceObjects(prefix = "workspace/"): Promise<string[]> {
  return (await workspaceSnapshot(prefix)).keys;
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

async function completeWorkspaceObject(objectKey: string): Promise<void> {
  const response = await mf.dispatchFetch(`${mfUrl}__test/workspace/delete-object`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ object_key: objectKey }),
  });
  expect(response.status).toBe(200);
}
