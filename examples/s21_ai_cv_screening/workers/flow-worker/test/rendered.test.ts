import { File } from "node:buffer";
import { readFile } from "node:fs/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { FormData } from "undici";
import { Miniflare } from "miniflare";
import { parse } from "smol-toml";

const flowToml = parse(await readFile("deploy/wrangler.toml", "utf8")) as Record<string, any>;
const extractionToml = parse(
  await readFile("deploy/extraction-worker/wrangler.toml", "utf8"),
) as Record<string, any>;
const service = (flowToml.services as Array<Record<string, string>>).find(
  (entry) => entry.binding === "LATTICE_EXTRACT_PDF",
);
if (service === undefined) throw new Error("rendered extraction service binding is absent");

const vars = Object.fromEntries(
  Object.entries(flowToml.vars ?? {}).map(([key, value]) => [key, String(value)]),
);
const mf = new Miniflare({
  workers: [
    {
      name: String(flowToml.name),
      scriptPath: `./deploy/${String(flowToml.main)}`,
      compatibilityDate: String(flowToml.compatibility_date),
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
        { type: "ESModule", include: ["**/*.js", "**/*.mjs"], fallthrough: true },
      ],
      durableObjects: Object.fromEntries(
        (flowToml.durable_objects?.bindings ?? []).map((binding: Record<string, string>) => [
          binding.name,
          { className: binding.class_name, useSQLite: true },
        ]),
      ),
      r2Buckets: (flowToml.r2_buckets ?? []).map(
        (binding: Record<string, string>) => binding.binding,
      ),
      kvNamespaces: (flowToml.kv_namespaces ?? []).map(
        (binding: Record<string, string>) => binding.binding,
      ),
      serviceBindings: {
        LATTICE_EXTRACT_PDF: service.service,
        LATTICE_S21_PROVIDER: "mock-provider",
      },
      bindings: {
        ...vars,
        LATTICE_S21_HTTP_MODE: "service_binding",
        LATTICE_CONNECTOR_AUTH_LLM_API_KEY: "test-llm-token",
        LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH: "test-google-token",
        LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL: "http://mock-provider",
        LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL: "http://mock-provider",
        LATTICE_CONNECTOR_ENDPOINT_GOOGLE_GMAIL_DEFAULT_BASE_URL: "http://mock-provider",
      },
    },
    {
      name: String(extractionToml.name),
      scriptPath: "./deploy/extraction-worker/dist/index.mjs",
      compatibilityDate: String(extractionToml.compatibility_date),
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
        { type: "ESModule", include: ["**/*.mjs"], fallthrough: true },
      ],
    },
    {
      name: "mock-provider",
      scriptPath: "./mock-provider/src/index.mjs",
      compatibilityDate: "2026-07-15",
      modules: true,
      bindings: {
        MOCK_ADMIN_BEARER: "test-admin-token",
        MOCK_LLM_BEARER: "test-llm-token",
        MOCK_GOOGLE_BEARER: "test-google-token",
      },
    },
  ],
});
const providerFetcher = await mf.getWorker("mock-provider");

beforeAll(async () => {
  await mf.ready;
});
afterAll(async () => {
  await mf.dispose();
});

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
  let pdf = "%PDF-1.4\n% independent rendered S21 fixture\n";
  const offsets = [0];
  for (let index = 0; index < objects.length; index += 1) {
    offsets.push(encoder.encode(pdf).length);
    pdf += `${index + 1} 0 obj\n${objects[index]}\nendobj\n`;
  }
  const xref = encoder.encode(pdf).length;
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (const offset of offsets.slice(1)) pdf += `${String(offset).padStart(10, "0")} 00000 n \n`;
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  return encoder.encode(pdf);
}

function applicationForm(pdf: Uint8Array, email: string): FormData {
  const form = new FormData();
  form.set("full_name", "Ada Lovelace");
  form.set("email", email);
  form.set("expectation", "Build reliable systems");
  form.set("linkedin", "https://example.test/ada");
  form.set("cv", new File([pdf], "resume.pdf", { type: "application/pdf" }));
  return form;
}

async function effects(): Promise<Record<string, number>> {
  const response = await providerFetcher.fetch("http://mock-provider/__counts", {
    headers: { authorization: "Bearer test-admin-token" },
  });
  expect(response.status).toBe(200);
  return response.json() as Promise<Record<string, number>>;
}

async function invoke(pdf: Uint8Array, email = "ada@example.test"): Promise<Response> {
  return mf.dispatchFetch("http://s21-w4-flow/cv-screening", {
    method: "POST",
    body: applicationForm(pdf, email),
  });
}

async function configurationFailure(
  mode: string | undefined,
  includeProvider: boolean,
  endpoint = "http://mock-provider",
): Promise<number> {
  const isolated = new Miniflare({
    workers: [
      {
        name: "s21-invalid-config-flow",
        scriptPath: `./deploy/${String(flowToml.main)}`,
        compatibilityDate: String(flowToml.compatibility_date),
        modules: true,
        modulesRules: [
          { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
          { type: "ESModule", include: ["**/*.js", "**/*.mjs"], fallthrough: true },
        ],
        durableObjects: Object.fromEntries(
          (flowToml.durable_objects?.bindings ?? []).map((binding: Record<string, string>) => [
            binding.name,
            { className: binding.class_name, useSQLite: true },
          ]),
        ),
        r2Buckets: ["WORKSPACE_BUCKET"],
        kvNamespaces: ["FLOW_KV"],
        serviceBindings: {
          LATTICE_EXTRACT_PDF: service.service,
          ...(includeProvider ? { LATTICE_S21_PROVIDER: "mock-provider" } : {}),
        },
        bindings: {
          ...vars,
          ...(mode === undefined ? {} : { LATTICE_S21_HTTP_MODE: mode }),
          LATTICE_CONNECTOR_AUTH_LLM_API_KEY: "test-llm-token",
          LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH: "test-google-token",
          LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL: endpoint,
        },
      },
      {
        name: String(extractionToml.name),
        scriptPath: "./deploy/extraction-worker/dist/index.mjs",
        compatibilityDate: String(extractionToml.compatibility_date),
        modules: true,
        modulesRules: [
          { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
          { type: "ESModule", include: ["**/*.mjs"], fallthrough: true },
        ],
      },
      {
        name: "mock-provider",
        scriptPath: "./mock-provider/src/index.mjs",
        compatibilityDate: "2026-07-15",
        modules: true,
        bindings: {
          MOCK_ADMIN_BEARER: "test-admin-token",
          MOCK_LLM_BEARER: "test-llm-token",
          MOCK_GOOGLE_BEARER: "test-google-token",
        },
      },
    ],
  });
  try {
    await isolated.ready;
    const response = await isolated.dispatchFetch("http://s21-invalid-config-flow/cv-screening", {
      method: "POST",
      body: applicationForm(syntheticPdf("must fail before egress"), "no-egress@example.test"),
    });
    return response.status;
  } finally {
    await isolated.dispose();
  }
}

describe("render-derived full S21 two-Worker package", () => {
  it("executes extraction and exact mocked downstream effects, then suppresses redelivery", async () => {
    const pdf = syntheticPdf("Ada builds bounded distributed systems");
    const first = await invoke(pdf);
    expect(first.status).toBe(200);
    const firstBody = (await first.json()) as Record<string, unknown>;
    expect(firstBody.stored).toBe(true);
    expect(await effects()).toEqual({ llm: 1, sheetsRead: 1, sheetsAppend: 1, gmail: 2 });

    const second = await invoke(pdf);
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as Record<string, unknown>;
    expect(secondBody.stored).toBe(false);
    expect(await effects()).toEqual({ llm: 1, sheetsRead: 1, sheetsAppend: 1, gmail: 2 });

    const bucket = await mf.getR2Bucket("WORKSPACE_BUCKET");
    expect((await bucket.list()).objects).toHaveLength(0);
  }, 30_000);

  it("fails hostile input without provider effects and the next valid request survives", async () => {
    const before = await effects();
    const hostile = await invoke(
      new TextEncoder().encode("%PDF-private-hostile-sentinel"),
      "hostile@example.test",
    );
    expect(hostile.status).toBe(500);
    const hostileBody = JSON.stringify(await hostile.json());
    expect(hostileBody).toBe('{"error":"execution_failed"}');
    expect(hostileBody).not.toContain("private-hostile-sentinel");
    expect(hostileBody).not.toContain("hostile@example.test");
    expect(await effects()).toEqual(before);

    const recovered = await invoke(
      syntheticPdf("Grace recovers after hostile input"),
      "grace@example.test",
    );
    expect(recovered.status).toBe(200);
    const afterRecovery = {
      llm: before.llm + 1,
      sheetsRead: before.sheetsRead + 1,
      sheetsAppend: before.sheetsAppend + 1,
      gmail: before.gmail + 2,
    };
    expect(await effects()).toEqual(afterRecovery);

    const reflected = await invoke(
      syntheticPdf("reflect-provider private-document-sentinel"),
      "private-applicant@example.test",
    );
    expect(reflected.status).toBe(500);
    const reflectedBody = JSON.stringify(await reflected.json());
    expect(reflectedBody).toBe('{"error":"execution_failed"}');
    expect(reflectedBody).not.toContain("private-applicant");
    expect(reflectedBody).not.toContain("private-document-sentinel");
    expect(await effects()).toEqual({ ...afterRecovery, llm: afterRecovery.llm + 1 });
  }, 30_000);

  it("fails closed for absent proof routing, unknown mode, and cleartext ambient endpoints", async () => {
    const before = await effects();
    expect(await configurationFailure(undefined, true)).toBe(500);
    expect(await configurationFailure("service_binding", false)).toBe(500);
    expect(await configurationFailure("unknown", true)).toBe(500);
    expect(await configurationFailure("ambient_https", false, "http://provider.invalid")).toBe(500);
    expect(await effects()).toEqual(before);
  }, 30_000);
});
