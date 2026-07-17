import { afterAll, describe, expect, it } from "vitest";
import { Miniflare } from "miniflare";
import { unstable_readConfig } from "wrangler";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const config = unstable_readConfig({ config: path.join(root, "wrangler.toml") });
const moduleRules = [
  { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
  { type: "ESModule", include: ["**/*.mjs"], fallthrough: true },
];

function miniflare(scriptPath) {
  return new Miniflare({
    workers: [
      {
        name: "pdf-extract-under-test",
        scriptPath,
        compatibilityDate: config.compatibility_date,
        modules: true,
        modulesRules: moduleRules,
      },
    ],
  });
}

const canonical = miniflare(path.join(root, "dist", "index.mjs"));
const fixtureNames = [
  "fresh",
  "guest-failed",
  "invalid-pointer",
  "oversize-output",
  "invalid-abi",
  "forbidden-import",
];
const fixtures = Object.fromEntries(
  fixtureNames.map((name) => [
    name,
    miniflare(path.join(root, `test-dist-${name}`, "index.mjs")),
  ]),
);
const canonicalUrl = await canonical.ready;
const fixtureUrls = Object.fromEntries(
  await Promise.all(
    Object.entries(fixtures).map(async ([name, mf]) => [name, await mf.ready]),
  ),
);

const protocolHeaders = {
  "content-type": "application/pdf",
  "x-lattice-transform-id": "lattice.pdf.extract_text.v1",
  "x-lattice-transform-abi": "lattice.transform.v1",
};

function escapePdfLiteral(bytes) {
  const escaped = [];
  for (const byte of bytes) {
    if (byte === 0x28 || byte === 0x29 || byte === 0x5c) escaped.push(0x5c);
    escaped.push(byte);
  }
  return Uint8Array.from(escaped);
}

function syntheticPdf(text) {
  const encoder = new TextEncoder();
  const escaped = escapePdfLiteral(encoder.encode(text));
  const prefix = encoder.encode("BT /F1 12 Tf 72 720 Td (");
  const suffix = encoder.encode(") Tj ET");
  const stream = new Uint8Array(prefix.length + escaped.length + suffix.length);
  stream.set(prefix);
  stream.set(escaped, prefix.length);
  stream.set(suffix, prefix.length + escaped.length);
  const objects = [
    encoder.encode("<< /Type /Catalog /Pages 2 0 R >>"),
    encoder.encode("<< /Type /Pages /Kids [3 0 R] /Count 1 >>"),
    encoder.encode(
      "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
    ),
    new Uint8Array([
      ...encoder.encode(`<< /Length ${stream.length} >>\nstream\n`),
      ...stream,
      ...encoder.encode("\nendstream"),
    ]),
    encoder.encode("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"),
  ];

  const parts = [encoder.encode("%PDF-1.4\n% independent worker fixture\n")];
  const offsets = [0];
  let length = parts[0].length;
  objects.forEach((object, index) => {
    offsets.push(length);
    const part = new Uint8Array([
      ...encoder.encode(`${index + 1} 0 obj\n`),
      ...object,
      ...encoder.encode("\nendobj\n"),
    ]);
    parts.push(part);
    length += part.length;
  });
  const xref = length;
  const xrefLines = offsets
    .slice(1)
    .map((offset) => `${String(offset).padStart(10, "0")} 00000 n \n`)
    .join("");
  parts.push(
    encoder.encode(
      `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n${xrefLines}` +
        `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\n` +
        `startxref\n${xref}\n%%EOF\n`,
    ),
  );

  const pdf = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    pdf.set(part, offset);
    offset += part.length;
  }
  return pdf;
}

async function invoke(mf, url, body, headers = protocolHeaders) {
  return mf.dispatchFetch(`${url}v1/transform`, {
    method: "POST",
    headers,
    body,
  });
}

afterAll(async () => {
  await Promise.all([canonical.dispose(), ...Object.values(fixtures).map((mf) => mf.dispose())]);
});

describe("dedicated extraction Worker deployment shape", () => {
  it("has no public route or ambient bindings and declares a platform CPU policy", () => {
    expect(config.name).toBe("lattice-pdf-extract");
    expect(config.main).toMatch(/dist\/index\.mjs$/);
    expect(config.workers_dev).toBe(false);
    expect(config.preview_urls).toBe(false);
    expect(config.routes ?? []).toEqual([]);
    expect(config.kv_namespaces ?? []).toEqual([]);
    expect(config.r2_buckets ?? []).toEqual([]);
    expect(config.d1_databases ?? []).toEqual([]);
    expect(config.services ?? []).toEqual([]);
    expect(config.queues?.producers ?? []).toEqual([]);
    expect(config.queues?.consumers ?? []).toEqual([]);
    expect(config.vars ?? {}).toEqual({});
    expect(config.limits?.cpu_ms).toBe(30_000);
  });

  it("builds only the pinned W0 module and labels its digest as attestation", async () => {
    const attestation = await import(path.join(root, "dist", "attestation.mjs"));
    expect(attestation.TRANSFORM_ID).toBe("lattice.pdf.extract_text.v1");
    expect(attestation.ABI_VERSION).toBe("lattice.transform.v1");
    expect(attestation.MODULE_SHA256_ATTESTATION).toBe(
      "048f650aec8502659633289a4ace493c56a7bc6e95c8da3d4a34e293e96d4e96",
    );
  });
});

describe("bounded checked transform protocol", () => {
  it("extracts a checked PDF and returns only a bounded binary envelope", async () => {
    const response = await invoke(canonical, canonicalUrl, syntheticPdf("Worker fresh instance"));
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("application/octet-stream");
    expect(response.headers.get("x-lattice-transform-id")).toBe(
      "lattice.pdf.extract_text.v1",
    );
    expect(response.headers.get("x-lattice-transform-abi")).toBe("lattice.transform.v1");
    expect(response.headers.get("x-lattice-module-sha256-attestation")).toBe(
      "048f650aec8502659633289a4ace493c56a7bc6e95c8da3d4a34e293e96d4e96",
    );

    const envelope = new Uint8Array(await response.arrayBuffer());
    expect(envelope.length).toBeLessThanOrEqual(4 + 512 * 1024);
    expect(new DataView(envelope.buffer).getUint32(0, true)).toBe(1);
    expect(new TextDecoder().decode(envelope.subarray(4))).toContain("Worker fresh instance");
  });

  it("maps hostile parser input to one stable sanitized class", async () => {
    const fixture = await readFile(
      path.resolve(root, "..", "..", "tests", "fixtures", "qpdf-encrypted.pdf"),
    );
    const response = await invoke(canonical, canonicalUrl, fixture);
    expect(response.status).toBe(422);
    expect(response.headers.get("x-lattice-transform-id")).toBe("lattice.pdf.extract_text.v1");
    expect(response.headers.get("x-lattice-transform-abi")).toBe("lattice.transform.v1");
    expect(response.headers.get("x-lattice-module-sha256-attestation")).toBe(
      "048f650aec8502659633289a4ace493c56a7bc6e95c8da3d4a34e293e96d4e96",
    );
    expect(await response.json()).toEqual({ error: "unsupported_document" });
  });

  it("rejects protocol drift and oversized input before guest copy", async () => {
    const wrongPath = await canonical.dispatchFetch(`${canonicalUrl}other`, { method: "POST" });
    expect(wrongPath.status).toBe(404);
    expect(await wrongPath.json()).toEqual({ error: "invalid_transform" });

    const wrongMethod = await canonical.dispatchFetch(`${canonicalUrl}v1/transform`);
    expect(wrongMethod.status).toBe(405);
    expect(await wrongMethod.json()).toEqual({ error: "invalid_method" });

    const wrongMime = await invoke(canonical, canonicalUrl, new Uint8Array(), {
      ...protocolHeaders,
      "content-type": "application/octet-stream",
    });
    expect(wrongMime.status).toBe(415);
    expect(await wrongMime.json()).toEqual({ error: "unsupported_media_type" });

    const wrongId = await invoke(canonical, canonicalUrl, new Uint8Array(), {
      ...protocolHeaders,
      "x-lattice-transform-id": "request-secret-transform",
    });
    expect(wrongId.status).toBe(400);
    expect(await wrongId.json()).toEqual({ error: "invalid_transform" });

    const wrongAbi = await invoke(canonical, canonicalUrl, new Uint8Array(), {
      ...protocolHeaders,
      "x-lattice-transform-abi": "request-secret-abi",
    });
    expect(wrongAbi.status).toBe(400);
    expect(await wrongAbi.json()).toEqual({ error: "invalid_abi" });

    const oversized = await invoke(
      canonical,
      canonicalUrl,
      new Uint8Array(8 * 1024 * 1024 + 1),
    );
    expect(oversized.status).toBe(413);
    expect(await oversized.json()).toEqual({ error: "input_too_large" });

    const chunkedBody = new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array(4 * 1024 * 1024));
        controller.enqueue(new Uint8Array(4 * 1024 * 1024));
        controller.enqueue(new Uint8Array([1]));
        controller.close();
      },
    });
    const chunked = await canonical.dispatchFetch(`${canonicalUrl}v1/transform`, {
      method: "POST",
      headers: protocolHeaders,
      body: chunkedBody,
      duplex: "half",
    });
    expect(chunked.status).toBe(413);
    expect(await chunked.json()).toEqual({ error: "input_too_large" });
  });

  it("returns no request bytes or identifiers in error envelopes", async () => {
    const sentinel = "PII_SENTINEL_do_not_emit";
    const response = await invoke(canonical, canonicalUrl, new TextEncoder().encode(sentinel), {
      ...protocolHeaders,
      "x-request-id": sentinel,
    });
    const body = await response.text();
    expect(response.status).toBe(422);
    expect(body).toBe('{"error":"unsupported_document"}');
    expect(body).not.toContain(sentinel);
  });

  it("checks guest status, output bounds, pointers, imports, and ABI shape", async () => {
    const cases = [
      ["guest-failed", 422, "guest_failed"],
      ["invalid-pointer", 500, "invalid_output"],
      ["oversize-output", 502, "output_too_large"],
      ["invalid-abi", 500, "invalid_abi"],
      ["forbidden-import", 500, "invalid_module"],
    ];
    for (const [name, status, errorClass] of cases) {
      const response = await invoke(fixtures[name], fixtureUrls[name], new Uint8Array([1]));
      expect(response.status, name).toBe(status);
      expect(await response.json(), name).toEqual({ error: errorClass });
    }
  });
});

describe("isolate lifecycle", () => {
  it("admits one request per isolate without a waiter queue", async () => {
    let controller;
    const slowBody = new ReadableStream({
      start(value) {
        controller = value;
        value.enqueue(new Uint8Array([0x25]));
      },
    });
    const firstPromise = canonical.dispatchFetch(`${canonicalUrl}v1/transform`, {
      method: "POST",
      headers: protocolHeaders,
      body: slowBody,
      duplex: "half",
    });

    // Let the first handler acquire isolate-local admission and block on body I/O.
    await new Promise((resolve) => setTimeout(resolve, 25));
    const second = await invoke(canonical, canonicalUrl, syntheticPdf("must not queue"));
    expect(second.status).toBe(503);
    expect(second.headers.get("retry-after")).toBe("0");
    expect(await second.json()).toEqual({ error: "busy" });

    controller.close();
    const first = await firstPromise;
    expect(first.status).toBe(422);
  });

  it("instantiates fresh guest state for every request", async () => {
    const first = await invoke(fixtures.fresh, fixtureUrls.fresh, new Uint8Array([1]));
    const second = await invoke(fixtures.fresh, fixtureUrls.fresh, new Uint8Array([2]));
    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    expect(new Uint8Array(await first.arrayBuffer())).toEqual(
      new Uint8Array([1, 0, 0, 0, 120]),
    );
    expect(new Uint8Array(await second.arrayBuffer())).toEqual(
      new Uint8Array([1, 0, 0, 0, 120]),
    );
  });
});
