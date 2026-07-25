import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

const DIGEST_BINDINGS = {
  REPLACE_WITH_AUTH_DRIVER_SOURCE_SHA256: "AUTH_DRIVER_SERVICE",
  REPLACE_WITH_GOOGLE_TOKEN_SOURCE_SHA256: "GOOGLE_TOKEN_SERVICE",
  REPLACE_WITH_GOOGLE_PROVIDER_SOURCE_SHA256: "GOOGLE_PROVIDER_SERVICE",
};

export function renderWorkerUploadedSourceDigests(config, services) {
  let rendered = config;
  for (const [placeholder, binding] of Object.entries(DIGEST_BINDINGS)) {
    const digest = services?.[binding]?.uploaded_source_sha256;
    if (!/^[0-9a-f]{64}$/.test(digest ?? "")) {
      throw new Error(`approved dependency uploaded source digest invalid:${binding}`);
    }
    rendered = rendered.replaceAll(placeholder, `sha256:${digest}`);
  }
  return rendered;
}

export async function computeUploadedSourceSha256(files) {
  if (!Array.isArray(files) || files.length === 0) throw new Error("uploaded source files required");
  const records = [];
  for (const entry of [...files].sort((left, right) => left.path.localeCompare(right.path))) {
    const bytes = await readFile(entry.file);
    records.push({
      path: entry.path,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      sizeBytes: bytes.length,
    });
  }
  const digest = createHash("sha256");
  for (const record of records) digest.update(`${record.path}\0${record.sha256}\0${record.sizeBytes}\n`);
  return digest.digest("hex");
}
