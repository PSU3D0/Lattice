import { createHash } from "node:crypto";
import { cp, mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import wabtFactory from "wabt";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const wabt = await wabtFactory();

async function writeFixture(name, wat) {
  const parsed = wabt.parseWat(`${name}.wat`, wat);
  const { buffer } = parsed.toBinary({ canonicalize_lebs: true, write_debug_names: false });
  parsed.destroy();
  const moduleBytes = Buffer.from(buffer);
  const sha256 = createHash("sha256").update(moduleBytes).digest("hex");
  const dist = path.join(root, `test-dist-${name}`);

  await rm(dist, { recursive: true, force: true });
  await mkdir(dist, { recursive: true });
  await Promise.all([
    cp(path.join(root, "src", "index.mjs"), path.join(dist, "index.mjs")),
    cp(path.join(root, "src", "runtime.mjs"), path.join(dist, "runtime.mjs")),
    writeFile(path.join(dist, "pdf_extract.wasm"), moduleBytes),
  ]);
  await writeFile(
    path.join(dist, "attestation.mjs"),
    [
      'export const TRANSFORM_ID = "lattice.pdf.extract_text.v1";',
      'export const ABI_VERSION = "lattice.transform.v1";',
      `export const MODULE_SHA256_ATTESTATION = ${JSON.stringify(sha256)};`,
      "",
    ].join("\n"),
  );
}

const exactPrefix = String.raw`
  (memory (export "memory") 1 1024)
  (global (export "__data_end") i32 (i32.const 69))
  (global (export "__heap_base") i32 (i32.const 128))
  (data (i32.const 64) "\01\00\00\00x")
  (func (export "lf_alloc") (param i32) (result i32) (i32.const 0))`;
const exactSuffix = String.raw`
  (func (export "lf_output_ptr") (result i32) (i32.const 64))
  (func (export "lf_output_len") (result i32) (i32.const 5))`;

await Promise.all([
  writeFixture(
    "fresh",
    String.raw`(module
      (memory (export "memory") 1 1024)
      (global $calls (mut i32) (i32.const 0))
      (global (export "__data_end") i32 (i32.const 69))
      (global (export "__heap_base") i32 (i32.const 128))
      (data (i32.const 64) "\01\00\00\00x")
      (func (export "lf_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "lf_transform") (param i32 i32) (result i32)
        (global.set $calls (i32.add (global.get $calls) (i32.const 1)))
        (i32.gt_s (global.get $calls) (i32.const 1)))
      (func (export "lf_output_ptr") (result i32) (i32.const 64))
      (func (export "lf_output_len") (result i32) (i32.const 5)))`,
  ),
  writeFixture(
    "guest-failed",
    `(module ${exactPrefix}
      (func (export "lf_transform") (param i32 i32) (result i32) (i32.const 99))
      ${exactSuffix})`,
  ),
  writeFixture(
    "invalid-pointer",
    `(module ${exactPrefix}
      (func (export "lf_transform") (param i32 i32) (result i32) (i32.const 0))
      (func (export "lf_output_ptr") (result i32) (i32.const 65535))
      (func (export "lf_output_len") (result i32) (i32.const 5)))`,
  ),
  writeFixture(
    "oversize-output",
    String.raw`(module
      (memory (export "memory") 9 1024)
      (global (export "__data_end") i32 (i32.const 0))
      (global (export "__heap_base") i32 (i32.const 0))
      (func (export "lf_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "lf_transform") (param i32 i32) (result i32) (i32.const 0))
      (func (export "lf_output_ptr") (result i32) (i32.const 0))
      (func (export "lf_output_len") (result i32) (i32.const 524293)))`,
  ),
  writeFixture(
    "invalid-abi",
    String.raw`(module
      (memory (export "memory") 1 1024)
      (global (export "__data_end") i32 (i32.const 0))
      (func (export "lf_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "lf_transform") (param i32 i32) (result i32) (i32.const 0))
      (func (export "lf_output_ptr") (result i32) (i32.const 0))
      (func (export "lf_output_len") (result i32) (i32.const 4)))`,
  ),
  writeFixture(
    "forbidden-import",
    String.raw`(module
      (import "ambient" "forbidden" (func))
      (memory (export "memory") 1 1024)
      (global (export "__data_end") i32 (i32.const 0))
      (global (export "__heap_base") i32 (i32.const 0))
      (func (export "lf_alloc") (param i32) (result i32) (i32.const 0))
      (func (export "lf_transform") (param i32 i32) (result i32) (i32.const 0))
      (func (export "lf_output_ptr") (result i32) (i32.const 0))
      (func (export "lf_output_len") (result i32) (i32.const 4)))`,
  ),
]);
