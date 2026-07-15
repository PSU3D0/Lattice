const MAX_INPUT_BYTES = 8 * 1024 * 1024;
const MAX_OUTPUT_BYTES = 4 + 512 * 1024;
const ENDPOINT = "/v1/transform";
const EXPECTED_EXPORTS = new Map([
  ["memory", "memory"],
  ["lf_alloc", "function"],
  ["lf_transform", "function"],
  ["lf_output_ptr", "function"],
  ["lf_output_len", "function"],
  ["__data_end", "global"],
  ["__heap_base", "global"],
]);

class PublicFailure extends Error {
  constructor(status, errorClass) {
    super(errorClass);
    this.status = status;
    this.errorClass = errorClass;
  }
}

function errorResponse(status, errorClass) {
  return new Response(JSON.stringify({ error: errorClass }), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function checkedContentLength(request) {
  const raw = request.headers.get("content-length");
  if (raw === null) return;
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new PublicFailure(400, "invalid_input");
  }
  const length = Number(raw);
  if (!Number.isSafeInteger(length)) {
    throw new PublicFailure(400, "invalid_input");
  }
  if (length > MAX_INPUT_BYTES) {
    throw new PublicFailure(413, "input_too_large");
  }
}

async function readBoundedBody(request) {
  checkedContentLength(request);
  if (request.body === null) return new Uint8Array();

  const reader = request.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!(value instanceof Uint8Array)) {
        throw new PublicFailure(400, "invalid_input");
      }
      total += value.byteLength;
      if (total > MAX_INPUT_BYTES) {
        void reader.cancel().catch(() => {});
        throw new PublicFailure(413, "input_too_large");
      }
      chunks.push(value);
    }
  } catch (error) {
    if (error instanceof PublicFailure) throw error;
    throw new PublicFailure(503, "runtime_unavailable");
  } finally {
    reader.releaseLock();
  }

  const input = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    input.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return input;
}

function admitModule(module) {
  if (!(module instanceof WebAssembly.Module)) {
    throw new PublicFailure(500, "invalid_module");
  }
  if (WebAssembly.Module.imports(module).length !== 0) {
    throw new PublicFailure(500, "invalid_module");
  }
  const exports = WebAssembly.Module.exports(module);
  if (exports.length !== EXPECTED_EXPORTS.size) {
    throw new PublicFailure(500, "invalid_abi");
  }
  for (const entry of exports) {
    if (EXPECTED_EXPORTS.get(entry.name) !== entry.kind) {
      throw new PublicFailure(500, "invalid_abi");
    }
  }
}

function checkedRange(pointer, length, memoryLength) {
  return (
    Number.isInteger(pointer) &&
    Number.isInteger(length) &&
    pointer >= 0 &&
    length >= 0 &&
    pointer <= memoryLength &&
    length <= memoryLength - pointer
  );
}

function admitInstance(instance) {
  const exports = instance.exports;
  if (!(exports.memory instanceof WebAssembly.Memory)) {
    throw new PublicFailure(500, "invalid_abi");
  }
  for (const [name, arity] of [
    ["lf_alloc", 1],
    ["lf_transform", 2],
    ["lf_output_ptr", 0],
    ["lf_output_len", 0],
  ]) {
    if (typeof exports[name] !== "function" || exports[name].length !== arity) {
      throw new PublicFailure(500, "invalid_abi");
    }
  }
  return exports;
}

function execute(module, input) {
  admitModule(module);

  let instance;
  try {
    // Never cache or reuse this instance. The guest contains mutable static state.
    instance = new WebAssembly.Instance(module, {});
  } catch {
    throw new PublicFailure(500, "invalid_module");
  }
  const exports = admitInstance(instance);

  let inputPointer;
  try {
    inputPointer = exports.lf_alloc(input.byteLength);
  } catch {
    throw new PublicFailure(422, "guest_failed");
  }
  if (!checkedRange(inputPointer, input.byteLength, exports.memory.buffer.byteLength)) {
    throw new PublicFailure(500, "invalid_output");
  }
  new Uint8Array(exports.memory.buffer, inputPointer, input.byteLength).set(input);

  let status;
  try {
    status = exports.lf_transform(inputPointer, input.byteLength);
  } catch {
    throw new PublicFailure(422, "guest_failed");
  }
  if (status === 1) {
    throw new PublicFailure(422, "unsupported_document");
  }
  if (status !== 0) {
    throw new PublicFailure(422, "guest_failed");
  }

  let outputPointer;
  let outputLength;
  try {
    outputPointer = exports.lf_output_ptr();
    outputLength = exports.lf_output_len();
  } catch {
    throw new PublicFailure(500, "invalid_output");
  }
  if (!Number.isInteger(outputLength)) {
    throw new PublicFailure(500, "invalid_output");
  }
  if (outputLength > MAX_OUTPUT_BYTES) {
    throw new PublicFailure(502, "output_too_large");
  }
  if (
    outputLength < 4 ||
    !checkedRange(outputPointer, outputLength, exports.memory.buffer.byteLength)
  ) {
    throw new PublicFailure(500, "invalid_output");
  }

  // Copy before returning so no response references mutable guest memory.
  return new Uint8Array(exports.memory.buffer, outputPointer, outputLength).slice();
}

export function createExtractionWorker({ module, attestation }) {
  let active = false;

  return {
    async fetch(request) {
      const url = new URL(request.url);
      if (url.pathname !== ENDPOINT) return errorResponse(404, "invalid_transform");
      if (request.method !== "POST") return errorResponse(405, "invalid_method");
      if (request.headers.get("content-type") !== "application/pdf") {
        return errorResponse(415, "unsupported_media_type");
      }
      if (request.headers.get("x-lattice-transform-id") !== attestation.transformId) {
        return errorResponse(400, "invalid_transform");
      }
      if (request.headers.get("x-lattice-transform-abi") !== attestation.abiVersion) {
        return errorResponse(400, "invalid_abi");
      }
      if (active) {
        return new Response(JSON.stringify({ error: "busy" }), {
          status: 503,
          headers: {
            "content-type": "application/json; charset=utf-8",
            "cache-control": "no-store",
            "retry-after": "0",
          },
        });
      }

      active = true;
      try {
        const input = await readBoundedBody(request);
        const output = execute(module, input);
        return new Response(output, {
          status: 200,
          headers: {
            "content-type": "application/octet-stream",
            "cache-control": "no-store",
            "x-lattice-transform-id": attestation.transformId,
            "x-lattice-transform-abi": attestation.abiVersion,
            "x-lattice-module-sha256-attestation": attestation.moduleSha256,
          },
        });
      } catch (error) {
        if (error instanceof PublicFailure) {
          return errorResponse(error.status, error.errorClass);
        }
        return errorResponse(503, "runtime_unavailable");
      } finally {
        active = false;
      }
    },
  };
}

export const limits = Object.freeze({
  inputBytes: MAX_INPUT_BYTES,
  outputBytes: MAX_OUTPUT_BYTES,
});
