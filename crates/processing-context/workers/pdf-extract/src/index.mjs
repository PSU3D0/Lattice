import PDF_EXTRACT_MODULE from "./pdf_extract.wasm";
import {
  ABI_VERSION,
  MODULE_SHA256_ATTESTATION,
  TRANSFORM_ID,
} from "./attestation.mjs";
import { createExtractionWorker } from "./runtime.mjs";

export default createExtractionWorker({
  module: PDF_EXTRACT_MODULE,
  attestation: Object.freeze({
    transformId: TRANSFORM_ID,
    abiVersion: ABI_VERSION,
    moduleSha256: MODULE_SHA256_ATTESTATION,
  }),
});
