import extractionWorker from "../extraction-worker/index.mjs";

let blocked = false;
let nextMode;

const identityHeaders = {
  "content-type": "application/json; charset=utf-8",
  "x-lattice-transform-id": "lattice.pdf.extract_text.v1",
  "x-lattice-transform-abi": "lattice.transform.v1",
  "x-lattice-module-sha256-attestation":
    "048f650aec8502659633289a4ace493c56a7bc6e95c8da3d4a34e293e96d4e96",
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/__test/block") {
      blocked = true;
      return Response.json({ blocked: true });
    }
    if (url.pathname === "/__test/release") {
      blocked = false;
      return Response.json({ released: true });
    }
    if (url.pathname === "/__test/mode") {
      nextMode = url.searchParams.get("value");
      return Response.json({ mode: nextMode });
    }
    while (blocked) {
      await scheduler.wait(10);
    }
    const mode = nextMode;
    nextMode = undefined;
    if (mode === "wrong-attestation") {
      return new Response('{"error":"unsupported_document"}', {
        status: 422,
        headers: { ...identityHeaders, "x-lattice-module-sha256-attestation": "00".repeat(32) },
      });
    }
    if (mode === "platform-terminated") {
      return new Response('{"error":"platform_terminated"}', {
        status: 503,
        headers: identityHeaders,
      });
    }
    return extractionWorker.fetch(request, env, ctx);
  },
};
