import {
  GMAIL_ORIGIN, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, SHEETS_ORIGIN,
  boundedBytes, errorResponse, exactObject, requestIdentity, secretMatches, sha256,
  upstreamFetch, validOpaque,
} from "./shared.mjs";

const GMAIL_PATH = "/gmail/v1/users/me/messages/send";
const SHEETS_PATH = /^\/v4\/spreadsheets\/([^/]{1,768})\/values\/([^/]{1,3072}):append$/;

function exactQuery(url, kind) {
  if (kind === "gmail") return url.search === "";
  const entries = [...url.searchParams.entries()];
  if (entries.length !== 2 || new Set(entries.map(([name]) => name)).size !== 2) return false;
  return url.searchParams.get("insertDataOption") === "INSERT_ROWS" &&
    ["RAW", "USER_ENTERED"].includes(url.searchParams.get("valueInputOption"));
}

function classify(url) {
  if (url.pathname === GMAIL_PATH && exactQuery(url, "gmail")) {
    return { kind: "gmail", upstream: `${GMAIL_ORIGIN}${GMAIL_PATH}` };
  }
  const match = url.pathname.match(SHEETS_PATH);
  let decodedSheetTarget;
  try {
    decodedSheetTarget = match === null ? null : [decodeURIComponent(match[1]), decodeURIComponent(match[2])];
  } catch { decodedSheetTarget = null; }
  if (match !== null && decodedSheetTarget !== null &&
      /^[A-Za-z0-9_-]{1,256}$/.test(decodedSheetTarget[0]) &&
      decodedSheetTarget[1].length >= 1 && decodedSheetTarget[1].length <= 1024 &&
      !decodedSheetTarget[1].includes("/") && exactQuery(url, "sheets")) {
    const query = new URLSearchParams({
      insertDataOption: "INSERT_ROWS",
      valueInputOption: url.searchParams.get("valueInputOption"),
    });
    return { kind: "sheets", upstream: `${SHEETS_ORIGIN}${url.pathname}?${query}` };
  }
  return null;
}

function validAuthorization(value) {
  return typeof value === "string" && value.startsWith("Bearer ") && value.length >= 16 && value.length <= 8192 &&
    !/[\r\n]/.test(value);
}

function validateBody(kind, value, encodedLength) {
  if (kind === "gmail") {
    return exactObject(value, ["raw"]) && typeof value.raw === "string" && value.raw.length >= 1 &&
      value.raw.length <= 120 * 1024 && /^[A-Za-z0-9_-]+$/.test(value.raw);
  }
  return exactObject(value, ["values"]) && Array.isArray(value.values) && value.values.length === 1 &&
    Array.isArray(value.values[0]) && value.values[0].length >= 1 && value.values[0].length <= 256 && encodedLength <= MAX_REQUEST_BYTES;
}

async function perform(env, request, target, correlationId, body) {
  const authorization = request.headers.get("authorization");
  if (request.method !== "POST" || !validAuthorization(authorization) ||
      request.headers.get("accept") !== "application/json" ||
      request.headers.get("content-type")?.split(";", 1)[0] !== "application/json") {
    return errorResponse("request_invalid");
  }
  let value;
  try { value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body)); }
  catch { return errorResponse("request_invalid"); }
  if (!validateBody(target.kind, value, body.byteLength)) return errorResponse("request_invalid");
  let response;
  try {
    response = await upstreamFetch(env, new Request(target.upstream, {
      method: "POST",
      headers: {
        accept: "application/json",
        authorization,
        "content-type": "application/json",
        "x-lattice-correlation-id": correlationId,
      },
      body,
    }));
  } catch { return errorResponse("provider_unavailable", 503); }
  let responseBytes;
  try { responseBytes = await boundedBytes(response, MAX_RESPONSE_BYTES); }
  catch { return errorResponse("provider_response_too_large", 503); }
  if (response.status < 200 || response.status >= 300) {
    return errorResponse(response.status >= 400 && response.status < 500 ? "provider_rejected" : "provider_unavailable", response.status >= 500 ? 503 : 400);
  }
  let providerValue;
  try { providerValue = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(responseBytes)); }
  catch { return errorResponse("provider_response_invalid", 503); }
  let projection;
  if (target.kind === "gmail") {
    if (!exactObject(providerValue, ["id", "threadId"]) || !validOpaque(providerValue.id, 1, 256) || !validOpaque(providerValue.threadId, 1, 256)) return errorResponse("provider_response_invalid", 503);
    projection = { id: providerValue.id, thread_id: providerValue.threadId };
  } else {
    const updates = providerValue?.updates;
    if (updates === null || typeof updates !== "object" || Array.isArray(updates) ||
        !Number.isInteger(updates.updatedCells) || !Number.isInteger(updates.updatedColumns) ||
        !Number.isInteger(updates.updatedRows) || typeof updates.updatedRange !== "string" || updates.updatedRange.length > 1024) {
      return errorResponse("provider_response_invalid", 503);
    }
    projection = { updated_cells: updates.updatedCells, updated_columns: updates.updatedColumns, updated_range: updates.updatedRange, updated_rows: updates.updatedRows };
  }
  responseBytes = new TextEncoder().encode(JSON.stringify(projection));
  const headers = new Headers({
    "cache-control": "no-store", "content-type": "application/json", "x-lattice-correlation-id": correlationId,
  });
  const requestId = response.headers.get("x-request-id");
  if (requestId !== null && /^[\x21-\x7e]{1,128}$/.test(requestId)) headers.set("x-request-id", requestId);
  return new Response(responseBytes, { status: response.status, headers });
}

export class GoogleProviderIdempotency {
  constructor(state, env) { this.state = state; this.env = env; }

  async fetch(request) {
    const url = new URL(request.url);
    const target = classify(url);
    const correlationId = request.headers.get("x-lattice-correlation-id");
    const idempotencyKey = request.headers.get("x-lattice-idempotency-key");
    if (target === null || !validOpaque(correlationId) || !validOpaque(idempotencyKey)) return errorResponse("request_invalid");
    const body = await boundedBytes(request, MAX_REQUEST_BYTES);
    const requestHash = await sha256(new TextEncoder().encode(`${request.method}\0${url.pathname}${url.search}\0${new TextDecoder().decode(body)}`));
    const existing = await this.state.storage.get("result");
    if (existing !== undefined) {
      if (existing.request_hash !== requestHash) return errorResponse("idempotency_conflict", 409);
      if (existing.phase !== "complete") return errorResponse("outcome_ambiguous", 503);
      return new Response(new Uint8Array(existing.body), {
        status: existing.status,
        headers: { "cache-control": "no-store", "content-type": "application/json", "x-lattice-correlation-id": correlationId, "x-lattice-remote-dispatch-proof": existing.durable_proof, ...(existing.request_id === null ? {} : { "x-request-id": existing.request_id }) },
      });
    }
    await this.state.storage.put("result", { phase: "prepared", request_hash: requestHash });
    const response = await perform(this.env, request, target, correlationId, body);
    const responseBody = await boundedBytes(response, MAX_RESPONSE_BYTES);
    const requestId = response.headers.get("x-request-id");
    const durableProof = `sha256:${await sha256(new TextEncoder().encode(`lattice.google-egress.durable.v1\0${requestHash}\0${response.status}\0${await sha256(responseBody)}`))}`;
    await this.state.storage.put("result", {
      phase: "complete", request_hash: requestHash, status: response.status,
      body: [...responseBody], request_id: requestId, durable_proof: durableProof,
    });
    const responseHeaders = new Headers(response.headers);
    responseHeaders.set("x-lattice-remote-dispatch-proof", durableProof);
    return new Response(responseBody, { status: response.status, headers: responseHeaders });
  }
}

export default {
  async fetch(request, env) {
    if (!await secretMatches(request, env)) return errorResponse("unauthorized", 401);
    let identity;
    try { identity = requestIdentity(request); } catch { return errorResponse("request_identity_invalid"); }
    const url = new URL(request.url);
    if (request.method !== "POST") return errorResponse("request_invalid");
    if (classify(url) === null) return errorResponse("not_found", 404);
    const body = await boundedBytes(request, MAX_REQUEST_BYTES).catch(() => null);
    if (body === null) return errorResponse("request_too_large", 413);
    const id = env.GOOGLE_PROVIDER_IDEMPOTENCY.idFromName(identity.idempotencyKey);
    return env.GOOGLE_PROVIDER_IDEMPOTENCY.get(id).fetch(new Request(`http://provider.internal${url.pathname}${url.search}`, {
      method: request.method,
      headers: request.headers,
      body,
    }));
  },
};
