export const GMAIL_SCOPE = "https://www.googleapis.com/auth/gmail.send";
export const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets";
export const OPENID_SCOPE = "openid";
export const EXACT_SCOPES = Object.freeze([GMAIL_SCOPE, SHEETS_SCOPE, OPENID_SCOPE].sort());
export const TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token";
export const TOKENINFO_ENDPOINT = "https://oauth2.googleapis.com/tokeninfo";
export const REVOCATION_ENDPOINT = "https://oauth2.googleapis.com/revoke";
export const GMAIL_ORIGIN = "https://gmail.googleapis.com";
export const SHEETS_ORIGIN = "https://sheets.googleapis.com";
export const MAX_REQUEST_BYTES = 128 * 1024;
export const MAX_RESPONSE_BYTES = 64 * 1024;

export function errorResponse(code, status = 400) {
  return Response.json({ error: code }, {
    status,
    headers: { "cache-control": "no-store", "content-type": "application/json" },
  });
}

export async function boundedBytes(source, maximum) {
  const declared = Number(source.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > maximum) throw new Error("body_too_large");
  const reader = source.body?.getReader();
  if (reader === undefined) return new Uint8Array();
  const chunks = [];
  let length = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.byteLength;
    if (length > maximum) {
      await reader.cancel();
      throw new Error("body_too_large");
    }
    chunks.push(value);
  }
  const result = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

export async function boundedJson(source, maximum = MAX_REQUEST_BYTES) {
  const bytes = await boundedBytes(source, maximum);
  if (bytes.byteLength === 0) throw new Error("invalid_json");
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
}

export function exactObject(value, fields) {
  if (value === null || Array.isArray(value) || typeof value !== "object") return false;
  const actual = Object.keys(value).sort();
  const expected = [...fields].sort();
  return actual.length === expected.length && actual.every((field, index) => field === expected[index]);
}

export function validOpaque(value, minimum = 16, maximum = 128) {
  return typeof value === "string" && value.length >= minimum && value.length <= maximum &&
    /^[A-Za-z0-9._:-]+$/.test(value);
}

function textBytes(value) {
  return new TextEncoder().encode(value);
}

export async function sha256(value) {
  return [...new Uint8Array(await crypto.subtle.digest("SHA-256", value instanceof Uint8Array ? value : textBytes(value)))]
    .map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function secretMatches(request, env) {
  const supplied = request.headers.get("x-lattice-egress-auth") ?? "";
  const expected = env.GOOGLE_EGRESS_SERVICE_AUTH ?? "";
  if (supplied.length < 32 || expected.length < 32) return false;
  const [left, right] = await Promise.all([sha256(supplied), sha256(expected)]);
  let mismatch = left.length ^ right.length;
  for (let index = 0; index < Math.min(left.length, right.length); index++) {
    mismatch |= left.charCodeAt(index) ^ right.charCodeAt(index);
  }
  return mismatch === 0;
}

export function requestIdentity(request) {
  const correlationId = request.headers.get("x-lattice-correlation-id");
  const idempotencyKey = request.headers.get("x-lattice-idempotency-key");
  if (!validOpaque(correlationId) || !validOpaque(idempotencyKey)) throw new Error("request_identity_invalid");
  return { correlationId, idempotencyKey };
}

export async function upstreamFetch(env, request) {
  if (env.GOOGLE_UPSTREAM !== undefined) return env.GOOGLE_UPSTREAM.fetch(request);
  return fetch(request);
}

export function normalizeScopes(scope) {
  const values = Array.isArray(scope) ? scope : typeof scope === "string" ? scope.split(/\s+/) : [];
  const normalized = [...new Set(values.filter(Boolean))].sort();
  if (normalized.length !== EXACT_SCOPES.length || normalized.some((value, index) => value !== EXACT_SCOPES[index])) {
    throw new Error("scope_mismatch");
  }
  return normalized;
}

export function sanitizedUpstreamStatus(status) {
  if (status >= 400 && status < 500) return errorResponse("provider_rejected", 400);
  return errorResponse("provider_unavailable", 503);
}

export function copyBoundedResponse(response, bytes, correlationId) {
  const headers = new Headers({
    "cache-control": "no-store",
    "content-type": "application/json",
    "x-lattice-correlation-id": correlationId,
  });
  const requestId = response.headers.get("x-request-id");
  if (requestId !== null && /^[\x21-\x7e]{1,128}$/.test(requestId)) headers.set("x-request-id", requestId);
  return new Response(bytes, { status: response.status, headers });
}
