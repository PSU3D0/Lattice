import {
  EXACT_SCOPES, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, REVOCATION_ENDPOINT, TOKEN_ENDPOINT, TOKENINFO_ENDPOINT,
  boundedBytes, boundedJson, errorResponse, exactObject, normalizeScopes, requestIdentity,
  sanitizedUpstreamStatus, secretMatches, sha256, upstreamFetch, validOpaque,
} from "./shared.mjs";

function validVerifier(value) {
  return typeof value === "string" && value.length >= 43 && value.length <= 128 && /^[A-Za-z0-9._~-]+$/.test(value);
}

function validProviderSecret(value, minimum, maximum) {
  return typeof value === "string" && value.length >= minimum && value.length <= maximum && /^[\x21-\x7e]+$/.test(value);
}

function tokenHeaders(correlationId) {
  return {
    accept: "application/json",
    "content-type": "application/x-www-form-urlencoded",
    "x-lattice-correlation-id": correlationId,
  };
}

async function tokenEndpoint(env, form, correlationId) {
  const response = await upstreamFetch(env, new Request(TOKEN_ENDPOINT, {
    method: "POST",
    headers: tokenHeaders(correlationId),
    body: form,
  }));
  const bytes = await boundedBytes(response, MAX_RESPONSE_BYTES);
  if (response.status !== 200) return { failure: sanitizedUpstreamStatus(response.status) };
  let value;
  try { value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)); }
  catch { return { failure: errorResponse("provider_response_invalid", 503) }; }
  return { value };
}

function validUnsignedInteger(value) {
  return (Number.isSafeInteger(value) && value >= 0) ||
    (typeof value === "string" && /^[0-9]{1,20}$/.test(value));
}

async function discoverSubject(env, accessToken, correlationId) {
  const url = new URL(TOKENINFO_ENDPOINT);
  url.searchParams.set("access_token", accessToken);
  const response = await upstreamFetch(env, new Request(url, {
    method: "GET",
    headers: { accept: "application/json", "x-lattice-correlation-id": correlationId },
  }));
  const bytes = await boundedBytes(response, MAX_RESPONSE_BYTES);
  if (response.status !== 200) throw new Error("account_discovery_failed");
  const value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  const allowed = ["access_type", "aud", "azp", "email", "email_verified", "exp", "expires_in", "scope", "sub", "user_id"];
  const subject = value !== null && typeof value === "object" && !Array.isArray(value) && Object.hasOwn(value, "sub")
    ? value.sub
    : value?.user_id;
  if (value === null || typeof value !== "object" || Array.isArray(value) ||
      Object.keys(value).some((key) => !allowed.includes(key)) ||
      value.aud !== env.GOOGLE_OAUTH_CLIENT_ID || !validOpaque(value.azp, 8, 512) ||
      !validUnsignedInteger(value.exp) || !validUnsignedInteger(value.expires_in) ||
      !["online", "offline"].includes(value.access_type) || !validOpaque(subject, 3, 255) ||
      (value.email !== undefined && (typeof value.email !== "string" || value.email.length < 3 || value.email.length > 320 || /[\x00-\x1f\x7f]/.test(value.email))) ||
      (value.email_verified !== undefined && !["true", "false"].includes(value.email_verified)) ||
      normalizeScopes(value.scope).join("\0") !== EXACT_SCOPES.join("\0")) {
    throw new Error("account_discovery_invalid");
  }
  return subject;
}

async function exchange(env, input, correlationId) {
  if (!exactObject(input, ["code", "code_verifier", "grant_type", "redirect_uri"]) ||
      input.grant_type !== "authorization_code" || !validProviderSecret(input.code, 1, 2048) ||
      !validVerifier(input.code_verifier) || input.redirect_uri !== env.GOOGLE_OAUTH_REDIRECT_URI) {
    return errorResponse("request_invalid");
  }
  const clientId = env.GOOGLE_OAUTH_CLIENT_ID;
  const clientSecret = env.GOOGLE_OAUTH_CLIENT_SECRET;
  if (!validOpaque(clientId, 8, 512) || typeof clientSecret !== "string" || clientSecret.length < 16) {
    return errorResponse("service_unavailable", 503);
  }
  const form = new URLSearchParams({
    grant_type: "authorization_code", code: input.code, code_verifier: input.code_verifier,
    redirect_uri: input.redirect_uri, client_id: clientId, client_secret: clientSecret,
  });
  const result = await tokenEndpoint(env, form, correlationId);
  if (result.failure !== undefined) return result.failure;
  const token = result.value;
  try {
    if (token === null || typeof token !== "object" || Array.isArray(token) ||
        typeof token.access_token !== "string" || token.access_token.length < 8 ||
        typeof token.refresh_token !== "string" || token.refresh_token.length < 8 ||
        token.token_type !== "Bearer" || !Number.isInteger(token.expires_in) || token.expires_in < 1 || token.expires_in > 86400) {
      throw new Error("token_response_invalid");
    }
    const scopes = normalizeScopes(token.scope);
    const accountSubject = await discoverSubject(env, token.access_token, correlationId);
    return Response.json({
      access_token: token.access_token,
      refresh_token: token.refresh_token,
      expires_in: token.expires_in,
      scopes,
      account_subject: accountSubject,
    }, { headers: { "cache-control": "no-store", "x-lattice-correlation-id": correlationId } });
  } catch {
    return errorResponse("provider_response_invalid", 503);
  }
}

async function refresh(env, input, correlationId) {
  if (!exactObject(input, ["grant_type", "refresh_token"]) || input.grant_type !== "refresh_token" ||
      !validProviderSecret(input.refresh_token, 8, 8192)) {
    return errorResponse("request_invalid");
  }
  const clientId = env.GOOGLE_OAUTH_CLIENT_ID;
  const clientSecret = env.GOOGLE_OAUTH_CLIENT_SECRET;
  if (!validOpaque(clientId, 8, 512) || typeof clientSecret !== "string" || clientSecret.length < 16) {
    return errorResponse("service_unavailable", 503);
  }
  const form = new URLSearchParams({
    grant_type: "refresh_token", refresh_token: input.refresh_token,
    client_id: clientId, client_secret: clientSecret,
  });
  const result = await tokenEndpoint(env, form, correlationId);
  if (result.failure !== undefined) return result.failure;
  const token = result.value;
  try {
    if (token === null || typeof token !== "object" || Array.isArray(token) ||
        typeof token.access_token !== "string" || token.access_token.length < 8 ||
        token.token_type !== "Bearer" || !Number.isInteger(token.expires_in) || token.expires_in < 1 || token.expires_in > 86400) {
      throw new Error("token_response_invalid");
    }
    const scopes = normalizeScopes(token.scope);
    return Response.json(
      { access_token: token.access_token, expires_in: token.expires_in, scopes },
      { headers: { "cache-control": "no-store", "x-lattice-correlation-id": correlationId } },
    );
  } catch {
    return errorResponse("provider_response_invalid", 503);
  }
}

async function revoke(env, input, correlationId) {
  if (!exactObject(input, ["token"]) || !validProviderSecret(input.token, 8, 8192)) {
    return errorResponse("request_invalid");
  }
  const response = await upstreamFetch(env, new Request(REVOCATION_ENDPOINT, {
    method: "POST",
    headers: tokenHeaders(correlationId),
    body: new URLSearchParams({ token: input.token }),
  }));
  await boundedBytes(response, MAX_RESPONSE_BYTES);
  if (response.status !== 200) return sanitizedUpstreamStatus(response.status);
  return Response.json({ revoked: true }, {
    headers: { "cache-control": "no-store", "x-lattice-correlation-id": correlationId },
  });
}

async function authorize(env, input, correlationId) {
  if (!exactObject(input, ["code_challenge", "redirect_uri", "response_type", "scopes", "state"]) ||
      input.response_type !== "code" || input.redirect_uri !== env.GOOGLE_OAUTH_REDIRECT_URI ||
      !validOpaque(input.state) || typeof input.code_challenge !== "string" ||
      !/^[A-Za-z0-9_-]{43}$/.test(input.code_challenge)) {
    return errorResponse("request_invalid");
  }
  let scopes;
  try { scopes = normalizeScopes(input.scopes); } catch { return errorResponse("scope_mismatch"); }
  const clientId = env.GOOGLE_OAUTH_CLIENT_ID;
  if (!validOpaque(clientId, 8, 512)) return errorResponse("service_unavailable", 503);
  const url = new URL("https://accounts.google.com/o/oauth2/v2/auth");
  url.search = new URLSearchParams({
    client_id: clientId,
    redirect_uri: input.redirect_uri,
    scope: scopes.join(" "),
    state: input.state,
    response_type: "code",
    code_challenge: input.code_challenge,
    code_challenge_method: "S256",
  }).toString();
  return Response.json(
    { authorization_url: url.toString(), exact_scopes: EXACT_SCOPES },
    { headers: { "cache-control": "no-store", "x-lattice-correlation-id": correlationId } },
  );
}

async function perform(env, request, path, correlationId) {
  if (request.method !== "POST" || request.headers.get("content-type")?.split(";", 1)[0] !== "application/json") {
    return errorResponse("request_invalid");
  }
  let input;
  try { input = await boundedJson(request); } catch { return errorResponse("request_invalid"); }
  if (path === "/authorize") return authorize(env, input, correlationId);
  if (path === "/exchange") return exchange(env, input, correlationId);
  if (path === "/refresh") return refresh(env, input, correlationId);
  if (path === "/revoke") return revoke(env, input, correlationId);
  return errorResponse("not_found", 404);
}

function hexKey(value) {
  if (!/^[0-9a-f]{64}$/.test(value ?? "")) throw new Error("result_key_invalid");
  return Uint8Array.from(value.match(/../g), (byte) => Number.parseInt(byte, 16));
}

async function seal(env, aad, bytes) {
  const key = await crypto.subtle.importKey("raw", hexKey(env.GOOGLE_TOKEN_RESULT_KEY), "AES-GCM", false, ["encrypt"]);
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce, additionalData: new TextEncoder().encode(aad) }, key, bytes));
  return { nonce: [...nonce], ciphertext: [...ciphertext] };
}

async function open(env, aad, value) {
  const key = await crypto.subtle.importKey("raw", hexKey(env.GOOGLE_TOKEN_RESULT_KEY), "AES-GCM", false, ["decrypt"]);
  return new Uint8Array(await crypto.subtle.decrypt({ name: "AES-GCM", iv: new Uint8Array(value.nonce), additionalData: new TextEncoder().encode(aad) }, key, new Uint8Array(value.ciphertext)));
}

export class GoogleTokenIdempotency {
  constructor(state, env) { this.state = state; this.env = env; }

  async fetch(request) {
    const url = new URL(request.url);
    const correlationId = request.headers.get("x-lattice-correlation-id");
    const idempotencyKey = request.headers.get("x-lattice-idempotency-key");
    if (!validOpaque(correlationId) || !validOpaque(idempotencyKey)) return errorResponse("request_identity_invalid");
    const body = await boundedBytes(request, MAX_REQUEST_BYTES);
    const requestHash = await sha256(new TextEncoder().encode(`${url.pathname}\0${new TextDecoder().decode(body)}`));
    const existing = await this.state.storage.get("result");
    if (existing !== undefined) {
      if (existing.request_hash !== requestHash) return errorResponse("idempotency_conflict", 409);
      if (existing.phase !== "complete") return errorResponse("outcome_ambiguous", 503);
      try {
        const bytes = await open(this.env, requestHash, existing.sealed);
        return new Response(bytes, { status: existing.status, headers: { "cache-control": "no-store", "content-type": "application/json", "x-lattice-correlation-id": correlationId } });
      } catch { return errorResponse("service_unavailable", 503); }
    }
    await this.state.storage.put("result", { phase: "prepared", request_hash: requestHash });
    let response;
    try {
      response = await perform(this.env, new Request(request.url, { method: "POST", headers: request.headers, body }), url.pathname, correlationId);
    } catch {
      return errorResponse("outcome_ambiguous", 503);
    }
    const responseBytes = await boundedBytes(response, MAX_RESPONSE_BYTES);
    const sealed = await seal(this.env, requestHash, responseBytes);
    await this.state.storage.put("result", { phase: "complete", request_hash: requestHash, status: response.status, sealed });
    return new Response(responseBytes, { status: response.status, headers: response.headers });
  }
}

export default {
  async fetch(request, env) {
    if (!await secretMatches(request, env)) return errorResponse("unauthorized", 401);
    let identity;
    try { identity = requestIdentity(request); } catch { return errorResponse("request_identity_invalid"); }
    const url = new URL(request.url);
    if (!["/authorize", "/exchange", "/refresh", "/revoke"].includes(url.pathname) || url.search !== "") return errorResponse("not_found", 404);
    const bytes = await boundedBytes(request, MAX_REQUEST_BYTES).catch(() => null);
    if (bytes === null) return errorResponse("request_too_large", 413);
    const id = env.GOOGLE_TOKEN_IDEMPOTENCY.idFromName(identity.idempotencyKey);
    return env.GOOGLE_TOKEN_IDEMPOTENCY.get(id).fetch(new Request(`http://token.internal${url.pathname}`, {
      method: "POST",
      headers: request.headers,
      body: bytes,
    }));
  },
};
