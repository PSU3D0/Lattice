const DNS_LABEL = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;

export function validateWorkersSubdomain(workersSubdomain) {
  if (typeof workersSubdomain !== "string" || !DNS_LABEL.test(workersSubdomain)) {
    throw new Error("workers subdomain must be one lowercase DNS label with no dots");
  }
  return workersSubdomain;
}

export function validateDnsHostname(hostname) {
  if (typeof hostname !== "string" || hostname.length > 253) {
    throw new Error("public callback hostname exceeds the DNS name limit");
  }
  const labels = hostname.split(".");
  if (labels.some((label) => !DNS_LABEL.test(label))) {
    throw new Error("public callback hostname contains an invalid DNS label");
  }
  return hostname;
}

export function validatePublicCallbackBase(prefix, workersSubdomain, publicCallbackBase) {
  validateWorkersSubdomain(workersSubdomain);
  const publicWorker = `${prefix}-broker-public`;
  const hostname = `${publicWorker}.${workersSubdomain}.workers.dev`;
  validateDnsHostname(hostname);
  const obsoleteBase = `https://${publicWorker}.workers.dev`;
  if (publicCallbackBase === obsoleteBase) {
    throw new Error("public callback base is missing the required account Workers subdomain label");
  }
  const expectedBase = `https://${hostname}`;
  if (publicCallbackBase !== expectedBase) {
    throw new Error(`public callback base must equal the exact owned origin ${expectedBase}`);
  }
  return {
    publicCallbackBase: expectedBase,
    googleOauthRedirectUri: `${expectedBase}/v0.2/credential-callback`,
  };
}

export async function verifyLiveWorkersSubdomain({
  accountId,
  workersSubdomain,
  apiToken,
  fetchImpl = globalThis.fetch,
}) {
  validateWorkersSubdomain(workersSubdomain);
  if (!apiToken) throw new Error("Workers subdomain verification requires CLOUDFLARE_API_TOKEN");
  if (typeof fetchImpl !== "function") throw new Error("Workers subdomain verification fetch is unavailable");

  let response;
  try {
    response = await fetchImpl(
      `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/subdomain`,
      { method: "GET", headers: { Authorization: `Bearer ${apiToken}` } },
    );
  } catch {
    throw new Error("Cloudflare Workers subdomain lookup failed");
  }
  if (!response?.ok) throw new Error("Cloudflare Workers subdomain lookup failed");

  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error("Cloudflare Workers subdomain lookup returned invalid JSON");
  }
  const liveSubdomain = payload?.success === true ? payload.result?.subdomain : undefined;
  if (typeof liveSubdomain !== "string" || liveSubdomain.length === 0) {
    throw new Error("Cloudflare account has no verified Workers subdomain");
  }
  if (liveSubdomain !== workersSubdomain) {
    throw new Error("supplied Workers subdomain does not match the live Cloudflare account");
  }
  return liveSubdomain;
}
