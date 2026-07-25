export const D1_ID_SENTINEL = "00000000-0000-0000-0000-000000000000";

const D1_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const ACCOUNT_ID = /^[0-9a-f]{32}$/;

export function assertAuthenticatedAccount(whoamiJson, expectedAccountId) {
  if (!ACCOUNT_ID.test(expectedAccountId ?? "")) throw new Error("expected_account_id_invalid");

  let identity;
  try {
    identity = typeof whoamiJson === "string" ? JSON.parse(whoamiJson) : whoamiJson;
  } catch {
    throw new Error("account_identity_invalid_json");
  }
  if (identity === null || typeof identity !== "object" || Array.isArray(identity)) {
    throw new Error("account_identity_unexpected_shape");
  }
  if (identity.loggedIn !== true) throw new Error("account_not_authenticated");

  const uniqueAccountIds = new Set();
  const hasLegacyAccountId = Object.hasOwn(identity, "account_id");
  if (hasLegacyAccountId) {
    if (!ACCOUNT_ID.test(identity.account_id ?? "")) throw new Error("account_identity_unexpected_shape");
    uniqueAccountIds.add(identity.account_id);
  }
  let listedAccountIds;
  if (Object.hasOwn(identity, "accounts")) {
    if (!Array.isArray(identity.accounts)) throw new Error("account_identity_unexpected_shape");
    if (identity.accounts.length === 0) throw new Error("account_accounts_empty");
    listedAccountIds = new Set();
    for (const account of identity.accounts) {
      if (account === null || typeof account !== "object" || Array.isArray(account) || !ACCOUNT_ID.test(account.id ?? "")) {
        throw new Error("account_identity_unexpected_shape");
      }
      if (listedAccountIds.has(account.id)) throw new Error("account_identity_unexpected_shape");
      listedAccountIds.add(account.id);
      uniqueAccountIds.add(account.id);
    }
  }
  if (uniqueAccountIds.size === 0) throw new Error("account_identity_unexpected_shape");
  if (hasLegacyAccountId && identity.account_id !== expectedAccountId) throw new Error("account_mismatch");
  if (listedAccountIds && !listedAccountIds.has(expectedAccountId)) throw new Error("account_mismatch");
  if (uniqueAccountIds.size > 1 && process.env.CLOUDFLARE_ACCOUNT_ID !== expectedAccountId) {
    throw new Error("multiple_accounts_require_matching_CLOUDFLARE_ACCOUNT_ID");
  }
}

export function validateD1Id(value) {
  if (typeof value !== "string" || !D1_ID.test(value)) {
    throw new Error("D1 id must be an exact canonical lowercase UUID with dashes");
  }
  return value;
}

export function renderD1DatabaseId(config, d1Id) {
  validateD1Id(d1Id);
  if (!config.includes(D1_ID_SENTINEL)) throw new Error("D1 id template sentinel is missing");
  const rendered = config.replaceAll(D1_ID_SENTINEL, d1Id);
  if (rendered.includes(D1_ID_SENTINEL)) throw new Error("rendered config retains the D1 id sentinel");
  return rendered;
}
