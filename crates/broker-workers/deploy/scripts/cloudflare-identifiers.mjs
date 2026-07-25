export const D1_ID_SENTINEL = "00000000-0000-0000-0000-000000000000";

const D1_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

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
