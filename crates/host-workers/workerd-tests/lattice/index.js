export function lf_cap_call(op, in_ptr, in_len, out_ptr, out_cap) {
  const host = globalThis.lattice?.lf_cap_call ?? globalThis.lf_cap_call;
  if (typeof host !== "function") {
    throw new Error("lattice host bridge is unavailable");
  }
  return host(op, in_ptr, in_len, out_ptr, out_cap);
}
