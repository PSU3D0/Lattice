import test from "node:test";
import assert from "node:assert/strict";
import { verifyBundle } from "./operator-artifacts.mjs";

test("operator bundle verifier fails closed before accepting incomplete input", () => {
  const trust = { key_id: "operator-test", public_key_b64u: Buffer.alloc(32, 7).toString("base64url") };
  assert.throws(() => verifyBundle({}, trust), /canonical|trust root/);
  assert.throws(() => verifyBundle({ schema_version: "1", key_id: trust.key_id, public_key_b64u: trust.public_key_b64u }, trust), /canonical|trust root/);
});
