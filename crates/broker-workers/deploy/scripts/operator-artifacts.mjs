#!/usr/bin/env node
import { createHash, createPrivateKey, createPublicKey, sign, verify } from "node:crypto";
import { readFile, writeFile, unlink } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";

const DOMAINS = {
  RegistryDefinition: "lattice.registry-definition.v0.2",
  RegistryDecision: "lattice.registry-decision.v0.2",
  StandingAuthority: "lattice.standing-authority.v0.2",
  ContractSet: "lattice.contract-set.v0.2",
  LegacyAdmissionInventory: "lattice.legacy-admission-inventory.v0.2",
  LegacyInventoryDecision: "lattice.legacy-inventory-decision.v0.2",
  HistoricalKeyValidityEvidence: "lattice.historical-key-validity-evidence.v0.2",
  HistoricalKeyRevocationEvidence: "lattice.historical-key-revocation-evidence.v0.2",
  HistoricalVerificationKeyArchive: "lattice.historical-verification-key-archive.v0.2",
};
const BUNDLE_DOMAIN = "lattice.operator-artifact-bundle.v1";
const REQUIRED = [
  "deployment_standing_authority", "deployment_contract_set", "registry_definitions",
  "registry_decisions", "historical_inventory", "historical_key_evidence",
];

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.entries(value).sort(([left], [right]) => left.localeCompare(right)).map(([key, child]) => `${JSON.stringify(key)}:${canonical(child)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function unsigned(value) {
  const copy = structuredClone(value);
  delete copy.signature;
  return copy;
}

function preimage(domain, value) {
  return Buffer.concat([Buffer.from(domain), Buffer.from([0]), Buffer.from(canonical(unsigned(value)))]);
}

function sha(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function unsignedHash(value) {
  return sha(canonical(unsigned(value)));
}

function rawPublic(key) {
  return createPublicKey(key).export({ format: "jwk" }).x;
}

function signature(key, keyId, domain, value) {
  return { alg: "Ed25519", key_id: keyId, value: sign(null, preimage(domain, value), key).toString("base64url") };
}

function verifyOne(value, schema, key, keyId) {
  const domain = DOMAINS[schema];
  if (!domain || value?.signature?.alg !== "Ed25519" || value.signature.key_id !== keyId || !verify(null, preimage(domain, value), key, Buffer.from(value.signature.value ?? "", "base64url"))) throw new Error(`invalid ${schema} signature`);
}

function validateShape(value, schema) {
  if (!value || typeof value !== "object" || (!schema.startsWith("HistoricalKey") && value.schema_version !== "0.2")) throw new Error(`${schema} fails C1 closed shape`);
  if (!schema.startsWith("HistoricalKey") && !Array.isArray(value.critical_fields)) throw new Error(`${schema} fails C1 signed shape`);
}

function signArtifact(value, schema, key, keyId) {
  const output = structuredClone(value);
  output.signature = { alg: "Ed25519", key_id: keyId, value: "pending" };
  if (schema === "HistoricalVerificationKeyArchive") {
    for (const [name, nested] of [["validity_evidence", "HistoricalKeyValidityEvidence"], ["revocation_evidence", "HistoricalKeyRevocationEvidence"]]) {
      if (output[name]) output[name] = signArtifact(output[name], nested, key, keyId);
    }
  }
  validateShape(output, schema);
  output.signature = signature(key, keyId, DOMAINS[schema], output);
  return output;
}

function verifyArtifact(value, schema, key, keyId) {
  validateShape(value, schema);
  verifyOne(value, schema, key, keyId);
  if (schema === "HistoricalVerificationKeyArchive") {
    verifyArtifact(value.validity_evidence, "HistoricalKeyValidityEvidence", key, keyId);
    verifyArtifact(value.revocation_evidence, "HistoricalKeyRevocationEvidence", key, keyId);
  }
}

function parseArgs() {
  const output = new Map();
  for (let index = 3; index < process.argv.length; index += 2) {
    if (!process.argv[index]?.startsWith("--") || process.argv[index + 1] === undefined || output.has(process.argv[index])) throw new Error("arguments must be unique --name value pairs");
    output.set(process.argv[index], process.argv[index + 1]);
  }
  return output;
}

async function keyFromFile(path) {
  let bytes;
  if (path === "-") {
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    bytes = Buffer.concat(chunks);
  } else {
    bytes = await readFile(path);
  }
  const text = bytes.toString("utf8").trim();
  bytes.fill(0);
  if (text.includes("PRIVATE KEY")) return createPrivateKey(text);
  const seed = Buffer.from(text, "base64url");
  if (seed.length !== 32) throw new Error("operator key must be PKCS8 PEM or exact raw Ed25519 seed");
  return createPrivateKey({ key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]), format: "der", type: "pkcs8" });
}

function entry(schema, signed) {
  const canonicalJcs = canonical(signed);
  return { schema, hash: sha(canonicalJcs), canonical_jcs: canonicalJcs };
}

function single(config, group, schema) {
  const inputs = config.artifacts?.[group];
  if (!Array.isArray(inputs) || inputs.length !== 1 || inputs[0]?.schema !== schema) throw new Error(`${group} must contain exactly one ${schema}`);
  return inputs[0].value;
}

function patchHistoricalPublicKey(archive, placeholder, publicKey) {
  const output = structuredClone(archive);
  const locations = [output, output.validity_evidence];
  if (locations.some((value) => value?.public_key_base64url !== placeholder)) throw new Error("historical public key derivation mismatch");
  for (const value of locations) value.public_key_base64url = publicKey;
  return output;
}

function signDerivedArtifacts(config, key, keyId) {
  const integrity = config.derived_integrity;
  if (!integrity || typeof integrity !== "object") throw new Error("generated input is missing derived integrity pins");

  const contractValue = single(config, "deployment_contract_set", "ContractSet");
  const contractPrehash = unsignedHash(contractValue);
  if (integrity.contract_set_unsigned_hash !== contractPrehash || contractValue.key_id !== keyId) throw new Error("contract set derived hash mismatch");
  const signedContract = signArtifact(contractValue, "ContractSet", key, keyId);
  const contractEntry = entry("ContractSet", signedContract);

  const standingValue = single(config, "deployment_standing_authority", "StandingAuthority");
  if (standingValue.contract_set_hash !== contractPrehash || standingValue.contract_set_ref !== contractValue.contract_set_ref || standingValue.key_id !== keyId || standingValue.org_id !== contractValue.org_id || standingValue.deployment_id !== contractValue.deployment_id || standingValue.connector_ref !== contractValue.connector_ref) throw new Error("standing authority contract_set_hash mismatch");
  const finalStanding = structuredClone(standingValue);
  finalStanding.contract_set_hash = contractEntry.hash;
  const signedStanding = signArtifact(finalStanding, "StandingAuthority", key, keyId);

  const definitions = config.artifacts?.registry_definitions;
  const expectedPrehashes = integrity.registry_definition_unsigned_hashes;
  if (!Array.isArray(definitions) || definitions.length === 0 || !expectedPrehashes || typeof expectedPrehashes !== "object") throw new Error("registry definitions are missing");
  const definitionByRef = new Map();
  for (const item of definitions) {
    if (item?.schema !== "RegistryDefinition" || typeof item.value?.entry_ref !== "string" || item.value.publisher_key_id !== keyId || definitionByRef.has(item.value.entry_ref)) throw new Error("invalid or duplicate registry definition");
    const actual = unsignedHash(item.value);
    if (expectedPrehashes[item.value.entry_ref] !== actual) throw new Error(`registry definition derived hash mismatch: ${item.value.entry_ref}`);
    definitionByRef.set(item.value.entry_ref, item.value);
  }

  const signedDefinitionEntries = [];
  const finalDefinitionHashes = new Map();
  const signDefinition = (value) => {
    const signed = signArtifact(value, "RegistryDefinition", key, keyId);
    const artifactEntry = entry("RegistryDefinition", signed);
    signedDefinitionEntries.push(artifactEntry);
    finalDefinitionHashes.set(value.entry_ref, artifactEntry.hash);
  };
  for (const value of definitionByRef.values()) if (value.class !== "auth_profile") signDefinition(value);
  for (const source of definitionByRef.values()) {
    if (source.class !== "auth_profile") continue;
    const value = structuredClone(source);
    const descriptor = value.class_payload?.descriptor;
    for (const name of ["trusted_auth_driver", "claim_normalizer"]) {
      const pin = descriptor?.[name];
      if (!pin || pin.definition_hash !== expectedPrehashes[pin.entry_ref] || !finalDefinitionHashes.has(pin.entry_ref)) throw new Error(`auth profile ${name} derivation mismatch`);
      pin.definition_hash = finalDefinitionHashes.get(pin.entry_ref);
    }
    value.class_payload.descriptor_hash = sha(canonical(descriptor));
    signDefinition(value);
  }
  signedDefinitionEntries.sort((left, right) => left.canonical_jcs.localeCompare(right.canonical_jcs));

  const decisions = config.artifacts?.registry_decisions;
  if (!Array.isArray(decisions) || decisions.length !== definitions.length) throw new Error("registry decision count mismatch");
  const signedDecisionEntries = decisions.map((item) => {
    if (item?.schema !== "RegistryDecision") throw new Error("invalid registry decision schema");
    const value = structuredClone(item.value);
    if (value.authority_key_id !== keyId || value.definition_hash !== expectedPrehashes[value.entry_ref] || !finalDefinitionHashes.has(value.entry_ref)) throw new Error(`registry decision definition_hash mismatch: ${value.entry_ref ?? "unknown"}`);
    value.definition_hash = finalDefinitionHashes.get(value.entry_ref);
    return entry("RegistryDecision", signArtifact(value, "RegistryDecision", key, keyId));
  }).sort((left, right) => left.canonical_jcs.localeCompare(right.canonical_jcs));

  const inventoryValue = single(config, "historical_inventory", "LegacyAdmissionInventory");
  if (inventoryValue.key_id !== keyId || !Array.isArray(inventoryValue.items) || inventoryValue.items.length !== 0 || inventoryValue.expires_at !== inventoryValue.created_at) throw new Error("legacy V1 inventory must be empty and non-executable");
  const archiveValue = single(config, "historical_key_evidence", "HistoricalVerificationKeyArchive");
  if (archiveValue.key_id !== keyId || archiveValue.archive_key_id !== keyId || archiveValue.validity_evidence?.evidence_key_id !== keyId || archiveValue.revocation_evidence?.evidence_key_id !== keyId) throw new Error("historical key evidence authority mismatch");
  const finalArchive = patchHistoricalPublicKey(archiveValue, integrity.historical_public_key_placeholder, rawPublic(key));

  return {
    deployment_standing_authority: [entry("StandingAuthority", signedStanding)],
    deployment_contract_set: [contractEntry],
    registry_definitions: signedDefinitionEntries,
    registry_decisions: signedDecisionEntries,
    historical_inventory: [entry("LegacyAdmissionInventory", signArtifact(inventoryValue, "LegacyAdmissionInventory", key, keyId))],
    historical_key_evidence: [entry("HistoricalVerificationKeyArchive", signArtifact(finalArchive, "HistoricalVerificationKeyArchive", key, keyId))],
  };
}

export function verifyBundle(bundle, trustRoot, now = Math.floor(Date.now() / 1000)) {
  if (canonical(bundle) !== JSON.stringify(bundle)) throw new Error("bundle is not canonical JCS");
  if (bundle.schema_version !== "1" || bundle.key_id !== trustRoot.key_id || bundle.public_key_b64u !== trustRoot.public_key_b64u || bundle.revoked_at !== null || bundle.activation_recipient?.suite !== "DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM" || Buffer.from(bundle.activation_recipient?.public_key_b64u ?? "", "base64url").length !== 32) throw new Error("bundle trust root mismatch or revoked");
  const from = Date.parse(bundle.not_before) / 1000;
  const until = Date.parse(bundle.expires_at) / 1000;
  if (!Number.isFinite(from) || !Number.isFinite(until) || now < from || now >= until) throw new Error("bundle is outside validity window");
  const key = createPublicKey({ key: { kty: "OKP", crv: "Ed25519", x: trustRoot.public_key_b64u }, format: "jwk" });
  if (bundle.signature?.key_id !== bundle.key_id || !verify(null, preimage(BUNDLE_DOMAIN, bundle), key, Buffer.from(bundle.signature?.value ?? "", "base64url"))) throw new Error("invalid bundle signature");
  for (const name of REQUIRED) if (!Array.isArray(bundle.artifacts?.[name]) || bundle.artifacts[name].length === 0) throw new Error(`missing ${name}`);
  for (const entries of Object.values(bundle.artifacts)) {
    for (const artifactEntry of entries) {
      if (artifactEntry.hash !== sha(artifactEntry.canonical_jcs) || canonical(JSON.parse(artifactEntry.canonical_jcs)) !== artifactEntry.canonical_jcs) throw new Error("artifact hash/canonical mismatch");
      verifyArtifact(JSON.parse(artifactEntry.canonical_jcs), artifactEntry.schema, key, bundle.key_id);
    }
  }
  const contractHash = bundle.artifacts.deployment_contract_set[0].hash;
  const standing = JSON.parse(bundle.artifacts.deployment_standing_authority[0].canonical_jcs);
  if (standing.contract_set_hash !== contractHash) throw new Error("standing authority does not pin the emitted contract set");
  return true;
}

function rustVerify(bundlePath, trustPath) {
  const root = resolve(new URL("../..", import.meta.url).pathname);
  const result = spawnSync("cargo", ["run", "--quiet", "--bin", "broker-artifact-verifier", "--", "--bundle", bundlePath, "--trust-root", trustPath, "--now", new Date().toISOString().replace(/\.\d{3}Z$/, "Z")], { cwd: root, encoding: "utf8" });
  if (result.status !== 0) throw new Error(result.stderr.trim() || "Rust C1 verifier rejected bundle");
  return result.stdout.trim();
}

async function main() {
  const command = process.argv[2];
  const args = parseArgs();
  if (command === "build") {
    const config = JSON.parse(await readFile(args.get("--config"), "utf8"));
    const key = await keyFromFile(args.get("--key-file"));
    const keyId = config.key_id;
    if (!keyId || !args.get("--output")) throw new Error("build requires key_id and output");
    const artifacts = signDerivedArtifacts(config, key, keyId);
    const bundle = {
      schema_version: "1",
      key_id: keyId,
      public_key_b64u: rawPublic(key),
      not_before: config.not_before,
      expires_at: config.expires_at,
      revoked_at: null,
      activation_recipient: config.activation_recipient,
      artifacts,
      signature: { alg: "Ed25519", key_id: keyId, value: "pending" },
    };
    bundle.signature = signature(key, keyId, BUNDLE_DOMAIN, bundle);
    const exact = canonical(bundle);
    verifyBundle(JSON.parse(exact), { key_id: keyId, public_key_b64u: bundle.public_key_b64u });
    await writeFile(args.get("--output"), exact, { mode: 0o600 });
    const trustPath = `${args.get("--output")}.trust.tmp`;
    await writeFile(trustPath, JSON.stringify({ key_id: keyId, public_key_b64u: bundle.public_key_b64u }), { mode: 0o600 });
    try {
      rustVerify(args.get("--output"), trustPath);
    } finally {
      await unlink(trustPath).catch(() => {});
    }
    console.log(JSON.stringify({ bundle_hash: sha(exact), key_id: keyId, output: args.get("--output") }));
    return;
  }
  if (command === "verify") {
    const exact = (await readFile(args.get("--bundle"), "utf8")).trim();
    const trust = JSON.parse(await readFile(args.get("--trust-root"), "utf8"));
    const bundle = JSON.parse(exact);
    verifyBundle(bundle, trust);
    rustVerify(args.get("--bundle"), args.get("--trust-root"));
    console.log(JSON.stringify({ bundle_hash: sha(exact), key_id: bundle.key_id }));
    return;
  }
  throw new Error("usage: operator-artifacts.mjs build|verify");
}

if (import.meta.url === `file://${process.argv[1]}`) main().catch((error) => {
  console.error(`operator artifact verification failed: ${error.message}`);
  process.exitCode = 1;
});
