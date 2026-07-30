#!/usr/bin/env node
import { createHash } from "node:crypto";
import { chmod, readFile, writeFile } from "node:fs/promises";
import { isAbsolute } from "node:path";

// Mirrors the control set the broker advertises when admitting bindings; see
// crates/broker-workers/src/wasm/v2_production.rs. Keep these in lockstep.
const BROKER_KERNEL_CONTROLS = ["durable_before_dispatch", "exact_redelivery", "pop_bound"];

const PRINTABLE = /^[\x21-\x7e]{1,256}$/;
const DIGEST = /^[0-9a-f]{64}$/;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;
const PREFIX = /^lattice-(?:b5|c5)-[a-z0-9]{6,20}$/;
const SPEND = /^\d+(?:\.\d{1,2})?$/;
const RATE = /^[1-9]\d{0,5}$/;
const PENDING_SIGNATURE = Object.freeze({ alg: "Ed25519", key_id: "pending", value: "pending" });

function compareText(left, right) {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

export function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.entries(value).sort(([left], [right]) => compareText(left, right)).map(([key, child]) => `${JSON.stringify(key)}:${canonical(child)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function hash(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function unsignedHash(value) {
  const copy = structuredClone(value);
  delete copy.signature;
  return hash(canonical(copy));
}

function fail(message) {
  throw new Error(message);
}

function requirePrintable(name, value) {
  if (!PRINTABLE.test(value ?? "")) fail(`${name} must match the C1 printable identifier pattern`);
  return value;
}

function parseTimestamp(name, value) {
  if (!TIMESTAMP.test(value ?? "")) fail(`${name} must be an exact C1 UTC timestamp`);
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds) || new Date(milliseconds).toISOString().replace(".000Z", "Z") !== value) fail(`${name} is not a real UTC timestamp`);
  return milliseconds;
}

function parseArguments(argv) {
  if (argv.length % 2 !== 0) fail("arguments must be --name value pairs");
  const args = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    if (!name?.startsWith("--") || args.has(name)) fail("arguments must be unique --name value pairs");
    args.set(name, argv[index + 1]);
  }
  return args;
}

const REQUIRED_ARGS = [
  "--org-id", "--deployment-id", "--prefix", "--not-before", "--expires-at",
  "--spend-limit-usd", "--rate-limit-per-minute", "--required-assurance-predicates",
  "--activation-recipient-key-id", "--activation-recipient-public-key-b64u", "--key-id",
  "--broker-wasm-sha256", "--auth-driver-sha256", "--google-token-sha256",
  "--google-provider-sha256", "--output",
];

export function parsePredicates(exact) {
  let predicates;
  try {
    predicates = JSON.parse(exact);
  } catch {
    fail("required assurance predicates must be exact JSON");
  }
  if (!Array.isArray(predicates) || predicates.length < 1 || predicates.length > 128) fail("required assurance predicates must contain 1 through 128 entries");
  const encoded = predicates.map((predicate) => canonical(predicate));
  if (new Set(encoded).size !== encoded.length) fail("required assurance predicates must be unique");
  for (let index = 1; index < encoded.length; index += 1) {
    if (Buffer.compare(Buffer.from(encoded[index - 1]), Buffer.from(encoded[index])) >= 0) fail("required assurance predicates must be JCS-lexically sorted");
  }
  for (const predicate of predicates) {
    if (!predicate || typeof predicate !== "object" || Array.isArray(predicate) || !PRINTABLE.test(predicate.predicate_id ?? "")) fail("malformed assurance predicate");
    const keys = Object.keys(predicate).sort();
    if (predicate.kind === "brokered_count") {
      if (canonical(keys) !== canonical(["kind", "predicate_id", "required_kernel_controls"])) fail("malformed brokered_count predicate");
      const controls = predicate.required_kernel_controls;
      if (!Array.isArray(controls) || controls.length < 1 || controls.length > 32 || controls.some((value) => !PRINTABLE.test(value))) fail("malformed required kernel controls");
      if (new Set(controls).size !== controls.length || controls.some((value, index) => index > 0 && Buffer.compare(Buffer.from(controls[index - 1]), Buffer.from(value)) >= 0)) fail("required kernel controls must be unique and JCS-lexically sorted");
      // The broker admits a binding only when the standing authority's required
      // kernel controls are a SUBSET of the controls it actually implements
      // (crates/broker-workers/src/wasm/v2_production.rs). Demanding a control the
      // broker does not advertise is unsatisfiable and fails at bind time with
      // Brk108, long after deployment succeeded. Reject it here instead.
      const unsupported = controls.filter((value) => !BROKER_KERNEL_CONTROLS.includes(value));
      if (unsupported.length > 0) fail("unsupported required kernel controls: " + unsupported.join(", ") + " (broker implements: " + BROKER_KERNEL_CONTROLS.join(", ") + ")");
    } else if (predicate.kind === "semantic_policy") {
      if (canonical(keys) !== canonical(["kind", "predicate_id", "required_policy_instance_hashes"])) fail("malformed semantic_policy predicate");
      const hashes = predicate.required_policy_instance_hashes;
      if (!Array.isArray(hashes) || hashes.length < 1 || hashes.length > 128 || hashes.some((value) => !/^sha256:[0-9a-f]{64}$/.test(value))) fail("malformed required policy instance hashes");
      if (new Set(hashes).size !== hashes.length || hashes.some((value, index) => index > 0 && value <= hashes[index - 1])) fail("required policy instance hashes must be unique and sorted");
    } else {
      fail("unsupported assurance predicate kind");
    }
  }
  return predicates;
}

function validateArguments(args, now) {
  for (const name of REQUIRED_ARGS) if (!args.has(name)) fail(`missing ${name}`);
  if (args.size !== REQUIRED_ARGS.length) fail("unknown argument");
  requirePrintable("org id", args.get("--org-id"));
  requirePrintable("deployment id", args.get("--deployment-id"));
  requirePrintable("activation recipient key id", args.get("--activation-recipient-key-id"));
  requirePrintable("operator key id", args.get("--key-id"));
  if (!PREFIX.test(args.get("--prefix") ?? "")) fail("prefix is outside the disposable broker namespace");
  const notBefore = parseTimestamp("not-before", args.get("--not-before"));
  const expiresAt = parseTimestamp("expires-at", args.get("--expires-at"));
  if (notBefore > now || notBefore >= expiresAt || expiresAt <= now) fail("artifact validity window is expired, future-dated, or not increasing");
  if (!SPEND.test(args.get("--spend-limit-usd") ?? "")) fail("invalid spend limit");
  if (!RATE.test(args.get("--rate-limit-per-minute") ?? "")) fail("invalid rate limit");
  const recipient = args.get("--activation-recipient-public-key-b64u") ?? "";
  if (!/^[A-Za-z0-9_-]{43}$/.test(recipient) || Buffer.from(recipient, "base64url").length !== 32 || Buffer.from(recipient, "base64url").toString("base64url") !== recipient) fail("activation recipient public key must be canonical raw 32-byte base64url");
  for (const name of ["--broker-wasm-sha256", "--auth-driver-sha256", "--google-token-sha256", "--google-provider-sha256"]) {
    if (!DIGEST.test(args.get(name) ?? "")) fail(`${name} must be exactly 64 lowercase hexadecimal characters`);
  }
  if (!isAbsolute(args.get("--output") ?? "") || args.get("--output").includes("\0")) fail("output must be an absolute path");
  return { notBefore, expiresAt, predicates: parsePredicates(args.get("--required-assurance-predicates")) };
}

function replacePlaceholderHashes(value, replacement) {
  if (Array.isArray(value)) return value.map((item) => replacePlaceholderHashes(item, replacement));
  if (!value || typeof value !== "object") return value === `sha256:${"a".repeat(64)}` ? replacement : value;
  return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, replacePlaceholderHashes(child, replacement)]));
}

async function descriptors() {
  const paths = [
    new URL("../../../connectors/google/gmail/broker/operations/send_message.json", import.meta.url),
    new URL("../../../connectors/google/sheets/broker/operations/append_row.json", import.meta.url),
    new URL("../../../connectors/google/sheets/broker/operations/create_spreadsheet.json", import.meta.url),
  ];
  const output = [];
  for (const path of paths) {
    const descriptor = JSON.parse(await readFile(path, "utf8"));
    const actual = hash(canonical(descriptor.contract));
    if (descriptor.contract_hash !== actual) fail(`generated descriptor contract hash mismatch: ${descriptor.contract?.contract_id ?? path.pathname}`);
    output.push(descriptor);
  }
  output.sort((left, right) => compareText(left.contract.contract_id, right.contract.contract_id));
  return output;
}

function contractEntry(descriptor) {
  return {
    contract_id: descriptor.contract.contract_id,
    contract_hash: descriptor.contract_hash,
    claim_requirement_hash: hash(canonical({ auth_role: descriptor.contract.auth_role, minimum_scopes: descriptor.contract.minimum_scopes })),
    credential_response_policy: { kind: "forbidden", sensitive_json_pointers: [], sensitive_headers: [] },
  };
}

async function registryArtifacts(args, generatedDescriptors) {
  const seed = JSON.parse(await readFile(new URL("../credential-registry-seed.json", import.meta.url), "utf8"));
  const digestByClass = {
    auth_driver: `sha256:${args.get("--google-token-sha256")}`,
    claim_normalizer: `sha256:${args.get("--broker-wasm-sha256")}`,
    custodian: `sha256:${args.get("--google-token-sha256")}`,
    transport: `sha256:${args.get("--google-provider-sha256")}`,
    privileged_response_firewall: `sha256:${args.get("--broker-wasm-sha256")}`,
    capsule_planner: `sha256:${args.get("--broker-wasm-sha256")}`,
    response_projector: `sha256:${args.get("--broker-wasm-sha256")}`,
  };
  const corpusHash = hash(generatedDescriptors.map((descriptor) => canonical(descriptor)).join("\n"));
  const definitions = seed.signed_v2_entries.map(({ definition }) => {
    let value = structuredClone(definition);
    delete value.signature;
    value.publisher_ref = `${args.get("--prefix")}.operator`;
    value.publisher_key_id = args.get("--key-id");
    value.published_at = args.get("--not-before");
    value = replacePlaceholderHashes(value, corpusHash);
    if (value.class_payload.implementation_digest) value.class_payload.implementation_digest = digestByClass[value.class];
    if (["capsule_planner", "response_projector"].includes(value.class)) {
      // Each planner/projector supports exactly the operation named by its own
      // entry ref. Matching only on connector family gave every sheets entry all
      // sheets contracts, so the append_row planner claimed create_spreadsheet
      // too. Hashes are sorted because the artifact schema requires a
      // deterministic, canonically ordered array.
      const contractId = REGISTRY_ENTRY_CONTRACTS[value.entry_ref];
      if (contractId === undefined) fail("unmapped registry entry: " + value.entry_ref);
      const match = generatedDescriptors.find((descriptor) => descriptor.contract.contract_id === contractId);
      if (match === undefined) fail("registry entry " + value.entry_ref + " references unknown contract " + contractId);
      value.class_payload.supported_contract_hashes = [match.contract_hash];
    }
    return value;
  });
  const genericDriver = structuredClone(definitions.find((definition) => definition.class === "auth_driver"));
  genericDriver.entry_ref = `${args.get("--prefix")}.generic-auth-driver`;
  genericDriver.class_payload.implementation_digest = `sha256:${args.get("--auth-driver-sha256")}`;
  genericDriver.class_payload.supported_profile_refs = [{ profile_ref: `${args.get("--prefix")}.generic`, version: "1" }];
  genericDriver.class_payload.supported_scheme_refs = ["credential.generic.deployed@1"];
  definitions.push(genericDriver);

  const definitionPrehashes = new Map();
  for (const definition of definitions.filter((value) => value.class !== "auth_profile")) definitionPrehashes.set(definition.entry_ref, unsignedHash(definition));
  const profile = definitions.find((definition) => definition.class === "auth_profile");
  profile.class_payload.descriptor.trusted_auth_driver.definition_hash = definitionPrehashes.get(profile.class_payload.descriptor.trusted_auth_driver.entry_ref);
  profile.class_payload.descriptor.claim_normalizer.definition_hash = definitionPrehashes.get(profile.class_payload.descriptor.claim_normalizer.entry_ref);
  profile.class_payload.descriptor_hash = hash(canonical(profile.class_payload.descriptor));
  definitionPrehashes.set(profile.entry_ref, unsignedHash(profile));

  definitions.sort((left, right) => compareText(canonical(left), canonical(right)));
  const decisions = definitions.map((definition) => ({
    schema_version: "0.2",
    critical_fields: [],
    extensions: {},
    entry_ref: definition.entry_ref,
    version: definition.version,
    definition_hash: definitionPrehashes.get(definition.entry_ref),
    approval_status: "approved",
    approval_epoch: 1,
    revocation_status: "active",
    revocation_epoch: 0,
    authority_ref: `${args.get("--prefix")}.operator`,
    authority_key_id: args.get("--key-id"),
    policy_hash: hash(canonical({ class: definition.class, implementation_digest: definition.class_payload.implementation_digest ?? definition.class_payload.descriptor_hash })),
    not_before: args.get("--not-before"),
    expires_at: args.get("--expires-at"),
  })).sort((left, right) => compareText(canonical(left), canonical(right)));
  return { definitions, decisions, definitionPrehashes: Object.fromEntries(definitionPrehashes) };
}

// Registry entry refs do not consistently encode their operation: the Gmail
// planner is named for its adapter (rfc822_message), not its contract
// (send_message). Inferring from the ref therefore silently mis-assigns
// authority, so the mapping is explicit and unknown refs fail closed.
const REGISTRY_ENTRY_CONTRACTS = {
  "google.gmail.rfc822_message.v1": "connector.google.gmail.send_message@1",
  "projector.connector.google.gmail.send_message@1": "connector.google.gmail.send_message@1",
  "google.sheets.append_row.v1": "connector.google.sheets.append_row@1",
  "projector.connector.google.sheets.append_row@1": "connector.google.sheets.append_row@1",
  "google.sheets.create_spreadsheet.v1": "connector.google.sheets.create_spreadsheet@1",
  "projector.connector.google.sheets.create_spreadsheet@1": "connector.google.sheets.create_spreadsheet@1",
};

function descriptorFor(descriptors, digest) {
  return descriptors.find((descriptor) => descriptor.contract_hash === digest);
}

export async function generateInput(args, { now = Date.now() } = {}) {
  const { predicates } = validateArguments(args, now);
  const generatedDescriptors = await descriptors();
  const prefix = args.get("--prefix");
  const common = {
    schema_version: "0.2",
    critical_fields: [],
    extensions: {},
  };
  const contractSet = {
    ...common,
    contract_set_ref: `${prefix}.google-workspace.contracts`,
    org_id: args.get("--org-id"),
    deployment_id: args.get("--deployment-id"),
    connector_ref: "connector.google.workspace@1",
    contracts: generatedDescriptors.map(contractEntry).sort((left, right) => compareText(canonical(left), canonical(right))),
    issuer: `${prefix}.operator`,
    key_id: args.get("--key-id"),
  };
  const contractSetPrehash = unsignedHash(contractSet);
  const rate = Number(args.get("--rate-limit-per-minute"));
  const maximumBudgets = {
    logical_calls: rate,
    dispatch_attempts_per_call: 1,
    flow_logical_calls: rate,
    connection_logical_calls: rate,
    node_logical_calls: rate,
  };
  const standingAuthority = {
    ...common,
    critical_fields: [],
    extensions: { rate_limit_per_minute: rate, spend_limit_usd: args.get("--spend-limit-usd") },
    standing_authority_ref: `${prefix}.google-workspace.authority`,
    org_id: args.get("--org-id"),
    deployment_id: args.get("--deployment-id"),
    connector_ref: contractSet.connector_ref,
    contract_set_ref: contractSet.contract_set_ref,
    contract_set_hash: contractSetPrehash,
    maximum_budgets: maximumBudgets,
    required_assurance_predicates: predicates,
    operator_policy_hash: hash(canonical({ maximum_budgets: maximumBudgets, rate_limit_per_minute: rate, spend_limit_usd: args.get("--spend-limit-usd") })),
    not_before: args.get("--not-before"),
    expires_at: args.get("--expires-at"),
    issuer: `${prefix}.operator`,
    key_id: args.get("--key-id"),
  };
  const registry = await registryArtifacts(args, generatedDescriptors);
  const historicalIssuer = `${prefix}.legacy-v1-history`;
  const historicalPublicKey = args.get("--activation-recipient-public-key-b64u");
  const historicalInventory = {
    ...common,
    inventory_ref: `${prefix}.legacy-v1.empty`,
    issuer: historicalIssuer,
    key_id: args.get("--key-id"),
    created_at: args.get("--not-before"),
    expires_at: args.get("--not-before"),
    items: [],
  };
  const validityEvidence = {
    issuer: historicalIssuer,
    key_id: args.get("--key-id"),
    algorithm: "ed25519",
    public_key_encoding: "raw_base64url",
    public_key_base64url: historicalPublicKey,
    valid_from: args.get("--not-before"),
    valid_until: args.get("--expires-at"),
    observed_at: args.get("--not-before"),
    evidence_authority: `${prefix}.operator`,
    evidence_key_id: args.get("--key-id"),
  };
  const revocationEvidence = {
    status: "not_revoked_through",
    observed_through: args.get("--expires-at"),
    issuer: historicalIssuer,
    key_id: args.get("--key-id"),
    evidence_authority: `${prefix}.operator`,
    evidence_key_id: args.get("--key-id"),
  };
  const historicalArchive = {
    ...common,
    archive_ref: `${prefix}.legacy-v1.keys`,
    issuer: historicalIssuer,
    key_id: args.get("--key-id"),
    algorithm: "ed25519",
    public_key_encoding: "raw_base64url",
    public_key_base64url: historicalPublicKey,
    valid_from: args.get("--not-before"),
    valid_until: args.get("--expires-at"),
    validity_evidence: validityEvidence,
    revocation_evidence: revocationEvidence,
    archive_authority: `${prefix}.operator`,
    archive_key_id: args.get("--key-id"),
  };
  return {
    key_id: args.get("--key-id"),
    activation_recipient: {
      key_id: args.get("--activation-recipient-key-id"),
      public_key_b64u: args.get("--activation-recipient-public-key-b64u"),
      suite: "DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM",
    },
    not_before: args.get("--not-before"),
    expires_at: args.get("--expires-at"),
    derived_integrity: {
      contract_set_unsigned_hash: contractSetPrehash,
      registry_definition_unsigned_hashes: registry.definitionPrehashes,
      historical_public_key_placeholder: historicalPublicKey,
    },
    artifacts: {
      deployment_standing_authority: [{ schema: "StandingAuthority", value: { ...standingAuthority, signature: PENDING_SIGNATURE } }],
      deployment_contract_set: [{ schema: "ContractSet", value: { ...contractSet, signature: PENDING_SIGNATURE } }],
      registry_definitions: registry.definitions.map((value) => ({ schema: "RegistryDefinition", value: { ...value, signature: PENDING_SIGNATURE } })),
      registry_decisions: registry.decisions.map((value) => ({ schema: "RegistryDecision", value: { ...value, signature: PENDING_SIGNATURE } })),
      historical_inventory: [{ schema: "LegacyAdmissionInventory", value: { ...historicalInventory, signature: PENDING_SIGNATURE } }],
      historical_key_evidence: [{ schema: "HistoricalVerificationKeyArchive", value: { ...historicalArchive, signature: PENDING_SIGNATURE } }],
    },
  };
}

async function main() {
  const args = parseArguments(process.argv.slice(2));
  const output = await generateInput(args);
  const path = args.get("--output");
  await writeFile(path, `${JSON.stringify(output, null, 2)}\n`, { mode: 0o600 });
  await chmod(path, 0o600);
  console.log(JSON.stringify({ output: path, contract_set_unsigned_hash: output.derived_integrity.contract_set_unsigned_hash }));
}

if (import.meta.url === `file://${process.argv[1]}`) main().catch((error) => {
  console.error(`operator input generation failed: ${error.message}`);
  process.exitCode = 1;
});
