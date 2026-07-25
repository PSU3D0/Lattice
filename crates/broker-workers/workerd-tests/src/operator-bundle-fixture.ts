import { createHash, sign, type KeyObject } from "node:crypto";

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right));
    return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export function makeOperatorBundleFixture({
  protocolVectors,
  privateKey,
  keyId,
  publicKeyB64u,
  activationRecipientKeyId,
  activationRecipientPublicKeyB64u,
}: {
  protocolVectors: any;
  privateKey: KeyObject;
  keyId: string;
  publicKeyB64u: string;
  activationRecipientKeyId: string;
  activationRecipientPublicKeyB64u: string;
}): string {
  const domains: Record<string, string> = {
    StandingAuthority: "lattice.standing-authority.v0.2",
    ContractSet: "lattice.contract-set.v0.2",
    RegistryDefinition: "lattice.registry-definition.v0.2",
    RegistryDecision: "lattice.registry-decision.v0.2",
    LegacyAdmissionInventory: "lattice.legacy-admission-inventory.v0.2",
    HistoricalKeyValidityEvidence: "lattice.historical-key-validity-evidence.v0.2",
    HistoricalKeyRevocationEvidence: "lattice.historical-key-revocation-evidence.v0.2",
    HistoricalVerificationKeyArchive: "lattice.historical-verification-key-archive.v0.2",
  };
  const signValue = (schema: string, value: any) => {
    const signed = { ...value, signature: { alg: "Ed25519", key_id: keyId, value: "pending" } };
    const unsigned = Object.fromEntries(Object.entries(signed).filter(([name]) => name !== "signature"));
    signed.signature.value = sign(null, Buffer.concat([
      Buffer.from(domains[schema]), Buffer.from([0]), Buffer.from(canonicalJson(unsigned)),
    ]), privateKey).toString("base64url");
    return signed;
  };
  const source = (schema: string) => structuredClone(
    protocolVectors.signed_artifact_vectors.find((item: any) => item.artifact_schema === schema).artifact,
  );
  const archiveSource = source("HistoricalVerificationKeyArchive");
  archiveSource.validity_evidence = signValue("HistoricalKeyValidityEvidence", archiveSource.validity_evidence);
  archiveSource.revocation_evidence = signValue("HistoricalKeyRevocationEvidence", archiveSource.revocation_evidence);
  const archive = signValue("HistoricalVerificationKeyArchive", archiveSource);
  const entry = (schema: string, value: any = source(schema)) => {
    const canonical_jcs = canonicalJson(signValue(schema, value));
    return { schema, hash: `sha256:${createHash("sha256").update(canonical_jcs).digest("hex")}`, canonical_jcs };
  };
  const artifacts: Record<string, any[]> = {
    deployment_standing_authority: [entry("StandingAuthority")],
    deployment_contract_set: [entry("ContractSet")],
    registry_definitions: [entry("RegistryDefinition")],
    registry_decisions: [entry("RegistryDecision")],
    historical_inventory: [entry("LegacyAdmissionInventory")],
    historical_key_evidence: [],
  };
  const archiveJcs = canonicalJson(archive);
  artifacts.historical_key_evidence.push({
    schema: "HistoricalVerificationKeyArchive",
    hash: `sha256:${createHash("sha256").update(archiveJcs).digest("hex")}`,
    canonical_jcs: archiveJcs,
  });
  const bundle: any = {
    schema_version: "1",
    key_id: keyId,
    public_key_b64u: publicKeyB64u,
    not_before: "2026-01-01T00:00:00Z",
    expires_at: "2030-01-01T00:00:00Z",
    revoked_at: null,
    activation_recipient: {
      key_id: activationRecipientKeyId,
      public_key_b64u: activationRecipientPublicKeyB64u,
      suite: "DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM",
    },
    artifacts,
    signature: { alg: "Ed25519", key_id: keyId, value: "pending" },
  };
  const unsigned = Object.fromEntries(Object.entries(bundle).filter(([name]) => name !== "signature"));
  bundle.signature.value = sign(null, Buffer.concat([
    Buffer.from("lattice.operator-artifact-bundle.v1"), Buffer.from([0]), Buffer.from(canonicalJson(unsigned)),
  ]), privateKey).toString("base64url");
  return canonicalJson(bundle);
}
