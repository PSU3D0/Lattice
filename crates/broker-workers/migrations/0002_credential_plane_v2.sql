CREATE TABLE IF NOT EXISTS credential_plane_schema_v2 (
  version INTEGER PRIMARY KEY CHECK (version = 2)
);
INSERT OR IGNORE INTO credential_plane_schema_v2(version) VALUES (2);

CREATE TABLE IF NOT EXISTS registry_definitions_v2 (
  org_id TEXT NOT NULL,
  entry_ref TEXT NOT NULL,
  version TEXT NOT NULL,
  registry_class TEXT NOT NULL CHECK (registry_class IN (
    'auth_profile', 'capsule_planner', 'response_projector', 'auth_driver',
    'custodian', 'transport', 'policy_evaluator', 'dynamic_source',
    'claim_normalizer', 'privileged_response_firewall', 'legacy_inventory'
  )),
  definition_hash TEXT NOT NULL,
  canonical_definition_json TEXT NOT NULL,
  PRIMARY KEY (org_id, entry_ref, version),
  UNIQUE (org_id, definition_hash)
);

CREATE TABLE IF NOT EXISTS registry_decisions_v2 (
  org_id TEXT NOT NULL,
  entry_ref TEXT NOT NULL,
  version TEXT NOT NULL,
  definition_hash TEXT NOT NULL,
  approval_epoch INTEGER NOT NULL CHECK (approval_epoch >= 0),
  revocation_epoch INTEGER NOT NULL CHECK (revocation_epoch >= 0),
  approval_status TEXT NOT NULL CHECK (approval_status IN ('approved', 'suspended', 'rejected')),
  revocation_status TEXT NOT NULL CHECK (revocation_status IN ('active', 'revoked')),
  canonical_decision_json TEXT NOT NULL,
  PRIMARY KEY (org_id, entry_ref, version),
  FOREIGN KEY (org_id, entry_ref, version)
    REFERENCES registry_definitions_v2(org_id, entry_ref, version)
);

CREATE TABLE IF NOT EXISTS auth_profiles_v2 (
  org_id TEXT NOT NULL,
  profile_ref TEXT NOT NULL,
  version TEXT NOT NULL,
  descriptor_hash TEXT NOT NULL,
  authorization_claims_vocabulary_ref TEXT NOT NULL,
  authorization_claims_schema_hash TEXT NOT NULL,
  canonical_descriptor_json TEXT NOT NULL,
  PRIMARY KEY (org_id, profile_ref, version),
  UNIQUE (org_id, descriptor_hash)
);

CREATE TABLE IF NOT EXISTS connection_authority_views_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 0),
  authority_view_hash TEXT NOT NULL,
  canonical_authority_view_json TEXT NOT NULL,
  PRIMARY KEY (org_id, connection_ref),
  UNIQUE (org_id, authority_view_hash)
);

CREATE TABLE IF NOT EXISTS material_generations_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  envelope_hash TEXT NOT NULL,
  sealed_envelope BLOB NOT NULL CHECK (length(sealed_envelope) > 0),
  material_state TEXT NOT NULL CHECK (material_state IN ('prepared', 'active', 'retiring', 'destroyed')),
  PRIMARY KEY (org_id, connection_ref, generation),
  UNIQUE (org_id, connection_ref, envelope_hash)
);

CREATE TABLE IF NOT EXISTS rotation_journal_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  rotation_ref TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN (
    'prepared', 'provider_request_recorded', 'provider_result_observed',
    'new_material_sealed', 'authority_reconciled', 'switched',
    'retirement_enqueued', 'old_material_destroyed', 'complete'
  )),
  cas_version INTEGER NOT NULL CHECK (cas_version >= 0),
  canonical_rotation_json TEXT NOT NULL,
  PRIMARY KEY (org_id, connection_ref, rotation_ref)
);

CREATE TABLE IF NOT EXISTS credential_fences_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  phase TEXT NOT NULL DEFAULT 'v1_authoritative' CHECK (phase = 'v1_authoritative'),
  fence_generation INTEGER NOT NULL CHECK (fence_generation >= 0),
  v2_lease_ever_issued INTEGER NOT NULL DEFAULT 0 CHECK (v2_lease_ever_issued = 0),
  v2_rotation_ever_started INTEGER NOT NULL DEFAULT 0 CHECK (v2_rotation_ever_started = 0),
  v1_leasing_disabled INTEGER NOT NULL DEFAULT 0 CHECK (v1_leasing_disabled = 0),
  active_v2_generation INTEGER CHECK (active_v2_generation IS NULL),
  cas_version INTEGER NOT NULL CHECK (cas_version >= 0),
  canonical_fence_json TEXT NOT NULL,
  PRIMARY KEY (org_id, connection_ref)
);

CREATE TABLE IF NOT EXISTS standing_authorities_v2 (
  org_id TEXT NOT NULL,
  standing_authority_ref TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL,
  PRIMARY KEY (org_id, standing_authority_ref),
  UNIQUE (org_id, artifact_hash)
);

CREATE TABLE IF NOT EXISTS policy_instances_v2 (
  org_id TEXT NOT NULL,
  instance_id TEXT NOT NULL,
  instance_hash TEXT NOT NULL,
  canonical_instance_json TEXT NOT NULL,
  PRIMARY KEY (org_id, instance_id),
  UNIQUE (org_id, instance_hash)
);

CREATE TABLE IF NOT EXISTS node_leases_v2 (
  org_id TEXT NOT NULL,
  node_lease_ref TEXT NOT NULL,
  lease_hash TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'prepared' CHECK (state = 'prepared'),
  canonical_lease_json TEXT NOT NULL,
  PRIMARY KEY (org_id, node_lease_ref),
  UNIQUE (org_id, lease_hash)
);

CREATE TABLE IF NOT EXISTS exact_grants_v2 (
  org_id TEXT NOT NULL,
  grant_ref TEXT NOT NULL,
  grant_hash TEXT NOT NULL,
  canonical_input_commitment_hash TEXT NOT NULL,
  logical_effect_id TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'prepared' CHECK (state = 'prepared'),
  canonical_grant_json TEXT NOT NULL,
  PRIMARY KEY (org_id, grant_ref),
  UNIQUE (org_id, grant_hash)
);

CREATE TABLE IF NOT EXISTS exact_receipts_v2 (
  org_id TEXT NOT NULL,
  receipt_ref TEXT NOT NULL,
  receipt_hash TEXT NOT NULL,
  grant_ref TEXT NOT NULL,
  dispatch_attempt INTEGER NOT NULL CHECK (dispatch_attempt BETWEEN 0 AND 255),
  canonical_receipt_json TEXT NOT NULL,
  PRIMARY KEY (org_id, receipt_ref),
  UNIQUE (org_id, receipt_hash)
);

CREATE TABLE IF NOT EXISTS legacy_admission_inventories_v2 (
  org_id TEXT NOT NULL,
  inventory_ref TEXT NOT NULL,
  inventory_hash TEXT NOT NULL,
  canonical_inventory_json TEXT NOT NULL,
  PRIMARY KEY (org_id, inventory_ref),
  UNIQUE (org_id, inventory_hash)
);

CREATE TABLE IF NOT EXISTS legacy_inventory_decisions_v2 (
  org_id TEXT NOT NULL,
  inventory_ref TEXT NOT NULL,
  inventory_hash TEXT NOT NULL,
  canonical_decision_json TEXT NOT NULL,
  PRIMARY KEY (org_id, inventory_ref)
);

CREATE TABLE IF NOT EXISTS historical_verification_keys_v2 (
  org_id TEXT NOT NULL,
  issuer TEXT NOT NULL,
  key_id TEXT NOT NULL,
  archive_hash TEXT NOT NULL,
  canonical_archive_json TEXT NOT NULL,
  PRIMARY KEY (org_id, issuer, key_id),
  UNIQUE (org_id, archive_hash)
);

CREATE INDEX IF NOT EXISTS idx_registry_decisions_v2_hash
  ON registry_decisions_v2(org_id, definition_hash);
CREATE INDEX IF NOT EXISTS idx_rotation_journal_v2_phase
  ON rotation_journal_v2(org_id, connection_ref, phase);
CREATE INDEX IF NOT EXISTS idx_exact_receipts_v2_grant
  ON exact_receipts_v2(org_id, grant_ref);
