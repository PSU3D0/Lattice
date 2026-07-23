CREATE TABLE broker_schema_v2_cutover (
  version INTEGER PRIMARY KEY CHECK (version = 3),
  minimum_worker_protocol TEXT NOT NULL CHECK (minimum_worker_protocol = '0.2'),
  v1_executable_admission INTEGER NOT NULL CHECK (v1_executable_admission = 0)
);
INSERT INTO broker_schema_v2_cutover VALUES (3, '0.2', 0);

ALTER TABLE connection_intents ADD COLUMN activation_deployment_id TEXT;

ALTER TABLE connections RENAME TO connections_v1_quarantine;
ALTER TABLE bindings RENAME TO bindings_v1_quarantine;
ALTER TABLE grants RENAME TO grants_v1_quarantine;
ALTER TABLE receipts RENAME TO receipts_v1_history;

CREATE TABLE connections_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  profile_ref TEXT NOT NULL,
  profile_version TEXT NOT NULL,
  profile_descriptor_hash TEXT,
  authority_view_hash TEXT,
  canonical_authority_view_json TEXT,
  standing_authority_ref TEXT,
  standing_authority_hash TEXT,
  canonical_standing_authority_json TEXT,
  contract_set_ref TEXT,
  contract_set_hash TEXT,
  canonical_contract_set_json TEXT,
  registry_decision_set_hash TEXT,
  account_subject_commitment TEXT,
  material_do_route TEXT,
  material_mode TEXT NOT NULL DEFAULT 'local_sealed' CHECK (material_mode IN ('local_sealed','remote_external')),
  active_material_generation INTEGER CHECK (active_material_generation > 0),
  fence_generation INTEGER NOT NULL DEFAULT 0 CHECK (fence_generation >= 0),
  revocation_epoch INTEGER NOT NULL DEFAULT 0 CHECK (revocation_epoch >= 0),
  status TEXT NOT NULL CHECK (status IN ('reconciling', 'active', 'revoking', 'cleanup_pending', 'revoked', 'blocked')),
  PRIMARY KEY (org_id, connection_ref)
);

INSERT INTO connections_v2 (
  org_id, connection_ref, profile_ref, profile_version, fence_generation,
  revocation_epoch, status
)
SELECT
  org_id, connection_ref,
  CASE
    WHEN auth_profile_ref = 'auth.google.workspace.oauth2@1' THEN 'auth.google.workspace.oauth2'
    ELSE auth_profile_ref
  END,
  CASE WHEN auth_profile_ref = 'auth.google.workspace.oauth2@1' THEN '1' ELSE 'legacy-import' END,
  0, revocation_epoch,
  CASE WHEN status = 'revoked' THEN 'revoked' ELSE 'reconciling' END
FROM connections_v1_quarantine;

CREATE TABLE generic_activation_intents_v2 (
  org_id TEXT NOT NULL,
  activation_ref TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  operator_id TEXT NOT NULL,
  request_jti TEXT NOT NULL,
  profile_ref TEXT NOT NULL,
  profile_version TEXT NOT NULL,
  profile_hash TEXT NOT NULL,
  canonical_profile_json TEXT NOT NULL,
  driver_config_json TEXT NOT NULL,
  activation_kind TEXT NOT NULL CHECK (activation_kind IN ('secret_submission','workload_binding','external_custodian_binding')),
  expected_claims_json TEXT NOT NULL,
  channel_ref_hash TEXT,
  challenge_hash TEXT,
  workload_nonce_hash TEXT,
  expires_at INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('awaiting_action','claimed','active','failed','expired')),
  connection_ref TEXT,
  remote_handle TEXT,
  PRIMARY KEY (org_id, activation_ref),
  UNIQUE (org_id, deployment_id, request_jti)
);

CREATE TABLE connection_revocations_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('snapshot','provider_revoked','material_destroyed','complete','cleanup_pending')),
  expected_generation INTEGER NOT NULL,
  authority_epoch INTEGER NOT NULL,
  provider_evidence_hash TEXT,
  provider_evidence_json TEXT,
  remote_destruction_proof_hash TEXT,
  destruction_evidence_hash TEXT,
  canonical_journal_json TEXT NOT NULL,
  cas_version INTEGER NOT NULL,
  PRIMARY KEY (org_id, connection_ref)
);

CREATE TABLE binding_attestations_v2 (
  org_id TEXT NOT NULL,
  binding_ref TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  binding_hash TEXT NOT NULL,
  canonical_binding_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
  PRIMARY KEY (org_id, binding_ref),
  UNIQUE (org_id, binding_hash),
  FOREIGN KEY (org_id, connection_ref) REFERENCES connections_v2(org_id, connection_ref)
);

CREATE TABLE v2_host_records (
  org_id TEXT NOT NULL,
  artifact_ref TEXT NOT NULL,
  artifact_kind TEXT NOT NULL CHECK (artifact_kind IN ('binding', 'node_lease', 'execution_grant', 'invocation_receipt')),
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  parent_ref TEXT,
  cas_version INTEGER NOT NULL DEFAULT 0 CHECK (cas_version >= 0),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (org_id, artifact_ref),
  UNIQUE (org_id, artifact_hash)
);

CREATE TABLE v2_binding_manifests (
  org_id TEXT NOT NULL,
  binding_ref TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  manifest_json TEXT NOT NULL,
  flow_ir_hash TEXT NOT NULL,
  flow_ir_json TEXT NOT NULL,
  PRIMARY KEY (org_id, binding_ref)
);

CREATE TABLE v2_invocation_outbox (
  org_id TEXT NOT NULL,
  grant_ref TEXT NOT NULL,
  logical_effect_id TEXT NOT NULL,
  canonical_input_hash TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('prepared', 'planned', 'dispatched', 'terminal', 'ambiguous')),
  canonical_receipt_json TEXT,
  response_projection_json TEXT,
  dispatch_attempt INTEGER NOT NULL CHECK (dispatch_attempt BETWEEN 0 AND 255),
  PRIMARY KEY (org_id, grant_ref, logical_effect_id)
);

CREATE TABLE credential_cutover_state_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN (
    'inventoried', 'material_sealed', 'registry_verified', 'binding_verified',
    'fence_switched', 'legacy_material_destroyed', 'complete', 'blocked'
  )),
  expected_material_generation INTEGER CHECK (expected_material_generation > 0),
  sealed_envelope_hash TEXT,
  profile_descriptor_hash TEXT,
  authority_view_hash TEXT,
  binding_hash TEXT,
  legacy_destruction_evidence_hash TEXT,
  cas_version INTEGER NOT NULL CHECK (cas_version >= 0),
  PRIMARY KEY (org_id, connection_ref),
  FOREIGN KEY (org_id, connection_ref) REFERENCES connections_v2(org_id, connection_ref)
);

INSERT OR IGNORE INTO credential_cutover_state_v2 (
  org_id, connection_ref, phase, cas_version
)
SELECT org_id, connection_ref, 'inventoried', 0
FROM connections_v2
WHERE status = 'reconciling';

CREATE TABLE credential_cutover_events_v2 (
  event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  phase TEXT NOT NULL,
  event_hash TEXT NOT NULL UNIQUE,
  canonical_event_json TEXT NOT NULL,
  recorded_at INTEGER NOT NULL,
  FOREIGN KEY (org_id, connection_ref) REFERENCES connections_v2(org_id, connection_ref)
);
CREATE INDEX idx_credential_cutover_events_connection
  ON credential_cutover_events_v2(org_id, connection_ref, event_sequence);

CREATE TABLE legacy_destruction_evidence_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  refresh_do_route_hash TEXT NOT NULL,
  destroyed_storage_key_hash TEXT NOT NULL,
  confirmation_hash TEXT NOT NULL,
  canonical_confirmation_json TEXT NOT NULL,
  PRIMARY KEY (org_id, connection_ref),
  UNIQUE (org_id, confirmation_hash)
);

CREATE TABLE executable_admission_policy_v2 (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  minimum_protocol TEXT NOT NULL CHECK (minimum_protocol = '0.2'),
  legacy_inventory_ref TEXT NOT NULL,
  legacy_admission_expires_at TEXT NOT NULL,
  legacy_admission_enabled INTEGER NOT NULL CHECK (legacy_admission_enabled = 0),
  historical_verification_enabled INTEGER NOT NULL CHECK (historical_verification_enabled = 1)
);
INSERT INTO executable_admission_policy_v2 VALUES (
  1, '0.2', 'legacy.production.inventory.final', '2026-07-21T00:00:00Z', 0, 1
);

ALTER TABLE credential_fences_v2 RENAME TO credential_fences_v2_pre_cutover;
CREATE TABLE credential_fences_v2 (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('v1_authoritative', 'v2_prepared', 'v2_authoritative')),
  fence_generation INTEGER NOT NULL CHECK (fence_generation >= 0),
  v2_lease_ever_issued INTEGER NOT NULL CHECK (v2_lease_ever_issued IN (0, 1)),
  v2_rotation_ever_started INTEGER NOT NULL CHECK (v2_rotation_ever_started IN (0, 1)),
  v1_leasing_disabled INTEGER NOT NULL CHECK (v1_leasing_disabled IN (0, 1)),
  active_v2_generation INTEGER CHECK (active_v2_generation IS NULL OR active_v2_generation > 0),
  cas_version INTEGER NOT NULL CHECK (cas_version >= 0),
  canonical_fence_json TEXT NOT NULL,
  PRIMARY KEY (org_id, connection_ref),
  CHECK (
    (phase IN ('v1_authoritative', 'v2_prepared') AND v1_leasing_disabled = 0 AND active_v2_generation IS NULL)
    OR
    (phase = 'v2_authoritative' AND v1_leasing_disabled = 1 AND active_v2_generation IS NOT NULL)
  )
);

INSERT INTO credential_fences_v2 (
  org_id, connection_ref, phase, fence_generation, v2_lease_ever_issued,
  v2_rotation_ever_started, v1_leasing_disabled, active_v2_generation,
  cas_version, canonical_fence_json
)
SELECT
  org_id, connection_ref, phase, fence_generation, v2_lease_ever_issued,
  v2_rotation_ever_started, v1_leasing_disabled, active_v2_generation,
  cas_version, canonical_fence_json
FROM credential_fences_v2_pre_cutover;

INSERT OR IGNORE INTO credential_fences_v2 (
  org_id, connection_ref, phase, fence_generation, v2_lease_ever_issued,
  v2_rotation_ever_started, v1_leasing_disabled, active_v2_generation,
  cas_version, canonical_fence_json
)
SELECT org_id, connection_ref, 'v1_authoritative', 0, 0, 0, 0, NULL, 0,
  '{"active_v2_generation":null,"cas_version":0,"critical_fields":[],"extensions":{},"fence_generation":0,"phase":"v1_authoritative","schema_version":"0.2","v1_leasing_disabled":false,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false}'
FROM connections_v2
WHERE status = 'reconciling';
