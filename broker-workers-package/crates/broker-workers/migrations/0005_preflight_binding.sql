-- Stable logical binding identities and expiring signed revisions.
-- These tables are the authoritative V2 binding lifecycle view. The older
-- binding_attestations_v2 table remains only for migration compatibility.
CREATE TABLE logical_bindings_v2 (
  org_id TEXT NOT NULL,
  logical_binding_ref TEXT NOT NULL,
  identity_hash TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
  current_binding_ref TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (org_id, logical_binding_ref),
  UNIQUE (org_id, identity_hash),
  FOREIGN KEY (org_id, connection_ref) REFERENCES connections_v2(org_id, connection_ref)
);

CREATE TABLE binding_revisions_v2 (
  org_id TEXT NOT NULL,
  binding_ref TEXT NOT NULL,
  logical_binding_ref TEXT NOT NULL,
  revision_hash TEXT NOT NULL,
  binding_hash TEXT NOT NULL,
  canonical_binding_json TEXT NOT NULL,
  authority_view_hash TEXT NOT NULL,
  material_generation INTEGER NOT NULL CHECK (material_generation > 0),
  state TEXT NOT NULL CHECK (state IN ('active', 'superseded', 'revoked')),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (org_id, binding_ref),
  UNIQUE (org_id, logical_binding_ref, revision_hash),
  UNIQUE (org_id, binding_hash),
  FOREIGN KEY (org_id, logical_binding_ref) REFERENCES logical_bindings_v2(org_id, logical_binding_ref)
);

CREATE INDEX binding_revisions_by_logical
  ON binding_revisions_v2(org_id, logical_binding_ref, state);

CREATE TABLE binding_run_pins_v2 (
  org_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  logical_binding_ref TEXT NOT NULL,
  binding_ref TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (org_id, deployment_id, run_id),
  FOREIGN KEY (org_id, logical_binding_ref) REFERENCES logical_bindings_v2(org_id, logical_binding_ref),
  FOREIGN KEY (org_id, binding_ref) REFERENCES binding_revisions_v2(org_id, binding_ref)
);

-- Existing revisions remain leaseable but are deliberately not candidates for
-- content deduplication: their old install request identity was not persisted.
INSERT OR IGNORE INTO logical_bindings_v2 (
  org_id, logical_binding_ref, identity_hash, connection_ref, deployment_id,
  state, current_binding_ref, created_at, updated_at
)
SELECT
  h.org_id,
  'logical_binding_v2_legacy_' || substr(h.artifact_ref, 12),
  h.artifact_hash,
  h.connection_ref,
  h.deployment_id,
  CASE WHEN c.status = 'revoked' OR a.state = 'revoked' THEN 'revoked' ELSE 'active' END,
  h.artifact_ref,
  h.created_at,
  h.created_at
FROM v2_host_records h
JOIN connections_v2 c ON c.org_id = h.org_id AND c.connection_ref = h.connection_ref
LEFT JOIN binding_attestations_v2 a ON a.org_id = h.org_id AND a.binding_ref = h.artifact_ref
WHERE h.artifact_kind = 'binding';

INSERT OR IGNORE INTO binding_revisions_v2 (
  org_id, binding_ref, logical_binding_ref, revision_hash, binding_hash,
  canonical_binding_json, authority_view_hash, material_generation, state,
  created_at
)
SELECT
  h.org_id,
  h.artifact_ref,
  'logical_binding_v2_legacy_' || substr(h.artifact_ref, 12),
  h.artifact_hash,
  h.artifact_hash,
  h.canonical_artifact_json,
  json_extract(h.canonical_artifact_json, '$.authority_view_hash'),
  json_extract(h.canonical_artifact_json, '$.minimum_material_generation'),
  CASE WHEN c.status = 'revoked' OR a.state = 'revoked' THEN 'revoked' ELSE 'active' END,
  h.created_at
FROM v2_host_records h
JOIN connections_v2 c ON c.org_id = h.org_id AND c.connection_ref = h.connection_ref
LEFT JOIN binding_attestations_v2 a ON a.org_id = h.org_id AND a.binding_ref = h.artifact_ref
WHERE h.artifact_kind = 'binding';
