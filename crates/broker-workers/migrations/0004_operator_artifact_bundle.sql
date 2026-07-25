CREATE TABLE operator_artifact_bundles (
  deployment_id TEXT NOT NULL,
  bundle_hash TEXT NOT NULL CHECK (
    length(bundle_hash) = 71
    AND substr(bundle_hash, 1, 7) = 'sha256:'
    AND substr(bundle_hash, 8) NOT GLOB '*[^0-9a-f]*'
  ),
  canonical_bundle_jcs BLOB NOT NULL,
  seeded_at INTEGER NOT NULL,
  PRIMARY KEY (deployment_id, bundle_hash),
  CHECK (length(canonical_bundle_jcs) > 0)
);
CREATE INDEX idx_operator_artifact_bundles_hash
  ON operator_artifact_bundles(bundle_hash);
