CREATE TABLE IF NOT EXISTS broker_schema (
  version INTEGER PRIMARY KEY CHECK (version = 1)
);
INSERT OR IGNORE INTO broker_schema(version) VALUES (1);

CREATE TABLE IF NOT EXISTS deployment_keys (
  org_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  key_hash TEXT NOT NULL UNIQUE,
  expires_at INTEGER NOT NULL,
  revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1)),
  PRIMARY KEY (org_id, deployment_id, key_hash)
);
CREATE INDEX IF NOT EXISTS idx_deployment_keys_hash ON deployment_keys(key_hash);

CREATE TABLE IF NOT EXISTS sessions (
  session_ref TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  pop_key_thumbprint TEXT NOT NULL,
  pop_public_key TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_sessions_tenant ON sessions(org_id, session_ref);

CREATE TABLE IF NOT EXISTS session_exchange_nonces (
  deployment_key_id TEXT NOT NULL,
  client_nonce TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  PRIMARY KEY (deployment_key_id, client_nonce)
);
CREATE INDEX IF NOT EXISTS idx_exchange_nonce_expiry ON session_exchange_nonces(timestamp);

CREATE TABLE IF NOT EXISTS session_request_jtis (
  session_ref TEXT NOT NULL,
  jti TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  PRIMARY KEY (session_ref, jti)
);
CREATE INDEX IF NOT EXISTS idx_session_jti_expiry ON session_request_jtis(timestamp);

CREATE TABLE IF NOT EXISTS connection_intents (
  intent_ref TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  connector_ref TEXT NOT NULL,
  auth_profile_ref TEXT NOT NULL,
  execution_lane TEXT NOT NULL CHECK (execution_lane = 'semantic_broker'),
  custody TEXT NOT NULL CHECK (custody = 'hosted_broker'),
  oauth_state_hash TEXT NOT NULL UNIQUE,
  pkce_nonce TEXT NOT NULL,
  pkce_ciphertext TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'claimed', 'activating', 'cleanup_pending', 'restart_required', 'ready', 'failed')),
  activation_phase TEXT CHECK (activation_phase IN ('exchange_pending', 'exchange_inflight', 'route_reserved', 'credential_registered', 'connection_inserted', 'cleanup_pending')),
  exchange_nonce TEXT,
  exchange_ciphertext TEXT,
  activation_connection_ref TEXT,
  activation_route TEXT,
  activation_account_commitment TEXT,
  activation_scopes_json TEXT,
  activation_nonce TEXT,
  activation_ciphertext TEXT,
  failure_code TEXT
);
CREATE INDEX IF NOT EXISTS idx_connection_intents_tenant ON connection_intents(org_id, intent_ref);

CREATE TABLE IF NOT EXISTS connections (
  org_id TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  intent_ref TEXT NOT NULL,
  connector_ref TEXT NOT NULL,
  auth_profile_ref TEXT NOT NULL,
  execution_lane TEXT NOT NULL CHECK (execution_lane = 'semantic_broker'),
  custody TEXT NOT NULL CHECK (custody = 'hosted_broker'),
  account_commitment TEXT NOT NULL,
  actual_scopes_json TEXT NOT NULL,
  refresh_do_route TEXT NOT NULL,
  revocation_epoch INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN ('activating', 'active', 'revoked', 'blocked')),
  PRIMARY KEY (org_id, connection_ref),
  UNIQUE (org_id, intent_ref)
);

CREATE TABLE IF NOT EXISTS bindings (
  org_id TEXT NOT NULL,
  binding_ref TEXT NOT NULL,
  connection_ref TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  bundle_id TEXT NOT NULL,
  flow_ir_hash TEXT NOT NULL,
  binding_lock_hash TEXT NOT NULL,
  flow_id TEXT NOT NULL,
  contract_set_json TEXT NOT NULL,
  flow_ir_json TEXT NOT NULL,
  authority_manifest_json TEXT NOT NULL,
  authority_manifest_hash TEXT NOT NULL,
  attestation_json TEXT NOT NULL,
  revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1)),
  PRIMARY KEY (org_id, binding_ref)
);

CREATE TABLE IF NOT EXISTS grants (
  org_id TEXT NOT NULL,
  grant_ref TEXT NOT NULL,
  binding_ref TEXT NOT NULL,
  canonical_grant TEXT NOT NULL,
  node_id TEXT NOT NULL,
  operation_contract TEXT NOT NULL,
  allocated_logical_calls INTEGER NOT NULL CHECK (allocated_logical_calls > 0),
  flow_aggregate_limit INTEGER,
  connection_aggregate_key TEXT,
  connection_aggregate_limit INTEGER,
  expires_at INTEGER NOT NULL,
  revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1)),
  PRIMARY KEY (org_id, grant_ref)
);

CREATE TABLE IF NOT EXISTS receipts (
  org_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  receipt_ref TEXT NOT NULL,
  grant_ref TEXT NOT NULL,
  reservation_identity_hash TEXT NOT NULL,
  receipt_json TEXT NOT NULL,
  PRIMARY KEY (org_id, deployment_id, receipt_ref)
);
CREATE INDEX IF NOT EXISTS idx_receipts_grant ON receipts(org_id, deployment_id, grant_ref);
