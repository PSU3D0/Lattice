-- Additive protocol-0.2 lifecycle-separated-1 storage. Corrected artifacts are
-- append-only and legacy authority remains quarantined evidence only.
CREATE TABLE IF NOT EXISTS lifecycle_separated_schema_v2 (
  version INTEGER PRIMARY KEY CHECK (version = 6),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  legacy_default_disposition TEXT NOT NULL CHECK (legacy_default_disposition = 'quarantined')
);
CREATE TABLE IF NOT EXISTS authority_model_cutovers_v2 (
  tenant_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  cutover_epoch INTEGER NOT NULL CHECK (cutover_epoch >= 0),
  control_epoch INTEGER NOT NULL CHECK (control_epoch >= 0),
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, deployment_id, cutover_epoch),
  UNIQUE (tenant_id, artifact_hash)
);

CREATE TABLE IF NOT EXISTS authorization_observations_v2 (
  tenant_id TEXT NOT NULL,
  observation_ref TEXT NOT NULL,
  provider TEXT NOT NULL,
  auth_profile_ref TEXT NOT NULL,
  observation_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  observed_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, observation_ref),
  UNIQUE (tenant_id, observation_hash)
);

CREATE TABLE IF NOT EXISTS provider_grant_lineages_v2 (
  tenant_id TEXT NOT NULL,
  provider_grant_lineage_ref TEXT NOT NULL,
  provider TEXT NOT NULL,
  auth_profile_ref TEXT NOT NULL,
  account_subject_commitment TEXT NOT NULL,
  canonical_record_json TEXT NOT NULL CHECK (json_valid(canonical_record_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, provider_grant_lineage_ref),
  UNIQUE (
    tenant_id, provider_grant_lineage_ref,
    provider, auth_profile_ref, account_subject_commitment
  )
);

CREATE TABLE IF NOT EXISTS provider_grant_versions_v2 (
  tenant_id TEXT NOT NULL,
  provider_grant_version_ref TEXT NOT NULL,
  provider_grant_lineage_ref TEXT NOT NULL,
  provider TEXT NOT NULL,
  auth_profile_ref TEXT NOT NULL,
  account_subject_commitment TEXT NOT NULL,
  provider_authority_epoch INTEGER NOT NULL CHECK (provider_authority_epoch >= 0),
  source_observation_ref TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, provider_grant_version_ref),
  UNIQUE (tenant_id, artifact_hash),
  UNIQUE (
    tenant_id, provider_grant_version_ref,
    provider_grant_lineage_ref, account_subject_commitment
  ),
  UNIQUE (tenant_id, provider_grant_version_ref, account_subject_commitment),
  UNIQUE (
    tenant_id, provider_grant_version_ref,
    provider, auth_profile_ref, account_subject_commitment
  ),
  FOREIGN KEY (
    tenant_id, provider_grant_lineage_ref,
    provider, auth_profile_ref, account_subject_commitment
  ) REFERENCES provider_grant_lineages_v2(
    tenant_id, provider_grant_lineage_ref,
    provider, auth_profile_ref, account_subject_commitment
  )
);

CREATE TABLE IF NOT EXISTS provider_grant_adoption_records_v2 (
  tenant_id TEXT NOT NULL,
  adoption_ref TEXT NOT NULL,
  provider_grant_version_ref TEXT NOT NULL,
  provider_grant_lineage_ref TEXT NOT NULL,
  account_subject_commitment TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, adoption_ref),
  UNIQUE (tenant_id, artifact_hash),
  FOREIGN KEY (
    tenant_id, provider_grant_version_ref,
    provider_grant_lineage_ref, account_subject_commitment
  ) REFERENCES provider_grant_versions_v2(
    tenant_id, provider_grant_version_ref,
    provider_grant_lineage_ref, account_subject_commitment
  ),
  FOREIGN KEY (tenant_id, provider_grant_lineage_ref)
    REFERENCES provider_grant_lineages_v2(tenant_id, provider_grant_lineage_ref)
);

CREATE TABLE IF NOT EXISTS connection_alias_records_v2 (
  tenant_id TEXT NOT NULL,
  connection_alias TEXT NOT NULL,
  alias_epoch INTEGER NOT NULL CHECK (alias_epoch >= 0),
  provider_grant_version_ref TEXT NOT NULL,
  provider TEXT NOT NULL,
  auth_profile_ref TEXT NOT NULL,
  account_subject_commitment TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, connection_alias, alias_epoch),
  UNIQUE (tenant_id, artifact_hash),
  FOREIGN KEY (
    tenant_id, provider_grant_version_ref,
    provider, auth_profile_ref, account_subject_commitment
  ) REFERENCES provider_grant_versions_v2(
    tenant_id, provider_grant_version_ref,
    provider, auth_profile_ref, account_subject_commitment
  )
);

CREATE TABLE IF NOT EXISTS actor_connection_acl_records_v2 (
  tenant_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  actor_subject_commitment TEXT NOT NULL,
  provider_grant_version_ref TEXT NOT NULL,
  account_subject_commitment TEXT NOT NULL,
  acl_epoch INTEGER NOT NULL CHECK (acl_epoch >= 0),
  selector_hash TEXT NOT NULL,
  record_commitment TEXT NOT NULL,
  record_hash TEXT NOT NULL,
  canonical_record_json TEXT NOT NULL CHECK (json_valid(canonical_record_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (
    tenant_id, deployment_id, actor_subject_commitment,
    provider_grant_version_ref, acl_epoch
  ),
  UNIQUE (tenant_id, record_hash),
  FOREIGN KEY (tenant_id, provider_grant_version_ref, account_subject_commitment)
    REFERENCES provider_grant_versions_v2(
      tenant_id, provider_grant_version_ref, account_subject_commitment
    )
);

CREATE TABLE IF NOT EXISTS registry_decision_vectors_v2 (
  tenant_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  registry_vector_ref TEXT NOT NULL,
  vector_epoch INTEGER NOT NULL CHECK (vector_epoch >= 0),
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, deployment_id, registry_vector_ref),
  UNIQUE (tenant_id, artifact_hash)
);

CREATE TABLE IF NOT EXISTS ceiling_amendments_v2 (
  tenant_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  ceiling_amendment_ref TEXT NOT NULL,
  standing_authority_ref TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, deployment_id, ceiling_amendment_ref),
  UNIQUE (tenant_id, artifact_hash)
);

CREATE TABLE IF NOT EXISTS corrected_binding_records_v2 (
  tenant_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  binding_ref TEXT NOT NULL,
  provider_grant_version_ref TEXT NOT NULL,
  provider_grant_lineage_ref TEXT NOT NULL,
  account_subject_commitment TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, deployment_id, binding_ref),
  UNIQUE (tenant_id, artifact_hash),
  FOREIGN KEY (
    tenant_id, provider_grant_version_ref,
    provider_grant_lineage_ref, account_subject_commitment
  ) REFERENCES provider_grant_versions_v2(
    tenant_id, provider_grant_version_ref,
    provider_grant_lineage_ref, account_subject_commitment
  ),
  FOREIGN KEY (tenant_id, provider_grant_lineage_ref)
    REFERENCES provider_grant_lineages_v2(tenant_id, provider_grant_lineage_ref)
);

CREATE TABLE IF NOT EXISTS legacy_authority_inputs_v2 (
  org_id TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_identity TEXT NOT NULL,
  source_artifact_hash TEXT,
  source_metadata_json TEXT NOT NULL CHECK (json_valid(source_metadata_json)),
  classification TEXT NOT NULL CHECK (classification IN (
    'terminal', 'explicitly_quarantined', 'malformed', 'ambiguous'
  )),
  quarantine_state TEXT NOT NULL DEFAULT 'quarantined' CHECK (quarantine_state = 'quarantined'),
  evidence_reason TEXT NOT NULL,
  inventoried_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (org_id, source_table, source_identity)
);
CREATE INDEX IF NOT EXISTS legacy_authority_inputs_by_classification
  ON legacy_authority_inputs_v2(org_id, classification, source_table);

CREATE TABLE IF NOT EXISTS legacy_attempt_inventories_v2 (
  tenant_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  inventory_ref TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, deployment_id, inventory_ref),
  UNIQUE (tenant_id, artifact_hash)
);

CREATE TABLE IF NOT EXISTS receipt_verification_keysets_v2 (
  receipt_issuer TEXT NOT NULL,
  keyset_ref TEXT NOT NULL,
  keyset_epoch INTEGER NOT NULL CHECK (keyset_epoch >= 0),
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (receipt_issuer, keyset_ref),
  UNIQUE (receipt_issuer, artifact_hash)
);

CREATE TABLE IF NOT EXISTS receipt_key_compromise_records_v2 (
  receipt_issuer TEXT NOT NULL,
  compromise_record_ref TEXT NOT NULL,
  keyset_ref TEXT NOT NULL,
  affected_key_id TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  canonical_artifact_json TEXT NOT NULL CHECK (json_valid(canonical_artifact_json)),
  authority_model_revision TEXT NOT NULL CHECK (authority_model_revision = 'lifecycle-separated-1'),
  recorded_at INTEGER NOT NULL,
  PRIMARY KEY (receipt_issuer, compromise_record_ref),
  UNIQUE (receipt_issuer, artifact_hash)
);

-- Runtime verifies signatures before insertion. These storage guards independently
-- prevent canonical/index projection drift and pre-fix type confusion.
CREATE TRIGGER IF NOT EXISTS authority_model_cutovers_v2_projection
BEFORE INSERT ON authority_model_cutovers_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'AuthorityModelCutover' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.deployment_id') IS NOT NEW.deployment_id OR
  json_extract(NEW.canonical_artifact_json, '$.cutover_epoch') IS NOT NEW.cutover_epoch OR
  json_extract(NEW.canonical_artifact_json, '$.control_epoch') IS NOT NEW.control_epoch
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS authorization_observations_v2_projection
BEFORE INSERT ON authorization_observations_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'AuthorizationObservation' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.observation_ref') IS NOT NEW.observation_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider') IS NOT NEW.provider OR
  json_extract(NEW.canonical_artifact_json, '$.auth_profile_ref') IS NOT NEW.auth_profile_ref
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS provider_grant_lineages_v2_projection
BEFORE INSERT ON provider_grant_lineages_v2 WHEN
  json_extract(NEW.canonical_record_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_record_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_record_json, '$.record_type') IS NOT 'ProviderGrantLineage' OR
  json_extract(NEW.canonical_record_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_record_json, '$.provider_grant_lineage_ref') IS NOT NEW.provider_grant_lineage_ref OR
  json_extract(NEW.canonical_record_json, '$.provider') IS NOT NEW.provider OR
  json_extract(NEW.canonical_record_json, '$.auth_profile_ref') IS NOT NEW.auth_profile_ref OR
  json_extract(NEW.canonical_record_json, '$.account_subject_commitment') IS NOT NEW.account_subject_commitment
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS provider_grant_versions_v2_projection
BEFORE INSERT ON provider_grant_versions_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'ProviderGrantVersion' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.provider_grant_version_ref') IS NOT NEW.provider_grant_version_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider_grant_lineage_ref') IS NOT NEW.provider_grant_lineage_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider') IS NOT NEW.provider OR
  json_extract(NEW.canonical_artifact_json, '$.auth_profile_ref') IS NOT NEW.auth_profile_ref OR
  json_extract(NEW.canonical_artifact_json, '$.account_subject_commitment') IS NOT NEW.account_subject_commitment OR
  json_extract(NEW.canonical_artifact_json, '$.provider_authority_epoch') IS NOT NEW.provider_authority_epoch OR
  json_extract(NEW.canonical_artifact_json, '$.source_observation_ref') IS NOT NEW.source_observation_ref
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS provider_grant_adoption_records_v2_projection
BEFORE INSERT ON provider_grant_adoption_records_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'ProviderGrantAdoptionRecord' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.adoption_ref') IS NOT NEW.adoption_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider_grant_version_ref') IS NOT NEW.provider_grant_version_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider_grant_lineage_ref') IS NOT NEW.provider_grant_lineage_ref OR
  json_extract(NEW.canonical_artifact_json, '$.account_subject_commitment') IS NOT NEW.account_subject_commitment
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS connection_alias_records_v2_projection
BEFORE INSERT ON connection_alias_records_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'ConnectionAliasRecord' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.connection_alias') IS NOT NEW.connection_alias OR
  json_extract(NEW.canonical_artifact_json, '$.alias_epoch') IS NOT NEW.alias_epoch OR
  json_extract(NEW.canonical_artifact_json, '$.current_provider_grant_version_ref') IS NOT NEW.provider_grant_version_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider') IS NOT NEW.provider OR
  json_extract(NEW.canonical_artifact_json, '$.auth_profile_ref') IS NOT NEW.auth_profile_ref OR
  json_extract(NEW.canonical_artifact_json, '$.account_subject_commitment') IS NOT NEW.account_subject_commitment
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS actor_connection_acl_records_v2_projection
BEFORE INSERT ON actor_connection_acl_records_v2 WHEN
  json_extract(NEW.canonical_record_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_record_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_record_json, '$.record_type') IS NOT 'ActorConnectionAcl' OR
  json_extract(NEW.canonical_record_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_record_json, '$.deployment_id') IS NOT NEW.deployment_id OR
  json_extract(NEW.canonical_record_json, '$.actor_subject_commitment') IS NOT NEW.actor_subject_commitment OR
  json_extract(NEW.canonical_record_json, '$.provider_grant_version_ref') IS NOT NEW.provider_grant_version_ref OR
  json_extract(NEW.canonical_record_json, '$.account_subject_commitment') IS NOT NEW.account_subject_commitment OR
  json_extract(NEW.canonical_record_json, '$.selector_hash') IS NOT NEW.selector_hash OR
  json_extract(NEW.canonical_record_json, '$.acl_epoch') IS NOT NEW.acl_epoch OR
  json_extract(NEW.canonical_record_json, '$.record_commitment') IS NOT NEW.record_commitment OR
  json_extract(NEW.canonical_record_json, '$.record_hash') IS NOT NEW.record_hash
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS registry_decision_vectors_v2_projection
BEFORE INSERT ON registry_decision_vectors_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'RegistryDecisionVector' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.deployment_id') IS NOT NEW.deployment_id OR
  json_extract(NEW.canonical_artifact_json, '$.registry_vector_ref') IS NOT NEW.registry_vector_ref OR
  json_extract(NEW.canonical_artifact_json, '$.vector_epoch') IS NOT NEW.vector_epoch
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS ceiling_amendments_v2_projection
BEFORE INSERT ON ceiling_amendments_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'CeilingAmendment' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.deployment_id') IS NOT NEW.deployment_id OR
  json_extract(NEW.canonical_artifact_json, '$.ceiling_amendment_ref') IS NOT NEW.ceiling_amendment_ref OR
  json_extract(NEW.canonical_artifact_json, '$.standing_authority_ref') IS NOT NEW.standing_authority_ref
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS corrected_binding_records_v2_projection
BEFORE INSERT ON corrected_binding_records_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'CorrectedBindingAttestation' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.deployment_id') IS NOT NEW.deployment_id OR
  json_extract(NEW.canonical_artifact_json, '$.binding_ref') IS NOT NEW.binding_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider_grant_version_ref') IS NOT NEW.provider_grant_version_ref OR
  json_extract(NEW.canonical_artifact_json, '$.provider_grant_lineage_ref') IS NOT NEW.provider_grant_lineage_ref OR
  json_extract(NEW.canonical_artifact_json, '$.account_subject_commitment') IS NOT NEW.account_subject_commitment
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS legacy_attempt_inventories_v2_projection
BEFORE INSERT ON legacy_attempt_inventories_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'LegacyAttemptInventory' OR
  json_extract(NEW.canonical_artifact_json, '$.tenant_id') IS NOT NEW.tenant_id OR
  json_extract(NEW.canonical_artifact_json, '$.deployment_id') IS NOT NEW.deployment_id OR
  json_extract(NEW.canonical_artifact_json, '$.inventory_ref') IS NOT NEW.inventory_ref
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS receipt_verification_keysets_v2_projection
BEFORE INSERT ON receipt_verification_keysets_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'ReceiptVerificationKeyset' OR
  json_extract(NEW.canonical_artifact_json, '$.receipt_issuer') IS NOT NEW.receipt_issuer OR
  json_extract(NEW.canonical_artifact_json, '$.keyset_ref') IS NOT NEW.keyset_ref OR
  json_extract(NEW.canonical_artifact_json, '$.keyset_epoch') IS NOT NEW.keyset_epoch
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

CREATE TRIGGER IF NOT EXISTS receipt_key_compromise_records_v2_projection
BEFORE INSERT ON receipt_key_compromise_records_v2 WHEN
  json_extract(NEW.canonical_artifact_json, '$.schema_version') IS NOT '0.2' OR
  json_extract(NEW.canonical_artifact_json, '$.authority_model_revision') IS NOT 'lifecycle-separated-1' OR
  json_extract(NEW.canonical_artifact_json, '$.artifact_type') IS NOT 'ReceiptKeyCompromiseRecord' OR
  json_extract(NEW.canonical_artifact_json, '$.receipt_issuer') IS NOT NEW.receipt_issuer OR
  json_extract(NEW.canonical_artifact_json, '$.compromise_record_ref') IS NOT NEW.compromise_record_ref OR
  json_extract(NEW.canonical_artifact_json, '$.keyset_ref') IS NOT NEW.keyset_ref OR
  json_extract(NEW.canonical_artifact_json, '$.affected_key_id') IS NOT NEW.affected_key_id
BEGIN SELECT RAISE(ABORT, 'lifecycle projection mismatch'); END;

-- Every corrected or inventory row is append-only. Head selection and run/effect
-- transitions intentionally live outside D1.
CREATE TRIGGER IF NOT EXISTS lifecycle_separated_schema_v2_no_update BEFORE UPDATE ON lifecycle_separated_schema_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle schema marker'); END;
CREATE TRIGGER IF NOT EXISTS lifecycle_separated_schema_v2_no_delete BEFORE DELETE ON lifecycle_separated_schema_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle schema marker'); END;
CREATE TRIGGER IF NOT EXISTS authority_model_cutovers_v2_no_update BEFORE UPDATE ON authority_model_cutovers_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS authority_model_cutovers_v2_no_delete BEFORE DELETE ON authority_model_cutovers_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS authorization_observations_v2_no_update BEFORE UPDATE ON authorization_observations_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS authorization_observations_v2_no_delete BEFORE DELETE ON authorization_observations_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS provider_grant_lineages_v2_no_update BEFORE UPDATE ON provider_grant_lineages_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS provider_grant_lineages_v2_no_delete BEFORE DELETE ON provider_grant_lineages_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS provider_grant_versions_v2_no_update BEFORE UPDATE ON provider_grant_versions_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS provider_grant_versions_v2_no_delete BEFORE DELETE ON provider_grant_versions_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS provider_grant_adoption_records_v2_no_update BEFORE UPDATE ON provider_grant_adoption_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS provider_grant_adoption_records_v2_no_delete BEFORE DELETE ON provider_grant_adoption_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS connection_alias_records_v2_no_update BEFORE UPDATE ON connection_alias_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS connection_alias_records_v2_no_delete BEFORE DELETE ON connection_alias_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS actor_connection_acl_records_v2_no_update BEFORE UPDATE ON actor_connection_acl_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS actor_connection_acl_records_v2_no_delete BEFORE DELETE ON actor_connection_acl_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS registry_decision_vectors_v2_no_update BEFORE UPDATE ON registry_decision_vectors_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS registry_decision_vectors_v2_no_delete BEFORE DELETE ON registry_decision_vectors_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS ceiling_amendments_v2_no_update BEFORE UPDATE ON ceiling_amendments_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS ceiling_amendments_v2_no_delete BEFORE DELETE ON ceiling_amendments_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS corrected_binding_records_v2_no_update BEFORE UPDATE ON corrected_binding_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS corrected_binding_records_v2_no_delete BEFORE DELETE ON corrected_binding_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS legacy_authority_inputs_v2_no_update BEFORE UPDATE ON legacy_authority_inputs_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle inventory'); END;
CREATE TRIGGER IF NOT EXISTS legacy_authority_inputs_v2_no_delete BEFORE DELETE ON legacy_authority_inputs_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle inventory'); END;
CREATE TRIGGER IF NOT EXISTS legacy_attempt_inventories_v2_no_update BEFORE UPDATE ON legacy_attempt_inventories_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS legacy_attempt_inventories_v2_no_delete BEFORE DELETE ON legacy_attempt_inventories_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS receipt_verification_keysets_v2_no_update BEFORE UPDATE ON receipt_verification_keysets_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS receipt_verification_keysets_v2_no_delete BEFORE DELETE ON receipt_verification_keysets_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS receipt_key_compromise_records_v2_no_update BEFORE UPDATE ON receipt_key_compromise_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;
CREATE TRIGGER IF NOT EXISTS receipt_key_compromise_records_v2_no_delete BEFORE DELETE ON receipt_key_compromise_records_v2 BEGIN SELECT RAISE(ABORT, 'immutable lifecycle artifact'); END;

-- Legacy logical bindings and revisions carry identity only. Similarly named
-- columns are never copied into corrected authority tables.
INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'logical_bindings_v2', logical_binding_ref, identity_hash,
  json_object('logical_binding_ref', logical_binding_ref, 'identity_hash', identity_hash,
    'connection_ref', connection_ref, 'deployment_id', deployment_id, 'state', state,
    'current_binding_ref', current_binding_ref),
  'explicitly_quarantined', 'legacy logical binding identity; no corrected head inferred'
FROM logical_bindings_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT r.org_id, 'binding_revisions_v2', r.binding_ref, r.binding_hash,
  json_object('binding_ref', r.binding_ref, 'logical_binding_ref', r.logical_binding_ref,
    'revision_hash', r.revision_hash, 'binding_hash', r.binding_hash,
    'authority_view_hash', r.authority_view_hash,
    'material_generation', r.material_generation, 'state', r.state),
  CASE
    WHEN NOT json_valid(r.canonical_binding_json) THEN 'malformed'
    WHEN NOT EXISTS (SELECT 1 FROM v2_host_records h WHERE h.org_id=r.org_id AND h.artifact_ref=r.binding_ref AND h.artifact_kind='binding')
      OR NOT EXISTS (SELECT 1 FROM v2_binding_manifests m WHERE m.org_id=r.org_id AND m.binding_ref=r.binding_ref)
      THEN 'ambiguous'
    ELSE 'explicitly_quarantined'
  END,
  CASE
    WHEN NOT json_valid(r.canonical_binding_json) THEN 'malformed canonical binding JSON'
    WHEN NOT EXISTS (SELECT 1 FROM v2_host_records h WHERE h.org_id=r.org_id AND h.artifact_ref=r.binding_ref AND h.artifact_kind='binding') THEN 'missing binding host record'
    WHEN NOT EXISTS (SELECT 1 FROM v2_binding_manifests m WHERE m.org_id=r.org_id AND m.binding_ref=r.binding_ref) THEN 'missing binding manifest'
    ELSE 'legacy binding revision; state is evidence only'
  END
FROM binding_revisions_v2 r;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'binding_run_pins_v2', json_array(deployment_id, run_id),
  NULL,
  json_object('deployment_id', deployment_id, 'run_id', run_id,
    'logical_binding_ref', logical_binding_ref, 'binding_ref', binding_ref),
  'ambiguous', 'run pin is not attempt or dispatch evidence'
FROM binding_run_pins_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'binding_attestations_v2', binding_ref, binding_hash,
  json_object('binding_ref', binding_ref, 'connection_ref', connection_ref,
    'binding_hash', binding_hash, 'state', state),
  CASE WHEN json_valid(canonical_binding_json) THEN 'explicitly_quarantined' ELSE 'malformed' END,
  CASE WHEN json_valid(canonical_binding_json) THEN 'pre-separation binding attestation' ELSE 'malformed canonical binding JSON' END
FROM binding_attestations_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT h.org_id, 'v2_host_records', h.artifact_ref, h.artifact_hash,
  json_object('artifact_ref', h.artifact_ref, 'artifact_kind', h.artifact_kind,
    'artifact_hash', h.artifact_hash, 'connection_ref', h.connection_ref, 'deployment_id', h.deployment_id,
    'parent_ref', h.parent_ref, 'cas_version', h.cas_version),
  CASE
    WHEN NOT json_valid(h.canonical_artifact_json) THEN 'malformed'
    WHEN h.artifact_kind='invocation_receipt' THEN 'ambiguous'
    WHEN h.artifact_kind='binding' AND NOT EXISTS (
      SELECT 1 FROM v2_binding_manifests m WHERE m.org_id=h.org_id AND m.binding_ref=h.artifact_ref
    ) THEN 'ambiguous'
    ELSE 'ambiguous'
  END,
  CASE
    WHEN NOT json_valid(h.canonical_artifact_json) THEN 'malformed canonical host artifact JSON'
    WHEN h.artifact_kind='invocation_receipt' THEN 'pending_authenticated_verification'
    WHEN h.artifact_kind='binding' THEN 'legacy binding host evidence; manifest may be missing'
    ELSE 'lease or grant is not terminal dispatch evidence'
  END
FROM v2_host_records h;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT m.org_id, 'v2_binding_manifests', m.binding_ref, m.manifest_hash,
  json_object('binding_ref', m.binding_ref, 'manifest_hash', m.manifest_hash,
    'flow_ir_hash', m.flow_ir_hash),
  CASE
    WHEN NOT json_valid(m.manifest_json) OR NOT json_valid(m.flow_ir_json) THEN 'malformed'
    WHEN NOT EXISTS (SELECT 1 FROM v2_host_records h WHERE h.org_id=m.org_id AND h.artifact_ref=m.binding_ref AND h.artifact_kind='binding') THEN 'ambiguous'
    ELSE 'explicitly_quarantined'
  END,
  CASE
    WHEN NOT json_valid(m.manifest_json) OR NOT json_valid(m.flow_ir_json) THEN 'malformed binding manifest or flow JSON'
    WHEN NOT EXISTS (SELECT 1 FROM v2_host_records h WHERE h.org_id=m.org_id AND h.artifact_ref=m.binding_ref AND h.artifact_kind='binding') THEN 'missing binding host record'
    ELSE 'legacy manifest evidence only'
  END
FROM v2_binding_manifests m;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'v2_invocation_outbox', json_array(grant_ref, logical_effect_id), canonical_input_hash,
  json_object('grant_ref', grant_ref, 'logical_effect_id', logical_effect_id,
    'canonical_input_hash', canonical_input_hash, 'phase', phase,
    'receipt_present', canonical_receipt_json IS NOT NULL, 'dispatch_attempt', dispatch_attempt),
  CASE
    WHEN canonical_receipt_json IS NOT NULL AND NOT json_valid(canonical_receipt_json) THEN 'malformed'
    WHEN phase='terminal' AND canonical_receipt_json IS NOT NULL THEN 'ambiguous'
    ELSE 'ambiguous'
  END,
  CASE
    WHEN canonical_receipt_json IS NOT NULL AND NOT json_valid(canonical_receipt_json) THEN 'malformed terminal receipt JSON'
    WHEN phase='terminal' AND canonical_receipt_json IS NOT NULL THEN 'pending_authenticated_verification'
    WHEN phase='terminal' THEN 'terminal phase missing receipt evidence'
    ELSE 'nonterminal or explicitly ambiguous outbox phase'
  END
FROM v2_invocation_outbox;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'bindings_v1_quarantine', binding_ref, binding_lock_hash,
  json_object('binding_ref', binding_ref, 'connection_ref', connection_ref,
    'deployment_id', deployment_id, 'bundle_id', bundle_id, 'flow_ir_hash', flow_ir_hash,
    'binding_lock_hash', binding_lock_hash, 'flow_id', flow_id,
    'authority_manifest_hash', authority_manifest_hash, 'revoked', revoked),
  CASE
    WHEN NOT json_valid(contract_set_json) OR NOT json_valid(flow_ir_json)
      OR NOT json_valid(authority_manifest_json) OR NOT json_valid(attestation_json)
      THEN 'malformed'
    ELSE 'explicitly_quarantined'
  END,
  CASE
    WHEN NOT json_valid(contract_set_json) OR NOT json_valid(flow_ir_json)
      OR NOT json_valid(authority_manifest_json) OR NOT json_valid(attestation_json)
      THEN 'malformed legacy binding JSON'
    ELSE 'legacy binding authority is quarantined'
  END
FROM bindings_v1_quarantine;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'grants_v1_quarantine', grant_ref, NULL,
  json_object('grant_ref', grant_ref, 'binding_ref', binding_ref,
    'node_id', node_id, 'revoked', revoked),
  CASE WHEN json_valid(canonical_grant) THEN 'ambiguous' ELSE 'malformed' END,
  CASE WHEN json_valid(canonical_grant) THEN 'legacy grant has no terminal execution evidence' ELSE 'malformed canonical grant JSON' END
FROM grants_v1_quarantine;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'receipts_v1_history', json_array(deployment_id, receipt_ref), reservation_identity_hash,
  json_object('deployment_id', deployment_id, 'receipt_ref', receipt_ref, 'grant_ref', grant_ref,
    'reservation_identity_hash', reservation_identity_hash),
  CASE WHEN json_valid(receipt_json) THEN 'ambiguous' ELSE 'malformed' END,
  CASE WHEN json_valid(receipt_json) THEN 'pending_authenticated_verification' ELSE 'malformed receipt JSON' END
FROM receipts_v1_history;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'node_leases_v2', node_lease_ref, lease_hash,
  json_object('node_lease_ref', node_lease_ref, 'lease_hash', lease_hash, 'state', state),
  CASE WHEN json_valid(canonical_lease_json) THEN 'ambiguous' ELSE 'malformed' END,
  CASE WHEN json_valid(canonical_lease_json) THEN 'lease is not terminal dispatch evidence' ELSE 'malformed lease JSON' END
FROM node_leases_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'exact_grants_v2', grant_ref, grant_hash,
  json_object('grant_ref', grant_ref, 'grant_hash', grant_hash,
    'canonical_input_commitment_hash', canonical_input_commitment_hash,
    'logical_effect_id', logical_effect_id, 'state', state),
  CASE WHEN json_valid(canonical_grant_json) THEN 'ambiguous' ELSE 'malformed' END,
  CASE WHEN json_valid(canonical_grant_json) THEN 'grant is not terminal dispatch evidence' ELSE 'malformed grant JSON' END
FROM exact_grants_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'exact_receipts_v2', receipt_ref, receipt_hash,
  json_object('receipt_ref', receipt_ref, 'receipt_hash', receipt_hash, 'grant_ref', grant_ref,
    'dispatch_attempt', dispatch_attempt),
  CASE WHEN json_valid(canonical_receipt_json) THEN 'ambiguous' ELSE 'malformed' END,
  CASE WHEN json_valid(canonical_receipt_json) THEN 'pending_authenticated_verification' ELSE 'malformed receipt JSON' END
FROM exact_receipts_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'dispatch_outbox_v2', request_ref, request_hash,
  json_object('request_ref', request_ref, 'activation_ref', activation_ref, 'phase', phase,
    'request_hash', request_hash, 'result_present', result_json IS NOT NULL),
  CASE
    WHEN result_json IS NOT NULL AND NOT json_valid(result_json) THEN 'malformed'
    WHEN phase='terminal' AND result_json IS NOT NULL THEN 'ambiguous'
    ELSE 'ambiguous'
  END,
  CASE
    WHEN result_json IS NOT NULL AND NOT json_valid(result_json) THEN 'malformed outbox result JSON'
    WHEN phase='terminal' AND result_json IS NOT NULL THEN 'pending_authenticated_verification'
    WHEN phase='terminal' THEN 'terminal phase missing result evidence'
    ELSE 'nonterminal activation dispatch evidence'
  END
FROM dispatch_outbox_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'activation_private_replay_v2', json_array(activation_ref, submission_jti),
  COALESCE(terminal_result_hash, request_hash),
  json_object('activation_ref', activation_ref, 'submission_jti', submission_jti,
    'request_hash', request_hash, 'terminal_result_hash', terminal_result_hash),
  CASE WHEN terminal_result_hash IS NULL THEN 'ambiguous' ELSE 'terminal' END,
  CASE WHEN terminal_result_hash IS NULL THEN 'one-use marker missing terminal result' ELSE 'one-use marker has terminal result hash' END
FROM activation_private_replay_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'material_generations_v2', json_array(connection_ref, generation), envelope_hash,
  json_object('connection_ref', connection_ref, 'generation', generation,
    'envelope_hash', envelope_hash, 'material_state', material_state),
  CASE WHEN material_state='destroyed' AND length(envelope_hash)>0 THEN 'terminal' ELSE 'ambiguous' END,
  CASE WHEN material_state='destroyed' AND length(envelope_hash)>0 THEN 'trusted local custody destruction marker' ELSE 'custody generation is not destroyed' END
FROM material_generations_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'connection_revocations_v2', connection_ref, destruction_evidence_hash,
  json_object('connection_ref', connection_ref, 'phase', phase,
    'provider_evidence_hash', provider_evidence_hash,
    'destruction_evidence_hash', destruction_evidence_hash, 'cas_version', cas_version,
    'canonical_json_valid', json_valid(canonical_journal_json)),
  CASE
    WHEN NOT json_valid(canonical_journal_json) THEN 'malformed'
    WHEN phase='complete' AND length(destruction_evidence_hash)>0 THEN 'terminal'
    ELSE 'ambiguous'
  END,
  CASE
    WHEN NOT json_valid(canonical_journal_json) THEN 'malformed canonical revocation journal JSON'
    WHEN phase='complete' AND length(destruction_evidence_hash)>0 THEN 'trusted local revocation destruction marker'
    ELSE 'revocation evidence incomplete'
  END
FROM connection_revocations_v2;

INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'legacy_destruction_evidence_v2', connection_ref, confirmation_hash,
  json_object('connection_ref', connection_ref, 'refresh_do_route_hash', refresh_do_route_hash,
    'destroyed_storage_key_hash', destroyed_storage_key_hash, 'confirmation_hash', confirmation_hash,
    'canonical_json_valid', json_valid(canonical_confirmation_json)),
  CASE
    WHEN NOT json_valid(canonical_confirmation_json) THEN 'malformed'
    ELSE 'ambiguous'
  END,
  CASE
    WHEN NOT json_valid(canonical_confirmation_json) THEN 'malformed canonical destruction confirmation JSON'
    WHEN length(refresh_do_route_hash)>0 AND length(destroyed_storage_key_hash)>0
      AND length(confirmation_hash)>0 THEN 'pending_authenticated_verification'
    ELSE 'destruction marker incomplete or invalid'
  END
FROM legacy_destruction_evidence_v2;

-- Fences remain AdmissionAuthority evidence. Only a projection-consistent,
-- unused v1 fence is clearly pre-authoritative; every other state is ambiguous.
INSERT OR IGNORE INTO legacy_authority_inputs_v2
  (org_id, source_table, source_identity, source_artifact_hash, source_metadata_json, classification, evidence_reason)
SELECT org_id, 'credential_fences_v2', connection_ref, NULL,
  json_object(
    'connection_ref', connection_ref, 'phase', phase,
    'fence_generation', fence_generation,
    'v2_lease_ever_issued', v2_lease_ever_issued,
    'v2_rotation_ever_started', v2_rotation_ever_started,
    'v1_leasing_disabled', v1_leasing_disabled,
    'active_v2_generation', active_v2_generation,
    'cas_version', cas_version,
    'canonical_json_valid', json_valid(canonical_fence_json),
    'canonical_projection_consistent',
      CASE WHEN json_valid(canonical_fence_json)
        AND json_extract(canonical_fence_json, '$.phase') IS phase
        AND json_extract(canonical_fence_json, '$.fence_generation') IS fence_generation
        AND json_extract(canonical_fence_json, '$.v2_lease_ever_issued') IS v2_lease_ever_issued
        AND json_extract(canonical_fence_json, '$.v2_rotation_ever_started') IS v2_rotation_ever_started
        AND json_extract(canonical_fence_json, '$.v1_leasing_disabled') IS v1_leasing_disabled
        AND json_extract(canonical_fence_json, '$.active_v2_generation') IS active_v2_generation
        AND json_extract(canonical_fence_json, '$.cas_version') IS cas_version
        THEN 1 ELSE 0 END
  ),
  CASE
    WHEN NOT json_valid(canonical_fence_json) THEN 'malformed'
    WHEN phase='v1_authoritative'
      AND v2_lease_ever_issued=0 AND v2_rotation_ever_started=0
      AND v1_leasing_disabled=0 AND active_v2_generation IS NULL
      AND json_valid(canonical_fence_json)
      AND json_extract(canonical_fence_json, '$.phase') IS phase
      AND json_extract(canonical_fence_json, '$.fence_generation') IS fence_generation
      AND json_extract(canonical_fence_json, '$.v2_lease_ever_issued') IS v2_lease_ever_issued
      AND json_extract(canonical_fence_json, '$.v2_rotation_ever_started') IS v2_rotation_ever_started
      AND json_extract(canonical_fence_json, '$.v1_leasing_disabled') IS v1_leasing_disabled
      AND json_extract(canonical_fence_json, '$.active_v2_generation') IS active_v2_generation
      AND json_extract(canonical_fence_json, '$.cas_version') IS cas_version
      THEN 'explicitly_quarantined'
    ELSE 'ambiguous'
  END,
  CASE
    WHEN NOT json_valid(canonical_fence_json) THEN 'invalid fence canonical JSON'
    WHEN json_extract(canonical_fence_json, '$.phase') IS NOT phase
      OR json_extract(canonical_fence_json, '$.fence_generation') IS NOT fence_generation
      OR json_extract(canonical_fence_json, '$.v2_lease_ever_issued') IS NOT v2_lease_ever_issued
      OR json_extract(canonical_fence_json, '$.v2_rotation_ever_started') IS NOT v2_rotation_ever_started
      OR json_extract(canonical_fence_json, '$.v1_leasing_disabled') IS NOT v1_leasing_disabled
      OR json_extract(canonical_fence_json, '$.active_v2_generation') IS NOT active_v2_generation
      OR json_extract(canonical_fence_json, '$.cas_version') IS NOT cas_version
      THEN 'inconsistent fence canonical projection'
    WHEN v2_lease_ever_issued=1 THEN 'v2 lease was issued'
    WHEN v2_rotation_ever_started=1 THEN 'v2 rotation was started'
    WHEN phase='v2_authoritative' OR v1_leasing_disabled=1 OR active_v2_generation IS NOT NULL
      THEN 'authoritative fence evidence requires reconciliation'
    WHEN phase='v2_prepared' THEN 'prepared fence evidence requires reconciliation'
    ELSE 'clearly unused pre-authoritative fence'
  END
FROM credential_fences_v2;

-- D1's migration runner should apply the file atomically. This completion row is
-- an additional crash guard: partial schema/inventory publication is never authority,
-- and an idempotent full rerun publishes completion only after every prior statement.
INSERT OR IGNORE INTO lifecycle_separated_schema_v2
  (version, authority_model_revision, legacy_default_disposition)
VALUES (6, 'lifecycle-separated-1', 'quarantined');
