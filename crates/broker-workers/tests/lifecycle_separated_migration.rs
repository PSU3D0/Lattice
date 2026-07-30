use rusqlite::Connection;

const MIGRATION: &str = include_str!("../migrations/0006_lifecycle_separated_authority.sql");

fn apply_through_0005(db: &Connection) {
    db.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    for migration in [
        include_str!("../migrations/0001_broker.sql"),
        include_str!("../migrations/0002_credential_plane_v2.sql"),
        include_str!("../migrations/0003_production_v2_cutover.sql"),
        include_str!("../migrations/0004_operator_artifact_bundle.sql"),
        include_str!("../migrations/0005_preflight_binding.sql"),
    ] {
        db.execute_batch(migration).unwrap();
    }
}

fn populate_live_shape(db: &Connection) {
    db.execute_batch(
        r#"
        INSERT INTO connections_v2(org_id,connection_ref,profile_ref,profile_version,status) VALUES
          ('org','connection-active','profile','1','active'),
          ('org','connection-revoked','profile','1','revoked');

        INSERT INTO credential_fences_v2
          (org_id,connection_ref,phase,fence_generation,v2_lease_ever_issued,
           v2_rotation_ever_started,v1_leasing_disabled,active_v2_generation,
           cas_version,canonical_fence_json) VALUES
          ('org','fence-clean','v1_authoritative',0,0,0,0,NULL,0,
           '{"phase":"v1_authoritative","fence_generation":0,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,"v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":0}'),
          ('org','fence-issued','v1_authoritative',1,1,0,0,NULL,1,
           '{"phase":"v1_authoritative","fence_generation":1,"v2_lease_ever_issued":true,"v2_rotation_ever_started":false,"v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":1}'),
          ('org','fence-rotated','v1_authoritative',2,0,1,0,NULL,2,
           '{"phase":"v1_authoritative","fence_generation":2,"v2_lease_ever_issued":false,"v2_rotation_ever_started":true,"v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":2}'),
          ('org','fence-prepared','v2_prepared',3,0,0,0,NULL,3,
           '{"phase":"v2_prepared","fence_generation":3,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,"v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":3}'),
          ('org','fence-authoritative','v2_authoritative',4,1,1,1,2,4,
           '{"phase":"v2_authoritative","fence_generation":4,"v2_lease_ever_issued":true,"v2_rotation_ever_started":true,"v1_leasing_disabled":true,"active_v2_generation":2,"cas_version":4}'),
          ('org','fence-inconsistent','v1_authoritative',5,0,0,0,NULL,5,
           '{"phase":"v2_prepared","fence_generation":5,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false,"v1_leasing_disabled":false,"active_v2_generation":null,"cas_version":5}'),
          ('org','fence-invalid','v1_authoritative',6,0,0,0,NULL,6,'{');

        INSERT INTO logical_bindings_v2
          (org_id,logical_binding_ref,identity_hash,connection_ref,deployment_id,state,current_binding_ref,created_at,updated_at) VALUES
          ('org','logical-known','identity-known','connection-active','deployment','active','binding-known',1,1),
          ('org','logical-revoked','identity-revoked','connection-revoked','deployment','revoked','binding-revoked',1,2),
          ('org','logical-multiple','identity-multiple','connection-active','deployment','active','binding-multiple',1,3);

        INSERT INTO binding_revisions_v2
          (org_id,binding_ref,logical_binding_ref,revision_hash,binding_hash,canonical_binding_json,authority_view_hash,material_generation,state,created_at) VALUES
          ('org','binding-known','logical-known','revision-known','binding-hash-known','{}','authority-known',1,'active',1),
          ('org','binding-superseded','logical-known','revision-superseded','binding-hash-superseded','{}','authority-old',1,'superseded',1),
          ('org','binding-revoked','logical-revoked','revision-revoked','binding-hash-revoked','{}','authority-revoked',1,'revoked',1),
          ('org','binding-malformed','logical-known','revision-malformed','binding-hash-malformed','{','authority-malformed',1,'superseded',1),
          ('org','binding-multiple','logical-multiple','revision-multiple','binding-hash-multiple','{}','authority-known',1,'active',1);

        INSERT INTO binding_run_pins_v2
          (org_id,deployment_id,run_id,logical_binding_ref,binding_ref,created_at) VALUES
          ('org','deployment','run-without-attempt','logical-known','binding-known',1),
          ('org','left:right','tail','logical-known','binding-known',1),
          ('org','left','right:tail','logical-known','binding-known',1);

        INSERT INTO binding_attestations_v2
          (org_id,binding_ref,connection_ref,binding_hash,canonical_binding_json,state) VALUES
          ('org','binding-known','connection-active','attestation-conflicting-hash','{}','active'),
          ('org','binding-revoked','connection-revoked','attestation-revoked-hash','{}','revoked');

        INSERT INTO v2_host_records
          (org_id,artifact_ref,artifact_kind,artifact_hash,canonical_artifact_json,connection_ref,deployment_id,parent_ref,cas_version,created_at) VALUES
          ('org','binding-known','binding','host-conflicting-hash','{}','connection-active','deployment',NULL,0,1),
          ('org','binding-superseded','binding','host-binding-superseded','{}','connection-active','deployment',NULL,0,1),
          ('org','binding-host-only','binding','host-binding-only','{}','connection-active','deployment',NULL,0,1),
          ('org','binding-multiple','binding','host-binding-multiple','{}','connection-active','deployment',NULL,0,1),
          ('org','lease-host','node_lease','host-lease','{}','connection-active','deployment','binding-known',0,1),
          ('org','grant-host','execution_grant','host-grant','{}','connection-active','deployment','lease-host',0,1),
          ('org','receipt-host','invocation_receipt','host-receipt','{}','connection-active','deployment','grant-host',0,1),
          ('org','receipt-host-malformed','invocation_receipt','host-receipt-malformed','{','connection-active','deployment','grant-host',0,1);

        INSERT INTO v2_binding_manifests
          (org_id,binding_ref,manifest_hash,manifest_json,flow_ir_hash,flow_ir_json) VALUES
          ('org','binding-known','manifest-known','{}','flow-known','{}'),
          ('org','binding-superseded','manifest-superseded','{}','flow-superseded','{}'),
          ('org','binding-manifest-only','manifest-only','{}','flow-only','{}'),
          ('org','binding-multiple','manifest-multiple','{}','flow-multiple','{}');

        INSERT INTO v2_invocation_outbox
          (org_id,grant_ref,logical_effect_id,canonical_input_hash,phase,canonical_receipt_json,response_projection_json,dispatch_attempt) VALUES
          ('org','grant-prepared','effect-prepared','input-prepared','prepared',NULL,NULL,0),
          ('org','grant-planned','effect-planned','input-planned','planned',NULL,NULL,0),
          ('org','grant-dispatched','effect-dispatched','input-dispatched','dispatched',NULL,NULL,1),
          ('org','grant-ambiguous','effect-ambiguous','input-ambiguous','ambiguous',NULL,NULL,1),
          ('org','grant-terminal','effect-terminal','input-terminal','terminal','{}',NULL,1),
          ('org','grant-terminal-missing','effect-terminal-missing','input-terminal-missing','terminal',NULL,NULL,1),
          ('org','grant-terminal-malformed','effect-terminal-malformed','input-terminal-malformed','terminal','{',NULL,1),
          ('org','grant:effect','tail','input-collision-a','prepared',NULL,NULL,0),
          ('org','grant','effect:tail','input-collision-b','prepared',NULL,NULL,0);

        INSERT INTO bindings_v1_quarantine
          (org_id,binding_ref,connection_ref,deployment_id,bundle_id,flow_ir_hash,binding_lock_hash,flow_id,contract_set_json,flow_ir_json,authority_manifest_json,authority_manifest_hash,attestation_json,revoked) VALUES
          ('org','v1-binding-a','connection-active','deployment','bundle','flow-a','lock-a','flow','{}','{}','{}','manifest-a','{}',0),
          ('org','v1-binding-b','connection-active','deployment','bundle','flow-b','lock-b','flow','{}','{}','{}','manifest-b','{}',0);
        INSERT INTO grants_v1_quarantine
          (org_id,grant_ref,binding_ref,canonical_grant,node_id,operation_contract,allocated_logical_calls,expires_at,revoked)
          VALUES ('org','v1-grant','v1-binding-a','{}','node','operation',1,10,0);
        INSERT INTO receipts_v1_history
          (org_id,deployment_id,receipt_ref,grant_ref,reservation_identity_hash,receipt_json) VALUES
          ('org','deployment','v1-receipt','v1-grant','reservation-hash','{}'),
          ('org','deployment','v1-receipt-malformed','v1-grant','reservation-malformed','{');

        INSERT INTO node_leases_v2(org_id,node_lease_ref,lease_hash,state,canonical_lease_json)
          VALUES ('org','old-lease','old-lease-hash','prepared','{}');
        INSERT INTO exact_grants_v2
          (org_id,grant_ref,grant_hash,canonical_input_commitment_hash,logical_effect_id,state,canonical_grant_json)
          VALUES ('org','old-grant','old-grant-hash','old-input-hash','old-effect','prepared','{}');
        INSERT INTO exact_receipts_v2
          (org_id,receipt_ref,receipt_hash,grant_ref,dispatch_attempt,canonical_receipt_json) VALUES
          ('org','old-receipt','old-receipt-hash','old-grant',1,'{}'),
          ('org','old-receipt-malformed','old-receipt-malformed-hash','old-grant',1,'{');

        INSERT INTO dispatch_outbox_v2
          (org_id,request_ref,activation_ref,phase,request_hash,result_json) VALUES
          ('org','activation-prepared','activation','prepared','request-prepared',NULL),
          ('org','activation-terminal-missing','activation','terminal','request-terminal',NULL),
          ('org','activation-terminal-valid','activation','terminal','request-valid','{}'),
          ('org','activation-terminal-malformed','activation','terminal','request-malformed','{');
        INSERT INTO activation_private_replay_v2
          (org_id,activation_ref,submission_jti,request_hash,terminal_result_hash) VALUES
          ('org','activation','jti-pending','replay-pending',NULL),
          ('org','activation','jti-terminal','replay-terminal','terminal-result');

        INSERT INTO material_generations_v2
          (org_id,connection_ref,generation,envelope_hash,sealed_envelope,material_state) VALUES
          ('org','connection-active',1,'envelope-active',x'01','active'),
          ('org','connection-revoked',1,'envelope-destroyed',x'02','destroyed');
        INSERT INTO connection_revocations_v2
          (org_id,connection_ref,phase,expected_generation,authority_epoch,destruction_evidence_hash,canonical_journal_json,cas_version) VALUES
          ('org','connection-active','snapshot',1,1,NULL,'{}',0),
          ('org','connection-revoked','complete',1,1,'destruction-hash','{}',1),
          ('org','connection-malformed','complete',1,1,'destruction-malformed','{',2);
        INSERT INTO legacy_destruction_evidence_v2
          (org_id,connection_ref,refresh_do_route_hash,destroyed_storage_key_hash,confirmation_hash,canonical_confirmation_json) VALUES
          ('org','connection-revoked','route-hash','storage-hash','confirmation-hash','{}'),
          ('org','connection-active','','storage-incomplete','confirmation-incomplete','{}'),
          ('org','connection-malformed','route-malformed','storage-malformed','confirmation-malformed','{');
        "#,
    )
    .unwrap();
}

#[test]
fn populated_0005_matrix_is_fully_inventoried_and_quarantined_without_inference() {
    let db = Connection::open_in_memory().unwrap();
    apply_through_0005(&db);
    populate_live_shape(&db);
    db.execute_batch(MIGRATION).unwrap();

    let counts: (i64, i64, i64, i64, i64) = db
        .query_row(
            "SELECT COUNT(*),
                    SUM(classification='terminal'),
                    SUM(classification='explicitly_quarantined'),
                    SUM(classification='malformed'),
                    SUM(classification='ambiguous')
             FROM legacy_authority_inputs_v2",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(counts, (64, 3, 14, 9, 38));

    let source_tables: i64 = db
        .query_row(
            "SELECT COUNT(DISTINCT source_table) FROM legacy_authority_inputs_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(source_tables, 19);
    let logical_states: (i64, i64) = db
        .query_row(
            "SELECT SUM(json_extract(source_metadata_json,'$.state')='active'),
                    SUM(json_extract(source_metadata_json,'$.state')='revoked')
             FROM legacy_authority_inputs_v2 WHERE source_table='logical_bindings_v2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(logical_states, (2, 1));
    let revision_states: (i64, i64, i64) = db
        .query_row(
            "SELECT SUM(json_extract(source_metadata_json,'$.state')='active'),
                    SUM(json_extract(source_metadata_json,'$.state')='superseded'),
                    SUM(json_extract(source_metadata_json,'$.state')='revoked')
             FROM legacy_authority_inputs_v2 WHERE source_table='binding_revisions_v2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(revision_states, (2, 2, 1));
    let outbox_shape: (i64, i64) = db
        .query_row(
            "SELECT COUNT(*), COUNT(DISTINCT json_extract(source_metadata_json,'$.phase'))
             FROM legacy_authority_inputs_v2 WHERE source_table='v2_invocation_outbox'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(outbox_shape, (9, 5));
    for (source_table, expected) in [
        ("binding_run_pins_v2", 3_i64),
        ("v2_invocation_outbox", 9_i64),
        ("receipts_v1_history", 2_i64),
        ("activation_private_replay_v2", 2_i64),
        ("material_generations_v2", 2_i64),
    ] {
        let identities: (i64, i64) = db
            .query_row(
                "SELECT COUNT(*), COUNT(DISTINCT source_identity)
                 FROM legacy_authority_inputs_v2
                 WHERE source_table=?1 AND json_valid(source_identity)
                   AND json_type(source_identity)='array'",
                [source_table],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(identities, (expected, expected), "{source_table}");
    }
    for source_table in ["binding_run_pins_v2", "v2_invocation_outbox"] {
        let collision_pair: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM legacy_authority_inputs_v2
                 WHERE source_table=?1 AND source_identity IN (
                   json_array('left:right','tail'), json_array('left','right:tail'),
                   json_array('grant:effect','tail'), json_array('grant','effect:tail'))",
                [source_table],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(collision_pair, 2, "{source_table}");
    }
    for identity in ["binding-host-only", "binding-manifest-only"] {
        let partial: String = db
            .query_row(
                "SELECT classification FROM legacy_authority_inputs_v2
                 WHERE source_identity=?1",
                [identity],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(partial, "ambiguous");
    }
    let non_quarantined: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM legacy_authority_inputs_v2 WHERE quarantine_state != 'quarantined'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(non_quarantined, 0);

    let terminal_missing: String = db
        .query_row(
            "SELECT classification FROM legacy_authority_inputs_v2
             WHERE source_table='v2_invocation_outbox'
               AND source_identity=json_array('grant-terminal-missing','effect-terminal-missing')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(terminal_missing, "ambiguous");
    let sql_only_terminal_artifacts: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM legacy_authority_inputs_v2
             WHERE classification='terminal' AND source_table IN (
               'v2_host_records','v2_invocation_outbox','receipts_v1_history',
               'exact_receipts_v2','dispatch_outbox_v2')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sql_only_terminal_artifacts, 0);
    for (source_table, valid_identity, malformed_identity) in [
        ("v2_host_records", "receipt-host", "receipt-host-malformed"),
        (
            "v2_invocation_outbox",
            "[\"grant-terminal\",\"effect-terminal\"]",
            "[\"grant-terminal-malformed\",\"effect-terminal-malformed\"]",
        ),
        (
            "receipts_v1_history",
            "[\"deployment\",\"v1-receipt\"]",
            "[\"deployment\",\"v1-receipt-malformed\"]",
        ),
        ("exact_receipts_v2", "old-receipt", "old-receipt-malformed"),
        (
            "dispatch_outbox_v2",
            "activation-terminal-valid",
            "activation-terminal-malformed",
        ),
    ] {
        let valid: (String, String) = db
            .query_row(
                "SELECT classification,evidence_reason FROM legacy_authority_inputs_v2
                 WHERE source_table=?1 AND source_identity=?2",
                [source_table, valid_identity],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            valid,
            (
                "ambiguous".into(),
                "pending_authenticated_verification".into()
            )
        );
        let malformed: String = db
            .query_row(
                "SELECT classification FROM legacy_authority_inputs_v2
                 WHERE source_table=?1 AND source_identity=?2",
                [source_table, malformed_identity],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(malformed, "malformed");
    }
    let fence_counts: (i64, i64) = db
        .query_row(
            "SELECT SUM(classification='explicitly_quarantined'),
                    SUM(classification='ambiguous')
             FROM legacy_authority_inputs_v2 WHERE source_table='credential_fences_v2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(fence_counts, (1, 5));
    for identity in [
        "fence-issued",
        "fence-rotated",
        "fence-prepared",
        "fence-authoritative",
        "fence-inconsistent",
    ] {
        let classification: String = db
            .query_row(
                "SELECT classification FROM legacy_authority_inputs_v2
                 WHERE source_table='credential_fences_v2' AND source_identity=?1",
                [identity],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(classification, "ambiguous", "{identity}");
    }
    let clean_fence: (String, i64, i64) = db
        .query_row(
            "SELECT classification,
                    json_extract(source_metadata_json,'$.canonical_json_valid'),
                    json_extract(source_metadata_json,'$.canonical_projection_consistent')
             FROM legacy_authority_inputs_v2
             WHERE source_table='credential_fences_v2' AND source_identity='fence-clean'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(clean_fence, ("explicitly_quarantined".into(), 1, 1));
    let invalid_fence: (String, i64, i64) = db
        .query_row(
            "SELECT classification,
                    json_extract(source_metadata_json,'$.canonical_json_valid'),
                    json_extract(source_metadata_json,'$.canonical_projection_consistent')
             FROM legacy_authority_inputs_v2
             WHERE source_table='credential_fences_v2' AND source_identity='fence-invalid'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(invalid_fence, ("malformed".into(), 0, 0));
    let destruction_classes: (String, String, String, i64, i64) = db
        .query_row(
            "SELECT
               (SELECT classification FROM legacy_authority_inputs_v2
                WHERE source_table='legacy_destruction_evidence_v2'
                  AND source_identity='connection-revoked'),
               (SELECT classification FROM legacy_authority_inputs_v2
                WHERE source_table='legacy_destruction_evidence_v2'
                  AND source_identity='connection-active'),
               (SELECT classification FROM legacy_authority_inputs_v2
                WHERE source_table='legacy_destruction_evidence_v2'
                  AND source_identity='connection-malformed'),
               (SELECT json_extract(source_metadata_json,'$.canonical_json_valid')
                FROM legacy_authority_inputs_v2
                WHERE source_table='legacy_destruction_evidence_v2'
                  AND source_identity='connection-revoked'),
               (SELECT json_extract(source_metadata_json,'$.canonical_json_valid')
                FROM legacy_authority_inputs_v2
                WHERE source_table='legacy_destruction_evidence_v2'
                  AND source_identity='connection-malformed')",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        destruction_classes,
        (
            "ambiguous".into(),
            "ambiguous".into(),
            "malformed".into(),
            1,
            0,
        )
    );
    let revocation_classes: (String, String, String, i64, i64) = db
        .query_row(
            "SELECT
               (SELECT classification FROM legacy_authority_inputs_v2
                WHERE source_table='connection_revocations_v2'
                  AND source_identity='connection-active'),
               (SELECT classification FROM legacy_authority_inputs_v2
                WHERE source_table='connection_revocations_v2'
                  AND source_identity='connection-revoked'),
               (SELECT classification FROM legacy_authority_inputs_v2
                WHERE source_table='connection_revocations_v2'
                  AND source_identity='connection-malformed'),
               (SELECT json_extract(source_metadata_json,'$.canonical_json_valid')
                FROM legacy_authority_inputs_v2
                WHERE source_table='connection_revocations_v2'
                  AND source_identity='connection-revoked'),
               (SELECT json_extract(source_metadata_json,'$.canonical_json_valid')
                FROM legacy_authority_inputs_v2
                WHERE source_table='connection_revocations_v2'
                  AND source_identity='connection-malformed')",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        revocation_classes,
        (
            "ambiguous".into(),
            "terminal".into(),
            "malformed".into(),
            1,
            0,
        )
    );
    let pin: String = db
        .query_row(
            "SELECT classification FROM legacy_authority_inputs_v2
             WHERE source_table='binding_run_pins_v2'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pin, "ambiguous");
    let malformed: String = db
        .query_row(
            "SELECT classification FROM legacy_authority_inputs_v2
             WHERE source_table='binding_revisions_v2' AND source_identity='binding-malformed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(malformed, "malformed");
    let missing_reference: String = db
        .query_row(
            "SELECT classification FROM legacy_authority_inputs_v2
             WHERE source_table='binding_revisions_v2' AND source_identity='binding-revoked'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(missing_reference, "ambiguous");

    let conflicting_hashes: (String, String) = db
        .query_row(
            "SELECT
               (SELECT source_artifact_hash FROM legacy_authority_inputs_v2
                WHERE source_table='binding_attestations_v2' AND source_identity='binding-known'),
               (SELECT source_artifact_hash FROM legacy_authority_inputs_v2
                WHERE source_table='v2_host_records' AND source_identity='binding-known')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(conflicting_hashes.0, "attestation-conflicting-hash");
    assert_eq!(conflicting_hashes.1, "host-conflicting-hash");

    let deployment_bindings: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM legacy_authority_inputs_v2
             WHERE source_table='logical_bindings_v2'
               AND json_extract(source_metadata_json,'$.deployment_id')='deployment'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(deployment_bindings, 3);

    let corrected_rows: i64 = db
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM authority_model_cutovers_v2) +
               (SELECT COUNT(*) FROM authorization_observations_v2) +
               (SELECT COUNT(*) FROM provider_grant_lineages_v2) +
               (SELECT COUNT(*) FROM provider_grant_versions_v2) +
               (SELECT COUNT(*) FROM provider_grant_adoption_records_v2) +
               (SELECT COUNT(*) FROM connection_alias_records_v2) +
               (SELECT COUNT(*) FROM actor_connection_acl_records_v2) +
               (SELECT COUNT(*) FROM registry_decision_vectors_v2) +
               (SELECT COUNT(*) FROM ceiling_amendments_v2) +
               (SELECT COUNT(*) FROM corrected_binding_records_v2) +
               (SELECT COUNT(*) FROM legacy_attempt_inventories_v2) +
               (SELECT COUNT(*) FROM receipt_verification_keysets_v2) +
               (SELECT COUNT(*) FROM receipt_key_compromise_records_v2)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(corrected_rows, 0);

    db.execute_batch(MIGRATION).unwrap();
    let after_reapply: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM legacy_authority_inputs_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_reapply, 64);
}

#[test]
fn empty_0005_creates_only_additive_storage_and_no_d1_run_effect_ledger() {
    let db = Connection::open_in_memory().unwrap();
    apply_through_0005(&db);
    db.execute_batch(MIGRATION).unwrap();
    db.execute_batch(MIGRATION).unwrap();

    let marker: (i64, String, String) = db
        .query_row(
            "SELECT version, authority_model_revision, legacy_default_disposition
             FROM lifecycle_separated_schema_v2",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        marker,
        (6, "lifecycle-separated-1".into(), "quarantined".into())
    );
    let inventory: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM legacy_authority_inputs_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(inventory, 0);

    let added_tables: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND
             (name='lifecycle_separated_schema_v2' OR name IN (
               'authority_model_cutovers_v2','authorization_observations_v2',
               'provider_grant_lineages_v2','provider_grant_versions_v2',
               'provider_grant_adoption_records_v2','connection_alias_records_v2',
               'actor_connection_acl_records_v2','registry_decision_vectors_v2',
               'ceiling_amendments_v2','corrected_binding_records_v2',
               'legacy_authority_inputs_v2','legacy_attempt_inventories_v2',
               'receipt_verification_keysets_v2','receipt_key_compromise_records_v2'))",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(added_tables, 15);
    let projection_triggers: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='trigger' AND name LIKE '%_v2_projection'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(projection_triggers, 13);

    let forbidden_columns: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('authority_model_cutovers_v2')
             WHERE name LIKE '%run%' OR name LIKE '%effect%' OR name LIKE 'current_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(forbidden_columns, 0);
}

#[test]
fn late_partial_application_withholds_completion_until_idempotent_full_rerun() {
    let db = Connection::open_in_memory().unwrap();
    apply_through_0005(&db);
    populate_live_shape(&db);
    let prefix = MIGRATION
        .split("-- D1's migration runner should apply the file atomically.")
        .next()
        .unwrap();
    let forced_failure = format!("{prefix}\nSELECT * FROM forced_missing_table;");
    assert!(db.execute_batch(&forced_failure).is_err());

    let marker_rows: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM lifecycle_separated_schema_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(marker_rows, 0);
    let corrected_rows: i64 = db
        .query_row(
            "SELECT (SELECT COUNT(*) FROM corrected_binding_records_v2) +
                    (SELECT COUNT(*) FROM provider_grant_versions_v2) +
                    (SELECT COUNT(*) FROM registry_decision_vectors_v2)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(corrected_rows, 0);

    db.execute_batch(MIGRATION).unwrap();
    let completed: (i64, i64) = db
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM lifecycle_separated_schema_v2),
               (SELECT COUNT(*) FROM legacy_authority_inputs_v2)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(completed, (1, 64));
}

#[test]
fn corrected_storage_rejects_projection_drift_and_cross_tenant_references() {
    let db = Connection::open_in_memory().unwrap();
    apply_through_0005(&db);
    db.execute_batch(MIGRATION).unwrap();

    assert!(
        db.execute(
            "INSERT INTO authority_model_cutovers_v2
             (tenant_id,deployment_id,cutover_epoch,control_epoch,artifact_hash,
              canonical_artifact_json,authority_model_revision,recorded_at)
             VALUES ('tenant-a','deployment',1,1,'hash-empty','{}','lifecycle-separated-1',1)",
            [],
        )
        .is_err()
    );
    assert!(
        db.execute(
            r#"INSERT INTO authority_model_cutovers_v2
               (tenant_id,deployment_id,cutover_epoch,control_epoch,artifact_hash,
                canonical_artifact_json,authority_model_revision,recorded_at)
               VALUES ('tenant-a','deployment',1,1,'hash-mismatch',
                '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"AuthorityModelCutover","tenant_id":"tenant-other","deployment_id":"deployment","cutover_epoch":1,"control_epoch":1}',
                'lifecycle-separated-1',1)"#,
            [],
        )
        .is_err()
    );
    db.execute(
        r#"INSERT INTO authority_model_cutovers_v2
           (tenant_id,deployment_id,cutover_epoch,control_epoch,artifact_hash,
            canonical_artifact_json,authority_model_revision,recorded_at)
           VALUES ('tenant-a','deployment',1,1,'hash-valid',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"AuthorityModelCutover","tenant_id":"tenant-a","deployment_id":"deployment","cutover_epoch":1,"control_epoch":1}',
            'lifecycle-separated-1',1)"#,
        [],
    )
    .unwrap();

    db.execute(
        r#"INSERT INTO provider_grant_lineages_v2
           (tenant_id,provider_grant_lineage_ref,provider,auth_profile_ref,
            account_subject_commitment,canonical_record_json,authority_model_revision,created_at)
           VALUES ('tenant-a','lineage','google','profile','account',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","record_type":"ProviderGrantLineage","tenant_id":"tenant-a","provider_grant_lineage_ref":"lineage","provider":"google","auth_profile_ref":"profile","account_subject_commitment":"account"}',
            'lifecycle-separated-1',1)"#,
        [],
    )
    .unwrap();
    db.execute(
        r#"INSERT INTO provider_grant_versions_v2
           (tenant_id,provider_grant_version_ref,provider_grant_lineage_ref,
            provider,auth_profile_ref,account_subject_commitment,provider_authority_epoch,
            source_observation_ref,artifact_hash,canonical_artifact_json,
            authority_model_revision,recorded_at)
           VALUES ('tenant-a','grant','lineage','google','profile','account',1,
            'observation','grant-hash',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"ProviderGrantVersion","tenant_id":"tenant-a","provider_grant_version_ref":"grant","provider_grant_lineage_ref":"lineage","provider":"google","auth_profile_ref":"profile","account_subject_commitment":"account","provider_authority_epoch":1,"source_observation_ref":"observation"}',
            'lifecycle-separated-1',1)"#,
        [],
    )
    .unwrap();
    db.execute(
        r#"INSERT INTO provider_grant_adoption_records_v2
           (tenant_id,adoption_ref,provider_grant_version_ref,provider_grant_lineage_ref,
            account_subject_commitment,artifact_hash,canonical_artifact_json,
            authority_model_revision,recorded_at)
           VALUES ('tenant-a','adoption','grant','lineage','account','adoption-hash',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"ProviderGrantAdoptionRecord","tenant_id":"tenant-a","adoption_ref":"adoption","provider_grant_version_ref":"grant","provider_grant_lineage_ref":"lineage","account_subject_commitment":"account"}',
            'lifecycle-separated-1',1)"#,
        [],
    )
    .unwrap();

    for statement in [
        r#"INSERT INTO provider_grant_adoption_records_v2
           (tenant_id,adoption_ref,provider_grant_version_ref,provider_grant_lineage_ref,
            account_subject_commitment,artifact_hash,canonical_artifact_json,
            authority_model_revision,recorded_at)
           VALUES ('tenant-b','adoption-cross','grant','lineage','account','adoption-cross-hash',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"ProviderGrantAdoptionRecord","tenant_id":"tenant-b","adoption_ref":"adoption-cross","provider_grant_version_ref":"grant","provider_grant_lineage_ref":"lineage","account_subject_commitment":"account"}',
            'lifecycle-separated-1',1)"#,
        r#"INSERT INTO connection_alias_records_v2
           (tenant_id,connection_alias,alias_epoch,provider_grant_version_ref,provider,
            auth_profile_ref,account_subject_commitment,artifact_hash,canonical_artifact_json,
            authority_model_revision,recorded_at)
           VALUES ('tenant-b','alias',1,'grant','google','profile','account','alias-hash',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"ConnectionAliasRecord","tenant_id":"tenant-b","connection_alias":"alias","alias_epoch":1,"current_provider_grant_version_ref":"grant","provider":"google","auth_profile_ref":"profile","account_subject_commitment":"account"}',
            'lifecycle-separated-1',1)"#,
        r#"INSERT INTO actor_connection_acl_records_v2
           (tenant_id,deployment_id,actor_subject_commitment,provider_grant_version_ref,
            account_subject_commitment,acl_epoch,selector_hash,record_commitment,record_hash,
            canonical_record_json,authority_model_revision,recorded_at)
           VALUES ('tenant-b','deployment','actor','grant','account',1,'selector','record-commitment','record-hash',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","record_type":"ActorConnectionAcl","tenant_id":"tenant-b","deployment_id":"deployment","actor_subject_commitment":"actor","provider_grant_version_ref":"grant","account_subject_commitment":"account","acl_epoch":1,"selector_hash":"selector","record_commitment":"record-commitment","record_hash":"record-hash"}',
            'lifecycle-separated-1',1)"#,
        r#"INSERT INTO corrected_binding_records_v2
           (tenant_id,deployment_id,binding_ref,provider_grant_version_ref,
            provider_grant_lineage_ref,account_subject_commitment,artifact_hash,
            canonical_artifact_json,authority_model_revision,recorded_at)
           VALUES ('tenant-b','deployment','binding','grant','lineage','account','binding-hash',
            '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"CorrectedBindingAttestation","tenant_id":"tenant-b","deployment_id":"deployment","binding_ref":"binding","provider_grant_version_ref":"grant","provider_grant_lineage_ref":"lineage","account_subject_commitment":"account"}',
            'lifecycle-separated-1',1)"#,
    ] {
        assert!(
            db.execute(statement, []).is_err(),
            "cross-tenant insert accepted"
        );
    }

    assert!(
        db.execute(
            r#"INSERT INTO connection_alias_records_v2
               (tenant_id,connection_alias,alias_epoch,provider_grant_version_ref,provider,
                auth_profile_ref,account_subject_commitment,artifact_hash,canonical_artifact_json,
                authority_model_revision,recorded_at)
               VALUES ('tenant-a','alias-wrong-account',1,'grant','google','profile','other-account','alias-wrong-hash',
                '{"schema_version":"0.2","authority_model_revision":"lifecycle-separated-1","artifact_type":"ConnectionAliasRecord","tenant_id":"tenant-a","connection_alias":"alias-wrong-account","alias_epoch":1,"current_provider_grant_version_ref":"grant","provider":"google","auth_profile_ref":"profile","account_subject_commitment":"other-account"}',
                'lifecycle-separated-1',1)"#,
            [],
        )
        .is_err()
    );

    for table in [
        "provider_grant_adoption_records_v2",
        "connection_alias_records_v2",
        "actor_connection_acl_records_v2",
        "corrected_binding_records_v2",
    ] {
        let tenant_fk_columns: i64 = db
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM pragma_foreign_key_list('{table}') WHERE \"from\"='tenant_id'"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            tenant_fk_columns > 0,
            "missing tenant-qualified FK on {table}"
        );
    }

    assert!(
        db.execute(
            "UPDATE authority_model_cutovers_v2 SET artifact_hash='changed'",
            [],
        )
        .is_err()
    );
    assert!(
        db.execute("DELETE FROM authority_model_cutovers_v2", [])
            .is_err()
    );
}
