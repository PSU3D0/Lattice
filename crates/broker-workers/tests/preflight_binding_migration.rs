use std::sync::{Arc, Barrier};

use rusqlite::Connection;

#[test]
fn concurrent_first_install_has_one_logical_identity_and_revocation_is_terminal() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let db = Connection::open(file.path()).unwrap();
    db.execute_batch(include_str!("../migrations/0001_broker.sql"))
        .unwrap();
    db.execute_batch(include_str!("../migrations/0002_credential_plane_v2.sql"))
        .unwrap();
    db.execute_batch(include_str!("../migrations/0003_production_v2_cutover.sql"))
        .unwrap();
    db.execute_batch(include_str!("../migrations/0005_preflight_binding.sql"))
        .unwrap();
    db.execute(
        "INSERT INTO connections_v2(org_id,connection_ref,profile_ref,profile_version,status) VALUES('org','connection','profile','1','active')",
        [],
    )
    .unwrap();
    drop(db);

    let barrier = Arc::new(Barrier::new(8));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let path = file.path().to_owned();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let db = Connection::open(path).unwrap();
            db.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
            barrier.wait();
            db.execute(
                "INSERT OR IGNORE INTO logical_bindings_v2(org_id,logical_binding_ref,identity_hash,connection_ref,deployment_id,state,current_binding_ref,created_at,updated_at) VALUES('org','logical_binding_v2_same','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','connection','deployment','active',NULL,1,1)",
                [],
            )
            .unwrap();
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }

    let db = Connection::open(file.path()).unwrap();
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM logical_bindings_v2", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
    db.execute_batch(
        "UPDATE logical_bindings_v2 SET current_binding_ref='binding_v2_a' WHERE org_id='org';
         INSERT INTO binding_revisions_v2(org_id,binding_ref,logical_binding_ref,revision_hash,binding_hash,canonical_binding_json,authority_view_hash,material_generation,state,created_at) VALUES('org','binding_v2_a','logical_binding_v2_same','revision-a','binding-a','{}','authority-a',1,'active',1);
         INSERT INTO binding_run_pins_v2(org_id,deployment_id,run_id,logical_binding_ref,binding_ref,created_at) VALUES('org','deployment','run','logical_binding_v2_same','binding_v2_a',1);
         INSERT OR IGNORE INTO binding_run_pins_v2(org_id,deployment_id,run_id,logical_binding_ref,binding_ref,created_at) VALUES('org','deployment','run','logical_binding_v2_same','binding_v2_b',2);",
    )
    .unwrap();
    let pinned: String = db
        .query_row("SELECT binding_ref FROM binding_run_pins_v2", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(pinned, "binding_v2_a");
    db.execute(
        "UPDATE logical_bindings_v2 SET state='revoked' WHERE org_id='org'",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT OR IGNORE INTO logical_bindings_v2(org_id,logical_binding_ref,identity_hash,connection_ref,deployment_id,state,current_binding_ref,created_at,updated_at) VALUES('org','logical_binding_v2_same','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','connection','deployment','active',NULL,2,2)",
        [],
    )
    .unwrap();
    let state: String = db
        .query_row("SELECT state FROM logical_bindings_v2", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(state, "revoked");
}

#[test]
fn populated_0005_known_shape_is_evidence_only_not_cutover_authority() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(include_str!("../migrations/0001_broker.sql"))
        .unwrap();
    db.execute_batch(include_str!("../migrations/0002_credential_plane_v2.sql"))
        .unwrap();
    db.execute_batch(include_str!("../migrations/0003_production_v2_cutover.sql"))
        .unwrap();
    db.execute_batch(include_str!("../migrations/0005_preflight_binding.sql"))
        .unwrap();
    db.execute(
        "INSERT INTO connections_v2(org_id,connection_ref,profile_ref,profile_version,status) VALUES('org','connection','profile','1','active')",
        [],
    )
    .unwrap();
    db.execute_batch(
        "INSERT INTO logical_bindings_v2(org_id,logical_binding_ref,identity_hash,connection_ref,deployment_id,state,current_binding_ref,created_at,updated_at)
           VALUES('org','logical_binding_v2_known','identity-known','connection','deployment','active','binding_v2_known',1,1);
         INSERT INTO binding_revisions_v2(org_id,binding_ref,logical_binding_ref,revision_hash,binding_hash,canonical_binding_json,authority_view_hash,material_generation,state,created_at)
           VALUES('org','binding_v2_known','logical_binding_v2_known','revision-known','binding-known','{\"schema_version\":\"0.2\"}','authority-known',1,'active',1);",
    )
    .unwrap();

    let shape: (i64, i64, i64, i64) = db
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM logical_bindings_v2),
               (SELECT COUNT(*) FROM binding_revisions_v2),
               (SELECT COUNT(*) FROM binding_run_pins_v2),
               (SELECT COUNT(*) FROM v2_invocation_outbox)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(shape, (1, 1, 0, 0));

    let cutover_authority_rows: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM credential_cutover_state_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cutover_authority_rows, 0);
}
