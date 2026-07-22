use rusqlite::Connection;

#[test]
fn credential_plane_migration_is_idempotent_and_keeps_v1_authoritative() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(include_str!("../migrations/0001_broker.sql"))
        .unwrap();
    let migration = include_str!("../migrations/0002_credential_plane_v2.sql");
    db.execute_batch(migration).unwrap();
    db.execute_batch(migration).unwrap();

    let version: i64 = db
        .query_row(
            "SELECT version FROM credential_plane_schema_v2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 2);
    let v1_version: i64 = db
        .query_row("SELECT version FROM broker_schema", [], |row| row.get(0))
        .unwrap();
    assert_eq!(v1_version, 1);

    db.execute(
        "INSERT INTO credential_fences_v2 (
          org_id, connection_ref, fence_generation, cas_version, canonical_fence_json
        ) VALUES ('org-a', 'connection-a', 0, 0, '{}')",
        [],
    )
    .unwrap();
    assert!(
        db.execute(
            "UPDATE credential_fences_v2 SET v2_lease_ever_issued = 1
             WHERE org_id = 'org-a' AND connection_ref = 'connection-a'",
            [],
        )
        .is_err()
    );
}
