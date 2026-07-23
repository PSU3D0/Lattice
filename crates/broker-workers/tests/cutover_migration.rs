use rusqlite::{Connection, params};

fn apply(connection: &Connection, sql: &str) {
    connection.execute_batch(sql).expect("migration applies");
}

#[test]
fn current_google_connection_is_quarantined_and_staged_without_scope_shaped_v2_columns() {
    let db = Connection::open_in_memory().unwrap();
    apply(&db, include_str!("../migrations/0001_broker.sql"));
    apply(
        &db,
        include_str!("../migrations/0002_credential_plane_v2.sql"),
    );
    db.execute(
        "INSERT INTO connections (org_id, connection_ref, intent_ref, connector_ref, auth_profile_ref, execution_lane, custody, account_commitment, actual_scopes_json, refresh_do_route, revocation_epoch, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'active')",
        params![
            "org-cutover", "connection-cutover", "intent-cutover", "connector.google.workspace@1",
            "auth.google.workspace.oauth2@1", "semantic_broker", "hosted_broker",
            "hmac-sha256:commitment", "[\"private-claim\"]", "route-private"
        ],
    ).unwrap();
    let fence = r#"{"active_v2_generation":null,"cas_version":0,"critical_fields":[],"extensions":{},"fence_generation":0,"phase":"v1_authoritative","schema_version":"0.2","v1_leasing_disabled":false,"v2_lease_ever_issued":false,"v2_rotation_ever_started":false}"#;
    db.execute(
        "INSERT INTO credential_fences_v2 (org_id, connection_ref, phase, fence_generation, v2_lease_ever_issued, v2_rotation_ever_started, v1_leasing_disabled, active_v2_generation, cas_version, canonical_fence_json) VALUES (?, ?, 'v1_authoritative', 0, 0, 0, 0, NULL, 0, ?)",
        params!["org-cutover", "connection-cutover", fence],
    ).unwrap();

    apply(
        &db,
        include_str!("../migrations/0003_production_v2_cutover.sql"),
    );

    let staged: (String, String, String) = db.query_row(
        "SELECT profile_ref, profile_version, status FROM connections_v2 WHERE org_id = ? AND connection_ref = ?",
        params!["org-cutover", "connection-cutover"],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert_eq!(
        staged,
        (
            "auth.google.workspace.oauth2".into(),
            "1".into(),
            "reconciling".into()
        )
    );
    let legacy_scopes: String = db
        .query_row(
            "SELECT actual_scopes_json FROM connections_v1_quarantine WHERE org_id = ?",
            ["org-cutover"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(legacy_scopes, "[\"private-claim\"]");
    let v2_columns = db
        .prepare("PRAGMA table_info(connections_v2)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        v2_columns
            .iter()
            .all(|column| !column.contains("scope") && !column.contains("provider"))
    );
    let policy: (i64, i64) = db.query_row(
        "SELECT legacy_admission_enabled, historical_verification_enabled FROM executable_admission_policy_v2",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert_eq!(policy, (0, 1));
    let cutover: (String, i64) = db.query_row(
        "SELECT phase,cas_version FROM credential_cutover_state_v2 WHERE org_id=? AND connection_ref=?",
        params!["org-cutover", "connection-cutover"],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert_eq!(cutover, ("inventoried".into(), 0));
}

#[test]
fn authoritative_fence_survives_forward_only_schema_rebuild() {
    let db = Connection::open_in_memory().unwrap();
    apply(&db, include_str!("../migrations/0001_broker.sql"));
    apply(
        &db,
        include_str!("../migrations/0002_credential_plane_v2.sql"),
    );
    let active = r#"{"active_v2_generation":1,"cas_version":2,"critical_fields":[],"extensions":{},"fence_generation":2,"phase":"v2_authoritative","schema_version":"0.2","v1_leasing_disabled":true,"v2_lease_ever_issued":true,"v2_rotation_ever_started":false}"#;
    // Packet C2's restrictive table deliberately cannot represent this state.
    // Simulate the forward-fix drill by advancing its schema constraint before C5.
    db.execute_batch("ALTER TABLE credential_fences_v2 RENAME TO credential_fences_repair; CREATE TABLE credential_fences_v2 (org_id TEXT NOT NULL, connection_ref TEXT NOT NULL, phase TEXT NOT NULL, fence_generation INTEGER NOT NULL, v2_lease_ever_issued INTEGER NOT NULL, v2_rotation_ever_started INTEGER NOT NULL, v1_leasing_disabled INTEGER NOT NULL, active_v2_generation INTEGER, cas_version INTEGER NOT NULL, canonical_fence_json TEXT NOT NULL, PRIMARY KEY(org_id, connection_ref));").unwrap();
    db.execute(
        "INSERT INTO credential_fences_v2 VALUES (?, ?, 'v2_authoritative', 2, 1, 0, 1, 1, 2, ?)",
        params!["org", "connection", active],
    )
    .unwrap();
    apply(
        &db,
        include_str!("../migrations/0003_production_v2_cutover.sql"),
    );
    let fence: (String, i64, i64) = db
        .query_row(
            "SELECT phase, v1_leasing_disabled, active_v2_generation FROM credential_fences_v2",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(fence, ("v2_authoritative".into(), 1, 1));
    assert!(
        db.execute(
            "UPDATE credential_fences_v2 SET phase = 'v1_authoritative'",
            []
        )
        .is_err()
    );
}
