#[test]
fn ui_failures() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/node_missing_name.rs");
    t.compile_fail("tests/ui/node_invalid_effect.rs");
    t.compile_fail("tests/ui/node_effect_hint_conflict.rs");
    t.compile_fail("tests/ui/node_determinism_hint_conflict.rs");
    t.compile_fail("tests/ui/node_db_effect_conflict.rs");
    t.compile_fail("tests/ui/node_queue_effect_conflict.rs");
    t.compile_fail("tests/ui/node_blob_determinism_conflict.rs");
    t.compile_fail("tests/ui/workflow_duplicate_alias.rs");
    t.compile_fail("tests/ui/flow_enum_not_enum.rs");
    t.compile_fail("tests/ui/workflow_unknown_alias.rs");
}
