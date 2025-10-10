#[test]
fn ui_failures() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/node_missing_name.rs");
    t.compile_fail("tests/ui/node_invalid_effect.rs");
    t.compile_fail("tests/ui/workflow_duplicate_alias.rs");
}
