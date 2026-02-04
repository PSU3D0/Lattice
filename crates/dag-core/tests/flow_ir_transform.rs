use dag_core::{EdgeIR, EdgeTransformKind};

#[test]
fn test_edge_transform_roundtrips_when_into_transform_present() {
    let json = r#"{
        "from": "a",
        "to": "b",
        "delivery": "at_least_once",
        "ordering": "ordered",
        "buffer": {},
        "transform": { "kind": "into" }
    }"#;

    let edge: EdgeIR = serde_json::from_str(json).unwrap();
    assert_eq!(
        edge.transform.as_ref().unwrap().kind,
        EdgeTransformKind::Into
    );

    let serialized = serde_json::to_string(&edge).unwrap();
    let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(
        value
            .get("transform")
            .and_then(|transform| transform.get("kind"))
            .and_then(|kind| kind.as_str()),
        Some("into")
    );
}
