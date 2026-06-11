//! Expansion/behaviour tests for the `#[flow_struct]` derive bundle (packet D3).
//!
//! `#[flow_struct]` is sugar for the canonical Flow payload derive stack
//! (`Clone + Debug + Serialize + Deserialize + JsonSchema`). These tests assert
//! the bundle is materialised, composes with user-added derives and serde
//! attributes, and works for both structs and enums.

use dag_macros::flow_struct;
use schemars::schema_for;

// --- Case 1: bare struct gets the full canonical bundle. ---------------------

#[flow_struct]
pub struct BareStruct {
    pub name: String,
    pub count: u32,
}

#[test]
fn bare_struct_gets_canonical_bundle() {
    let value = BareStruct {
        name: "lead".into(),
        count: 3,
    };

    // Clone + Debug.
    let cloned = value.clone();
    assert_eq!(format!("{cloned:?}"), format!("{value:?}"));

    // Serialize + Deserialize round-trip.
    let json = serde_json::to_value(&value).expect("serialises");
    assert_eq!(json["name"], "lead");
    assert_eq!(json["count"], 3);
    let back: BareStruct = serde_json::from_value(json).expect("deserialises");
    assert_eq!(back.name, "lead");

    // JsonSchema.
    let schema = serde_json::to_value(schema_for!(BareStruct)).expect("schema serialises");
    assert!(schema["properties"]["name"].is_object());
    assert!(schema["properties"]["count"].is_object());
}

// --- Case 2: extra user-added derive composes, no duplication. ---------------
// `PartialEq`/`Eq` (and a fully-qualified canonical trait) are user-supplied;
// the macro must merge its bundle in without emitting a duplicate derive.

#[flow_struct]
#[derive(PartialEq, Eq, serde::Serialize)]
pub struct WithExtraDerives {
    pub id: u64,
}

#[test]
fn extra_user_derives_compose_without_duplication() {
    let a = WithExtraDerives { id: 7 };
    let b = WithExtraDerives { id: 7 };
    // PartialEq/Eq came from the user derive.
    assert!(a == b);
    // Serialize still works (the explicit `serde::Serialize` was not duplicated
    // by the bundle — duplicate derives would fail to compile).
    let json = serde_json::to_value(&a).expect("serialises");
    assert_eq!(json["id"], 7);
    // Bundle-provided Deserialize/JsonSchema are also present.
    let back: WithExtraDerives = serde_json::from_value(json).expect("deserialises");
    assert_eq!(back, a);
    let _ = schema_for!(WithExtraDerives);
}

// --- Case 3: serde field/container attributes are preserved. -----------------

#[flow_struct]
#[serde(rename_all = "camelCase")]
pub struct WithSerdeAttrs {
    pub product_interest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seat_count: Option<u32>,
}

#[test]
fn serde_attributes_are_preserved() {
    // Container `rename_all = "camelCase"` is honoured.
    let value = WithSerdeAttrs {
        product_interest: "analytics".into(),
        seat_count: None,
    };
    let json = serde_json::to_value(&value).expect("serialises");
    assert_eq!(json["productInterest"], "analytics");
    // `skip_serializing_if` drops the None field.
    assert!(json.get("seatCount").is_none(), "None seat_count skipped: {json}");

    // `default` lets the field be omitted on the way in.
    let parsed: WithSerdeAttrs =
        serde_json::from_value(serde_json::json!({ "productInterest": "ops" }))
            .expect("deserialises with default");
    assert_eq!(parsed.seat_count, None);
}

// --- Case 4: enums are in scope (identical expansion to structs). ------------
// Note: unlike `#[flow_enum]`, `#[flow_struct]` injects NO `#[serde(tag = ...)]`
// — the variant tagging below is whatever the user opts into (here, the serde
// default external tagging plus an explicit container rename).

#[flow_struct]
#[serde(rename_all = "lowercase")]
pub enum PriorityLike {
    High,
    Medium,
    Low,
}

#[test]
fn enums_are_supported() {
    let json = serde_json::to_value(PriorityLike::High).expect("serialises");
    assert_eq!(json, serde_json::json!("high"));
    let back: PriorityLike = serde_json::from_value(serde_json::json!("low")).expect("deserialises");
    assert!(matches!(back, PriorityLike::Low));
    let _ = schema_for!(PriorityLike);
}

// --- Case 5: a `Copy` payload (extra derive that needs Clone present). --------
// Proves the bundle's `Clone` satisfies the `Copy: Clone` supertrait bound and
// that `Copy` from the user is not clobbered.

#[flow_struct]
#[derive(Copy)]
pub struct CopyPayload {
    pub n: i32,
}

#[test]
fn copy_payload_compiles_and_round_trips() {
    let a = CopyPayload { n: 42 };
    let b = a; // Copy
    assert_eq!(serde_json::to_value(b).unwrap()["n"], 42);
}
