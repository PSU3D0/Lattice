use dag_macros::flow_enum;
use schemars::schema_for;

#[flow_enum]
pub enum PaymentResult {
    Ok {
        receipt_id: String,
        amount_cents: i64,
    },
    Failed {
        reason: String,
    },
}

#[test]
fn flow_enum_supports_serialisation_and_schema() {
    let ok = PaymentResult::Ok {
        receipt_id: "rcpt_123".into(),
        amount_cents: 5000,
    };
    let ok_json = serde_json::to_value(&ok).expect("serialises");
    assert_eq!(ok_json["type"], "Ok");
    assert_eq!(ok_json["receipt_id"], "rcpt_123");
    assert_eq!(ok_json["amount_cents"], 5000);

    let failed = PaymentResult::Failed {
        reason: "card_declined".into(),
    };
    let failed_json = serde_json::to_value(&failed).expect("serialises");
    assert_eq!(failed_json["type"], "Failed");
    assert_eq!(failed_json["reason"], "card_declined");

    // Ensure JsonSchema derivation succeeds and produces tagged representation.
    let schema = schema_for!(PaymentResult);
    let schema_json = serde_json::to_value(&schema).expect("schema serialises");
    let type_prop = &schema_json["oneOf"][0]["properties"]["type"];
    assert!(
        type_prop.is_object(),
        "expected `type` property to exist in schema: {type_prop:?}"
    );
    assert_eq!(
        type_prop["enum"][0], "Ok",
        "first enum variant should be present in tagged schema"
    );
}
