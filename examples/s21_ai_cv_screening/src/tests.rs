use super::*;
use std::sync::Arc;

use capabilities::artifact::{Artifact, Handle, StoreRef};
use capabilities::kv::MemoryKv;
use capabilities::scoped::ScopedResources;
use capabilities::{ResourceAccess, ResourceBag, context};
use dag_core::EffectHint;
use dag_core::requirements::TriggerKind;
use kernel_plan::derive_requirements;

// ---- Fixtures --------------------------------------------------------------

fn application() -> CvApplication {
    let pdf = b"%PDF-1.4 test fixture";
    CvApplication {
        full_name: "Ada Lovelace".to_string(),
        email: "ada@applicant.test".to_string(),
        expectation: "5000-6000".to_string(),
        linkedin: "https://linkedin.test/in/ada".to_string(),
        cv_filename: "ada_lovelace_cv.pdf".to_string(),
        cv: Artifact {
            handle: Handle::host_mint_exact(
                StoreRef::new("workspace"),
                b"s21-test-root",
                "s21-test-root-id",
                "ingress/test.pdf",
            ),
            content_type: "application/pdf".to_string(),
            len: pdf.len() as u64,
            content_hash: Some(capabilities::artifact::sha256_hex(pdf)),
        },
    }
}

fn application_json(app: &CvApplication) -> serde_json::Value {
    serde_json::to_value(app).expect("serialize application")
}

const RATING_TEXT: &str = "Rating: 9/10. Strong match. Recommend an interview.";

// ---- Pure helpers ----------------------------------------------------------

#[test]
fn screening_key_is_scoped_to_flow_trigger_and_email() {
    assert_eq!(
        screening_key("ada@applicant.test"),
        "s21_ai_cv_screening_flow:screening_trigger:ada@applicant.test"
    );
}

#[test]
fn screening_prompt_embeds_role_and_resume_text() {
    let prompt = screening_prompt(&ExtractedCvApplication {
        application: application(),
        resume_text: "10 years building analytical engines and Rust services.".to_string(),
        page_count: 1,
    });
    assert!(prompt.contains(JOB_TITLE), "role present: {prompt}");
    assert!(
        prompt.contains("analytical engines"),
        "resume text present: {prompt}"
    );
}

#[test]
fn emails_are_pure_functions_of_the_submission() {
    let (c_subject, c_body) = confirmation_email(&application());
    assert_eq!(c_subject, "We received your application");
    assert!(
        c_body.contains("Ada Lovelace"),
        "candidate greeted: {c_body}"
    );

    let (hr_subject, hr_body) = hr_email(&application(), RATING_TEXT);
    assert!(
        hr_subject.contains(JOB_TITLE),
        "role in subject: {hr_subject}"
    );
    assert!(
        hr_body.contains("ada@applicant.test"),
        "email present: {hr_body}"
    );
    assert!(hr_body.contains(RATING_TEXT), "rating relayed: {hr_body}");
}

#[test]
fn candidate_row_uses_our_own_column_keys() {
    let row = candidate_row(&application(), RATING_TEXT);
    assert_eq!(row["full_name"], serde_json::json!("Ada Lovelace"));
    assert_eq!(row["email"], serde_json::json!("ada@applicant.test"));
    assert_eq!(row["cv_filename"], serde_json::json!("ada_lovelace_cv.pdf"));
    assert_eq!(row["ai_rating"], serde_json::json!(RATING_TEXT));
}

// ---- Flow validation + requirements ----------------------------------------

#[test]
fn flow_validates_and_derives_http_trigger_requirement() {
    let ir = validated_ir();
    let requirements = derive_requirements(&ir);

    let trigger = requirements
        .triggers
        .iter()
        .find(|t| t.alias == TRIGGER_ALIAS)
        .expect("screening trigger requirement present");
    assert_eq!(trigger.kind, TriggerKind::Http);

    let entrypoint = requirements
        .entrypoints
        .iter()
        .find(|e| e.trigger_alias == TRIGGER_ALIAS)
        .expect("http entrypoint requirement present");
    assert_eq!(entrypoint.capture_alias, "capture");
    assert_eq!(entrypoint.method.as_deref(), Some("POST"));
    assert_eq!(entrypoint.route_path.as_deref(), Some("/cv-screening"));
}

#[test]
fn public_boundary_requires_exact_cv_artifact_and_has_no_resume_text() {
    let value = application_json(&application());
    assert!(value.get("cv").is_some());
    assert!(value.get("resume_text").is_none());
    let mut legacy = value;
    legacy.as_object_mut().unwrap().remove("cv");
    legacy["resume_text"] = serde_json::json!("legacy text");
    assert!(serde_json::from_value::<CvApplication>(legacy).is_err());
}

#[test]
fn flow_shape_declares_honest_effects_per_node() {
    let ir = flow();

    let hints = |alias: &str| -> Vec<String> {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .unwrap_or_else(|| panic!("node `{alias}` present"))
            .effect_hints
            .clone()
    };

    assert!(hints("extract_cv_text").contains(&EffectHint::WorkspaceRead.as_str().to_string()));

    // LLM completion: http_write only (POST rides the write capability).
    assert!(hints("rate_candidate").contains(&EffectHint::HttpWrite.as_str().to_string()));
    assert!(!hints("rate_candidate").contains(&EffectHint::HttpRead.as_str().to_string()));
    // Sheets append reads the header row then writes: http_read + http_write.
    assert!(hints("record_candidate").contains(&EffectHint::HttpRead.as_str().to_string()));
    assert!(hints("record_candidate").contains(&EffectHint::HttpWrite.as_str().to_string()));
    // Both gmail sends: http_write only.
    for alias in ["confirm_candidate", "notify_hr"] {
        assert!(hints(alias).contains(&EffectHint::HttpWrite.as_str().to_string()));
        assert!(!hints(alias).contains(&EffectHint::HttpRead.as_str().to_string()));
    }
    // Terminal KV upsert.
    assert!(hints("record_screening").contains(&EffectHint::KvWrite.as_str().to_string()));

    // Every connector node carries its connector-prefixed op identifier so the
    // runtime can infer the connection scope.
    let identifier = |alias: &str| {
        ir.nodes
            .iter()
            .find(|n| n.alias == alias)
            .map(|n| n.identifier.clone())
            .expect("node present")
    };
    assert!(
        identifier("extract_cv_text").contains("pdf_extraction"),
        "the extraction handler must remain owned by the S21 example"
    );
    let extract = ir
        .nodes
        .iter()
        .find(|node| node.alias == "extract_cv_text")
        .expect("extract node present");
    assert_eq!(extract.implementation_dependencies.len(), 1);
    assert_eq!(
        extract.implementation_dependencies[0].kind,
        dag_core::ImplementationDependencyKind::SandboxedTransform
    );
    assert_eq!(
        extract.implementation_dependencies[0].key,
        crate::pdf_extraction::PDF_EXTRACT_TRANSFORM_ID
    );
    assert_eq!(identifier("rate_candidate"), "connector.llm.rate_candidate");
    assert_eq!(
        identifier("record_candidate"),
        "connector.google.sheets.record_candidate"
    );
    assert_eq!(
        identifier("confirm_candidate"),
        "connector.google.gmail.confirm_candidate"
    );
    assert_eq!(identifier("notify_hr"), "connector.google.gmail.notify_hr");
}

// ---- record_screening honesty + idempotency (s18 pattern) ------------------

fn kv_bag() -> (Arc<MemoryKv>, Arc<dyn ResourceAccess>) {
    let kv = Arc::new(MemoryKv::new());
    let bag: Arc<dyn ResourceAccess> = Arc::new(ResourceBag::default().with_kv(kv.clone()));
    (kv, bag)
}

fn sample_notified(email: &str) -> NotifiedScreening {
    let mut app = application();
    app.email = email.to_string();
    NotifiedScreening {
        application: app,
        ai_rating: RATING_TEXT.to_string(),
        appended_range: "'Candidates'!A2:F2".to_string(),
        candidate_message_id: "msg-candidate".to_string(),
        hr_message_id: "msg-hr".to_string(),
    }
}

#[tokio::test]
async fn record_screening_succeeds_under_exactly_declared_kv_hints() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_screening",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let record = context::with_resources(view, async {
        record_screening(sample_notified("ada@applicant.test"))
            .await
            .expect("write succeeds under declared hints")
    })
    .await;

    assert!(record.stored);
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn record_screening_denied_without_kv_grant() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new("record_screening", bag, []));
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        record_screening(sample_notified("ada@applicant.test"))
            .await
            .expect_err("write must fail when kv is not granted")
    })
    .await;
    assert!(
        err.to_string().contains("KV capability"),
        "expected a missing-kv error, got: {err}"
    );

    let denials = scoped.take_denials();
    assert!(
        denials.iter().any(|d| d.capability == "kv"),
        "expected a CAP110 kv denial, got: {denials:?}"
    );
}

#[tokio::test]
async fn record_screening_is_idempotent_on_email() {
    let (_kv, bag) = kv_bag();
    let scoped = Arc::new(ScopedResources::new(
        "record_screening",
        bag,
        [EffectHint::KvRead, EffectHint::KvWrite],
    ));

    let first = context::with_resources(scoped.clone(), async {
        record_screening(sample_notified("ada@applicant.test"))
            .await
            .expect("first")
    })
    .await;
    let second = context::with_resources(scoped.clone(), async {
        record_screening(sample_notified("ada@applicant.test"))
            .await
            .expect("redelivery")
    })
    .await;

    assert!(first.stored, "first delivery writes the record");
    assert!(!second.stored, "redelivery is deduplicated");
    assert_eq!(first.key, second.key);

    let other = context::with_resources(scoped.clone(), async {
        record_screening(sample_notified("grace@applicant.test"))
            .await
            .expect("a distinct applicant")
    })
    .await;
    assert!(other.stored, "a distinct applicant writes its own record");
    assert_ne!(first.key, other.key);
}
