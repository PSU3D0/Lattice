use connector_formualizer_sheetport::ops::SheetPortEvaluate;
use connector_formualizer_sheetport::types::{
    ManifestSourceRef, SheetPortEvaluateInput, SheetPortInputPayload, SheetPortModelSelector,
    WorkbookSourceRef,
};

fn canonical_bound_request() -> SheetPortEvaluateInput {
    SheetPortEvaluateInput {
        model: None,
        inputs: SheetPortInputPayload::default(),
        eval: None,
        emit_debug_artifacts: false,
    }
}

fn explicit_late_bound_request() -> SheetPortEvaluateInput {
    SheetPortEvaluateInput {
        model: Some(SheetPortModelSelector::LateBoundSources {
            workbook_source: WorkbookSourceRef::Blob {
                key: "tenant-a/models/quote.xlsx".to_string(),
            },
            manifest_source: ManifestSourceRef::InlineYaml {
                value: "spec: fio\nspec_version: \"0.3.0\"\nmanifest:\n  id: quote-model\n  name: Quote Model\nports: []\n"
                    .to_string(),
            },
            eval_defaults: None,
        }),
        inputs: SheetPortInputPayload::default(),
        eval: None,
        emit_debug_artifacts: false,
    }
}

fn main() {
    // Canonical topology-visible usage: deployment-bound/default mode.
    let bound = canonical_bound_request();

    // Advanced internal-op usage: explicit late-bound typed refs.
    let late_bound = explicit_late_bound_request();

    println!(
        "{} supports bound/default mode and explicit late-bound typed refs",
        SheetPortEvaluate::META.operation_id
    );
    println!("bound selector present? {}", bound.model.is_some());
    println!(
        "late-bound selector present? {}",
        late_bound.model.is_some()
    );
}
