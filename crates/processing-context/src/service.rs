use async_trait::async_trait;
use capabilities::transform::{
    TransformBeginError, TransformBudgets, TransformErrorClass, TransformExecutionRecord,
    TransformFailure, TransformLease, TransformOutcome, TransformRuntime,
    TransformTerminationClass,
};
use std::sync::Arc;

use crate::{
    BeginError, BoundedRecordObserver, InitializationError, ProcessingBudgets, ProcessingContext,
    ProcessingFailure, ProcessingLease, ProcessingOutcome, ProcessingRuntime, PublicError,
    TerminationClass, TransformRecord,
};

#[derive(Clone)]
pub struct PdfTransformRuntime {
    context: ProcessingContext,
}

impl PdfTransformRuntime {
    pub fn new() -> Result<Self, InitializationError> {
        let runtime = ProcessingRuntime::new_pdf_host()?;
        let context = runtime.create_pdf_extract_context()?;
        Ok(Self { context })
    }

    #[cfg(test)]
    fn from_context(context: ProcessingContext) -> Self {
        Self { context }
    }

    #[doc(hidden)]
    pub fn install_test_observer(&self, observer: Arc<BoundedRecordObserver>) {
        self.context.install_test_observer(observer);
    }
}

impl TransformRuntime for PdfTransformRuntime {
    fn try_begin(
        &self,
        transform_id: &str,
    ) -> Result<Box<dyn TransformLease>, TransformBeginError> {
        let lease = self
            .context
            .try_begin(transform_id)
            .map_err(map_begin_error)?;
        lease.activate_for_adapter().map_err(map_begin_error)?;
        Ok(Box::new(PdfTransformLease { lease }))
    }
}

struct PdfTransformLease {
    lease: ProcessingLease,
}

#[async_trait]
impl TransformLease for PdfTransformLease {
    fn max_input_bytes(&self) -> u64 {
        self.lease.max_input_bytes()
    }

    async fn run(self: Box<Self>, input: Vec<u8>) -> Result<TransformOutcome, TransformFailure> {
        self.lease
            .run(input)
            .await
            .map(map_outcome)
            .map_err(map_failure)
    }
}

fn map_begin_error(error: BeginError) -> TransformBeginError {
    match error {
        BeginError::Busy => TransformBeginError::Busy,
        BeginError::InvalidTransform => TransformBeginError::InvalidTransform,
        BeginError::RuntimeUnavailable => TransformBeginError::RuntimeUnavailable,
    }
}

fn map_error(error: PublicError) -> TransformErrorClass {
    match error {
        PublicError::InvalidModule => TransformErrorClass::InvalidModule,
        PublicError::InvalidAbi => TransformErrorClass::InvalidAbi,
        PublicError::InputTooLarge => TransformErrorClass::InputTooLarge,
        PublicError::Busy => TransformErrorClass::Busy,
        PublicError::RuntimeUnavailable => TransformErrorClass::RuntimeUnavailable,
        PublicError::FuelExhausted => TransformErrorClass::FuelExhausted,
        PublicError::MemoryExhausted => TransformErrorClass::MemoryExhausted,
        PublicError::WallTimeExceeded => TransformErrorClass::WallTimeExceeded,
        PublicError::Cancelled => TransformErrorClass::Cancelled,
        PublicError::OutputTooLarge => TransformErrorClass::OutputTooLarge,
        PublicError::GuestFailed => TransformErrorClass::GuestFailed,
        PublicError::InvalidOutput => TransformErrorClass::InvalidOutput,
        PublicError::UnsupportedDocument => TransformErrorClass::UnsupportedDocument,
    }
}

fn map_termination(termination: TerminationClass) -> TransformTerminationClass {
    match termination {
        TerminationClass::Success => TransformTerminationClass::Success,
        TerminationClass::Failure(error) => TransformTerminationClass::Failure(map_error(error)),
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("supported native usize values fit transform provenance u64")
}

fn map_budgets(budgets: ProcessingBudgets) -> TransformBudgets {
    TransformBudgets {
        module_bytes: usize_to_u64(budgets.module_bytes),
        input_bytes: usize_to_u64(budgets.input_bytes),
        memory_bytes: usize_to_u64(budgets.memory_bytes),
        memories: usize_to_u64(budgets.memories),
        table_elements: budgets.table_elements,
        tables: usize_to_u64(budgets.tables),
        instances: usize_to_u64(budgets.instances),
        output_bytes: usize_to_u64(budgets.output_bytes),
        wall_time: budgets.wall_time,
        epoch_interval: budgets.epoch_interval,
        stale_heartbeat: budgets.stale_heartbeat,
        fuel: budgets.fuel,
        fuel_yield_interval: budgets.fuel_yield_interval,
        concurrency: usize_to_u64(budgets.concurrency),
    }
}

fn map_record(record: TransformRecord) -> TransformExecutionRecord {
    TransformExecutionRecord {
        transform_id: record.transform_id.to_string(),
        module_sha256: record.module_sha256,
        abi_version: record.abi_version.to_string(),
        runtime_version: record.runtime_version.to_string(),
        effective_budgets: map_budgets(record.effective_budgets),
        input_sha256: record.input_sha256,
        output_sha256: record.output_sha256,
        termination_class: map_termination(record.termination_class),
    }
}

fn map_outcome(outcome: ProcessingOutcome) -> TransformOutcome {
    TransformOutcome {
        output: outcome.output,
        record: map_record(outcome.record),
    }
}

fn map_failure(failure: ProcessingFailure) -> TransformFailure {
    let class = map_error(failure.public_error);
    TransformFailure::new(class, map_record(failure.record), failure.private_source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use capabilities::transform::TransformTerminationClass;

    #[test]
    fn public_error_mapping_is_exhaustive_and_stable() {
        let cases = [
            (PublicError::InvalidModule, "invalid_module"),
            (PublicError::InvalidAbi, "invalid_abi"),
            (PublicError::InputTooLarge, "input_too_large"),
            (PublicError::Busy, "busy"),
            (PublicError::RuntimeUnavailable, "runtime_unavailable"),
            (PublicError::FuelExhausted, "fuel_exhausted"),
            (PublicError::MemoryExhausted, "memory_exhausted"),
            (PublicError::WallTimeExceeded, "wall_time_exceeded"),
            (PublicError::Cancelled, "cancelled"),
            (PublicError::OutputTooLarge, "output_too_large"),
            (PublicError::GuestFailed, "guest_failed"),
            (PublicError::InvalidOutput, "invalid_output"),
            (PublicError::UnsupportedDocument, "unsupported_document"),
        ];
        for (input, expected) in cases {
            assert_eq!(map_error(input).as_str(), expected);
        }
    }

    #[tokio::test]
    async fn pdf_service_is_allowlisted_and_reports_effective_input_ceiling() {
        let service = PdfTransformRuntime::new().expect("PDF service");
        assert!(matches!(
            service.try_begin("author.selected.transform"),
            Err(TransformBeginError::InvalidTransform)
        ));
        let lease = service
            .try_begin(crate::PDF_EXTRACT_TRANSFORM_ID)
            .expect("checked transform admitted");
        assert_eq!(
            lease.max_input_bytes(),
            usize_to_u64(crate::pdf_extract_processing_budgets().input_bytes)
        );
    }

    #[tokio::test]
    async fn activated_pdf_leases_hold_both_slots_past_deadline_until_drop() {
        let mut policy = ProcessingBudgets::default();
        policy.wall_time = std::time::Duration::from_millis(15);
        policy.epoch_interval = std::time::Duration::from_millis(2);
        policy.stale_heartbeat = std::time::Duration::from_millis(250);
        policy.concurrency = 2;
        let host = ProcessingRuntime::new(policy.clone()).expect("runtime");
        let context = host
            .create_context(&crate::PDF_EXTRACT_MODULE, policy)
            .expect("PDF context");
        let service = PdfTransformRuntime::from_context(context);

        let first = service
            .try_begin(crate::PDF_EXTRACT_TRANSFORM_ID)
            .expect("first activated lease");
        let second = service
            .try_begin(crate::PDF_EXTRACT_TRANSFORM_ID)
            .expect("second activated lease");
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        assert!(matches!(
            service.try_begin(crate::PDF_EXTRACT_TRANSFORM_ID),
            Err(TransformBeginError::Busy)
        ));
        drop(first);
        let replacement = service
            .try_begin(crate::PDF_EXTRACT_TRANSFORM_ID)
            .expect("dropping one lease releases exactly one slot");
        assert!(matches!(
            service.try_begin(crate::PDF_EXTRACT_TRANSFORM_ID),
            Err(TransformBeginError::Busy)
        ));
        drop(second);
        let next = service
            .try_begin(crate::PDF_EXTRACT_TRANSFORM_ID)
            .expect("dropping second lease releases its slot");
        drop((replacement, next));
    }

    #[test]
    fn record_mapping_preserves_all_public_metadata() {
        let budgets = crate::pdf_extract_processing_budgets();
        let source = TransformRecord {
            transform_id: crate::PDF_EXTRACT_TRANSFORM_ID,
            module_sha256: [7; 32],
            abi_version: crate::ABI_VERSION,
            runtime_version: crate::RUNTIME_VERSION,
            effective_budgets: budgets.clone(),
            input_sha256: Some([8; 32]),
            output_sha256: Some([9; 32]),
            termination_class: TerminationClass::Failure(PublicError::InvalidOutput),
        };
        let mapped = map_record(source);
        assert_eq!(mapped.transform_id, crate::PDF_EXTRACT_TRANSFORM_ID);
        assert_eq!(mapped.module_sha256, [7; 32]);
        assert_eq!(mapped.abi_version, crate::ABI_VERSION);
        assert_eq!(mapped.runtime_version, crate::RUNTIME_VERSION);
        assert_eq!(mapped.effective_budgets, map_budgets(budgets));
        assert_eq!(mapped.input_sha256, Some([8; 32]));
        assert_eq!(mapped.output_sha256, Some([9; 32]));
        assert_eq!(
            mapped.termination_class,
            TransformTerminationClass::Failure(TransformErrorClass::InvalidOutput)
        );
    }
}
