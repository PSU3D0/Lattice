use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{sleep, timeout};

const NORMAL: &str = r#"
(module
  (memory (export "memory") 1 2)
  (global $len (mut i32) (i32.const 0))
  (func (export "lf_alloc") (param i32) (result i32) i32.const 1024)
  (func (export "lf_transform") (param i32 i32) (result i32)
    local.get 1
    global.set $len
    i32.const 0)
  (func (export "lf_output_ptr") (result i32) i32.const 1024)
  (func (export "lf_output_len") (result i32) global.get $len))
"#;

fn descriptor(wat_source: &str) -> &'static ModuleDescriptor {
    let bytes: &'static [u8] = Box::leak(wat::parse_str(wat_source).unwrap().into_boxed_slice());
    Box::leak(Box::new(ModuleDescriptor::synthetic(
        "test.transform",
        sha256(bytes),
        bytes,
    )))
}

fn budgets() -> ProcessingBudgets {
    ProcessingBudgets {
        input_bytes: 1024,
        memory_bytes: 2 * 65_536,
        output_bytes: 64,
        wall_time: Duration::from_millis(300),
        epoch_interval: Duration::from_millis(5),
        stale_heartbeat: Duration::from_millis(40),
        fuel: 200_000,
        fuel_yield_interval: 1_000,
        concurrency: 1,
        ..ProcessingBudgets::default()
    }
}

fn new_context(
    descriptor: &'static ModuleDescriptor,
    policy: ProcessingBudgets,
) -> Result<ProcessingContext, InitializationError> {
    let host = ProcessingRuntime::new(policy.clone())?;
    host.create_context(descriptor, policy)
}

fn runtime(wat_source: &str) -> ProcessingContext {
    new_context(descriptor(wat_source), budgets()).unwrap()
}

async fn run(
    runtime: &ProcessingContext,
    input: &[u8],
) -> Result<ProcessingOutcome, ProcessingFailure> {
    runtime
        .try_begin("test.transform")
        .unwrap()
        .run(input.to_vec())
        .await
}

fn conditional_transform(body: &str, output_ptr: &str, output_len: &str) -> String {
    format!(
        r#"
(module
  (memory (export "memory") 1 2)
  (global $mode (mut i32) (i32.const 0))
  (func (export "lf_alloc") (param i32) (result i32) i32.const 1024)
  (func (export "lf_transform") (param $ptr i32) (param i32) (result i32)
    local.get $ptr
    i32.load8_u
    global.set $mode
    {body}
    i32.const 0)
  (func (export "lf_output_ptr") (result i32) {output_ptr})
  (func (export "lf_output_len") (result i32) {output_len}))
"#
    )
}

#[test]
fn configured_engine_admits_mvp_and_rejects_disabled_feature_fixtures() {
    assert!(new_context(descriptor(NORMAL), budgets()).is_ok());

    let fixtures = [
        NORMAL.replace(
            "local.get 1\n    global.set $len",
            "v128.const i32x4 0 0 0 0 drop\n    local.get 1\n    global.set $len",
        ),
        NORMAL.replace(
            "local.get 1\n    global.set $len",
            "i32.const 0 i32.const 0 i32.const 0 memory.copy\n    local.get 1\n    global.set $len",
        ),
        NORMAL.replacen("(memory", "(global externref (ref.null extern)) (memory", 1),
        NORMAL.replacen(
            "(memory",
            "(func $f) (elem declare func $f) (func $r (result funcref) ref.func $f) (memory",
            1,
        ),
        NORMAL.replacen(
            "(memory",
            "(func $multi (result i32 i32) i32.const 0 i32.const 0) (memory",
            1,
        ),
        NORMAL.replacen(
            "(memory",
            "(func $callee) (func $tail return_call $callee) (memory",
            1,
        ),
        NORMAL.replace("(memory (export \"memory\") 1 2)", "(memory 1 1 shared)"),
        NORMAL.replacen("(memory", "(memory 1 1) (memory", 1),
    ];
    for (index, fixture) in fixtures.into_iter().enumerate() {
        let result = new_context(descriptor(&fixture), budgets());
        assert!(
            matches!(
                result,
                Err(ref error) if error.public_error() == PublicError::InvalidModule
            ),
            "disabled feature fixture {index} was admitted"
        );
    }
}

#[tokio::test]
async fn normal_async_transform_has_fresh_store_and_success_record() {
    let fresh = r#"
(module
  (memory (export "memory") 1 2)
  (global $calls (mut i32) (i32.const 0))
  (func (export "lf_alloc") (param i32) (result i32) i32.const 1024)
  (func (export "lf_transform") (param i32 i32) (result i32)
    global.get $calls i32.const 1 i32.add global.set $calls
    i32.const 0)
  (func (export "lf_output_ptr") (result i32) i32.const 1024)
  (func (export "lf_output_len") (result i32) global.get $calls))
"#;
    let runtime = runtime(fresh);
    let first = run(&runtime, b"a").await.unwrap();
    let second = run(&runtime, b"b").await.unwrap();
    assert_eq!(first.output.len(), 1);
    assert_eq!(second.output.len(), 1);
    assert_eq!(first.record.termination_class, TerminationClass::Success);
    assert_eq!(first.record.input_sha256, Some(sha256(b"a")));
    assert_eq!(first.record.output_sha256, Some(sha256(&first.output)));
    assert_eq!(first.record.abi_version, ABI_VERSION);
    assert_eq!(first.record.runtime_version, RUNTIME_VERSION);
}

#[test]
fn forbidden_import_start_wrong_abi_extra_export_and_hash_are_rejected() {
    let forbidden_import =
        NORMAL.replacen("(module", "(module (import \"env\" \"x\" (func $x))", 1);
    assert_eq!(
        new_context(descriptor(&forbidden_import), budgets())
            .unwrap_err()
            .public_error(),
        PublicError::InvalidAbi
    );

    let start = NORMAL.replacen("(memory", "(func $start) (start $start) (memory", 1);
    assert_eq!(
        new_context(descriptor(&start), budgets())
            .unwrap_err()
            .public_error(),
        PublicError::InvalidAbi
    );

    let wrong_abi = NORMAL.replace(
        "(func (export \"lf_alloc\") (param i32) (result i32)",
        "(func (export \"lf_alloc\") (param i64) (result i32)",
    );
    assert_eq!(
        new_context(descriptor(&wrong_abi), budgets())
            .unwrap_err()
            .public_error(),
        PublicError::InvalidAbi
    );

    let extra = NORMAL.replacen("(memory", "(func (export \"extra\")) (memory", 1);
    assert_eq!(
        new_context(descriptor(&extra), budgets())
            .unwrap_err()
            .public_error(),
        PublicError::InvalidAbi
    );

    let bytes: &'static [u8] = Box::leak(wat::parse_str(NORMAL).unwrap().into_boxed_slice());
    let wrong_hash = Box::leak(Box::new(ModuleDescriptor::synthetic(
        "test.transform",
        [0; 32],
        bytes,
    )));
    let error = new_context(wrong_hash, budgets()).unwrap_err();
    assert_eq!(error.public_error(), PublicError::InvalidModule);
    assert_eq!(error.to_string(), "invalid_module");
    assert!(!format!("{error:?}").contains("hash mismatch"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn infinite_loop_exhausts_fuel_or_epoch_without_starving_executor_then_survives() {
    let looping = conditional_transform(
        r#"
        (block $done
          (loop $again
            global.get $mode
            i32.eqz
            br_if $done
            br $again))
        "#,
        "i32.const 1024",
        "i32.const 1",
    );
    let runtime = runtime(&looping);
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticking = Arc::clone(&ticks);
    let responsive = tokio::spawn(async move {
        for _ in 0..5 {
            sleep(Duration::from_millis(2)).await;
            ticking.fetch_add(1, Ordering::Relaxed);
        }
    });
    let error = timeout(Duration::from_secs(1), run(&runtime, &[1]))
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error.public_error,
        PublicError::FuelExhausted | PublicError::WallTimeExceeded
    ));
    responsive.await.unwrap();
    assert_eq!(ticks.load(Ordering::Relaxed), 5);
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn memory_growth_is_contained_and_a_new_store_survives() {
    let growing = conditional_transform(
        "global.get $mode (if (then i32.const 1 memory.grow drop i32.const 1 memory.grow drop))",
        "i32.const 1024",
        "i32.const 1",
    );
    let runtime = runtime(&growing);
    let error = run(&runtime, &[1]).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::MemoryExhausted);
    assert_eq!(error.to_string(), "memory_exhausted");
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn oversized_output_is_rejected_before_copy_then_survives() {
    let oversized = conditional_transform(
        "",
        "i32.const 1024",
        "global.get $mode (if (result i32) (then i32.const 1000) (else i32.const 1))",
    );
    let runtime = runtime(&oversized);
    let error = run(&runtime, &[1]).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::OutputTooLarge);
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn bad_pointer_is_rejected_without_copy_then_survives() {
    let bad_pointer = conditional_transform(
        "",
        "global.get $mode (if (result i32) (then i32.const 131072) (else i32.const 1024))",
        "i32.const 1",
    );
    let runtime = runtime(&bad_pointer);
    let error = run(&runtime, &[1]).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::InvalidOutput);
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn unreachable_is_sanitized_and_a_new_store_survives() {
    let panicking = conditional_transform(
        "global.get $mode (if (then unreachable))",
        "i32.const 1024",
        "i32.const 1",
    );
    let runtime = runtime(&panicking);
    let error = run(&runtime, &[1]).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::GuestFailed);
    assert_eq!(error.to_string(), "guest_failed");
    assert!(!format!("{error:?}").contains("unreachable"));
    assert!(error.private_source().unwrap().contains("unreachable"));
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn input_ceiling_and_guest_status_failures_always_have_records() {
    let normal_runtime = runtime(NORMAL);
    let error = run(&normal_runtime, &[0; 1025]).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::InputTooLarge);
    assert_eq!(error.record.input_sha256, None);
    assert_eq!(
        error.record.termination_class,
        TerminationClass::Failure(PublicError::InputTooLarge)
    );

    let unsupported = NORMAL.replace(
        "i32.const 0)\n  (func (export \"lf_output_ptr\")",
        "i32.const 1)\n  (func (export \"lf_output_ptr\")",
    );
    let runtime = runtime(&unsupported);
    let error = run(&runtime, b"x").await.unwrap_err();
    assert_eq!(error.public_error, PublicError::UnsupportedDocument);
    assert_eq!(error.record.output_sha256, None);
}

#[tokio::test]
async fn cancellation_traps_quickly_releases_permit_and_runtime_survives() {
    let looping = conditional_transform(
        r#"
        (block $done
          (loop $again
            global.get $mode
            i32.eqz
            br_if $done
            br $again))
        "#,
        "i32.const 1024",
        "i32.const 1",
    );
    let mut policy = budgets();
    policy.fuel = ProcessingBudgets::default().fuel;
    let runtime = new_context(descriptor(&looping), policy).unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = runtime
        .try_begin("test.transform")
        .unwrap()
        .run_cancellable(vec![1], cancellation)
        .await
        .unwrap_err();
    assert_eq!(error.public_error, PublicError::Cancelled);
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn pre_cancelled_fast_guests_never_return_success() {
    let runtime = runtime(NORMAL);
    let external = CancellationToken::new();
    external.cancel();
    let error = runtime
        .try_begin("test.transform")
        .unwrap()
        .run_cancellable(b"x".to_vec(), external)
        .await
        .unwrap_err();
    assert_eq!(error.public_error, PublicError::Cancelled);
    assert_eq!(error.record.input_sha256, Some(sha256(b"x")));

    let lease = runtime.try_begin("test.transform").unwrap();
    lease.cancel();
    let error = lease.run(b"x".to_vec()).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::Cancelled);
}

#[tokio::test]
async fn dropping_run_future_signals_cancellation_and_eventually_releases_permit() {
    let looping = conditional_transform(
        r#"
        (block $done
          (loop $again
            global.get $mode
            i32.eqz
            br_if $done
            br $again))
        "#,
        "i32.const 1024",
        "i32.const 1",
    );
    let mut policy = budgets();
    policy.fuel = ProcessingBudgets::default().fuel;
    let runtime = new_context(descriptor(&looping), policy).unwrap();
    let lease = runtime.try_begin("test.transform").unwrap();
    let task = tokio::spawn(lease.run(vec![1]));
    sleep(Duration::from_millis(10)).await;
    task.abort();

    timeout(Duration::from_millis(200), async {
        loop {
            if let Ok(lease) = runtime.try_begin("test.transform") {
                drop(lease);
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn stale_ticker_fails_closed_but_existing_runtime_remains_owned() {
    let runtime = runtime(NORMAL);
    assert!(runtime.try_begin("test.transform").is_ok());
    runtime.pause_ticker();
    sleep(Duration::from_millis(70)).await;
    assert!(matches!(
        runtime.try_begin("test.transform"),
        Err(BeginError::RuntimeUnavailable)
    ));
}

#[test]
fn try_acquire_saturation_is_immediate_and_has_no_waiter_queue() {
    let runtime = runtime(NORMAL);
    let lease = runtime.try_begin("test.transform").unwrap();
    assert!(matches!(
        runtime.try_begin("test.transform"),
        Err(BeginError::Busy)
    ));
    drop(lease);
    assert!(runtime.try_begin("test.transform").is_ok());
}

#[test]
fn contexts_from_one_host_share_the_process_admission_limit() {
    let policy = budgets();
    let host = ProcessingRuntime::new(policy.clone()).unwrap();
    let first = host
        .create_context(descriptor(NORMAL), policy.clone())
        .unwrap();
    let second = host.create_context(descriptor(NORMAL), policy).unwrap();

    let lease = first.try_begin("test.transform").unwrap();
    assert!(matches!(
        second.try_begin("test.transform"),
        Err(BeginError::Busy)
    ));
    drop(lease);
    assert!(second.try_begin("test.transform").is_ok());
}

#[tokio::test]
async fn unstarted_lease_expires_and_releases_shared_admission() {
    let mut policy = budgets();
    policy.wall_time = Duration::from_millis(15);
    let host = ProcessingRuntime::new(policy.clone()).unwrap();
    let first = host
        .create_context(descriptor(NORMAL), policy.clone())
        .unwrap();
    let second = host.create_context(descriptor(NORMAL), policy).unwrap();
    let expired = first.try_begin("test.transform").unwrap();

    sleep(Duration::from_millis(30)).await;
    let replacement = second.try_begin("test.transform").unwrap();
    drop(replacement);
    let error = expired.run(b"late".to_vec()).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::WallTimeExceeded);
    assert_eq!(error.record.input_sha256, Some(sha256(b"late")));
}

#[test]
fn running_timed_permit_is_cancelled_but_not_released_before_drop() {
    let semaphore = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&semaphore).try_acquire_owned().unwrap();
    let timed = TimedPermit {
        permit: Mutex::new(Some(permit)),
        cancellation: CancellationToken::new(),
        deadline: Instant::now(),
        started: AtomicBool::new(true),
    };

    timed.release_if_expired(Instant::now());
    assert!(timed.cancellation.is_cancelled());
    assert_eq!(semaphore.available_permits(), 0);
    drop(timed);
    assert_eq!(semaphore.available_permits(), 1);
}

#[test]
fn context_budgets_cannot_exceed_lower_host_policy() {
    let mut host_policy = budgets();
    host_policy.input_bytes = 32;
    host_policy.memory_bytes = 2 * 65_536;
    host_policy.output_bytes = 8;
    host_policy.wall_time = Duration::from_millis(40);
    host_policy.fuel = 5_000;
    let host = ProcessingRuntime::new(host_policy.clone()).unwrap();
    let context = host
        .create_context(descriptor(NORMAL), ProcessingBudgets::default())
        .unwrap();

    assert_eq!(context.inner.budgets.input_bytes, host_policy.input_bytes);
    assert_eq!(context.inner.budgets.memory_bytes, host_policy.memory_bytes);
    assert_eq!(context.inner.budgets.output_bytes, host_policy.output_bytes);
    assert_eq!(context.inner.budgets.wall_time, host_policy.wall_time);
    assert_eq!(context.inner.budgets.fuel, host_policy.fuel);
    assert_eq!(
        context.inner.budgets.epoch_interval,
        host_policy.epoch_interval
    );
    assert_eq!(context.inner.budgets.concurrency, host_policy.concurrency);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn fuel_and_epoch_backstops_are_independently_classified() {
    let looping = conditional_transform(
        r#"
        (block $done
          (loop $again
            global.get $mode
            i32.eqz
            br_if $done
            br $again))
        "#,
        "i32.const 1024",
        "i32.const 1",
    );

    let mut fuel_policy = budgets();
    fuel_policy.fuel = 1_000;
    let fuel_runtime = new_context(descriptor(&looping), fuel_policy).unwrap();
    let fuel_error = run(&fuel_runtime, &[1]).await.unwrap_err();
    assert_eq!(fuel_error.public_error, PublicError::FuelExhausted);

    let mut epoch_policy = budgets();
    epoch_policy.wall_time = Duration::from_millis(10);
    epoch_policy.epoch_interval = Duration::from_millis(2);
    epoch_policy.stale_heartbeat = Duration::from_millis(100);
    epoch_policy.fuel = ProcessingBudgets::default().fuel;
    let epoch_runtime = new_context(descriptor(&looping), epoch_policy).unwrap();
    let epoch_error = timeout(Duration::from_millis(100), run(&epoch_runtime, &[1]))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(epoch_error.public_error, PublicError::WallTimeExceeded);
}

#[tokio::test]
async fn wasm_stack_exhaustion_is_contained_and_host_survives() {
    let recursive = conditional_transform(
        r#"
        global.get $mode
        (if
          (then
            (call $recurse)
            drop))
        "#,
        "i32.const 1024",
        "i32.const 1",
    )
    .replacen(
        "(func (export \"lf_alloc\")",
        "(func $recurse (result i32) call $recurse)\n  (func (export \"lf_alloc\")",
        1,
    );
    let runtime = runtime(&recursive);
    let error = run(&runtime, &[1]).await.unwrap_err();
    assert_eq!(error.public_error, PublicError::GuestFailed);
    assert_eq!(run(&runtime, &[0]).await.unwrap().output, vec![0]);
}

#[tokio::test]
async fn store_limits_are_installed_before_instantiation() {
    let oversized_table = NORMAL.replacen("(memory", "(table 5000 funcref)\n  (memory", 1);
    let policy = budgets();
    let host = ProcessingRuntime::new(policy.clone()).unwrap();
    let limited = host
        .create_context(descriptor(&oversized_table), policy.clone())
        .unwrap();
    let error = run(&limited, b"x").await.unwrap_err();
    assert_eq!(error.public_error, PublicError::GuestFailed);

    let healthy = host.create_context(descriptor(NORMAL), policy).unwrap();
    assert_eq!(run(&healthy, b"x").await.unwrap().output, b"x");
}

#[tokio::test]
async fn invalid_allocator_status_and_output_lengths_are_classified() {
    let allocator_invalid = NORMAL.replacen("i32.const 1024)", "i32.const -2)", 1);
    let allocator_oom = NORMAL.replacen("i32.const 1024)", "i32.const -1)", 1);
    let negative_output = NORMAL.replace(
        "(func (export \"lf_output_len\") (result i32) global.get $len)",
        "(func (export \"lf_output_len\") (result i32) i32.const -1)",
    );
    let unknown_status = NORMAL.replace(
        "i32.const 0)\n  (func (export \"lf_output_ptr\")",
        "i32.const 99)\n  (func (export \"lf_output_ptr\")",
    );

    assert_eq!(
        run(&runtime(&allocator_invalid), b"x")
            .await
            .unwrap_err()
            .public_error,
        PublicError::InvalidAbi
    );
    assert_eq!(
        run(&runtime(&allocator_oom), b"x")
            .await
            .unwrap_err()
            .public_error,
        PublicError::MemoryExhausted
    );
    assert_eq!(
        run(&runtime(&negative_output), b"x")
            .await
            .unwrap_err()
            .public_error,
        PublicError::InvalidOutput
    );
    assert_eq!(
        run(&runtime(&unknown_status), b"x")
            .await
            .unwrap_err()
            .public_error,
        PublicError::InvalidAbi
    );
}

#[tokio::test]
async fn bounded_test_observer_retains_latest_success_and_failure_records() {
    let runtime = runtime(NORMAL);
    let observer = Arc::new(BoundedRecordObserver::new(2));
    runtime.install_test_observer(Arc::clone(&observer));
    run(&runtime, b"one").await.unwrap();
    run(&runtime, &[0; 1025]).await.unwrap_err();
    run(&runtime, b"three").await.unwrap();
    let records = observer.records();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].termination_class,
        TerminationClass::Failure(PublicError::InputTooLarge)
    );
    assert_eq!(records[1].termination_class, TerminationClass::Success);
}
