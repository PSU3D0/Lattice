schema_version: "2.0"
baseline: "fc0e350"
review: "B5-final-adversarial-re-review"
status: "critical-and-high-findings-closed-with-local-evidence"
findings:
  atomic_ledger_compaction:
    implementation:
      - "src/wasm.rs#BrokerLedgerDurableObject uses worker-0.8.1 Storage::put_multiple for snapshot+tail"
      - "src/durable.rs#apply_persisted_command"
      - "crates/broker-core/src/ledger.rs#InMemoryLedger::restore"
    tests:
      - "src/durable.rs#compaction_commit_has_only_old_or_new_restorable_pairs"
      - "src/durable.rs#compaction_keeps_long_histories_operable_and_tail_bounded"
      - "workerd-tests/src/index.test.ts#compacts DO history without returning events and continues after the threshold"
  production_package_isolation:
    implementation:
      - "workerd-tests/scripts/package.mjs clean root+staging production builds without test-fixtures"
      - "scripts/build-matrix.sh explicitly separates build, build-test, workerd build-production, and fixture build"
      - "workerd-tests/scripts/verify-package.mjs every-file Cargo.lock/hash/path/sentinel verification"
      - "fixture-only literals are structurally split in source; package.mjs excludes build-test/build-production/workerd-tests/node_modules/target"
      - "broker-workers-package dependency closure outside baseline hint-gate scan roots"
    tests:
      - "workerd-tests/src/index.test.ts#keeps fixture code absent from the production WASM route table"
      - "root/package production WASM SHA-256 equality"
      - "workerd-tests/scripts/package.test.mjs asserts every exclusion, every-file sentinel absence, lock byte equality, and WASM parity"
      - "production every-file sentinel scan"
      - "package verifier repeated after final generation"
  deploy_ownership_and_preflight:
    implementation:
      - "deploy/scripts/deploy-lib.mjs#executeApply"
      - "deploy/approved-dependencies.example.json immutable service and worker ownership pins"
      - "workerd-tests/scripts/production-preflight.mjs"
      - "deploy/scripts/cleanup-lib.mjs live deployment+metadata+ownership hash verification"
      - "deploy/scripts/cleanup.mjs refuses deletion until every target is live-verified"
    tests:
      - "deploy/scripts/deploy.test.mjs#pre-existing unowned target fails before mutation"
      - "deploy/scripts/deploy.test.mjs#pre-existing owned update is never deleted after later failure"
      - "deploy/scripts/deploy.test.mjs#newly created partial resources clean public then private"
      - "deploy/scripts/deploy.test.mjs#hermetic preflight failure occurs before every remote command"
      - "deploy/scripts/deploy.test.mjs#immutable dependency hash mismatch fails before mutation"
      - "deploy/scripts/deploy.test.mjs#OAuth callback readiness requires exact safe 400"
      - "deploy/scripts/cleanup.test.mjs unowned/hash-mismatch/missing-metadata refusal and exact-owned success"
  request_bound_pop:
    implementation:
      - "src/wasm.rs#exact_request_target preserves runtime pathname plus raw query serialization"
      - "src/wasm.rs#bounded_body streams with a hard bound"
      - "src/wasm.rs#reject_nonempty_body for GET/DELETE"
      - "src/management.rs canonical base64url and verify_strict"
      - "crates/broker-core/src/signing.rs canonical base64url and verify_strict"
    tests:
      - "workerd-tests/src/index.test.ts#rejects replay, JTI reuse, stale time, wrong key, and body/path/method mutation before effects"
      - "src/management.rs#base64url_keys_and_signatures_require_canonical_spelling_and_strict_ed25519"
  binding_validity:
    implementation:
      - "crates/broker-core/src/artifacts.rs#BindingAttestation.not_before"
      - "crates/broker-core/src/artifacts.rs#BindingAttestation::validate_active_at"
      - "crates/broker-core/src/engine.rs#validate_admission"
      - "src/wasm.rs#issue_grant and immediate pre-dispatch validate_admission"
    tests:
      - "crates/broker-core/src/artifacts.rs#binding_validity_window_is_half_open"
  exact_oauth_scopes_and_activation_saga:
    implementation:
      - "src/wasm.rs#oauth_callback exact pinned profile scope equality"
      - "src/refresh.rs#ConnectionTokenState::complete_refresh exact scope equality"
      - "migrations/0001_broker.sql#exchange_pending/exchange_inflight/cleanup_pending encrypted recovery columns"
      - "src/wasm.rs#oauth_callback encrypted resume-safe claim and idempotent intent_ref token exchange"
      - "src/wasm.rs#oauth_callback route_reserved/credential_registered/connection_inserted saga"
      - "src/wasm.rs#compensate_activation and retry_activation_cleanup durable cleanup_pending"
      - "src/refresh.rs#ConnectionTokenState::matches_registration idempotent registration"
    tests:
      - "src/refresh.rs#provider_added_scope_is_rejected_and_wiped"
      - "workerd-tests/src/index.test.ts#resumes OAuth activation idempotently at every credential saga boundary"
      - "workerd-tests/src/index.test.ts#recovers claimed and exchange crashes and durably retries cleanup"
      - "workerd-tests/src/index.test.ts#fails expired callback state and actual missing scopes/account or precomputed commitments"
  descriptor_complete_dispatch:
    implementation:
      - "crates/broker-host/src/executor.rs#descriptor_plan_template"
      - "src/wasm.rs#dispatch_provider uses planned method/path/query/body/static headers and exact Accept/Content-Type"
    tests:
      - "workerd-tests/src/mock-provider.mjs rejects method/Accept/Content-Type drift"
      - "workerd-tests/src/index.test.ts#executes an exact Google descriptor through the private binding and redelivers the receipt"
  tenant_scoped_receipts:
    implementation:
      - "migrations/0001_broker.sql#receipts org+deployment+grant+reservation identity"
      - "src/wasm.rs#reservation_identity_hash and receipt_ref v2"
      - "src/wasm.rs#receipt_route filters authenticated org and deployment"
    tests:
      - "workerd-tests/src/index.test.ts#isolates receipt lookup by session tenant including same-org cross-deployment"
  prior_b5_controls_retained:
    implementation:
      - "shared admission, exact FlowAuthorityManifest grants, provider-neutral OAuth, refresh fencing, bounded responses, private bootstrap/grants, signed receipts"
    tests:
      - "broker-core/host/workers/custodian/Google/llm-lattice Rust suites"
      - "full Workerd suite"
      - "baseline scripts/check-hint-literals.sh unchanged and passing"
local_limits:
  - "No remote deployment, provider call, production credential, or live infrastructure mutation was performed."
  - "Apply behavior is proven with an injected command runner and local Wrangler dry-runs; live qualification remains an operator action."
  - "Service-binding timeout drops the fetch future; no claim is made about remote server cancellation after the future is dropped."
