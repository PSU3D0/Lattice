Status: Canonical
Purpose: scenario
Owner: Product
Last reviewed: 2025-12-12

# Addendum — User Stories & Execution Patterns (v1.0)

**Assumptions**

* Effects/Determinism lattices, capability typestates, idempotency spec, Temporal lowering, windowing/watermarks, SSE streaming, cache/Continue‑As‑New defaults are implemented per the RFC.
* Profiles: `Web`, `Queue`, `Temporal`, `Dev`, `WASM`.
* Registry certification and policy engine are active.

**Format per story**

* Context & constraints
* Macro surface (abridged)
* Flow IR highlights
* Execution plan (per host)
* Idempotency, Effects, Determinism, Policy
* Failure modes & mitigations
* Tests & acceptance criteria
* Agent loop (PM → Builder → Policy → Test → Deploy)

---

## S1. Webhook → Inline Map → Respond (low‑latency API)

**Context**: ping endpoint that normalizes payload and echoes typed JSON within 50ms p95.

**Macros**

```rust
inline_node! {
  name:"Normalize", in: InReq, out: OutRes,
  effects: Pure, determinism: Strict,
  |_, r| { Ok(OutRes { v: r.v.trim().to_lowercase() }) }
}

workflow! {
  name: echo_norm, profile: Web, version: 1_0_0;
  let hook = HttpTrigger::post("/echo").respond(on_last).deadline(ms=250);
  let map  = Normalize;
  let rsp  = Respond; // streaming=false default
  connect!(hook -> map);
  connect!(map -> rsp);
  rate_limit!(workflow, qps=1000, burst=2000);
}
```

**IR highlights**

* Nodes: Normalize(Pure, Strict) → Respond(Effectful=Write only to adapter, BestEffort).
* Edge: Ordered, small buffer, timeout 250ms.

**Execution plan**

* Web profile: inline Normalize in process; Respond via adapter oneshot. No queue/Temporal.

**Idempotency/Policy**

* No effectful external writes; no dedupe needed.
* Policy: require `deadline` and `qps` caps for Web profile routes.

**Failure modes**

* Backpressure stall → 503; cancel propagation from client; compile‑time: none.

**Tests**

* p95 latency < 50ms at 500 rps.
* Cancellation: client abort → node sees `ctx.is_cancelled() == true` and drops work.

**Agent loop**

* PM defines route and SLO; Builder compiles; Policy checks no capabilities; Test measures latency; Deploy maps route.

---

## S2. Marketing Website (15 routes), **SSE streaming**, contact form

**Context**: GET pages + POST forms; stream changelog as SSE; sessions external to kernel.

**Macros (abridged)**

```rust
inline_node! { name:"RouteSwitch", in: HttpRequest, out: Route, effects: Pure, determinism: Strict, |_, r| { /* match … */ } }

inline_node! { name:"RenderChangelogSSE", in: HttpRequest, out: HttpChunk,
  effects: ReadOnly, determinism: BestEffort, resources(kv(KvRead)),
  |ctx, _| { Ok(stream_changelog(ctx.resources.kv())) } } // yields chunks

workflow! {
  name: site, profile: Web;
  let ingress = HttpTrigger::any("/").respond(explicit).deadline(ms=25000);
  let route   = RouteSwitch;
  connect!(ingress -> route);

  let mux = Merge::new(); // fan-in

  let sse   = RenderChangelogSSE;  // out: stream<HttpChunk>
  let form  = HandleContact;       // Effectful email
  let pages = RenderPage;          // Pure/ReadOnly
  // connect!(route.case(...)-> ... ) for all 15 routes

  let respond = Respond(stream=true); // SSE chunk bridge
  connect!(sse -> respond);
  connect!(pages -> respond);
  connect!(form  -> respond);
}
```

**IR highlights**

* SSE path emits `stream<HttpChunk>`; Respond has `stream=true`.
* Contact POST is Effectful → idempotency by form checksum.

**Execution plan**

* Web profile: SSE backpressure bridged to edge demand. Session is an Axum facet; session mutations via SessionStore capability if used.

**Idempotency/Determinism/Policy**

* Form POST `idem.key = blake3(canonical(FormSubmission))`.
* `RenderChangelogSSE`: BestEffort; policy allows BestEffort on pages but not for PII routes.
* Egress deny‑all (no network declared).

**Failure modes**

* Client disconnect → cancel token propagates; SSE ends.
* Large page → chunked with backpressure.

**Tests**

* SSE p95 **write gap** < 30ms at 100 concurrent streams.
* POST dedupe: double submit returns 200 single side‑effect.

**Agent loop**

* PM enumerates routes; Builder wires switch; Policy approves BestEffort for SSE; Test runs latency; Deploy binds routes.

---

## S3. **Batch ETL** with **windowing & watermarks** (S3 → Transform → Warehouse)

**Context**: Ingest S3 logs, 5‑minute tumbling windows by event‑time, lateness 2 minutes. Exactly‑once upsert to Warehouse.

**Macros**

```rust
#[node(name="S3List", out=ObjectRef, resources(blob(BlobRead)))] struct S3List;

#[node(name="Parse", in=Bytes, out=Log, effects=Pure, determinism=Strict)] struct Parse;

#[node(name="WriteWarehouse", in=AggRow, out=Ack,
  effects=Effectful, determinism=BestEffort,
  resources(db(DbWrite), dedupe(DedupeStore)),
  idempotency="by_input_hash")]
struct WriteWarehouse;

workflow! {
  name: etl_logs, profile: Queue;
  let src   = S3List { prefix: "s3://bucket/logs/2025/*" };
  let bytes = BlobGet; // BlobRead::get_by_hash -> Strict
  let parse = Parse;
  let win   = Window::tumbling(event_time="ts", size=5m, allowed_lateness=2m, watermark="max(ts)-2m");
  let agg   = Aggregate::by_key(|l| l.customer_id).sum("bytes");
  let upsert= WriteWarehouse;

  connect!(src -> bytes);
  connect!(bytes -> parse);
  connect!(parse -> win);
  connect!(win -> agg);
  partition_by!(upsert, key = |r: &AggRow| r.customer_id);
  connect!(agg -> upsert);

  delivery!(agg->upsert, ExactlyOnce);
}
```

**IR highlights**

* Strict path till aggregation; Effectful sink with DedupeStore; ExactlyOnce edge.
* Window metadata: event‑time, watermark, lateness; Determinism **Stable** for window because inputs pinned (Blob by hash).

**Execution plan**

* Queue workers: batch by 1000 records; watermark advancement closes windows; ExactlyOnce enforced via DedupeStore; retry owner = Orchestrator.
* Temporal (optional): window close → timer; fan‑out child workflows per partition.

**Idempotency/Policy**

* `idem.key = blake3(flow_id, partition=customer_id, window_start, window_end, data_hash)` (canonical CBOR).
* Policy requires DedupeStore bound & warehouse upsert conforming to sink contract.

**Failure modes**

* Late events beyond 2m → side output “late\_events”; BI team can reprocess via out‑of‑band flow.
* Dedupe collision suspected → hard fail with IDEM099.

**Tests**

* Watermark correctness (out‑of‑order + late).
* Duplicate & reorder injections → single upsert.
* Throughput target met at 20k rec/min, p99 worker CPU < 80%.

**Agent loop**

* PM chooses 5‑min window; Builder inserts `Window`; Policy enforces DedupeStore; Test runs lateness harness; Deploy scales workers.

---

## S4. Payments with **HITL** & **SAGA compensation** (Temporal)

**Context**: Charge cards; if downstream fails, refund; require human approval > \$5k.

**Macros**

```rust
#[node(name="Charge", in=Invoice, out=Receipt,
  effects=Effectful, determinism=BestEffort,
  resources(http(HttpWrite), creds(StripeKey), dedupe(DedupeStore)),
  idempotency="by_input_hash",
  compensate=Refund)]
struct Charge;

#[node(name="Refund", in=Receipt, out=Ack,
  effects=Effectful, determinism=BestEffort,
  resources(http(HttpWrite), creds(StripeKey)),
  idempotency="by_input_hash")]
struct Refund;

workflow! {
  name: payments, profile: Temporal;
  let inv   = HttpTrigger::post("/invoice").respond(immediate);
  let gate  = If::<Invoice>::new(|i| i.amount > 5000);
  hitl!("approve_high_value", on=gate.true, view=ApprovalCard { /* ... */});
  let charge= Charge;
  let ship  = CreateShipment; // effectful
  let ack   = Respond;

  connect!(inv -> gate.in);
  connect!(gate.true -> charge.in);
  connect!(gate.false-> charge.in);
  connect!(charge -> ship);
  connect!(ship -> ack);

  // Retry ownership: Orchestrator
  on_error!(scope=charge, strategy=Retry{max=5, backoff=Exp{base_ms=200}});
}
```

**Execution plan (Temporal)**

* Charge → Activity with idempotency; Refund is compensator on failure after Charge.
* HITL → Signal wait with TTL 24h.
* Continue‑As‑New every 2000 events or 3h.

**Idempotency**

* Key: blake3(invoice\_id, customer\_id, amount\_cents, currency, item\_hashes).
* Sink contract: Stripe idempotency keys + local DedupeStore.

**Failure modes**

* Post‑charge failure in shipping → SAGA triggers Refund in reverse order; partial unwind → alert & manual queue.
* HITL timeout → auto‑reject or escalate.

**Tests**

* Duplicate invoice submissions → single charge.
* SAGA: inject failure after charge → refund called exactly once.
* Temporal history growth bounded.

**Agent loop**

* PM sets approval rule; Builder compiles; Policy checks Effectful+idempotency; Test injects failures; Deploy with Temporal adapter.

---

## S5. High‑throughput ingestion (50k msg/s), hot shards

**Context**: Clickstream; shard by `user_id`; fairness; spill.

**Macros**

```rust
workflow! {
  name: clicks, profile: Queue;
  let ingest = KafkaTrigger::topic("clicks").respond(immediate); // imagined trigger
  let parse  = ParseClick;    // Pure, Strict
  let route  = Switch::<Click>::new(); // route types
  partition_by!(route, key = |c:&Click| c.user_id);
  connect!(ingest -> parse);
  connect!(parse -> route);

  let sink = ClickSink; // Effectful, idempotent
  connect!(route.case(Type::View) -> sink);
  /* … */
  rate_limit!(sink, qps=100000, burst=200000);
}
```

**Execution plan**

* Queue workers with **max\_parallel\_shards=N** per worker; weighted fairness.
* BufferPolicy: spill at 64MiB per edge; spill bandwidth cap 50MB/s/worker.

**Idempotency**

* Key: blake3(user\_id, event\_id, ts).
* Delivery AtLeastOnce; dedupe in sink.

**Failure modes**

* Hot shards throttled; fairness skew < 2.0 target.
* When spill engaged, latency increases but no data loss.

**Tests**

* Synthetic hot‑user distribution; fairness skew metrics; spill‑resume verification.

**Agent loop**

* PM sets throughput goals; Builder sets partition & limits; Policy checks egress; Test load; Deploy auto‑scale workers.

---

## S6. Long‑running onboarding (weeks), reminders, documents (Temporal)

**Context**: KYC onboarding with periodic reminders, document review, final approval.

**Macros**

```rust
workflow! {
  name: onboarding, profile: Temporal;
  let start = HttpTrigger::post("/start").respond(immediate);
  let capture = CaptureDocs; // ReadOnly (upload to blob), BestEffort
  let review  = HumanReview; // HITL
  hitl!("doc_review", on=review, view=DocCard{});
  let remind = CronTrigger::cron("0 9 * * 1,3").respond(immediate); // reminders

  connect!(start -> capture);
  connect!(capture -> review);

  // Temporal timers for reminder cadence, cancel on approved
  // (implicitly generated by lowering rules based on HITL TTL)
}
```

**Execution plan**

* Review wait via Signal; Reminders via Timers; Continue‑As‑New every 3h or 2000 events.

**Policy**

* PII classification enforced; region residency tags checked.

**Tests**

* Signals reorder → still accepted once; timer jitter 5–10%.

**Agent loop**

* PM defines cadence; Builder wires; Policy enforces PII rules; Test replays history with reordering.

---

## S7. DS pipeline with Python plugin, sandboxed

**Context**: Data clean with Pandas; no outbound network; budget 2 CPU‑min.

**Macros**

```rust
#[node(name="PyClean", in=Row, out=Row,
  effects=Pure, determinism=Strict, // if fully deterministic transform
  resources(python(PluginPython{network="deny", cpu=2, mem_mb=1024})))]
struct PyClean;

workflow! {
  name: ds_clean, profile: Queue;
  let src = CsvRead; // BlobRead::get_by_hash
  let py  = PyClean;
  let sink= CsvWrite; // BlobWrite
  connect!(src -> py);
  connect!(py -> sink);
}
```

**Execution plan**

* Out‑of‑proc gRPC; canonical CBOR JSON; seccomp/AppArmor; timeouts.

**Tests**

* Determinism replay; sandbox enforcement (network touches denied).

**Agent loop**

* Builder declares plugin; Policy checks sandbox; Test replays; Deploy.

---

## S8. Edge/WASM offline → later reconcile

**Context**: Field device processes jobs offline; later pushes results to cloud.

**Macros**

```rust
#[node(name="LocalFilter", in=Reading, out=Reading, effects=Pure, determinism=Strict)]
struct LocalFilter;

workflow! {
  name: field_pipeline, profile: WASM;
  let read = SensorRead;    // Pure, Strict
  let filt = LocalFilter;
  let sum  = LocalAggregate::window("1m"); // Stable with pinned tick
  let cache= CacheWriteStrict{ key = |r| blake3(r.tick, r.id) }; // CacheSpec
  connect!(read -> filt);
  connect!(filt -> sum);
  connect!(sum  -> cache);
}
```

**Reconcile flow (cloud)**

* Reads cached Strict entries; merges into global store; conflict policy `last_writer_wins` by tick.

**Tests**

* Offline mode replay; merge semantics; cache TTL enforcement.

**Agent loop**

* PM sets reconcile policy; Builder emits cloud merge flow; Policy checks residency; Test offline/online transitions.

---

## S9. LLM summarizer with budgets & determinism (Stable)

**Context**: Summarize text with pinned model/version/seed; budget \$X/day; stream tokens to client.

**Macros**

```rust
#[node(name="OpenAIChatStream", in=ChatReq, out=ChatDelta,
  effects=ReadOnly, determinism=Stable, // pinned model + seed required
  resources(ai(OpenAIRead), budget(TokenBudget)),
  idempotency="by_input_hash")]
struct OpenAIChatStream;

workflow! {
  name: doc_summarize, profile: Web;
  let hook   = HttpTrigger::post("/sum").respond(explicit).deadline(ms=25000);
  let prep   = MakePrompt;     // Pure, Strict
  let chat   = OpenAIChatStream; // stream deltas
  let stream = Respond(stream=true);
  connect!(hook -> prep);
  connect!(prep -> chat);
  connect!(chat -> stream);
}
```

**Policy**

* Budget capability enforces token ceilings; downgrade determinism if seed/model missing; registry certification replays a stable sample.

**Tests**

* Budget stop → graceful end; SSE latency within bounds; determinism replay passes within tolerance.

**Agent loop**

* PM sets budget; Builder pins model/seed; Policy approves; Test runs contract; Deploy.

---

## S10. Exactly‑once DB sink via **Outbox pattern**

**Context**: Write order events exactly once into DB; also publish to Kafka.

**Macros**

```rust
#[node(name="UpsertOutbox", in=OrderEvt, out=OutboxPtr,
  effects=Effectful, determinism=BestEffort,
  resources(db(DbWrite), dedupe(DedupeStore)),
  idempotency="by_input_hash")]
struct UpsertOutbox;

#[node(name="ProjectToKafka", in=OutboxPtr, out=Ack,
  effects=Effectful, determinism=BestEffort, resources(kafka(KafkaWrite)))]
struct ProjectToKafka;

workflow! {
  name: order_eox, profile: Queue;
  let src = OrdersTrigger;
  let out = UpsertOutbox;      // idempotent upsert (db tx)
  let proj= ProjectToKafka;    // reads outbox row -> publish -> mark sent
  delivery!(src->out, ExactlyOnce);
  connect!(src -> out);
  connect!(out -> proj);
}
```

**Execution plan**

* Queue workers; dedupe before upsert; outbox tx ensures idempotent write; projector publishes and marks sent (idempotent).
* Certification duplicates → single DB row, single publish.

**Agent loop**

* Builder chooses outbox; Policy requires DedupeStore; Test harness injects duplicates; Deploy.

---

# Cross‑cutting Pseudo‑code & Harnesses

## P0. **Idempotency key** (canonical CBOR → BLAKE3)

```rust
fn idem_key(flow_id: &str, node_id: &str, partition: &str, fields: &serde_json::Value) -> [u8; 32] {
    use cbor_event::se::Serializer;
    let mut buf = Vec::with_capacity(256);
    let mut ser = Serializer::new(&mut buf);
    // Canonical CBOR map: sorted keys, shortest ints, NFC strings
    ser.write_map(Some(4)).unwrap();
    ser.write_text("f").unwrap(); ser.write_text(flow_id).unwrap();
    ser.write_text("n").unwrap(); ser.write_text(node_id).unwrap();
    ser.write_text("p").unwrap(); ser.write_text(partition).unwrap();
    ser.write_text("x").unwrap(); ser.write_value(&canonicalize(fields)).unwrap();
    blake3::hash(&buf).into()
}
```

*Test vectors:* include three fixtures with known outputs.

---

## P1. **Windowing & watermarks** (planner)

```pseudo
for record in stream:
  ts = record.event_time
  if ts <= watermark:
      emit side_output("late_events", record)
      continue
  bucket = floor(ts / WINDOW_SIZE) * WINDOW_SIZE
  state[bucket].accumulate(record)
  if now >= bucket + WINDOW_SIZE + ALLOWED_LATENESS:
      close(bucket); emit window_aggregate(bucket)
      advance watermark to min(open_buckets) - ALLOWED_LATENESS
```

---

## P2. **SSE bridge** (Web profile)

```pseudo
fn respond_stream(rx: EdgeStream<HttpChunk>, http_tx: HttpBody) {
  loop:
    select:
      chunk = rx.next() => http_tx.write(chunk); http_tx.flush();
      client_closed => cancel_token.set(); break;
}
```

---

## P3. **Temporal lowering** (activity vs inline)

```pseudo
for unit in ExecPlan.units:
  if unit.is_inline_pure_strict():
     emit_inline(unit)
  else:
     emit_activity(unit, retry=policy.retry(unit), timeout=unit.timeout)

for cp in checkpoints:
  await_signal(cp.name, ttl=cp.ttl)

if history_size > H_THRESHOLD or wall_time > T_THRESHOLD:
  continue_as_new(cursor)
```

---

## P4. **Certification harness** (registry)

```pseudo
run effects_check(spec)       // resources -> effects derivation
run determinism_replay(spec)  // Strict byte-identical; Stable pinned-equal
run contract_tests(spec)      // 200/401/429/5xx
run idem_injection(spec)      // duplicate & reordering
run policy_evidence(spec)     // egress, scopes, residency
run budget_check(spec)        // estimated vs policy ceiling
if any fails: reject publish
```

---

# Operational playbooks (snippets)

**Incident: duplicate charges observed**

* Run `flows certify idem --flow payments --since 24h`.
* Check DedupeStore retention ≥ visibility timeout × max retries.
* Verify Stripe idempotency key equals registry key derivation.
* Inspect SAGA log; re‑apply compensations for orphaned receipts.

**Scaling: hot partitions**

* Increase `max_parallel_shards` + shard over `(user_id % N)`; adjust fairness weights; monitor `fairness.skew_ratio`.

**Budget breach (LLM)**

* Budget capability halts Effectful AI nodes; PM agent proposes `chunk_size` reduction; Builder agent updates `MakePrompt`; redeploy.

---

# Acceptance criteria (global deltas)

* **Idempotency**: 100% Effectful sinks provide deterministic keys; duplicate injection harness passes.
* **Windowing**: Watermark/lateness tests pass; late side‑output coverage ≥ 99.9% accurate vs oracle.
* **Web streaming**: SSE p95 write gap < 30ms; cancellation observed by nodes.
* **Temporal**: history growth below thresholds; child workflow IDs align with naming scheme; Continue‑As‑New triggers correctly.
* **Cache**: Strict nodes have CacheSpec; Stable nodes use pinned reads; replay hit rate ≥ 95% on goldens.
* **Policy evidence report** attached to every publish.

---

# Agent loops (canonical patterns)

**Builder loop (compile‑fix)**

1. Emit macros → `cargo check` → parse diagnostics.
2. Fix PortTypeMismatch by inserting `Map<T,U>` or `From<T> for U`.
3. Tighten determinism claims after removing clock/rng.

**Policy loop**

1. Read NodeSpec (effects/determinism/egress/scopes).
2. Compare with org policy; add waivers or block publish.
3. For upgrades, run cert harness delta (only changed nodes).

**Test loop**

1. Generate fixtures from schemas (fuzzers).
2. Run replay; inject errors (429, 5xx).
3. For Temporal flows, replay history permutations.

**Optimizer loop**

1. Read cost & latency; try batching/concurrency adjustments.
2. Ensure retry ownership single‑sourced; update schedule hints.
3. Propose new plan; PM approves; Builder applies diffs.

---

# Checklists

**Pre‑publish (connector/subflow)**

* [ ] Effects derived == declared
* [ ] Determinism replay proof attached
* [ ] Idempotency key present (if Effectful)
* [ ] Egress domains & scopes listed
* [ ] Contract tests (200/401/429/5xx)
* [ ] Data‑class tags on all fields
* [ ] SBOM + signature

**Flow readiness**

* [ ] Partition keys for hot paths
* [ ] Retry owner singular & justified
* [ ] Window/watermark specified when aggregating
* [ ] CacheSpec for Strict/Stable segments
* [ ] HITL TTL & escalation policy defined
* [ ] Temporal thresholds set (if Temporal)

