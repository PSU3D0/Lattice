Status: Draft (design only — packet A3; no code accompanies this document)
Purpose: design / spec
Owner: Core
Last reviewed: 2026-06-11

# Type-Level Capability Tokens (post-0.1.x design)

This document designs the step **after** runtime capability enforcement (packet
A2, scoped per-node resource views): making undeclared capability access a
**compile error** instead of a runtime denial. The macro-declared
`resources(...)` set becomes typed values the node body receives — and the node
body has no other way to reach a capability.

Source of truth (current state this design builds on):
- Capability interfaces + `ResourceAccess` (~17 optional accessors): `crates/capabilities/src/lib.rs`
- Ambient context (`with_resources` / `with_current_async`): `crates/capabilities/src/lib.rs` (`context` module)
- `resources(alias(Trait))` parsing + hint emission: `crates/dag-macros/src/lib.rs` (`ResourceSpec`, `compute_resource_hints`)
- Typed hints (packet A1): `crates/dag-core/src/effect_hint.rs` (`EffectHint`)
- Runtime enforcement (packet A2): `ScopedResources` + CAP11x denial, `impl-docs/spec/capabilities-and-binding.md`
- Real declaration usage: `examples/s4_preflight/src/lib.rs`, `examples/s11_lead_intake/src/lib.rs`

Related specs:
- `impl-docs/spec/capabilities-and-binding.md`
- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/connector-connection-bindings.md`

## Goal (restated against the thesis)

The platform promise is **compile-verifiability of what may be called**. The
ladder so far:

1. **Declared** (pre-A1): `resources(...)` emits string hints; nothing checks
   the body against them.
2. **Validated** (A1): hints are a closed `EffectHint` enum; typos and unknown
   hints fail closed at IR validation.
3. **Runtime-enforced** (A2): each node executes against a `ScopedResources`
   view built from its declared hints; undeclared accessors return `None` plus
   a structured CAP11x denial.
4. **Compile-enforced** (this design): the declared resources become typed
   parameters of the node function. A body that uses an undeclared capability
   does not compile — there is no expression that produces it.

Step 4 matters because of unit economics, not security. A2 catches over-reach
at first execution; an agent authoring a node discovers the mistake one
edit→build→deploy→run cycle late. Types move the failure to `cargo check` (and
to the editor diagnostic before that), which is the loop we are optimizing
(workstream B). The runtime denial does not become redundant — see
"Division of labour with A2" below.

## Current authoring shape (what must change)

Today a node declares resources but acquires them ambiently:

```rust
#[def_node(
    name = "StoreImage",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_write(capabilities::workspace::Workspace))
)]
async fn store_image(input: HighPriorityLeadImage) -> NodeResult<StoredLeadPackage> {
    let written = capabilities::context::with_current_async(|resources| async move {
        let workspace = resources
            .workspace()
            .ok_or_else(|| NodeError::new("store_image requires Workspace"))?;
        // ...
    })
    .await
    .ok_or_else(|| NodeError::new("missing ResourceAccess context"))??;
}
```

Three problems: the `resources(...)` line and the body are connected only by
A2's runtime check; every access pays two layers of `Option`/`ok_or_else`
ceremony for a capability that preflight (CAP101) already guaranteed is bound;
and helpers like `LatticeHttpClient::from_current_resources()` reach into
ambient context invisibly.

## Candidate designs

All candidates keep `register_fn`'s object-safe `Fn(In) -> Fut` registry shape
by having the macro emit a one-argument **adapter** that the registry calls;
the adapter obtains the A2 `ScopedResources` view and constructs the typed
values before invoking the author's function. The IR, hints, and preflight are
unchanged: typed parameters and `EffectHint` emission derive from the same
`resources(...)` list, so static claims stay in lockstep with the body by
construction.

### (a) Capability tokens as individual typed parameters

```rust
/// In `capabilities`: a typed grant. Constructed only by macro-generated
/// adapters (constructor is #[doc(hidden)]); Deref's to the trait object.
pub struct Cap<T: ?Sized + 'static> { inner: Arc<T> }

impl<T: ?Sized> std::ops::Deref for Cap<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.inner }
}

// Authoring shape:
#[def_node(
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_write(capabilities::workspace::Workspace))
)]
async fn store_image(
    input: HighPriorityLeadImage,
    workspace_write: Cap<dyn Workspace>,
) -> NodeResult<StoredLeadPackage> {
    workspace_write.write_normalized(&path, &bytes, opts).await.map_err(node_error)?;
}
```

The macro checks that the parameter list after `input` matches the
`resources(...)` aliases one-to-one (name and trait), producing a targeted
compile error on mismatch — the declaration and the signature cannot drift.

- **Ergonomics**: infallible access (no `Option`); but nodes with 3–4
  resources grow long signatures, and every added resource is a two-place edit
  (attribute + signature). For agents the redundancy is actually a feature
  (the macro cross-check converts drift into a precise diagnostic), but it is
  ceremony.
- **Macro complexity**: moderate — signature rewriting, parameter/alias
  matching, adapter generation.
- **Dispatch cost**: `Arc<dyn Trait>` exactly as today; zero new cost, no
  monomorphization growth.
- **Ambient context**: must be removed from the author-reachable surface for
  the guarantee to hold (see below — true for all candidates).
- **wasm32**: tokens hold the same `Remote*` providers the guest bag holds
  today; the `async_trait(?Send)` traits (`KeyValue`, `DedupeStore`, …) stay
  object-safe; no `Send` bound is required on `Cap` itself.
- **Testability**: tests construct tokens directly from any provider
  (`Cap::for_test(Arc::new(MemoryKv::new()))`) — strictly better than wrapping
  the test body in `context::with_resources`.

### (b) Generated per-node context struct (one parameter)

```rust
#[def_node(
    effects = "Effectful",
    determinism = "BestEffort",
    resources(http_write(capabilities::http::HttpWrite),
              kv(capabilities::kv::KeyValue))
)]
async fn normalize(input: Lead, ctx: NormalizeCtx) -> NodeResult<Normalized> {
    let resp = ctx.http_write().send(req).await.map_err(node_error)?;
    ctx.kv().put(&key, &bytes, None).await.map_err(node_error)?;
}
```

The macro generates, next to the node:

```rust
pub struct NormalizeCtx {
    http_write: Arc<dyn capabilities::http::HttpWrite>,
    kv: Arc<dyn capabilities::kv::KeyValue>,
}

impl NormalizeCtx {
    pub fn http_write(&self) -> &dyn capabilities::http::HttpWrite { &*self.http_write }
    pub fn kv(&self) -> &dyn capabilities::kv::KeyValue { &*self.kv }
    /// Test/bench constructor; also the seam the generated adapter uses.
    pub fn from_resources(r: &dyn ResourceAccess) -> Result<Self, MissingCapability> { /* generated */ }
}
```

The struct name is derived from the function name (`normalize` →
`NormalizeCtx`); the author writes it in the signature and the macro verifies
the second parameter's type matches the expected ident. Only the declared
accessors exist — `ctx.blob()` is a method-not-found error with the
`resources(...)` list one screen away.

- **Ergonomics**: best of the three. One parameter regardless of arity; adding
  a resource is a one-place edit (the attribute) and the new accessor simply
  appears; accessors are infallible `&dyn` borrows. The generated ctx is also
  the natural value to pass into shared helpers, replacing
  `from_current_resources()`.
- **Macro complexity**: highest — generates a struct + impl per node and the
  `from_resources` constructor. All mechanical; the existing macro already
  generates per-node types (`{fn}_Input`, `{fn}_NODE_SPEC`).
- **Dispatch cost**: identical to (a) — `Arc<dyn>` fields, dyn dispatch.
- **wasm32**: the generated guest dispatch (`lf_invoke_node` expansion) builds
  the ctx from the same per-node bag it builds today; same `?Send` story
  as (a).
- **Testability**: `NormalizeCtx::from_resources(&bag)` or a generated
  field-wise constructor; mocks are per-trait fakes, unchanged.

### (c) Generic node functions over capability marker traits

```rust
pub trait HasHttpWrite { fn http_write(&self) -> &dyn HttpWrite; }
pub trait HasKv { fn kv(&self) -> &dyn KeyValue; }

async fn normalize<C: HasHttpWrite + HasKv>(input: Lead, ctx: &C) -> NodeResult<Normalized>
```

Maximally idiomatic Rust capability style, but the registry needs a concrete
`Fn`, so the macro must instantiate the generic with a concrete scoped type
anyway; authors gain little over (b) while paying monomorphization and
compile-time cost (workstream B treats build time as unit economics).
**Rejected as the primary surface**, but the `Has*` traits are worth keeping
as a *secondary* device: the macro implements them on each generated ctx
struct, so shared helpers written once against `impl HasHttpWrite` accept any
node's ctx.

### Ambient context: the honest answer

For **any** candidate, the compile-time guarantee holds only if
`capabilities::context::with_current_async` / `with_current` /
`current_handle` stop being reachable from node-author code. A token is not a
guard if the full bag is one ambient call away. Consequences:

- Ambient read APIs must be deprecated for flow/connector authors and
  eventually restricted to host/runtime crates (visibility move or
  `#[doc(hidden)]` + a CI `clippy::disallowed_methods` gate over
  `examples/`, `connectors/`, `stdlib`).
- Until removal, A2's `ScopedResources` is what keeps ambient access honest:
  ambient calls still resolve to the *scoped* view, so over-reach via ambient
  context is runtime-denied, not silently granted. The two mechanisms are
  complementary during the entire migration window.
- This is **not** a sandbox. A node can always link `reqwest` directly and
  bypass the capability system; the threat model is honest-but-drifting
  authors (human or agent), not malicious code. True confinement exists only
  on the wasm path, where the host mediates every `cap_call` opcode (see open
  question 6).

## Division of labour with A2 (what each layer catches)

A2 runtime denial catches, and continues to catch after this design ships:
- Ambient over-reach from *any* code — node bodies, shared helpers, connector
  op implementations — during the migration window.
- Connector-resolved grants (next section): capability needs derived from
  runtime-resolved connection config cannot be a compile-time fact.
- Defense-in-depth for hand-built registries, host bugs, and guests not built
  by our macro.

Only types catch:
- Over-reach at `cargo check` time, before any deploy — the agent-loop win.
- Declaration/body drift as a *local, span-accurate* diagnostic instead of a
  runtime error attributed to a node alias.

Types do **not** catch (stays runtime):
- Misuse *within* a granted capability (declared `http_write`, calls an
  unexpected URL) — out of scope for both layers today.
- Provider incompatibility (`CAP101` presence, requirement-language checks).
- Anything resolved from `BoundConnection` config at runtime.

## Boundary with connector-resolved (BoundConnection) grants

Connector nodes (`connector_ops(...)`) have a capability surface that is
partly dynamic: `ConnectorRuntime::resolve_required_effect_hints` can add
hints based on the resolved connection (e.g. SheetPort bound connections that
fan out to `blob`), and the wasm bundle expansion injects
`RemoteConnectorRuntime` + `RemoteBlobStore` for any node with connector ops.
That is inherently runtime and stays runtime. The boundary rule:

- **Statically tokenized**: every capability named in `resources(...)` — the
  ~17 `ResourceAccess` families. These become typed parameters/ctx accessors.
- **Runtime-resolved**: `connector_runtime()`, `connector_scope()`, and any
  capability granted *because of* a resolved connection. These are reached
  through a single typed gateway — a `ConnectorCtx` token granted iff the node
  declares `connector_ops(...)` — but the *set* of capabilities behind that
  gateway is checked by A2's scoped view against the hints recorded in
  `bindings.lock` (workstream C2 makes that resolution lock-time, so preflight
  stays static even though the grant set is connection-dependent).

So the type system answers "may this node talk to the connector runtime at
all?"; the lock + runtime view answer "what may this connection's resolution
add?". Durability services (`CheckpointStore` etc.) remain host-internal and
are never tokenized (see `checkpointing-and-durability.md`).

## Recommendation

**Adopt (b), the generated per-node context struct, with (c)'s `Has*` marker
traits implemented on every generated ctx for shared-helper reuse.** Rationale:

- One-place edits: `resources(...)` stays the single declaration site; the
  signature gains exactly one stable parameter. Lowest drift surface for
  agent-authored nodes, and `method not found` on the ctx is the most
  self-explanatory possible diagnostic.
- Infallible accessors delete the double-`Option` ceremony (s11's
  `store_image` loses ~6 lines of error plumbing per access) without weakening
  any claim — CAP101 preflight already guarantees boundness.
- Dyn dispatch via `Arc<dyn>` keeps cost and compile time flat (no
  monomorphization), which candidate (c) cannot promise.
- The ctx struct is the natural replacement for every
  `from_current_resources()`-style ambient helper, giving the migration a
  mechanical recipe.

Candidate (a)'s `Cap<T>` should still land as the *field/parameter type
vocabulary* used inside generated ctx structs and helper signatures
(`fn helper(http: &dyn HttpWrite)` or `Cap<dyn HttpWrite>` for owned moves
into spawned futures) — but not as the primary per-node surface.

### Migration path (orderable, with blast radius)

1. **`capabilities`**: add `Cap<T>`, `MissingCapability`, `Has*` traits.
   Additive; no consumers break. [LOCK-CAPS]
2. **`dag-macros`**: opt-in ctx generation — if the node fn has a second
   parameter, generate the ctx struct + adapter; zero-arg-ctx nodes keep
   today's shape (ambient + A2 denial). trybuild cases for alias/type
   mismatch, undeclared accessor, `Pure` node with ctx param. [LOCK-MACROS]
3. **Shared helpers** (`connectors-std`, llm client setup, `LatticeHttpClient`):
   accept `impl Has*`/`&dyn Trait` parameters; deprecate
   `from_current_resources`. [connectors-std + llm crates]
4. **Examples sweep** (parallelizable per-crate, mirrors A2-r): migrate
   s1–s14 + connector families to ctx style; each crate's A2 enforcement tests
   keep passing unchanged. [LOCK-EXAMPLES]
5. **Flip the default**: non-empty `resources(...)` *requires* the ctx
   parameter (compile error otherwise); ambient read APIs deprecated +
   CI-linted outside host crates. [LOCK-MACROS, LOCK-CAPS]
6. **Remove**: ambient read APIs become `pub(crate)`/host-only; A2's
   `ScopedResources` remains as the connector-grant enforcement point and
   defense-in-depth. [LOCK-CAPS, LOCK-HOSTS]

Steps 1–4 are individually shippable with both models coexisting; the
guarantee upgrades from "runtime-enforced" to "compile-enforced" only at
step 5/6, and the spec (`capabilities-and-binding.md`) should not claim it
earlier.

## Non-goals

- **Not a sandbox**: no confinement of arbitrary Rust (direct `reqwest`,
  filesystem, etc.). Wasm host-side opcode mediation is the confinement story.
- **Not parameter-level capability narrowing** (URL allowlists, key prefixes,
  table scopes) — composable wrapper providers per `resource-catalog.md`, later.
- **No IR/schema change**: hints, preflight, CAP101/CAP11x, and Flow IR JSON
  are untouched; this is purely an authoring-surface and macro change.
- **No tokenization of durability services or host internals.**
- **No change to connector op resolution semantics** (C2 owns lock-time
  resolution).

## Open questions for the maintainer

1. Ctx naming: fixed convention (`{PascalFn}Ctx`) vs explicit
   `resources(as = "NormalizeCtx", ...)` override for collision cases?
2. Should `effects = "Pure"` nodes be *forbidden* a ctx parameter (hard
   compile error), making purity structurally visible? (Proposed: yes.)
3. Ambient removal timeline: deprecation cycle through 0.1.x with the CI lint,
   or a hard break at 0.2? Affects how long A2 must catch ambient over-reach.
4. For connector nodes: should plain `resources(http_write(...))` declarations
   be subsumed by `ConnectorCtx` (auth-applying HTTP only), or remain separate
   tokens alongside it?
5. Determinism-only resources (`clock`, `rng`): tokenize now for symmetry, or
   leave ambient until a `Clock`/`Rng` accessor consolidation pass?
6. Wasm hard enforcement: should the host filter `cap_call` opcodes against
   the node's declared hint set, upgrading wasm to true confinement? Separate
   packet if so.
7. Accessor return shape: `&dyn Trait` borrows (proposed) vs owned
   `Cap<dyn Trait>` clones for nodes that spawn; or both (`x()` / `x_owned()`)?
8. Streaming (`register_stream_fn`) and halt nodes: confirm the adapter shape
   generalizes (it should — same `Fn(In) -> Fut` envelope), and whether
   `CheckpointHandle` should ride the ctx instead of its own task-local.
