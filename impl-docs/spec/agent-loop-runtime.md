Status: Draft
Purpose: architecture-decision / spec
Owner: Runtime
Last reviewed: 2026-03-31

# Agent Loop Runtime (0.1.x)

This document defines how bounded AI/agent loops fit into Lattice.

It addresses a design tension:
- an agent loop is not a good fit for plain DAG topology because it is
  iterative/circular,
- but it also should not become an ungoverned blob of ambient host powers.

## Decision summary

A bounded agent loop should be modeled as:

- **one graph-visible semantic node**,
- with an **internal bounded multi-step execution loop**,
- using a **declared tool set**,
- and producing **traceable step records**.

Agent loops are therefore:
- **not** graph-level cycles,
- **not** first-class Flow IR topology loops,
- **not** implicit ambient runtime powers.

They are a node-local orchestration runtime.

## Why this decision exists

A graph-level representation of every model/tool/model/tool step would:
- bloat topology,
- create the wrong retry boundaries,
- misrepresent semantic orchestration,
- make dynamic tool-choice loops awkward or impossible.

An unconstrained “run an agent inside arbitrary Rust” model would:
- weaken deployment validation,
- obscure tool/capability usage,
- make operator visibility much worse,
- undermine trust in the node contract.

The chosen model keeps the boundary honest:
- topology sees one semantic unit,
- execution retains bounded internal loop behavior,
- deployment/governance still knows what the node is allowed to do.

## Primary rule

An agent loop is a **semantic node-local runtime pattern**.

The loop may internally perform multiple steps, but the node boundary remains
responsible for truthfully declaring:
- effects,
- determinism,
- resource hints,
- connector/tool dependencies,
- and durability behavior if later supported.

## Conceptual runtime shape

A bounded loop should carry configuration such as:

```rust
pub struct AgentLoopConfig {
    pub max_steps: u32,
    pub stop_when: StopCondition,
    pub allowed_tools: Vec<ToolId>,
    pub tool_choice_policy: ToolChoicePolicy,
}
```

This is conceptual only for 0.1.x.

### Core ideas
- `max_steps` prevents infinite loops
- `stop_when` allows configurable termination policies
- `allowed_tools` constrains what the model may invoke
- `tool_choice_policy` allows phased or policy-controlled behavior

## Step lifecycle concepts

The runtime should support concepts analogous to:
- `prepare_step`
- `on_step_finish`
- `stop_when`
- step budgets

These correspond to patterns seen in external agent SDKs, but the Lattice
interpretation is different:
- the loop remains inside one semantic node,
- the tools remain declared and governable,
- the host/runtime can observe the step trace without turning each step into a
  topology edge.

## Tool model

### Tools are not ambient powers

Tools exposed to an agent loop must not be ad hoc ambient closures with
undeclared host access.

Instead, tools should be backed by one of:
- connector operations,
- stdlib operations,
- explicitly declared custom operations.

### Tool declaration rule

An agent-loop node should declare the set of tool operations it may invoke.

This is the same philosophy used in:
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`

So the node’s contract is derived from:
1. the model operation(s) it performs, and
2. the tool set it is allowed to invoke.

## Effect and determinism envelope

The loop node’s semantic envelope should be the least-permissive truthful
combination that covers:
- model calls,
- tool calls,
- and any local side-effects.

### Default posture

Most agent-loop nodes will default to:
- `Effects::Effectful`
- `Determinism::Nondeterministic`

because:
- model calls are external/billable,
- tool calls often involve external effects,
- repeated loop execution can vary across retries.

## Observability

Although loop steps are not graph topology, they should still be observable.

The runtime should be able to surface per-step information such as:
- step number,
- prompt/message summary,
- tool calls requested,
- tool results returned,
- finish reason,
- cumulative usage,
- final output summary.

This should be an execution trace / telemetry concern, not a topology concern.

## Retry semantics

The retry unit for an agent loop is normally the **entire semantic node**, not
individual internal steps.

This is intentional:
- the loop is one orchestration unit,
- the node author chooses that retry granularity,
- internal tool/model interplay is part of that semantic unit.

If an author wants step boundaries exposed explicitly, they should build an
explicit flow with canonical nodes instead of a loop node.

## Relationship to canonical AI nodes

Both of these should exist:

1. **Canonical graph-visible AI nodes**
   - `ai.complete`
   - `ai.extract_structured`
   - `ai.generate_image`
   - etc.

2. **A bounded agent-loop runtime used inside a semantic node**

The first is best for:
- simple composition,
- examples,
- generators,
- operator readability.

The second is best for:
- richer tool-using behaviors,
- bounded internal loops,
- collapsing multiple steps into one semantic retry unit.

## Relationship to subflows

Subflows may still be useful around agentic patterns, but a bounded agent loop
itself should **not** be modeled primarily as a subflow.

Reasons:
- the loop is dynamic,
- it may be circular/iterative,
- step count varies at runtime,
- topology would misrepresent the real execution pattern.

So:
- subflows remain useful for reusable orchestration around agent nodes,
- agent loops themselves remain node-local runtimes.

## Durability posture for 0.1.x

The first implementation pass should assume:
- bounded, in-memory step loops,
- no full halt/resume semantics inside the loop runtime,
- no requirement to checkpoint every internal step.

Later follow-on work may consider:
- checkpointing between steps,
- explicit resume contracts,
- time/token budget preemption,
- operator inspection of paused loops.

That is follow-on work, not a first requirement.

## Implications for examples

The first serious AI example should **not** require the agent loop.

Recommended progression:
1. explicit graph-visible AI operations first,
2. bounded agent-loop example second.

This keeps the first worked example focused on:
- transport portability,
- auth/bindings,
- structured output,
- image generation,
- workspace persistence,
- Workers deployment.

## Suggested future runtime surface

A future runtime may expose concepts like:
- family-scoped AI clients from current context,
- a declared tool registry,
- bounded loop runner,
- step callbacks/policies,
- structured step trace emission.

But 0.1.x should avoid overcommitting to exact APIs before the first explicit AI
example and bridge layer land.

## Non-goals for 0.1.x

This document does **not** require:
- graph-level cycle support,
- a new Flow IR loop primitive,
- a requirement that every internal tool/model step be visible as topology,
- immediate durability/resume support for bounded loops,
- unbounded autonomous agent behavior.

## Cross-references

- `impl-docs/spec/ai-surface-and-layering.md`
- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`
- `impl-docs/spec/external-sandbox-dispatch-and-callback-resume.md`
- `impl-docs/spec/llm-lead-intake-example.md`
