Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-01-29

# Workspace Capability (0.1.x)

This document defines the **workspace** capability: a run-scoped, sandboxed file workspace intended
for deterministic file operations inside flows. It replaces host filesystem access in portable flows.

## Goals

- Provide safe, run-scoped file semantics (read/write/list/delete).
- Enforce isolation per flow run by default.
- Support predictable lifecycle policies (discard on completion).
- Map cleanly to native and Workers storage backends.

## Non-goals

- Arbitrary host filesystem access.
- Long-term archival without explicit export.

## Conceptual Model

Each flow run receives a **workspace root**. Paths are always relative to this root and MUST be
normalized (no `..`, no absolute paths).

Workspace entries are **binary-first**; text is handled by caller encoding.

## Operations (Logical API)

This is the conceptual surface; actual Rust traits live in capability crates.

```
workspace.read(path) -> { bytes | blob_ref }
workspace.write(path, data, options) -> { path, size_bytes, updated_at }
workspace.list(prefix?, glob?) -> [entry]
workspace.delete(path) -> { deleted: bool }
```

### Path Rules

- Paths are relative (e.g., `inbox/file.json`).
- Path normalization removes `.` and rejects `..`.
- Paths are UTF-8 and MUST be ASCII-safe by default (host may reject otherwise).

### Entry Metadata

Workspace entries SHOULD include:
- `path`
- `size_bytes`
- `updated_at`
- optional `content_hash`

## Defaults and Retention

**Default policy**: discard workspace contents on run completion.

Optional policy (host-configured):
- `retain_completed_for`: retain workspace contents for a fixed duration.
- `max_total_bytes`: cap per-run workspace size.
- `max_file_count`: cap per-run file count.

Explicit retention should be opt-in and visible to operators.

## Backends

### Native Host

- Backing store: local filesystem or temp directory.
- Root path: `.../runs/{flow_id}/{run_id}/`.
- Enforce permissions and cleanup on completion.

### Workers Host

- Backing store: KV or R2.
- Path mapping: `workspace/{flow_id}/{run_id}/{path}`.
- KV for small files; R2 for large blobs.

## Relationship to Blob Store

Workspace is a **path-based** surface. Blob stores are **content-addressed** or key-addressed.
Implementations may use blobs internally, but flows access workspace entries by path.

If a workspace entry exceeds an inline threshold, implementations MAY store it in a blob store and
return a `blob_ref` for downstream nodes.

## Security and Isolation

- Workspace MUST be isolated per run (default).
- Hosts MAY provide shared workspaces for advanced use cases, but these must be explicit.
- Access to workspace data is scoped to the runtime; flows cannot access host filesystem directly.

## Stdlib Nodes

The standard library should include thin wrappers:

- `std.workspace.read`
- `std.workspace.write`
- `std.workspace.list`
- `std.workspace.delete`

These nodes are deterministic and checkpointable.
