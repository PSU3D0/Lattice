# s9_workspace_artifacts

A documentation-first example for **artifact-heavy flows** that use the Lattice
workspace capability as a run-scoped scratch volume.

## Why this example exists

This example captures the shape of an n8n/Make-style pipeline where nodes need a
shared, temporary filesystem-like area for intermediate artifacts:

- downloads
- extracted archives
- OCR/image conversions
- generated reports
- staged publish payloads
- resumable run-local scratch trees

It is intentionally focused on **workspace semantics**, not durable publication.

## Mental model

- **workspace** = run-scoped named artifact area
- **blob** = durable published artifact store

A typical pattern is:
1. download/source material
2. write intermediates into workspace paths
3. read/list them across downstream nodes
4. optionally halt/resume while preserving the same workspace run scope
5. publish only final outputs elsewhere
6. allow the host to clean the workspace on terminal completion

## Concrete route examples already exercised in workerd tests

See the deployable fixture under:
- `crates/host-workers/workerd-tests/`

Routes:
- `POST /workspace`
  - writes `artifacts/original.txt`
  - writes `artifacts/upper.txt`
  - lists `artifacts/`
  - reads both files back
  - verifies missing read/delete semantics
  - deletes one artifact before returning
- `POST /workspace-resume`
  - writes `resume/input.txt`
  - halts via timer wait
  - resumes with the same stable run id
  - reads the pre-halt artifact after resume
- `POST /workspace-retained`
  - writes a retained artifact without deleting it
  - demonstrates host-owned retention prior to retained cleanup dispatch
- `POST /workspace-quota`
  - validates workers-side quota enforcement
  - exercises `max_single_file_bytes`, `max_total_bytes`, and `max_file_count`
- `POST /workspace-invalid-path`
  - validates traversal rejection before backend operations
- `POST /workspace-mutation`
  - validates overwrite-delta accounting and delete/rewrite counter reset behavior
- `POST /workspace-blocked-prefix`
  - validates blocked-prefix and max depth/length host policy checks

These routes are a good reference for how to think about workspace-backed
artifact farming on Workers.

## Suggested flow shape

```text
trigger
  -> fetch/download/input-normalize
  -> workspace-write(source)
  -> fanout transforms
  -> workspace-write(derived/*)
  -> optional halt/resume boundary
  -> workspace-list(prefix)
  -> workspace-read(selected artifacts)
  -> publish final durable outputs
  -> complete run
```

## Host bindings on Workers

Recommended bindings:
- `WORKSPACE_BUCKET` -> R2 bucket
- `WORKSPACE_DO` -> `WorkspaceDurableObject`

Optional host-policy vars:
- `LATTICE_WORKSPACE_OBJECT_PREFIX`
- `LATTICE_WORKSPACE_MAX_TOTAL_BYTES`
- `LATTICE_WORKSPACE_MAX_FILE_COUNT`
- `LATTICE_WORKSPACE_MAX_SINGLE_FILE_BYTES`
- `LATTICE_WORKSPACE_RETAIN_COMPLETED_FOR_MS`

## Sample payloads

- `payloads/workspace-roundtrip.json`
- `payloads/workspace-resume.json`
- `payloads/workspace-retained.json`
- `payloads/workspace-quota-single-file.json`
- `payloads/workspace-quota-total-bytes.json`
- `payloads/workspace-quota-file-count.json`
- `payloads/workspace-invalid-path-write.json`
- `payloads/workspace-invalid-path-list.json`
- `payloads/workspace-mutation-overwrite.json`
- `payloads/workspace-mutation-delete-rewrite.json`
- `payloads/workspace-blocked-prefix-write.json`
- `payloads/workspace-blocked-prefix-max-depth.json`
- `payloads/workspace-blocked-prefix-max-length.json`

## Key semantic expectations

- prefix-only listing in 0.1
- missing read => `None`
- missing delete => `{ deleted: false }`
- host-owned retention only
- workspace continuity across halt/resume for the same run
- terminal completion cleanup driven by host policy
