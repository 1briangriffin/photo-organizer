# Catalog State Management Refactor - Implementation Status

## Status

Implemented on `feature/catalog-maintenance`.

This document began as the design plan for separating observed catalog reality
from accepted catalog state. The branch now implements the planned file
maintenance work and the schema foundation for face workflows. The remaining
work is listed under "Deferred / Future Work" rather than in the original
rollout-plan form.

## Implemented Schema

Current schema version: `6`.

### Migration 3: Catalog State Foundation

Implemented:

- `file_observations`
- `file_location_state`
- `run_actions`
- path-key normalization for new path-bearing state
- backfill of `file_location_state` from `files.dest_path` and
  `file_occurrences`
- relationship provenance columns:
  - `raw_outputs.created_by_run_id`
  - `raw_sidecars.created_by_run_id`
  - `psd_source_links.created_by_run_id`

### Migration 4: Face State Primitives

Implemented:

- `face_persons`
- `face_detections`
- `face_embeddings`
- `face_clusters`
- `face_cluster_members`
- `face_person_links`

The branch includes DB primitives in `photo_organizer.faces.db_ops`, but the
real facial-recognition CLI/workflow adoption is deferred until the
facial-recognition branch is rebased. See
`PLAN_facial_recognition_rebase_handoff.md`.

### Migration 5: Run Action Attempt History

Implemented:

- changed `run_actions` idempotency from globally unique
  `idempotency_key` to unique `(proposed_by_run_id, idempotency_key)`
- retained a non-unique index on `idempotency_key`
- preserves proposed/applied/failed action history across separate runs

### Migration 6: RAW Camera Identity Metadata

Implemented:

- `media_metadata.camera_serial_number`
- `media_metadata.camera_file_number`
- index on `(camera_model, camera_serial_number, camera_file_number)`

Normal organization and ingest scans now attempt to populate these fields for
RAW files through ExifTool when available. RAW edit-aware reconciliation also
extracts these values from candidate CR2 files at match time.

## Implemented Behavior

### Observation / Proposal / Apply Contract

Implemented:

- dry-runs may persist observations and proposed actions
- dry-runs do not mutate accepted catalog state or move/copy files
- real runs apply accepted catalog mutations and filesystem operations
- failed real-run apply phases mark proposed actions as `failed` where possible

Transaction classes now map to:

| Class | Current behavior |
|---|---|
| Observation | `file_observations`, `file_location_state`, and observed identity rows persist. |
| Proposal | `run_actions.status='proposed'` persists for dry-runs. |
| Apply | accepted state and filesystem mutations occur only on real runs. |

### PR 1: Schema and Passive Recording

Completed.

Implemented:

- versioned migration framework and migration tests
- observation recording for destination validation/sync and pipeline discovery
- `file_location_state` materialization
- path-key normalization tests
- dry-run observation persistence without accepted-state mutation

### PR 2: Durable Actions for `ingest-dest`

Completed.

Implemented:

- proposed `run_actions` for canonical RAW path updates
- proposed/applied RAW-output, RAW-sidecar, and PSD-source link actions
- proposed/applied move/copy actions
- dry-run action persistence
- real-run action application and status tracking
- created-by run provenance for accepted relationship rows

### PR 3: Organize and Sync Adoption

Completed.

Implemented:

- organize pipeline records run actions while preserving existing behavior
- `sync-dest` records canonical path update actions
- validation reports accepted path versus observed path
- run-scoped action history works across organize, ingest, sync, and validate

### PR 4: Face Schema Adoption

Partially completed by design.

Completed:

- face schema tables
- face DB operation primitives
- run/action provenance support for face primitives

Deferred:

- rebasing/adapting the real facial-recognition CLI and workflows
- creating/enabling the `photo-faces` console entry
- replacing legacy face branch table writes with `face_*` table operations

### PR 5: RAW Edit-Aware Reconciliation

Completed.

Implemented:

- detects RAW files renamed and modified by Canon DPP or similar editors when
  byte hashes no longer match
- records `modified_rename_candidate` observations
- proposes canonical path updates using
  `method='raw_edit_metadata_same_directory'`
- keeps ambiguous RAW edit candidates review-only instead of importing them as
  ordinary new RAWs
- surfaces review-required RAW edit candidates in validation CSV output
- uses CR2 `FileNumber` from ExifTool as a strong disambiguation signal
- normalizes Canon `FileNumber` values such as `100-4678`, `1004678`, and
  `4678` to the same camera sequence

## Tier 1 Ingest-Dest Fixes Absorbed

The earlier Tier 1 ingest-dest plan is complete and superseded by this durable
catalog-state work. Implemented behavior includes:

- detect/apply split for destination rename reconciliation
- Phase B candidate scoping via `PipelineCandidates`
- scoped planner, linker, linked-destination assignment, and mover behavior
- dual-key full/sparse hash lookup for rename detection
- opportunistic full-hash upgrade when sparse fallback computed a full hash
- editor-export RAW-to-JPEG linking via current RAW stem and capture datetime
- dry-run preview correctness for DPP-style RAW rename + JPEG export workflows

See `PLAN_ingest_dest_fixes.md` for the Tier 1 completion note.

## Implemented Validation / Reporting Changes

Implemented:

- `--validate-dest` reports:
  - confirmed
  - missing
  - untracked
  - renamed
  - moved
  - review-required ambiguous RAW edit candidates
- accepted-vs-observed section shows current catalog path versus observed path
- ambiguous RAW edit candidates are no longer hidden behind only a missing count

Not yet implemented:

- multiple CSV outputs by category
- `.xlsx` workbook with one tab per category
- default omission/summary of the very large confirmed section

## Production Catalog Notes

Observed/verified during production catalog cleanup and smoke testing:

- earlier dry-run orphan RAW rows caused by edited/rotated CR2 files were
  manually reconciled
- RAW edit-aware reconciliation prevented the same issue from recurring
- schema migration reached version `6`
- validation after RAW camera file-number support reduced the problematic
  missing set to the intentionally deleted CR2
- real `--ingest-dest --move` was run after the dry-run workflow was reviewed

## Deferred / Future Work

### Reporting UX

- replace the large single validation CSV with multiple CSVs or an `.xlsx`
  workbook
- put exception categories first
- optionally omit or summarize confirmed rows by default

### Intentional Deletion State

- add an accepted-state way to mark a catalog file as intentionally deleted,
  retired, or no longer expected
- use that state so intentionally removed files do not remain permanently
  reported as `missing`

### RAW Metadata Backfill

- add a rescan/backfill command that populates `camera_serial_number` and
  `camera_file_number` for existing RAW catalog rows
- optionally update accepted metadata from a matched edited RAW when a RAW
  edit-aware canonical path update is applied

### Proposal Lifecycle (implemented, schema v7)

Decisions taken:

- dry-run proposals do NOT remain pending indefinitely: recording a newer
  `proposed`/`applied`/`skipped` action for the same idempotency key
  auto-supersedes older pending proposals (`RunActionRecorder`), and each
  successful run also supersedes leftover proposals from earlier runs of the
  same command scope (`pipeline.lifecycle.supersede_stale_proposals`; scope =
  tool + command + src_root + dest_root). A `failed` attempt leaves the prior
  proposal pending.
- applying a previous dry-run by `run_id` is NOT supported; real runs always
  recompute against current disk state. Proposals are review artifacts, not
  work queues.
- users can reject pending proposals explicitly:
  `photo-catalog-query --reject-proposal <id ...> [--note ...]` (records its
  own command run; `resolved_by_run_id` / `resolved_at` / `resolution_note`
  are set). Rejection is a review dismissal — a later dry-run that still
  observes the same state will re-propose under a new row.
- review surface: `photo-catalog-query --show-proposals`
  (`--action-type`, `--action-status`, `--run-id`, `--limit`, `--csv-output`).

### Database Growth / Maintenance

- define retention and compaction policy for `file_observations`
- define retention and compaction policy for `run_actions`
- define retention policy for future high-volume face detections/embeddings
- add maintenance/reporting queries for table sizes and stale proposals

### Path / Location Model

- decide whether `file_occurrences` remains as compatibility state or is
  eventually replaced by `file_observations` / `file_location_state`
- decide whether `path_key` remains lexical-only or resolves
  symlinks/junctions where available

### Naming Clarity

- consider eventually renaming `files.dest_path` to `canonical_dest_path`

### Facial Recognition

- rebase `feature/facial-recognition` after this branch lands
- adapt real face workflows to the v4 `face_*` schema and run/action
  provenance model
- keep embeddings in `face_embeddings`, not `file_observations`

## Current Test Coverage

The branch includes focused coverage for:

- schema migrations, including failed migration rollback behavior
- path identity normalization
- observation recording and materialized location state
- run action recording, apply provenance, per-run idempotency, and failed
  action status
- ingest-dest dry-run and real-run behavior
- scoped planner/linker/mover behavior
- sparse/full hash rename reconciliation
- editor-export RAW-to-JPEG linking
- RAW edit-aware reconciliation and ambiguous review behavior
- Canon CR2 file-number disambiguation
- validation reporting for accepted-vs-observed and review-required rows
- face DB primitives and run-action provenance

Latest full suite run during implementation:

```text
196 passed
```
