# Tier 1 Ingest-Dest Fixes - Complete

## Status

Fully implemented.

This plan originally described the smallest safe fix for
`--ingest-dest --dry-run --move` before the durable catalog-state refactor.
Those Tier 1 fixes landed and were later absorbed into the broader
`feature/catalog-maintenance` work documented in
`PLAN_catalog_state_refactor.md`.

## Original Problem

The DPP workflow exposed three related bugs:

1. Destination rename reconciliation could be detected but remained invisible
   to Phase B during dry-run.
2. Phase B planner/linker/mover queries were global, so stale unrelated rows
   could be resurrected or planned.
3. DPP/other editor JPEG exports next to renamed RAWs were not linked because
   linker matching used stale `orig_name` stems instead of the RAW's current
   on-disk stem.

The catalog also had many sparse-hash-only rows, so rename detection needed to
check both full and sparse identity keys.

## Implemented Fixes

### Detect/Apply Split

Implemented:

- destination sync can detect renames without immediately applying them
- `SyncReport.renames` carries structured `RenameRecord` data
- `DestinationSyncer.apply_renames(report)` applies detected renames inside
  the caller's transaction/savepoint
- ingest-dest applies renames inside Phase B so dry-run rollback and real-run
  failure rollback behave consistently

### Candidate-Scoped Phase B

Implemented:

- `PipelineCandidates` tracks the run's working set
- both discoverers populate `DiscoveryResult.imported_file_ids`
- renamed file IDs are added to the candidate set
- planner, linked-destination assignment, linkers, and mover are scoped to the
  candidate set
- rename-only ingest-dest runs proceed into Phase B instead of exiting early

### Editor Export Linking

Implemented:

- RAW side reads current stem from `dest_path`
- editor-export matching links outputs where:
  - output `orig_name` stem matches current RAW stem
  - capture datetime matches
- method is `editor_export_identity`
- ambiguity guard skips auto-linking if the RAW-side key resolves to multiple
  RAW rows

### Hash Reconciliation

Implemented:

- rename detection uses full hash first and sparse hash as fallback
- sparse-only catalog rows can be matched when the observed file escalates to
  full hashing
- `update_dest_path_atomic` opportunistically stores a full hash when available
- full-hash uniqueness conflicts are logged and do not abort the rename

### Regression Coverage

Implemented tests cover:

- detect-only destination sync leaves accepted state unchanged
- applying structured rename records updates accepted destination paths
- sparse-only rename detection
- full-hash upgrade and conflict handling
- scoped mover behavior
- scoped planner/linker behavior
- editor-export RAW-to-JPEG linking
- DPP-style ingest-dest dry-run preview
- rename-only ingest-dest runs
- rollback on Phase B failure
- command-run error recording
- rerun behavior when a prior dry-run already created candidate JPEG rows

## Superseded By Durable Catalog State

The Tier 1 plan deliberately avoided schema work. The broader
catalog-maintenance branch now adds:

- `file_observations`
- `file_location_state`
- `run_actions`
- relationship provenance columns
- per-run run-action idempotency
- RAW edit-aware reconciliation
- CR2 camera file-number metadata support

Those additions are the long-term implementation path. This Tier 1 document is
kept only as historical context for the first ingest-dest bugfix slice.

## Remaining Work

No remaining Tier 1 items.

Future work now belongs in `PLAN_catalog_state_refactor.md`, especially:

- validation report UX cleanup
- accepted-state handling for intentionally deleted files
- proposal lifecycle management
- database retention/maintenance policy
