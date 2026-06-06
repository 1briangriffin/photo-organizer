# Facial Recognition Rebase Handoff

This file captures the work intentionally deferred from PR 4 of
`PLAN_catalog_state_refactor.md`.

## Context

`feature/catalog-maintenance` now owns the catalog-state schema and action model:

- `command_runs`
- `file_observations`
- `file_location_state`
- `run_actions`
- face schema tables under schema migration v4
- face DB primitives in `photo_organizer.faces.db_ops`

The older `feature/facial-recognition` branch has real face CLI/workflow modules,
but they were built against an earlier `faces` / `persons` table shape. Do not
merge those modules directly without adapting them to the v4 face schema and
run/action contract.

## Rebase Steps

1. Rebase `feature/facial-recognition` onto the branch that contains the
   catalog-state work.
2. Keep the v4 face table names and semantics:
   - `face_detections`
   - `face_embeddings`
   - `face_clusters`
   - `face_cluster_members`
   - `face_persons`
   - `face_person_links`
3. Replace old direct writes to `faces`, `persons`, and old `face_clusters`
   columns with the primitives in `photo_organizer.faces.db_ops`.
4. Add a `photo-faces` console entry only after the adapted command tests pass.
5. Ensure every command creates or receives a `command_runs.id` and records
   provenance through face tables and `run_actions`.

## Expected Command Mapping

| Command | Required catalog behavior |
|---|---|
| `faces_detect` | Records model observations: detections and embeddings with model metadata and run provenance. |
| `faces_cluster` | Records cluster proposals as `run_actions.status='proposed'`; accepted cluster membership must remain reviewable. |
| `faces_seed` | Creates or updates `face_persons` and accepted links with `created_by_run_id` / `updated_by_run_id`. |
| `faces_refine` | Records reviewed merge/split/relabel decisions as auditable actions. |
| `faces_link` | Creates accepted `face_person_links` with applied action provenance. |

## Required Tests

- CLI smoke tests prove the `photo-faces` entry point dispatches each command.
- Detection records are tied to a run and do not create accepted labels.
- Cluster generation can be previewed without mutating accepted labels.
- Seed/review/link commands create accepted state with run provenance.
- Re-running a face command is idempotent within the same run and auditable
  across separate runs.
- Query/report tests use the new `face_*` tables, not legacy `faces` /
  `persons` tables.

## Non-Goals For The Rebase

- Do not reintroduce the legacy face schema.
- Do not bypass `run_actions` for accepted user/review decisions.
- Do not store high-volume embeddings in `file_observations`; embeddings belong
  in `face_embeddings`.
