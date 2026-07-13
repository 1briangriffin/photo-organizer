# Facial Recognition Rebase Handoff

This file captures the work intentionally deferred from PR 4 of
`PLAN_catalog_state_refactor.md`.

## Context

`feature/catalog-maintenance` now owns the catalog-state schema and action model:

- `command_runs`
- `file_observations`
- `file_location_state`
- `run_actions`
- `media_metadata.camera_serial_number`
- `media_metadata.camera_file_number`
- face schema tables under schema migration v4
- face DB primitives in `photo_organizer.faces.db_ops`

The current schema version on main is `8`:

- v3: catalog observations, location state, run actions, relationship provenance
- v4: face tables
- v5: per-run run-action idempotency
- v6: camera identity metadata for RAW reconciliation
- v7: proposal lifecycle columns on `run_actions`
  (`resolved_by_run_id`, `resolved_at`, `resolution_note`) plus
  auto-supersede semantics and `photo-catalog-query` review commands
  (`--show-proposals`, `--reject-proposal`)
- v8: file retirement status on `files` (`status`, `status_changed_at`,
  `status_changed_by_run_id`, `status_note`) with
  `photo-catalog-query --retire-file / --restore-file / --list-retired`

The older `feature/facial-recognition` branch has real face CLI/workflow modules,
but they were built against an earlier `faces` / `persons` table shape. Do not
merge those modules directly without adapting them to the v4 face schema and
run/action contract.

## Rebase Steps

1. Rebase `feature/facial-recognition` onto the branch that contains the
   catalog-state work.
2. Confirm the rebased branch preserves schema version `8` as the base. Any new
   face workflow schema changes must become version `9` or later.
3. Keep the v4 face table names and semantics:
   - `face_detections`
   - `face_embeddings`
   - `face_clusters`
   - `face_cluster_members`
   - `face_persons`
   - `face_person_links`
4. Replace old direct writes to `faces`, `persons`, and old `face_clusters`
   columns with the primitives in `photo_organizer.faces.db_ops`.
5. Add a `photo-faces` console entry only after the adapted command tests pass.
6. Ensure every command creates or receives a `command_runs.id` and records
   provenance through face tables and `run_actions`.
7. Keep high-volume model outputs in face-specific tables:
   - detections in `face_detections`
   - embeddings in `face_embeddings`
   - clusters/memberships in `face_clusters` and `face_cluster_members`
   - user-accepted identity links in `face_person_links`

## Expected Command Mapping

| Command | Required catalog behavior |
|---|---|
| `faces_detect` | Records model observations: detections and embeddings with model metadata and run provenance. |
| `faces_cluster` | Records cluster proposals as `run_actions.status='proposed'`; accepted cluster membership must remain reviewable. |
| `faces_seed` | Creates or updates `face_persons` and accepted links with `created_by_run_id` / `updated_by_run_id`. |
| `faces_refine` | Records reviewed merge/split/relabel decisions as auditable actions. |
| `faces_link` | Creates accepted `face_person_links` with applied action provenance. |

## Branch Boundary

Already complete on `feature/catalog-maintenance`:

- schema tables for face state
- low-level face DB operations
- run-action recording primitives
- command-run infrastructure

Port progress (branch `feature/faces-port`, porting modules instead of a
literal rebase — both sides rewrote `schema.py` and `faces/db_ops.py`, so a
rebase would only produce conflicts):

- DONE (phase 1): `photo-faces scan` — detection + embeddings on the v4 face
  tables with run provenance; `get_unscanned_files` (JPEG/TIFF default,
  `--include-raw` opt-in, linked RAWs always excluded, retired files
  excluded); no-faces sentinel rows (`detection_index = -1`,
  `status='no_faces'`) so scans are incremental; thumbnails in detection
  payload; `photo-faces` console entry (scan command tests pass).
- DONE (phase 2): schema v9 (`face_persons.birth_date`; `era_start` /
  `era_end` / `representative_embedding` / `representative_dim` on
  `face_clusters`) and `photo-faces cluster` — era-based HDBSCAN with
  birth-date developmental windows, proposals into `face_clusters` /
  `face_cluster_members` (the member writer gap is closed:
  `propose_cluster_assignment` now writes membership rows), re-clustering
  supersedes prior proposed clusters via stable era-based cluster keys while
  accepted state is never touched.

- DONE (phase 3): `photo-faces link` — cross-age merge suggestions as
  `face_cluster_merge` run_actions proposals (weighted multi-signal scoring:
  embedding/co-occurrence/age-progression/temporal/supervised anchors),
  reviewable and rejectable through `photo-catalog-query`; re-linking
  supersedes stale suggestions via the standard scope supersede. The old
  branch's always-true era-gap check was fixed (MAX_ERA_GAP_YEARS, default
  1.0 — identities chain across decades transitively). UnionFind ported for
  the acceptance phase.

Still expected on this port:

- applying accepted merges: union-find over accepted `face_cluster_merge`
  actions -> person creation + accepted cluster memberships/links
- seed-from-YAML (persons + birth dates), refinement, and review workflows
  on the run_actions proposal lifecycle
- Streamlit review app (kept as the review surface)
- reports/queries specific to face state (photos-for-person, timelines)

## Required Tests

- CLI smoke tests prove the `photo-faces` entry point dispatches each command.
- Detection records are tied to a run and do not create accepted labels.
- Cluster generation can be previewed without mutating accepted labels.
- Seed/review/link commands create accepted state with run provenance.
- Re-running a face command is idempotent within the same run and auditable
  across separate runs.
- Query/report tests use the new `face_*` tables, not legacy `faces` /
  `persons` tables.
- Schema migration tests prove any new face-specific migration starts after
  version `8` and does not regress the catalog-maintenance migrations.

## Non-Goals For The Rebase

- Do not reintroduce the legacy face schema.
- Do not bypass `run_actions` for accepted user/review decisions.
- Do not store high-volume embeddings in `file_observations`; embeddings belong
  in `face_embeddings`.
