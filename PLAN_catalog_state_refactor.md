# Plan: Catalog State Management Refactor

## Purpose

Tier 1 fixed the immediate `--ingest-dest --dry-run --move` failure mode by
making Phase B run-scoped and savepoint-safe. It did not resolve the deeper
architecture issue: the catalog still has no durable, first-class model for
"what was observed on disk" separate from "what the catalog expects or plans to
do."

This plan describes the remaining refactor needed for long-term catalog
maintenance and for the facial-recognition workflows on `feature/facial-recognition`.

## Core Problem

The current schema and pipeline conflate several different kinds of state:

| Current field/table | Current meaning | Problem |
|---|---|---|
| `files.orig_path` | First/canonical source path | Not enough to represent current observed location after catalog maintenance. |
| `files.dest_path` | Planned destination, applied destination, expected catalog path, rename target | Too many meanings. A dry-run rename would have to mutate this field to reflect reality. |
| `file_occurrences` | Some observed paths for a file | Not run-scoped, not clearly current, and not sufficient as a durable observation/audit model. |
| link tables | Accepted links, sometimes inferred during pipeline execution | No durable proposal/review/apply boundary. |
| `command_runs.stats_json` | Run summary | Useful audit summary, but not enough to reconstruct which facts/actions a run observed or proposed. |

The most visible symptom is the dry-run inconsistency:

- New JPEG rows persist during dry-run because they are treated as observed
  reality.
- RAW rename reconciliation rolls back during dry-run because applying it
  currently requires mutating `files.dest_path`.

Both facts are observations, but the schema can only persist one of them without
also changing canonical catalog state.

## Goals

1. Persist observed on-disk reality independently from planned or accepted
   catalog changes.
2. Let dry-runs persist observations while rolling back or merely recording
   proposed actions.
3. Make `files.dest_path` mean one thing: the catalog's accepted/canonical
   destination path.
4. Make previews rerunnable and auditable without relying on in-memory
   `PipelineCandidates`.
5. Give facial-recognition operations the same run/observation/action model as
   file-maintenance operations.
6. Keep migration incremental. Do not rewrite the scanner, syncer, linker,
   planner, and mover in one PR.

## Non-Goals

- Do not merge user-facing CLI modes. `organize`, `ingest-dest`, `sync-dest`,
  validation, reports, and future face commands can stay distinct.
- Do not replace `DiskScanner`, `DestinationSyncer`, or the face detection
  pipeline wholesale.
- Do not require a new service process or background worker.
- Do not make SQLite support truly concurrent mutating catalog workflows. WAL
  helps, but SQLite remains single-writer.

## Target Mental Model

The catalog should distinguish four layers:

| Layer | Meaning | Dry-run behavior |
|---|---|---|
| Identity | Content identity: file row, hashes, stable metadata | Persist if observed. |
| Observation | A run saw a file/path/hash/face/embedding on disk or in an image | Persist. |
| Proposal | A run proposes a move, canonical path update, link, label, or face merge | Persist as proposed, but do not apply unless requested. |
| Accepted state | The catalog's current canonical paths, links, labels, and applied actions | Mutate only on real apply. |

Under this model, the DPP workflow becomes:

1. Dry-run observes RAW IDs `19318-19320` at the `Evie_napping...CR2` paths.
2. Dry-run observes 3 JPEGs at their current paths.
3. Dry-run proposes canonical RAW path updates, 3 RAW-to-JPEG links, and 3 JPEG moves.
4. Dry-run leaves `files.dest_path`, `raw_outputs`, and moved files unchanged.
5. A real run applies the accepted proposals.

## Transaction and Dry-Run Contract

The refactor should stop using "do work, then roll it back" as the primary
dry-run mechanism. That pattern caused the current inconsistency: newly
observed JPEG rows can persist because they are treated as observed reality,
while RAW rename reconciliation disappears because it currently requires a
`files.dest_path` mutation.

Instead, every pipeline phase should fall into one of three transaction
classes:

| Transaction class | Meaning | Dry-run behavior |
|---|---|---|
| Observation transaction | Durable facts about what was seen on disk or in media content. | Always commit. |
| Proposal transaction | Durable proposed actions derived from observations. | Always commit as `run_actions.status='proposed'`. |
| Apply transaction | Accepted catalog-state or filesystem mutations. | Skip entirely, or rollback if entered defensively. |

The core rule is:

> Dry-run may write facts and proposals, but must not write accepted decisions
> or mutate files on disk.

Suggested file-maintenance phase contract:

| Phase | Transaction class | Typical writes | Dry-run behavior |
|---|---|---|---|
| `10 observe` | Observation | `file_observations`, `file_location_state` | Commit. |
| `20 identity_upsert` | Observation / identity | `files`, `media_metadata` for real observed files | Commit if the file truly exists and identity was observed. |
| `30 canonical_path_propose` | Proposal | `run_actions(action_type='update_canonical_dest_path')` | Commit as proposed. |
| `40 canonical_path_apply` | Apply | `files.dest_path`, accepted location state | Skip. |
| `50 relationship_propose` | Proposal | `run_actions` for RAW-output, RAW-sidecar, PSD-source links | Commit as proposed. |
| `60 relationship_apply` | Apply | `raw_outputs`, `raw_sidecars`, `psd_source_links` | Skip. |
| `70 destination_propose` | Proposal | `run_actions` for destination assignment and move/copy targets | Commit as proposed. |
| `80 destination_apply` | Apply | accepted `files.dest_path` assignments | Skip. |
| `90 filesystem_apply` | Apply | move/copy files on disk | Skip. |
| `100 post_apply_observe` | Observation | final path observations and `file_location_state` updates | Usually real-run only because no move/copy occurred in dry-run. |

Workflows do not need to execute every phase. The phase numbers are a shared
ordering vocabulary, not a requirement that every command run every phase.

Examples:

- Initial `organize` and net-new source organization generally use phases 10,
  20, 50, 60, 70, 80, 90, and 100.
- `--ingest-dest` generally uses phases 10, 20, 30, 40, 50, 60, 70, 80, 90,
  and 100 because destination rename reconciliation can affect link matching.
- Future face workflows should extend the same observe/propose/apply contract
  with face-specific phases rather than forcing face detection and labeling
  into file-maintenance phases.

Important dry-run planning requirement:

- Linkers and planners must be able to query an effective current path from
  run observations or `file_location_state`.
- They must not require mutating `files.dest_path` just to see a renamed RAW's
  observed name during dry-run.
- `record_post_move_observation` records final observed file locations. It must
  not create RAW-output or other accepted relationships; those belong to
  relationship apply actions.

## Proposed Schema Additions

### Path Identity Normalization

Before adding durable location state, define a path identity policy. The
catalog currently stores `str(Path(...))` in several places, which is not enough
for durable uniqueness across case-insensitive filesystems, slash differences,
relative paths, symlinks/junctions, and multiple roots.

New path-bearing tables should store both display and lookup forms:

| Column | Meaning |
|---|---|
| `path` | Original/display path as recorded for reports and operator review. |
| `path_key` | Normalized lookup key used for uniqueness and joins. |
| `root_kind` | Logical root category such as `source`, `dest`, `unknown`, or future face/media roots. |
| `root_id` / `root_path_key` | Optional future extension if multiple roots of the same kind need independent identity. |

Initial normalization policy:

- Resolve absolute paths where safe and available.
- Normalize separators.
- Strip trailing separators except for filesystem roots.
- On Windows, case-fold `path_key`.
- Preserve the original `path` for display and CSV output.
- Use `path_key` for unique constraints and matching, not `path`.

Open implementation decision:

- Whether `path_key` should resolve symlinks/junctions or only normalize the
  lexical path. Lexical normalization is safer and less surprising for
  removable drives; full resolution may better deduplicate aliases.

### `file_observations`

Append-only facts about file/path state observed during a command run.

```sql
CREATE TABLE file_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES command_runs(id),
    file_id INTEGER REFERENCES files(id),
    observed_at TEXT NOT NULL,
    observation_type TEXT NOT NULL,
    path TEXT NOT NULL,
    path_key TEXT NOT NULL,
    root_kind TEXT NOT NULL,
    root_path_key TEXT,
    hash TEXT,
    sparse_hash TEXT,
    hash_is_sparse INTEGER NOT NULL DEFAULT 0,
    size_bytes INTEGER,
    mtime REAL,
    match_method TEXT,
    confidence INTEGER,
    payload_json TEXT
);
```

Recommended indexes:

```sql
CREATE INDEX idx_file_observations_run_id ON file_observations(run_id);
CREATE INDEX idx_file_observations_file_time ON file_observations(file_id, observed_at);
CREATE INDEX idx_file_observations_path_key ON file_observations(path_key);
CREATE INDEX idx_file_observations_type_run ON file_observations(observation_type, run_id);
```

Suggested `observation_type` values:

| Value | Meaning |
|---|---|
| `present` | File was seen at this path. |
| `missing_expected` | Canonical/expected path was absent. |
| `renamed_candidate` | File matched an existing row in the same directory under a different name. |
| `moved_candidate` | File matched an existing row in a different directory. |
| `modified_rename_candidate` | File likely matches an existing missing row after an editor modified RAW bytes/metadata, so hash equality no longer holds. |
| `new_candidate` | File was not matched to an existing row and may need ingest. |

Important contract:

- This table records observed facts, not accepted catalog mutations.
- A dry-run may write here.
- Rows are run-scoped and should never be deleted by rollback of the planning
  savepoint.

### `file_location_state`

Materialized latest-known path state for operational queries.

```sql
CREATE TABLE file_location_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    path TEXT NOT NULL,
    path_key TEXT NOT NULL UNIQUE,
    root_kind TEXT NOT NULL,
    root_path_key TEXT,
    status TEXT NOT NULL,
    first_observed_run_id INTEGER REFERENCES command_runs(id),
    last_observed_run_id INTEGER REFERENCES command_runs(id),
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    hash TEXT,
    sparse_hash TEXT,
    hash_is_sparse INTEGER NOT NULL DEFAULT 0,
    size_bytes INTEGER,
    mtime REAL
);
```

Recommended indexes and constraints:

```sql
CREATE INDEX idx_file_location_state_file_status ON file_location_state(file_id, status);
CREATE INDEX idx_file_location_state_root_path ON file_location_state(root_kind, path_key);
CREATE INDEX idx_file_location_state_last_run ON file_location_state(last_observed_run_id);
```

Suggested `status` values:

| Value | Meaning |
|---|---|
| `present` | Most recent observation saw this file at this path. |
| `missing` | Most recent observation expected this path but did not find it. |
| `superseded` | This path used to be current but a later observation found the file elsewhere. |

Why both `file_observations` and `file_location_state`:

- `file_observations` is the audit log.
- `file_location_state` is the current-state cache for fast validation and UI.

### `run_actions`

Durable proposed/applied actions produced by a command run.

```sql
CREATE TABLE run_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposed_by_run_id INTEGER NOT NULL REFERENCES command_runs(id),
    applied_by_run_id INTEGER REFERENCES command_runs(id),
    action_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER,
    source_path TEXT,
    source_path_key TEXT,
    target_path TEXT,
    target_path_key TEXT,
    status TEXT NOT NULL,
    confidence INTEGER,
    method TEXT,
    idempotency_key TEXT NOT NULL,
    phase INTEGER NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 0,
    depends_on_action_id INTEGER REFERENCES run_actions(id),
    payload_json TEXT,
    created_at TEXT NOT NULL,
    applied_at TEXT,
    error_message TEXT
);
```

Recommended indexes and constraints:

```sql
CREATE UNIQUE INDEX idx_run_actions_idempotency_key ON run_actions(idempotency_key);
CREATE INDEX idx_run_actions_proposed_run ON run_actions(proposed_by_run_id);
CREATE INDEX idx_run_actions_applied_run ON run_actions(applied_by_run_id);
CREATE INDEX idx_run_actions_status_phase ON run_actions(status, phase, sequence, id);
CREATE INDEX idx_run_actions_entity ON run_actions(entity_type, entity_id);
CREATE INDEX idx_run_actions_type_status ON run_actions(action_type, status);
```

`proposed_by_run_id` and `applied_by_run_id` intentionally separate proposal
provenance from apply provenance. A dry-run may propose actions under run 101,
while a later real run 102 applies those actions. For actions created and
applied during the same real run, both columns can reference the same
`command_runs.id`.

`phase` and `sequence` provide deterministic apply order. Prefer phase ordering
first; use `depends_on_action_id` only when a specific per-action dependency is
unavoidable. The initial file-maintenance workflows should avoid becoming a
general dependency graph or job scheduler.

Suggested `action_type` values:

| Value | Meaning |
|---|---|
| `update_canonical_dest_path` | Accept that a catalog file's canonical path is now a different path. |
| `move_file` | Move a file from source path to destination path. |
| `copy_file` | Copy a file from source path to destination path. |
| `link_raw_output` | Insert a RAW-to-output relationship. |
| `link_raw_sidecar` | Insert a RAW-to-sidecar relationship. |
| `link_psd_source` | Insert a PSD-to-source relationship. |
| `unlink_relationship` | Remove or reject a relationship. |
| `face_detect` | Record or accept detected face boxes for an image. |
| `face_cluster_assign` | Assign face detections to a cluster. |
| `face_person_link` | Link a face/cluster to a person identity. |
| `face_label_update` | Apply user label/refinement decision. |

Suggested `status` values:

| Value | Meaning |
|---|---|
| `proposed` | Created by a preview or dry-run, not applied. |
| `applied` | Successfully applied to accepted catalog state. |
| `skipped` | Not needed because target state already existed. |
| `failed` | Attempted but failed. |
| `rejected` | User/operator rejected the proposal. |
| `superseded` | Replaced by a later proposal. |

`idempotency_key` should be deterministic, for example:

- `update_canonical_dest_path:file_id:new_path`
- `link_raw_output:raw_id:output_id`
- `move_file:file_id:source_path:target_path`
- `face_detect:model_version:file_id:image_hash`

Use path keys rather than raw display paths in idempotency keys where path
identity matters.

### Accepted-State Metadata Columns

Add run provenance to accepted relationship tables:

```sql
ALTER TABLE raw_outputs ADD COLUMN created_by_run_id INTEGER REFERENCES command_runs(id);
ALTER TABLE raw_sidecars ADD COLUMN created_by_run_id INTEGER REFERENCES command_runs(id);
ALTER TABLE psd_source_links ADD COLUMN created_by_run_id INTEGER REFERENCES command_runs(id);
```

`created_by_run_id` on accepted-state tables should reference the applying run,
not necessarily the proposing run. The associated `run_actions` row preserves
both `proposed_by_run_id` and `applied_by_run_id`.

If the face branch adds face tables, use the same pattern:

- `created_by_run_id`
- `updated_by_run_id` where user review/refinement changes state
- `status` where rows can be proposed, accepted, rejected, or superseded
- `model_name` and `model_version` for model-produced observations

## Schema Version Coordination

Current `feature/catalog-maintenance` has `CURRENT_SCHEMA_VERSION = 2`.

Before implementing the new schema additions, add an explicit migration runner.
The current schema initialization is mostly additive `CREATE TABLE IF NOT
EXISTS` plus direct version bumping. That is not enough for this refactor
because the plan adds tables, indexes, constraints, backfill, and future
branch coordination.

Minimum migration framework requirements:

- Keep `schema_version` strictly monotonic.
- Run migration functions in order inside explicit transactions.
- Never mark a migration version applied until all DDL, DML, indexes, and
  backfill for that version succeed.
- Make migrations idempotent only where that is safe, but do not rely on
  `CREATE TABLE IF NOT EXISTS` as the migration mechanism.
- Include migration tests for empty catalogs, v1/v2 catalogs, partially
  populated catalogs, and repeated startup after a successful migration.
- Keep `init_schema()` responsible for creating a brand-new latest-version
  catalog and for invoking migrations on older catalogs.

Before implementing this plan:

1. Check the merge order with `feature/facial-recognition`.
2. Whichever branch merges first takes the next schema version.
3. The second branch rebases and renumbers its migration.
4. Do not implement these additions with only `CREATE TABLE IF NOT EXISTS`.
   Add an explicit versioned migration.

If catalog-maintenance merges first and facial recognition is rebased after it:

- Command run history remains version `2`.
- Facial branch can become version `3` if it only adds face tables.
- This refactor becomes version `4`.

If this refactor lands before facial recognition:

- This refactor becomes version `3`.
- Facial recognition becomes version `4` and adopts the run/action/observation
  conventions.

## Code Architecture

### New `RunContext`

Promote `PipelineCandidates` into a richer `RunContext`.

```python
@dataclass
class RunContext:
    run_id: int
    command: str
    dry_run: bool
    observed_file_ids: set[int]
    candidate_file_ids: set[int]
    proposed_action_ids: set[int]
```

Responsibilities:

- Record observations.
- Seed and update the candidate set.
- Create `run_actions`.
- Provide scoped query inputs to linkers, planners, and movers.
- Carry run provenance into accepted-state writes.

### Observation Recorder

New module: `photo_organizer/pipeline/observations.py`

Core API:

```python
class ObservationRecorder:
    def record_file_present(...): ...
    def record_missing_expected(...): ...
    def record_rename_candidate(...): ...
    def record_move_candidate(...): ...
    def materialize_latest_state(...): ...
```

`DestinationSyncer` and `DiskScanner` wrappers should record observations here
instead of treating every observation as an immediate mutation to `files`.

### Action Planner

New module: `photo_organizer/pipeline/actions.py`

Core API:

```python
class ActionPlanner:
    def propose_canonical_path_update(...): ...
    def propose_move(...): ...
    def propose_link(...): ...
    def apply_actions(...): ...
```

The current `DestinationPlanner`, linkers, and `FileMover` can initially be
wrapped rather than rewritten:

- Existing linkers propose `run_actions` first.
- Existing planner proposes move/copy/canonical-path actions.
- Existing mover applies only `run_actions` selected for this run.

### Pipeline Shape

Target pipeline:

```text
start command_run
open pipeline connection

Phase A: observe
  scanner/syncer records file_observations
  file_location_state is updated
  content identity rows are upserted when needed
  commit observations

Phase B: propose
  build RunContext from observations
  propose canonical-path updates, links, and file moves as run_actions
  if dry_run:
      commit proposed actions as proposed
      do not mutate accepted state
      finish command_run success
      return

Phase C: apply
  apply selected run_actions
  mutate accepted state: files.dest_path, link tables, disk moves
  mark run_actions applied/skipped/failed
  commit
  finish command_run success/error
```

This replaces the current "savepoint rolls everything inferred back on dry-run"
model with a more durable preview model. Dry-runs can leave behind both
observations and proposals without changing accepted state.

## Schema Migrations

Schema migrations describe database shape changes only. Behavior changes should
be tracked separately in the behavior milestones below, even when a PR bundles
schema and behavior together.

### Schema Migration A: Migration Runner

Add an explicit versioned migration runner before introducing the new catalog
state tables.

Changes:

- Keep `schema_version` strictly monotonic.
- Run migration functions in order inside explicit transactions.
- Never mark a migration version applied until all DDL, DML, indexes, and
  backfill for that version succeed.
- Keep `init_schema()` responsible for creating a brand-new latest-version
  catalog and invoking migrations on older catalogs.

Tests:

- Empty catalog initializes at the latest schema version.
- Existing v1/v2 catalogs migrate forward.
- Re-running startup after a successful migration does not reapply migration
  side effects.
- A failed migration does not advance `schema_version`.

### Schema Migration B: Observation, Location, and Action Tables

Add the durable state-management tables and path identity support.

Changes:

- `file_observations`
- `file_location_state`
- `run_actions`
- Indexes and uniqueness constraints listed in the schema section.
- Path identity normalization helper used consistently by new path-key columns.

Backfill:

- Create `file_location_state` rows from `files.dest_path` and
  `file_occurrences`.
- Mark backfilled rows with `root_kind='unknown'` or `root_kind='dest'` where
  path is under a known destination root.
- Populate `path_key` and `root_path_key` using the chosen normalization policy.

Tests:

- Backfill creates expected current-state rows.
- Path keys normalize expected Windows case and separator variants.
- Existing reports and sync queries continue to work while old behavior is
  still active.

### Schema Migration C: Accepted-State Provenance

Add provenance columns to accepted relationship tables.

Changes:

- `raw_outputs.created_by_run_id`
- `raw_sidecars.created_by_run_id`
- `psd_source_links.created_by_run_id`

Tests:

- Existing relationship rows with NULL provenance still work.
- New schema supports references to the applying `command_runs.id`.

### Schema Migration D: Future Face Tables

If the facial-recognition branch has not already added these tables, add face
tables using the same observe/propose/apply conventions.

Face tables should distinguish:

- Model observations: detection boxes, embeddings, confidence, model version.
- Proposed state: cluster assignments, person links, labels under review.
- Accepted state: user-approved person identities and face/person links.

## Behavior Adoption Milestones

Behavior milestones describe when pipeline code starts using the new schema.
They are deliberately separate from schema migrations so PRs can bundle them
pragmatically without confusing database versioning with rollout order.

### Milestone 1: Passive Observation Recording

Keep existing behavior, but also write observations.

Changes:

- `UntrackedTreeDiscoverer` records `present` observations.
- `CataloguedTreeDiscoverer` records `present`, `missing_expected`, and
  `renamed_candidate` observations.
- `sync-dest` and `validate-dest` record observations consistently.
- `file_location_state` updates on dry-run.

Tests:

- `ingest-dest --dry-run` records RAW rename observations but does not update
  `files.dest_path`.
- New JPEG observations persist as both `files` rows and observation rows.
- `validate-dest` can report that an expected path is missing and a matched
  current path exists.

### Milestone 2: Durable Run Actions for Ingest Maintenance

Replace in-memory-only planning with `run_actions` for `--ingest-dest`.

Changes:

- Linkers create proposed link actions.
- Planner creates proposed canonical path and move/copy actions.
- Mover applies selected actions.
- Dry-run commits `run_actions.status='proposed'` but does not apply them.
- Real run applies current run's actions and marks them `applied` or `skipped`.
- Accepted relationship writes populate `created_by_run_id` with the applying
  run.

Tests:

- DPP dry-run leaves proposed actions for RAW renames, RAW-output links, and
  JPEG moves.
- Real run applies those actions exactly once.
- Re-running real mode marks already-applied actions as skipped or does not
  create duplicate accepted state.
- Failure mid-apply leaves action status useful for diagnosis.
- Link validation can report which run created questionable links.

### Milestone 3: Organize, Sync, and Validation Adoption

Move `organize`, `sync-dest`, and validation onto the same observation/action
contracts.

Changes:

- `organize` uses observations and run-scoped actions without changing user
  behavior.
- `sync-dest` can preview and apply canonical path updates.
- Validation reports accepted path versus latest observed path.

Tests:

- `organize` behavior remains compatible.
- `sync-dest` can preview and apply canonical path updates.
- Validation can show latest observed reality versus accepted catalog state.

### Milestone 4: Facial Recognition Integration

Adopt the same pattern for face workflows.

Expected face operation mapping:

| Face operation | Observation/action model |
|---|---|
| `faces_detect` | Observes image inputs and face boxes/embeddings with model metadata. |
| `faces_cluster` | Proposes cluster assignments as actions. |
| `faces_seed` | Applies user-provided identity seed actions. |
| `faces_refine` | Applies reviewed merge/split/relabel actions. |
| `faces_link` | Applies face-to-person links with run provenance. |

Tests:

- Face detection dry-run records command history but does not write accepted
  face state unless the operation is explicitly defined as observational.
- Face clustering can be previewed without changing accepted labels.
- User review actions are auditable and tied to `command_runs`.

### Milestone 5: RAW Edit-Aware Reconciliation

Handle destination RAW files that were renamed and modified by an editor such
as Canon DPP, so byte hashes and sparse hashes no longer match the accepted
catalog row.

This is a deferred extension built on observations and actions. It should not
block the initial schema and passive recording work.

Changes:

- Record `modified_rename_candidate` observations when an expected RAW path is
  missing and a new RAW-like file in the same destination context matches by
  metadata, date, camera, image number, stem pattern, near file size, or other
  high-confidence signals.
- Propose `update_canonical_dest_path` actions with a lower-confidence
  `match_method`, rather than importing the edited RAW as a normal net-new
  primary by default.
- Prevent modified-rename candidates from flowing into ordinary destination
  planning unless the operator accepts them as genuinely new files.
- Preserve dry-run behavior: observations and proposals persist, accepted
  `files.dest_path` does not change.

Tests:

- Edited/rotated RAWs with changed byte hashes are reported as modified rename
  candidates instead of ordinary new RAW moves.
- Simple byte-identical renames still use the stronger hash-based path.
- Ambiguous metadata matches are proposed for review or left unaccepted, not
  auto-applied.

## Query and UX Changes

Add queries for:

- Last observed path for a file.
- Files whose accepted `dest_path` differs from latest observed location.
- Proposed actions from the last dry-run.
- Actions from a run that can be applied, rejected, or superseded.
- Links or face labels created by a specific run.

Potential CLI additions:

```text
photo-organizer --show-history
photo-organizer --show-run <run_id>
photo-organizer --show-proposals <run_id>
photo-organizer --apply-run <run_id>
photo-organizer --reject-run <run_id>
```

These do not need to land in the first schema PR. They become more valuable
once `run_actions` exists.

## Test Strategy

### Unit Tests

- Observation recorder inserts append-only facts.
- Latest-state materialization updates `file_location_state` correctly.
- Action planner creates deterministic idempotency keys.
- Action applier handles duplicate accepted state as `skipped`.
- Migration backfills state from existing catalogs.

### Integration Tests

- DPP dry-run:
  - persists observations for renamed RAWs and new JPEGs
  - persists proposed actions
  - does not update `files.dest_path`
  - does not insert accepted links
  - does not move files

- DPP real run:
  - applies canonical RAW path updates
  - applies RAW-output links
  - moves JPEGs
  - marks actions applied
  - records created-by run IDs

- Validation after dry-run:
  - reports accepted path stale
  - reports latest observed path known
  - reports proposed action available

- Facial recognition:
  - detection observations are tied to a run
  - cluster proposals are previewable
  - accepted labels are run-provenanced

### Production Catalog Smoke Tests

Against `D:\Organized_Images`:

1. Run `--ingest-dest --move --dry-run`.
2. Confirm observations exist for the renamed RAWs and new JPEGs.
3. Confirm accepted state is unchanged.
4. Confirm proposed actions match expected count.
5. Run real apply.
6. Confirm actions applied and accepted state matches disk.

## Rollout Plan

### PR 1: Schema and Passive Recording

Implement Schema Migrations A, B, and C plus Behavior Milestone 1. No change to
apply behavior.

Success criteria:

- Existing tests pass.
- Migration runner tests pass.
- New observation tests pass.
- Dry-run can persist rename observations without mutating accepted state.

### PR 2: Durable Actions for `ingest-dest`

Implement Behavior Milestone 2.

Success criteria:

- DPP dry-run records proposed actions.
- DPP real run applies actions exactly once.
- `PipelineCandidates` is either backed by `RunContext` or becomes an
  implementation detail of `RunContext`.

### PR 3: Organize and Sync Adoption

Implement Behavior Milestone 3.

Success criteria:

- `organize` behavior remains compatible.
- `sync-dest` can preview and apply canonical path updates.
- Validation can show latest observed reality versus accepted catalog state.

### PR 4: Facial Recognition Adoption

Implement Schema Migration D if still needed, plus Behavior Milestone 4.

Success criteria:

- Face commands have run provenance.
- Model observations are separated from accepted user labels.
- Review/refinement operations are auditable.

### PR 5: RAW Edit-Aware Reconciliation

Implement Behavior Milestone 5.

Success criteria:

- Edited RAWs that changed byte hashes are not treated as ordinary new primary
  moves by default.
- High-confidence edited RAW matches propose canonical path updates.
- Ambiguous edited RAW matches remain reviewable and are not auto-applied.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Schema version conflict with facial branch | Check branch version before implementation, rebase and renumber migrations. |
| Observation tables grow quickly | Add indexes first, then decide retention policy after usage patterns are known. |
| Dry-run semantics surprise users because it writes observations | Document explicitly: dry-run does not apply actions, but may record observed facts. |
| Existing reports assume `files.dest_path` is current reality | Update reports to display accepted path and latest observed path separately. |
| Action replay creates duplicates | Require deterministic `idempotency_key` and idempotent apply code. |
| Face workflows need different confidence/review semantics | Keep common run/action framework, but allow face-specific `payload_json` and statuses. |

## Open Questions

1. Should dry-run proposals remain indefinitely, or should they be superseded
   automatically by the next run for the same command/root?
2. Should users be able to apply a previous dry-run by `run_id`, or should
   real runs always recompute proposals from current disk state?
3. Should `files.dest_path` eventually be renamed to `canonical_dest_path` for
   clarity?
4. Should `file_occurrences` be kept as a compatibility table, migrated into
   `file_observations`, or replaced after a deprecation period?
5. What is the retention policy for high-volume face detection observations and
   embeddings?
6. Should `path_key` use lexical normalization only, or resolve symlinks and
   junctions where the filesystem allows it?

## Recommended Next Step

Implement PR 1 only:

1. Add the explicit migration runner and versioned migration tests.
2. Add versioned schema migration for `file_observations`,
   `file_location_state`, and `run_actions`.
3. Backfill `file_location_state`.
4. Record file observations in parallel with current behavior.
5. Add tests proving dry-run can persist rename observations without changing
   accepted state.

Do not change `ingest-dest` application semantics again until the observation
layer is in place and verified against the production catalog.
