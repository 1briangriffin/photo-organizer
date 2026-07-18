# Photo Organizer

Python tools for organizing a large collection of digital photos and videos into a clean, deduplicated, EXIF-aware library with a persistent SQLite catalog.

## Features

- Supports:
  - RAW files (CR2, CR3, NEF, ARW, ORF, RW2, DNG)
  - JPEGs
  - Videos (MP4, MOV, AVI, MTS, etc.)
  - PSD / PSB (Photoshop documents)
  - TIFFs (TIF / TIFF)
  - Sidecar metadata (XMP, VRD, etc.)
  - Any other extension (cataloged as `other` for later review)
- Organizes into a structure like:

  ```text
  DEST_ROOT/
    raw/
      2020/
        2020-01/
        2020-02/
    output/
      2020/
        2020-01/
        2020-02/
          psd/
  ```

## Workflows

Three primary workflows cover the lifecycle of a library: seeding it from scratch, adding new originals over time, and ingesting derivative files (DPP JPEG exports, edited PSDs, etc.) that land inside the library after the fact. Every command writes a row to the `command_runs` audit table so you can see exactly what has run, when, and whether it actually mutated the catalog or files on disk.

In every command below, `<dest>` is the library root and the catalog lives at `<dest>/photo_catalog.db` by default (override with `--db`). A `.log` file and any preview/validation CSVs are written under `<dest>` as well.

### Workflow 1: Initial organization

Build the library from one or more source trees. This is the only mode that will create the catalog if it does not already exist.

```bash
# Preview first — writes <dest>/dry_run_preview.csv, no files moved, no dest_paths persisted
uv run photo-organizer <src> <dest> --dry-run

# Execute the organization (copy by default; add --move to move instead)
uv run photo-organizer <src> <dest>
uv run photo-organizer <src> <dest> --move

# If this source is your canonical library (e.g. an already-curated archive), flag it
uv run photo-organizer <src> <dest> --seed --move
```

Dry-run behaviour: scan data (hashes, `file_occurrences`, `media_metadata`) IS persisted because it describes what is actually on disk. Inferred/planned state (links, `dest_path`s, moves) sits inside a savepoint and is rolled back. Re-running a dry-run is idempotent and cheap.

### Workflow 2: Processing net-new images

You shot more photos. Point the organize command at the new source tree — previously-organized files are recognized by hash and skipped, so you can re-run against overlapping sources without duplicating.

```bash
uv run photo-organizer <new-src> <dest> --move --dry-run
uv run photo-organizer <new-src> <dest> --move
```

Optional `--auto-sync`: after the organize run, also reconcile any files in `<dest>` that were renamed outside the tool (Lightroom, Finder, Explorer) so the catalog path matches disk.

```bash
uv run photo-organizer <new-src> <dest> --move --auto-sync
```

### Workflow 3: Processing new outputs of already-catalogued RAWs

You edited a RAW in Canon DPP (or similar) and exported a JPEG. DPP drops the JPEG next to the RAW in `<dest>/raw/...`, but you want it routed to `<dest>/output/...` and linked to its source RAW. Use `--ingest-dest`:

```bash
# Preview — writes <dest>/ingest_dry_run_preview.csv
uv run photo-organizer <dest> --ingest-dest --move --dry-run

# Execute: discovers new JPEGs/PSDs/XMPs in <dest>, links them to their source
# RAWs, plans output destinations, and moves them into <dest>/output/...
uv run photo-organizer <dest> --ingest-dest --move
```

`--move` is almost always the right flag here: the exported JPEG already lives inside the library, so you want it relocated, not duplicated. Ingest-mode imports always persist (the files exist on disk — that's observed reality); only the planning/linking phase is rolled back on dry-run.

If Phase A finds no new files (e.g. DPP wrote only XMP sidecars that aren't net-new), the command early-exits before Phase B. Re-run once the JPEGs are present.

## Facial recognition

Face workflows live in a sibling CLI, `photo-faces`, and record into the
durable `face_*` tables with full run provenance. Install the optional stack
first (GPU inference via onnxruntime; Blackwell GPUs need the ≥1.21 builds
this project pins):

```bash
uv sync --extra faces
```

### `photo-faces scan` — detect faces and record embeddings

```bash
# Scan editor-exported JPEG/TIFF outputs (recommended first pass; RAWs with a
# linked output are covered by their output and always skipped)
uv run photo-faces --db <dest>/photo_catalog.db scan

# Small trial batch / force CPU
uv run photo-faces --db <dest>/photo_catalog.db scan --limit 100
uv run photo-faces --db <dest>/photo_catalog.db scan --cpu

# Opt in to RAWs that have no linked output (requires rawpy; much slower)
uv run photo-faces --db <dest>/photo_catalog.db scan --include-raw
```

Scans are incremental and safe to interrupt: files with any detection row for
the current model (including a "no faces found" sentinel) are skipped on the
next run, and progress commits every 500 faces. Detections carry bbox,
confidence, estimated age/gender, and a face thumbnail (under
`<db_dir>/.face_thumbnails`); embeddings are stored per detection keyed by
model version, so upgrading the model later triggers a clean re-scan.
Every run appears in `photo-catalog-query --show-runs` (tool `photo-faces`).

### `photo-faces cluster` — propose identity clusters

```bash
uv run photo-faces --db <dest>/photo_catalog.db cluster
uv run photo-faces --db <dest>/photo_catalog.db cluster --era-size 2.5 --min-cluster-size 3
```

Groups scanned embeddings into identity clusters within overlapping temporal
eras (HDBSCAN). Detections below `--min-det-score` (default 0.7) are excluded
— the detector records everything above 0.5, but the low band is mostly
pareidolia (bricks, wires); the floor is query-time, so it's tunable without
rescanning, and the UI's "Not a face" button handles stragglers. Faces change over decades, so clustering happens per era
window; persons seeded with a birth date additionally get tighter
developmental windows (0-2, 2-5, 5-10, 10-15 years) where children's faces
change fastest. Embeddings are PCA-reduced to 64 dims for the clustering
math (~10x faster, same clusters; representatives stay full-dimension) —
tune or disable with `--pca-dims N` / `--pca-dims 0`. Results are **proposals**: `face_clusters` /
`face_cluster_members` rows with `status='proposed'` plus reviewable
run_actions — nothing is accepted until reviewed. Re-running supersedes
previous proposed clusters (accepted ones are never touched), so tuning
`--era-size` / `--min-cluster-size` and re-clustering is cheap.

### `photo-faces link` — propose cross-age merges

```bash
uv run photo-faces --db <dest>/photo_catalog.db link
uv run photo-faces --db <dest>/photo_catalog.db link --min-confidence 0.5

# Review and reject the suggestions like any other proposal
uv run photo-catalog-query --db <dest>/photo_catalog.db --show-proposals --action-type face_cluster_merge
uv run photo-catalog-query --db <dest>/photo_catalog.db --reject-proposal <id> --note "different people"
```

Two tiers of suggestions, both `face_cluster_merge` proposals in the
standard lifecycle:

- **Window duplicates** (`method=window_duplicate`, confidence 100):
  clusters from overlapping era windows that share a majority of their
  member detections are the same identity by construction — the same faces
  clustered twice. These are ideal for bulk acceptance (below).
- **Scored cross-age pairs**: overlapping/adjacent-era pairs scored with the
  weighted multi-signal model — embedding similarity, co-occurrence with the
  same other people, temporal continuity, and similarity to already-labeled
  persons. (Estimated age is deliberately not a signal: the model's age head
  is too unreliable, especially on children; true age comes from birth_date +
  capture date once a person is named.) Only
  each cluster's `--top-k` best suggestions are kept (default 3): merging is
  transitive, so a spanning set of each identity's pair graph reviews the
  same as the complete graph at a fraction of the volume.

Re-running the linker supersedes stale suggestions, and identities chain
across decades transitively (2005→2007→2010… rather than comparing 2005
directly against 2020).

### `photo-faces seed / accept / label` — identities

```bash
# Seed known people up front (birth dates unlock developmental era windows
# in clustering and supervised anchors in linking)
uv run photo-faces --db <dest>/photo_catalog.db seed --config faces_config.yaml

# Accept merge suggestions by proposal id: the linked clusters join into a
# person (reused if one is already linked, created otherwise)
uv run photo-faces --db <dest>/photo_catalog.db accept 1234 1235

# Bulk mode: accept ALL pending suggestions at/above a confidence percent.
# 95+ sweeps the window duplicates in one audited command; the union-find
# and named-person conflict guard apply exactly as in id mode.
uv run photo-faces --db <dest>/photo_catalog.db accept --min-confidence 95

# Name a person created by acceptance
uv run photo-faces --db <dest>/photo_catalog.db label 7 "Emma" --birth-date 2010-08-22
```

`faces_config.yaml` format:

```yaml
known_people:
  - name: Sam
    birth_date: 2005-03-15
    notes: "oldest child"
  - name: Emma
    birth_date: 2010-08-22
```

Acceptance runs union-find over the accepted pairs plus existing
person↔cluster links, so chains merge transitively. A component that would
join two *different* named persons is refused and reported for review
instead of silently merging identities. All decisions are audited: seeds,
labels, accepted clusters/memberships/links, and the merge proposals flip
from `proposed` to `applied` with the accepting run's id.

### `photo-faces refine` — auto-assign suggestions from labeled people

```bash
uv run photo-faces --db <dest>/photo_catalog.db refine
uv run photo-faces --db <dest>/photo_catalog.db refine --threshold 0.85 --margin 0.1
```

Once identities exist, each person's accepted faces define an anchor
embedding. Refine proposes assigning still-unlinked clusters to a person when
the cluster is very close to exactly one anchor with a clear margin over the
second-best — the labeling flywheel: every accept/label makes the next refine
smarter. Suggestions are `face_person_assign` proposals; review them with
`--show-proposals` and apply them with `photo-faces accept` alongside merges.

### `photo-faces ui` — review, label, name

```bash
uv run photo-faces --db <dest>/photo_catalog.db ui
```

The Streamlit app is the primary labeling surface. **Label Photos** is the
front door: it samples the photos whose faces would resolve the most
still-unlabeled cluster mass (spread across the years), shows each full photo
with numbered face boxes and its capture date, and lets you name each face —
"label this face's whole cluster" multiplies one click into dozens of faces,
and any face can be skipped or marked "Not a face". Every label feeds
refine/link anchors, so alternate labeling sessions with
`photo-faces refine && photo-faces link` and the machinery converges on the
rest. The other pages: Stats (named-progress dashboard), Cluster Review,
Suggestion Review (accept/reject merge + auto-assign proposals), Name People
(name or fold anonymous person groups), Timeline, and Query. Every UI
mutation records its own audited command run (tool `photo-faces-ui`).

### `photo-faces persons / query` — find your people

```bash
uv run photo-faces --db <dest>/photo_catalog.db persons

uv run photo-faces --db <dest>/photo_catalog.db query "Emma"
uv run photo-faces --db <dest>/photo_catalog.db query "Emma" --from 2012-01-01 --to 2015-12-31
uv run photo-faces --db <dest>/photo_catalog.db query "Emma" --timeline
uv run photo-faces --db <dest>/photo_catalog.db query "Emma" --csv-output emma.csv
```

Queries return accepted appearances only (proposals never leak into
results). When a matched photo is an editor export linked to a RAW, the
query resolves and prints the source RAW path too.

### `photo-faces ui` — visual review & labeling

```bash
uv run photo-faces --db <dest>/photo_catalog.db ui
```

Launches the Streamlit review app in your browser: a stats dashboard with
labeling progress, cluster review with face thumbnail grids (assign to an
existing person or create one), side-by-side suggestion review for merge and
auto-assign proposals with signal breakdowns and accept/reject buttons, a
per-person timeline of face crops across the years, and photo query. Every
decision made in the UI goes through the same primitives as the CLI and
records its own audited command run (tool `photo-faces-ui`).

## Catalog maintenance

Commands that reconcile the catalog with on-disk reality without running a full organize pipeline.

### `--sync-dest` — reconcile renames and optionally import stragglers

```bash
# Detect files renamed in <dest> and update dest_path in the catalog
uv run photo-organizer <dest> --sync-dest

# Also import any files found in <dest> that are not yet in the catalog
# (treats them as correctly placed — dest_path is set to the current location)
uv run photo-organizer <dest> --sync-dest --import-new
```

### `--validate-dest` — audit the catalog against disk

```bash
# Writes <dest>/dest_validation.csv with CONFIRMED / MISSING / UNTRACKED /
# RENAMED / MOVED sections
uv run photo-organizer <dest> --validate-dest
```

Read-only: no catalog or filesystem mutations. Use this after any bulk operation to confirm the catalog still reflects reality.

### `--backfill-raw-metadata` — populate camera identity for existing RAWs

Catalogs created before camera identity support (schema v6) have RAW rows
without `camera_serial_number` / `camera_file_number`. Those fields power the
RAW edit-aware rename reconciliation, so backfill them once by re-reading the
tags from disk (uses `exiftool`; only rows with missing identity are touched,
and existing values are never overwritten):

```bash
# Preview scope: how many rows need backfill, how many are missing on disk
uv run photo-organizer <dest> --backfill-raw-metadata --dry-run

# Run it (commits progress in batches; safe to interrupt and re-run)
uv run photo-organizer <dest> --backfill-raw-metadata

# Incremental first pass on a big library
uv run photo-organizer <dest> --backfill-raw-metadata --limit 1000
```

Renames applied by the RAW edit reconciliation also fill missing identity
fields opportunistically (the tags were already read during matching), so
freshly reconciled files never need a second pass.

### Intentional deletion — retiring files you removed on purpose

When you deliberately delete a file from the library (a rejected shot, a
duplicate), `--validate-dest` would report it as MISSING forever. Mark it
retired instead — the catalog keeps its full history (hashes, links, run
provenance), but maintenance commands stop expecting it on disk:

```bash
# Retire by file id or by dest path (as printed in the validation CSV)
uv run photo-catalog-query --db <dest>/photo_catalog.db --retire-file 123 "<dest>/raw/2023/2023-06/IMG_0042.CR2" --note "rejected shot"

# Review what's retired; bring one back if you change your mind
uv run photo-catalog-query --db <dest>/photo_catalog.db --list-retired
uv run photo-catalog-query --db <dest>/photo_catalog.db --restore-file 123
```

Retired files disappear from MISSING and get their own section in the
validation CSV. Retire/restore decisions are themselves audited command runs.
Note: content dedup still applies — if a copy of a retired photo shows up in a
new source batch, it matches the retired row and is skipped rather than
re-imported. `--restore-file` is the explicit way back.

### Proposal lifecycle — what happens to dry-run proposals

Dry-runs persist their planned actions as `run_actions` rows with
`status='proposed'` so you can review them before running for real. Pending
proposals resolve automatically:

- Re-running (dry or real) supersedes older pending proposals for the same
  action, and each successful run also supersedes leftover proposals from
  earlier runs of the same command + roots (e.g. a file that vanished between
  the dry-run and the real run). A failed apply leaves the proposal pending.
- Real runs always recompute against current disk state — there is no "apply
  dry-run #N" replay; proposals are review artifacts, not work queues.

Review and reject pending proposals with `photo-catalog-query`:

```bash
# List pending proposals (newest first)
uv run photo-catalog-query --db <dest>/photo_catalog.db --show-proposals
uv run photo-catalog-query --db <dest>/photo_catalog.db --show-proposals --action-type move_file --limit 100

# Inspect resolved history (superseded / rejected / applied attempts)
uv run photo-catalog-query --db <dest>/photo_catalog.db --show-proposals --action-status superseded

# Reject proposals you don't want; the decision is itself an audited command run
uv run photo-catalog-query --db <dest>/photo_catalog.db --reject-proposal 123 124 --note "wrong destination"
```

### `photo-catalog-query` — ad-hoc catalog inspection

A sibling CLI for querying the catalog without running any pipeline.

```bash
# Show unprocessed RAWs (no linked JPEG/TIFF output)
uv run photo-catalog-query --db <dest>/photo_catalog.db --unprocessed-raws

# Inspect one RAW and its linked outputs, by id or by path
uv run photo-catalog-query --db <dest>/photo_catalog.db --raw-id 1234
uv run photo-catalog-query --db <dest>/photo_catalog.db --raw-path "<dest>/raw/2024/2024-07/IMG_1234.CR3"

# Check RAW↔output link-naming consistency (renames that drifted apart)
uv run photo-catalog-query --db <dest>/photo_catalog.db --check-links

# Show recent command history (what ran, when, whether it actually mutated anything)
uv run photo-catalog-query --db <dest>/photo_catalog.db --show-runs --limit 20
uv run photo-catalog-query --db <dest>/photo_catalog.db --show-runs --status error
uv run photo-catalog-query --db <dest>/photo_catalog.db --show-runs --command organize --since 2026-04-01
```

`--show-runs` reads the `command_runs` table that every invocation writes to, including the `db_mutates` / `files_mutate` flags (derived from *actual* counts, not intent), the stats dict for the run, and any error type/message on failures.

## Common flags

- `--dry-run` — preview only. Scan data persists (it describes disk); links, `dest_path`s, moves roll back. Writes a mode-specific preview CSV.
- `--move` — move instead of copy (organize and ingest-dest).
- `--db <path>` — override the catalog location. Default is `<dest>/photo_catalog.db`.
- `--skip-dirs-file <path>` — file listing directories to ignore during source scans.
- `--workers N` — parallelism for scanning/hashing. Defaults: 3 for HDD, 8 for SSD.
- `--report-csv <path>` — override the output path for the mode-specific CSV.
- `-v` / `--verbose` — debug logging.

## Architecture

![Architecture Diagram](architecture.svg)

High-level flow:
- CLI (`photo_organizer/main.py`) parses args and runs report mode or the organizing pipeline.
- `PhotoOrganizerApp` (`photo_organizer/core.py`) orchestrates scanning, linking, planning, and moving while holding the DB connection.
- Scanning combines `DiskScanner`, `FileHasher`, and `MetadataExtractor` to produce `FileRecord`s stored through `DBOperations`.
- `DBManager`/`DBOperations` initialize and write to the SQLite catalog (`schema.py`) that powers linking, planning, and reporting.
- `FileLinker`, `DestinationPlanner`, and `FileMover` read/write catalog records to keep linked files together and execute planned moves.
- `ReportGenerator` uses catalog lookups and hashing to audit source trees without running the full move/copy workflow.

## Known Limitations

### RAW↔Output linking performance

`FileLinker.link_raw_outputs()` builds RAW↔JPEG/TIFF relationships using datetime and stem matching across the full catalog. On large libraries this is an O(R×J) operation and can be slow. It is called as part of the organize pipeline and the `--ingest-dest` maintenance workflow. If performance becomes a bottleneck, the operation can be scoped to a subset of file IDs (e.g. only newly imported files) rather than the full catalog — this is left as a future optimization.
