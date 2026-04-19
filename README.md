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
