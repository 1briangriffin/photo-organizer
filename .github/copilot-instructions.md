# Copilot Instructions for photo-organizer

## Architecture Overview

**photo-organizer** is a Python tool for ingesting and organizing digital photo libraries (RAW, JPEG, video, PSD, TIFF) into a deduplicated, EXIF-aware directory structure with a persistent SQLite catalog.

### Data Flow

1. **File Discovery** (`scan_tree`, `iter_files_scandir`): Recursively enumerate source directory, classify files by extension, gather metadata in parallel (up to 8 workers).
2. **Metadata Extraction** (`gather_file_record`): For each file, extract capture date (EXIF → infer from path → filesystem mtime), camera model, dimensions, hash (SHA256), and optional perceptual hash (pHash).
3. **Database Cataloging**: Store all file records in SQLite (`files` table), deduplicate by hash, link sidecars to RAW files (`raw_sidecars` table).
4. **Destination Planning** (`decide_dest_for_file`): Assign destination paths based on file type and capture date using pattern `{year}/{year}-{month:02d}/`.
5. **Copying & Cleanup**: Copy/move files to destination, export reports (unprocessed RAWs, unknown files).

### Database Schema

- **files**: `id, hash (UNIQUE), type, ext, orig_name, orig_path, dest_path, size_bytes, is_seed, name_score, first_seen_at, last_seen_at`
- **media_metadata**: `file_id (FK), capture_datetime, camera_model, lens_model, width, height, duration_sec, aspect_ratio, phash`
- **raw_sidecars**: `raw_file_id (FK), sidecar_file_id (FK)` – links XMP/VRD/DOP to RAW files

### Key Constants & Patterns

- **File Types**: RAW (`cr2, cr3, nef, arw, orf, rw2, dng`), JPEG, Video, PSD, TIFF, Sidecar (`xmp, vrd, dop, dpp, pp3`), Other
- **Dest Structure**: `raw/YYYY/YYYY-MM/`, `output/YYYY/YYYY-MM/` (PSD in `psd/` subfolder)
- **Hash Chunks**: 8MB chunks to minimize syscall overhead (`DEFAULT_HASH_CHUNK_SIZE`)
- **EXIF Date Tags**: `'EXIF DateTimeOriginal'`, `'EXIF DateTimeDigitized'`, `'Image DateTime'` (tried in order)
- **Datetime Fallback Chain**: EXIF → infer from path (YYYY-MM-DD, YYYYMMDD, year/month) → filesystem mtime

## Conventions & Patterns

### File Processing

1. **Open Once Per File**: When gathering metadata, open the file once and seek/rewind to extract hash and EXIF together (see `gather_file_record` monkeypatch test pattern in `test_photo_organizer.py`).
2. **Metadata Extraction Boundaries**: 
   - RAW/JPEG/PSD/TIFF: extract EXIF capture_dt, camera_model, lens_model, dimensions
   - Video: use MediaInfo library; fallback to filesystem mtime
   - Sidecar/Other: only filesystem mtime (no metadata extraction)
3. **Deduplication**: Files with identical SHA256 hash are skipped (UNIQUE constraint).
4. **Name Scoring**: `descriptiveness_score()` penalizes camera-default names (IMG_*, DSC_*), favors longer/separated names. Used to pick "best" JPEG among duplicates.

### Datetime Inference

- Try EXIF tags in order; if none found, extract from directory structure (e.g., `2020/01/photo.jpg` → 2020-01-01).
- Compact format: `YYYYMMDD` (e.g., `20100420` → 2020-04-20).
- If still no date, use file's mtime; log it as fallback.

### JPEG Grouping

When a JPEG has no RAW counterpart, the tool groups related JPEGs by normalized stem (e.g., `IMG_0001 (1)` → `img_0001`) and `capture_datetime`. Groups are sorted by: is_seed (seeds first), name_score (descending), width×height (descending). The best JPEG is assigned to `output/`, others to `output/resized/`.

### Metadata Failure Handling

Wrapped in try-catch blocks with logging:
- EXIF read failures are logged but don't halt processing (fallback to datetime inference).
- MediaInfo failures for videos also log and fall back.
- pHash computation failures logged but optional (only if `--use-phash` flag set).

## Critical Workflows

### Running the Main Organizer

```bash
uv run photo_organizer.py [--seed-output SEED_DIR] SRC_DIR DEST_DIR
```

**Key flags**:
- `--db DB_PATH`: Override catalog location (default: `DEST_DIR/photo_catalog.db`).
- `--seed-output SEED_DIR`: Scan pre-organized outputs first (links RAW sidecars from seed outputs before scanning source).
- `--move`: Move files instead of copy.
- `--dry-run`: Preview without copying.
- `--use-phash`: Compute perceptual hash for JPEG/TIFF (slow; used for RAW→output lineage).
- `--max-workers N`: Parallel scan threads (default 2, capped at 8).
- `--verbose`: Enable DEBUG logging.

**Database pragmas for performance**:
```python
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("PRAGMA cache_size=-200MB;")  # Speed up inserts
```

### Testing

```bash
pytest tests/
```

- Unit tests in `tests/test_photo_organizer.py` use fixtures with in-memory SQLite and `monkeypatch` to mock expensive I/O (EXIF reads, file hashing).
- Example pattern: `monkeypatch.setattr(po, "get_image_metadata_exif", lambda ...)` to stub metadata extraction.

### Query the Catalog

```bash
uv run photo_catalog_query.py --db CATALOG.db unprocessed_raws
```

Lists RAW files without linked outputs (for post-processing workflow).

### Analyze Metadata Issues

```bash
uv run analyze_metadata_issues.py --log organizer.log --unknown unknown_files.csv --output issues.csv
```

Correlates log messages with catalog to find EXIF failures, image size issues, etc.

## Integration Points & External Dependencies

- **exifread**: Extract EXIF tags; failures logged but non-blocking (try-catch).
- **Pillow (PIL)**: Get image dimensions, crop/resize for JPEG grouping.
- **pymediainfo**: Parse video metadata (optional; graceful fallback).
- **imagehash**: Compute pHash for perceptual deduplication (optional; only if `--use-phash`).
- **tqdm**: Progress bars in tree scans and linking operations.
- **sqlite3**: In-memory or file-based catalog; WAL mode for concurrent access.

## Code Organization

- **photo_organizer.py**: Main workflow (scan, deduplicate, organize, export).
- **photo_catalog_query.py**: Read-only queries (unprocessed RAWs, raw details, output links).
- **analyze_metadata_issues.py**: Parse logs and correlate with unknown_files.csv; output CSV of issues per file.
- **tests/**: Unit tests with fixtures and monkeypatching.

## Exactly-Once Deduplication Guarantee

**Mechanism**: Each file is identified by SHA256 hash. The `files` table has a UNIQUE constraint on `hash`, ensuring at most one canonical record per unique file content.

**How it works**:
1. When a file with hash H is first encountered, a record is inserted.
2. If hash H is seen again (duplicate), the existing record is reused (ID stays same).
3. Canonical metadata (path, name_score) is updated only if new copy has higher priority (seed > non-seed, or same seed but higher name_score).
4. **Critical**: `dest_path` is assigned once per unique hash during `decide_dest_for_file()` and never changes (even if re-run).

**File Placement Rules**:
- **RAW files**: Always placed in `raw/{year}/{year-month}/`; one per hash.
- **JPEGs without RAW**: Grouped by `(normalized_stem, capture_datetime)`. Highest resolution gets `output/{year}/{year-month}/`, lower resolutions get `output/{year}/{year-month}/resized/`.
- **Videos, PSD, TIFF**: One per hash in `output/{year}/{year-month}/` (or `output/{year}/{year-month}/psd/` for PSD).
- **Sidecars**: Linked to RAW files via `raw_sidecars` table and copied to destination of the associated RAW file.  Sidecars that are not linked to any RAW file are catalogued but **never copied**.
- **Unknown files (type='other')**: Cataloged but **never copied** (no `dest_path`).

**Exactly-Once Guarantee Limitations**:
1. **Re-runs with modified source**: If you modify a file's name or location but keep the same content (hash), re-running may assign a different `dest_path` due to local collision-avoidance logic in `used_names`. **Mitigation**: Catalog is only rebuilt on fresh runs; existing `dest_path` values are kept (checked via `if dest_path: continue`).
2. **JPEG grouping edge case**: If two JPEGs have the same capture time but no metadata, they fall back to filesystem mtime independently, which could differ if checked at different times. **Mitigation**: Unlikely in practice; capture times usually come from EXIF.
3. **pHash collisions**: If `--use-phash` is used, visually similar images might be linked even if not the same file. **Mitigation**: pHash linking has confidence=70 (lower than other methods); manually review `unprocessed_raws.csv` for edge cases.

## Common Pitfalls & Edge Cases

1. **RAW Sidecars Not Linked**: If XMP/VRD files have different stem case or extension, the `raw_sidecar_index` normalization (`key = (path.parent, stem.lower())`) may fail. Check `link_raw_sidecars_from_index()` logic.
2. **EXIF Date Parsing**: Some cameras use non-standard EXIF date formats. `parse_exif_datetime()` expects `YYYY:MM:DD HH:MM:SS`; other formats silently fail → fallback.
3. **Duplicate JPEGs with No RAW**: Grouped by `(capture_datetime, normalized_stem)`. If timestamps conflict, grouping may split them. Check `decide_dest_for_file()` JPEG grouping logic.
4. **pHash Mismatches**: pHash is used for RAW→JPEG lineage; computed only if `--use-phash`. Without it, lineage is built on file hash + proximity (timestamp/stem proximity). See `build_raw_output_links()`.
5. **Very Large Files**: Hash computation is chunked (8MB default); for files >1GB, increase `DEFAULT_HASH_CHUNK_SIZE` to reduce syscall count.
6. **Destination Path Reassignment on Re-run**: Once a `dest_path` is assigned, it is preserved on subsequent runs (query checks `if dest_path: continue`). However, if you manually delete the catalog and re-run, collision-avoidance counters reset. **Best practice**: Keep the catalog persistent across runs.

## Logging & Debugging

- **Log File**: `DEST_DIR/organizer.log` (INFO by default; DEBUG with `--verbose`).
- **exifread Logs**: Silenced (ERROR level) to reduce noise.
- **Key Log Messages**: Search for "Using folder-inferred datetime", "Using filesystem mtime", "EXIF read failed", "pHash computation failed" to diagnose metadata issues.
- **Reports**: `unprocessed_raws.csv` and `unknown_files.csv` exported at the end for manual review.
