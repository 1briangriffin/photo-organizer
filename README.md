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
