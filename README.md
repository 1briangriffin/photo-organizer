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
