"""
Path synchronization module for detecting and updating renamed files in destinations.

This module scans destination directories and compares the actual file locations
against the database records, detecting files that have been renamed and updating
the database to reflect the current state.
"""

import csv
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ..database.ops import DBOperations
from ..scanning.hasher import FileHasher
from .. import config


class SyncStatus(Enum):
    """Classification of file sync status."""
    UNCHANGED = "unchanged"   # File at expected location
    RENAMED = "renamed"       # Same parent dir, different filename
    MOVED = "moved"          # Different parent directory (warn only)
    MISSING = "missing"       # Not found anywhere in destination
    NEW = "new"              # Found on disk but not in database


@dataclass
class SyncResult:
    """Result of comparing a single DB record to disk state."""
    file_id: int
    status: SyncStatus
    old_path: str
    new_path: Optional[str] = None
    hash_value: Optional[str] = None
    is_sparse: bool = False
    mtime: float = 0.0
    size_bytes: int = 0


@dataclass
class SyncReport:
    """Statistics and details from a sync operation."""
    scanned_count: int = 0
    unchanged_count: int = 0
    renamed_count: int = 0
    moved_count: int = 0
    missing_count: int = 0
    new_count: int = 0
    imported_count: int = 0
    skipped_hash_count: int = 0
    error_count: int = 0

    renamed_files: List[Tuple[str, str]] = field(default_factory=list)  # (old, new)
    moved_files: List[Tuple[str, str]] = field(default_factory=list)
    missing_files: List[str] = field(default_factory=list)
    new_files: List[str] = field(default_factory=list)
    imported_files: List[str] = field(default_factory=list)
    errors: List[Tuple[str, str]] = field(default_factory=list)  # (path, error)


class DestinationSyncer:
    """
    Synchronizes database paths with actual file locations in destination directories.

    Detects renamed files by comparing content hashes and updates the database
    to reflect the current on-disk state.
    """

    def __init__(self, db_ops: DBOperations, max_workers: int = 3):
        self.db = db_ops
        self.hasher = FileHasher()
        self.max_workers = max_workers

    def sync_destinations(
        self,
        dest_roots: List[Path],
        dry_run: bool = False,
        csv_output: Optional[str] = None,
    ) -> SyncReport:
        """
        Main entry point. Scans destination directories and syncs database.

        Algorithm:
        1. Load all dest_path records from database
        2. For each dest_root, scan actual files on disk
        3. Match by hash (with mtime optimization - skip if unchanged)
        4. Detect renames (same parent dir, different name)
        5. Update database (unless dry_run)
        6. Return SyncReport

        Args:
            dest_roots: List of destination root directories to sync
            dry_run: If True, don't modify database
            csv_output: Optional path to write sync report CSV

        Returns:
            SyncReport with statistics and details
        """
        report = SyncReport()

        # Load expected files from database
        db_files = self._load_expected_files()
        logging.info(f"Loaded {len(db_files)} files with dest_path from database")

        # Build lookup structures
        # path_to_file: {dest_path: (file_id, hash, sparse_hash, db_mtime, db_size)}
        path_to_file: Dict[str, Tuple[int, Optional[str], Optional[str], Optional[float], Optional[int]]] = {}
        # hash_to_file: {hash: [(file_id, dest_path), ...]}
        hash_to_file: Dict[str, List[Tuple[int, str]]] = {}

        for file_id, dest_path, hash_val, sparse_hash, mtime, size in db_files:
            path_to_file[dest_path] = (file_id, hash_val, sparse_hash, mtime, size)
            # Index by both full and sparse hash
            if hash_val:
                if hash_val not in hash_to_file:
                    hash_to_file[hash_val] = []
                hash_to_file[hash_val].append((file_id, dest_path))
            if sparse_hash:
                if sparse_hash not in hash_to_file:
                    hash_to_file[sparse_hash] = []
                hash_to_file[sparse_hash].append((file_id, dest_path))

        # Track which DB files we've matched
        matched_file_ids: Set[int] = set()

        # Scan each destination root
        for dest_root in dest_roots:
            if not dest_root.exists():
                logging.warning(f"Destination root does not exist: {dest_root}")
                continue

            logging.info(f"Scanning destination: {dest_root}")
            self._scan_and_match(
                dest_root=dest_root,
                path_to_file=path_to_file,
                hash_to_file=hash_to_file,
                matched_file_ids=matched_file_ids,
                report=report,
                dry_run=dry_run,
            )

        # Find missing files (in DB but not found on disk)
        for dest_path, (file_id, _, _, _, _) in path_to_file.items():
            if file_id not in matched_file_ids:
                report.missing_count += 1
                report.missing_files.append(dest_path)
                logging.warning(f"Missing: {dest_path}")

        # Write CSV report if requested
        if csv_output:
            self._write_csv_report(csv_output, report)

        return report

    def _load_expected_files(self) -> List[Tuple[int, str, Optional[str], Optional[str], Optional[float], Optional[int]]]:
        """Load all files with dest_path from database."""
        return self.db.get_all_dest_files()

    def _scan_and_match(
        self,
        dest_root: Path,
        path_to_file: Dict[str, Tuple[int, Optional[str], Optional[str], Optional[float], Optional[int]]],
        hash_to_file: Dict[str, List[Tuple[int, str]]],
        matched_file_ids: Set[int],
        report: SyncReport,
        dry_run: bool,
    ):
        """Scan a destination directory and match files against database."""
        # Get all supported extensions
        supported_exts = set(config.EXT_TO_TYPE.keys())

        # Walk the destination tree
        for file_path in self._iter_files(dest_root, supported_exts):
            report.scanned_count += 1
            path_str = str(file_path)

            try:
                stat = file_path.stat()
                disk_mtime = stat.st_mtime
                disk_size = stat.st_size
            except OSError as e:
                report.error_count += 1
                report.errors.append((path_str, str(e)))
                continue

            # Check if file is at expected location
            if path_str in path_to_file:
                file_id, db_hash, db_sparse, db_mtime, db_size = path_to_file[path_str]
                matched_file_ids.add(file_id)
                report.unchanged_count += 1
                continue

            # File not at expected path - need to identify by hash
            # Check if we can skip hashing based on mtime
            hash_result = self._compute_hash_for_file(file_path, disk_size)
            if hash_result is None:
                report.error_count += 1
                report.errors.append((path_str, "Failed to compute hash"))
                continue

            hash_value = hash_result.full_hash or hash_result.sparse_hash
            is_sparse = hash_result.is_sparse

            # Look up file by hash
            matches = hash_to_file.get(hash_value, [])
            if not matches:
                # File not in database - it's new
                report.new_count += 1
                report.new_files.append(path_str)
                continue

            # Found matching hash - check if it's a rename or move
            for file_id, old_dest_path in matches:
                if file_id in matched_file_ids:
                    continue  # Already matched this file

                old_path = Path(old_dest_path)
                new_path = file_path

                # Check if same parent directory (rename) or different (move)
                if old_path.parent == new_path.parent:
                    # RENAME: Same directory, different filename
                    report.renamed_count += 1
                    report.renamed_files.append((old_dest_path, path_str))
                    matched_file_ids.add(file_id)
                    logging.info(f"Renamed: {old_path.name} → {new_path.name}")

                    if not dry_run:
                        self.db.update_dest_path_atomic(
                            file_id=file_id,
                            old_path=old_dest_path,
                            new_path=path_str,
                            new_mtime=disk_mtime,
                            size_bytes=disk_size,
                            hash_value=hash_value,
                            is_sparse=is_sparse,
                        )
                else:
                    # MOVE: Different directory (warn but don't auto-update)
                    report.moved_count += 1
                    report.moved_files.append((old_dest_path, path_str))
                    matched_file_ids.add(file_id)
                    logging.warning(
                        f"Moved (not auto-synced): {old_dest_path} → {path_str}"
                    )

                break  # Only match once per file

    def _iter_files(self, root: Path, extensions: Set[str]) -> List[Path]:
        """Iterate over files with supported extensions in directory tree."""
        files = []
        try:
            for item in root.rglob("*"):
                if item.is_file() and item.suffix.lower() in extensions:
                    # Skip hidden files and macOS metadata
                    if not item.name.startswith("."):
                        files.append(item)
        except PermissionError as e:
            logging.warning(f"Permission denied: {e}")
        return files

    def _compute_hash_for_file(self, file_path: Path, file_size: int):
        """Compute hash for a file, using sparse hash for large files."""
        try:
            # Get known sparse hashes to check for collisions
            known_sparse = self.db.fetch_known_sparse_hashes()
            return self.hasher.compute_hash(file_path, known_sparse)
        except Exception as e:
            logging.error(f"Error hashing {file_path}: {e}")
            return None

    def import_new_files(
        self,
        new_file_paths: List[str],
        report: SyncReport,
        dry_run: bool = False,
    ) -> int:
        """
        Import new files found in destinations into the database.

        These are files that exist on disk but aren't tracked in the database.
        They are added with dest_path set to their current location (already organized).

        Args:
            new_file_paths: List of file paths to import (from report.new_files)
            report: SyncReport to update with import results
            dry_run: If True, don't modify database

        Returns:
            Number of files successfully imported
        """
        from ..scanning.filesystem import DiskScanner
        from ..models import FileRecord

        if not new_file_paths:
            return 0

        scanner = DiskScanner()
        known_sparse = self.db.fetch_known_sparse_hashes()
        imported = 0

        for path_str in new_file_paths:
            file_path = Path(path_str)

            if not file_path.exists():
                logging.warning(f"File no longer exists: {path_str}")
                continue

            try:
                # Use scanner to process the file (extracts metadata, computes hash)
                record = scanner._process_single_file(
                    path=file_path,
                    is_seed=False,  # Files in destination are not seeds
                    known_sparse_hashes=known_sparse,
                )

                if record is None:
                    logging.warning(f"Failed to process file: {path_str}")
                    report.error_count += 1
                    report.errors.append((path_str, "Failed to process file"))
                    continue

                if dry_run:
                    logging.info(f"Would import: {file_path.name} ({record.type})")
                    report.imported_count += 1
                    report.imported_files.append(path_str)
                    imported += 1
                    continue

                # Insert into database
                file_id = self.db.upsert_file_record(record)

                # Add media metadata if applicable
                if record.type in ('raw', 'jpeg', 'video', 'psd', 'tiff'):
                    self.db.upsert_media_metadata(file_id, record)

                # Set dest_path to current location (already in destination)
                self.db.update_dest_path(file_id, path_str)

                # Record occurrence
                hash_value = record.hash or record.sparse_hash
                if hash_value:
                    stat = file_path.stat()
                    self.db.record_occurrence(
                        file_id=file_id,
                        path=file_path,
                        is_seed=False,
                        mtime=stat.st_mtime,
                        size_bytes=stat.st_size,
                        hash_value=hash_value,
                        is_sparse=record.hash_is_sparse,
                    )

                # Track sparse hash for collision detection
                if record.sparse_hash:
                    known_sparse.add(record.sparse_hash)

                logging.info(f"Imported: {file_path.name} ({record.type})")
                report.imported_count += 1
                report.imported_files.append(path_str)
                imported += 1

            except Exception as e:
                logging.error(f"Error importing {path_str}: {e}")
                report.error_count += 1
                report.errors.append((path_str, str(e)))

        return imported

    def _write_csv_report(self, csv_path: str, report: SyncReport):
        """Write sync results to CSV file."""
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Summary section
            writer.writerow(['=== SYNC SUMMARY ==='])
            writer.writerow(['Metric', 'Count'])
            writer.writerow(['Scanned', report.scanned_count])
            writer.writerow(['Unchanged', report.unchanged_count])
            writer.writerow(['Renamed', report.renamed_count])
            writer.writerow(['Moved (not synced)', report.moved_count])
            writer.writerow(['Missing', report.missing_count])
            writer.writerow(['New (not in DB)', report.new_count])
            writer.writerow(['Imported', report.imported_count])
            writer.writerow(['Errors', report.error_count])
            writer.writerow([])

            # Renamed files
            if report.renamed_files:
                writer.writerow(['=== RENAMED FILES ==='])
                writer.writerow(['Old Path', 'New Path'])
                for old, new in report.renamed_files:
                    writer.writerow([old, new])
                writer.writerow([])

            # Moved files
            if report.moved_files:
                writer.writerow(['=== MOVED FILES (not auto-synced) ==='])
                writer.writerow(['Old Path', 'New Path'])
                for old, new in report.moved_files:
                    writer.writerow([old, new])
                writer.writerow([])

            # Missing files
            if report.missing_files:
                writer.writerow(['=== MISSING FILES ==='])
                writer.writerow(['Path'])
                for path in report.missing_files:
                    writer.writerow([path])
                writer.writerow([])

            # New files
            if report.new_files:
                writer.writerow(['=== NEW FILES (not in database) ==='])
                writer.writerow(['Path'])
                for path in report.new_files:
                    writer.writerow([path])
                writer.writerow([])

            # Imported files
            if report.imported_files:
                writer.writerow(['=== IMPORTED FILES ==='])
                writer.writerow(['Path'])
                for path in report.imported_files:
                    writer.writerow([path])
                writer.writerow([])

            # Errors
            if report.errors:
                writer.writerow(['=== ERRORS ==='])
                writer.writerow(['Path', 'Error'])
                for path, error in report.errors:
                    writer.writerow([path, error])

        logging.info(f"Wrote sync report to: {csv_path}")
