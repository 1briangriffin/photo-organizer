"""
Discovery strategies for the unified organize/ingest pipeline.

A Discoverer is responsible for Phase A (pre-savepoint) of the pipeline: producing
new file records from a tree and committing the observed-reality rows (files,
media_metadata, file_occurrences). It returns a DiscoveryResult with the list of
imported paths so the pipeline can feed downstream steps (preview CSV, etc).

Observed-reality commits live here — outside the savepoint — because they describe
what is actually on disk right now. Inferred/planned data (links, dest_paths) is
the pipeline's responsibility and sits inside the savepoint.
"""
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Protocol, Set

from ..database.ops import DBOperations


@dataclass
class DiscoveryResult:
    """Summary returned by a Discoverer after Phase A completes."""
    imported_paths: List[str] = field(default_factory=list)
    scanned_count: int = 0
    renamed_count: int = 0


class Discoverer(Protocol):
    """A scanner strategy that fills in observed-reality catalog rows and commits them."""

    def discover(self, db_ops: DBOperations, conn: sqlite3.Connection) -> DiscoveryResult:
        ...


class UntrackedTreeDiscoverer:
    """
    Walks an external source tree with DiskScanner, upserting file records,
    media metadata, and source occurrences. Used by the organize pipeline.
    """

    def __init__(self, src_root: Path, is_seed: bool,
                 skip_dirs: Optional[Set[Path]] = None,
                 max_workers: int = 3):
        self.src_root = src_root
        self.is_seed = is_seed
        self.skip_dirs = skip_dirs
        self.max_workers = max_workers

    def discover(self, db_ops: DBOperations, conn: sqlite3.Connection) -> DiscoveryResult:
        from ..scanning.filesystem import DiskScanner

        logging.info(f"Scanning {self.src_root} (Seed={self.is_seed})...")
        scanner = DiskScanner()
        known_sparse_hashes = db_ops.fetch_known_sparse_hashes()

        result = DiscoveryResult()
        for record in scanner.scan(self.src_root, self.is_seed, known_sparse_hashes,
                                   self.skip_dirs, max_workers=self.max_workers):
            file_id = db_ops.upsert_file_record(record)
            if record.type in ('raw', 'jpeg', 'video', 'psd', 'tiff'):
                db_ops.upsert_media_metadata(file_id, record)

            hash_value = record.hash or record.sparse_hash
            if hash_value:
                mtime = record.mtime if record.mtime is not None else record.orig_path.stat().st_mtime
                db_ops.record_occurrence(
                    file_id=file_id,
                    path=record.orig_path,
                    is_seed=record.is_seed,
                    mtime=mtime,
                    size_bytes=record.size_bytes,
                    hash_value=hash_value,
                    is_sparse=record.hash_is_sparse,
                )
                result.imported_paths.append(str(record.orig_path))

            result.scanned_count += 1
            if result.scanned_count % 1000 == 0:
                conn.commit()

        conn.commit()
        logging.info(f"Scan complete. Processed {result.scanned_count} files.")
        return result


class CataloguedTreeDiscoverer:
    """
    Walks an already-catalogued dest tree with DestinationSyncer, reconciling renames
    and importing net-new files in ingest_mode (dest_path=NULL). Used by --ingest-dest.

    The rename reconciliation step is propagated dry_run so a preview does not mutate
    existing files' dest_path; net-new imports are always committed because they
    describe newly-observed files on disk.
    """

    def __init__(self, dest_root: Path, max_workers: int = 3,
                 dry_run_renames: bool = False):
        self.dest_root = dest_root
        self.max_workers = max_workers
        self.dry_run_renames = dry_run_renames

    def discover(self, db_ops: DBOperations, conn: sqlite3.Connection) -> DiscoveryResult:
        from ..sync.path_sync import DestinationSyncer

        syncer = DestinationSyncer(db_ops, max_workers=self.max_workers)

        # Rename detection + new-file discovery. dry_run propagation ensures rename
        # updates are not persisted when the caller asked for a preview.
        sync_report = syncer.sync_destinations([self.dest_root], dry_run=self.dry_run_renames)

        result = DiscoveryResult(
            scanned_count=sync_report.scanned_count,
            renamed_count=sync_report.renamed_count,
        )

        # Commit rename reconciliation unconditionally on real runs — renames are
        # observed reality (the file is at its current on-disk path, period). Must
        # happen before any early-return so the reconciliation persists even when
        # there are no new files to import.
        if not self.dry_run_renames and sync_report.renamed_count > 0:
            conn.commit()

        if not sync_report.new_files:
            logging.info("No new files found in destination.")
            return result

        logging.info(f"Found {len(sync_report.new_files)} new file(s) to ingest.")
        # Import is always a real write (ingest_mode leaves dest_path NULL so the planner
        # can route). These files genuinely exist on disk — observational, not speculative.
        syncer.import_new_files(sync_report.new_files, sync_report,
                                dry_run=False, ingest_mode=True)
        conn.commit()
        result.imported_paths = list(sync_report.imported_files)
        return result
