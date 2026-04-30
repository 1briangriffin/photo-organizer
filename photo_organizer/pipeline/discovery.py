"""
Discovery strategies for the unified organize/ingest pipeline.

A Discoverer is responsible for Phase A (pre-savepoint) of the pipeline: producing
new file records from a tree and committing the observed-reality rows (files,
media_metadata, file_occurrences). It returns a DiscoveryResult with the ids and
paths of imported files so the pipeline can feed downstream steps (preview CSV,
PipelineCandidates seeding, etc).

Observed-reality commits live here — outside the savepoint — because they describe
what is actually on disk right now. Inferred/planned data (links, dest_paths) is
the pipeline's responsibility and sits inside the savepoint.

A Discoverer may also queue *deferred changes* — state mutations discovered in
Phase A that must be applied inside Phase B's savepoint so dry-run rollback can
undo them. The canonical case is rename reconciliation for --ingest-dest:
sync_destinations detects renames in Phase A but leaves them as structured
RenameRecords on the SyncReport; apply_deferred_changes then applies them inside
the savepoint via DestinationSyncer.apply_renames.
"""
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Protocol, Set, TYPE_CHECKING

from ..database.ops import DBOperations
from .observations import ObservationRecorder

if TYPE_CHECKING:
    from ..sync.path_sync import SyncReport


@dataclass
class DiscoveryResult:
    """Summary returned by a Discoverer after Phase A completes.

    imported_file_ids seeds the pipeline's PipelineCandidates — the working set
    Phase B writers operate on. It includes ids returned by upsert for every
    record the discoverer accepted (even re-upserts of already-catalogued rows),
    because those rows are part of *this* run's working set even when no insert
    occurred.

    sync_report is present only for discoverers that use DestinationSyncer
    (CataloguedTreeDiscoverer). It carries detected-but-not-yet-applied rename
    records so apply_deferred_changes can apply them inside Phase B's savepoint.
    """
    imported_paths: List[str] = field(default_factory=list)
    scanned_count: int = 0
    renamed_count: int = 0
    imported_file_ids: Set[int] = field(default_factory=set)
    sync_report: Optional["SyncReport"] = None


class Discoverer(Protocol):
    """A scanner strategy that fills in observed-reality catalog rows and commits them."""

    def discover(self, db_ops: DBOperations, conn: sqlite3.Connection) -> DiscoveryResult:
        ...

    def apply_deferred_changes(
        self,
        db_ops: DBOperations,
        conn: sqlite3.Connection,
        result: DiscoveryResult,
    ) -> Set[int]:
        """
        Apply state changes the discoverer deferred to Phase B.

        Called by the pipeline as the first step *inside* the Phase B savepoint
        so the changes roll back on dry-run. Returns the set of file_ids whose
        catalog state was just mutated, for inclusion in PipelineCandidates.

        UntrackedTreeDiscoverer has no deferred changes (everything it does is
        observed reality, committed in Phase A) — it returns an empty set.
        CataloguedTreeDiscoverer applies the rename reconciliation it detected
        during discover().
        """
        ...


class UntrackedTreeDiscoverer:
    """
    Walks an external source tree with DiskScanner, upserting file records,
    media metadata, and source occurrences. Used by the organize pipeline.

    No deferred changes — every row written here describes observed reality on
    disk and is committed in Phase A.
    """

    def __init__(self, src_root: Path, is_seed: bool,
                 skip_dirs: Optional[Set[Path]] = None,
                 max_workers: int = 3,
                 run_id: Optional[int] = None):
        self.src_root = src_root
        self.is_seed = is_seed
        self.skip_dirs = skip_dirs
        self.max_workers = max_workers
        self.run_id = run_id

    def discover(self, db_ops: DBOperations, conn: sqlite3.Connection) -> DiscoveryResult:
        from ..scanning.filesystem import DiskScanner

        logging.info(f"Scanning {self.src_root} (Seed={self.is_seed})...")
        scanner = DiskScanner()
        known_sparse_hashes = db_ops.fetch_known_sparse_hashes()
        observations = ObservationRecorder(db_ops, self.run_id)

        result = DiscoveryResult()
        for record in scanner.scan(self.src_root, self.is_seed, known_sparse_hashes,
                                   self.skip_dirs, max_workers=self.max_workers):
            file_id = db_ops.upsert_file_record(record)
            result.imported_file_ids.add(file_id)
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
                observations.record_file_present(
                    file_id=file_id,
                    path=record.orig_path,
                    root_kind="source",
                    hash_value=record.hash,
                    sparse_hash=record.sparse_hash,
                    hash_is_sparse=record.hash_is_sparse,
                    size_bytes=record.size_bytes,
                    mtime=mtime,
                    match_method="scan",
                )
                result.imported_paths.append(str(record.orig_path))

            result.scanned_count += 1
            if result.scanned_count % 1000 == 0:
                conn.commit()

        conn.commit()
        logging.info(f"Scan complete. Processed {result.scanned_count} files.")
        return result

    def apply_deferred_changes(
        self,
        db_ops: DBOperations,
        conn: sqlite3.Connection,
        result: DiscoveryResult,
    ) -> Set[int]:
        # Source discovery has no deferred mutations — every row it writes is
        # observed reality and was committed in Phase A.
        return set()


class CataloguedTreeDiscoverer:
    """
    Walks an already-catalogued dest tree with DestinationSyncer. Used by
    --ingest-dest.

    Phase A (discover):
    - sync_destinations(apply_renames=False): DETECTS renames but does not apply
      them. Structured RenameRecords ride on the SyncReport.
    - import_new_files(ingest_mode=True): commits net-new file rows with
      dest_path=NULL so the planner can route them.

    Phase B (apply_deferred_changes):
    - syncer.apply_renames(sync_report): persists the detected renames. Called
      inside the pipeline savepoint so dry-run rollback undoes them.
    """

    def __init__(self, dest_root: Path, max_workers: int = 3,
                 run_id: Optional[int] = None):
        self.dest_root = dest_root
        self.max_workers = max_workers
        self.run_id = run_id

    def discover(self, db_ops: DBOperations, conn: sqlite3.Connection) -> DiscoveryResult:
        from ..sync.path_sync import DestinationSyncer

        syncer = DestinationSyncer(db_ops, max_workers=self.max_workers, run_id=self.run_id)

        # Detect-only here. The pipeline applies the renames inside its Phase B
        # savepoint so dry-run rollback unwinds them along with the rest of
        # Phase B's inferred state.
        sync_report = syncer.sync_destinations(
            [self.dest_root], apply_renames=False,
        )

        result = DiscoveryResult(
            scanned_count=sync_report.scanned_count,
            renamed_count=sync_report.renamed_count,
            sync_report=sync_report,
        )

        if sync_report.new_files:
            logging.info(f"Found {len(sync_report.new_files)} new file(s) to ingest.")
            # Net-new files are always committed — they exist on disk, that's
            # observed reality. ingest_mode=True leaves dest_path NULL so the
            # planner can route them in Phase B.
            imported_ids = syncer.import_new_files(
                sync_report.new_files, sync_report,
                dry_run=False, ingest_mode=True,
            )
            result.imported_file_ids.update(imported_ids)
            result.imported_paths = list(sync_report.imported_files)
            conn.commit()
        else:
            logging.info("No new files found in destination.")

        return result

    def apply_deferred_changes(
        self,
        db_ops: DBOperations,
        conn: sqlite3.Connection,
        result: DiscoveryResult,
    ) -> Set[int]:
        if not result.sync_report or not result.sync_report.renames:
            return set()

        from ..sync.path_sync import DestinationSyncer
        syncer = DestinationSyncer(db_ops, max_workers=self.max_workers, run_id=self.run_id)
        return syncer.apply_renames(result.sync_report)
