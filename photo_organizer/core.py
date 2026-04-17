import logging
from pathlib import Path
from typing import Set, Optional

from .database.db import DBManager
from .database.ops import DBOperations
from .reporting import ReportGenerator
from .scanning.filesystem import DiskScanner
from .metadata.linking import FileLinker
from .organization.rules import DestinationPlanner
from .organization.mover import FileMover
from . import config

class PhotoOrganizerApp:
    def __init__(self, db_path: Path):
        self.db_manager = DBManager(db_path)

    def organize(self,
                 src_root: Path,
                 dest_root: Path,
                 is_seed: bool = False,
                 move: bool = False,
                 dry_run: bool = False,
                 dry_run_csv: Optional[Path] = None,
                 skip_dirs: Optional[Set[Path]] = None,
                 max_workers: int = 3,
                 auto_sync: bool = False):
        """
        Executes the 'Phase 1' Organization Pipeline.
        1. Scan & Hash (Deduplicate)
        2. Link (Sidecars/PSDs)
        3. Plan (Calculate Destinations)
        4. Execute (Copy/Move)
        5. (Optional) Sync paths with renamed files in destination

        Args:
            max_workers: Number of parallel workers for hashing and file operations
            auto_sync: If True, run path sync after organization to detect renames
        """
        with self.db_manager as conn:
            db_ops = DBOperations(conn)

            # --- Step 1: Scanning ---
            logging.info(f"Scanning {src_root} (Seed={is_seed})...")
            scanner = DiskScanner()

            # Load known sparse hashes to optimize 2-stage hashing
            known_sparse_hashes = db_ops.fetch_known_sparse_hashes()

            processed_count = 0
            for record in scanner.scan(src_root, is_seed, known_sparse_hashes, skip_dirs, max_workers=max_workers):
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

                processed_count += 1
                if processed_count % 1000 == 0:
                    conn.commit()

            conn.commit()
            logging.info(f"Scan complete. Processed {processed_count} files.")

            # --- Step 2: Linking ---
            # We must link Sidecars/PSDs before planning destinations
            linker = FileLinker(db_ops)
            linker.link_raw_sidecars()
            linker.link_psds()
            linker.link_raw_outputs()
            # Scan and link commits have now fired; DB reflects real source-derived facts.

            # --- Step 3: Planning ---
            # Use a savepoint so dry-run can preview planned dest_paths then roll them back.
            # Scan/hash/link data written above is already committed and will be preserved.
            logging.info("Planning destinations...")
            conn.execute("SAVEPOINT plan_start")
            try:
                planner = DestinationPlanner(db_ops)
                planner.plan_all(dest_root)

                # Important: Assign destinations for Linked files (Sidecars/PSDs)
                # (Note: You may need to add specific methods in Planner for this,
                #  or ensure plan_all covers them via dependency logic.
                #  For now, we assume Planner handles the main files,
                #  and we might need a 'Planner.assign_linked' pass here.)
                self._assign_linked_destinations(db_ops)

                if dry_run:
                    # Generate preview CSV while dest_paths are visible in the DB,
                    # then roll back so the catalog never reflects unexecuted plans.
                    self._write_dry_run_report(db_ops, src_root, dest_root, dry_run_csv)
                    conn.execute("ROLLBACK TO SAVEPOINT plan_start")
                    conn.execute("RELEASE SAVEPOINT plan_start")
                    logging.info("Dry-run complete. No dest_path changes were persisted.")
                    return
                else:
                    conn.execute("RELEASE SAVEPOINT plan_start")
                    conn.commit()
            except Exception:
                conn.execute("ROLLBACK TO SAVEPOINT plan_start")
                conn.execute("RELEASE SAVEPOINT plan_start")
                raise

            # --- Step 4: Execution ---
            mover = FileMover(db_ops)
            mover.execute(move_mode=move, dry_run=dry_run)

            logging.info("Organization phase complete.")

            # --- Step 5: Auto-Sync (Optional) ---
            if auto_sync and not dry_run:
                self._run_auto_sync(db_ops, dest_root, conn)

    def _write_dry_run_report(self, db_ops: DBOperations, src_root: Path, dest_root: Path, csv_path: Optional[Path]):
        resolved_csv = csv_path or (dest_root / "dry_run_preview.csv")
        resolved_csv.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"[DRY RUN] Writing preview report to {resolved_csv}")
        reporter = ReportGenerator(db_ops)
        reporter.generate_source_report(str(src_root), str(resolved_csv))

    def _assign_linked_destinations(self, db_ops: DBOperations):
        """
        Helper to assign destinations for Sidecars/PSDs based on their parents.
        (Ported logic from original assign_sidecar_destinations)
        """
        cur = db_ops.conn.cursor()
        
        # 1. Sidecars follow RAWs
        cur.execute("""
            SELECT rs.sidecar_file_id, f.orig_name, r.dest_path
            FROM raw_sidecars rs
            JOIN files f ON rs.sidecar_file_id = f.id
            JOIN files r ON rs.raw_file_id = r.id
            WHERE f.dest_path IS NULL AND r.dest_path IS NOT NULL
        """)
        rows = cur.fetchall()
        for sidecar_id, name, raw_dest_str in rows:
            raw_dest = Path(raw_dest_str)
            sidecar_dest = raw_dest.with_suffix(Path(name).suffix)
            # (Collision handling omitted for brevity, usually matches RAW stem)
            db_ops.update_dest_path(sidecar_id, str(sidecar_dest))
            
        # 2. PSDs follow Sources
        cur.execute("""
            SELECT psl.psd_file_id, f.orig_name, s.dest_path
            FROM psd_source_links psl
            JOIN files f ON psl.psd_file_id = f.id
            JOIN files s ON psl.source_file_id = s.id
            WHERE f.dest_path IS NULL AND s.dest_path IS NOT NULL
        """)
        rows = cur.fetchall()
        for psd_id, name, src_dest_str in rows:
            src_dest = Path(src_dest_str)
            dest_parent = src_dest.parent

            # PSDs are considered outputs; if the source lives in the raw tree, mirror the
            # folder structure under output instead.
            parts = list(dest_parent.parts)
            for idx, part in enumerate(parts):
                if part.lower() == "raw":
                    parts[idx] = "output"
                    dest_parent = Path(*parts)
                    break

            psd_dest = dest_parent / name  # PSD keeps its own name, just moves folder
            db_ops.update_dest_path(psd_id, str(psd_dest))

    def _run_auto_sync(self, db_ops: DBOperations, dest_root: Path, conn):
        """
        Run path sync to detect renamed files in the destination.

        This catches files that were renamed outside of the organizer
        (e.g., in Lightroom, Finder, or Explorer).
        """
        from .sync.path_sync import DestinationSyncer

        logging.info("--- Running Auto-Sync ---")
        syncer = DestinationSyncer(db_ops)

        report = syncer.sync_destinations([dest_root], dry_run=False)

        if report.renamed_count > 0:
            conn.commit()

        self._log_sync_report(report)

    @staticmethod
    def _log_sync_report(report) -> None:
        """Log a human-readable summary of a SyncReport."""
        if report.renamed_count > 0:
            logging.info(f"Auto-sync: Updated {report.renamed_count} renamed file(s)")
            for old, new in report.renamed_files[:5]:
                logging.info(f"  {Path(old).name} → {Path(new).name}")
            if len(report.renamed_files) > 5:
                logging.info(f"  ... and {len(report.renamed_files) - 5} more")
        else:
            logging.info("Auto-sync: All paths are up to date")

        if report.new_count > 0:
            logging.info(f"Auto-sync: Found {report.new_count} new file(s) not in database")
            logging.info("  Use --sync-dest --import-new to add them")

        if report.missing_count > 0:
            logging.warning(f"Auto-sync: {report.missing_count} file(s) missing from destination")

    # ------------------------------------------------------------------
    # Stub methods — implemented in subsequent PRs
    # ------------------------------------------------------------------

    def sync_dest(self, dest_root: Path, dry_run: bool = False,
                  import_new: bool = False, max_workers: int = 3) -> None:
        """
        Sync the catalog with renamed files in dest without running a source scan.

        Detects renames (same directory, different filename) and optionally imports
        files found on disk that are not yet in the catalog. Imported files have
        their dest_path set to their current location (ingest_mode=False) since
        they are already correctly placed.
        """
        from .sync.path_sync import DestinationSyncer

        logging.info("--- Running Destination Sync ---")
        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            syncer = DestinationSyncer(db_ops, max_workers=max_workers)

            report = syncer.sync_destinations([dest_root], dry_run=dry_run)

            if import_new and report.new_files:
                logging.info(f"Importing {len(report.new_files)} new file(s) found in dest...")
                syncer.import_new_files(report.new_files, report,
                                        dry_run=dry_run, ingest_mode=False)

            if not dry_run and (report.renamed_count > 0 or report.imported_count > 0):
                conn.commit()

            self._log_sync_report(report)

    def validate_dest(self, dest_root: Path, report_csv: Optional[Path] = None) -> None:
        """
        Scan dest_root against the catalog and write a validation CSV.

        Reports CONFIRMED (at expected location), MISSING (in catalog but absent),
        and UNTRACKED (on disk but not in catalog) files.
        """
        resolved_csv = report_csv or (dest_root / "dest_validation.csv")
        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            reporter = ReportGenerator(db_ops)
            reporter.generate_dest_validation_report(dest_root, resolved_csv)

    def ingest_dest(self, dest_root: Path, move: bool = False,
                    dry_run: bool = False, dry_run_csv: Optional[Path] = None,
                    max_workers: int = 3) -> None:
        """
        Discover, link, and route new files that appeared in dest after a previous
        organize run (e.g. Canon DPP exports written next to already-organized RAWs).

        Pipeline:
        1. Scan dest for files not in catalog (new files).
        2. Import them with dest_path=NULL (ingest_mode) so the planner can route them.
        3. Link: sidecars by dest co-location, JPEG/TIFF outputs by metadata, PSDs by stem.
        4. Savepoint → assign linked destinations + plan primary files → move (or preview).
        5. On dry_run: rollback savepoint so dest_path assignments are not persisted.

        Note: import and link data (steps 2-3) are committed even on dry_run, consistent
        with the organize pipeline's dry-run behaviour.

        Side effect (non-dry-run only): the step-1 scan also detects tracked files
        that were renamed in dest and writes those path updates to the catalog.
        This is broader than pure ingest; run --sync-dest explicitly if you want to
        separate rename reconciliation from ingestion. In dry_run mode no rename
        updates are written (dry_run is propagated to sync_destinations).
        """
        from .sync.path_sync import DestinationSyncer

        logging.info("--- Running Destination Ingest ---")
        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            syncer = DestinationSyncer(db_ops, max_workers=max_workers)

            # Step 1: Discover new files. Propagate dry_run so rename syncs are not
            # persisted when the caller asked for a preview.
            report = syncer.sync_destinations([dest_root], dry_run=dry_run)

            if not report.new_files:
                logging.info("No new files found in destination.")
                return

            logging.info(f"Found {len(report.new_files)} new file(s) to ingest.")

            # Step 2: Import without dest_path so the planner can assign correct location
            syncer.import_new_files(report.new_files, report,
                                    dry_run=False, ingest_mode=True)
            conn.commit()
            imported_paths = list(report.imported_files)  # snapshot for dry-run CSV

            # Step 3: Link — sidecars by dest co-location; outputs + PSDs by metadata/stem
            linker = FileLinker(db_ops)
            linker.link_raw_sidecars_by_dest()
            linker.link_raw_outputs()
            linker.link_psds()
            # link_raw_sidecars_by_dest, link_psds each commit internally

            # Step 4: Plan (savepoint so dry-run can preview and roll back dest_path writes)
            conn.execute("SAVEPOINT ingest_plan")
            try:
                self._assign_linked_destinations(db_ops)

                planner = DestinationPlanner(db_ops)
                planner.plan_all(dest_root)

                mover = FileMover(db_ops)
                mover.execute(move_mode=move, dry_run=dry_run)

                if dry_run:
                    # Emit preview CSV while planned dest_paths are still in the DB.
                    resolved_csv = dry_run_csv or (dest_root / "ingest_dry_run_preview.csv")
                    reporter = ReportGenerator(db_ops)
                    reporter.generate_ingest_preview_report(
                        imported_paths, resolved_csv, move_mode=move,
                    )

                    conn.execute("ROLLBACK TO SAVEPOINT ingest_plan")
                    conn.execute("RELEASE SAVEPOINT ingest_plan")
                    logging.info("Dry-run complete. No dest_path changes were persisted.")
                else:
                    conn.execute("RELEASE SAVEPOINT ingest_plan")
                    conn.commit()
            except Exception:
                conn.execute("ROLLBACK TO SAVEPOINT ingest_plan")
                conn.execute("RELEASE SAVEPOINT ingest_plan")
                raise
