import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set

from .database.db import DBManager
from .database.ops import DBOperations
from .reporting import ReportGenerator
from .metadata.linking import FileLinker
from .organization.rules import DestinationPlanner
from .organization.mover import FileMover
from . import config
from .pipeline.actions import (
    ActionSpec,
    RunActionRecorder,
    action_status_from_target,
    canonical_path_action,
    link_action,
    move_action,
)
from .pipeline.candidates import PipelineCandidates
from .pipeline.lifecycle import supersede_stale_proposals
from .pipeline.discovery import (
    CataloguedTreeDiscoverer,
    Discoverer,
    DiscoveryResult,
    UntrackedTreeDiscoverer,
)

# Callback contract for writing the dry-run preview CSV.
# Captures mode-specific context (src_root, move_mode, etc.) via closure in the
# organize/ingest_dest wrappers; the pipeline itself stays mode-agnostic.
PreviewWriter = Callable[[DBOperations, DiscoveryResult, Optional[Path]], None]

# Callback for selecting the sidecar-linking heuristic. Two implementations exist
# in FileLinker (link_raw_sidecars matches by orig_path, link_raw_sidecars_by_dest
# matches by dest_path). The wrapper picks the right one for its mode. Takes the
# run's candidate_file_ids so the linker can scope its writes to this run's
# working set.
SidecarLinker = Callable[[FileLinker, Set[int]], None]


class PhotoOrganizerApp:
    def __init__(self, db_path: Path):
        self.db_manager = DBManager(db_path)

    # ------------------------------------------------------------------
    # Public mode entry points (thin wrappers over _run_pipeline)
    # ------------------------------------------------------------------

    def organize(self,
                 src_root: Path,
                 dest_root: Path,
                 is_seed: bool = False,
                 move: bool = False,
                 dry_run: bool = False,
                 dry_run_csv: Optional[Path] = None,
                 skip_dirs: Optional[Set[Path]] = None,
                 max_workers: int = 3,
                 auto_sync: bool = False,
                 run_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Organize pipeline: scan an external source tree and route files into dest.
        Phases: (A) discover/import source files; (B) savepoint-wrapped link + plan +
        execute; (C) optional auto-sync of dest for renames.
        """
        discoverer = UntrackedTreeDiscoverer(
            src_root=src_root, is_seed=is_seed,
            skip_dirs=skip_dirs, max_workers=max_workers, run_id=run_id,
        )

        def preview_writer(db_ops: DBOperations, _result: DiscoveryResult,
                           csv_path: Optional[Path]) -> None:
            resolved = csv_path or (dest_root / "dry_run_preview.csv")
            resolved.parent.mkdir(parents=True, exist_ok=True)
            logging.info(f"[DRY RUN] Writing preview report to {resolved}")
            # Pass skip_dirs through so the preview matches what the real scan would
            # process — otherwise the CSV can list files the organize run would ignore.
            ReportGenerator(db_ops).generate_source_report(
                str(src_root), str(resolved), skip_dirs=skip_dirs,
            )

        def sidecar_linker(linker: FileLinker, candidate_ids: Set[int]) -> None:
            linker.link_raw_sidecars(candidate_ids)

        stats = self._run_pipeline(
            discoverer=discoverer,
            dest_root=dest_root,
            move=move,
            dry_run=dry_run,
            dry_run_csv=dry_run_csv,
            preview_writer=preview_writer,
            sidecar_linker=sidecar_linker,
            savepoint_name="plan_start",
            run_id=run_id,
            record_run_actions=True,
        )

        # Auto-sync runs outside the pipeline because it is a post-organize reconciliation
        # that does its own discovery + commits and should not be rolled back by the
        # pipeline savepoint. Only meaningful on real runs.
        if auto_sync and not dry_run:
            with self.db_manager as conn:
                db_ops = DBOperations(conn)
                auto_stats = self._run_auto_sync(db_ops, dest_root, conn)
                stats["auto_sync_renamed"] = auto_stats.get("renamed", 0)
                stats["auto_sync_new"] = auto_stats.get("new", 0)
                stats["auto_sync_missing"] = auto_stats.get("missing", 0)

        return stats

    def ingest_dest(self, dest_root: Path, move: bool = False,
                    dry_run: bool = False, dry_run_csv: Optional[Path] = None,
                    max_workers: int = 3,
                    run_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Discover, link, and route new files that appeared in dest after a previous
        organize run (e.g. Canon DPP exports written next to already-organized RAWs).

        Pipeline:
        1. Discovery (Phase A): sync_destinations for rename detection + import_new_files
           in ingest_mode. New file rows describe observed reality and are committed.
        2. Savepoint (Phase B): link sidecars/PSDs/outputs, assign linked destinations,
           plan net-new files, execute moves/copies. On dry_run: ROLLBACK. Otherwise:
           RELEASE + commit so destination file_occurrences from the mover persist.

        Note: rename detection in step 1 is dry_run-propagated, so in dry-run mode no
        existing dest_path rows are updated.
        """
        discoverer = CataloguedTreeDiscoverer(
            dest_root=dest_root, max_workers=max_workers, run_id=run_id,
        )

        def preview_writer(db_ops: DBOperations, result: DiscoveryResult,
                           csv_path: Optional[Path]) -> None:
            resolved = csv_path or (dest_root / "ingest_dry_run_preview.csv")
            resolved.parent.mkdir(parents=True, exist_ok=True)
            ReportGenerator(db_ops).generate_ingest_preview_report(
                result.imported_paths,
                resolved,
                move_mode=move,
                rename_records=result.sync_report.renames if result.sync_report else [],
            )

        def sidecar_linker(linker: FileLinker, candidate_ids: Set[int]) -> None:
            linker.link_raw_sidecars_by_dest(candidate_ids)

        return self._run_pipeline(
            discoverer=discoverer,
            dest_root=dest_root,
            move=move,
            dry_run=dry_run,
            dry_run_csv=dry_run_csv,
            preview_writer=preview_writer,
            sidecar_linker=sidecar_linker,
            savepoint_name="ingest_plan",
            skip_if_no_imports=True,
            run_id=run_id,
            record_run_actions=True,
        )

    # ------------------------------------------------------------------
    # Shared pipeline
    # ------------------------------------------------------------------

    def _run_pipeline(self,
                      discoverer: Discoverer,
                      dest_root: Path,
                      move: bool,
                      dry_run: bool,
                      dry_run_csv: Optional[Path],
                      preview_writer: PreviewWriter,
                      sidecar_linker: SidecarLinker,
                      savepoint_name: str,
                      skip_if_no_imports: bool = False,
                      run_id: Optional[int] = None,
                      record_run_actions: bool = False) -> Dict[str, Any]:
        """
        Unified pipeline shared by organize() and ingest_dest().

        Phase A (outside savepoint): discoverer commits observed reality — files rows,
        media_metadata, file_occurrences from the scan.

        Phase B (inside savepoint): sidecar/PSD/output linking, linked-dest assignment,
        destination planning, and mover execution. On dry_run these are all rolled back.
        On a real run, a single commit after mover.execute persists everything in phase B,
        including the destination file_occurrences the mover writes via record_occurrence.

        This commit-after-mover is a deliberate change from the previous organize()
        implementation, which committed before mover.execute and silently dropped
        destination occurrences when DBManager closed without committing.
        """
        mode_label = "dry-run" if dry_run else "real run"
        logging.info(f"--- Running pipeline ({mode_label}, savepoint={savepoint_name}) ---")

        stats: Dict[str, Any] = {
            "scanned": 0,
            "renamed": 0,
            "imported": 0,
            "planned": 0,
            "skipped": 0,
            "moved": 0,
            "copied": 0,
            "errors": 0,
            "review_required": 0,
        }

        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            action_recorder = RunActionRecorder(
                db_ops,
                run_id if record_run_actions else None,
            )

            # Phase A: discover + commit observed reality
            result = discoverer.discover(db_ops, conn)
            stats["scanned"] = result.scanned_count
            stats["renamed"] = result.renamed_count
            stats["imported"] = len(result.imported_paths)
            if result.sync_report is not None:
                stats["review_required"] = result.sync_report.review_count

            # Early-exit: nothing to do if the working set is empty. The
            # ingest-dest mode opts into this via skip_if_no_imports. Renames
            # alone are also worth doing Phase B for — applying them inside
            # the savepoint is the whole point of Change 1 — so a run with
            # zero net-new imports but detected renames still proceeds.
            has_deferred_renames = bool(
                result.sync_report and result.sync_report.renames
            )
            if (skip_if_no_imports
                    and not result.imported_file_ids
                    and not has_deferred_renames):
                logging.info("No new files or renames to process — pipeline exiting early.")
                return stats

            # Phase B: link + plan + execute inside a savepoint
            conn.execute(f"SAVEPOINT {savepoint_name}")
            action_specs: list[ActionSpec] = []
            move_specs: list[ActionSpec] = []
            try:
                # PipelineCandidates seeds from Phase A's upserted ids, then
                # accumulates ids from apply_deferred_changes and plan_all as
                # downstream writers produce them. Every Phase B writer uses
                # this set as an input filter — stale dest_path=NULL rows from
                # interrupted prior runs stay invisible.
                candidates = PipelineCandidates()
                candidates.add_many(result.imported_file_ids)

                # Apply deferred state (renames for ingest-dest) *inside* the
                # savepoint so dry-run rollback undoes them. Feed the resulting
                # ids into candidates so Phase B writers see renamed rows.
                rename_ids = discoverer.apply_deferred_changes(db_ops, conn, result)
                candidates.add_many(rename_ids)

                linker = FileLinker(db_ops, run_id=run_id)
                sidecar_linker(linker, candidates.ids())
                linker.link_psds(candidates.ids())
                raw_output_links = []
                linker.link_raw_outputs(
                    candidates.ids(),
                    dry_run=dry_run,
                    proposed_links_out=raw_output_links,
                )

                # Two passes: the first picks up sidecars/PSDs whose RAW/source parent
                # already has a dest_path (ingest-dest case — RAWs were organized in a
                # prior run). The second picks up ones whose parent was just assigned
                # by plan_all (organize case — RAWs are net-new). Each pass filters on
                # `f.dest_path IS NULL AND r.dest_path IS NOT NULL`, so each is a no-op
                # for the other mode.
                linked_dest_ids = self._assign_linked_destinations(db_ops, candidates.ids())
                candidates.add_many(linked_dest_ids)
                planner = DestinationPlanner(db_ops)
                planned_ids = planner.plan_all(dest_root, candidates.ids())
                candidates.add_many(planned_ids)
                linked_dest_ids = self._assign_linked_destinations(db_ops, candidates.ids())
                candidates.add_many(linked_dest_ids)

                move_specs = self._build_move_action_specs(
                    db_ops,
                    candidates.ids(),
                    move_mode=move,
                    status="proposed" if dry_run else "applied",
                )

                if action_recorder.enabled:
                    action_specs.extend(self._build_canonical_action_specs_from_renames(
                        result.sync_report.renames if result.sync_report else [],
                        status="proposed" if dry_run else "applied",
                    ))
                    action_specs.extend(self._build_relationship_action_specs(
                        db_ops,
                        candidates.ids(),
                        raw_output_links,
                        run_id=run_id,
                        status="proposed" if dry_run else "applied",
                    ))
                    if dry_run:
                        action_specs.extend(move_specs)

                mover = FileMover(db_ops)
                mover_counts = mover.execute(
                    file_ids=candidates.ids(),
                    move_mode=move,
                    dry_run=dry_run,
                )
                stats.update(mover_counts)

                if action_recorder.enabled and not dry_run:
                    action_specs.extend(
                        self._finalize_move_action_statuses(move_specs)
                    )

                if dry_run:
                    preview_writer(db_ops, result, dry_run_csv)
                    conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    action_recorder.record_many(action_specs)
                    superseded = 0
                    if action_recorder.enabled:
                        superseded = supersede_stale_proposals(db_ops, run_id)
                    if action_specs or superseded:
                        conn.commit()
                    logging.info("Dry-run complete. No link or dest_path changes were persisted.")
                else:
                    action_recorder.record_many(action_specs)
                    if action_recorder.enabled:
                        supersede_stale_proposals(db_ops, run_id)
                    conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    # Single commit persists link tables, dest_paths, and destination
                    # file_occurrences from mover.execute.
                    conn.commit()
                    logging.info("Pipeline complete.")
            except Exception as exc:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                if action_recorder.enabled and not dry_run:
                    failed_specs = self._mark_action_specs_failed(
                        [*action_specs, *move_specs],
                        error_message=str(exc),
                    )
                    action_recorder.record_many(failed_specs)
                    if failed_specs:
                        conn.commit()
                raise

        return stats

    @staticmethod
    def _build_canonical_action_specs_from_renames(
        renames,
        *,
        status: str,
    ) -> list[ActionSpec]:
        actions: list[ActionSpec] = []
        for sequence, rec in enumerate(renames, start=1):
            actions.append(canonical_path_action(
                file_id=rec.file_id,
                old_path=rec.old_path,
                new_path=rec.new_path,
                status=status,
                sequence=sequence,
                confidence=getattr(
                    rec,
                    "confidence",
                    100 if not rec.matched_hash_is_sparse else 80,
                ),
                method=getattr(rec, "match_method", "hash_same_directory"),
            ))
        return actions

    @staticmethod
    def _build_relationship_action_specs(
        db_ops: DBOperations,
        candidate_file_ids: Set[int],
        raw_output_links: list,
        *,
        run_id: Optional[int],
        status: str,
    ) -> list[ActionSpec]:
        actions: list[ActionSpec] = []
        sequence = 1

        if candidate_file_ids:
            placeholders = ",".join("?" for _ in candidate_file_ids)
            candidate_params = [int(i) for i in candidate_file_ids]
            cur = db_ops.conn.cursor()
            cur.execute(
                f"""
                SELECT raw_file_id, sidecar_file_id, created_by_run_id
                FROM raw_sidecars
                WHERE sidecar_file_id IN ({placeholders})
                """,
                candidate_params,
            )
            for raw_id, sidecar_id, created_by_run_id in cur.fetchall():
                row_status = status
                if status != "proposed" and created_by_run_id != run_id:
                    row_status = "skipped"
                actions.append(link_action(
                    action_type="link_raw_sidecar",
                    entity_id=int(sidecar_id),
                    left_id=int(raw_id),
                    right_id=int(sidecar_id),
                    status=row_status,
                    sequence=sequence,
                    method="dest_stem",
                    confidence=100,
                ))
                sequence += 1

            cur.execute(
                f"""
                SELECT psd_file_id, source_file_id, confidence, link_method, created_by_run_id
                FROM psd_source_links
                WHERE psd_file_id IN ({placeholders})
                """,
                candidate_params,
            )
            for psd_id, source_id, confidence, method, created_by_run_id in cur.fetchall():
                row_status = status
                if status != "proposed" and created_by_run_id != run_id:
                    row_status = "skipped"
                actions.append(link_action(
                    action_type="link_psd_source",
                    entity_id=int(psd_id),
                    left_id=int(psd_id),
                    right_id=int(source_id),
                    status=row_status,
                    sequence=sequence,
                    confidence=confidence,
                    method=method,
                ))
                sequence += 1

        cur = db_ops.conn.cursor()
        for raw_id, _, out_id, _, _, _, _, _, confidence, method in raw_output_links:
            if confidence < config.RAW_JPEG_MIN_CONFIDENCE:
                continue
            row_status = status
            if status != "proposed":
                cur.execute(
                    """
                    SELECT created_by_run_id
                    FROM raw_outputs
                    WHERE raw_file_id = ? AND output_file_id = ?
                    """,
                    (raw_id, out_id),
                )
                row = cur.fetchone()
                if row is None:
                    row_status = "failed"
                elif row[0] != run_id:
                    row_status = "skipped"
            actions.append(link_action(
                action_type="link_raw_output",
                entity_id=int(out_id),
                left_id=int(raw_id),
                right_id=int(out_id),
                status=row_status,
                sequence=sequence,
                confidence=confidence,
                method=method,
            ))
            sequence += 1

        return actions

    @staticmethod
    def _build_move_action_specs(
        db_ops: DBOperations,
        candidate_file_ids: Set[int],
        *,
        move_mode: bool,
        status: str,
    ) -> list[ActionSpec]:
        actions: list[ActionSpec] = []
        for sequence, (file_id, src, dest, _ftype, _hash, _sparse) in enumerate(
            db_ops.get_pending_moves_for_ids(candidate_file_ids),
            start=1,
        ):
            if Path(dest).exists():
                continue
            actions.append(move_action(
                file_id=int(file_id),
                source_path=src,
                target_path=dest,
                move_mode=move_mode,
                status=status,
                sequence=sequence,
            ))
        return actions

    @staticmethod
    def _finalize_move_action_statuses(actions: list[ActionSpec]) -> list[ActionSpec]:
        finalized: list[ActionSpec] = []
        for action in actions:
            finalized.append(ActionSpec(
                action_type=action.action_type,
                entity_type=action.entity_type,
                entity_id=action.entity_id,
                source_path=action.source_path,
                target_path=action.target_path,
                status=action_status_from_target(action.target_path or ""),
                phase=action.phase,
                sequence=action.sequence,
                idempotency_key=action.idempotency_key,
                confidence=action.confidence,
                method=action.method,
                payload=action.payload,
                error_message=None if Path(action.target_path or "").exists() else "target path not found after apply",
            ))
        return finalized

    @staticmethod
    def _mark_action_specs_failed(
        actions: list[ActionSpec],
        *,
        error_message: str,
    ) -> list[ActionSpec]:
        failed: list[ActionSpec] = []
        seen: set[str] = set()
        for action in actions:
            if action.idempotency_key in seen:
                continue
            seen.add(action.idempotency_key)
            failed.append(ActionSpec(
                action_type=action.action_type,
                entity_type=action.entity_type,
                entity_id=action.entity_id,
                source_path=action.source_path,
                target_path=action.target_path,
                status="failed",
                phase=action.phase,
                sequence=action.sequence,
                idempotency_key=action.idempotency_key,
                confidence=action.confidence,
                method=action.method,
                payload=action.payload,
                error_message=error_message,
            ))
        return failed

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_scoped(cur, base_sql: str, id_column: str,
                      candidate_file_ids: Optional[Set[int]]) -> list:
        """
        Execute ``base_sql`` optionally scoped by ``candidate_file_ids`` on
        ``id_column``. Returns an empty list when the set is empty (no IDs
        to match), executes unscoped when the set is None, otherwise appends
        a parameterized ``AND {id_column} IN (...)`` clause.
        """
        if candidate_file_ids is None:
            cur.execute(base_sql)
            return cur.fetchall()
        if not candidate_file_ids:
            return []
        placeholders = ",".join("?" * len(candidate_file_ids))
        params = [int(i) for i in candidate_file_ids]
        cur.execute(f"{base_sql} AND {id_column} IN ({placeholders})", params)
        return cur.fetchall()

    def _assign_linked_destinations(self, db_ops: DBOperations,
                                    candidate_file_ids: Optional[Set[int]] = None) -> Set[int]:
        """
        Helper to assign destinations for Sidecars/PSDs based on their parents.

        ``candidate_file_ids`` filters which *children* (sidecars / PSDs) are
        eligible — the parent RAW/source does not need to be in the set (it
        may have been organized in a prior run). This matches the ingest-dest
        case where a newly imported XMP/PSD attaches to a long-established RAW.
        """
        cur = db_ops.conn.cursor()
        assigned_ids: Set[int] = set()

        # 1. Sidecars follow RAWs
        sidecar_rows = self._fetch_scoped(
            cur,
            """
            SELECT rs.sidecar_file_id, f.orig_name, r.dest_path
            FROM raw_sidecars rs
            JOIN files f ON rs.sidecar_file_id = f.id
            JOIN files r ON rs.raw_file_id = r.id
            WHERE f.dest_path IS NULL AND r.dest_path IS NOT NULL
            """,
            id_column="rs.sidecar_file_id",
            candidate_file_ids=candidate_file_ids,
        )
        for sidecar_id, name, raw_dest_str in sidecar_rows:
            raw_dest = Path(raw_dest_str)
            sidecar_dest = raw_dest.with_suffix(Path(name).suffix)
            db_ops.update_dest_path(sidecar_id, str(sidecar_dest))
            assigned_ids.add(sidecar_id)

        # 2. PSDs follow Sources
        psd_rows = self._fetch_scoped(
            cur,
            """
            SELECT psl.psd_file_id, f.orig_name, s.dest_path
            FROM psd_source_links psl
            JOIN files f ON psl.psd_file_id = f.id
            JOIN files s ON psl.source_file_id = s.id
            WHERE f.dest_path IS NULL AND s.dest_path IS NOT NULL
            """,
            id_column="psl.psd_file_id",
            candidate_file_ids=candidate_file_ids,
        )
        for psd_id, name, src_dest_str in psd_rows:
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

            psd_dest = dest_parent / name
            db_ops.update_dest_path(psd_id, str(psd_dest))
            assigned_ids.add(psd_id)

        return assigned_ids

    def _run_auto_sync(self, db_ops: DBOperations, dest_root: Path, conn) -> Dict[str, int]:
        """
        Run path sync to detect renamed files in the destination.

        This catches files that were renamed outside of the organizer
        (e.g., in Lightroom, Finder, or Explorer).
        """
        from .sync.path_sync import DestinationSyncer

        logging.info("--- Running Auto-Sync ---")
        syncer = DestinationSyncer(db_ops)

        # auto-sync is a real-run reconciliation — apply renames immediately.
        report = syncer.sync_destinations([dest_root], apply_renames=True)

        if report.renamed_count > 0:
            conn.commit()

        self._log_sync_report(report)
        return {
            "renamed": report.renamed_count,
            "new": report.new_count,
            "missing": report.missing_count,
        }

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
    # sync_dest / validate_dest (unchanged, not part of the unified pipeline)
    # ------------------------------------------------------------------

    def sync_dest(self, dest_root: Path, dry_run: bool = False,
                  import_new: bool = False, max_workers: int = 3,
                  run_id: Optional[int] = None) -> Dict[str, int]:
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
            syncer = DestinationSyncer(db_ops, max_workers=max_workers, run_id=run_id)
            action_recorder = RunActionRecorder(db_ops, run_id)

            # Detect + apply renames in one call when not a preview; on dry-run
            # we still want to report what *would* change, so detect-only and
            # rely on the caller not committing.
            report = syncer.sync_destinations(
                [dest_root], apply_renames=not dry_run,
            )
            action_recorder.record_many(
                self._build_canonical_action_specs_from_renames(
                    report.renames,
                    status="proposed" if dry_run else "applied",
                )
            )

            if import_new and report.new_files:
                logging.info(f"Importing {len(report.new_files)} new file(s) found in dest...")
                syncer.import_new_files(report.new_files, report,
                                        dry_run=dry_run, ingest_mode=False)

            if action_recorder.enabled:
                supersede_stale_proposals(db_ops, run_id)

            if (not dry_run and (report.renamed_count > 0 or report.imported_count > 0)) or run_id is not None:
                conn.commit()

            self._log_sync_report(report)
            return {
                "scanned": report.scanned_count,
                "renamed": report.renamed_count,
                "imported": report.imported_count,
                "missing": report.missing_count,
                "new": report.new_count,
            }

    def backfill_raw_metadata(self, dest_root: Path, dry_run: bool = False,
                              max_workers: int = 3,
                              limit: Optional[int] = None,
                              run_id: Optional[int] = None) -> Dict[str, int]:
        """
        Populate missing camera identity metadata (camera_serial_number,
        camera_file_number) for RAW rows by re-reading identity tags from the
        files on disk. Fills NULL columns only — accepted values are never
        overwritten — so re-running is idempotent and an interrupted run
        resumes where it left off.

        Dry-run reports the candidate scope without invoking exiftool: on a
        large library extraction is the expensive part, and the real run is
        already non-destructive.
        """
        from concurrent.futures import ThreadPoolExecutor
        from .metadata.extract import MetadataExtractor

        logging.info("--- Running RAW Metadata Backfill ---")
        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            candidates = db_ops.get_raw_metadata_backfill_candidates(
                [dest_root], limit=limit,
            )
            stats = {
                "candidates": len(candidates),
                "missing_on_disk": 0,
                "updated": 0,
                "no_identity_found": 0,
            }

            present = []
            for file_id, dest_path in candidates:
                if Path(dest_path).exists():
                    present.append((file_id, Path(dest_path)))
                else:
                    stats["missing_on_disk"] += 1

            if dry_run:
                logging.info(
                    f"Dry-run: {len(present)} RAW file(s) would be scanned for "
                    f"camera identity ({stats['missing_on_disk']} candidate(s) "
                    f"missing on disk were skipped)."
                )
                return stats

            extractor = MetadataExtractor()

            def extract(item):
                file_id, path = item
                meta = extractor.get_image_metadata_details(
                    path, include_camera_identity=True,
                )
                return file_id, meta

            processed = 0
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                for file_id, meta in pool.map(extract, present):
                    if meta.camera_serial_number is None and meta.camera_file_number is None:
                        stats["no_identity_found"] += 1
                    elif db_ops.fill_camera_identity_if_null(
                        file_id,
                        meta.camera_serial_number,
                        meta.camera_file_number,
                    ):
                        stats["updated"] += 1
                    processed += 1
                    if processed % config.BACKFILL_COMMIT_BATCH_SIZE == 0:
                        conn.commit()
                        logging.info(f"Backfill progress: {processed}/{len(present)}")
            conn.commit()

            logging.info(
                f"Backfill complete: {stats['updated']} row(s) updated, "
                f"{stats['no_identity_found']} file(s) had no identity tags, "
                f"{stats['missing_on_disk']} candidate(s) missing on disk."
            )
            return stats

    def validate_dest(self, dest_root: Path,
                      report_csv: Optional[Path] = None,
                      run_id: Optional[int] = None) -> Dict[str, int]:
        """
        Scan dest_root against the catalog and write a validation CSV.

        Reports CONFIRMED (at expected location), MISSING (in catalog but absent),
        and UNTRACKED (on disk but not in catalog) files.
        """
        resolved_csv = report_csv or (dest_root / "dest_validation.csv")
        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            reporter = ReportGenerator(db_ops)
            stats = reporter.generate_dest_validation_report(dest_root, resolved_csv, run_id=run_id)
            if run_id is not None:
                conn.commit()
            return stats
