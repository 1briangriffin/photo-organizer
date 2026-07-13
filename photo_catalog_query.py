#!/usr/bin/env python

import argparse
import csv
import logging
import sqlite3
from pathlib import Path
from typing import List, Optional


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")
    return sqlite3.connect(db_path)


def list_unprocessed_raws(conn: sqlite3.Connection, csv_output: Optional[str] = None):
    """
    List RAW files with no linked outputs.

    Args:
        conn: Database connection
        csv_output: Optional CSV path to write unprocessed RAWs for review
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.dest_path, m.capture_datetime, m.camera_model
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type = 'raw'
        ORDER BY m.capture_datetime
    """)
    raws = cur.fetchall()

    cur.execute("SELECT DISTINCT raw_file_id FROM raw_outputs")
    linked_raw_ids = {row[0] for row in cur.fetchall()}

    # Collect unprocessed RAWs
    unprocessed = []
    for raw_id, dest_path, capture_str, cam in raws:
        if raw_id not in linked_raw_ids:
            unprocessed.append((raw_id, dest_path, capture_str, cam))

    # Write to CSV if requested
    if csv_output:
        with open(csv_output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['RAW ID', 'Capture DateTime', 'Camera Model', 'Destination Path'])
            for raw_id, dest_path, capture_str, cam in unprocessed:
                writer.writerow([raw_id, capture_str or '', cam or '', dest_path or ''])
        print(f"Wrote {len(unprocessed)} unprocessed RAWs to: {csv_output}")

    # Print to terminal
    print("Unprocessed RAW files (no linked outputs):")
    print("raw_id | capture_datetime        | camera_model              | dest_path")
    print("-------+--------------------------+---------------------------+----------")
    for raw_id, dest_path, capture_str, cam in unprocessed:
        print(f"{raw_id:6d} | {(capture_str or '').ljust(24)} | {(cam or '').ljust(25)} | {dest_path or ''}")


def _resolve_raw_id_from_path(conn: sqlite3.Connection, path: Path) -> Optional[int]:
    cur = conn.cursor()
    candidates = {str(path), path.as_posix()}
    # Try exact match on dest_path first, then on orig_path
    for cand in candidates:
        cur.execute("SELECT id FROM files WHERE dest_path = ? AND type = 'raw'", (cand,))
        row = cur.fetchone()
        if row:
            return row[0]
    for cand in candidates:
        cur.execute("SELECT id FROM files WHERE orig_path = ? AND type = 'raw'", (cand,))
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def show_raw_details(conn: sqlite3.Connection, raw_id: int):
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.orig_path, f.dest_path, m.capture_datetime, m.camera_model, m.lens_model
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.id = ? AND f.type = 'raw'
    """, (raw_id,))
    row = cur.fetchone()
    if not row:
        print(f"No RAW with id={raw_id}")
        return

    fid, orig_path, dest_path, capture_str, cam, lens = row
    print("RAW file:")
    print(f"  id:            {fid}")
    print(f"  orig_path:     {orig_path}")
    print(f"  dest_path:     {dest_path}")
    print(f"  capture_time:  {capture_str}")
    print(f"  camera_model:  {cam}")
    print(f"  lens_model:    {lens}")

    # Linked outputs
    cur.execute("""
        SELECT o.id, o.type, o.orig_path, o.dest_path, mm.capture_datetime, ro.link_method, ro.confidence
        FROM raw_outputs ro
        JOIN files o ON ro.output_file_id = o.id
        LEFT JOIN media_metadata mm ON o.id = mm.file_id
        WHERE ro.raw_file_id = ?
        ORDER BY mm.capture_datetime
    """, (raw_id,))
    outs = cur.fetchall()

    if not outs:
        print("  (No linked outputs)")
        return

    print("\n  Linked outputs:")
    print("  id   | type  | link_method   | conf | capture_datetime        | dest_path")
    print("  -----+-------+---------------+------+--------------------------+----------")
    for oid, otype, o_orig, o_dest, o_dt, method, conf in outs:
        print(f"  {oid:4d} | {otype.ljust(5)} | {(method or '').ljust(13)} | {str(conf or '').rjust(4)} | {(o_dt or '').ljust(24)} | {o_dest or o_orig}")


def list_unknown_files(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, ext, orig_path, size_bytes, is_seed, first_seen_at, last_seen_at
        FROM files
        WHERE type = 'other'
        ORDER BY ext, orig_path
    """)
    rows = cur.fetchall()
    if not rows:
        print("No files with type='other' found.")
        return

    print("Files with unhandled type='other':")
    print("id   | ext   | size_bytes | seed | first_seen           | last_seen            | orig_path")
    print("-----+-------+------------+------+----------------------+----------------------+----------")
    for fid, ext, orig_path, size_bytes, is_seed, first_seen, last_seen in rows:
        print(f"{fid:4d} | {ext.ljust(5)} | {str(size_bytes or 0).rjust(10)} | {int(is_seed)}    | {first_seen or ''} | {last_seen or ''} | {orig_path}")


def build_raw_output_links(conn: sqlite3.Connection, dry_run: bool = False, csv_output: Optional[str] = None):
    """
    Build RAW→JPEG links and report statistics.

    Args:
        conn: Database connection
        dry_run: If True, don't modify database
        csv_output: Optional CSV path to write proposed links for review
    """
    from photo_organizer.database.ops import DBOperations
    from photo_organizer.metadata.linking import FileLinker

    db_ops = DBOperations(conn)
    linker = FileLinker(db_ops)

    if dry_run:
        print("DRY RUN MODE - No database changes will be made")
    if csv_output:
        print(f"Writing proposed links to: {csv_output}")

    print("Building RAW→JPEG output links...")
    links_created = linker.link_raw_outputs(dry_run=dry_run, csv_output=csv_output)
    if not dry_run:
        conn.commit()

    if dry_run:
        print(f"\nProposed {links_created} links (not saved to database)")
        if csv_output:
            print(f"Review the proposed links in: {csv_output}")
            print("Run without --dry-run to apply the links to the database")
    else:
        print(f"Created {links_created} links")

        # Show summary statistics from database
        cur = conn.cursor()
        cur.execute("""
            SELECT link_method, confidence, COUNT(*)
            FROM raw_outputs
            GROUP BY link_method, confidence
            ORDER BY confidence DESC, link_method
        """)
        print("\nLinking Statistics:")
        print(f"{'Method':<25s} {'Confidence':>10s} {'Count':>8s}")
        print("-" * 45)
        for method, conf, count in cur.fetchall():
            print(f"{method:<25s} {conf:>10d} {count:>8d}")


def sync_paths(
    conn: sqlite3.Connection,
    dest_roots: List[Path],
    dry_run: bool = False,
    csv_output: Optional[str] = None,
    import_new: bool = False,
    build_links: bool = False,
):
    """
    Sync database paths with renamed files in destination directories.

    Scans destination directories and updates dest_path in the database
    when files have been renamed (same folder, different filename).

    Args:
        conn: Database connection
        dest_roots: List of destination root directories to scan
        dry_run: If True, don't modify database
        csv_output: Optional CSV path to write sync report
        import_new: If True, import new files found in destinations
        build_links: If True, run RAW→JPEG linking after import
    """
    from photo_organizer.database.ops import DBOperations
    from photo_organizer.sync.path_sync import DestinationSyncer

    # Set up logging for sync operations
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    db_ops = DBOperations(conn)
    syncer = DestinationSyncer(db_ops)

    if dry_run:
        print("DRY RUN MODE - No database changes will be made")

    print(f"Syncing paths in {len(dest_roots)} destination(s)...")
    for root in dest_roots:
        print(f"  - {root}")

    report = syncer.sync_destinations(dest_roots, apply_renames=not dry_run, csv_output=None)

    # Import new files if requested
    if import_new and report.new_files:
        print(f"\nImporting {len(report.new_files)} new files...")
        syncer.import_new_files(report.new_files, report, dry_run=dry_run)

    # Commit changes if not dry run
    if not dry_run and (report.renamed_count > 0 or report.imported_count > 0):
        conn.commit()

    # Write CSV report after all operations
    if csv_output:
        syncer._write_csv_report(csv_output, report)

    # Print summary
    print("\n=== SYNC SUMMARY ===")
    print(f"  Scanned:     {report.scanned_count} files")
    print(f"  Unchanged:   {report.unchanged_count} files")
    print(f"  Renamed:     {report.renamed_count} files {'(updated)' if not dry_run else '(not saved - dry run)'}")
    print(f"  Moved:       {report.moved_count} files (not auto-synced)")
    print(f"  Missing:     {report.missing_count} files")
    if import_new:
        print(f"  Imported:    {report.imported_count} files {'(saved)' if not dry_run else '(not saved - dry run)'}")
    else:
        print(f"  New:         {report.new_count} files (not in database, use --import-new to add)")
    print(f"  Errors:      {report.error_count}")

    if report.renamed_files:
        print("\nRenamed files:")
        for old, new in report.renamed_files[:10]:
            print(f"  {Path(old).name} → {Path(new).name}")
        if len(report.renamed_files) > 10:
            print(f"  ... and {len(report.renamed_files) - 10} more")

    if report.imported_files:
        print("\nImported files:")
        for path in report.imported_files[:10]:
            print(f"  {Path(path).name}")
        if len(report.imported_files) > 10:
            print(f"  ... and {len(report.imported_files) - 10} more")

    if report.moved_files:
        print("\nMoved files (not auto-synced - different folder):")
        for old, new in report.moved_files[:5]:
            print(f"  {old}")
            print(f"    → {new}")
        if len(report.moved_files) > 5:
            print(f"  ... and {len(report.moved_files) - 5} more")

    if csv_output:
        print(f"\nDetailed report written to: {csv_output}")

    # Run link building if requested and we imported files
    if build_links and report.imported_count > 0 and not dry_run:
        print("\n--- Building RAW→JPEG links ---")
        build_raw_output_links(conn, dry_run=False, csv_output=None)


def show_runs(conn: sqlite3.Connection, *, tool: Optional[str] = None,
              command: Optional[str] = None, status: Optional[str] = None,
              limit: int = 20, since: Optional[str] = None,
              csv_output: Optional[str] = None) -> None:
    """Print recent command_runs rows, most-recent first.

    Filters stack (all ANDed). `since` accepts an ISO-8601 prefix — SQLite's
    lexicographic comparison works correctly on ISO-8601 strings.
    """
    where: List[str] = []
    params: List = []
    if tool:
        where.append("tool = ?")
        params.append(tool)
    if command:
        where.append("command = ?")
        params.append(command)
    if status:
        where.append("exit_status = ?")
        params.append(status)
    if since:
        where.append("started_at >= ?")
        params.append(since)

    sql = (
        "SELECT id, tool, command, started_at, finished_at, exit_status, "
        "dry_run, db_mutates, files_mutate, src_root, dest_root, stats_json, "
        "error_type, error_message "
        "FROM command_runs"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()

    if not rows:
        print("No command_runs rows match the filters.")
        return

    if csv_output:
        with open(csv_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "id", "tool", "command", "started_at", "finished_at",
                "exit_status", "dry_run", "db_mutates", "files_mutate",
                "src_root", "dest_root", "stats_json",
                "error_type", "error_message",
            ])
            for row in rows:
                writer.writerow(row)
        print(f"Wrote {len(rows)} rows to: {csv_output}")

    print(f"Showing {len(rows)} run(s) (most recent first):\n")
    for (rid, rtool, rcmd, started, finished, xstatus, dry, db_m, files_m,
         src, dest, stats_json, err_type, err_msg) in rows:
        flags = []
        if dry:
            flags.append("dry-run")
        if db_m:
            flags.append("db-mutated")
        if files_m:
            flags.append("files-mutated")
        flag_str = f"[{', '.join(flags)}]" if flags else ""
        print(f"#{rid} {rtool} {rcmd} {flag_str}")
        print(f"  started:  {started}")
        print(f"  finished: {finished or '(not finalized)'}")
        print(f"  status:   {xstatus}")
        if src:
            print(f"  src:      {src}")
        if dest:
            print(f"  dest:     {dest}")
        if stats_json:
            print(f"  stats:    {stats_json}")
        if err_type:
            print(f"  error:    {err_type}: {err_msg or ''}")
        print()


def show_proposals(conn: sqlite3.Connection, *,
                   status: str = "proposed",
                   action_type: Optional[str] = None,
                   run_id: Optional[int] = None,
                   limit: int = 50,
                   csv_output: Optional[str] = None) -> None:
    """Print run_actions proposals (pending by default), most-recent first."""
    from photo_organizer.database.ops import DBOperations
    from photo_organizer.pipeline.lifecycle import list_proposals

    rows = list_proposals(
        DBOperations(conn),
        status=status,
        action_type=action_type,
        proposed_by_run_id=run_id,
        limit=limit,
    )
    if not rows:
        print(f"No run_actions rows with status '{status}' match the filters.")
        return

    if csv_output:
        with open(csv_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} rows to: {csv_output}")

    print(f"Showing {len(rows)} '{status}' action(s) (most recent first):\n")
    for row in rows:
        run_bits = []
        if row["tool"] or row["command"]:
            run_bits.append(f"{row['tool'] or '?'} {row['command'] or '?'}")
        if row["run_dry_run"]:
            run_bits.append("dry-run")
        if row["run_started_at"]:
            run_bits.append(f"started {row['run_started_at']}")
        print(f"#{row['id']} {row['action_type']} "
              f"({row['entity_type']} {row['entity_id'] if row['entity_id'] is not None else '?'})")
        print(f"  proposed by run #{row['proposed_by_run_id']}"
              f"{' - ' + ', '.join(run_bits) if run_bits else ''}")
        if row["source_path"]:
            print(f"  source:   {row['source_path']}")
        if row["target_path"]:
            print(f"  target:   {row['target_path']}")
        detail_bits = []
        if row["method"]:
            detail_bits.append(f"method={row['method']}")
        if row["confidence"] is not None:
            detail_bits.append(f"confidence={row['confidence']}")
        if detail_bits:
            print(f"  {'  '.join(detail_bits)}")
        if row["resolved_by_run_id"] is not None:
            note = f" ({row['resolution_note']})" if row["resolution_note"] else ""
            print(f"  resolved: by run #{row['resolved_by_run_id']} "
                  f"at {row['resolved_at']}{note}")
        print()


def reject_proposals_cmd(conn: sqlite3.Connection, db_path: Path,
                         action_ids: List[int],
                         note: Optional[str] = None) -> None:
    """Reject pending proposals by run_actions id, recording the decision as a
    command run so resolved_by_run_id points at an auditable row."""
    import sys

    from photo_organizer.database.ops import DBOperations
    from photo_organizer.pipeline.lifecycle import reject_proposals
    from photo_organizer.run_log import RunRecorder

    recorder = RunRecorder(
        db_path,
        tool="photo-catalog-query",
        command="reject-proposal",
        argv=sys.argv,
        params={"action_ids": action_ids, "note": note},
        db_path_for_row=db_path,
    )
    recorder.start()
    try:
        rejected, skipped = reject_proposals(
            DBOperations(conn), action_ids,
            run_id=recorder.row_id, note=note,
        )
        conn.commit()
    except Exception as exc:
        recorder.finish_error(exc)
        raise
    recorder.finish_success(
        stats={"rejected": len(rejected), "skipped": len(skipped)},
        db_mutates=bool(rejected),
    )

    if rejected:
        print(f"Rejected {len(rejected)} proposal(s): {', '.join(str(i) for i in rejected)}")
    if skipped:
        print(f"Skipped {len(skipped)} id(s) not in 'proposed' status: "
              f"{', '.join(str(i) for i in skipped)}")


def _resolve_file_targets(conn: sqlite3.Connection,
                          targets: List[str]) -> tuple[List[int], List[str]]:
    """Resolve --retire-file/--restore-file arguments to file ids.

    Each target may be a numeric file id or a dest_path (as printed by
    --validate-dest's MISSING section). Returns (file_ids, unresolved)."""
    from photo_organizer.database.ops import DBOperations

    ids: List[int] = []
    paths: List[str] = []
    for target in targets:
        if target.isdigit():
            ids.append(int(target))
        else:
            paths.append(target)
    resolved = DBOperations(conn).find_file_ids_by_dest_paths(paths) if paths else {}
    unresolved = [p for p in paths if p not in resolved]
    ids.extend(resolved[p] for p in paths if p in resolved)
    return ids, unresolved


def set_file_status_cmd(conn: sqlite3.Connection, db_path: Path,
                        targets: List[str], *, retire: bool,
                        note: Optional[str] = None) -> None:
    """Retire or restore catalog files, recording the decision as a command run."""
    import sys

    from photo_organizer.database.ops import DBOperations
    from photo_organizer.pipeline.lifecycle import restore_files, retire_files
    from photo_organizer.run_log import RunRecorder

    verb = "retire" if retire else "restore"
    file_ids, unresolved = _resolve_file_targets(conn, targets)
    for path in unresolved:
        print(f"No catalog file with dest_path: {path}")
    if not file_ids:
        print(f"Nothing to {verb}.")
        return

    recorder = RunRecorder(
        db_path,
        tool="photo-catalog-query",
        command=f"{verb}-file",
        argv=sys.argv,
        params={"targets": targets, "note": note},
        db_path_for_row=db_path,
    )
    recorder.start()
    try:
        op = retire_files if retire else restore_files
        changed, skipped = op(
            DBOperations(conn), file_ids,
            run_id=recorder.row_id, note=note,
        )
        conn.commit()
    except Exception as exc:
        recorder.finish_error(exc)
        raise
    recorder.finish_success(
        stats={f"{verb}d": len(changed), "skipped": len(skipped) + len(unresolved)},
        db_mutates=bool(changed),
    )

    if changed:
        print(f"{verb.capitalize()}d {len(changed)} file(s): "
              f"{', '.join(str(i) for i in changed)}")
    if skipped:
        print(f"Skipped {len(skipped)} id(s) (not found or already "
              f"{'retired' if retire else 'active'}): "
              f"{', '.join(str(i) for i in skipped)}")


def list_retired(conn: sqlite3.Connection,
                 csv_output: Optional[str] = None) -> None:
    """Print retired catalog files."""
    from photo_organizer.database.ops import DBOperations

    rows = DBOperations(conn).get_retired_files()
    if not rows:
        print("No retired files.")
        return

    if csv_output:
        with open(csv_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["file_id", "dest_path", "retired_at", "note"])
            writer.writerows(rows)
        print(f"Wrote {len(rows)} rows to: {csv_output}")

    print(f"{len(rows)} retired file(s):\n")
    for file_id, dest_path, retired_at, note in rows:
        print(f"#{file_id} {dest_path or '(no dest_path)'}")
        print(f"  retired: {retired_at or '?'}{' - ' + note if note else ''}")
        print()


def check_links(conn: sqlite3.Connection, csv_output: Optional[str] = None):
    """
    Check RAW→JPEG link consistency and report naming mismatches.

    Validates that linked RAW and JPEG files have matching names.
    Reports cases where one file was renamed without the other.

    Args:
        conn: Database connection
        csv_output: Optional CSV path to write validation report
    """
    from photo_organizer.database.ops import DBOperations
    from photo_organizer.sync.link_validation import LinkConsistencyChecker

    db_ops = DBOperations(conn)
    checker = LinkConsistencyChecker(db_ops)

    print("Checking RAW→JPEG link consistency...")
    report = checker.check_all_links()

    # Write CSV if requested
    if csv_output:
        checker.write_csv_report(csv_output, report)

    # Print summary
    print("\n=== LINK CONSISTENCY SUMMARY ===")
    print(f"  Total links:  {report.total_links}")
    print(f"  Consistent:   {report.consistent_count}")
    print(f"  Mismatched:   {report.mismatch_count}")

    if report.mismatches:
        print("\n=== NAMING MISMATCHES ===")
        print("(RAW and JPEG stems don't match - consider renaming one to match the other)\n")

        for m in report.mismatches[:20]:
            print(f"  RAW:  {Path(m.raw_path).name if m.raw_path else m.raw_stem}")
            print(f"  JPEG: {Path(m.output_path).name if m.output_path else m.output_stem}")
            print(f"  → {m.suggestion}")
            print()

        if len(report.mismatches) > 20:
            print(f"  ... and {len(report.mismatches) - 20} more mismatches")
            if csv_output:
                print(f"  See full list in: {csv_output}")

    elif report.total_links > 0:
        print("\n✓ All links are consistent!")

    if csv_output:
        print(f"\nDetailed report written to: {csv_output}")


def parse_args():
    p = argparse.ArgumentParser(description="Query helper for photo_catalog SQLite DB.")
    p.add_argument("--db", required=True, help="Path to photo_catalog.db (typically under your dest root)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--unprocessed-raws", action="store_true", help="List RAW files with no linked outputs")
    group.add_argument("--raw-id", type=int, help="Show details and outputs for a RAW by id")
    group.add_argument("--raw-path", help="Show details and outputs for a RAW by dest/orig path")
    group.add_argument("--unknown-files", action="store_true", help="List files of type='other'")
    group.add_argument("--build-raw-links", action="store_true", help="Build RAW→JPEG output links")
    group.add_argument("--sync-paths", action="store_true", help="Sync database with renamed files in destinations")
    group.add_argument("--check-links", action="store_true", help="Check RAW→JPEG link naming consistency")
    group.add_argument("--show-runs", action="store_true", help="Show recent command_runs history")
    group.add_argument("--show-proposals", action="store_true",
                       help="Show pending run_actions proposals (from dry-runs)")
    group.add_argument("--reject-proposal", nargs="+", type=int, metavar="ACTION_ID",
                       help="Reject pending proposals by run_actions id")
    group.add_argument("--retire-file", nargs="+", metavar="ID_OR_PATH",
                       help="Mark files as intentionally deleted (by file id or dest path); "
                            "they stop being reported as MISSING")
    group.add_argument("--restore-file", nargs="+", metavar="ID_OR_PATH",
                       help="Reactivate previously retired files (by file id or dest path)")
    group.add_argument("--list-retired", action="store_true",
                       help="List files marked as intentionally deleted")

    # Options for various commands
    p.add_argument("--dry-run", action="store_true", help="Dry run: don't modify database")
    p.add_argument("--csv-output", type=str, help="CSV file to write results")
    p.add_argument("--dest-roots", nargs="+", type=Path, help="Destination roots to sync (use with --sync-paths)")
    p.add_argument("--import-new", action="store_true", help="Import new files found in destinations (use with --sync-paths)")
    p.add_argument("--build-links", action="store_true", help="Run RAW→JPEG linking after import (use with --sync-paths --import-new)")

    # --- --show-runs filters ---
    p.add_argument("--tool", help="With --show-runs: filter by tool (e.g. photo-organizer, photo-faces)")
    p.add_argument("--command", help="With --show-runs: filter by command (e.g. organize, sync-dest)")
    p.add_argument("--status", choices=["success", "error", "interrupted"],
                   help="With --show-runs: filter by exit status")
    p.add_argument("--limit", type=int, default=20, help="With --show-runs: max rows to return (default 20)")
    p.add_argument("--since", help="With --show-runs: ISO-8601 lower bound on started_at (e.g. 2026-04-01)")
    p.add_argument("--action-type", help="With --show-proposals: filter by action_type (e.g. move_file)")
    p.add_argument("--action-status",
                   choices=["proposed", "applied", "skipped", "failed", "superseded", "rejected"],
                   default="proposed",
                   help="With --show-proposals: which status to list (default: proposed)")
    p.add_argument("--run-id", type=int,
                   help="With --show-proposals: only actions proposed by this run")
    p.add_argument("--note", help="With --reject-proposal / --retire-file / --restore-file: "
                                  "reason recorded on the affected rows")

    return p.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db).resolve()
    conn = connect_db(db_path)

    try:
        if args.unprocessed_raws:
            list_unprocessed_raws(conn, csv_output=args.csv_output)
        elif args.raw_id is not None:
            show_raw_details(conn, args.raw_id)
        elif args.raw_path:
            raw_path = Path(args.raw_path).resolve()
            raw_id = _resolve_raw_id_from_path(conn, raw_path)
            if raw_id is None:
                print(f"No RAW found for path: {raw_path}")
            else:
                show_raw_details(conn, raw_id)
        elif args.unknown_files:
            list_unknown_files(conn)
        elif args.build_raw_links:
            build_raw_output_links(
                conn,
                dry_run=args.dry_run,
                csv_output=args.csv_output
            )
        elif args.sync_paths:
            if not args.dest_roots:
                raise SystemExit("--sync-paths requires --dest-roots to specify directories to scan")
            sync_paths(
                conn,
                dest_roots=[p.resolve() for p in args.dest_roots],
                dry_run=args.dry_run,
                csv_output=args.csv_output,
                import_new=args.import_new,
                build_links=args.build_links,
            )
        elif args.check_links:
            check_links(conn, csv_output=args.csv_output)
        elif args.show_runs:
            show_runs(
                conn,
                tool=args.tool,
                command=args.command,
                status=args.status,
                limit=args.limit,
                since=args.since,
                csv_output=args.csv_output,
            )
        elif args.show_proposals:
            show_proposals(
                conn,
                status=args.action_status,
                action_type=args.action_type,
                run_id=args.run_id,
                limit=args.limit,
                csv_output=args.csv_output,
            )
        elif args.reject_proposal:
            reject_proposals_cmd(
                conn,
                db_path,
                action_ids=args.reject_proposal,
                note=args.note,
            )
        elif args.retire_file:
            set_file_status_cmd(conn, db_path, args.retire_file,
                                retire=True, note=args.note)
        elif args.restore_file:
            set_file_status_cmd(conn, db_path, args.restore_file,
                                retire=False, note=args.note)
        elif args.list_retired:
            list_retired(conn, csv_output=args.csv_output)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
