import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Tuple

from .core import PhotoOrganizerApp
from .database.db import DBManager
from .database.ops import DBOperations
from .database.schema import init_schema
from .reporting import ReportGenerator
from .run_log import RunRecorder

APP_VERSION = "photo-organizer 0.1.0"

# Command labels recorded in command_runs.command. Only ORGANIZE_COMMAND is
# allowed to create the catalog DB; every other command requires a pre-existing
# catalog and is rejected at main() if the DB file is absent.
ORGANIZE_COMMAND = "organize"
REPORT_COMMAND = "report"
SYNC_DEST_COMMAND = "sync-dest"
VALIDATE_DEST_COMMAND = "validate-dest"
INGEST_DEST_COMMAND = "ingest-dest"


def setup_logging(dest_root: Path, verbose: bool):
    """Sets up logging to both console and a file in the destination."""
    log_level = logging.DEBUG if verbose else logging.INFO

    # Create dest root if it doesn't exist so we can log there
    dest_root.mkdir(parents=True, exist_ok=True)
    log_file = dest_root / "organizer.log"

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Silence chatty libraries
    logging.getLogger("exifread").setLevel(logging.ERROR)
    logging.getLogger("PIL").setLevel(logging.WARNING)

def parse_args():
    p = argparse.ArgumentParser(description="Photo Organizer: Organize, sync, validate, and ingest photo libraries")

    # --- Positional arguments ---
    # src is optional for dest-only modes (--sync-dest, --validate-dest, --ingest-dest)
    p.add_argument("src", type=Path, nargs="?", default=None,
                   help="Source directory to scan (required unless --sync-dest, --validate-dest, or --ingest-dest)")
    p.add_argument("dest", type=Path, help="Destination library root")

    # --- Mutually exclusive mode flags ---
    modes = p.add_mutually_exclusive_group()
    modes.add_argument("--report", action="store_true",
                       help="Generate a copy/status report for the source directory (requires src)")
    modes.add_argument("--sync-dest", action="store_true",
                       help="Sync database with renamed files in dest without a source scan")
    modes.add_argument("--validate-dest", action="store_true",
                       help="Validate dest tree against the catalog and report confirmed/missing/untracked files")
    modes.add_argument("--ingest-dest", action="store_true",
                       help="Discover, link, and route new files that appeared in dest after a previous organize run")

    # --- Organize options ---
    p.add_argument("--seed", action="store_true", help="Treat source as 'Seed' (canonical) files")
    p.add_argument("--move", action="store_true", help="Move files instead of copying (organize and ingest-dest)")
    p.add_argument("--dry-run", action="store_true",
                   help="Simulate actions without modifying disk (creates dest dir and log file, "
                        "but no files are copied/moved and no dest_paths are persisted to the catalog)")
    p.add_argument("--auto-sync", action="store_true",
                   help="After organizing, sync database with any renamed files in destination")
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    # --- Shared options ---
    p.add_argument("--db", type=Path, default=None,
                   help="Custom path for SQLite DB (default: dest/photo_catalog.db)")
    p.add_argument("--skip-dirs-file", type=Path, default=None,
                   help="File containing paths to ignore")
    p.add_argument("--workers", type=int, default=None,
                   help="Number of parallel workers (default: 3 for HDD, 8 for SSD)")
    p.add_argument("--report-csv", type=Path, default=None,
                   help="Output path for the report/preview/validation CSV. "
                        "Default depends on mode: dest/ingest_dry_run_preview.csv for "
                        "--ingest-dest --dry-run, dest/dry_run_preview.csv for --dry-run, "
                        "dest/dest_validation.csv for --validate-dest, "
                        "dest/organization_report.csv otherwise.")

    # --- sync-dest options ---
    p.add_argument("--import-new", action="store_true",
                   help="With --sync-dest: import files found in dest that are not in the catalog")

    args = p.parse_args()

    # Validate: src is required for organize and --report modes
    dest_only_modes = args.sync_dest or args.validate_dest or args.ingest_dest
    if not dest_only_modes and args.src is None:
        p.error("src is required unless --sync-dest, --validate-dest, or --ingest-dest is specified")

    # Validate: --move and --seed only apply to organize/ingest-dest
    if args.move and args.sync_dest:
        p.error("--move cannot be used with --sync-dest")
    if args.move and args.validate_dest:
        p.error("--move cannot be used with --validate-dest")
    if args.seed and dest_only_modes:
        p.error("--seed only applies to the organize pipeline")
    if args.auto_sync and dest_only_modes:
        p.error("--auto-sync only applies to the organize pipeline")
    if args.import_new and not args.sync_dest:
        p.error("--import-new requires --sync-dest")

    return args

def load_skip_dirs(skip_file: Path) -> set[Path]:
    if not skip_file or not skip_file.exists():
        return set()

    skips = set()
    with skip_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                if "#" in line:
                    line = line.split("#", 1)[0].strip()
                if line:
                    skips.add(Path(line))
    return skips


def _determine_command(args) -> str:
    """Map parsed args to the command label recorded in command_runs.command."""
    if args.report:
        return REPORT_COMMAND
    if args.sync_dest:
        return SYNC_DEST_COMMAND
    if args.validate_dest:
        return VALIDATE_DEST_COMMAND
    if args.ingest_dest:
        return INGEST_DEST_COMMAND
    return ORGANIZE_COMMAND


def _ensure_schema(db_path: Path) -> None:
    """Open-and-close a short connection to guarantee the schema exists.

    Required so RunRecorder can INSERT into command_runs on the very first
    invocation (before the main pipeline connection is opened).
    """
    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
    finally:
        conn.close()


def _params_snapshot(args, command: str) -> Dict[str, Any]:
    """Subset of args persisted to params_json — structured, not free-form."""
    snap: Dict[str, Any] = {
        "command": command,
        "dry_run": bool(args.dry_run),
        "move": bool(args.move),
        "seed": bool(args.seed),
        "auto_sync": bool(args.auto_sync),
        "import_new": bool(args.import_new),
        "workers": args.workers,
        "verbose": bool(args.verbose),
    }
    if args.skip_dirs_file is not None:
        snap["skip_dirs_file"] = str(args.skip_dirs_file)
    if args.report_csv is not None:
        snap["report_csv"] = str(args.report_csv)
    return snap


def _compute_mutation_flags(command: str, args, stats: Dict[str, Any]) -> Tuple[bool, bool]:
    """Return (db_mutates, files_mutate) describing what the run actually did.

    Semantics are *actual*, derived from the stats the pipeline produced:
      - files_mutate: any successful move or copy recorded by FileMover.
      - db_mutates: anything committed to the catalog.

    Per-command details:
      organize     — UntrackedTreeDiscoverer commits one row per scanned file
                     unconditionally, so scanned>0 ⇒ Phase A mutated the DB
                     (even on dry-run). Phase B adds links/dest_paths on a real
                     successful run when moves/copies happen.
      ingest-dest  — Phase A commits renames only on a real run; net-new
                     imports are always committed (ingest_mode writes even on
                     dry-run). Phase B only runs when imports > 0.
      sync-dest    — commits only on a real run with rename/import counts > 0.
      validate-dest, report — read-only paths.
    """
    moved = stats.get("moved", 0) + stats.get("copied", 0)
    files_mutate = moved > 0

    if command == ORGANIZE_COMMAND:
        db_mutates = stats.get("scanned", 0) > 0 or files_mutate
        return db_mutates, files_mutate

    if command == INGEST_DEST_COMMAND:
        renames_persisted = (not args.dry_run) and stats.get("renamed", 0) > 0
        imports_persisted = stats.get("imported", 0) > 0
        db_mutates = renames_persisted or imports_persisted or files_mutate
        return db_mutates, files_mutate

    if command == SYNC_DEST_COMMAND:
        if args.dry_run:
            return False, False
        db_mutates = stats.get("renamed", 0) > 0 or stats.get("imported", 0) > 0
        return db_mutates, False

    # report and validate-dest
    return False, False


def _run_report(db_path: Path, src_root: Path, report_csv_path: Path,
                skip_dirs) -> Dict[str, Any]:
    # Pre-existence of db_path is enforced by main() before this is called.
    logging.info("ENTERING REPORT MODE")
    conn = sqlite3.connect(db_path)
    try:
        db_ops = DBOperations(conn)
        reporter = ReportGenerator(db_ops)
        reporter.generate_source_report(str(src_root), str(report_csv_path), skip_dirs=skip_dirs)
        logging.info(f"Report generation complete: {report_csv_path}")
    finally:
        conn.close()
    return {}


def _dispatch(args, db_path: Path, dest_root: Path, src_root, report_csv_path: Path,
              skip_dirs, workers: int, run_id: int | None = None) -> Dict[str, Any]:
    """Run the selected command and return its stats dict.

    DB pre-existence for non-organize commands is enforced by main() before
    this function is called.
    """
    if args.report:
        return _run_report(db_path, src_root, report_csv_path, skip_dirs)

    if args.sync_dest:
        app = PhotoOrganizerApp(db_path)
        return app.sync_dest(
            dest_root=dest_root,
            dry_run=args.dry_run,
            import_new=args.import_new,
            max_workers=workers,
            run_id=run_id,
        )

    if args.validate_dest:
        app = PhotoOrganizerApp(db_path)
        return app.validate_dest(
            dest_root=dest_root,
            report_csv=args.report_csv,
            run_id=run_id,
        )

    if args.ingest_dest:
        app = PhotoOrganizerApp(db_path)
        return app.ingest_dest(
            dest_root=dest_root,
            move=args.move,
            dry_run=args.dry_run,
            dry_run_csv=report_csv_path if args.dry_run else None,
            max_workers=workers,
            run_id=run_id,
        )

    # Default: organize
    assert src_root is not None  # guaranteed by parse_args validation
    logging.info(f"Using {workers} parallel workers for file processing")
    app = PhotoOrganizerApp(db_path)
    return app.organize(
        src_root=src_root,
        dest_root=dest_root,
        is_seed=args.seed,
        move=args.move,
        dry_run=args.dry_run,
        dry_run_csv=report_csv_path if args.dry_run else None,
        skip_dirs=skip_dirs,
        max_workers=workers,
        auto_sync=args.auto_sync,
        run_id=run_id,
    )


def main():
    args = parse_args()

    dest_root = args.dest.resolve()
    src_root = args.src.resolve() if args.src else None

    setup_logging(dest_root, args.verbose)

    logging.info("=== Photo Organizer Started ===")
    if src_root:
        logging.info(f"Source: {src_root}")
    logging.info(f"Dest:   {dest_root}")

    db_path = args.db if args.db else dest_root / "photo_catalog.db"
    skip_dirs = load_skip_dirs(args.skip_dirs_file) if args.skip_dirs_file else set()

    # Resolve report CSV path. Ingest-dry-run uses a distinct default filename
    # so an organize dry-run and an ingest dry-run in the same dest don't
    # clobber each other's preview.
    if args.report_csv:
        report_csv_path = args.report_csv
    elif args.dry_run and args.ingest_dest:
        report_csv_path = dest_root / "ingest_dry_run_preview.csv"
    elif args.dry_run:
        report_csv_path = dest_root / "dry_run_preview.csv"
    else:
        report_csv_path = dest_root / "organization_report.csv"

    from . import config
    workers = args.workers if args.workers is not None else config.DEFAULT_WORKERS_HDD

    command = _determine_command(args)

    # Only organize is permitted to create the catalog; every other mode must
    # fail *before* _ensure_schema runs — otherwise we'd create the DB,
    # silently bypass the pre-existence check, and record a spurious success.
    # These pre-existence failures cannot be logged to command_runs (the DB
    # doesn't exist yet) — documented limitation.
    if command != ORGANIZE_COMMAND and not db_path.exists():
        logging.error(f"Database not found at {db_path}.")
        sys.exit(1)

    _ensure_schema(db_path)
    recorder = RunRecorder(
        db_path,
        tool="photo-organizer",
        command=command,
        argv=sys.argv,
        params=_params_snapshot(args, command),
        dry_run=bool(args.dry_run),
        db_path_for_row=db_path,
        src_root=src_root,
        dest_root=dest_root,
        app_version=APP_VERSION,
    )
    recorder.start()

    try:
        stats = _dispatch(args, db_path, dest_root, src_root, report_csv_path,
                          skip_dirs, workers, run_id=recorder.row_id)
    except KeyboardInterrupt:
        recorder.finish_interrupted(note="cancelled by user")
        logging.warning("Operation cancelled by user.")
        sys.exit(1)
    except SystemExit as e:
        # _dispatch raises SystemExit for pre-condition failures (e.g. missing DB).
        # Record as an error before re-raising so the row reflects reality.
        recorder.finish_error(e)
        logging.error(str(e))
        raise
    except Exception as e:
        recorder.finish_error(e)
        logging.exception(f"Fatal error during {command}.")
        sys.exit(1)

    stats = stats or {}
    db_mutates, files_mutate = _compute_mutation_flags(command, args, stats)
    recorder.finish_success(
        stats=stats,
        db_mutates=db_mutates,
        files_mutate=files_mutate,
    )
    sys.exit(0)

if __name__ == "__main__":
    main()
