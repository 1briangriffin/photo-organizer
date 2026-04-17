import argparse
import logging
import sqlite3
import sys
from pathlib import Path

from .core import PhotoOrganizerApp
from .database.ops import DBOperations
from .database.schema import init_schema
from .reporting import ReportGenerator

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
                        "Default depends on mode: dest/dry_run_preview.csv for --dry-run, "
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

    # Resolve report CSV path
    if args.report_csv:
        report_csv_path = args.report_csv
    elif args.dry_run:
        report_csv_path = dest_root / "dry_run_preview.csv"
    else:
        report_csv_path = dest_root / "organization_report.csv"

    from . import config
    workers = args.workers if args.workers is not None else config.DEFAULT_WORKERS_HDD

    # --- Mode: --report ---
    if args.report:
        if not db_path.exists():
            logging.error(f"Database not found at {db_path}. Cannot generate report without an existing catalog.")
            sys.exit(1)
        logging.info("ENTERING REPORT MODE")
        try:
            conn = sqlite3.connect(db_path)
            db_ops = DBOperations(conn)
            reporter = ReportGenerator(db_ops)
            reporter.generate_source_report(str(src_root), str(report_csv_path), skip_dirs=skip_dirs)
            logging.info(f"Report generation complete: {report_csv_path}")
            conn.close()
            sys.exit(0)
        except Exception:
            logging.exception("Failed to generate report.")
            sys.exit(1)

    # --- Mode: --sync-dest ---
    if args.sync_dest:
        if not db_path.exists():
            logging.error(f"Database not found at {db_path}.")
            sys.exit(1)
        app = PhotoOrganizerApp(db_path)
        try:
            app.sync_dest(
                dest_root=dest_root,
                dry_run=args.dry_run,
                import_new=args.import_new,
                max_workers=workers,
            )
        except KeyboardInterrupt:
            logging.warning("Operation cancelled by user.")
            sys.exit(1)
        except Exception:
            logging.exception("Fatal error during sync.")
            sys.exit(1)
        sys.exit(0)

    # --- Mode: --validate-dest ---
    if args.validate_dest:
        if not db_path.exists():
            logging.error(f"Database not found at {db_path}.")
            sys.exit(1)
        app = PhotoOrganizerApp(db_path)
        try:
            # Pass args.report_csv through (possibly None) so validate_dest's
            # default (dest/dest_validation.csv) applies when the user did not
            # specify --report-csv. The organize-centric default computed above
            # does not fit this mode.
            app.validate_dest(
                dest_root=dest_root,
                report_csv=args.report_csv,
            )
        except KeyboardInterrupt:
            logging.warning("Operation cancelled by user.")
            sys.exit(1)
        except Exception:
            logging.exception("Fatal error during validation.")
            sys.exit(1)
        sys.exit(0)

    # --- Mode: --ingest-dest ---
    if args.ingest_dest:
        if not db_path.exists():
            logging.error(f"Database not found at {db_path}.")
            sys.exit(1)
        app = PhotoOrganizerApp(db_path)
        try:
            app.ingest_dest(
                dest_root=dest_root,
                move=args.move,
                dry_run=args.dry_run,
                max_workers=workers,
            )
        except KeyboardInterrupt:
            logging.warning("Operation cancelled by user.")
            sys.exit(1)
        except Exception:
            logging.exception("Fatal error during ingest.")
            sys.exit(1)
        sys.exit(0)

    # --- Default mode: organize pipeline ---
    assert src_root is not None  # guaranteed by parse_args validation
    logging.info(f"Using {workers} parallel workers for file processing")
    app = PhotoOrganizerApp(db_path)
    try:
        app.organize(
            src_root=src_root,
            dest_root=dest_root,
            is_seed=args.seed,
            move=args.move,
            dry_run=args.dry_run,
            dry_run_csv=report_csv_path if args.dry_run else None,
            skip_dirs=skip_dirs,
            max_workers=workers,
            auto_sync=args.auto_sync,
        )
    except KeyboardInterrupt:
        logging.warning("Operation cancelled by user.")
        sys.exit(1)
    except Exception:
        logging.exception("Fatal error during organization.")
        sys.exit(1)

if __name__ == "__main__":
    main()
