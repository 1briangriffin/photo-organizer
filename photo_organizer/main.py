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
    p = argparse.ArgumentParser(description="Photo Organizer: Phase 1 (Organize & Import)")
    
    p.add_argument("src", type=Path, help="Source directory to scan")
    p.add_argument("dest", type=Path, help="Destination library root")
    
    p.add_argument("--seed", action="store_true", help="Treat source as 'Seed' (canonical) files")
    p.add_argument("--move", action="store_true", help="Move files instead of Copying")
    p.add_argument("--dry-run", action="store_true", help="Simulate actions without modifying disk")
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    
    p.add_argument("--db", type=Path, default=None, help="Custom path for SQLite DB (default: dest/photo_catalog.db)")
    p.add_argument("--skip-dirs-file", type=Path, default=None, help="File containing paths to ignore")
    # Add arguments for report
    p.add_argument("--report", action="store_true", help="Generate a copy/status report for the source directory.")
    p.add_argument("--report-csv", type=str, default="organization_report.csv", help="Output path for the report CSV.")

    return p.parse_args()

def load_skip_dirs(skip_file: Path) -> set[Path]:
    if not skip_file or not skip_file.exists():
        return set()
    
    skips = set()
    with skip_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                skips.add(Path(line))
    return skips

def main():
    args = parse_args()
    
    # 1. Setup
    dest_root = args.dest.resolve()
    src_root = args.src.resolve()
    
    setup_logging(dest_root, args.verbose)
    
    logging.info("=== Photo Organizer Started ===")
    logging.info(f"Source: {src_root}")
    logging.info(f"Dest:   {dest_root}")
    
    # 2. Config
    db_path = args.db if args.db else dest_root / "photo_catalog.db"
    skip_dirs = load_skip_dirs(args.skip_dirs_file) if args.skip_dirs_file else set()

    # Check for report mode
    if args.report:
        if not db_path.exists():
            logging.error(f"Database not found at {db_path}. Cannot generate report without an existing catalog.")
            sys.exit(1)
        
        logging.info("ENTERING REPORT MODE")
        try:
            # Manually connect for reporting to avoid initializing the full App
            conn = sqlite3.connect(db_path)
            db_ops = DBOperations(conn)
            
            reporter = ReportGenerator(db_ops)
            reporter.generate_source_report(str(src_root), args.report_csv)
            
            logging.info(f"Report generation complete: {args.report_csv}")
            conn.close()
            sys.exit(0)
        except Exception as e:
            logging.exception("Failed to generate report.")
            sys.exit(1)

    # 3. Execution
    app = PhotoOrganizerApp(db_path)
    
    try:
        app.organize(
            src_root=src_root,
            dest_root=dest_root,
            is_seed=args.seed,
            move=args.move,
            dry_run=args.dry_run,
            skip_dirs=skip_dirs
        )
    except KeyboardInterrupt:
        logging.warning("Operation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        logging.exception("Fatal error during organization.")
        sys.exit(1)

if __name__ == "__main__":
    main()