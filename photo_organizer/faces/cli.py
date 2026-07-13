"""
CLI entry point for face recognition tools.

Provides the `photo-faces` command. Phase 1 ships the `scan` subcommand
(detection + embeddings); clustering, linking, and review commands arrive
with later phases of the facial-recognition port.

Every invocation records a command_runs row (tool='photo-faces') so face
runs appear in `photo-catalog-query --show-runs` alongside organizer runs.
"""
import argparse
import logging
import sqlite3
import sys
import warnings
from pathlib import Path

from ..database.schema import init_schema
from ..run_log import RunRecorder
from . import config

APP_VERSION = "photo-faces 0.1.0"


def setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # insightface 1.0.x calls a scikit-image API deprecated in skimage 0.26
    # (SimilarityTransform.estimate); the warning fires once per aligned face
    # and is not actionable here. Scoped to insightface so our own
    # deprecations stay visible.
    warnings.filterwarnings(
        "ignore", category=FutureWarning, module=r"insightface(\..*)?",
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Face recognition tools for photo-organizer"
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to photo_catalog.db")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Enable debug logging")

    sub = p.add_subparsers(dest="command", required=True)

    scan_p = sub.add_parser(
        "scan",
        help="Detect faces and record embeddings for unscanned catalog images",
    )
    scan_p.add_argument("--gpu", dest="use_gpu",
                        action="store_true", default=True,
                        help="Use GPU acceleration (default)")
    scan_p.add_argument("--cpu", dest="use_gpu", action="store_false",
                        help="Force CPU inference")
    scan_p.add_argument("--limit", type=int, default=None,
                        help="Max images to process this run")
    scan_p.add_argument("--include-raw", action="store_true",
                        help="Also scan RAW files that have no linked JPEG/TIFF "
                             "output (requires rawpy; much slower per file)")
    scan_p.add_argument("--thumbnail-dir", type=Path, default=None,
                        help="Dir for face thumbnails "
                             f"(default: <db_dir>/{config.THUMBNAIL_DIR_NAME})")

    return p.parse_args(argv)


def _run_scan(args, db_path: Path, run_id) -> dict:
    from .detection import FaceScanPipeline

    thumbnail_dir = args.thumbnail_dir or (db_path.parent / config.THUMBNAIL_DIR_NAME)
    pipeline = FaceScanPipeline(
        db_path,
        thumbnail_dir=thumbnail_dir,
        use_gpu=args.use_gpu,
        include_raw=args.include_raw,
    )
    return pipeline.run(run_id=run_id, limit=args.limit)


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)

    db_path = Path(args.db).resolve()
    # photo-faces never creates the catalog — it annotates an existing one.
    if not db_path.exists():
        logging.error(f"Database not found at {db_path}.")
        return 1

    # Bring the schema up to date so RunRecorder and the face tables exist.
    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
    finally:
        conn.close()

    params = {
        "command": args.command,
        "use_gpu": bool(getattr(args, "use_gpu", False)),
        "limit": getattr(args, "limit", None),
        "include_raw": bool(getattr(args, "include_raw", False)),
        "model_name": config.MODEL_NAME,
        "model_version": config.MODEL_VERSION_TAG,
    }
    recorder = RunRecorder(
        db_path,
        tool="photo-faces",
        command=args.command,
        argv=sys.argv if argv is None else ["photo-faces", *argv],
        params=params,
        db_path_for_row=db_path,
        app_version=APP_VERSION,
    )
    recorder.start()

    try:
        if args.command == "scan":
            stats = _run_scan(args, db_path, recorder.row_id)
        else:  # pragma: no cover - argparse enforces the choices
            raise SystemExit(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        recorder.finish_interrupted(note="cancelled by user")
        logging.warning("Operation cancelled by user.")
        return 1
    except Exception as e:
        recorder.finish_error(e)
        logging.exception(f"Fatal error during {args.command}.")
        return 1

    recorder.finish_success(
        stats=stats,
        db_mutates=stats.get("faces_detected", 0) > 0
                   or stats.get("images_no_faces", 0) > 0,
        files_mutate=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
