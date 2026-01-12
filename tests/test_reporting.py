import pytest
import sqlite3
import csv
from pathlib import Path
from datetime import datetime
from photo_organizer.reporting import ReportGenerator
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


def test_report_respects_skip_dirs(tmp_path):
    """Verify that report generation respects skip_dirs and doesn't include files from skipped directories."""
    # Setup directory structure
    root = tmp_path / "source"
    root.mkdir()

    skip_dir = root / "skip_me"
    skip_dir.mkdir()

    normal_dir = root / "normal"
    normal_dir.mkdir()

    # Create files
    (root / "root.jpg").write_bytes(b"root image")
    (skip_dir / "skipped.jpg").write_bytes(b"skipped image")
    (normal_dir / "normal.jpg").write_bytes(b"normal image")

    # Setup database
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)

    # Add the normal file to the database (simulating organization run that skipped skip_dir)
    normal_record = FileRecord(
        hash="abc123",
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name="normal.jpg",
        orig_path=normal_dir / "normal.jpg",
        size_bytes=12,
        mtime=1234567890,
        is_seed=True,
        name_score=50,
        capture_datetime=None,
        camera_model=None,
        lens_model=None,
        duration_sec=None
    )
    db_ops.upsert_file_record(normal_record)

    # Generate report with skip_dirs
    output_csv = tmp_path / "report.csv"
    reporter = ReportGenerator(db_ops)
    reporter.generate_source_report(str(root), str(output_csv), skip_dirs={skip_dir})

    # Read the report
    with open(output_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Verify results
    paths_in_report = [row["Source Path"] for row in rows]

    # Should include files NOT in skip_dir
    assert any("root.jpg" in p for p in paths_in_report), "root.jpg should be in report"
    assert any("normal.jpg" in p for p in paths_in_report), "normal.jpg should be in report"

    # Should NOT include files in skip_dir
    assert not any("skipped.jpg" in p for p in paths_in_report), "skipped.jpg should NOT be in report"

    # Verify the skipped directory itself was not traversed
    assert not any("skip_me" in p for p in paths_in_report), "skip_me directory should not appear in report"

    conn.close()


def test_report_without_skip_dirs_includes_all(tmp_path):
    """Verify that when no skip_dirs are provided, all files are included in the report."""
    # Setup directory structure
    root = tmp_path / "source"
    root.mkdir()

    dir1 = root / "dir1"
    dir1.mkdir()

    dir2 = root / "dir2"
    dir2.mkdir()

    # Create files
    (root / "root.jpg").write_bytes(b"root image")
    (dir1 / "file1.jpg").write_bytes(b"file1 image")
    (dir2 / "file2.jpg").write_bytes(b"file2 image")

    # Setup database
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    db_ops = DBOperations(conn)

    # Generate report without skip_dirs
    output_csv = tmp_path / "report.csv"
    reporter = ReportGenerator(db_ops)
    reporter.generate_source_report(str(root), str(output_csv))

    # Read the report
    with open(output_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Verify all files are included
    paths_in_report = [row["Source Path"] for row in rows]
    assert len(rows) == 3, "Should have 3 files in report"
    assert any("root.jpg" in p for p in paths_in_report)
    assert any("file1.jpg" in p for p in paths_in_report)
    assert any("file2.jpg" in p for p in paths_in_report)

    conn.close()
