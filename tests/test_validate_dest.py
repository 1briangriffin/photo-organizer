"""
Tests for PhotoOrganizerApp.validate_dest() (--validate-dest).

Verifies that the three CSV sections (CONFIRMED, MISSING, UNTRACKED) are
correctly populated, that root scoping prevents false MISSING reports, and
that the default CSV path is used when none is specified.
"""
import csv
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from photo_organizer.core import PhotoOrganizerApp
from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.models import FileRecord


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.close()
    return db_path


def _seed_file(db_path: Path, content: bytes, dest_path: str):
    conn = sqlite3.connect(str(db_path))
    ops = DBOperations(conn)
    h = hashlib.sha256(content).hexdigest()
    rec = FileRecord(
        hash=h, sparse_hash=None, hash_is_sparse=False,
        type="jpeg", ext=".jpg",
        orig_name=Path(dest_path).name,
        orig_path=Path(dest_path),
        size_bytes=len(content), mtime=0.0,
        is_seed=False, name_score=1,
        capture_datetime=datetime(2023, 6, 1),
        camera_model=None, lens_model=None, duration_sec=None,
    )
    fid = ops.upsert_file_record(rec)
    ops.update_dest_path(fid, dest_path)
    conn.commit()
    conn.close()


def _read_csv_sections(csv_path: Path) -> dict:
    """Parse the validation CSV into {section_name: [paths]}."""
    sections = {}
    current = None
    with csv_path.open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row:
                continue
            cell = row[0].strip()
            if cell.startswith("===") and cell.endswith("==="):
                current = cell.strip("= ").strip()
                sections[current] = []
            elif current and cell not in ("Path", "Status", "Count") and row[0] not in ("Confirmed", "Missing", "Untracked", "SUMMARY"):
                sections[current].append(row[0])
    return sections


# ---------------------------------------------------------------------------

def test_confirmed_file_appears_in_confirmed_section(tmp_path):
    """A file present at its expected dest_path appears in CONFIRMED."""
    dest_dir = tmp_path / "dest" / "2023-06"
    dest_dir.mkdir(parents=True)
    img = dest_dir / "IMG_001.jpg"
    content = b"confirmed" * 100
    img.write_bytes(content)

    db_path = _make_db(tmp_path)
    _seed_file(db_path, content, str(img))

    csv_out = tmp_path / "report.csv"
    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=tmp_path / "dest", report_csv=csv_out)

    sections = _read_csv_sections(csv_out)
    assert str(img) in sections.get("CONFIRMED", [])
    assert sections.get("MISSING", []) == []
    assert sections.get("UNTRACKED", []) == []


def test_missing_file_appears_in_missing_section(tmp_path):
    """A file with a dest_path in the catalog but absent from disk appears in MISSING."""
    dest_dir = tmp_path / "dest" / "2023-06"
    dest_dir.mkdir(parents=True)
    missing_path = str(dest_dir / "IMG_002.jpg")

    db_path = _make_db(tmp_path)
    _seed_file(db_path, b"ghost" * 100, missing_path)

    csv_out = tmp_path / "report.csv"
    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=tmp_path / "dest", report_csv=csv_out)

    sections = _read_csv_sections(csv_out)
    assert missing_path in sections.get("MISSING", [])
    assert sections.get("CONFIRMED", []) == []


def test_untracked_file_appears_in_untracked_section(tmp_path):
    """A file on disk not in the catalog appears in UNTRACKED."""
    dest_dir = tmp_path / "dest" / "2023-06"
    dest_dir.mkdir(parents=True)
    untracked = dest_dir / "surprise.jpg"
    untracked.write_bytes(b"surprise" * 100)

    db_path = _make_db(tmp_path)

    csv_out = tmp_path / "report.csv"
    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=tmp_path / "dest", report_csv=csv_out)

    sections = _read_csv_sections(csv_out)
    assert str(untracked) in sections.get("UNTRACKED", [])
    assert sections.get("CONFIRMED", []) == []
    assert sections.get("MISSING", []) == []


def test_other_root_files_not_reported_as_missing(tmp_path):
    """Files cataloged under a different dest_root must not appear as MISSING."""
    root_a = tmp_path / "root_a"
    root_b = tmp_path / "root_b"
    root_a.mkdir()
    root_b.mkdir()

    img_a = root_a / "IMG_a.jpg"
    content_a = b"root_a photo" * 100
    img_a.write_bytes(content_a)

    db_path = _make_db(tmp_path)
    _seed_file(db_path, content_a, str(img_a))
    # root_b file in catalog but NOT on disk — must not pollute root_a report
    _seed_file(db_path, b"root_b photo" * 100, str(root_b / "IMG_b.jpg"))

    csv_out = tmp_path / "report.csv"
    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=root_a, report_csv=csv_out)

    sections = _read_csv_sections(csv_out)
    assert sections.get("MISSING", []) == [], "root_b file must not appear as missing in root_a report"
    assert str(img_a) in sections.get("CONFIRMED", [])


def test_default_csv_path(tmp_path):
    """When report_csv is None the CSV lands at dest/dest_validation.csv."""
    dest = tmp_path / "dest"
    dest.mkdir()
    db_path = _make_db(tmp_path)

    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=dest, report_csv=None)

    assert (dest / "dest_validation.csv").exists()
