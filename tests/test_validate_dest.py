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


_HEADER_ROWS = {
    ("Path",),
    ("Status", "Count"),
    ("Old Path (catalog)", "New Path (on disk)"),
    ("Accepted Path", "Latest Observed Path", "Status"),
}
_SUMMARY_LABEL_PREFIXES = (
    "Confirmed",
    "Missing",
    "Untracked",
    "Renamed",
    "Moved",
)


def _read_csv_sections(csv_path: Path) -> dict:
    """Parse the validation CSV into {section_name: [rows]}.

    Each section's entries are the full rows (lists), so tests can inspect
    both single-path sections (CONFIRMED/MISSING/UNTRACKED) and the two-column
    RENAMED/MOVED sections.
    """
    sections: dict = {}
    current = None
    with csv_path.open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row:
                continue
            cell = row[0].strip()
            if cell.startswith("===") and cell.endswith("==="):
                current = cell.strip("= ").strip().split(" ")[0]  # CONFIRMED, RENAMED, ...
                sections[current] = []
                continue
            if current is None:
                continue
            # Skip header rows and SUMMARY label rows
            if tuple(row) in _HEADER_ROWS:
                continue
            if current == "SUMMARY" and row[0].startswith(_SUMMARY_LABEL_PREFIXES):
                continue
            sections[current].append(row)
    return sections


def _paths(sections: dict, key: str) -> list:
    """Return first-column entries (single-path sections)."""
    return [r[0] for r in sections.get(key, [])]


def _pairs(sections: dict, key: str) -> list:
    """Return (old, new) tuples from two-column sections (RENAMED/MOVED)."""
    return [(r[0], r[1]) for r in sections.get(key, []) if len(r) >= 2]


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
    assert str(img) in _paths(sections, "CONFIRMED")
    assert _paths(sections, "MISSING") == []
    assert _paths(sections, "UNTRACKED") == []
    assert _pairs(sections, "RENAMED") == []
    assert _pairs(sections, "MOVED") == []


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
    assert missing_path in _paths(sections, "MISSING")
    assert _paths(sections, "CONFIRMED") == []


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
    assert str(untracked) in _paths(sections, "UNTRACKED")
    assert _paths(sections, "CONFIRMED") == []
    assert _paths(sections, "MISSING") == []


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
    assert _paths(sections, "MISSING") == [], "root_b file must not appear as missing in root_a report"
    assert str(img_a) in _paths(sections, "CONFIRMED")


def test_default_csv_path(tmp_path):
    """When report_csv is None the CSV lands at dest/dest_validation.csv."""
    dest = tmp_path / "dest"
    dest.mkdir()
    db_path = _make_db(tmp_path)

    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=dest, report_csv=None)

    assert (dest / "dest_validation.csv").exists()


# ---------------------------------------------------------------------------
# Regression: renamed/moved files must appear in their own sections
# (previously they were dropped from the report entirely).
# ---------------------------------------------------------------------------

def test_renamed_file_appears_in_renamed_section(tmp_path):
    """A catalog-tracked file found under a different name in the same dir
    appears in RENAMED — not dropped from the report."""
    dest_dir = tmp_path / "dest" / "2023-06"
    dest_dir.mkdir(parents=True)

    content = b"renamed photo" * 100
    old_path = str(dest_dir / "IMG_0001.jpg")
    new_path = dest_dir / "Vacation_001.jpg"
    new_path.write_bytes(content)

    db_path = _make_db(tmp_path)
    _seed_file(db_path, content, old_path)

    csv_out = tmp_path / "report.csv"
    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=tmp_path / "dest", report_csv=csv_out)

    sections = _read_csv_sections(csv_out)
    pairs = _pairs(sections, "RENAMED")
    assert (old_path, str(new_path)) in pairs, \
        "renamed file must appear in RENAMED section"
    # And must not be silently counted as confirmed or missing
    assert _paths(sections, "CONFIRMED") == []
    assert _paths(sections, "MISSING") == []


def test_validation_report_includes_accepted_vs_observed_section(tmp_path):
    dest_dir = tmp_path / "dest" / "2023-06"
    dest_dir.mkdir(parents=True)

    content = b"renamed photo" * 100
    old_path = str(dest_dir / "IMG_0001.jpg")
    new_path = dest_dir / "Vacation_001.jpg"
    new_path.write_bytes(content)

    db_path = _make_db(tmp_path)
    _seed_file(db_path, content, old_path)

    csv_out = tmp_path / "report.csv"
    PhotoOrganizerApp(db_path).validate_dest(dest_root=tmp_path / "dest", report_csv=csv_out)

    sections = _read_csv_sections(csv_out)
    assert [old_path, str(new_path), "renamed"] in sections["ACCEPTED"]


def test_moved_file_appears_in_moved_section(tmp_path):
    """A catalog-tracked file found in a different directory appears in MOVED."""
    root = tmp_path / "dest"
    old_dir = root / "2023-06"
    new_dir = root / "2023-07"
    old_dir.mkdir(parents=True)
    new_dir.mkdir(parents=True)

    content = b"moved photo" * 100
    old_path = str(old_dir / "IMG_9000.jpg")
    new_path = new_dir / "IMG_9000.jpg"
    new_path.write_bytes(content)

    db_path = _make_db(tmp_path)
    _seed_file(db_path, content, old_path)

    csv_out = tmp_path / "report.csv"
    app = PhotoOrganizerApp(db_path)
    app.validate_dest(dest_root=root, report_csv=csv_out)

    sections = _read_csv_sections(csv_out)
    pairs = _pairs(sections, "MOVED")
    assert (old_path, str(new_path)) in pairs, \
        "moved file must appear in MOVED section"
    assert _paths(sections, "MISSING") == []


def test_validate_dest_cli_default_csv_path(tmp_path, monkeypatch):
    """End-to-end: `photo-organizer <dest> --validate-dest` (no --report-csv)
    writes to dest/dest_validation.csv, not dest/organization_report.csv."""
    import sys
    from photo_organizer import main as main_module

    dest = tmp_path / "dest"
    dest.mkdir()
    # validate_dest requires the DB file to exist; create an empty catalog.
    db = dest / "photo_catalog.db"
    _make_db(tmp_path)  # initialises schema into tmp/catalog.db; we want dest/photo_catalog.db
    import sqlite3
    from photo_organizer.database.schema import init_schema
    conn = sqlite3.connect(str(db))
    init_schema(conn)
    conn.close()

    monkeypatch.setattr(sys, "argv", ["photo-organizer", str(dest), "--validate-dest"])
    with pytest.raises(SystemExit) as exc:
        main_module.main()
    assert exc.value.code == 0

    assert (dest / "dest_validation.csv").exists(), \
        "CLI default must land at dest/dest_validation.csv"
    assert not (dest / "organization_report.csv").exists(), \
        "CLI must not fall back to the organize-mode default"
