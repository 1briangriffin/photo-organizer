import csv
import sys
from pathlib import Path

import analyze_metadata_issues as ami


def test_parse_log_and_load_unknowns(tmp_path):
    log_path = tmp_path / "organizer.log"
    log_path.write_text(
        "\n".join(
            [
                "EXIF read failed for /images/a.dng: error",
                "No EXIF tags found for /images/b.jpg",
                "Using filesystem mtime as capture_datetime for /images/c.dng",
            ]
        )
    )

    issues = ami.parse_log(log_path)
    assert "/images/a.dng" in issues
    assert "exif_read_failed" in issues["/images/a.dng"]
    assert "no_exif_tags" in issues["/images/b.jpg"]
    assert "using_filesystem_time_image" in issues["/images/c.dng"]

    unknown_csv = tmp_path / "unknown_files.csv"
    with unknown_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["orig_path", "size_bytes"])
        writer.writerow(["/images/a.dng", 10])
        writer.writerow(["/other/else.bin", 5])

    unknowns = ami.load_unknown_paths(unknown_csv)
    assert "/images/a.dng" in unknowns
    assert "/other/else.bin" in unknowns


def test_main_writes_summary(tmp_path, capsys, monkeypatch):
    log_path = tmp_path / "organizer.log"
    log_path.write_text("pHash computation failed for /images/z.jpg: boom")
    unknown_csv = tmp_path / "unknown_files.csv"
    unknown_csv.write_text("orig_path\n/images/z.jpg\n")
    out_path = tmp_path / "out.csv"

    argv = [
        "ami",
        "--log",
        str(log_path),
        "--unknown-csv",
        str(unknown_csv),
        "--out",
        str(out_path),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    ami.main()

    assert out_path.exists()
    content = out_path.read_text()
    assert "/images/z.jpg" in content
    assert "phash_failed" in content
    assert "1" in content  # in_unknown_files flag
