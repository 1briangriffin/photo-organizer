#!/usr/bin/env python
"""
Analyze metadata-related issues from organizer.log and correlate with unknown_files.csv.

Looks for log messages like:
  - EXIF read failed for <path>: ...
  - No EXIF tags found for <path>
  - No EXIF date found for <path> ...
  - Using filesystem mtime as capture_datetime for <path>
  - MediaInfo.parse failed for <path>: ...
  - No usable video metadata found for <path> ...
  - pHash computation failed for <path>: ...
  - Failed to get image size for <path>: ...

Outputs a CSV with columns:
  path,in_unknown_files,issues

where issues is a semicolon-separated list of issue types.
"""

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path


# --------- LOG PARSING ---------

# Map issue_type -> (pattern, group_index)
# We structure patterns so group 1 is the path.
LOG_PATTERNS = {
    "exif_read_failed": re.compile(r"EXIF read failed for (.+?):"),
    "no_exif_tags": re.compile(r"No EXIF tags found for (.+)"),
    "no_exif_date": re.compile(r"No EXIF date found for (.+?) \(tags tried:"),
    "using_filesystem_time_image": re.compile(
        r"Using filesystem mtime as capture_datetime for (.+)"
    ),
    "mediainfo_failed": re.compile(r"MediaInfo\.parse failed for (.+?):"),
    "no_video_metadata": re.compile(
        r"No usable video metadata found for (.+?); will fall back to filesystem time\."
    ),
    "using_filesystem_time_video": re.compile(
        r"Using filesystem mtime as capture_datetime for video (.+)"
    ),
    "phash_failed": re.compile(r"pHash computation failed for (.+?):"),
    "image_size_failed": re.compile(r"Failed to get image size for (.+?):"),
}


def normalize_path_str(p: str) -> str:
    """
    Normalize a path string for consistent comparison.
    Keeps it as a string (no actual existence checks).
    """
    # Strip whitespace and quotes
    p = p.strip().strip('"').strip("'")
    # Use Path to normalize slashes etc., then cast back to string
    # (so Windows paths become consistent "C:\\...").
    try:
        return str(Path(p))
    except Exception:
        return p


def parse_log(log_path: Path) -> dict:
    """
    Parse organizer.log and return dict[path_str] -> set(issue_types).
    """
    issues_by_path = defaultdict(set)

    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            for issue_type, pattern in LOG_PATTERNS.items():
                m = pattern.search(line)
                if m:
                    raw_path = m.group(1)
                    path_str = normalize_path_str(raw_path)
                    issues_by_path[path_str].add(issue_type)
                    break  # Avoid double-counting if multiple patterns somehow match

    return issues_by_path


# --------- UNKNOWN FILES CSV PARSING ---------

def load_unknown_paths(unknown_csv: Path) -> set:
    """
    Load unknown_files.csv and return set of orig_path strings.
    Expects a header with 'orig_path' or 'path'.
    """
    if not unknown_csv.exists():
        return set()

    paths = set()
    with unknown_csv.open("r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)

        # Capture original fieldnames once
        orig_fieldnames = reader.fieldnames
        if not orig_fieldnames:
            # No header row – nothing we can do
            return set()

        # Lowercased version for case-insensitive matching
        fieldnames = [fn.lower() for fn in orig_fieldnames]

        path_col = None
        for candidate in ("orig_path", "path"):
            if candidate in fieldnames:
                # Find original case-sensitive name
                idx = fieldnames.index(candidate)
                path_col = orig_fieldnames[idx]
                break

        if path_col is None:
            # Fallback if needed
            if "orig_path" in orig_fieldnames:
                path_col = "orig_path"
            else:
                return set()

        for row in reader:
            raw_path = row.get(path_col)
            if raw_path:
                paths.add(normalize_path_str(raw_path))


    return paths


# --------- MAIN / CLI ---------

def main():
    ap = argparse.ArgumentParser(
        description="Analyze metadata-related issues from organizer.log and correlate with unknown_files.csv."
    )
    ap.add_argument(
        "--log",
        type=str,
        required=True,
        help="Path to organizer.log (e.g., D:\\Organized_Images\\organizer.log)",
    )
    ap.add_argument(
        "--unknown-csv",
        type=str,
        required=True,
        help="Path to unknown_files.csv (e.g., D:\\Organized_Images\\unknown_files.csv)",
    )
    ap.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output CSV path (default: same directory as log, 'metadata_issues_summary.csv')",
    )
    args = ap.parse_args()

    log_path = Path(args.log)
    unknown_csv = Path(args.unknown_csv)

    if not log_path.exists():
        raise SystemExit(f"Log file not found: {log_path}")
    if not unknown_csv.exists():
        print(f"Warning: unknown_files.csv not found at {unknown_csv}. Continuing with empty set.")

    issues_by_path = parse_log(log_path)
    unknown_paths = load_unknown_paths(unknown_csv)

    print(f"Found {len(issues_by_path)} paths with metadata-related issues in log.")
    print(f"Found {len(unknown_paths)} paths in unknown_files.csv.")

    # Decide output path
    if args.out:
        out_path = Path(args.out)
    else:
        out_path = log_path.with_name("metadata_issues_summary.csv")

    # Write summary CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["path", "in_unknown_files", "issues"])
        for path_str in sorted(issues_by_path.keys()):
            in_unknown = 1 if path_str in unknown_paths else 0
            issues = ";".join(sorted(issues_by_path[path_str]))
            writer.writerow([path_str, in_unknown, issues])

    print(f"Wrote summary for {len(issues_by_path)} paths to {out_path}")


if __name__ == "__main__":
    main()
