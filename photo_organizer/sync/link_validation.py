"""
Link validation module for checking RAW↔JPEG naming consistency.

This module validates that linked RAW and JPEG files have consistent naming
and provides suggestions for renaming when discrepancies are detected.
"""

import csv
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..database.ops import DBOperations


@dataclass
class LinkMismatch:
    """Represents a naming mismatch between linked RAW and output files."""
    raw_id: int
    raw_path: str
    raw_stem: str
    output_id: int
    output_path: str
    output_stem: str
    confidence: int
    link_method: str
    suggestion: str  # Human-readable suggestion


@dataclass
class LinkValidationReport:
    """Statistics and details from link validation."""
    total_links: int = 0
    consistent_count: int = 0
    mismatch_count: int = 0

    mismatches: List[LinkMismatch] = field(default_factory=list)


# Suffixes that are acceptable on JPEG outputs (don't indicate a mismatch)
VALID_OUTPUT_SUFFIXES = [
    '_edit', '_edited', '_final', '_export', '_resized',
    '_web', '_print', '_hdr', '_pano', '_panorama',
    '_crop', '_cropped', '_bw', '_mono',
]


class LinkConsistencyChecker:
    """
    Validates RAW↔JPEG naming consistency after renames.

    Checks all existing links in raw_outputs table and identifies
    cases where the RAW and JPEG stems no longer match.
    """

    def __init__(self, db_ops: DBOperations):
        self.db = db_ops

    def check_all_links(self) -> LinkValidationReport:
        """
        Examines all RAW→JPEG links and identifies naming mismatches.

        A mismatch is when the normalized stems of the RAW and JPEG
        differ, suggesting one was renamed without the other.

        Returns:
            LinkValidationReport with statistics and mismatch details
        """
        report = LinkValidationReport()

        # Get all links with file paths
        cur = self.db.conn.cursor()
        cur.execute("""
            SELECT
                ro.raw_file_id,
                rf.dest_path as raw_dest,
                rf.orig_name as raw_orig_name,
                ro.output_file_id,
                of.dest_path as output_dest,
                of.orig_name as output_orig_name,
                ro.confidence,
                ro.link_method
            FROM raw_outputs ro
            JOIN files rf ON ro.raw_file_id = rf.id
            JOIN files of ON ro.output_file_id = of.id
        """)

        links = cur.fetchall()
        report.total_links = len(links)

        for row in links:
            raw_id, raw_dest, raw_orig, out_id, out_dest, out_orig, confidence, method = row

            # Get the actual filename from dest_path (or fall back to orig_name)
            raw_filename = Path(raw_dest).name if raw_dest else raw_orig
            out_filename = Path(out_dest).name if out_dest else out_orig

            raw_stem = Path(raw_filename).stem
            out_stem = Path(out_filename).stem

            # Check if stems match
            if self._stems_match(raw_stem, out_stem):
                report.consistent_count += 1
            else:
                report.mismatch_count += 1

                # Generate suggestion
                suggestion = self._generate_suggestion(
                    raw_stem, out_stem, raw_dest or "", out_dest or ""
                )

                mismatch = LinkMismatch(
                    raw_id=raw_id,
                    raw_path=raw_dest or raw_orig,
                    raw_stem=raw_stem,
                    output_id=out_id,
                    output_path=out_dest or out_orig,
                    output_stem=out_stem,
                    confidence=confidence or 0,
                    link_method=method or "",
                    suggestion=suggestion,
                )
                report.mismatches.append(mismatch)

        return report

    def _normalize_stem(self, stem: str) -> str:
        """
        Normalize a filename stem for comparison.

        Removes common suffixes, normalizes case, and strips trailing numbers.
        """
        s = stem.lower().strip()

        # Remove copy indicators
        s = re.sub(r'\(copy\)$', '', s)
        s = re.sub(r'_copy$', '', s)
        s = re.sub(r'\(\d+\)$', '', s)

        # Remove valid output suffixes for comparison
        for suffix in VALID_OUTPUT_SUFFIXES:
            if s.endswith(suffix):
                s = s[:-len(suffix)]

        # Remove resolution suffixes like _1920x1080
        s = re.sub(r'_\d+x\d+$', '', s)

        # Remove timestamp suffixes that organizer adds (YYYY-MM-DD_HH-MM-SS)
        s = re.sub(r'_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$', '', s)

        return s.strip('_- ')

    def _stems_match(self, raw_stem: str, output_stem: str) -> bool:
        """
        Determines if RAW and JPEG stems are consistent.

        Matching patterns:
        - Exact match after normalization
        - Output has valid suffix (edit, final, etc.)
        - Output has resolution suffix

        Returns:
            True if stems are consistent, False if they mismatch
        """
        norm_raw = self._normalize_stem(raw_stem)
        norm_out = self._normalize_stem(output_stem)

        # Exact match after normalization
        if norm_raw == norm_out:
            return True

        # Check if output stem starts with raw stem (allows suffix)
        if norm_out.startswith(norm_raw):
            return True

        # Check if raw stem starts with output stem (less common)
        if norm_raw.startswith(norm_out):
            return True

        return False

    def _generate_suggestion(
        self,
        raw_stem: str,
        output_stem: str,
        raw_path: str,
        output_path: str,
    ) -> str:
        """
        Generate a human-readable suggestion for resolving the mismatch.
        """
        # Determine which was likely renamed (has more descriptive name)
        raw_is_descriptive = not self._is_camera_name(raw_stem)
        out_is_descriptive = not self._is_camera_name(output_stem)

        if raw_is_descriptive and not out_is_descriptive:
            # RAW was renamed, suggest renaming output to match
            out_ext = Path(output_path).suffix if output_path else ".jpg"
            suggested_name = f"{raw_stem}{out_ext}"
            return f"Rename JPEG to match RAW: {suggested_name}"

        elif out_is_descriptive and not raw_is_descriptive:
            # Output was renamed, suggest renaming RAW to match
            raw_ext = Path(raw_path).suffix if raw_path else ".cr2"
            suggested_name = f"{output_stem}{raw_ext}"
            return f"Rename RAW to match JPEG: {suggested_name}"

        else:
            # Both renamed or both camera names - user decides
            return f"Names differ: RAW='{raw_stem}' vs JPEG='{output_stem}'"

    def _is_camera_name(self, stem: str) -> bool:
        """
        Check if a filename stem appears to be a camera-generated name.
        """
        patterns = [
            r'^img_\d+$',
            r'^dsc_?\d+$',
            r'^dscf?\d+$',
            r'^pxl_\d+',
            r'^sam_\d+$',
            r'^cimg\d+$',
            r'^p\d{7}$',  # Olympus
            r'^_?[a-z]{3}\d{4,}$',  # Generic pattern
        ]

        stem_lower = stem.lower()
        for pattern in patterns:
            if re.match(pattern, stem_lower):
                return True
        return False

    def get_linked_files_for_raw(self, raw_id: int) -> List[Tuple[int, str, str]]:
        """
        Get all output files linked to a specific RAW.

        Args:
            raw_id: The RAW file ID

        Returns:
            List of (output_id, dest_path, orig_name)
        """
        cur = self.db.conn.cursor()
        cur.execute("""
            SELECT of.id, of.dest_path, of.orig_name
            FROM raw_outputs ro
            JOIN files of ON ro.output_file_id = of.id
            WHERE ro.raw_file_id = ?
        """, (raw_id,))
        return cur.fetchall()

    def get_linked_raw_for_output(self, output_id: int) -> Optional[Tuple[int, str, str]]:
        """
        Get the RAW file linked to a specific output.

        Args:
            output_id: The output file ID

        Returns:
            (raw_id, dest_path, orig_name) or None
        """
        cur = self.db.conn.cursor()
        cur.execute("""
            SELECT rf.id, rf.dest_path, rf.orig_name
            FROM raw_outputs ro
            JOIN files rf ON ro.raw_file_id = rf.id
            WHERE ro.output_file_id = ?
        """, (output_id,))
        return cur.fetchone()

    def write_csv_report(self, csv_path: str, report: LinkValidationReport):
        """Write link validation results to CSV file."""
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Summary section
            writer.writerow(['=== LINK VALIDATION SUMMARY ==='])
            writer.writerow(['Metric', 'Count'])
            writer.writerow(['Total Links', report.total_links])
            writer.writerow(['Consistent', report.consistent_count])
            writer.writerow(['Mismatched', report.mismatch_count])
            writer.writerow([])

            # Mismatches
            if report.mismatches:
                writer.writerow(['=== NAMING MISMATCHES ==='])
                writer.writerow([
                    'RAW ID', 'RAW Path', 'RAW Stem',
                    'Output ID', 'Output Path', 'Output Stem',
                    'Confidence', 'Link Method', 'Suggestion'
                ])
                for m in report.mismatches:
                    writer.writerow([
                        m.raw_id, m.raw_path, m.raw_stem,
                        m.output_id, m.output_path, m.output_stem,
                        m.confidence, m.link_method, m.suggestion
                    ])

        logging.info(f"Wrote link validation report to: {csv_path}")
