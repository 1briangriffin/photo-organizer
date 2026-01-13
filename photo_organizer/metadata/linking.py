import csv
import logging
import re
from pathlib import Path
from typing import List, Optional, Tuple, cast, Any
from collections import defaultdict
from datetime import datetime, timedelta

# Optional import for PSD analysis
try:
    from psd_tools import PSDImage
except ImportError:
    PSDImage = None

from .. import config
from ..database.ops import DBOperations

class FileLinker:
    """
    Handles relationship discovery between files (RAW<->Sidecar, Source<->PSD).
    These links are critical for Organization Phase to keep files together.
    """
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops

    def link_raw_sidecars(self):
        """
        Links Sidecar files (.xmp) to RAW files in the same directory with same stem.
        """
        logging.info("Linking Sidecar files to RAWs...")
        cur = self.db.conn.cursor()
        
        # Fetch all RAWs and Sidecars
        # We fetch (id, parent_dir, stem)
        cur.execute("SELECT id, orig_path FROM files WHERE type = 'raw'")
        raws = []
        for rid, path_str in cur.fetchall():
            p = Path(path_str)
            raws.append((rid, p.parent, p.stem.lower()))

        cur.execute("SELECT id, orig_path FROM files WHERE type = 'sidecar'")
        sidecars = defaultdict(list)
        for sid, path_str in cur.fetchall():
            p = Path(path_str)
            # Index by (parent, stem) for O(1) lookup
            key = (p.parent, p.stem.lower())
            sidecars[key].append(sid)

        # Match
        links_made = 0
        for rid, parent, stem in raws:
            key = (parent, stem)
            if key in sidecars:
                for sid in sidecars[key]:
                    self.db.conn.execute(
                        "INSERT OR IGNORE INTO raw_sidecars (raw_file_id, sidecar_file_id) VALUES (?, ?)",
                        (rid, sid)
                    )
                    links_made += 1
        
        self.db.conn.commit()
        logging.info(f"Linked {links_made} sidecars.")

    def link_psds(self):
        """
        Links PSD files to their source images.
        Strategy 1: Exact Stem Match (High Confidence)
        Strategy 2: PSD Smart Object Parsing (Medium Confidence)
        """
        if not PSDImage:
            logging.warning("psd-tools not installed; skipping smart object analysis.")

        logging.info("Linking PSD files to sources...")
        cur = self.db.conn.cursor()

        # Get PSDs
        cur.execute("SELECT id, orig_name, orig_path FROM files WHERE type='psd'")
        psds = cur.fetchall()

        # Get Potential Sources (RAW, JPEG)
        cur.execute("SELECT id, orig_name FROM files WHERE type IN ('raw', 'jpeg')")
        # Build lookup: normalized_stem -> list of IDs
        source_map = defaultdict(list)
        for sid, name in cur.fetchall():
            stem = self._normalize_stem(Path(name).stem)
            source_map[stem].append(sid)

        for psd_id, psd_name, psd_path_str in psds:
            self._process_single_psd(psd_id, psd_name, Path(psd_path_str), source_map)

        self.db.conn.commit()

    def _process_single_psd(self, psd_id: int, name: str, path: Path, source_map: dict):
        # 1. Try Stem Matching
        psd_stem = self._normalize_stem(Path(name).stem)
        # Remove common edit suffixes for matching
        clean_stem = re.sub(r'[-_](edit|final|v\d+|copy|retouched)$', '', psd_stem)
        
        if clean_stem in source_map:
            for src_id in source_map[clean_stem]:
                self._save_psd_link(psd_id, src_id, 100, "stem")
            return  # Stop if stem match found (highest priority)

        # 2. Try Smart Object Analysis (slower, requires reading file)
        if PSDImage and path.exists():
            try:
                refs = self._extract_psd_references(path)
                for ref_name in refs:
                    ref_stem = self._normalize_stem(Path(ref_name).stem)
                    if ref_stem in source_map:
                        for src_id in source_map[ref_stem]:
                            self._save_psd_link(psd_id, src_id, 95, "smart_object")
            except Exception as e:
                logging.debug(f"Failed to parse PSD {path}: {e}")

    def _save_psd_link(self, psd_id: int, src_id: int, conf: int, method: str):
        self.db.conn.execute("""
            INSERT OR REPLACE INTO psd_source_links 
            (psd_file_id, source_file_id, confidence, link_method)
            VALUES (?, ?, ?, ?)
        """, (psd_id, src_id, conf, method))

    def _extract_psd_references(self, path: Path) -> List[str]:
        # Pylance guard: If library is missing, return empty
        if PSDImage is None:
            return []

        # Skip smart object extraction for large PSDs to prevent memory exhaustion
        file_size = path.stat().st_size
        if file_size > config.PSD_SMART_OBJECT_MAX_SIZE:
            logging.debug(
                f"Skipping smart object extraction for large PSD: {path.name} "
                f"({file_size / (1024 * 1024):.1f} MB > {config.PSD_SMART_OBJECT_MAX_SIZE / (1024 * 1024):.0f} MB)"
            )
            return []

        refs = []
        # cast(Any, ...) tells Pylance "Trust me, this object exists"
        # This fixes "open is not a known attribute of None"
        psd_class = cast(Any, PSDImage)
        psd = psd_class.open(path)

        for layer in psd.descendants():
            # Cast layer to Any to access dynamic attributes like 'smart_object'
            # This fixes "Cannot access attribute smart_object for class Layer"
            l_any = cast(Any, layer)

            if hasattr(l_any, 'smart_object') and l_any.smart_object:
                so = l_any.smart_object
                if hasattr(so, 'filename') and so.filename:
                    refs.append(so.filename)
        return refs

    def _normalize_stem(self, stem: str) -> str:
        s = stem.lower().strip()
        s = re.sub(r'\(copy\)$', '', s)
        s = re.sub(r'_copy$', '', s)
        s = re.sub(r'\(\d+\)$', '', s)
        return s.strip('_- ')

    def link_raw_outputs(self, dry_run: bool = False, csv_output: Optional[str] = None) -> int:
        """
        Links RAW files to their JPEG/TIFF outputs based on metadata.
        Uses multiple matching strategies with confidence scores.

        Args:
            dry_run: If True, don't insert into database
            csv_output: Optional CSV file path to write proposed links for review

        Returns:
            Number of links created/proposed
        """
        logging.info("Building RAW→JPEG output links...")
        cur = self.db.conn.cursor()

        # Load RAWs with metadata
        cur.execute("""
            SELECT f.id, f.orig_name, m.capture_datetime, m.camera_model
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type = 'raw'
        """)
        raws = cur.fetchall()

        # Load potential outputs (JPEG/TIFF) with metadata
        cur.execute("""
            SELECT f.id, f.orig_name, m.capture_datetime, m.camera_model
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type IN ('jpeg', 'tiff')
        """)
        outputs = cur.fetchall()

        # Build lookup indices for efficient matching
        # Index: (datetime, stem) -> list of output ids
        datetime_stem_index = defaultdict(list)
        # Index: (datetime, camera) -> list of output ids
        datetime_camera_index = defaultdict(list)

        for out_id, out_name, out_dt_str, out_camera in outputs:
            if out_dt_str:
                out_dt = self._parse_datetime(out_dt_str)
                if out_dt:
                    stem = self._normalize_stem(Path(out_name).stem)
                    datetime_stem_index[(out_dt, stem)].append((out_id, out_name, out_dt_str, out_camera))

                    if out_camera:
                        datetime_camera_index[(out_dt, out_camera)].append((out_id, out_name, out_dt_str, out_camera))

        # Collect all proposed links
        proposed_links = []

        # For each RAW, try matching strategies
        for raw_id, raw_name, raw_dt_str, raw_camera in raws:
            if not raw_dt_str:
                continue

            raw_dt = self._parse_datetime(raw_dt_str)
            if not raw_dt:
                continue

            raw_stem = self._normalize_stem(Path(raw_name).stem)

            # Strategy 1: Exact datetime + stem match (confidence=95)
            key = (raw_dt, raw_stem)
            if key in datetime_stem_index:
                for out_id, out_name, out_dt_str, out_camera in datetime_stem_index[key]:
                    proposed_links.append((
                        raw_id, raw_name, out_id, out_name,
                        raw_dt_str, out_dt_str, raw_camera or '', out_camera or '',
                        95, 'exact_stem_datetime'
                    ))
                continue  # Move to next RAW (highest priority match found)

            # Strategy 2: Exact datetime + camera match (confidence=90)
            if raw_camera:
                key = (raw_dt, raw_camera)
                if key in datetime_camera_index:
                    for out_id, out_name, out_dt_str, out_camera in datetime_camera_index[key]:
                        # Also check that stems are reasonably similar to avoid false positives
                        out_stem = self._normalize_stem(Path(out_name).stem)
                        if raw_stem == out_stem:
                            proposed_links.append((
                                raw_id, raw_name, out_id, out_name,
                                raw_dt_str, out_dt_str, raw_camera, out_camera or '',
                                90, 'exact_datetime_camera'
                            ))
                    if proposed_links and proposed_links[-1][0] == raw_id:
                        continue  # Found match, move to next RAW

            # Strategy 3: Datetime within ±2s + stem match (confidence=75)
            for seconds_delta in range(-config.RAW_JPEG_DATETIME_TIGHT_WINDOW_SEC,
                                      config.RAW_JPEG_DATETIME_TIGHT_WINDOW_SEC + 1):
                check_dt = raw_dt + timedelta(seconds=seconds_delta)
                key = (check_dt, raw_stem)
                if key in datetime_stem_index:
                    for out_id, out_name, out_dt_str, out_camera in datetime_stem_index[key]:
                        proposed_links.append((
                            raw_id, raw_name, out_id, out_name,
                            raw_dt_str, out_dt_str, raw_camera or '', out_camera or '',
                            75, 'close_stem_datetime'
                        ))
                    break  # Found match, stop searching this window

            if proposed_links and proposed_links[-1][0] == raw_id:
                continue  # Found match, move to next RAW

            # Strategy 4: Datetime within ±5s + camera match (confidence=60)
            if raw_camera:
                for seconds_delta in range(-config.RAW_JPEG_DATETIME_LOOSE_WINDOW_SEC,
                                          config.RAW_JPEG_DATETIME_LOOSE_WINDOW_SEC + 1):
                    check_dt = raw_dt + timedelta(seconds=seconds_delta)
                    key = (check_dt, raw_camera)
                    if key in datetime_camera_index:
                        for out_id, out_name, out_dt_str, out_camera in datetime_camera_index[key]:
                            out_stem = self._normalize_stem(Path(out_name).stem)
                            if raw_stem == out_stem:
                                proposed_links.append((
                                    raw_id, raw_name, out_id, out_name,
                                    raw_dt_str, out_dt_str, raw_camera, out_camera or '',
                                    60, 'loose_datetime_camera'
                                ))
                        break  # Found match, stop searching this window

        # Write to CSV if requested
        if csv_output:
            self._write_links_csv(proposed_links, csv_output)
            logging.info(f"Wrote {len(proposed_links)} proposed links to {csv_output}")

        # Insert into database if not dry-run
        if not dry_run:
            for raw_id, _, out_id, _, _, _, _, _, confidence, method in proposed_links:
                if confidence >= config.RAW_JPEG_MIN_CONFIDENCE:
                    self.db.conn.execute(
                        "INSERT OR IGNORE INTO raw_outputs (raw_file_id, output_file_id, link_method, confidence) VALUES (?, ?, ?, ?)",
                        (raw_id, out_id, method, confidence)
                    )
            self.db.conn.commit()
            logging.info(f"Created {len(proposed_links)} RAW→JPEG links")
        else:
            logging.info(f"DRY RUN: Proposed {len(proposed_links)} links (not saved to database)")

        return len(proposed_links)

    def _parse_datetime(self, dt_str: str) -> Optional[datetime]:
        """Parse ISO datetime string, return None if invalid"""
        try:
            return datetime.fromisoformat(dt_str)
        except (ValueError, TypeError):
            return None

    def _write_links_csv(self, links: List[Tuple], csv_path: str):
        """Write proposed links to CSV for review"""
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'RAW ID',
                'RAW Filename',
                'JPEG ID',
                'JPEG Filename',
                'RAW DateTime',
                'JPEG DateTime',
                'RAW Camera',
                'JPEG Camera',
                'Confidence',
                'Match Method'
            ])
            writer.writerows(links)