import csv
import logging
import re
from pathlib import Path
from typing import Any, Iterable, List, Optional, Set, Tuple, cast
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
    def __init__(self, db_ops: DBOperations, run_id: Optional[int] = None):
        self.db = db_ops
        self.run_id = run_id

    def link_raw_sidecars(self, candidate_file_ids: Optional[Set[int]] = None):
        """
        Links Sidecar files (.xmp) to RAW files in the same directory with same stem.

        ``candidate_file_ids`` filters which *sidecars* are eligible for linking
        (only rows in the working set get wired up). RAWs are not filtered —
        a newly discovered sidecar needs to see pre-existing RAWs to link to.
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
            if candidate_file_ids is not None and sid not in candidate_file_ids:
                continue
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
                        """
                        INSERT OR IGNORE INTO raw_sidecars
                        (raw_file_id, sidecar_file_id, created_by_run_id)
                        VALUES (?, ?, ?)
                        """,
                        (rid, sid, self.run_id)
                    )
                    links_made += 1

        # Commit owned by caller — link data belongs inside the pipeline savepoint.
        logging.info(f"Linked {links_made} sidecars.")

    def link_raw_sidecars_by_dest(self, candidate_file_ids: Optional[Set[int]] = None):
        """
        Links newly imported sidecar files to their RAWs using dest_path co-location.

        Used by --ingest-dest when sidecars were written next to already-organized RAWs
        in dest. Matches the sidecar's orig_path (its current location in dest) against
        the RAW's dest_path by parent directory and stem.

        Only considers sidecars with dest_path IS NULL (newly imported via ingest_mode).
        ``candidate_file_ids`` further restricts sidecars to this run's working set.
        """
        logging.info("Linking new sidecars to RAWs by dest co-location...")
        cur = self.db.conn.cursor()

        cur.execute("SELECT id, orig_path FROM files WHERE type = 'sidecar' AND dest_path IS NULL")
        new_sidecars = []
        for sid, path_str in cur.fetchall():
            if candidate_file_ids is not None and sid not in candidate_file_ids:
                continue
            p = Path(path_str)
            new_sidecars.append((sid, p.parent, p.stem.lower()))

        if not new_sidecars:
            logging.info("No new sidecars to link by dest co-location.")
            return

        cur.execute("SELECT id, dest_path FROM files WHERE type = 'raw' AND dest_path IS NOT NULL")
        raw_by_dest = {}
        for rid, dest_str in cur.fetchall():
            p = Path(dest_str)
            raw_by_dest[(p.parent, p.stem.lower())] = rid

        links_made = 0
        for sid, sidecar_parent, sidecar_stem in new_sidecars:
            if (sidecar_parent, sidecar_stem) in raw_by_dest:
                rid = raw_by_dest[(sidecar_parent, sidecar_stem)]
                self.db.conn.execute(
                    """
                    INSERT OR IGNORE INTO raw_sidecars
                    (raw_file_id, sidecar_file_id, created_by_run_id)
                    VALUES (?, ?, ?)
                    """,
                    (rid, sid, self.run_id)
                )
                links_made += 1

        # Commit owned by caller — link data belongs inside the pipeline savepoint.
        logging.info(f"Linked {links_made} sidecars by dest co-location.")

    def link_psds(self, candidate_file_ids: Optional[Set[int]] = None):
        """
        Links PSD files to their source images.
        Strategy 1: Exact Stem Match (High Confidence)
        Strategy 2: PSD Smart Object Parsing (Medium Confidence)

        ``candidate_file_ids`` filters which PSDs are eligible — only PSDs in
        the run's working set get linked. Sources (RAW/JPEG) are not filtered
        because a newly imported PSD needs to find pre-existing sources.
        """
        if not PSDImage:
            logging.warning("psd-tools not installed; skipping smart object analysis.")

        logging.info("Linking PSD files to sources...")
        cur = self.db.conn.cursor()

        # Get PSDs (filter to candidates if provided)
        cur.execute("SELECT id, orig_name, orig_path FROM files WHERE type='psd'")
        psds = [row for row in cur.fetchall()
                if candidate_file_ids is None or row[0] in candidate_file_ids]

        # Get Potential Sources (RAW, JPEG)
        cur.execute("SELECT id, orig_name FROM files WHERE type IN ('raw', 'jpeg')")
        # Build lookup: normalized_stem -> list of IDs
        source_map = defaultdict(list)
        for sid, name in cur.fetchall():
            stem = self._normalize_stem(Path(name).stem)
            source_map[stem].append(sid)

        for psd_id, psd_name, psd_path_str in psds:
            self._process_single_psd(psd_id, psd_name, Path(psd_path_str), source_map)

        # Commit owned by caller — link data belongs inside the pipeline savepoint.

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
            (psd_file_id, source_file_id, confidence, link_method, created_by_run_id)
            VALUES (?, ?, ?, ?, ?)
        """, (psd_id, src_id, conf, method, self.run_id))

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

    def _current_stem(self, orig_name: str, dest_path: Optional[str]) -> str:
        """
        Return the file's *current* stem — the one that matches what's on disk
        right now. When dest_path is set, the planner has already rewritten the
        filename (usually appending a capture_datetime suffix) and that's the
        identity editors like DPP see when they export next to it. When
        dest_path is NULL (pre-plan), fall back to orig_name.

        Used by Strategy 5 (editor_export_identity): DPP/Lightroom write an
        export filename that matches the RAW's current stem, so this is the
        correct key to compare against the JPEG's orig_name stem.
        """
        source = dest_path if dest_path else orig_name
        return Path(source).stem.lower()

    def link_raw_outputs(self, candidate_file_ids: Optional[Set[int]] = None,
                         dry_run: bool = False,
                         csv_output: Optional[str] = None,
                         proposed_links_out: Optional[List[Tuple]] = None) -> int:
        """
        Links RAW files to their JPEG/TIFF outputs based on metadata.
        Uses multiple matching strategies with confidence scores.

        Args:
            candidate_file_ids: If provided, restricts which JPEG/TIFF *outputs*
                are eligible for linking. RAWs are not filtered — a newly
                imported JPEG needs to see every catalogued RAW to find its
                parent. This matches the ingest-dest case where the output
                was just imported but its source RAW was organized long ago.
            dry_run: If True, don't insert into database
            csv_output: Optional CSV file path to write proposed links for review

        Returns:
            Number of links created/proposed
        """
        logging.info("Building RAW→JPEG output links...")
        cur = self.db.conn.cursor()

        # Load RAWs with metadata — include dest_path so Strategy 5 can use
        # the RAW's *current* stem (post-rename) when matching editor exports.
        cur.execute("""
            SELECT f.id, f.orig_name, f.dest_path, m.capture_datetime, m.camera_model
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type = 'raw'
        """)
        raws = cur.fetchall()

        # Load potential outputs (JPEG/TIFF) with metadata.
        cur.execute("""
            SELECT f.id, f.orig_name, f.dest_path, m.capture_datetime, m.camera_model
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type IN ('jpeg', 'tiff')
        """)
        outputs = cur.fetchall()

        # Build lookup indices for efficient matching
        # Index: (datetime, stem) -> list of output tuples
        datetime_stem_index = defaultdict(list)
        # Index: (datetime, camera) -> list of output tuples
        datetime_camera_index = defaultdict(list)
        # Change 3: Index outputs by their *orig-name* stem + capture_datetime.
        # Strategy 5 matches this against each RAW's *current* stem (derived
        # from its dest_path — post-rename). DPP-style workflow: user renames
        # a RAW after shoot, opens it in DPP, exports a JPEG whose filename
        # stem matches the renamed RAW exactly. We catch that even after the
        # planner appends a datetime suffix to the JPEG on move.
        editor_export_index = defaultdict(list)

        for out_id, out_name, out_dest, out_dt_str, out_camera in outputs:
            if candidate_file_ids is not None and out_id not in candidate_file_ids:
                continue
            if out_dt_str:
                out_dt = self._parse_datetime(out_dt_str)
                if out_dt:
                    stem = self._normalize_stem(Path(out_name).stem)
                    datetime_stem_index[(out_dt, stem)].append(
                        (out_id, out_name, out_dt_str, out_camera)
                    )

                    if out_camera:
                        datetime_camera_index[(out_dt, out_camera)].append(
                            (out_id, out_name, out_dt_str, out_camera)
                        )

                    # Editor-export index uses the JPEG's *orig* stem (what the
                    # editor wrote on disk) — the planner later suffixes it
                    # with a datetime on move, but orig_name preserves the
                    # export-time filename we need for Strategy 5.
                    editor_stem = Path(out_name).stem.lower()
                    editor_export_index[(editor_stem, out_dt)].append(
                        (out_id, out_name, out_dt_str, out_camera)
                    )

        # Precompute raw (current_stem, dt) → count so Strategy 5 can detect
        # ambiguous keys in O(1) rather than re-scanning raws for every RAW.
        raw_key_to_ids: dict = defaultdict(list)
        for raw_id, raw_name, raw_dest, raw_dt_str, _ in raws:
            if not raw_dt_str:
                continue
            raw_dt_for_key = self._parse_datetime(raw_dt_str)
            if not raw_dt_for_key:
                continue
            raw_key_to_ids[(self._current_stem(raw_name, raw_dest), raw_dt_for_key)].append(raw_id)

        # Collect all proposed links
        proposed_links = []

        # For each RAW, try matching strategies
        for raw_id, raw_name, raw_dest, raw_dt_str, raw_camera in raws:
            if not raw_dt_str:
                continue

            raw_dt = self._parse_datetime(raw_dt_str)
            if not raw_dt:
                continue

            raw_stem = self._normalize_stem(Path(raw_name).stem)

            # Strategy 5: editor-export identity — RAW's *current* stem matches
            # a JPEG/TIFF's *orig* stem at the same capture_datetime. Runs FIRST
            # because its confidence (100) exceeds every other strategy and
            # expresses a near-certain DPP/Lightroom export relationship.
            raw_current_stem = self._current_stem(raw_name, raw_dest)
            editor_key = (raw_current_stem, raw_dt)
            editor_hits = editor_export_index.get(editor_key, [])
            if editor_hits:
                # Ambiguity guard: if this (current_stem, dt) key matches
                # multiple RAWs we cannot be sure which RAW any given output
                # belongs to — skip and let lower-confidence strategies (or
                # no link) handle it.
                raw_ids_for_key = raw_key_to_ids[editor_key]
                if len(raw_ids_for_key) == 1:
                    for out_id, out_name, out_dt_str, out_camera in editor_hits:
                        proposed_links.append((
                            raw_id, raw_name, out_id, out_name,
                            raw_dt_str, out_dt_str, raw_camera or '', out_camera or '',
                            100, 'editor_export_identity'
                        ))
                    continue  # Move to next RAW — S5 is the strongest signal.

                logging.warning(
                    "Strategy 5 ambiguous: multiple RAWs share key "
                    "(stem=%r, dt=%s): %s; skipping editor-export auto-link.",
                    raw_current_stem,
                    raw_dt,
                    raw_ids_for_key,
                )

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
        if proposed_links_out is not None:
            proposed_links_out.extend(proposed_links)

        if csv_output:
            self._write_links_csv(proposed_links, csv_output)
            logging.info(f"Wrote {len(proposed_links)} proposed links to {csv_output}")

        # Insert into database if not dry-run. INSERT OR IGNORE silently skips
        # pairs that already exist, so track net-new inserts via total_changes
        # to report an accurate delta rather than the proposal count.
        if not dry_run:
            changes_before = self.db.conn.total_changes
            eligible = 0
            for raw_id, _, out_id, _, _, _, _, _, confidence, method in proposed_links:
                if confidence >= config.RAW_JPEG_MIN_CONFIDENCE:
                    eligible += 1
                    self.db.conn.execute(
                        """
                        INSERT OR IGNORE INTO raw_outputs
                        (raw_file_id, output_file_id, link_method, confidence, created_by_run_id)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (raw_id, out_id, method, confidence, self.run_id)
                    )
            inserted = self.db.conn.total_changes - changes_before
            # Commit owned by caller — link data belongs inside the pipeline savepoint.

            cur = self.db.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM raw_outputs")
            total_rows = cur.fetchone()[0]

            skipped_duplicates = eligible - inserted
            below_threshold = len(proposed_links) - eligible
            logging.info(
                f"RAW→JPEG linking: proposed={len(proposed_links)}, "
                f"inserted={inserted}, already_linked={skipped_duplicates}, "
                f"below_confidence_threshold={below_threshold}, "
                f"raw_outputs_total={total_rows}"
            )
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
