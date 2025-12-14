import logging
import re
from pathlib import Path
from typing import List, cast, Any
from collections import defaultdict

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