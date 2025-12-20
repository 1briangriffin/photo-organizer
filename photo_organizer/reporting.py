import csv
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from .database.ops import DBOperations
from .scanning.filesystem import DiskScanner
from .scanning.hasher import FileHasher
from . import config

class ReportGenerator:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops
        self.hasher = FileHasher()
        self.scanner = DiskScanner()

    def generate_source_report(self, source_root: str, output_csv: str):
        """
        Walks the source tree and produces a CSV report detailing the status 
        of every file.
        """
        root = Path(source_root)
        if not root.exists():
            raise FileNotFoundError(f"Source path {source_root} does not exist.")

        logging.info(f"Generating report for {source_root} -> {output_csv}")
        
        # --- 1. Bulk Load Data ---
        logging.info("Loading database index...")
        
        # Map: Source Path -> File ID (For files strictly tracked in occurrences)
        path_to_id = self._load_path_map()
        
        # Map: File ID -> Canonical Source Path (The "Winner" from files table)
        # This trusts ops.py logic (Seed > Name Score)
        canonical_map = self._load_canonical_map()
        
        # Map: File ID -> Destination Path (Where the winner is going)
        dest_map = self._load_dest_map()
        
        # Map: Hash -> File ID (For identifying duplicates via content)
        hash_to_id = self._load_hash_map()

        headers = [
            "Source Path", 
            "Status", 
            "File Type", 
            "Destination Path", 
            "Canonical Source (If Duplicate)", 
            "Notes"
        ]

        processed_count = 0
        
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for file_path in self._iter_all_files(root):
                processed_count += 1
                if processed_count % 1000 == 0:
                    logging.info(f"Analyzed {processed_count} files...")

                row = self._analyze_file(
                    file_path, 
                    path_to_id, 
                    canonical_map, 
                    dest_map, 
                    hash_to_id
                )
                writer.writerow(row)

        logging.info(f"Report complete. Analyzed {processed_count} files.")

    def _iter_all_files(self, root: Path):
        """Recursively yields all files."""
        for p in root.rglob("*"):
            if p.is_file():
                yield p

    def _analyze_file(self, 
                      path: Path, 
                      path_to_id: Dict[str, int], 
                      canonical_map: Dict[int, str], 
                      dest_map: Dict[int, str], 
                      hash_to_id: Dict[str, int]) -> list:
        
        str_path = str(path.resolve())
        ext = path.suffix.lower()
        file_type = config.EXT_TO_TYPE.get(ext, "other")

        # --- CASE 1: Ignored Files (System junk, etc) ---
        if file_type == "other":
            # If it happens to be in the DB (path_to_id), we note it, otherwise 'Skipped'
            status = "Indexed (Ignored Type)" if str_path in path_to_id else "Skipped"
            return [str_path, status, file_type, "", "", "Unsupported extension"]

        # --- Identify the File ID ---
        # Strategy: 1. Check Path Map (Fast) -> 2. Check Hash (Robust)
        file_id = None
        match_method = "unknown"

        if str_path in path_to_id:
            file_id = path_to_id[str_path]
            match_method = "path_lookup"
        else:
            # Not found by path? Hash it to see if it's a duplicate or new.
            try:
                # Use full hash for reporting to avoid sparse collisions
                hash_res = self.hasher.compute_hash(path, set(), force_full=True)
                file_hash = hash_res.full_hash or hash_res.sparse_hash
                if file_hash and file_hash in hash_to_id:
                    file_id = hash_to_id[file_hash]
                    match_method = "content_hash"
            except Exception as e:
                return [str_path, "Error", file_type, "", "", f"Hash failed: {e}"]

        # --- CASE 2: Not in Catalog ---
        if file_id is None:
             return [str_path, "Not In Catalog", file_type, "", "", "Pending Import"]

        # --- CASE 3: In Catalog (Determine Status) ---
        # Retrieve the single source of truth for this file ID
        canon_path = canonical_map.get(file_id, "Unknown")
        dest_path = dest_map.get(file_id, "")
        
        # Is THIS file the canonical source?
        # We compare strings. Resolve() handles slash differences usually, but be careful.
        is_canonical = (str_path == canon_path)

        if is_canonical:
            if dest_path:
                return [str_path, "Scheduled Copy/Move", file_type, dest_path, "", "Active Record"]
            else:
                # Canonical but no destination (e.g., PSDs not linked, or unorganized RAWs)
                return [str_path, "Indexed (No Dest)", file_type, "", "", "No destination assigned"]
        else:
            # It is a duplicate of the canonical version
            return [str_path, "Duplicate", file_type, "", canon_path, f"Duplicate of ID {file_id} ({match_method})"]

    # --- Data Loaders ---

    def _load_path_map(self) -> Dict[str, int]:
        """Returns Dict[path_str] -> file_id from file_occurrences"""
        # Note: If ops.py isn't populating file_occurrences, this might be empty.
        # That's okay; the hash fallback in _analyze_file will catch the files.
        cur = self.db.conn.cursor()
        try:
            cur.execute("SELECT path, file_id FROM file_occurrences")
            return {str(Path(row[0]).resolve()): row[1] for row in cur.fetchall()}
        except Exception:
            # Graceful fallback if table is empty or missing
            return {}

    def _load_canonical_map(self) -> Dict[int, str]:
        """
        Returns Dict[file_id] -> orig_path
        Trusts the 'files' table as the single source of truth for the 'best' version.
        """
        cur = self.db.conn.cursor()
        cur.execute("SELECT id, orig_path FROM files")
        # Resolve path to ensure string comparison matches scan
        return {row[0]: str(Path(row[1]).resolve()) for row in cur.fetchall()}

    def _load_dest_map(self) -> Dict[int, str]:
        """Returns Dict[file_id] -> dest_path (if assigned)"""
        cur = self.db.conn.cursor()
        cur.execute("SELECT id, dest_path FROM files WHERE dest_path IS NOT NULL")
        return {row[0]: row[1] for row in cur.fetchall()}

    def _load_hash_map(self) -> Dict[str, int]:
        """Returns Dict[hash] -> file_id"""
        cur = self.db.conn.cursor()
        cur.execute("SELECT hash, id FROM files")
        return {row[0]: row[1] for row in cur.fetchall() if row[0]}
