import csv
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

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
        of every file (Copied, Duplicate, or Ignored).
        """
        root = Path(source_root)
        if not root.exists():
            raise FileNotFoundError(f"Source path {source_root} does not exist.")

        logging.info(f"Generating report for {source_root} -> {output_csv}")
        
        # Pre-load DB lookup tables for performance
        # Map: Source Path (str) -> File ID
        logging.info("Loading database index...")
        path_map = self._load_path_map()
        # Map: Hash -> (File ID, Canonical Path, Dest Path)
        hash_map = self._load_hash_map()

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

            # We use the scanner's iterator to handle skip_dirs logic if needed,
            # or just raw os.walk if we want a COMPLETE audit (including skipped dirs).
            # For a copy report, we usually want everything.
            for file_path in self._iter_all_files(root):
                processed_count += 1
                if processed_count % 1000 == 0:
                    logging.info(f"Analyzed {processed_count} files...")

                row = self._analyze_file(file_path, path_map, hash_map)
                writer.writerow(row)

        logging.info(f"Report complete. Analyzed {processed_count} files.")

    def _iter_all_files(self, root: Path):
        """Recursively yields all files, ignoring simple system files."""
        for p in root.rglob("*"):
            if p.is_file():
                yield p

    def _analyze_file(self, path: Path, path_map: Dict[str, Tuple[int, str]], hash_map: Dict[str, tuple]) -> list:
        str_path = str(path.resolve())
        ext = path.suffix.lower()
        file_type = config.EXT_TO_TYPE.get(ext, "other")

        # 1. Check if this exact path is in the DB
        if str_path in path_map:
            fid, dest_path = path_map[str_path]
            
            # Logic Change: Distinguish between "Copied" and just "Indexed"
            if dest_path:
                status = "Copied"
                final_dest = dest_path
            else:
                status = "Indexed"
                final_dest = "N/A"

            return [str_path, status, file_type, final_dest, "", "Active Record"]

        # 2. If 'other', we ignored it.
        if file_type == "other":
            return [str_path, "Skipped", "other", "", "", "Unsupported extension"]

        # 3. It's a supported type but NOT the canonical path. Check duplicates.
        try:
            # We pass empty set for known_hashes because we just want the value
            file_hash = self.hasher.compute_hash(path, set()).value
            
            if file_hash in hash_map:
                fid, canonical_src, canonical_dest = hash_map[file_hash]
                return [str_path, "Duplicate", file_type, "", canonical_src, f"Duplicate of ID {fid}"]
            else:
                return [str_path, "Not In Catalog", file_type, "", "", "Scanned but not imported?"]

        except Exception as e:
            return [str_path, "Error", file_type, "", "", str(e)]
        
    def _load_path_map(self) -> Dict[str, tuple]:
        """Returns Dict[orig_path_str] -> (id, dest_path)"""
        cur = self.db.conn.cursor()
        cur.execute("SELECT id, orig_path, dest_path FROM files")
        # Resolve paths to match scan behavior
        return {str(Path(row[1]).resolve()): (row[0], row[2]) for row in cur.fetchall()}

    def _load_hash_map(self) -> Dict[str, tuple]:
        """Returns Dict[hash] -> (id, orig_path, dest_path)"""
        cur = self.db.conn.cursor()
        cur.execute("SELECT hash, id, orig_path, dest_path FROM files")
        return {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}