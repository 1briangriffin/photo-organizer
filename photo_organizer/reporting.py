import csv
import logging
import os
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Set, Iterator

from .database.ops import DBOperations
from .scanning.filesystem import DiskScanner
from .scanning.hasher import FileHasher
from . import config

class ReportGenerator:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops
        self.hasher = FileHasher()
        self.scanner = DiskScanner()

    def generate_source_report(self, source_root: str, output_csv: str, skip_dirs: Optional[Set[Path]] = None):
        """
        Walks the source tree and produces a CSV report detailing the status
        of every file.

        Args:
            source_root: Root directory to scan
            output_csv: Output CSV file path
            skip_dirs: Optional set of directories to skip (same as used during organization)
        """
        root = Path(source_root)
        if not root.exists():
            raise FileNotFoundError(f"Source path {source_root} does not exist.")

        skip_dirs = skip_dirs or set()
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

            for file_path in self._iter_all_files(root, skip_dirs):
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

    def _iter_all_files(self, root: Path, skip_dirs: Set[Path]) -> Iterator[Path]:
        """
        Recursively yields all files, respecting skip_dirs.
        Delegates to DiskScanner._iter_files for consistent behavior.
        """
        return self.scanner._iter_files(root, skip_dirs)

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

    def generate_dest_validation_report(
        self,
        dest_root: Path,
        output_csv: Path,
        run_id: Optional[int] = None,
    ) -> dict:
        """
        Scans dest_root against the catalog and writes a validation CSV with
        five sections: CONFIRMED, MISSING, UNTRACKED, RENAMED, MOVED.

        CONFIRMED  — file exists at the path recorded in dest_path.
        MISSING    — dest_path is in the catalog but the file is absent on disk.
        UNTRACKED  — file exists on disk but has no catalog entry.
        RENAMED    — catalog-tracked file found in the expected directory under
                     a different filename (catalog path is stale).
        MOVED      — catalog-tracked file found in a different directory
                     (catalog path is stale).

        RENAMED and MOVED both mean "catalog-tracked but path-changed": the file
        is accounted for on disk, but the catalog's dest_path no longer points
        at it. Use --sync-dest to reconcile.

        Does not attempt to infer whether a missing file represents an
        incomplete move or a lost file (the schema does not record move_mode).
        """
        from .sync.path_sync import DestinationSyncer

        logging.info(f"Validating destination: {dest_root}")
        output_csv.parent.mkdir(parents=True, exist_ok=True)

        syncer = DestinationSyncer(self.db, run_id=run_id)
        # validate_dest is read-only — detect but never apply.
        report = syncer.sync_destinations([dest_root], apply_renames=False)

        confirmed = report.confirmed_files
        missing = report.missing_files
        untracked = report.new_files
        renamed = report.renamed_files
        moved = report.moved_files

        logging.info(
            f"Validation complete — confirmed: {len(confirmed)}, "
            f"missing: {len(missing)}, untracked: {len(untracked)}, "
            f"renamed: {len(renamed)}, moved: {len(moved)}"
        )

        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            writer.writerow(["=== SUMMARY ==="])
            writer.writerow(["Status", "Count"])
            writer.writerow(["Confirmed", len(confirmed)])
            writer.writerow(["Missing", len(missing)])
            writer.writerow(["Untracked", len(untracked)])
            writer.writerow(["Renamed (catalog path stale)", len(renamed)])
            writer.writerow(["Moved (catalog path stale)", len(moved)])
            writer.writerow([])

            writer.writerow(["=== CONFIRMED ==="])
            writer.writerow(["Path"])
            for p in confirmed:
                writer.writerow([p])
            writer.writerow([])

            writer.writerow(["=== MISSING ==="])
            writer.writerow(["Path"])
            for p in missing:
                writer.writerow([p])
            writer.writerow([])

            writer.writerow(["=== UNTRACKED ==="])
            writer.writerow(["Path"])
            for p in untracked:
                writer.writerow([p])
            writer.writerow([])

            writer.writerow(["=== RENAMED (catalog-tracked, path changed) ==="])
            writer.writerow(["Old Path (catalog)", "New Path (on disk)"])
            for old, new in renamed:
                writer.writerow([old, new])
            writer.writerow([])

            writer.writerow(["=== MOVED (catalog-tracked, path changed) ==="])
            writer.writerow(["Old Path (catalog)", "New Path (on disk)"])
            for old, new in moved:
                writer.writerow([old, new])

            writer.writerow([])
            writer.writerow(["=== ACCEPTED VS OBSERVED ==="])
            writer.writerow(["Accepted Path", "Latest Observed Path", "Status"])
            for p in confirmed:
                writer.writerow([p, p, "confirmed"])
            for p in missing:
                writer.writerow([p, "", "missing"])
            for old, new in renamed:
                writer.writerow([old, new, "renamed"])
            for old, new in moved:
                writer.writerow([old, new, "moved"])

        logging.info(f"Validation report written to: {output_csv}")

        return {
            "confirmed": len(confirmed),
            "missing": len(missing),
            "untracked": len(untracked),
            "renamed": len(renamed),
            "moved": len(moved),
        }

    def generate_ingest_preview_report(
        self,
        imported_paths: List[str],
        output_csv: Path,
        move_mode: bool,
    ) -> None:
        """
        Write a CSV preview of planned destinations for files imported via
        --ingest-dest --dry-run.

        Must be called while the ingest savepoint is still open so the planned
        ``dest_path`` values are still visible in the DB. One row per imported
        file with its current location, file type, planned destination, the
        action that would occur (Copy / Move / No-op), and any linked
        companion (sidecar→RAW, output→RAW, PSD→source).
        """
        output_csv.parent.mkdir(parents=True, exist_ok=True)

        headers = [
            "Current Path",
            "Type",
            "Planned Destination",
            "Action",
            "Linked To",
        ]

        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            if not imported_paths:
                return

            cur = self.db.conn.cursor()
            placeholders = ",".join("?" for _ in imported_paths)
            cur.execute(
                f"""
                SELECT id, orig_path, type, dest_path
                FROM files
                WHERE orig_path IN ({placeholders})
                """,
                imported_paths,
            )
            rows = cur.fetchall()

            action_word = "Move" if move_mode else "Copy"
            for file_id, orig_path, ftype, dest_path in rows:
                linked = self._lookup_linked_partner(file_id, ftype)

                if dest_path is None:
                    action = "Unassigned"
                elif orig_path == dest_path:
                    action = "No-op (already in place)"
                else:
                    action = action_word

                writer.writerow([
                    orig_path,
                    ftype,
                    dest_path or "",
                    action,
                    linked or "",
                ])

        logging.info(
            f"Ingest preview written to: {output_csv} ({len(imported_paths)} files)"
        )

    def _lookup_linked_partner(self, file_id: int, file_type: str) -> Optional[str]:
        """Return the orig_name of the best-confidence linked partner, if any."""
        cur = self.db.conn.cursor()
        if file_type == "sidecar":
            cur.execute(
                """
                SELECT r.orig_name FROM raw_sidecars rs
                JOIN files r ON rs.raw_file_id = r.id
                WHERE rs.sidecar_file_id = ?
                """,
                (file_id,),
            )
        elif file_type in ("jpeg", "tiff"):
            cur.execute(
                """
                SELECT r.orig_name FROM raw_outputs ro
                JOIN files r ON ro.raw_file_id = r.id
                WHERE ro.output_file_id = ?
                ORDER BY ro.confidence DESC LIMIT 1
                """,
                (file_id,),
            )
        elif file_type == "psd":
            cur.execute(
                """
                SELECT s.orig_name FROM psd_source_links psl
                JOIN files s ON psl.source_file_id = s.id
                WHERE psl.psd_file_id = ?
                ORDER BY psl.confidence DESC LIMIT 1
                """,
                (file_id,),
            )
        else:
            return None
        row = cur.fetchone()
        return row[0] if row else None
