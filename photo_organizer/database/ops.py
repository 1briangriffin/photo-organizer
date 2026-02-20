import sqlite3
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any, Set

from ..models import FileRecord

class DBOperations:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_file_record(self, rec: FileRecord) -> int:
        """
        Inserts or updates a file record.
        Uses full hash when available; otherwise falls back to sparse_hash hints.
        """
        now_iso = datetime.now(UTC).isoformat()
        cur = self.conn.cursor()

        full_hash = rec.hash
        sparse_hash = rec.sparse_hash

        row = None
        if full_hash:
            cur.execute("SELECT id, is_seed, name_score, hash, sparse_hash FROM files WHERE hash = ?", (full_hash,))
            row = cur.fetchone()
        if row is None and sparse_hash:
            cur.execute("SELECT id, is_seed, name_score, hash, sparse_hash FROM files WHERE sparse_hash = ?", (sparse_hash,))
            row = cur.fetchone()

        file_id: int

        if row is None:
            # New File
            cur.execute("""
                INSERT INTO files (
                    hash, sparse_hash, type, ext, orig_name, orig_path, size_bytes,
                    is_seed, name_score, first_seen_at, last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                full_hash, sparse_hash, rec.type, rec.ext, rec.orig_name, str(rec.orig_path),
                rec.size_bytes, int(rec.is_seed), rec.name_score,
                now_iso, now_iso
            ))
            
            if cur.lastrowid is None:
                raise RuntimeError("Database INSERT failed to return a row ID.")
            file_id = cur.lastrowid
            return file_id
        else:
            # Existing File: Check priority
            existing_id, existing_seed, existing_score, existing_full_hash, existing_sparse = row
            file_id = int(existing_id)
            
            update_canonical = False
            
            # Seed trumps non-seed
            if int(rec.is_seed) > existing_seed:
                update_canonical = True
            # Tie-break on descriptive name
            elif int(rec.is_seed) == existing_seed and rec.name_score > existing_score:
                update_canonical = True

            if update_canonical:
                cur.execute("""
                    UPDATE files
                    SET orig_name = ?, orig_path = ?, is_seed = ?, name_score = ?, last_seen_at = ?
                    WHERE id = ?
                """, (rec.orig_name, str(rec.orig_path), int(rec.is_seed), rec.name_score, now_iso, file_id))
            else:
                cur.execute("UPDATE files SET last_seen_at = ? WHERE id = ?", (now_iso, file_id))

            # If we previously only had a sparse hash and now have a full hash, persist it.
            if existing_full_hash is None and full_hash:
                cur.execute("UPDATE files SET hash = ? WHERE id = ?", (full_hash, file_id))
            # Keep sparse_hash up to date (in case it was missing)
            if existing_sparse is None and sparse_hash:
                cur.execute("UPDATE files SET sparse_hash = ? WHERE id = ?", (sparse_hash, file_id))
            
            return file_id

    def upsert_media_metadata(self, file_id: int, rec: FileRecord):
        """Updates content metadata (Dimensions, Duration, Time)."""
        capture_str = rec.capture_datetime.isoformat() if rec.capture_datetime else None
        
        aspect = None
        if rec.width and rec.height:
            aspect = rec.width / rec.height

        self.conn.execute("""
            INSERT OR REPLACE INTO media_metadata
            (file_id, capture_datetime, camera_model, lens_model, width, height, duration_sec, aspect_ratio, phash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            file_id, capture_str, rec.camera_model, rec.lens_model, 
            rec.width, rec.height, rec.duration_sec, aspect, rec.phash
        ))

    def fetch_primary_files(self) -> List[Tuple[int, str, str, str, Optional[str]]]:
        """Fetches RAW, VIDEO, TIFF files that need destination assignment."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT f.id, f.orig_name, f.orig_path, f.type, m.capture_datetime
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type IN ('raw','video','tiff') AND f.dest_path IS NULL
        """)
        return cur.fetchall()

    def fetch_jpeg_groups(self) -> List[Dict[str, Any]]:
        """Fetches JPEGs that need destination assignment, grouped by visual content/time."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT f.id, f.orig_name, f.orig_path, m.capture_datetime, m.width, m.height
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type = 'jpeg' AND f.dest_path IS NULL
        """)
        # We return dicts to make the logic layer cleaner
        return [
            {
                'id': r[0], 'name': r[1], 'path': r[2], 
                'capture_dt': r[3], 'w': r[4], 'h': r[5]
            } 
            for r in cur.fetchall()
        ]

    def update_dest_path(self, file_id: int, dest_path: str):
        self.conn.execute("UPDATE files SET dest_path = ? WHERE id = ?", (dest_path, file_id))

    def get_dest_collision_set(self) -> Dict[Path, set]:
        """Returns a map of {ParentDir: {filename, filename...}} for collision checking."""
        cur = self.conn.cursor()
        cur.execute("SELECT dest_path FROM files WHERE dest_path IS NOT NULL")
        used = {}
        for (path_str,) in cur.fetchall():
            p = Path(path_str)
            if p.parent not in used:
                used[p.parent] = set()
            used[p.parent].add(p.name)
        return used

    def get_pending_moves(self) -> List[Tuple[int, str, str, str, Optional[str], Optional[str]]]:
        """Returns (id, orig_path, dest_path, type, hash, sparse_hash) for files ready to move."""
        cur = self.conn.cursor()
        cur.execute("SELECT id, orig_path, dest_path, type, hash, sparse_hash FROM files WHERE dest_path IS NOT NULL")
        return cur.fetchall()

    def fetch_known_sparse_hashes(self) -> Set[str]:
        """Returns all sparse hashes known to the catalog (from files and occurrences)."""
        cur = self.conn.cursor()

        # Use UNION to merge results from both tables in a single query
        cur.execute("""
            SELECT sparse_hash FROM files WHERE sparse_hash IS NOT NULL
            UNION
            SELECT hash FROM file_occurrences WHERE hash_is_sparse = 1
        """)

        return {h[0] for h in cur.fetchall() if h[0]}

    def record_occurrence(
        self,
        file_id: int,
        path: Path,
        is_seed: bool,
        mtime: float,
        size_bytes: int,
        hash_value: str,
        is_sparse: bool,
    ):
        """Tracks a specific on-disk occurrence (source or destination) for reporting/dedup."""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO file_occurrences
            (path, file_id, is_seed, seen_at, mtime, size_bytes, hash, hash_is_sparse)
            VALUES (?, ?, ?, strftime('%s','now'), ?, ?, ?, ?)
            """,
            (str(path), file_id, int(is_seed), mtime, size_bytes, hash_value, int(is_sparse)),
        )

    # ==================== Path Sync Operations ====================

    def get_all_dest_files(self) -> List[Tuple[int, str, str, Optional[str], Optional[float], Optional[int]]]:
        """
        Returns all files with assigned destinations for sync checking.

        Returns:
            List of (file_id, dest_path, hash, sparse_hash, mtime, size_bytes)
            where mtime comes from file_occurrences if available.
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT f.id, f.dest_path, f.hash, f.sparse_hash, fo.mtime, fo.size_bytes
            FROM files f
            LEFT JOIN file_occurrences fo ON fo.path = f.dest_path
            WHERE f.dest_path IS NOT NULL
        """)
        return cur.fetchall()

    def find_file_by_hash(self, hash_value: str) -> Optional[Tuple[int, Optional[str]]]:
        """
        Find file_id and dest_path by hash (checks both full hash and sparse_hash).

        Args:
            hash_value: The hash to search for (full or sparse)

        Returns:
            (file_id, dest_path) or None if not found
        """
        cur = self.conn.cursor()
        # Try full hash first
        cur.execute("SELECT id, dest_path FROM files WHERE hash = ?", (hash_value,))
        row = cur.fetchone()
        if row:
            return row
        # Try sparse hash
        cur.execute("SELECT id, dest_path FROM files WHERE sparse_hash = ?", (hash_value,))
        row = cur.fetchone()
        return row

    def update_dest_path_atomic(
        self,
        file_id: int,
        old_path: str,
        new_path: str,
        new_mtime: float,
        size_bytes: int,
        hash_value: str,
        is_sparse: bool,
    ):
        """
        Atomically update both files.dest_path and file_occurrences for a renamed file.

        Updates:
        1. files.dest_path to new_path
        2. Inserts/replaces file_occurrence for new_path
        3. Removes old file_occurrence entry

        Args:
            file_id: The file ID to update
            old_path: The previous dest_path (for occurrence cleanup)
            new_path: The new dest_path
            new_mtime: Current mtime of the file at new_path
            size_bytes: Size of file in bytes
            hash_value: Hash of the file (full or sparse)
            is_sparse: Whether hash_value is a sparse hash
        """
        # Update dest_path in files table
        self.conn.execute(
            "UPDATE files SET dest_path = ? WHERE id = ?",
            (new_path, file_id)
        )

        # Add new occurrence record
        self.conn.execute(
            """
            INSERT OR REPLACE INTO file_occurrences
            (path, file_id, is_seed, seen_at, mtime, size_bytes, hash, hash_is_sparse)
            VALUES (?, ?, 0, strftime('%s','now'), ?, ?, ?, ?)
            """,
            (new_path, file_id, new_mtime, size_bytes, hash_value, int(is_sparse)),
        )

        # Remove old occurrence if different from new
        if old_path != new_path:
            self.conn.execute(
                "DELETE FROM file_occurrences WHERE path = ?",
                (old_path,)
            )

    def get_dest_files_in_directory(self, directory: str) -> List[Tuple[int, str, str, Optional[str]]]:
        """
        Returns files with dest_path in a specific directory.

        Args:
            directory: Directory path to search (with trailing separator)

        Returns:
            List of (file_id, dest_path, hash, sparse_hash)
        """
        cur = self.conn.cursor()
        # Use LIKE with trailing % to match files in directory
        cur.execute(
            """
            SELECT id, dest_path, hash, sparse_hash
            FROM files
            WHERE dest_path LIKE ? || '%'
            """,
            (directory,)
        )
        return cur.fetchall()
