import sqlite3
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from ..models import FileRecord

class DBOperations:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_file_record(self, rec: FileRecord) -> int:
        """
        Inserts or updates a file record.
        """
        now_iso = datetime.now(UTC).isoformat()
        cur = self.conn.cursor()
        
        # Check existing
        cur.execute("SELECT id, is_seed, name_score FROM files WHERE hash = ?", (rec.hash,))
        row = cur.fetchone()

        file_id: int

        if row is None:
            # New File
            cur.execute("""
                INSERT INTO files (
                    hash, type, ext, orig_name, orig_path, size_bytes,
                    is_seed, name_score, first_seen_at, last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rec.hash, rec.type, rec.ext, rec.orig_name, str(rec.orig_path),
                rec.size_bytes, int(rec.is_seed), rec.name_score,
                now_iso, now_iso
            ))
            
            # Pylance Fix: explicit check ensures we don't return None
            if cur.lastrowid is None:
                raise RuntimeError("Database INSERT failed to return a row ID.")
            file_id = cur.lastrowid
            return file_id
        else:
            # Existing File: Check priority
            existing_id, existing_seed, existing_score = row
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
        """Fetches JPEGs to group them by visual content/time."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT f.id, f.orig_name, f.orig_path, m.capture_datetime, m.width, m.height
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type = 'jpeg'
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

    def get_pending_moves(self) -> List[Tuple[str, str, str]]:
        """Returns (orig_path, dest_path, type) for files ready to move."""
        cur = self.conn.cursor()
        cur.execute("SELECT orig_path, dest_path, type FROM files WHERE dest_path IS NOT NULL")
        return cur.fetchall()