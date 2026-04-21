import os
import sqlite3
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Iterable, Optional, Tuple, List, Dict, Any, Set

from ..models import FileRecord


def _id_list_clause(
    candidate_ids: Optional[Iterable[int]],
    column: str = "id",
) -> Tuple[str, List[int]]:
    """
    Build an ``AND <column> IN (?,?,...)`` fragment plus its parameter list
    for the optional-candidate-scoping pattern used across Phase B queries.

    ``column`` should be the fully qualified column expression (e.g. ``f.id``)
    for queries that use table aliases.

    Empty set means "no candidates" — returns a predicate that matches nothing
    (1=0) so a caller that explicitly passes an empty set gets zero rows back
    rather than all rows. Pass None to skip scoping entirely.
    """
    if candidate_ids is None:
        return "", []
    ids_list = list(candidate_ids)
    if not ids_list:
        return " AND 1 = 0", []
    placeholders = ",".join("?" for _ in ids_list)
    return f" AND {column} IN ({placeholders})", ids_list


def _root_like_pattern(root: Path) -> str:
    r"""
    Build a SQL LIKE pattern that matches dest_path values strictly under ``root``.

    dest_path is stored as ``str(Path(...))``, which on Windows uses ``\`` and on
    POSIX uses ``/``. We normalise the root the same way, strip any trailing
    separator, and append ``os.sep + '%'`` so the wildcard only matches
    descendants (not siblings with a shared prefix such as ``root_a`` vs
    ``root_ab``).

    LIKE metacharacters in the root (``\``, ``%``, ``_``) are escaped with ``\``
    so literal underscores / percent signs / backslashes in directory names cannot
    act as wildcards. The trailing path separator is also escaped when it is a
    backslash (Windows), since the ESCAPE clause is ``\``. The caller MUST pair
    this pattern with ``ESCAPE '\\'``.
    """
    normalized = str(Path(root)).rstrip("/\\")

    def _escape(s: str) -> str:
        # Escape the escape char first, then LIKE metacharacters.
        return (
            s.replace("\\", "\\\\")
             .replace("%", "\\%")
             .replace("_", "\\_")
        )

    return _escape(normalized) + _escape(os.sep) + "%"


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

    def fetch_primary_files(
        self,
        candidate_ids: Optional[Iterable[int]] = None,
    ) -> List[Tuple[int, str, str, str, Optional[str]]]:
        """Fetches RAW, VIDEO, TIFF files that need destination assignment.

        When ``candidate_ids`` is provided, the result is filtered to
        ``f.id IN candidate_ids``. This is the input-filter side of Phase B
        candidate scoping (Change 2) — planner should only assign destinations
        for files this pipeline run is working on.
        """
        id_clause, id_params = _id_list_clause(
            {int(x) for x in candidate_ids} if candidate_ids is not None else None,
            column="f.id",
        )
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT f.id, f.orig_name, f.orig_path, f.type, m.capture_datetime
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type IN ('raw','video','tiff') AND f.dest_path IS NULL{id_clause}
            """,
            id_params,
        )
        return cur.fetchall()

    def fetch_jpeg_groups(
        self,
        candidate_ids: Optional[Iterable[int]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetches JPEGs that need destination assignment, grouped by visual content/time.

        ``candidate_ids`` behaves the same as in fetch_primary_files.
        """
        id_clause, id_params = _id_list_clause(
            {int(x) for x in candidate_ids} if candidate_ids is not None else None,
            column="f.id",
        )
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT f.id, f.orig_name, f.orig_path, m.capture_datetime, m.width, m.height
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            WHERE f.type = 'jpeg' AND f.dest_path IS NULL{id_clause}
            """,
            id_params,
        )
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
        """Returns (id, orig_path, dest_path, type, hash, sparse_hash) for files ready to move.

        Global scan — preferred only for legacy callers. Pipeline code should
        use get_pending_moves_for_ids(candidate_ids) so Phase B does not act
        on files from outside this run's working set.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT id, orig_path, dest_path, type, hash, sparse_hash FROM files WHERE dest_path IS NOT NULL")
        return cur.fetchall()

    def get_pending_moves_for_ids(
        self,
        candidate_ids: Iterable[int],
    ) -> List[Tuple[int, str, str, str, Optional[str], Optional[str]]]:
        """Same shape as get_pending_moves, scoped to ``candidate_ids``.

        Empty candidate set returns no rows (by design — nothing to move).
        """
        ids_list = [int(x) for x in candidate_ids]
        if not ids_list:
            return []
        placeholders = ",".join("?" for _ in ids_list)
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT id, orig_path, dest_path, type, hash, sparse_hash
            FROM files
            WHERE dest_path IS NOT NULL AND id IN ({placeholders})
            """,
            ids_list,
        )
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

    def get_all_dest_files(self, dest_roots: Optional[List[Path]] = None) -> List[Tuple[int, str, str, Optional[str], Optional[float], Optional[int]]]:
        """
        Returns files with assigned destinations for sync checking.

        Args:
            dest_roots: If provided, only return files whose dest_path falls under
                        one of these roots. If None, returns all dest files globally.

        Returns:
            List of (file_id, dest_path, hash, sparse_hash, mtime, size_bytes)
            where mtime comes from file_occurrences if available.
        """
        cur = self.conn.cursor()
        if dest_roots:
            # Use LIKE with an explicit path-separator boundary so root_a does not
            # match root_ab. Escape SQL LIKE metacharacters (_, %, \) in the root
            # via ESCAPE '\' so literal underscores in directory names do not act
            # as single-character wildcards.
            patterns = [_root_like_pattern(r) for r in dest_roots]
            placeholders = " OR ".join("f.dest_path LIKE ? ESCAPE '\\'" for _ in patterns)
            cur.execute(f"""
                SELECT f.id, f.dest_path, f.hash, f.sparse_hash, fo.mtime, fo.size_bytes
                FROM files f
                LEFT JOIN file_occurrences fo ON fo.path = f.dest_path
                WHERE f.dest_path IS NOT NULL AND ({placeholders})
            """, patterns)
        else:
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
        observed_full_hash: Optional[str] = None,
    ):
        """
        Atomically update both files.dest_path and file_occurrences for a renamed file.

        Updates:
        1. files.dest_path to new_path
        2. Inserts/replaces file_occurrence for new_path (records the
           strongest observed identity — observed_full_hash when available,
           falling back to hash_value)
        3. Removes old file_occurrence entry
        4. If observed_full_hash is provided and files.hash is currently NULL
           for this row, attempts to upgrade it (Change 5). Skips and logs on
           UNIQUE conflict rather than aborting the rename — a conflict means
           the row is a content duplicate of another catalog row and needs
           operator attention, but the rename itself is still correct.

        Args:
            file_id: The file ID to update
            old_path: The previous dest_path (for occurrence cleanup)
            new_path: The new dest_path
            new_mtime: Current mtime of the file at new_path
            size_bytes: Size of file in bytes
            hash_value: Hash key that matched this row in the catalog lookup
                (full or sparse — whichever hit).
            is_sparse: Whether hash_value is a sparse hash.
            observed_full_hash: The full SHA-256 if FileHasher computed one
                during this rename's collision-path escalation. When present,
                it's used for the post-rename occurrence (hash_is_sparse=0)
                and for the opportunistic files.hash upgrade.
        """
        # Update dest_path in files table
        self.conn.execute(
            "UPDATE files SET dest_path = ? WHERE id = ?",
            (new_path, file_id)
        )

        # Opportunistic full-hash upgrade (Change 5). Runs before the
        # occurrence write so the occurrence row sees the strongest identity.
        if observed_full_hash:
            try:
                self.conn.execute(
                    "UPDATE files SET hash = ? WHERE id = ? AND hash IS NULL",
                    (observed_full_hash, file_id),
                )
            except sqlite3.IntegrityError:
                # files.hash is UNIQUE — another row already owns this full
                # hash, meaning this row is a content duplicate of an existing
                # catalog row that must be resolved out-of-band. Leave hash
                # NULL on this row; the rename is still correct and should
                # proceed.
                logging.warning(
                    "Full-hash upgrade blocked by UNIQUE conflict on file_id=%d "
                    "(full_hash=%s). Row remains sparse-only; likely duplicate "
                    "of another catalog row.",
                    file_id, observed_full_hash,
                )

        # Occurrence row records the strongest observed identity available.
        # If a full hash was observed (even via a sparse match), prefer it and
        # mark hash_is_sparse=0 — occurrences are a record of what was on
        # disk, so they should reflect actual observation, not which key hit
        # the catalog lookup.
        occurrence_hash = observed_full_hash or hash_value
        occurrence_is_sparse = 0 if observed_full_hash else int(is_sparse)

        # Add new occurrence record
        self.conn.execute(
            """
            INSERT OR REPLACE INTO file_occurrences
            (path, file_id, is_seed, seen_at, mtime, size_bytes, hash, hash_is_sparse)
            VALUES (?, ?, 0, strftime('%s','now'), ?, ?, ?, ?)
            """,
            (new_path, file_id, new_mtime, size_bytes, occurrence_hash, occurrence_is_sparse),
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
