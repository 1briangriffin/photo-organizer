import re
from pathlib import Path
from datetime import datetime, UTC
from collections import defaultdict
from typing import List, Dict, Any, Optional, Set

from .. import config
from ..database.ops import DBOperations

class DestinationPlanner:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops
        # Cache used names to prevent collisions within a single run
        self.used_names = defaultdict(set)
        # File ids whose dest_path plan_all assigned during this invocation.
        # Used by the pipeline to extend PipelineCandidates with just-planned
        # primaries/JPEGs so downstream writers (linked-dest assignment, mover)
        # see them in the candidate set.
        self._assigned_ids: Set[int] = set()

    def plan_all(self, dest_root: Path,
                 candidate_file_ids: Optional[Set[int]] = None) -> Set[int]:
        """
        Main entry point. Calculates destination paths for all file types.

        When ``candidate_file_ids`` is provided, planning is restricted to those
        ids — stale `dest_path IS NULL` rows (from an interrupted prior run, or
        rows outside this run's working set) are left alone.

        Returns the set of file_ids whose dest_path was assigned during this
        call, so the caller can extend its candidate set before running
        downstream linked-destination assignment and the mover.
        """
        self._assigned_ids = set()
        # 1. Load existing destinations to avoid collisions with previous runs
        existing = self.db.get_dest_collision_set()
        for parent, names in existing.items():
            self.used_names[parent].update(names)

        # 2. Assign Primary Media (RAW, VIDEO, TIFF)
        self._plan_primary(dest_root, candidate_file_ids)

        # 3. Assign JPEGs (with Grouping Logic)
        self._plan_jpegs(dest_root, candidate_file_ids)

        # 4. (Future: Sidecar & PSD assignment would be called here)
        # We can implement those as separate methods following the same pattern.
        self._plan_orphaned_psds(dest_root, candidate_file_ids)

        return set(self._assigned_ids)

    def _plan_primary(self, dest_root: Path,
                      candidate_file_ids: Optional[Set[int]] = None):
        rows = self.db.fetch_primary_files(candidate_ids=candidate_file_ids)

        for fid, orig_name, orig_path, ftype, capture_str in rows:
            dt = self._parse_or_fallback(capture_str, orig_path)
            
            # Determine Folder: output/YYYY/YYYY-MM or raw/YYYY/YYYY-MM
            base_folder = "raw" if ftype == 'raw' else "output"
            folder = dest_root / base_folder / config.FOLDER_PATTERN.format(year=dt.year, month=dt.month)
            
            # Determine Filename
            stem = Path(orig_name).stem
            ext = Path(orig_name).suffix
            dt_suffix = dt.strftime("%Y-%m-%d_%H-%M-%S")
            new_name = f"{stem}_{dt_suffix}{ext}"
            
            final_path = self._resolve_collision(folder, new_name)
            self.db.update_dest_path(fid, str(final_path))
            self._assigned_ids.add(fid)

    def _plan_jpegs(self, dest_root: Path,
                    candidate_file_ids: Optional[Set[int]] = None):
        """
        Groups JPEGs by Stem + Time.
        Largest resolution becomes 'Main', others become 'Resized'.
        """
        rows = self.db.fetch_jpeg_groups(candidate_ids=candidate_file_ids)
        groups = defaultdict(list)

        # Grouping Pass
        for row in rows:
            dt = self._parse_or_fallback(row['capture_dt'], row['path'])
            # Normalize stem to group "IMG_123" and "IMG_123 (copy)"
            norm_stem = self._normalize_stem(Path(row['name']).stem)
            
            # Key: (NormalizedName, TimestampToSeconds)
            key = (norm_stem, int(dt.timestamp()))
            groups[key].append({**row, 'parsed_dt': dt})

        # Assignment Pass
        for group in groups.values():
            # Find "Best" (Main) Image based on pixels
            best = max(group, key=lambda x: (x['w'] or 0) * (x['h'] or 0))
            
            for item in group:
                dt = item['parsed_dt']
                folder = dest_root / "output" / config.FOLDER_PATTERN.format(year=dt.year, month=dt.month)
                
                stem = Path(item['name']).stem
                ext = Path(item['name']).suffix
                dt_str = dt.strftime("%Y-%m-%d_%H-%M-%S")

                if item == best:
                    # Main version
                    new_name = f"{stem}_{dt_str}{ext}"
                else:
                    # Resized version
                    w, h = item['w'], item['h']
                    dim_str = f"_{w}x{h}" if w and h else ""
                    new_name = f"{stem}_resized{dim_str}_{dt_str}{ext}"

                final_path = self._resolve_collision(folder, new_name)
                self.db.update_dest_path(item['id'], str(final_path))
                self._assigned_ids.add(item['id'])


    def _plan_orphaned_psds(self, dest_root: Path,
                            candidate_file_ids: Optional[Set[int]] = None):
        """
        Finds PSDs that have NO entry in psd_source_links and assigns them
        a destination based on their own metadata/mtime.
        """
        cur = self.db.conn.cursor()
        sql = """
            SELECT f.id, f.orig_name, f.orig_path, m.capture_datetime
            FROM files f
            LEFT JOIN media_metadata m ON f.id = m.file_id
            LEFT JOIN psd_source_links l ON f.id = l.psd_file_id
            WHERE f.type = 'psd'
              AND f.dest_path IS NULL
              AND l.psd_file_id IS NULL
        """
        params: List[Any] = []
        if candidate_file_ids is not None:
            if not candidate_file_ids:
                return
            placeholders = ",".join("?" * len(candidate_file_ids))
            sql += f" AND f.id IN ({placeholders})"
            params.extend(int(i) for i in candidate_file_ids)

        cur.execute(sql, params)

        rows = cur.fetchall()
        for fid, name, path_str, capture_str in rows:
            dt = self._parse_or_fallback(capture_str, path_str)
            
            # Save to "output/YYYY/..." just like JPEGs
            folder = dest_root / "output" / config.FOLDER_PATTERN.format(year=dt.year, month=dt.month)
            
            stem = Path(name).stem
            ext = Path(name).suffix
            dt_suffix = dt.strftime("%Y-%m-%d_%H-%M-%S")
            new_name = f"{stem}_{dt_suffix}{ext}"
            
            final_path = self._resolve_collision(folder, new_name)
            self.db.update_dest_path(fid, str(final_path))
            self._assigned_ids.add(fid)

    def _resolve_collision(self, folder: Path, filename: str) -> Path:
        """Ensures filename is unique in the destination folder."""
        stem = Path(filename).stem
        ext = Path(filename).suffix
        candidate = filename
        counter = 1
        
        # Check against DB cache
        while candidate in self.used_names[folder]:
            candidate = f"{stem}_{counter}{ext}"
            counter += 1
            
        self.used_names[folder].add(candidate)
        return folder / candidate

    def _parse_or_fallback(self, date_str: Optional[str], path_str: str) -> datetime:
        if date_str:
            return datetime.fromisoformat(date_str)
        # Fallback to mtime
        try:
            ts = Path(path_str).stat().st_mtime
            return datetime.fromtimestamp(ts)
        except OSError:
            return datetime.now()

    def _normalize_stem(self, stem: str) -> str:
        s = stem.lower().strip()
        s = re.sub(r'\(copy\)$', '', s)
        s = re.sub(r'_copy$', '', s)
        s = re.sub(r'\(\d+\)$', '', s) # Remove (1), (2)
        return s.strip('_- ')