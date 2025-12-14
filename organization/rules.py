import re
from pathlib import Path
from datetime import datetime, UTC
from collections import defaultdict
from typing import List, Dict, Any, Optional

from .. import config
from ..database.ops import DBOperations

class DestinationPlanner:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops
        # Cache used names to prevent collisions within a single run
        self.used_names = defaultdict(set) 

    def plan_all(self, dest_root: Path):
        """
        Main entry point. Calculates destination paths for all file types.
        """
        # 1. Load existing destinations to avoid collisions with previous runs
        existing = self.db.get_dest_collision_set()
        for parent, names in existing.items():
            self.used_names[parent].update(names)

        # 2. Assign Primary Media (RAW, VIDEO, TIFF)
        self._plan_primary(dest_root)
        
        # 3. Assign JPEGs (with Grouping Logic)
        self._plan_jpegs(dest_root)

        # 4. (Future: Sidecar & PSD assignment would be called here)
        # We can implement those as separate methods following the same pattern.

    def _plan_primary(self, dest_root: Path):
        rows = self.db.fetch_primary_files()
        
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

    def _plan_jpegs(self, dest_root: Path):
        """
        Groups JPEGs by Stem + Time. 
        Largest resolution becomes 'Main', others become 'Resized'.
        """
        rows = self.db.fetch_jpeg_groups()
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