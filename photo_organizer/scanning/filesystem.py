import os
import logging
import re
from pathlib import Path
from typing import Iterator, Set, Optional
from datetime import datetime

from .. import config
from ..models import FileRecord
from ..metadata.extract import MetadataExtractor
from .hasher import FileHasher, HashResult

class DiskScanner:
    def __init__(self):
        self.hasher = FileHasher()
        self.metadata = MetadataExtractor()
        
        # Camera patterns for "Descriptiveness Score" logic
        self.cam_patterns = [re.compile(p) for p in config.CAMERA_PATTERNS]

    def scan(self, 
             root: Path, 
             is_seed: bool, 
             known_hashes: Set[str], 
             skip_dirs: Optional[Set[Path]] = None) -> Iterator[FileRecord]:
        """
        Generator that yields FileRecords for every valid file in root.
        
        Args:
            known_hashes: A set of sparse hashes already in the DB. 
                          Used to determine if we can skip full reading.
        """
        skip_dirs = skip_dirs or set()
        
        for path in self._iter_files(root, skip_dirs):
            try:
                # 1. Classify
                ext = path.suffix.lower()
                ftype = config.EXT_TO_TYPE.get(ext, 'other')
                
                # 2. Compute Hash (The Performance Logic)
                # If ftype is 'other', we might skip hashing entirely if you want,
                # but for safety we usually hash everything to detect duplicates.
                hash_res = self.hasher.compute_hash(path, known_hashes)
                
                # If we found a NEW sparse hash, add it to our local set 
                # so future files in this same scan don't collide.
                if hash_res.is_sparse:
                    known_hashes.add(hash_res.value)

                # 3. Basic Metadata (for Organization Phase)
                # We need capture_time to know where to sort it (YYYY/MM).
                # We do NOT calculate pHash here (too slow).
                capture_dt = None
                cam_model = None
                lens_model = None
                duration = None
                
                if ftype == 'video':
                    capture_dt, duration, cam_model = self.metadata.get_video_metadata(path)
                elif ftype in ('raw', 'jpeg', 'tiff', 'psd'):
                    capture_dt, cam_model, lens_model = self.metadata.get_image_metadata(path)
                
                # Fallback: If metadata failed, check if we can parse the path?
                # (You can re-add your 'infer_datetime_from_path' logic here if desired)
                if not capture_dt:
                    capture_dt = self._fallback_file_datetime(path)

                # 4. Score Name
                name_score = self._calculate_score(path.stem)

                yield FileRecord(
                    hash=hash_res.value,
                    type=ftype,
                    ext=ext,
                    orig_name=path.name,
                    orig_path=path,
                    size_bytes=path.stat().st_size,
                    is_seed=is_seed,
                    name_score=name_score,
                    capture_datetime=capture_dt,
                    camera_model=cam_model,
                    lens_model=lens_model,
                    duration_sec=duration
                )

            except Exception as e:
                logging.error(f"Failed to scan {path}: {e}")
                continue

    def _iter_files(self, root: Path, skip_dirs: Set[Path]) -> Iterator[Path]:
        """Depth-first walker using os.scandir for speed."""
        stack = [root]
        while stack:
            current = stack.pop()
            if skip_dirs and any(sd == current or sd in current.parents for sd in skip_dirs):
                continue
            
            try:
                with os.scandir(current) as it:
                    entries = list(it)
            except (OSError, PermissionError):
                logging.warning(f"Permission denied: {current}")
                continue

            # Sort for stable traversal order
            entries.sort(key=lambda e: e.name.lower())
            
            dirs = []
            files = []
            for e in entries:
                if e.is_dir(follow_symlinks=False):
                    dirs.append(Path(e.path))
                elif e.is_file(follow_symlinks=False):
                    files.append(Path(e.path))

            # Push dirs to stack (reversed so we process A before Z)
            for d in reversed(dirs):
                stack.append(d)
                
            for f in files:
                yield f

    def _calculate_score(self, stem: str) -> int:
        """Higher score = 'Better' filename (more descriptive)."""
        s = stem.lower()
        score = 0
        
        # Penalize generic camera names
        if any(pat.match(s) for pat in self.cam_patterns):
            score -= 5
            
        # Penalize copy suffixes
        if 'copy' in s: score -= 4
        
        # Reward word separators (human named?)
        if ' ' in s: score += 2
        if '-' in s or '_' in s: score += 1
        
        # Reward more letters than numbers
        if sum(c.isalpha() for c in s) > sum(c.isdigit() for c in s):
            score += 2
            
        return score

    def _fallback_file_datetime(self, path: Path) -> datetime:
        return datetime.fromtimestamp(path.stat().st_mtime)