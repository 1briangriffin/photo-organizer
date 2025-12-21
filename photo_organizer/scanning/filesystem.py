import os
import logging
import re
import threading
from pathlib import Path
from typing import Iterator, Set, Optional, List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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
             known_sparse_hashes: Set[str],
             skip_dirs: Optional[Set[Path]] = None,
             max_workers: int = 3) -> Iterator[FileRecord]:
        """
        Generator that yields FileRecords for every valid file in root.

        Args:
            known_sparse_hashes: Sparse hashes already observed (DB + current run).
                                 Used to decide when to fall back to full hashing.
            max_workers: Number of parallel workers for file processing (HDD-optimized default: 3)
        """
        skip_dirs = skip_dirs or set()

        if max_workers <= 1:
            # Sequential mode (original behavior)
            yield from self._scan_sequential(root, is_seed, known_sparse_hashes, skip_dirs)
        else:
            # Parallel mode with directory batching
            yield from self._scan_parallel(root, is_seed, known_sparse_hashes, skip_dirs, max_workers)

    def _scan_sequential(self,
                        root: Path,
                        is_seed: bool,
                        known_sparse_hashes: Set[str],
                        skip_dirs: Set[Path]) -> Iterator[FileRecord]:
        """Sequential scanning (original implementation)."""
        for path in self._iter_files(root, skip_dirs):
            record = self._process_single_file(path, is_seed, known_sparse_hashes)
            if record:
                yield record

    def _scan_parallel(self,
                      root: Path,
                      is_seed: bool,
                      known_sparse_hashes: Set[str],
                      skip_dirs: Set[Path],
                      max_workers: int) -> Iterator[FileRecord]:
        """
        Parallel scanning with directory-level batching.
        Groups files by directory to minimize disk head seeks on HDDs.
        """
        # Thread-safe lock for updating known_sparse_hashes
        hash_lock = threading.Lock()

        # Group files by directory for HDD-friendly sequential reads
        dir_batches = self._group_files_by_directory(root, skip_dirs)

        logging.info(f"Parallel scan: {len(dir_batches)} directories, {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all directory batches for processing
            future_to_batch = {}
            for directory, files in dir_batches.items():
                future = executor.submit(
                    self._process_directory_batch,
                    directory,
                    files,
                    is_seed,
                    known_sparse_hashes,
                    hash_lock
                )
                future_to_batch[future] = directory

            # Yield results as they complete
            for future in as_completed(future_to_batch):
                directory = future_to_batch[future]
                try:
                    records = future.result()
                    for record in records:
                        yield record
                except Exception as e:
                    logging.error(f"Failed to process directory {directory}: {e}")

    def _group_files_by_directory(self, root: Path, skip_dirs: Set[Path]) -> dict[Path, List[Path]]:
        """Groups files by their parent directory for sequential processing."""
        dir_batches: dict[Path, List[Path]] = {}

        for path in self._iter_files(root, skip_dirs):
            parent = path.parent
            if parent not in dir_batches:
                dir_batches[parent] = []
            dir_batches[parent].append(path)

        return dir_batches

    def _process_directory_batch(self,
                                 directory: Path,
                                 files: List[Path],
                                 is_seed: bool,
                                 known_sparse_hashes: Set[str],
                                 hash_lock: threading.Lock) -> List[FileRecord]:
        """Processes all files in a directory sequentially (HDD-friendly)."""
        records = []

        for path in files:
            record = self._process_single_file(path, is_seed, known_sparse_hashes, hash_lock)
            if record:
                records.append(record)

        return records

    def _process_single_file(self,
                            path: Path,
                            is_seed: bool,
                            known_sparse_hashes: Set[str],
                            hash_lock: Optional[threading.Lock] = None) -> Optional[FileRecord]:
        """Processes a single file and returns FileRecord or None on error."""
        try:
            stat_result = path.stat()
            size_bytes = stat_result.st_size
            mtime = stat_result.st_mtime

            # 1. Classify
            ext = path.suffix.lower()
            if path.name.startswith("._"):
                ftype = 'other'
            else:
                ftype = config.EXT_TO_TYPE.get(ext, 'other')

            # 2. Compute Hash (The Performance Logic)
            # Thread-safe access to known_sparse_hashes
            if hash_lock:
                with hash_lock:
                    hash_res = self.hasher.compute_hash(path, known_sparse_hashes)
                    if hash_res.sparse_hash:
                        known_sparse_hashes.add(hash_res.sparse_hash)
            else:
                # Sequential mode: no lock needed
                hash_res = self.hasher.compute_hash(path, known_sparse_hashes)
                if hash_res.sparse_hash:
                    known_sparse_hashes.add(hash_res.sparse_hash)

            # 3. Basic Metadata (for Organization Phase)
            capture_dt = None
            cam_model = None
            lens_model = None
            duration = None

            if ftype == 'video':
                capture_dt, duration, cam_model = self.metadata.get_video_metadata(path)
            elif ftype in ('raw', 'jpeg', 'tiff', 'psd'):
                capture_dt, cam_model, lens_model = self.metadata.get_image_metadata(path)

            if not capture_dt:
                capture_dt = self._fallback_file_datetime(path, mtime)

            # 4. Score Name
            name_score = self._calculate_score(path.stem)

            return FileRecord(
                hash=hash_res.full_hash,
                sparse_hash=hash_res.sparse_hash,
                hash_is_sparse=hash_res.is_sparse,
                type=ftype,
                ext=ext,
                orig_name=path.name,
                orig_path=path,
                size_bytes=size_bytes,
                mtime=mtime,
                is_seed=is_seed,
                name_score=name_score,
                capture_datetime=capture_dt,
                camera_model=cam_model,
                lens_model=lens_model,
                duration_sec=duration
            )

        except Exception as e:
            logging.error(f"Failed to scan {path}: {e}")
            return None

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

    def _fallback_file_datetime(self, path: Path, mtime: Optional[float] = None) -> datetime:
        ts = mtime if mtime is not None else path.stat().st_mtime
        return datetime.fromtimestamp(ts)
