import hashlib
from pathlib import Path
from dataclasses import dataclass
from .. import config

@dataclass
class HashResult:
    value: str
    is_sparse: bool  # True if we only read partial file (Identity confirmed)

class FileHasher:
    def compute_hash(self, path: Path, known_sparse_hashes: set[str]) -> HashResult:
        """
        Computes a fingerprint for the file.
        
        Strategy:
        1. If file < SPARSE_HASH_THRESHOLD:
           -> Full Read (SHA-256).
           
        2. If file >= SPARSE_HASH_THRESHOLD:
           -> Compute Sparse Hash (Header + Middle + Footer + Size).
           -> Check against `known_sparse_hashes` (from DB or current scan).
           -> If UNIQUE: Return Sparse Hash (Fast!).
           -> If COLLISION: Fallback to Full Read (SHA-256) to be safe.
        """
        try:
            file_size = path.stat().st_size
        except FileNotFoundError:
            # File might have been moved/deleted during scan
            return HashResult("error", False)

        # 1. Small files: Just read them. Overhead of seeking isn't worth it.
        if file_size < config.SPARSE_HASH_THRESHOLD:
            return HashResult(self._full_sha256(path), is_sparse=False)

        # 2. Large files: Try Sparse Hash first.
        sparse_h = self._sparse_hash(path, file_size)
        
        # KEY LOGIC: If this sparse fingerprint is globally unique so far,
        # we assume it's a unique file without reading the whole thing.
        # This turns O(N) I/O into O(1) I/O for 99% of large files.
        if sparse_h not in known_sparse_hashes:
            return HashResult(sparse_h, is_sparse=True)
        
        # 3. Collision detected!
        # This sparse hash matches another file (either in DB or just scanned).
        # We must read the full file to determine if it's a true duplicate
        # or just a sparse collision.
        return HashResult(self._full_sha256(path), is_sparse=False)

    def _full_sha256(self, path: Path) -> str:
        """Reads entire file. High I/O cost."""
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            while chunk := f.read(config.HASH_CHUNK_SIZE):
                h.update(chunk)
        return h.hexdigest()

    def _sparse_hash(self, path: Path, file_size: int) -> str:
        """
        Reads Header (4KB), Middle (4KB), Footer (4KB) and mixes in file size.
        Prefixes with 's-' to distinguish from full hashes.
        """
        chunk_size = 4096 # 4KB is sufficient for sparse sampling
        h = hashlib.sha256()
        
        # Mix file size into the hash prevents collisions between 
        # files with same data but different lengths (rare but possible in sparse)
        h.update(str(file_size).encode('ascii')) 
        
        with open(path, 'rb') as f:
            # 1. Start (Header)
            h.update(f.read(chunk_size))
            
            # 2. Middle
            if file_size > chunk_size * 3:
                f.seek(file_size // 2)
                h.update(f.read(chunk_size))
            
            # 3. End (Footer)
            if file_size > chunk_size * 2:
                # Seek from end (2 = SEEK_END)
                try:
                    f.seek(-chunk_size, 2)
                    h.update(f.read(chunk_size))
                except OSError:
                    # Handle edge case where file changed size or is special
                    pass
                
        return f"s-{h.hexdigest()}"