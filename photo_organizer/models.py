from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

@dataclass
class FileRecord:
    """
    Represents a file found during a scan.
    """
    hash: str
    type: str               # raw/jpeg/video/psd/sidecar/tiff/other
    ext: str
    orig_name: str
    orig_path: Path
    size_bytes: int
    is_seed: bool
    name_score: int
    
    # Metadata for Organization (needed for folder structure)
    capture_datetime: Optional[datetime] = None
    
    # Metadata for Enrichment (populated later or if convenient)
    camera_model: Optional[str] = None
    lens_model: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration_sec: Optional[float] = None
    phash: Optional[str] = None