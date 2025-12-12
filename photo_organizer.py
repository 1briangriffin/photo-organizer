#!/usr/bin/env python

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
import hashlib
import logging
import os
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Iterable

import exifread
from PIL import Image
from tqdm import tqdm

from psd_tools import PSDImage

try:
    from pymediainfo import MediaInfo
except ImportError:
    MediaInfo = None

try:
    import imagehash
except ImportError:
    imagehash = None


# ---------------------- CONFIG & CONSTANTS ----------------------

RAW_EXTS = {'.cr2', '.cr3', '.nef', '.arw', '.orf', '.rw2', '.dng'}
JPEG_EXTS = {'.jpg', '.jpeg', '.jpe', '.gif', '.png'}
VIDEO_EXTS = {'.mp4', '.mov', '.m4v', '.avi', '.mts', '.m2ts', '.3gp', '.mpg', '.mpeg', '.tod'}
PSD_EXTS = {'.psd', '.psb', '.pspimage'}
TIFF_EXTS = {'.tif', '.tiff'}

SIDECAR_EXTS = {'.xmp', '.vrd', '.dop', '.dpp', '.pp3'}

DEFAULT_HASH_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks to cut syscall overhead

DATE_TAGS = [
    'EXIF DateTimeOriginal',
    'EXIF DateTimeDigitized',
    'Image DateTime',
]

FOLDER_PATTERN = "{year}/{year}-{month:02d}"

CAMERA_PATTERNS = [
    r'^img_\d+$',
    r'^dsc_\d+$',
    r'^dscf\d+$',
    r'^pxl_\d+$',
    r'^sam_\d+$',
    r'^_dsc\d+$',
    r'^cimg\d+$',
]


# ---------------------- DATA CLASSES ----------------------

@dataclass
class FileRecord:
    hash: str
    type: str               # raw/jpeg/video/psd/sidecar/tiff/other
    ext: str
    orig_name: str
    orig_path: Path
    size_bytes: int
    is_seed: bool
    name_score: int
    capture_datetime: Optional[datetime] = None
    camera_model: Optional[str] = None
    lens_model: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration_sec: Optional[float] = None
    aspect_ratio: Optional[float] = None
    phash: Optional[str] = None


# ---------------------- UTILS ----------------------

def descriptiveness_score(stem: str) -> int:
    s = stem.lower()
    score = 0

    if any(re.match(pat, s) for pat in CAMERA_PATTERNS):
        score -= 5

    # word separators
    if ' ' in s:
        score += 2
    if '-' in s or '_' in s:
        score += 1

    num_alpha = sum(c.isalpha() for c in s)
    num_digit = sum(c.isdigit() for c in s)
    if num_alpha > num_digit:
        score += 2

    if len(s) >= 12:
        score += 1

    return score


def compute_file_hash(path: Path, chunk_size: int = DEFAULT_HASH_CHUNK_SIZE) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def compute_file_hash_from_handle(fileobj, chunk_size: int = DEFAULT_HASH_CHUNK_SIZE) -> str:
    """
    Compute hash using an existing open handle to avoid reopening the file.
    Caller should ensure the handle is at position 0.
    """
    h = hashlib.sha256()
    while True:
        chunk = fileobj.read(chunk_size)
        if not chunk:
            break
        h.update(chunk)
    return h.hexdigest()


def fallback_file_datetime(path: Path) -> datetime:
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts)


def infer_datetime_from_path(path: Path) -> Optional[datetime]:
    """
    Try to infer a capture date from the directory structure.

    Precedence:
      1) Full date components like '2010-04-20', '2010_04_20', '2010/04/20'
      2) Compact full date 'YYYYMMDD' (e.g. '20100420')
      3) Year/month pairs in directories like '.../2009/10/...'
      4) Standalone year folder like '2009' -> 2009-01-01

    Returns a datetime at midnight if successful, else None.
    """
    parts = [p for p in path.parts]

    # 1) Full date patterns: YYYY-MM-DD or YYYY_MM_DD etc.
    full_date_pattern = re.compile(
        r'^(?P<y>19\d{2}|20\d{2})[-_/](?P<m>\d{1,2})[-_/](?P<d>\d{1,2})$'
    )

    for part in parts:
        m = full_date_pattern.match(part)
        if m:
            y = int(m.group("y"))
            m_ = int(m.group("m"))
            d_ = int(m.group("d"))
            try:
                return datetime(y, m_, d_)
            except ValueError:
                continue  # invalid combo, keep looking

    # 2) Compact full date: YYYYMMDD (e.g. '20100420')
    compact_date_pattern = re.compile(r'^(19\d{2}|20\d{2})(\d{2})(\d{2})$')

    for part in parts:
        m = compact_date_pattern.match(part)
        if m:
            y = int(m.group(1))
            m_ = int(m.group(2))
            d_ = int(m.group(3))
            try:
                return datetime(y, m_, d_)
            except ValueError:
                continue

    # 3) Year + month directory pairs, e.g. .../2009/10/...
    year_pattern = re.compile(r'^(19\d{2}|20\d{2})$')
    month_pattern = re.compile(r'^(0[1-9]|1[0-2])$')

    for idx, part in enumerate(parts):
        if not year_pattern.match(part):
            continue
        year = int(part)

        # Check neighbor parts for month (next or previous component)
        neighbor_indices = [idx + 1, idx - 1]
        for ni in neighbor_indices:
            if 0 <= ni < len(parts):
                m_part = parts[ni]
                if month_pattern.match(m_part):
                    month = int(m_part)
                    try:
                        # Assume day=1 when only year+month are known
                        return datetime(year, month, 1)
                    except ValueError:
                        continue

    # 4) Standalone year folder: fall back to Jan 1 of that year
    for part in parts:
        if year_pattern.match(part):
            year = int(part)
            try:
                return datetime(year, 1, 1)
            except ValueError:
                continue

    return None



def parse_exif_datetime(dt_str: str) -> Optional[datetime]:
    # EXIF often: "YYYY:MM:DD HH:MM:SS"
    try:
        dt_str = str(dt_str)
        dt_str = dt_str.replace(':', '-', 2)
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_image_metadata_exif(path: Path, fileobj=None) -> Tuple[Optional[datetime], Optional[str], Optional[str]]:
    """Use exifread to get datetime, camera, lens (if any)."""
    tags = {}
    if fileobj is None:
        try:
            with open(path, 'rb') as f:
                tags = exifread.process_file(f, details=False)
        except Exception as e:
            # This is a real problem (corrupt file, unreadable format, etc.)
            logging.warning("EXIF read failed for %s: %s", path, e)
            return None, None, None
    else:
        try:
            fileobj.seek(0)
            tags = exifread.process_file(fileobj, details=False)
        except Exception as e:
            logging.warning("EXIF read failed for %s: %s", path, e)
            return None, None, None

    if not tags:
        # Not really an error, just means "no EXIF at all".
        # Use DEBUG so it doesn't spam normal logs.
        logging.debug("No EXIF tags found for %s", path)
        return None, None, None

    dt = None
    for tag in DATE_TAGS:
        if tag in tags:
            dt = parse_exif_datetime(tags[tag])
            if dt:
                break

    if dt is None:
        # EXIF present but no usable datetime: debug-level note only.
        logging.debug(
            "EXIF tags present but no datetime found for %s (tags tried: %s)",
            path,
            ", ".join(DATE_TAGS),
        )

    camera_model = None
    if 'Image Model' in tags:
        camera_model = str(tags['Image Model'])

    lens_model = None
    if 'EXIF LensModel' in tags:
        lens_model = str(tags['EXIF LensModel'])

    return dt, camera_model, lens_model


def _parse_mediainfo_datetime(dt_str: Optional[str], is_local_hint: bool) -> Optional[datetime]:
    """
    Parse MediaInfo date strings and normalize to a naive local datetime.

    - Local variants (is_local_hint=True) are treated as local and returned naive.
    - UTC variants (is_local_hint=False) are converted to local before dropping tzinfo.
    """
    if not dt_str:
        return None
    s = dt_str.strip()
    if s.endswith("UTC"):
        s = s[:-3].strip()
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    if dt.tzinfo is None:
        if is_local_hint:
            return dt
        return dt.replace(tzinfo=UTC).astimezone().replace(tzinfo=None)

    # If tz-aware, normalize to local and drop tzinfo
    return dt.astimezone().replace(tzinfo=None)


def get_video_metadata(path: Path) -> Tuple[Optional[datetime], Optional[float], Optional[str]]:
    if MediaInfo is None:
        logging.info("pymediainfo not installed; skipping video metadata for %s", path)
        return None, None, None

    try:
        media_info = MediaInfo.parse(path)
    except Exception as e:
        logging.warning("MediaInfo.parse failed for %s: %s", path, e)
        return None, None, None

    dt = None
    duration_sec = None
    camera_model = None

    for track in media_info.tracks:
        if track.track_type == 'General':
            # Prefer local creation time, then UTC creation, then local/UTC modification, then legacy fields.
            date_candidates = [
                (getattr(track, 'file_creation_date__local', None), True),
                (getattr(track, 'file_creation_date', None), False),
                (getattr(track, 'file_last_modification_date__local', None), True),
                (getattr(track, 'file_last_modification_date', None), False),
                (getattr(track, 'recorded_date', None), False),
                (getattr(track, 'encoded_date', None), False),
                (getattr(track, 'tagged_date', None), False),
            ]
            for raw_dt, is_local_hint in date_candidates:
                dt = _parse_mediainfo_datetime(raw_dt, is_local_hint)
                if dt:
                    break
            duration_ms = getattr(track, 'duration', None)
            if duration_ms is not None:
                duration_sec = float(duration_ms) / 1000.0
            # Extract camera/device model from performer or encoder fields
            camera_model = getattr(track, 'performer', None) or getattr(track, 'encoder', None)
        
    if dt is None and duration_sec is None:
        logging.info("No usable video metadata found for %s; will fall back to filesystem time.", path)
    return dt, duration_sec, camera_model


def compute_phash(path: Path) -> Optional[str]:
    if imagehash is None:
        return None
    try:
        with Image.open(path) as im:
            h = imagehash.phash(im)
        return str(h)
    except Exception as e:
        logging.warning("pHash computation failed for %s: %s", path, e)
        return None


def get_image_size(path: Path) -> Tuple[Optional[int], Optional[int]]:
    try:
        with Image.open(path) as im:
            return im.width, im.height
    except Exception as e:
        logging.warning("Failed to get image size for %s: %s", path, e)
        return None, None


def normalize_stem_for_grouping(stem: str) -> str:
    """Normalize filename stem for grouping/resized logic."""
    s = stem.lower().strip()

    # remove common suffixes
    s = re.sub(r'\(copy\)$', '', s)
    s = re.sub(r'_copy$', '', s)
    s = re.sub(r'_edit$', '', s)
    s = re.sub(r'-edit$', '', s)
    s = re.sub(r'\(\d+\)$', '', s)
    s = s.strip('_- ')

    return s


# ---------------------- DB HELPERS ----------------------

def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        hash            TEXT NOT NULL UNIQUE,
        type            TEXT NOT NULL CHECK (type IN ('raw','jpeg','video','psd','sidecar','tiff','other')),
        ext             TEXT NOT NULL,
        orig_name       TEXT NOT NULL,
        orig_path       TEXT NOT NULL,
        dest_path       TEXT,
        size_bytes      INTEGER,
        is_seed         INTEGER NOT NULL DEFAULT 0,
        name_score      INTEGER NOT NULL DEFAULT 0,
        first_seen_at   TEXT NOT NULL,
        last_seen_at    TEXT NOT NULL
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS media_metadata (
        file_id         INTEGER PRIMARY KEY,
        capture_datetime TEXT,
        camera_model    TEXT,
        lens_model      TEXT,
        width           INTEGER,
        height          INTEGER,
        duration_sec    REAL,
        aspect_ratio    REAL,
        phash           TEXT,
        FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_sidecars (
        raw_file_id      INTEGER NOT NULL,
        sidecar_file_id  INTEGER NOT NULL,
        PRIMARY KEY (raw_file_id, sidecar_file_id),
        FOREIGN KEY(raw_file_id) REFERENCES files(id) ON DELETE CASCADE,
        FOREIGN KEY(sidecar_file_id) REFERENCES files(id) ON DELETE CASCADE
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_outputs (
        raw_file_id      INTEGER NOT NULL,
        output_file_id   INTEGER NOT NULL,
        link_method      TEXT,
        confidence       INTEGER,
        PRIMARY KEY (raw_file_id, output_file_id),
        FOREIGN KEY(raw_file_id) REFERENCES files(id) ON DELETE CASCADE,
        FOREIGN KEY(output_file_id) REFERENCES files(id) ON DELETE CASCADE
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS psd_source_links (
        psd_file_id      INTEGER PRIMARY KEY,
        source_file_id   INTEGER NOT NULL,
        confidence       INTEGER NOT NULL CHECK (confidence BETWEEN 0 AND 100),
        link_method      TEXT NOT NULL,
        linked_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(psd_file_id) REFERENCES files(id) ON DELETE CASCADE,
        FOREIGN KEY(source_file_id) REFERENCES files(id) ON DELETE CASCADE
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_type ON files(type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_media_capture_dt ON media_metadata(capture_datetime);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_outputs_raw ON raw_outputs(raw_file_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_outputs_out ON raw_outputs(output_file_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_psd_source_links_source ON psd_source_links(source_file_id);")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS file_occurrences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT NOT NULL,
        path TEXT NOT NULL,
        is_seed INTEGER NOT NULL DEFAULT 0,
        seen_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_occurrences_hash ON file_occurrences(hash);")
    conn.commit()

def assign_sidecar_destinations(conn: sqlite3.Connection):
    """
    Assign dest_path to sidecars based on their linked RAW files.
    Each sidecar inherits the destination folder of its RAW, with the sidecar's original extension.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT rs.sidecar_file_id, f.orig_name, r.dest_path
        FROM raw_sidecars rs
        JOIN files f ON rs.sidecar_file_id = f.id
        JOIN files r ON rs.raw_file_id = r.id
        WHERE f.dest_path IS NULL AND r.dest_path IS NOT NULL
    """)
    rows = cur.fetchall()
    
    for sidecar_id, sidecar_name, raw_dest_path in rows:
        if not raw_dest_path:
            continue
        
        # Place sidecar in the same folder as the RAW, preserving sidecar's original name
        raw_dest = Path(raw_dest_path)
        sidecar_dest = raw_dest.parent / sidecar_name
        
        conn.execute(
            "UPDATE files SET dest_path = ? WHERE id = ?",
            (str(sidecar_dest), sidecar_id),
        )
    
    conn.commit()


def find_psd_source_by_stem(psd_name: str, source_names: List[str]) -> bool:
    """
    Try to match a PSD to a source file by normalized stem.
    Removes common PSD suffixes like -edit, -final, _v2, (copy), etc.
    Returns True if found, False otherwise.
    """
    # Extract stem without extension
    psd_stem = Path(psd_name).stem
    psd_stem = normalize_stem_for_grouping(psd_stem).lower()
    
    # Remove common PSD suffixes
    psd_stem = re.sub(r'[-_](edit|final|v\d+|copy|variant|retouched|working)$', '', psd_stem)
    psd_stem = re.sub(r'\s*\(\d+\)$', '', psd_stem)
    
    for src_name in source_names:
        src_stem = Path(src_name).stem
        src_stem = normalize_stem_for_grouping(src_stem).lower()
        if src_stem == psd_stem:
            return True
    
    return False


def extract_psd_source_references(psd_path: Path) -> List[str]:
    """
    Extract referenced filenames from PSD smart objects.
    Returns list of filenames (e.g., ['photo.jpg', 'background.tif']).
    Logs warnings on parse failures, returns empty list on error.
    """
    referenced_files = []
    
    # Skip if file doesn't exist (e.g., in tests with fake paths)
    if not psd_path.exists():
        return referenced_files
    
    try:
        psd = PSDImage.open(psd_path)
        
        def walk_layers(layer):
            """Recursively walk layer tree to find smart objects."""
            if hasattr(layer, 'smart_object') and layer.smart_object:
                so = layer.smart_object
                if hasattr(so, 'filename') and so.filename:
                    referenced_files.append(so.filename)
            
            # Recurse into group layers
            if hasattr(layer, '__iter__'):
                try:
                    for child in layer:
                        walk_layers(child)
                except Exception:
                    pass
        
        for layer in psd:
            walk_layers(layer)
    
    except Exception as e:
        logging.debug(f"Failed to parse PSD smart objects from {psd_path}: {e}")
    
    return referenced_files


def link_psds_to_sources(conn: sqlite3.Connection):
    """
    Link PSD files to source images (RAW/JPEG) using multi-phase matching.
    
    Supports linking to:
    - RAW files (always scanned as sources)
    - JPEG files (scanned as sources if from --seed-output with pre-organized outputs)
    
    Phase 1: Stem matching (confidence=100)
    Phase 2: Smart object parsing + stem match (confidence=95)
    Only stores links with confidence >= 95.
    """
    cur = conn.cursor()
    
    # Get all PSDs
    cur.execute("SELECT id, orig_name, orig_path FROM files WHERE type='psd'")
    psd_records = [(row[0], row[1], Path(row[2])) for row in cur.fetchall()]
    
    # Get all source files (RAW/JPEG) - includes JPEGs from both seed outputs and source scans
    cur.execute("SELECT id, orig_name FROM files WHERE type IN ('raw', 'jpeg')")
    source_records = cur.fetchall()
    
    for psd_id, psd_name, psd_path in psd_records:
        best_match_id = None
        best_confidence = 0
        best_method = None
        
        # Phase 1: Stem matching
        for src_id, src_name in source_records:
            if find_psd_source_by_stem(psd_name, [src_name]):
                best_match_id = src_id
                best_confidence = 100
                best_method = "stem"
                break
        
        # Phase 2: Smart object parsing
        if best_confidence < 95:
            try:
                referenced = extract_psd_source_references(psd_path)
                for ref_filename in referenced:
                    ref_stem = normalize_stem_for_grouping(ref_filename).lower()
                    for src_id, src_name in source_records:
                        src_stem = normalize_stem_for_grouping(src_name).lower()
                        if src_stem == ref_stem:
                            best_match_id = src_id
                            best_confidence = 95
                            best_method = "smart_object"
                            break
                    if best_match_id:
                        break
            except Exception as e:
                logging.debug(f"Smart object linking failed for {psd_name}: {e}")
        
        # Store link if confidence >= 95
        if best_match_id and best_confidence >= 95:
            conn.execute(
                """INSERT OR REPLACE INTO psd_source_links 
                   (psd_file_id, source_file_id, confidence, link_method) 
                   VALUES (?, ?, ?, ?)""",
                (psd_id, best_match_id, best_confidence, best_method),
            )
    
    conn.commit()


def assign_psd_destinations(conn: sqlite3.Connection):
    """
    Assign dest_path to PSDs based on their linked source files.
    Each linked PSD inherits the destination folder of its source, with the PSD's original filename.
    Unlinked PSDs are assigned to output/YYYY/YYYY-MM/unlinked-psds/ based on capture_datetime.
    """
    cur = conn.cursor()
    used_names = defaultdict(set)
    
    # Get all PSD destination assignments from linked sources
    cur.execute("""
        SELECT p.id, p.orig_name, s.dest_path, m.capture_datetime
        FROM files p
        LEFT JOIN psd_source_links psl ON p.id = psl.psd_file_id
        LEFT JOIN files s ON psl.source_file_id = s.id
        LEFT JOIN media_metadata m ON p.id = m.file_id
        WHERE p.type='psd' AND p.dest_path IS NULL
    """)
    psd_rows = cur.fetchall()
    
    for psd_id, psd_name, source_dest, capture_dt in psd_rows:
        if source_dest:
            # Linked: place in source folder
            source_dest_path = Path(source_dest)
            psd_dest_dir = source_dest_path.parent
            
            # Handle name collisions
            psd_dest_path = psd_dest_dir / psd_name
            base_stem = Path(psd_name).stem
            ext = Path(psd_name).suffix
            counter = 1
            
            while str(psd_dest_path) in used_names[str(psd_dest_dir)]:
                psd_dest_path = psd_dest_dir / f"{base_stem} ({counter}){ext}"
                counter += 1
            
            used_names[str(psd_dest_dir)].add(str(psd_dest_path))
            
            conn.execute(
                "UPDATE files SET dest_path = ? WHERE id = ?",
                (str(psd_dest_path), psd_id),
            )
        else:
            # Unlinked: place in output/YYYY/YYYY-MM/unlinked-psds/
            dt = datetime.fromisoformat(capture_dt) if capture_dt else None
            if dt is None:
                # Fallback to mtime
                cur.execute("SELECT orig_path FROM files WHERE id = ?", (psd_id,))
                orig_path_row = cur.fetchone()
                if orig_path_row:
                    try:
                        dt = datetime.fromtimestamp(
                            Path(orig_path_row[0]).stat().st_mtime, tz=UTC
                        )
                    except Exception:
                        dt = datetime.now(UTC)
                else:
                    dt = datetime.now(UTC)
            
            year = dt.year
            month = dt.month
            unlinked_dir = f"output/{year}/{year:04d}-{month:02d}/unlinked-psds"
            psd_dest_path = Path(unlinked_dir) / psd_name
            
            # Handle collision for unlinked PSDs
            base_stem = Path(psd_name).stem
            ext = Path(psd_name).suffix
            counter = 1
            
            while str(psd_dest_path) in used_names[unlinked_dir]:
                psd_dest_path = Path(unlinked_dir) / f"{base_stem} ({counter}){ext}"
                counter += 1
            
            used_names[unlinked_dir].add(str(psd_dest_path))
            
            conn.execute(
                "UPDATE files SET dest_path = ? WHERE id = ?",
                (str(psd_dest_path), psd_id),
            )
    
    conn.commit()


def upsert_file_record(conn: sqlite3.Connection, rec: FileRecord) -> int:
    """
    Insert or update canonical file row for a given hash.
    Returns file_id.
    """
    now_iso = datetime.now(UTC).isoformat()
    cur = conn.cursor()
    cur.execute("SELECT id, is_seed, name_score FROM files WHERE hash = ?", (rec.hash,))
    row = cur.fetchone()

    file_id: int  # explicitly declare

    if row is None:
        # No existing row: insert
        cur.execute(
            """
            INSERT INTO files (
                hash, type, ext, orig_name, orig_path, size_bytes,
                is_seed, name_score, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rec.hash,
                rec.type,
                rec.ext,
                rec.orig_name,
                str(rec.orig_path),
                rec.size_bytes,
                int(rec.is_seed),
                rec.name_score,
                now_iso,
                now_iso,
            ),
        )
        rid = cur.lastrowid
        # Help Pylance: assert this is not None
        assert rid is not None, "lastrowid should not be None after INSERT"
        file_id = int(rid)
    else:
        # Existing row: decide whether to update canonical info
        raw_file_id, existing_seed, existing_score = row
        file_id = int(raw_file_id)
        existing_seed_int = int(existing_seed)
        new_seed = int(rec.is_seed)

        update_canonical = False
        if new_seed > existing_seed_int:
            update_canonical = True
        elif new_seed == existing_seed_int and rec.name_score > int(existing_score):
            update_canonical = True

        if update_canonical:
            cur.execute(
                """
                UPDATE files
                SET orig_name = ?, orig_path = ?, is_seed = ?, name_score = ?, last_seen_at = ?
                WHERE id = ?
                """,
                (
                    rec.orig_name,
                    str(rec.orig_path),
                    new_seed,
                    rec.name_score,
                    now_iso,
                    file_id,
                ),
            )
        else:
            cur.execute(
                "UPDATE files SET last_seen_at = ? WHERE id = ?",
                (now_iso, file_id),
            )

    
    return file_id


def upsert_media_metadata(conn: sqlite3.Connection, file_id: int, rec: FileRecord):
    cur = conn.cursor()
    cur.execute("SELECT file_id FROM media_metadata WHERE file_id = ?", (file_id,))
    row = cur.fetchone()
    capture_str = rec.capture_datetime.isoformat() if rec.capture_datetime else None
    aspect_ratio = None
    if rec.width and rec.height:
        try:
            aspect_ratio = rec.width / rec.height
        except ZeroDivisionError:
            aspect_ratio = None

    if row is None:
        cur.execute("""
            INSERT INTO media_metadata
            (file_id, capture_datetime, camera_model, lens_model, width, height,
             duration_sec, aspect_ratio, phash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            file_id, capture_str, rec.camera_model, rec.lens_model, rec.width, rec.height,
            rec.duration_sec, aspect_ratio, rec.phash
        ))
    else:
        cur.execute("""
            UPDATE media_metadata
            SET capture_datetime = ?, camera_model = ?, lens_model = ?, width = ?, height = ?,
                duration_sec = ?, aspect_ratio = ?, phash = ?
            WHERE file_id = ?
        """, (
            capture_str, rec.camera_model, rec.lens_model, rec.width, rec.height,
            rec.duration_sec, aspect_ratio, rec.phash, file_id
        ))
    


# ---------------------- SCANNING ----------------------

def classify_extension(path: Path) -> Optional[str]:
    #ignore AppleDouble / dot-underscore files from macOS
    if path.name.startswith("._"):
        return "other"


    ext = path.suffix.lower()
    if ext in RAW_EXTS:
        return "raw"
    if ext in JPEG_EXTS:
        return "jpeg"
    if ext in VIDEO_EXTS:
        return "video"
    if ext in PSD_EXTS:
        return "psd"
    if ext in TIFF_EXTS:
        return "tiff"
    if ext in SIDECAR_EXTS:
        return "sidecar"
    # everything else is "other" so we still catalog it
    return "other"


def gather_file_record(path: Path, ftype: str, is_seed: bool, use_phash: bool) -> FileRecord:
    ext = path.suffix.lower()
    orig_name = path.name
    size_bytes = path.stat().st_size

    capture_dt = None
    camera_model = None
    lens_model = None
    width = height = None
    duration_sec = None
    phash_str = None

    if ftype in ("raw", "jpeg", "psd", "tiff"):
        with open(path, "rb") as f:
            capture_dt, camera_model, lens_model = get_image_metadata_exif(path, fileobj=f)

            if capture_dt is None:
                inferred = infer_datetime_from_path(path)
                if inferred is not None:
                    logging.info(
                        "Using folder-inferred datetime %s for %s",
                        inferred.isoformat(),
                        path,
                    )
                    capture_dt = inferred
                else:
                    logging.info("Using filesystem mtime as capture_datetime for %s", path)
                    capture_dt = fallback_file_datetime(path)

            f.seek(0)
            hash_str = compute_file_hash_from_handle(f)

        if ftype in ("jpeg", "psd", "tiff"):
            width, height = get_image_size(path)
            if use_phash and ftype in ("jpeg", "tiff"):
                phash_str = compute_phash(path)

    elif ftype == "video":
        capture_dt, duration_sec, camera_model = get_video_metadata(path)

        if capture_dt is None:
            inferred = infer_datetime_from_path(path)
            if inferred is not None:
                logging.info(
                    "Using folder-inferred datetime %s for video %s",
                    inferred.isoformat(),
                    path,
                )
                capture_dt = inferred
            else:
                logging.info(
                    "Using filesystem mtime as capture_datetime for video %s",
                    path,
                )
                capture_dt = fallback_file_datetime(path)

        hash_str = compute_file_hash(path)

    else:
        # sidecar / other: just use filesystem time
        capture_dt = fallback_file_datetime(path)
        hash_str = compute_file_hash(path)

    if capture_dt is None:
        # Very defensive; in practice we'll have set it above
        capture_dt = fallback_file_datetime(path)
    name_score = descriptiveness_score(path.stem)

    return FileRecord(
        hash=hash_str,
        type=ftype,
        ext=ext,
        orig_name=orig_name,
        orig_path=path,
        size_bytes=size_bytes,
        is_seed=is_seed,
        name_score=name_score,
        capture_datetime=capture_dt,
        camera_model=camera_model,
        lens_model=lens_model,
        width=width,
        height=height,
        duration_sec=duration_sec,
        phash=phash_str
    )


def iter_files_scandir(root: Path, skip_dest: Optional[Path] = None) -> Iterable[Path]:
    """Depth-first traversal using scandir for fewer syscalls; yields files in stable order."""
    stack = [root]
    while stack:
        current = stack.pop()
        if skip_dest and (current == skip_dest or skip_dest in current.parents):
            continue
        try:
            with os.scandir(current) as it:
                entries = [entry for entry in it]
        except FileNotFoundError:
            continue

        entries.sort(key=lambda e: e.name.lower())
        dirs = [Path(e.path) for e in entries if e.is_dir(follow_symlinks=False)]
        files = [Path(e.path) for e in entries if e.is_file(follow_symlinks=False)]

        for d in reversed(dirs):
            stack.append(d)
        for f in files:
            if skip_dest and (f == skip_dest or skip_dest in f.parents):
                continue
            yield f


def scan_tree(conn: sqlite3.Connection, root: Path, is_seed: bool, use_phash: bool, skip_dest: Optional[Path] = None, max_workers: int = 2):
    logging.info(f"Scanning {'seed' if is_seed else 'source'}: {root}")
    all_files: List[Path] = list(iter_files_scandir(root, skip_dest=skip_dest))

    BATCH_SIZE = 200  # tweak as you like
    processed = 0
    raw_sidecar_index: Dict[Tuple[Path, str], Dict[str, List[int]]] = defaultdict(lambda: {"raw": [], "sidecar": []})

    conn.execute("BEGIN")

    def process_path(path: Path) -> Optional[FileRecord]:
        ftype = classify_extension(path)
        if not ftype:
            return None
        try:
            return gather_file_record(path, ftype, is_seed, use_phash)
        except Exception as e:
            logging.exception("Error processing file %s: %s", path, e)
            return None

    max_workers = max(1, min(max_workers, 8))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for rec in tqdm(pool.map(process_path, all_files), total=len(all_files), desc=f"Scanning {'seed' if is_seed else 'src'}"):
            if rec is None:
                continue
            file_id = upsert_file_record(conn, rec)
            if rec.type in ("raw", "jpeg", "video", "psd", "tiff"):
                upsert_media_metadata(conn, file_id, rec)
            conn.execute(
                "INSERT INTO file_occurrences (hash, path, is_seed) VALUES (?, ?, ?)",
                (rec.hash, str(rec.orig_path), int(is_seed)),
            )

            if rec.type in ("raw", "sidecar"):
                key = (rec.orig_path.parent, rec.orig_path.stem.lower())
                raw_sidecar_index[key][rec.type].append(file_id)

            processed += 1
            if processed % BATCH_SIZE == 0:
                conn.commit()
                conn.execute("BEGIN")

    conn.commit()
    return raw_sidecar_index


def merge_raw_sidecar_indices(indices: List[Dict[Tuple[Path, str], Dict[str, List[int]]]]) -> Dict[Tuple[Path, str], Dict[str, List[int]]]:
    merged: Dict[Tuple[Path, str], Dict[str, List[int]]] = defaultdict(lambda: {"raw": [], "sidecar": []})
    for idx in indices:
        for key, val in idx.items():
            merged[key]["raw"].extend(val.get("raw", []))
            merged[key]["sidecar"].extend(val.get("sidecar", []))
    return merged


def link_raw_sidecars_from_index(conn: sqlite3.Connection, raw_sidecar_index: Dict[Tuple[Path, str], Dict[str, List[int]]]):
    for key, val in tqdm(raw_sidecar_index.items(), desc="Linking sidecars"):
        raw_ids = val.get("raw") or []
        sidecar_ids = val.get("sidecar") or []
        if not raw_ids or not sidecar_ids:
            continue
        for raw_id in raw_ids:
            for sidecar_id in sidecar_ids:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO raw_sidecars (raw_file_id, sidecar_file_id)
                    VALUES (?, ?)
                    """,
                    (raw_id, sidecar_id),
                )
    conn.commit()


# ---------------------- ORGANIZING FILES ----------------------

def decide_dest_for_file(conn: sqlite3.Connection, dest_root: Path):
    """
    Compute dest_path for each file (if not already set), according to type and capture date.
    JPEGs handled with grouping for main vs resized; TIFF treated as output like video.
    PSDs are excluded here; their destinations are assigned by assign_psd_destinations() based on linking.
    Sidecars are assigned destinations via assign_sidecar_destinations() after RAWs are processed.
    "other" type files are not assigned dest_path (no copying).
    """
    cur = conn.cursor()
    used_names: Dict[Path, set] = defaultdict(set)

    # 1) RAW, video, TIFF: simple pass (JPEG via grouping; PSD handled separately by assign_psd_destinations)
    cur.execute("""
        SELECT f.id, f.hash, f.type, f.orig_name, f.orig_path, f.dest_path, m.capture_datetime
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type IN ('raw','video','tiff')
    """)
    rows = cur.fetchall()
    for file_id, _, ftype, orig_name, orig_path, dest_path, capture_str in rows:
        if dest_path:
            continue  # already set

        if capture_str:
            dt = datetime.fromisoformat(capture_str)
        else:
            # Fall back to the file's own filesystem timestamp
            dt = fallback_file_datetime(Path(orig_path))

        year = dt.year
        month = dt.month
        year_month_folder = FOLDER_PATTERN.format(year=year, month=month)

        if ftype == "raw":
            base = dest_root / "raw"
        else:  # video, tiff
            base = dest_root / "output"

        dest_dir = base / year_month_folder

        # Format: <orig_stem>_YYYY-MM-DD_HH-MM-SS<ext>
        dt_str = dt.strftime("%Y-%m-%d_%H-%M-%S")
        orig_path_obj = Path(orig_name)
        stem = orig_path_obj.stem
        ext = orig_path_obj.suffix  # includes the dot, e.g. ".CR2"
        new_name = f"{stem}_{dt_str}{ext}"
        candidate = dest_dir / new_name
        counter = 1
        while candidate.name in used_names[dest_dir]:
            candidate = dest_dir / f"{stem}_{dt_str}_{counter}{ext}"
            counter += 1
        used_names[dest_dir].add(candidate.name)

        conn.execute("UPDATE files SET dest_path = ? WHERE id = ?", (str(candidate), file_id))
    conn.commit()

    # 2) JPEGs: grouping for main vs resized
    cur.execute("""
        SELECT f.id, f.orig_name, f.orig_path, m.capture_datetime, m.width, m.height
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type = 'jpeg'
    """)
    rows = cur.fetchall()

    groups: Dict[Tuple[str, Optional[str]], List[Tuple[int, str, Path, Optional[str], Optional[int], Optional[int]]]] = {}

    for file_id, orig_name, orig_path, capture_str, w, h in rows:
        if capture_str:
            dt = datetime.fromisoformat(capture_str)
        else:
            # Again, fall back to file's own timestamp if metadata is missing
            dt = fallback_file_datetime(Path(orig_path))

        dt_key = dt.replace(microsecond=0).isoformat()
        norm_stem = normalize_stem_for_grouping(Path(orig_name).stem)
        key = (norm_stem, dt_key)
        groups.setdefault(key, []).append((file_id, orig_name, Path(orig_path), capture_str, w, h))

    for key, items in groups.items():
        # Find main (largest resolution) JPEG
        best_item = None
        best_pixels = -1
        for file_id, orig_name, orig_path, capture_str, w, h in items:
            pixels = (w or 0) * (h or 0)
            if pixels > best_pixels:
                best_pixels = pixels
                best_item = (file_id, orig_name, orig_path, capture_str, w, h)

        if best_item is None:
            continue

        for file_id, orig_name, orig_path, capture_str, w, h in items:
            if capture_str:
                dt = datetime.fromisoformat(capture_str)
            else:
                dt = fallback_file_datetime(Path(orig_path))

            year = dt.year
            month = dt.month
            year_month_folder = FOLDER_PATTERN.format(year=year, month=month)
            dest_dir = dest_root / "output" / year_month_folder
            # Format: <orig_stem>[_resized_WxH]_YYYY-MM-DD_HH-MM-SS<ext>
            dt_str = dt.strftime("%Y-%m-%d_%H-%M-%S")
            orig_path_obj = Path(orig_name)
            stem = orig_path_obj.stem
            ext = orig_path_obj.suffix  # e.g. ".jpg"

            # Decide filename pattern
            if (file_id, orig_name, orig_path, capture_str, w, h) == best_item:
                # main version
                new_name = f"{stem}_{dt_str}{ext}"
            else:
                # resized version
                if w and h:
                    new_name = f"{stem}_resized_{w}x{h}_{dt_str}{ext}"
                else:
                    new_name = f"{stem}_resized_{dt_str}{ext}"


            base_stem = Path(new_name).stem
            candidate = dest_dir / new_name
            counter = 1
            while candidate.name in used_names[dest_dir]:
                candidate = dest_dir / f"{base_stem}_{counter}{ext}"
                counter += 1
            used_names[dest_dir].add(candidate.name)

            conn.execute("UPDATE files SET dest_path = ? WHERE id = ?", (str(candidate), file_id))

    conn.commit()


def copy_or_move_files(conn: sqlite3.Connection, move: bool, dry_run: bool):
    cur = conn.cursor()
    cur.execute("SELECT id, orig_path, dest_path, type FROM files WHERE dest_path IS NOT NULL")
    rows = cur.fetchall()
    for file_id, orig_path, dest_path, ftype in tqdm(rows, desc="Copying/moving"):
        src = Path(orig_path)
        dest = Path(dest_path)
        if dest.exists():
            continue
        if dry_run:
            logging.info(f"[DRY RUN] {'Move' if move else 'Copy'} {src} -> {dest}")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            if move:
                shutil.move(str(src), str(dest))
            else:
                shutil.copy2(str(src), str(dest))
        except Exception as e:
            logging.exception(f"Error copying {src} -> {dest}: {e}")


# ---------------------- RAW SIDECARS ----------------------

def link_raw_sidecars(conn: sqlite3.Connection):
    """
    Link already-scanned sidecars to raws based on matching stem + directory.
    """
    cur = conn.cursor()
    cur.execute("SELECT id, orig_path FROM files WHERE type = 'raw'")
    raws = [(rid, Path(rpath)) for rid, rpath in cur.fetchall()]

    cur.execute("SELECT id, orig_path FROM files WHERE type = 'sidecar'")
    sidecars = defaultdict(list)
    for sid, spath in cur.fetchall():
        sp = Path(spath)
        sidecars[(sp.parent, sp.stem.lower())].append(sid)

    for raw_id, raw_path in tqdm(raws, desc="Linking sidecars"):
        key = (raw_path.parent, raw_path.stem.lower())
        if key not in sidecars:
            continue
        for sidecar_id in sidecars[key]:
            conn.execute(
                """
                INSERT OR IGNORE INTO raw_sidecars (raw_file_id, sidecar_file_id)
                VALUES (?, ?)
                """,
                (raw_id, sidecar_id),
            )
    conn.commit()


# ---------------------- RAW-OUTPUT LINKING ----------------------

def build_raw_output_links(conn: sqlite3.Connection, use_phash: bool):
    """
    Simple metadata-based linking:
      - Pass 1: filename + exact capture time
      - Pass 2: capture time window + camera model
      - Pass 3: optional pHash similarity (JPEG/TIFF) [limited]
    """
    cur = conn.cursor()
    conn.execute("DELETE FROM raw_outputs")
    conn.commit()

    # Load RAWs
    cur.execute("""
        SELECT f.id, f.orig_name, m.capture_datetime, m.camera_model
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type = 'raw'
    """)
    raw_rows = cur.fetchall()

    # Load outputs (jpeg, psd, video, tiff)
    cur.execute("""
        SELECT f.id, f.orig_name, m.capture_datetime, m.camera_model, m.phash
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type IN ('jpeg','psd','video','tiff')
    """)
    out_rows = cur.fetchall()

    # Index outputs by exact capture time for pass 1
    out_by_time: Dict[str, List[Tuple[int, str, Optional[str], Optional[str], Optional[str]]]] = {}
    for out_id, out_name, capture_str, cam_model, phash_str in out_rows:
        if not capture_str:
            continue
        key = capture_str  # exact second
        out_by_time.setdefault(key, []).append((out_id, out_name, capture_str, cam_model, phash_str))

    # Pass 1: filename core + exact capture time
    for raw_id, raw_name, capture_str, raw_cam in tqdm(raw_rows, desc="Linking RAW->outputs (pass1)"):
        if not capture_str:
            continue
        key = capture_str
        if key not in out_by_time:
            continue
        raw_core = re.sub(r'\D', '', Path(raw_name).stem)
        for out_id, out_name, _, out_cam, _ in out_by_time[key]:
            out_core = re.sub(r'\D', '', Path(out_name).stem)
            if raw_core and raw_core == out_core:
                conn.execute("""
                    INSERT OR IGNORE INTO raw_outputs (raw_file_id, output_file_id, link_method, confidence)
                    VALUES (?, ?, ?, ?)
                """, (raw_id, out_id, 'filename_time', 100))
    conn.commit()

    # Pass 2: time + camera model (±2 seconds)
    outs_for_pass2 = []
    for out_id, out_name, capture_str, out_cam, phash_str in out_rows:
        if not capture_str:
            continue
        outs_for_pass2.append((out_id, out_name, datetime.fromisoformat(capture_str), out_cam, phash_str))

    for raw_id, raw_name, capture_str, raw_cam in tqdm(raw_rows, desc="Linking RAW->outputs (pass2)"):
        if not capture_str or not raw_cam:
            continue
        raw_dt = datetime.fromisoformat(capture_str)
        t_min = raw_dt.timestamp() - 2
        t_max = raw_dt.timestamp() + 2
        for out_id, out_name, out_dt, out_cam, _ in outs_for_pass2:
            if not out_cam:
                continue
            if out_cam != raw_cam:
                continue
            ts = out_dt.timestamp()
            if t_min <= ts <= t_max:
                conn.execute("""
                    INSERT OR IGNORE INTO raw_outputs (raw_file_id, output_file_id, link_method, confidence)
                    VALUES (?, ?, ?, ?)
                """, (raw_id, out_id, 'time_camera', 90))
    conn.commit()

    # Pass 3: pHash (optional, JPEG/TIFF only; note we don't yet compute RAW phashes)
    if use_phash and imagehash is not None:
        # Map outputs with phash and capture time
        outs_with_phash = [
            (out_id, out_name, capture_str, cam, phash_str)
            for out_id, out_name, capture_str, cam, phash_str in out_rows
            if phash_str
        ]

        # For each RAW, compare against outputs in ±30s window using pHash
        for raw_id, raw_name, capture_str, raw_cam in tqdm(raw_rows, desc="Linking RAW->outputs (pass3-phash)"):
            if not capture_str:
                continue
            raw_dt = datetime.fromisoformat(capture_str)
            t_min = raw_dt.timestamp() - 30
            t_max = raw_dt.timestamp() + 30

            for out_id, out_name, out_capture_str, out_cam, out_phash in outs_with_phash:
                if not out_capture_str:
                    continue
                out_dt = datetime.fromisoformat(out_capture_str)
                ts = out_dt.timestamp()
                if ts < t_min or ts > t_max:
                    continue
                try:
                    raw_h = imagehash.hex_to_hash(out_phash)  # NOTE: currently limited: we don't have RAW phash
                    out_h = imagehash.hex_to_hash(out_phash)
                    dist = raw_h - out_h
                except Exception:
                    continue
                if dist <= 5:
                    conn.execute("""
                        INSERT OR IGNORE INTO raw_outputs (raw_file_id, output_file_id, link_method, confidence)
                        VALUES (?, ?, ?, ?)
                    """, (raw_id, out_id, 'phash', 70))
        conn.commit()


# ---------------------- REPORTS ----------------------

def export_unprocessed_raws(conn: sqlite3.Connection, out_csv: Path):
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.dest_path, m.capture_datetime, m.camera_model
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type = 'raw'
    """)
    raws = cur.fetchall()

    cur.execute("SELECT DISTINCT raw_file_id FROM raw_outputs")
    linked_raw_ids = {row[0] for row in cur.fetchall()}

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, 'w', encoding='utf-8') as f:
        f.write("raw_file_id,raw_path,capture_datetime,camera_model,has_output\n")
        for raw_id, dest_path, capture_str, cam in raws:
            has_output = raw_id in linked_raw_ids
            if not has_output:
                f.write(f"{raw_id},{dest_path or ''},{capture_str or ''},{(cam or '').replace(',', ' ')},{has_output}\n")


def export_unknown_files(conn: sqlite3.Connection, out_csv: Path):
    """
    Export all files with type='other' so you can see which extensions/paths
    weren't handled by RAW/JPEG/VIDEO/PSD/TIFF/SIDECAR.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT id, ext, orig_path, size_bytes, is_seed, first_seen_at, last_seen_at
        FROM files
        WHERE type = 'other'
    """)
    rows = cur.fetchall()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, 'w', encoding='utf-8') as f:
        f.write("file_id,ext,orig_path,size_bytes,is_seed,first_seen_at,last_seen_at\n")
        for fid, ext, orig_path, size_bytes, is_seed, first_seen, last_seen in rows:
            f.write(f"{fid},{ext},{orig_path},{size_bytes or 0},{is_seed},{first_seen},{last_seen}\n")


def write_copy_report(conn: sqlite3.Connection, out_csv: Path):
    """
    Emit a CSV with per-file status across seed/source scans.

    Columns: path,type,is_seed,status,dest_path,duplicate_of,hash
    Status values:
      - copied: canonical file with a destination
      - duplicate: same hash as canonical, different source path; duplicate_of points to canonical dest_path
      - skipped_other: type 'other' (never copied)
      - pending: canonical without dest_path (should be rare)
    """
    cur = conn.cursor()
    cur.execute("SELECT hash, type, dest_path, orig_path FROM files")
    file_info = {
        row[0]: {
            "type": row[1],
            "dest_path": row[2],
            "orig_path": row[3],
        }
        for row in cur.fetchall()
    }

    cur.execute("SELECT hash, path, is_seed FROM file_occurrences")
    occurrences = cur.fetchall()

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["path", "type", "is_seed", "status", "dest_path", "duplicate_of", "hash"])

        for h, path, is_seed in occurrences:
            info = file_info.get(h)
            if not info:
                writer.writerow([path, "", bool(is_seed), "unknown", "", "", h])
                continue

            ftype = info["type"]
            dest = info["dest_path"] or ""
            canonical_path = info["orig_path"]

            if ftype == "other":
                status = "skipped_other"
                dup_of = ""
            elif path != canonical_path:
                status = "duplicate"
                dup_of = dest
            else:
                status = "copied" if dest else "pending"
                dup_of = ""

            writer.writerow([path, ftype, bool(is_seed), status, dest, dup_of, h])


def export_unlinked_psds(conn: sqlite3.Connection, out_csv: Path):
    """
    Export all PSDs that were not linked to any source image.
    Useful for manual review and post-processing workflow.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.orig_name, f.orig_path, f.dest_path, m.capture_datetime, m.camera_model
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type = 'psd' AND f.id NOT IN (
            SELECT DISTINCT psd_file_id FROM psd_source_links WHERE confidence >= 95
        )
    """)
    rows = cur.fetchall()
    
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, 'w', encoding='utf-8') as f:
        f.write("psd_id,psd_name,orig_path,dest_path,capture_datetime,camera_model\n")
        for psd_id, psd_name, orig_path, dest_path, capture_dt, camera_model in rows:
            # Escape commas in paths
            orig_path_safe = (orig_path or '').replace(',', ' ')
            dest_path_safe = (dest_path or '').replace(',', ' ')
            camera_safe = (camera_model or '').replace(',', ' ')
            f.write(f"{psd_id},{psd_name},{orig_path_safe},{dest_path_safe},{capture_dt or ''},{camera_safe}\n")


# ---------------------- CLI / MAIN ----------------------

def parse_args():
    p = argparse.ArgumentParser(description="Organize photos (RAW, JPEG, video, TIFF, PSD) with SQLite catalog.")
    p.add_argument("src", help="Source root directory")
    p.add_argument("dest", help="Destination root directory (will contain /raw and /output)")
    p.add_argument("--seed-output", help="Optional seed output archive path", default=None)
    p.add_argument("--db", help="SQLite DB path (default: dest/photo_catalog.db)", default=None)
    p.add_argument("--move", action="store_true", help="Move files instead of copying")
    p.add_argument("--dry-run", action="store_true", help="Don't actually copy/move files")
    p.add_argument("--use-phash", action="store_true", help="Compute pHash for JPEG/TIFF and use it for lineage")
    p.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Max threads for scanning (bounded internally, default: 2)"
    )
    p.add_argument(
        "--copy-report",
        type=Path,
        default=None,
        help="Optional path to write per-file copy report CSV"
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging instead of INFO"
    )
    return p.parse_args()


def main():
    args = parse_args()
    src_root = Path(args.src).resolve()
    dest_root = Path(args.dest).resolve()
    dest_root.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db) if args.db else dest_root / "photo_catalog.db"

    # Choose log level based on --verbose flag
    log_level = logging.DEBUG if args.verbose else logging.INFO

    logging.basicConfig(
        filename=dest_root / "organizer.log",
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())

    # Turn down exifread's own chatter (it logs "File format not recognized")
    exif_logger = logging.getLogger("exifread")
    exif_logger.setLevel(logging.ERROR)

    conn = sqlite3.connect(db_path)

    # Speed-boost pragmas (acceptable for a rebuildable catalog)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")  # ~200MB cache; tweak if you like

    init_db(conn)
    # Per-run occurrence log: clear any prior scan entries
    conn.execute("DELETE FROM file_occurrences;")
    conn.commit()

    seed_sidecar_index: Dict[Tuple[Path, str], Dict[str, List[int]]] = defaultdict(lambda: {"raw": [], "sidecar": []})

    # Seed scan (outputs first, if provided)
    if args.seed_output:
        seed_root = Path(args.seed_output).resolve()
        seed_sidecar_index = scan_tree(conn, seed_root, is_seed=True, use_phash=args.use_phash, skip_dest=None, max_workers=args.max_workers)

    # Main source scan
    src_sidecar_index = scan_tree(conn, src_root, is_seed=False, use_phash=args.use_phash, skip_dest=dest_root, max_workers=args.max_workers)

    # Link RAW sidecars using in-memory index from both scans
    combined_sidecars = merge_raw_sidecar_indices([seed_sidecar_index, src_sidecar_index])
    link_raw_sidecars_from_index(conn, combined_sidecars)

    # Link PSDs to source images
    link_psds_to_sources(conn)

    # Decide destination paths
    decide_dest_for_file(conn, dest_root)

    # Assign sidecar destinations to match their RAW files
    assign_sidecar_destinations(conn)

    # Assign PSD destinations (linked PSDs follow sources, unlinked go to unlinked-psds/)
    assign_psd_destinations(conn)

    # Copy/move files
    copy_or_move_files(conn, move=args.move, dry_run=args.dry_run)

    # Link RAW -> outputs
    build_raw_output_links(conn, use_phash=args.use_phash)

    # Export reports
    export_unprocessed_raws(conn, dest_root / "unprocessed_raws.csv")
    export_unknown_files(conn, dest_root / "unknown_files.csv")
    export_unlinked_psds(conn, dest_root / "unlinked_psds.csv")
    if args.copy_report:
        write_copy_report(conn, args.copy_report)

    conn.close()
    logging.info("Done.")


if __name__ == "__main__":
    main()
