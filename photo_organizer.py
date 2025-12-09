#!/usr/bin/env python

import argparse
import hashlib
import logging
import os
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import exifread
from PIL import Image
from tqdm import tqdm

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
JPEG_EXTS = {'.jpg', '.jpeg', '.jpe'}
VIDEO_EXTS = {'.mp4', '.mov', '.m4v', '.avi', '.mts', '.m2ts', '.3gp', '.mpg', '.mpeg'}
PSD_EXTS = {'.psd', '.psb'}
TIFF_EXTS = {'.tif', '.tiff'}

SIDECAR_EXTS = {'.xmp', '.vrd', '.dop', '.dpp', '.pp3'}

DATE_TAGS = [
    'EXIF DateTimeOriginal',
    'EXIF DateTimeDigitized',
    'Image DateTime',
]

FOLDER_PATTERN = "{year}/{year}-{month:02d}"
FILENAME_PATTERN = "{dt}_{orig}"

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


def compute_file_hash(path: Path, chunk_size: int = 1_048_576) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fallback_file_datetime(path: Path) -> datetime:
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts)


def parse_exif_datetime(dt_str: str) -> Optional[datetime]:
    # EXIF often: "YYYY:MM:DD HH:MM:SS"
    try:
        dt_str = str(dt_str)
        dt_str = dt_str.replace(':', '-', 2)
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_image_metadata_exif(path: Path) -> Tuple[Optional[datetime], Optional[str], Optional[str]]:
    """Use exifread to get datetime, camera, lens (if any)."""
    try:
        with open(path, 'rb') as f:
            tags = exifread.process_file(f, details=False)
    except Exception as e:
        logging.warning("EXIF read failed for %s: %s", path, e)
        return None, None, None

    if not tags:
        # exifread returned no tags at all
        logging.info("No EXIF tags found for %s", path)

    dt = None
    for tag in DATE_TAGS:
        if tag in tags:
            dt = parse_exif_datetime(tags[tag])
            if dt:
                break

    if dt is None:
        logging.info(
            "No EXIF date found for %s (tags tried: %s); will fall back to filesystem time.",
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



def get_video_metadata(path: Path) -> Tuple[Optional[datetime], Optional[float]]:
    if MediaInfo is None:
        logging.info("pymediainfo not installed; skipping video metadata for %s", path)
        return None, None

    try:
        media_info = MediaInfo.parse(path)
    except Exception as e:
        logging.warning("MediaInfo.parse failed for %s: %s", path, e)
        return None, None

    dt = None
    duration_sec = None

    for track in media_info.tracks:
        if track.track_type == 'General':
            # encoded_date / tagged_date are often available
            date_str = getattr(track, 'encoded_date', None) or getattr(track, 'tagged_date', None)
            if date_str:
                parts = date_str.split(' ')
                for i in range(len(parts)):
                    try_str = ' '.join(parts[i:])
                    try:
                        dt = datetime.fromisoformat(try_str)
                        break
                    except Exception:
                        try:
                            dt = datetime.strptime(try_str, "%Y-%m-%d %H:%M:%S")
                            break
                        except Exception:
                            pass
                if dt:
                    break
            duration_ms = getattr(track, 'duration', None)
            if duration_ms is not None:
                duration_sec = float(duration_ms) / 1000.0
        
    if dt is None and duration_sec is None:
        logging.info("No usable video metadata found for %s; will fall back to filesystem time.", path)
    return dt, duration_sec


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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_type ON files(type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_media_capture_dt ON media_metadata(capture_datetime);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_outputs_raw ON raw_outputs(raw_file_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_outputs_out ON raw_outputs(output_file_id);")
    conn.commit()


def upsert_file_record(conn: sqlite3.Connection, rec: FileRecord) -> int:
    """
    Insert or update canonical file row for a given hash.
    Returns file_id.
    """
    now_iso = datetime.utcnow().isoformat()
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
        capture_dt, camera_model, lens_model = get_image_metadata_exif(path)
        if capture_dt is None:
            logging.info("Using filesystem mtime as capture_datetime for %s", path)
            capture_dt = fallback_file_datetime(path)
        if ftype in ("jpeg", "psd", "tiff"):
            width, height = get_image_size(path)
            if use_phash and ftype in ("jpeg", "tiff"):
                phash_str = compute_phash(path)
    elif ftype == "video":
        capture_dt, duration_sec = get_video_metadata(path)
        if capture_dt is None:
            logging.info("Using filesystem mtime as capture_datetime for video %s", path)
            capture_dt = fallback_file_datetime(path)
    else:
        # sidecar / other: just use filesystem time
        capture_dt = fallback_file_datetime(path)

    if capture_dt is None:
        capture_dt = fallback_file_datetime(path)

    hash_str = compute_file_hash(path)
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


def scan_tree(conn: sqlite3.Connection, root: Path, is_seed: bool, use_phash: bool, skip_dest: Optional[Path] = None):
    logging.info(f"Scanning {'seed' if is_seed else 'source'}: {root}")
    all_files: List[Path] = []
    for dirpath, _, filenames in os.walk(root):
        d = Path(dirpath)
        if skip_dest and skip_dest in d.parents:
            continue
        for name in filenames:
            all_files.append(d / name)

    BATCH_SIZE = 200  # tweak as you like
    processed = 0

    # Start a transaction
    conn.execute("BEGIN")

    for path in tqdm(all_files, desc=f"Scanning {'seed' if is_seed else 'src'}"):
        ftype = classify_extension(path)
        if not ftype:
            continue
        try:
            rec = gather_file_record(path, ftype, is_seed, use_phash)
        except Exception as e:
            logging.exception(f"Error processing file {path}: {e}")
            continue

        file_id = upsert_file_record(conn, rec)
        # Only media-ish types get metadata rows
        if ftype in ("raw", "jpeg", "video", "psd", "tiff"):
            upsert_media_metadata(conn, file_id, rec)

        processed += 1
        if processed % BATCH_SIZE == 0:
            conn.commit()
            conn.execute("BEGIN")

    conn.commit()


# ---------------------- ORGANIZING FILES ----------------------

def decide_dest_for_file(conn: sqlite3.Connection, dest_root: Path):
    """
    Compute dest_path for each file (if not already set), according to type and capture date.
    JPEGs handled with grouping for main vs resized; TIFF treated as output like video.
    "other" and "sidecar" are not assigned dest_path (no copying) directly here.
    """
    cur = conn.cursor()

    # 1) RAW, video, PSD, TIFF: simple pass (JPEG via grouping)
    cur.execute("""
        SELECT f.id, f.hash, f.type, f.orig_name, f.orig_path, f.dest_path, m.capture_datetime
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type IN ('raw','video','psd','tiff')
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
        else:  # video, psd, tiff
            base = dest_root / "output"

        dest_dir = base / year_month_folder
        if ftype == "psd":
            dest_dir = dest_dir / "psd"

        dest_dir.mkdir(parents=True, exist_ok=True)
        dt_str = dt.strftime("%Y%m%d_%H%M%S")
        new_name = FILENAME_PATTERN.format(dt=dt_str, orig=orig_name)
        candidate = dest_dir / new_name
        counter = 1
        while candidate.exists():
            stem = candidate.stem
            suffix = candidate.suffix
            candidate = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

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
            dest_dir.mkdir(parents=True, exist_ok=True)
            dt_str = dt.strftime("%Y%m%d_%H%M%S")

            # Decide filename pattern
            if (file_id, orig_name, orig_path, capture_str, w, h) == best_item:
                # main
                new_name = FILENAME_PATTERN.format(dt=dt_str, orig=orig_name)
            else:
                # resized
                if w and h:
                    resized_prefix = f"resized_{w}x{h}_"
                else:
                    resized_prefix = "resized_"
                new_name = f"{dt_str}_{resized_prefix}{orig_name}"

            candidate = dest_dir / new_name
            counter = 1
            while candidate.exists():
                stem = candidate.stem
                suffix = candidate.suffix
                candidate = dest_dir / f"{stem}_{counter}{suffix}"
                counter += 1

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
    For each raw file, look for sidecars based on same stem in same original directory,
    add them as files (type=sidecar) and link via raw_sidecars.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT id, orig_path FROM files WHERE type = 'raw'
    """)
    raws = cur.fetchall()
    for raw_id, raw_orig_path in tqdm(raws, desc="Linking sidecars"):
        raw_path = Path(raw_orig_path)
        stem = raw_path.stem
        dir_path = raw_path.parent
        for ext in SIDECAR_EXTS:
            candidate = dir_path / f"{stem}{ext}"
            if not candidate.exists():
                continue
            rec = FileRecord(
                hash=compute_file_hash(candidate),
                type='sidecar',
                ext=ext,
                orig_name=candidate.name,
                orig_path=candidate,
                size_bytes=candidate.stat().st_size,
                is_seed=False,
                name_score=descriptiveness_score(candidate.stem)
            )
            sidecar_id = upsert_file_record(conn, rec)
            conn.execute("""
                INSERT OR IGNORE INTO raw_sidecars (raw_file_id, sidecar_file_id)
                VALUES (?, ?)
            """, (raw_id, sidecar_id))
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
    return p.parse_args()


def main():
    args = parse_args()
    src_root = Path(args.src).resolve()
    dest_root = Path(args.dest).resolve()
    dest_root.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db) if args.db else dest_root / "photo_catalog.db"

    logging.basicConfig(
        filename=dest_root / "organizer.log",
        level=logging.INFO,
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

    # Seed scan (outputs first, if provided)
    if args.seed_output:
        seed_root = Path(args.seed_output).resolve()
        scan_tree(conn, seed_root, is_seed=True, use_phash=args.use_phash, skip_dest=None)

    # Main source scan
    scan_tree(conn, src_root, is_seed=False, use_phash=args.use_phash, skip_dest=dest_root)

    # Link RAW sidecars
    link_raw_sidecars(conn)

    # Decide destination paths
    decide_dest_for_file(conn, dest_root)

    # Copy/move files
    copy_or_move_files(conn, move=args.move, dry_run=args.dry_run)

    # Link RAW -> outputs
    build_raw_output_links(conn, use_phash=args.use_phash)

    # Export reports
    export_unprocessed_raws(conn, dest_root / "unprocessed_raws.csv")
    export_unknown_files(conn, dest_root / "unknown_files.csv")

    conn.close()
    logging.info("Done.")


if __name__ == "__main__":
    main()
