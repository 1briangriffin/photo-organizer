"""Offline face annotation tool — Phase 0B (see PLAN_face_phase0a_prereg.md).

Collects clean, independently-produced face labels for the age-banded template
spike, WITHOUT touching production identity state.

Design constraints, all load-bearing:

  * The catalog is opened READ-ONLY. Labels go to a separate sidecar SQLite
    file. Nothing is ever written to face_person_links / face_detections.
  * Photo-first. The whole photo is shown with every detected face boxed,
    because recognition context (who else is present, whose event this is)
    lives in the photo — not in a cropped face.
  * One click = one assertion about one visible face. There is deliberately no
    "label the whole cluster" action: that is a compound, unverified claim.
  * Candidate names are filtered by date eligibility — a person who was not yet
    born cannot be offered. The date gate, delivered to the human.
  * Clusters appear only as an optional, collapsed hint. Never a default.
  * Every label is correctable and retractable in place, and every change is
    appended to an audit log.

Run:
    uv run streamlit run tools/face_annotate.py -- \
        --db  D:\\Organized_Images\\snapshot_20260731.db \
        --out D:\\Organized_Images\\face_annotations.db
"""
from __future__ import annotations

import argparse
import base64
import calendar
import io
import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

BANDS = [(0, 0.5), (0.5, 1), (1, 2), (2, 4), (4, 7), (7, 11),
         (11, 14), (14, 18), (18, 25), (25, 40), (40, 55), (55, 70), (70, 200)]
TARGET_PER_CELL = 6
# This is a human-review surface, so expose every detection the catalog chose
# to retain.  Production clustering deliberately uses the stricter 0.7 working
# floor to suppress pareidolia, but applying that floor here also hides real,
# difficult faces (especially infants).  The annotator can explicitly mark
# false positives as "not a face", so the catalog's 0.5 recording floor is the
# appropriate threshold.
MIN_ANNOTATION_DET_SCORE = 0.5

# --- family snapshot vs. crowd -------------------------------------------
# Face count alone is a poor proxy for "how many of OUR people are here".
# Dance-recital shots hold one or two family members among a dozen
# classmates, so scoring them by raw face count wildly overstates their
# coverage. Measured on this catalog: median mean-face-size falls from 220px
# (1 face) to 79px (5-8 faces) to ~60px (17+). Distance, not head count, is
# what separates a family snapshot from a crowd.
MAX_USEFUL_FACES = 8       # beyond this, extra faces are rarely family
CROWD_FACES = 12           # above this, assume a crowd
CROWD_FACE_PX = 70         # mean face smaller than this => shot at distance
CROWD_CELL_CAP = 2         # a crowd realistically yields 1-2 of our people
CLOSE_FACE_PX = 150        # mean face at/above this = close family shot

V_PERSON = "person"
V_NOT_A_FACE = "not_a_face"
V_DEPICTION = "depiction"
V_NOT_A_PERSON = "not_a_person"
V_UNKNOWN = "unknown_person"

SKIP = "— skip —"
NOT_A_FACE = "(not a face — detector error)"
DEPICTION = "(photo of a photo / screen / mirror)"
NOT_A_PERSON = "(doll / statue)"
UNKNOWN = "(a person, but not one of ours)"
OTHER = "(someone else on the roster…)"


# --------------------------------------------------------------------------
# args
# --------------------------------------------------------------------------
@st.cache_resource
def get_args():
    argv = sys.argv[1:]
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True, help="catalog snapshot (read-only)")
    p.add_argument("--out", required=True, help="sidecar annotations DB")
    p.add_argument("--config", default=str(REPO_ROOT / "faces_birth_intervals.json"))
    p.add_argument("--annotator", default="user")
    known, _ = p.parse_known_args(argv)
    return known


# --------------------------------------------------------------------------
# storage
# --------------------------------------------------------------------------
@st.cache_resource
def catalog(db_path: str) -> sqlite3.Connection:
    """Read-only. mode=ro makes accidental writes impossible, not merely rude."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True,
                           check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@st.cache_resource
def sidecar(out_path: str, db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(out_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS annotations (
            detection_id INTEGER PRIMARY KEY,
            file_id      INTEGER NOT NULL,
            verdict      TEXT    NOT NULL,
            person_id    INTEGER,
            person_name  TEXT,
            status       TEXT    NOT NULL DEFAULT 'active',
            sampled_for  TEXT,
            annotator    TEXT,
            note         TEXT,
            created_at   TEXT    NOT NULL,
            updated_at   TEXT    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ann_person ON annotations(person_id);
        CREATE INDEX IF NOT EXISTS idx_ann_status ON annotations(status);

        -- Append-only. Retraction and correction leave a trail; nothing is
        -- ever silently overwritten.
        CREATE TABLE IF NOT EXISTS annotation_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            detection_id  INTEGER NOT NULL,
            action        TEXT    NOT NULL,
            old_verdict   TEXT,
            old_person_id INTEGER,
            new_verdict   TEXT,
            new_person_id INTEGER,
            annotator     TEXT,
            at            TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS session_meta (
            key TEXT PRIMARY KEY, value TEXT
        );
        """
    )
    # Geometry columns: a label keyed only on detection_id dangles the moment
    # the catalog is rescanned, because invalidate_detections_for_files DELETES
    # detections rather than superseding them. Storing file_id + the
    # detect-space bbox lets labels be re-matched to new detections by IoU, so
    # a rescan (e.g. to lower MIN_FACE_SIZE) costs no human work. This is the
    # miniature form of the supersession reconciliation the plan calls for.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(annotations)")}
    for col in ("bbox_x", "bbox_y", "bbox_w", "bbox_h"):
        if col not in cols:
            conn.execute(f"ALTER TABLE annotations ADD COLUMN {col} REAL")

    orphans = conn.execute(
        "SELECT detection_id FROM annotations WHERE bbox_x IS NULL"
    ).fetchall()
    if orphans:
        cat = catalog(db_path)
        filled = 0
        for (did,) in orphans:
            row = cat.execute(
                "SELECT bbox_x, bbox_y, bbox_w, bbox_h FROM face_detections "
                "WHERE id = ?", (did,),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE annotations SET bbox_x=?, bbox_y=?, bbox_w=?, "
                    "bbox_h=? WHERE detection_id=?", (*row, did),
                )
                filled += 1
        if filled:
            conn.execute(
                "INSERT OR REPLACE INTO session_meta(key, value) "
                "VALUES ('bbox_backfilled', ?)", (str(filled),),
            )

    for key, value in (
        ("catalog_snapshot", db_path),
        ("opened_at", datetime.now().isoformat(timespec="seconds")),
        # Detect-space bboxes are only comparable across scans while this is
        # unchanged; record it so a future rematch can detect a mismatch.
        ("max_detection_dimension", str(_max_detection_dimension())),
    ):
        conn.execute(
            "INSERT OR REPLACE INTO session_meta(key, value) VALUES (?, ?)",
            (key, value),
        )
    conn.commit()
    return conn


def _max_detection_dimension() -> int:
    from photo_organizer.faces import config as fconfig
    return fconfig.MAX_DETECTION_DIMENSION


def write_label(sc, *, detection_id, file_id, verdict, person_id, person_name,
                sampled_for, annotator, bbox=None, note=None):
    now = datetime.now().isoformat(timespec="seconds")
    prev = sc.execute(
        "SELECT verdict, person_id FROM annotations WHERE detection_id = ?",
        (detection_id,),
    ).fetchone()
    bx, by, bw, bh = bbox if bbox else (None, None, None, None)
    sc.execute(
        """
        INSERT INTO annotations (detection_id, file_id, verdict, person_id,
                                 person_name, status, sampled_for, annotator,
                                 note, bbox_x, bbox_y, bbox_w, bbox_h,
                                 created_at, updated_at)
        VALUES (?,?,?,?,?, 'active', ?,?,?,?,?,?,?,?,?)
        ON CONFLICT(detection_id) DO UPDATE SET
            verdict=excluded.verdict, person_id=excluded.person_id,
            person_name=excluded.person_name, status='active',
            note=excluded.note, updated_at=excluded.updated_at,
            bbox_x=COALESCE(excluded.bbox_x, annotations.bbox_x),
            bbox_y=COALESCE(excluded.bbox_y, annotations.bbox_y),
            bbox_w=COALESCE(excluded.bbox_w, annotations.bbox_w),
            bbox_h=COALESCE(excluded.bbox_h, annotations.bbox_h)
        """,
        (detection_id, file_id, verdict, person_id, person_name,
         json.dumps(sampled_for) if sampled_for else None, annotator, note,
         bx, by, bw, bh, now, now),
    )
    sc.execute(
        """INSERT INTO annotation_log (detection_id, action, old_verdict,
               old_person_id, new_verdict, new_person_id, annotator, at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (detection_id, "correct" if prev else "create",
         prev[0] if prev else None, prev[1] if prev else None,
         verdict, person_id, annotator, now),
    )
    sc.commit()


def retract_label(sc, detection_id, annotator):
    now = datetime.now().isoformat(timespec="seconds")
    prev = sc.execute(
        "SELECT verdict, person_id FROM annotations WHERE detection_id = ?",
        (detection_id,),
    ).fetchone()
    if not prev:
        return
    sc.execute(
        "UPDATE annotations SET status='retracted', updated_at=? WHERE detection_id=?",
        (now, detection_id),
    )
    sc.execute(
        """INSERT INTO annotation_log (detection_id, action, old_verdict,
               old_person_id, new_verdict, new_person_id, annotator, at)
           VALUES (?,'retract',?,?,NULL,NULL,?,?)""",
        (detection_id, prev[0], prev[1], annotator, now),
    )
    sc.commit()


# --------------------------------------------------------------------------
# people / bands
# --------------------------------------------------------------------------
@st.cache_data
def load_people(config_path: str) -> list[dict]:
    cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    out = []
    for p in cfg["persons"]:
        out.append({
            **p,
            "_earliest": datetime.strptime(p["earliest"], "%Y-%m-%d"),
            "_latest": datetime.strptime(p["latest"], "%Y-%m-%d"),
        })
    return out


@st.cache_data
def load_roster(db_path: str, config_path: str) -> list[dict]:
    """Every active named person in the catalog, enriched with a birth interval
    where one is known.

    Deliberately sourced from the catalog rather than the config: a person with
    no birth date must still be nameable. Missing a birth date costs banding —
    it must not cost the ability to label the face, or those people get skipped
    and their faces silently pollute someone else's cell.
    """
    intervals = {p["person_id"]: p for p in load_people(config_path)}
    rows = catalog(db_path).execute(
        """SELECT id, display_name, birth_date FROM face_persons
           WHERE status='active' AND display_name IS NOT NULL
           ORDER BY display_name"""
    ).fetchall()
    out = []
    for pid, name, birth_date in rows:
        entry = {"person_id": pid, "name": name, "confusable_set": False}
        known = intervals.get(pid)
        if known:
            entry.update(known)
        elif birth_date:
            # Config silent but the catalog has an exact date — use it.
            try:
                entry["_earliest"] = entry["_latest"] = datetime.strptime(
                    birth_date, "%Y-%m-%d")
                entry["precision"] = "day"
            except ValueError:
                pass
        out.append(entry)
    return out


def band_window(person: dict, band: tuple) -> tuple:
    """Date range in which `person` is inside `band`, widened by birth-interval
    uncertainty (earliest birth -> earliest entry; latest birth -> latest exit)."""
    lo, hi = band
    start = person["_earliest"] + timedelta(days=lo * 365.25)
    end = person["_latest"] + timedelta(days=hi * 365.25)
    return start, end


def band_of(person: dict, when: datetime):
    # Use the conservative (oldest possible) age for coarse birth precision,
    # matching band_window() and the roster's hard birth gate.  A month-known
    # birth therefore anchors on the first day of that month rather than a
    # fabricated midpoint birthday.
    age = (when - person["_earliest"]).days / 365.25
    if age < 0:
        return None
    for lo, hi in BANDS:
        if lo <= age < hi:
            return (lo, hi)
    return None


# --------------------------------------------------------------------------
# coverage + sampling
# --------------------------------------------------------------------------
def trusted_counts(sc, people) -> dict:
    """{(person_id, band): n} from the sidecar's own active person-labels."""
    by_person = {p["person_id"]: p for p in people}
    counts: dict = {}
    rows = sc.execute(
        """SELECT a.person_id, a.detection_id FROM annotations a
           WHERE a.status='active' AND a.verdict='person' AND a.person_id IS NOT NULL"""
    ).fetchall()
    if not rows:
        return counts
    return {"_raw": rows, **counts}


@st.cache_data(show_spinner="Scoring candidate photos…")
def sample_photos(db_path: str, config_path: str, cell_key: str,
                  limit: int = 60, max_faces: int = CROWD_FACES,
                  include: str = "", exclude: str = "", out_path: str = "",
                  ann_version: int = 0, years: tuple[int, ...] = (),
                  months: tuple[int, ...] = (), batch: int = 0) -> list[dict]:
    """Greedy set-cover over target (person, band) cells.

    A photo is worth showing in proportion to how many *needed* cells it can
    realistically fill — a 2009 family snapshot can hold Hannah at 7, Emma at
    4 and Ava at 1 at once. A 21-face recital from the same year holds one
    sister and twenty classmates, so its coverage is capped rather than taken
    at face value (see CROWD_* constants).
    """
    cat = catalog(db_path)
    people = load_people(config_path)
    needed = json.loads(cell_key)  # [[person_id, lo, hi, deficit], ...]
    by_id = {p["person_id"]: p for p in people}
    required = [t.strip().lower() for t in include.split(",") if t.strip()]

    windows = []
    for pid, lo, hi, deficit in needed:
        p = by_id.get(pid)
        if not p:
            continue
        s, e = band_window(p, (lo, hi))
        windows.append((pid, (lo, hi), s, e, deficit))
    if not windows and not required:
        return []

    where_extra = []
    params: list = [MIN_ANNOTATION_DET_SCORE]
    if required:
        path_expr = "LOWER(COALESCE(f.dest_path, f.orig_path))"
        where_extra.append(
            "AND (" + " OR ".join(
                f"INSTR({path_expr}, ?) > 0" for _ in required
            ) + ")"
        )
        params.extend(required)
    else:
        span_start = min(w[2] for w in windows)
        span_end = max(w[3] for w in windows)
        where_extra.append(
            "AND mm.capture_datetime >= ? AND mm.capture_datetime < ?"
        )
        params.extend((span_start.isoformat(), span_end.isoformat()))

    rows = cat.execute(
        f"""
        SELECT d.file_id,
               COALESCE(f.dest_path, f.orig_path),
               f.type,
               mm.capture_datetime,
               COUNT(*) AS faces,
               AVG(MIN(d.bbox_w, d.bbox_h)) AS mean_face_px
        FROM face_detections d
        JOIN files f ON f.id = d.file_id AND f.status='active'
        JOIN media_metadata mm ON mm.file_id = d.file_id
        WHERE d.status='observed' AND d.confidence >= ?
          {' '.join(where_extra)}
        GROUP BY d.file_id
        HAVING faces > 0
        """,
        params,
    ).fetchall()

    # Face count and size do NOT separate a recital from a family group shot
    # on this catalog — measured, both sit around 8-12 faces at ~100px. The
    # reliable discriminator is the photographer's own naming (studio and
    # event names), which only the user knows, so it is supplied rather than
    # inferred.
    blocked = [t.strip().lower() for t in exclude.split(",") if t.strip()]

    # Faces already decided must not keep pulling their photo back into the
    # queue. `ann_version` is a monotonic counter over the audit log: it
    # changes on every write, which invalidates this cache so the queue
    # reflects the labelling that just happened.
    done: dict[int, int] = {}
    if out_path:
        try:
            side = sqlite3.connect(f"file:{out_path}?mode=ro", uri=True)
            done = dict(side.execute(
                "SELECT file_id, COUNT(*) FROM annotations "
                "WHERE status='active' GROUP BY file_id"
            ).fetchall())
            side.close()
        except sqlite3.Error:
            done = {}

    scored = []
    for file_id, path, ftype, cap, faces, mean_px in rows:
        low = (path or "").lower()
        if required and not any(tok in low for tok in required):
            continue
        if blocked and not required:
            if any(tok in low for tok in blocked):
                continue
        remaining = faces - done.get(file_id, 0)
        if remaining <= 0:
            continue  # every face in this photo has been decided
        try:
            dt = datetime.fromisoformat(cap)
        except (TypeError, ValueError):
            continue
        if not required:
            if years and dt.year not in years:
                continue
            if months and dt.month not in months:
                continue
        cells = [(pid, band) for pid, band, s, e, _d in windows if s <= dt < e]
        if not cells and not required:
            continue
        mean_px = mean_px or 0.0

        # Expected coverage, not potential coverage, in two steps.
        #
        # 1. A photo with ONE face fills at most ONE cell, however many band
        #    windows its date happens to sit inside — otherwise lone portraits
        #    outrank family group shots.
        # 2. A crowd (many faces, or faces small enough to mean it was shot at
        #    distance) is overwhelmingly non-family. Recital photos hold one
        #    sister among a dozen classmates, so their coverage is capped
        #    rather than believed.
        usable = min(remaining, MAX_USEFUL_FACES)
        is_crowd = faces > max_faces or (faces > 4 and mean_px < CROWD_FACE_PX)
        if is_crowd:
            usable = min(usable, CROWD_CELL_CAP)

        deficits = sorted(
            (d for _pid, _band, s, e, d in windows if s <= dt < e),
            reverse=True,
        )
        weight = sum(deficits[:usable])
        # Proximity bonus: bigger faces mean a closer, more likely-family
        # photo, and also a face the annotator can actually recognise.
        proximity = min(mean_px / CLOSE_FACE_PX, 1.0)
        scored.append({
            "file_id": file_id, "path": path, "file_type": ftype,
            "capture": cap, "faces": faces, "remaining": remaining,
            "mean_face_px": round(mean_px),
            "crowd": is_crowd,
            "cells": [[pid, b[0], b[1]] for pid, b in cells],
            "score": weight + proximity * 2.0,
        })

    scored.sort(key=lambda r: (-r["score"], r["capture"]))

    if required:
        # Explicit browsing is allowed to show a whole matching event. The
        # normal sampler's three-per-day diversity cap would otherwise hide
        # known photos from the search results.
        spread = scored
    else:
        # Spread across days so a single event cannot dominate the sample — the
        # split protocol needs independent photo-days, not one big afternoon.
        seen_days: dict = {}
        spread = []
        for r in scored:
            day = r["capture"][:10]
            if seen_days.get(day, 0) >= 3:
                continue
            seen_days[day] = seen_days.get(day, 0) + 1
            spread.append(r)

    if not spread:
        return []

    # Re-sampling advances through the ranked candidate pool instead of merely
    # clearing a deterministic cache and reproducing the same first page.
    # Wrap so even a final short page still fills the dropdown, while exposing
    # every candidate before the cycle repeats.
    offset = (max(0, batch) * limit) % len(spread)
    return (spread[offset:] + spread[:offset])[:limit]


@st.cache_data
def available_capture_years(db_path: str) -> list[int]:
    """Calendar years having at least one photo usable by the annotator."""
    rows = catalog(db_path).execute(
        """
        SELECT DISTINCT CAST(substr(mm.capture_datetime, 1, 4) AS INTEGER)
        FROM media_metadata mm
        JOIN files f ON f.id = mm.file_id AND f.status = 'active'
        JOIN face_detections d ON d.file_id = f.id
         AND d.status = 'observed' AND d.confidence >= ?
        WHERE mm.capture_datetime IS NOT NULL
        ORDER BY 1
        """,
        (MIN_ANNOTATION_DET_SCORE,),
    ).fetchall()
    return [int(r[0]) for r in rows if r[0]]


@st.cache_data
def photo_detections(db_path: str, file_id: int) -> list[dict]:
    cat = catalog(db_path)
    rows = cat.execute(
        """SELECT id, bbox_x, bbox_y, bbox_w, bbox_h, confidence
           FROM face_detections
           WHERE file_id = ? AND status='observed' AND confidence >= ?
           ORDER BY detection_index""",
        (file_id, MIN_ANNOTATION_DET_SCORE),
    ).fetchall()
    return [{"detection_id": r[0], "bbox": (r[1], r[2], r[3], r[4]),
             "confidence": r[5]} for r in rows]


@st.cache_data
def cluster_hint(db_path: str, detection_id: int):
    """Optional, collapsed. Legacy clusters are navigation aids, never evidence."""
    cat = catalog(db_path)
    row = cat.execute(
        """SELECT m.cluster_id,
                  (SELECT COUNT(*) FROM face_cluster_members m2
                    WHERE m2.cluster_id = m.cluster_id
                      AND m2.status IN ('proposed','accepted')),
                  (SELECT p.display_name FROM face_person_links l
                     JOIN face_persons p ON p.id = l.person_id
                    WHERE l.cluster_id = m.cluster_id AND l.status='accepted'
                    LIMIT 1)
           FROM face_cluster_members m
           JOIN face_clusters c ON c.id = m.cluster_id
          WHERE m.detection_id = ? AND m.status IN ('proposed','accepted')
            AND c.status IN ('proposed','accepted')
          LIMIT 1""",
        (detection_id,),
    ).fetchone()
    return row


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------
BASE_MAX = 3200  # working resolution held in cache for redraws

# Zoom viewer. Streamlit's stylesheet caps images at the container width, so
# st.image() silently scales anything larger back down and every zoom level
# above the column width looks identical. The viewer is therefore an explicit
# <img> with max-width:none inside a scrollable box.
ZOOM_WIDTHS = {900: "Fit", 1300: "M", 1800: "L", 2400: "XL", 3200: "Max"}
ZOOM_DEFAULT = 1300
VIEWER_MAX_HEIGHT = "78vh"
VIEWER_BORDER = "1px solid rgba(128, 128, 128, 0.35)"
VIEWER_RADIUS = "6px"


@st.cache_data(max_entries=6, show_spinner="Loading photo…")
def load_photo(path: str, file_type: str, det_key: tuple):
    """Decode once. Returns (base_jpeg, base_w, base_h, box_scale, crops).

    Detection bboxes live in detect-space (the <=MAX_DETECTION_DIMENSION
    downscale detection ran on), so every coordinate is divided by
    detect_scale before use. Getting this wrong is what produced the
    'brick wall' clusters previously — crops must come from full-res pixels
    in the same upright orientation detection saw.

    The expensive part (file decode + full-res crops) happens here and is
    cached; box drawing at a given zoom level is a separate, cheap step, so
    changing zoom never re-reads the file.
    """
    from PIL import Image

    from photo_organizer.faces import config as fconfig
    from photo_organizer.faces.image_loader import load_image_as_rgb

    img = load_image_as_rgb(Path(path), file_type)
    if img is None:
        return None
    full_h, full_w = img.shape[:2]
    detect_scale = min(1.0, fconfig.MAX_DETECTION_DIMENSION / max(full_h, full_w))

    pil = Image.fromarray(img)
    crops = []
    for _did, bbox in det_key:
        x, y, w, h = (v / detect_scale for v in bbox)
        px, py = w * 0.35, h * 0.35
        crop = pil.crop((max(0, int(x - px)), max(0, int(y - py)),
                         min(full_w, int(x + w + px)),
                         min(full_h, int(y + h + py))))
        crop.thumbnail((260, 260))
        buf = io.BytesIO()
        crop.save(buf, "JPEG", quality=90)
        crops.append(buf.getvalue())

    base_scale = min(1.0, BASE_MAX / max(full_w, full_h))
    if base_scale < 1.0:
        pil = pil.resize((int(full_w * base_scale), int(full_h * base_scale)),
                         Image.LANCZOS)
    buf = io.BytesIO()
    pil.save(buf, "JPEG", quality=90)
    # detect-space bbox -> base-image coords
    return buf.getvalue(), pil.width, pil.height, base_scale / detect_scale, crops


def _label_font(size: int):
    """A scalable font so box numbers stay readable when zoomed out. PIL's
    built-in bitmap font cannot scale, which makes 21 boxes unreadable."""
    from PIL import ImageFont
    for name in ("arial.ttf", "segoeui.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _viewer_html(jpeg: bytes, width: int) -> str:
    """Scrollable, genuinely magnifying image viewer.

    `max-width: none` is the load-bearing part — it overrides Streamlit's
    default image rule, without which the browser rescales everything back to
    the column width and the zoom control appears to do nothing past ~1300px.
    """
    b64 = base64.b64encode(jpeg).decode("ascii")
    return (
        f'<div style="overflow:auto; max-height:{VIEWER_MAX_HEIGHT}; '
        f'border:{VIEWER_BORDER}; border-radius:{VIEWER_RADIUS};">'
        f'<img src="data:image/jpeg;base64,{b64}" '
        f'style="width:{width}px; max-width:none; display:block;">'
        f'</div>'
    )


@st.cache_data(max_entries=40)
def annotated_at(path: str, file_type: str, det_key: tuple, width: int):
    """Draw numbered boxes at the requested display width. Cheap: works from
    the cached base image, so zooming never touches the filesystem."""
    from PIL import Image, ImageDraw

    loaded = load_photo(path, file_type, det_key)
    if loaded is None:
        return None
    base_bytes, base_w, _base_h, box_scale, _crops = loaded

    pil = Image.open(io.BytesIO(base_bytes)).convert("RGB")
    ds = width / base_w
    if abs(ds - 1.0) > 0.01:
        pil = pil.resize((int(pil.width * ds), int(pil.height * ds)),
                         Image.LANCZOS)
    draw = ImageDraw.Draw(pil)
    lw = max(2, pil.width // 400)
    fs = max(14, pil.width // 60)
    font = _label_font(fs)

    for i, (_did, bbox) in enumerate(det_key):
        x, y, w, h = (v * box_scale * ds for v in bbox)
        draw.rectangle((x, y, x + w, y + h), outline=(255, 90, 90), width=lw)
        tag = str(i + 1)
        try:
            tw = draw.textlength(tag, font=font)
        except AttributeError:  # very old Pillow
            tw = fs * len(tag) * 0.6
        pad = max(2, lw)
        bx0, by0 = x, max(0, y - fs - pad * 2)
        draw.rectangle((bx0, by0, bx0 + tw + pad * 2, by0 + fs + pad * 2),
                       fill=(255, 90, 90))
        draw.text((bx0 + pad, by0 + pad), tag, fill=(255, 255, 255), font=font)

    buf = io.BytesIO()
    pil.save(buf, "JPEG", quality=88)
    return buf.getvalue()


# --------------------------------------------------------------------------
# app
# --------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Face Annotation (offline)", layout="wide")
    args = get_args()
    cat = catalog(args.db)
    sc = sidecar(args.out, args.db)
    people = load_people(args.config)
    confusable = [p for p in people if p.get("confusable_set")]
    by_id = {p["person_id"]: p for p in people}

    st.title("Offline face annotation")
    st.caption(
        f"Catalog **{Path(args.db).name}** opened read-only · labels → "
        f"**{Path(args.out).name}** · nothing is written to catalog identity state."
    )

    # ---- coverage ---------------------------------------------------------
    labelled = sc.execute(
        """SELECT person_id, detection_id FROM annotations
           WHERE status='active' AND verdict='person' AND person_id IS NOT NULL"""
    ).fetchall()
    det_dates = {}
    if labelled:
        ids = [str(int(d)) for _p, d in labelled]
        for did, cap in cat.execute(
            f"""SELECT d.id, mm.capture_datetime FROM face_detections d
                JOIN media_metadata mm ON mm.file_id = d.file_id
                WHERE d.id IN ({','.join(ids)})"""
        ).fetchall():
            det_dates[did] = cap

    counts: dict = {}
    for pid, did in labelled:
        p, cap = by_id.get(pid), det_dates.get(did)
        if not p or not cap:
            continue
        try:
            b = band_of(p, datetime.fromisoformat(cap))
        except ValueError:
            continue
        if b:
            counts[(pid, b)] = counts.get((pid, b), 0) + 1

    with st.sidebar:
        st.header("Cell coverage")
        st.caption(f"target ≥{TARGET_PER_CELL} per cell")
        done = total = 0
        for p in confusable:
            marks = []
            for b in BANDS:
                s, e = band_window(p, b)
                if e < datetime(2001, 1, 1) or s > datetime(2027, 1, 1):
                    continue
                n = counts.get((p["person_id"], b), 0)
                total += 1
                if n >= TARGET_PER_CELL:
                    done += 1
                    marks.append(f"**{b[0]:g}-{b[1]:g}**✓")
                elif n:
                    marks.append(f"{b[0]:g}-{b[1]:g}:{n}")
            if marks:
                st.markdown(f"**{p['name']}** · " + " · ".join(marks))
            else:
                st.markdown(f"**{p['name']}** · _none yet_")
        st.progress(done / total if total else 0.0,
                    text=f"{done}/{total} cells complete")
        st.divider()
        st.metric("labels recorded", len(labelled))

        person_options = {p["name"]: p["person_id"] for p in confusable}
        person_focus = st.multiselect(
            "Person focus",
            options=list(person_options),
            default=[],
            help="Empty = all confusable people. Select one or more people to "
                 "rank only photos that could fill their incomplete cells. "
                 "Everyone eligible for the photo remains available in the "
                 "face-label dropdowns.",
        )

        # Greedy set-cover naturally favours later years, where every sister is
        # alive and in-band at once, so four cells match one photo. Person and
        # band focus let the annotator target a specific sparse part of the
        # matrix rather than waiting for the global greedy order to reach it.
        band_focus = st.multiselect(
            "Age-band focus",
            options=[f"{lo:g}-{hi:g}" for lo, hi in BANDS],
            default=[],
            help="Empty = all bands, ranked by expected coverage. Select bands "
                 "to target them first (e.g. 0-0.5 … 4-7 for the early-"
                 "childhood cells the spike depends on).",
        )

        year_focus = st.multiselect(
            "Calendar year focus",
            options=available_capture_years(args.db),
            default=[],
            help="Empty = any year. Select one or more years to restrict the "
                 "candidate-photo queue; combines with person, age-band, and "
                 "month focus.",
        )
        month_options = {calendar.month_name[i]: i for i in range(1, 13)}
        month_focus = st.multiselect(
            "Calendar month focus",
            options=list(month_options),
            default=[],
            help="Empty = any month. Select months to target recurring events "
                 "such as December holidays or summer gatherings. When years "
                 "are also selected, both filters must match.",
        )

        # Recital and event photos hold one or two family members among a
        # dozen strangers, so their apparent coverage is misleading. Above
        # this count (or when faces are small enough to imply distance) a
        # photo is treated as a crowd and its coverage capped.
        max_faces = st.slider(
            "Treat as a crowd above N faces", min_value=4, max_value=25,
            value=CROWD_FACES, step=1,
            help=f"Crowd photos are capped at {CROWD_CELL_CAP} cell(s) of "
                 f"coverage rather than one per face, and photos whose mean "
                 f"face is under {CROWD_FACE_PX}px are treated as crowds too "
                 f"regardless of count — distance, not head count, is what "
                 f"separates a family snapshot from a recital.",
        )
        include = st.text_input(
            "Include paths containing",
            value="",
            placeholder="Christmas, DSC01021, family reunion…",
            help="Optional, comma-separated, case-insensitive filename or "
                 "path fragments. Entering a value switches to browse mode: "
                 "matching photos override person, age-band, calendar, and "
                 "exclude filters. Leave empty for normal cell sampling.",
        )
        exclude = st.text_input(
            "Exclude paths containing",
            value="KDA, Nutcracker",
            help="Comma-separated, case-insensitive substrings matched against "
                 "the file path. Recital and studio photos are statistically "
                 "indistinguishable from family group shots by face count and "
                 "size (measured: both ~8-12 faces at ~100px), so naming is "
                 "the only reliable discriminator — and only you know it. "
                 "Clear this to see everything.",
        )

        queue_filter_signature = (
            tuple(person_focus), tuple(band_focus), tuple(year_focus),
            tuple(month_focus), max_faces, include.casefold(), exclude.casefold(),
        )
        if st.session_state.get("_queue_filter_signature") != queue_filter_signature:
            st.session_state["_queue_filter_signature"] = queue_filter_signature
            st.session_state["photo_batch"] = 0
        photo_batch = int(st.session_state.get("photo_batch", 0))

        st.divider()
        if st.button(
            "↻ Different photo batch",
            help="Advance to the next group of ranked candidate photos.",
        ):
            st.session_state["photo_batch"] = photo_batch + 1
            sample_photos.clear()
            st.rerun()

    # ---- target cells -> photo queue --------------------------------------
    needed = []
    focused_person_ids = {person_options[name] for name in person_focus}
    for p in confusable:
        if focused_person_ids and p["person_id"] not in focused_person_ids:
            continue
        for b in BANDS:
            s, e = band_window(p, b)
            if e < datetime(2001, 1, 1) or s > datetime(2027, 1, 1):
                continue
            if band_focus and f"{b[0]:g}-{b[1]:g}" not in band_focus:
                continue
            deficit = TARGET_PER_CELL - counts.get((p["person_id"], b), 0)
            if deficit > 0:
                needed.append([p["person_id"], b[0], b[1], deficit])

    if not needed and not include.strip():
        st.success("Every target cell in the current focus is covered. 🎉"
                   if person_focus or band_focus
                   else "Every target cell is covered. 🎉")
        return

    ann_version = sc.execute(
        "SELECT COALESCE(MAX(id), 0) FROM annotation_log").fetchone()[0]
    photos = sample_photos(args.db, args.config, json.dumps(needed),
                           max_faces=max_faces, include=include, exclude=exclude,
                           out_path=args.out, ann_version=ann_version,
                           years=tuple(year_focus),
                           months=tuple(month_options[m] for m in month_focus),
                           batch=photo_batch)
    if not photos:
        if include.strip():
            st.warning(
                "No unlabeled catalog photos matched that path search. Check "
                "the spelling; a matching photo must have at least one "
                "qualifying detection that has not already been decided."
            )
        else:
            st.warning("No candidate photos found for the remaining cells — "
                       "those are `data_exhausted` under the current filters. "
                       "Try clearing or widening the person, age-band, "
                       "calendar, or path filters.")
        return

    labels = {
        f"{p['capture'][:10]} · {p['remaining']} of {p['faces']} face(s) left"
        f" · ~{p['mean_face_px']}px"
        f"{' · crowd' if p['crowd'] else ''} · {Path(p['path']).name}": p
        for p in photos
    }
    choice = st.selectbox("Photo", list(labels))
    photo = labels[choice]

    dets = photo_detections(args.db, photo["file_id"])
    if not dets:
        st.warning("No qualifying detections in this photo.")
        return

    det_key = tuple((d["detection_id"], d["bbox"]) for d in dets)
    loaded = load_photo(photo["path"], photo["file_type"], det_key)
    if loaded is None:
        st.error(f"Could not load {photo['path']}")
        return
    crops = loaded[4]

    # Zoom lives in session_state so it persists photo-to-photo — a group shot
    # is unreadable at a width chosen for a portrait, and re-picking every time
    # would be its own tax.
    zc1, zc2 = st.columns([3, 1])
    with zc1:
        zoom = st.select_slider(
            "Zoom",
            options=list(ZOOM_WIDTHS),
            value=st.session_state.get("zoom_width", ZOOM_DEFAULT),
            format_func=ZOOM_WIDTHS.get,
            key="zoom_width",
            help="Redraws from a cached copy — changing zoom never re-reads "
                 "the file. Above 'M' the image scrolls inside its frame.",
        )
    with zc2:
        st.caption(f"{len(dets)} face(s) · {loaded[1]}×{loaded[2]} working px")

    annotated = annotated_at(photo["path"], photo["file_type"], det_key, zoom)
    when = datetime.fromisoformat(photo["capture"])

    st.markdown(_viewer_html(annotated, zoom), unsafe_allow_html=True)
    st.caption(f"{photo['capture'][:19]} · {photo['path']}")

    # Roster for this photo. People with a known birth interval are date-gated
    # (nobody born after the photo can be in it) and shown with their age at
    # capture. People with no birth date on file cannot be gated or aged, so
    # they are always offered — excluding them would make their faces
    # unlabellable, which is worse than offering an unfiltered name.
    roster = load_roster(args.db, args.config)
    dated, undated = [], []
    for p in roster:
        if p.get("_earliest") is None:
            undated.append(p)
        elif p["_earliest"] <= when:
            dated.append((p, (when - p["_earliest"]).days / 365.25))
    dated.sort(key=lambda t: (not t[0].get("confusable_set"), t[0]["name"]))
    undated.sort(key=lambda p: p["name"])

    name_opts = {f"{p['name']}  (age ~{age:.0f})": p for p, age in dated}
    for p in undated:
        name_opts[f"{p['name']}  (no birth date)"] = p

    if undated:
        st.caption(
            f"{len(undated)} person(s) have no birth date on file "
            f"({', '.join(p['name'] for p in undated[:6])}"
            f"{'…' if len(undated) > 6 else ''}) — they are offered without an "
            f"age and cannot be date-gated. Adding their birth year to "
            f"`faces_birth_intervals.json` improves the filtering."
        )

    existing = dict(sc.execute(
        "SELECT detection_id, verdict || '|' || COALESCE(person_name,'') "
        "FROM annotations WHERE status='active' AND file_id = ?",
        (photo["file_id"],),
    ).fetchall())

    st.divider()
    with st.form(key=f"ann_{photo['file_id']}"):
        for i, det in enumerate(dets):
            did = det["detection_id"]
            c1, c2 = st.columns([1, 3])
            with c1:
                if i < len(crops):
                    st.image(crops[i], caption=f"Face {i + 1}", width=150)
            with c2:
                if did in existing:
                    verdict, nm = existing[did].split("|", 1)
                    st.success(f"Labelled: **{nm or verdict}**")
                    st.checkbox("Return this face to unlabelled",
                                key=f"retract_{did}")
                    st.caption("Or choose a different answer below to correct it.")
                st.selectbox(
                    f"Face {i + 1} is…",
                    options=[SKIP, *name_opts, UNKNOWN, NOT_A_FACE,
                             DEPICTION, NOT_A_PERSON],
                    key=f"v_{did}",
                    help="Only people already born by this photo's date are "
                         "offered. Leave on skip if you are not certain — a "
                         "guess is worse than a gap.",
                )
                hint = cluster_hint(args.db, did)
                if hint:
                    with st.expander("Clustering hint (not evidence)"):
                        st.caption(
                            f"Grouped with {hint[1]} other face(s)"
                            + (f"; that group is currently linked to "
                               f"**{hint[2]}**." if hint[2] else "; unlinked.")
                            + "  Legacy clusters are known to be contaminated — "
                              "treat this as a pointer, not an answer."
                        )
            st.divider()

        # Bulk dismissal. Unlike whole-cluster propagation this is legitimate:
        # every face it touches is visible on screen right now, so the claim
        # "none of these is anyone on our roster" is one the annotator can
        # actually verify. It still writes one atomic assertion per face, not
        # a photo-level flag, so each remains individually correctable.
        st.checkbox(
            "Everything else in this photo is a stranger / not on the roster",
            key=f"rest_unknown_{photo['file_id']}",
            help="Marks every face left on 'skip' as "
                 "'a person, but not one of ours', which retires the photo "
                 "from the queue. Faces you have set explicitly are "
                 "untouched.",
        )
        submitted = st.form_submit_button("Save this photo", type="primary")

    if not submitted:
        return

    rest_unknown = bool(
        st.session_state.get(f"rest_unknown_{photo['file_id']}"))
    saved = retracted = dismissed = 0
    for det in dets:
        did = det["detection_id"]
        if st.session_state.get(f"retract_{did}"):
            retract_label(sc, did, args.annotator)
            retracted += 1
            continue
        pick = st.session_state.get(f"v_{did}", SKIP)
        if pick == SKIP:
            if rest_unknown and did not in existing:
                write_label(sc, detection_id=did, file_id=photo["file_id"],
                            verdict=V_UNKNOWN, person_id=None,
                            person_name=None, sampled_for=photo["cells"],
                            annotator=args.annotator, bbox=det["bbox"],
                            note="bulk: rest of photo not on roster")
                dismissed += 1
            continue
        verdict, pid, pname = V_PERSON, None, None
        if pick in name_opts:
            person = name_opts[pick]
            pid, pname = person["person_id"], person["name"]
        elif pick == UNKNOWN:
            verdict = V_UNKNOWN
        elif pick == NOT_A_FACE:
            verdict = V_NOT_A_FACE
        elif pick == DEPICTION:
            verdict = V_DEPICTION
        elif pick == NOT_A_PERSON:
            verdict = V_NOT_A_PERSON
        write_label(sc, detection_id=did, file_id=photo["file_id"],
                    verdict=verdict, person_id=pid, person_name=pname,
                    sampled_for=photo["cells"], annotator=args.annotator,
                    bbox=det["bbox"])
        saved += 1

    if saved or retracted or dismissed:
        parts = [f"saved {saved} label(s)"]
        if dismissed:
            parts.append(f"dismissed {dismissed} as not-on-roster")
        if retracted:
            parts.append(f"retracted {retracted}")
        st.success("; ".join(parts).capitalize() + ".")
        sample_photos.clear()
        st.rerun()
    else:
        st.info("Nothing selected — nothing saved.")


if __name__ == "__main__":
    # `streamlit run` executes this as __main__; the guard keeps the module
    # importable for headless tests of the sampling/coverage logic.
    main()
