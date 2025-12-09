#!/usr/bin/env python

import argparse
import sqlite3
from pathlib import Path
from typing import Optional


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")
    return sqlite3.connect(db_path)


def list_unprocessed_raws(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.dest_path, m.capture_datetime, m.camera_model
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.type = 'raw'
        ORDER BY m.capture_datetime
    """)
    raws = cur.fetchall()

    cur.execute("SELECT DISTINCT raw_file_id FROM raw_outputs")
    linked_raw_ids = {row[0] for row in cur.fetchall()}

    print("Unprocessed RAW files (no linked outputs):")
    print("raw_id | capture_datetime        | camera_model              | dest_path")
    print("-------+--------------------------+---------------------------+----------")
    for raw_id, dest_path, capture_str, cam in raws:
        if raw_id in linked_raw_ids:
            continue
        print(f"{raw_id:6d} | {(capture_str or '').ljust(24)} | {(cam or '').ljust(25)} | {dest_path or ''}")


def _resolve_raw_id_from_path(conn: sqlite3.Connection, path: Path) -> Optional[int]:
    cur = conn.cursor()
    candidates = {str(path), path.as_posix()}
    # Try exact match on dest_path first, then on orig_path
    for cand in candidates:
        cur.execute("SELECT id FROM files WHERE dest_path = ? AND type = 'raw'", (cand,))
        row = cur.fetchone()
        if row:
            return row[0]
    for cand in candidates:
        cur.execute("SELECT id FROM files WHERE orig_path = ? AND type = 'raw'", (cand,))
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def show_raw_details(conn: sqlite3.Connection, raw_id: int):
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.orig_path, f.dest_path, m.capture_datetime, m.camera_model, m.lens_model
        FROM files f
        LEFT JOIN media_metadata m ON f.id = m.file_id
        WHERE f.id = ? AND f.type = 'raw'
    """, (raw_id,))
    row = cur.fetchone()
    if not row:
        print(f"No RAW with id={raw_id}")
        return

    fid, orig_path, dest_path, capture_str, cam, lens = row
    print("RAW file:")
    print(f"  id:            {fid}")
    print(f"  orig_path:     {orig_path}")
    print(f"  dest_path:     {dest_path}")
    print(f"  capture_time:  {capture_str}")
    print(f"  camera_model:  {cam}")
    print(f"  lens_model:    {lens}")

    # Linked outputs
    cur.execute("""
        SELECT o.id, o.type, o.orig_path, o.dest_path, mm.capture_datetime, ro.link_method, ro.confidence
        FROM raw_outputs ro
        JOIN files o ON ro.output_file_id = o.id
        LEFT JOIN media_metadata mm ON o.id = mm.file_id
        WHERE ro.raw_file_id = ?
        ORDER BY mm.capture_datetime
    """, (raw_id,))
    outs = cur.fetchall()

    if not outs:
        print("  (No linked outputs)")
        return

    print("\n  Linked outputs:")
    print("  id   | type  | link_method   | conf | capture_datetime        | dest_path")
    print("  -----+-------+---------------+------+--------------------------+----------")
    for oid, otype, o_orig, o_dest, o_dt, method, conf in outs:
        print(f"  {oid:4d} | {otype.ljust(5)} | {(method or '').ljust(13)} | {str(conf or '').rjust(4)} | {(o_dt or '').ljust(24)} | {o_dest or o_orig}")


def list_unknown_files(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, ext, orig_path, size_bytes, is_seed, first_seen_at, last_seen_at
        FROM files
        WHERE type = 'other'
        ORDER BY ext, orig_path
    """)
    rows = cur.fetchall()
    if not rows:
        print("No files with type='other' found.")
        return

    print("Files with unhandled type='other':")
    print("id   | ext   | size_bytes | seed | first_seen           | last_seen            | orig_path")
    print("-----+-------+------------+------+----------------------+----------------------+----------")
    for fid, ext, orig_path, size_bytes, is_seed, first_seen, last_seen in rows:
        print(f"{fid:4d} | {ext.ljust(5)} | {str(size_bytes or 0).rjust(10)} | {int(is_seed)}    | {first_seen or ''} | {last_seen or ''} | {orig_path}")


def parse_args():
    p = argparse.ArgumentParser(description="Query helper for photo_catalog SQLite DB.")
    p.add_argument("--db", required=True, help="Path to photo_catalog.db (typically under your dest root)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--unprocessed-raws", action="store_true", help="List RAW files with no linked outputs")
    group.add_argument("--raw-id", type=int, help="Show details and outputs for a RAW by id")
    group.add_argument("--raw-path", help="Show details and outputs for a RAW by dest/orig path")
    group.add_argument("--unknown-files", action="store_true", help="List files of type='other'")
    return p.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db).resolve()
    conn = connect_db(db_path)

    try:
        if args.unprocessed_raws:
            list_unprocessed_raws(conn)
        elif args.raw_id is not None:
            show_raw_details(conn, args.raw_id)
        elif args.raw_path:
            raw_path = Path(args.raw_path).resolve()
            raw_id = _resolve_raw_id_from_path(conn, raw_path)
            if raw_id is None:
                print(f"No RAW found for path: {raw_path}")
            else:
                show_raw_details(conn, raw_id)
        elif args.unknown_files:
            list_unknown_files(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
