import argparse
import json
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from photo_organizer import init_db, scan_tree


def run_once(src: Path, skip_dest: Optional[Path], use_phash: bool, workers: int, db_dir: Optional[Path]) -> float:
    db_path: Optional[Path] = None
    conn: Optional[sqlite3.Connection] = None
    try:
        if db_dir:
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / f"bench_{uuid.uuid4().hex}.db"
            conn = sqlite3.connect(db_path)
        else:
            conn = sqlite3.connect(":memory:")

        init_db(conn)
        t0 = time.perf_counter()
        scan_tree(conn, src, is_seed=False, use_phash=use_phash, skip_dest=skip_dest, max_workers=workers)
        return time.perf_counter() - t0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass  # best effort close
        if db_path and db_path.exists():
            try:
                db_path.unlink()
            except OSError:
                pass  # best effort cleanup


def benchmark(src: Path, skip_dest: Optional[Path], use_phash: bool, workers: Iterable[int], repeats: int, db_dir: Optional[Path], out_file: Path):
    worker_list = list(workers)
    results = []
    for w in worker_list:
        warm_avg: Optional[float] = None
        times: List[float] = [run_once(src, skip_dest, use_phash, w, db_dir) for _ in range(repeats)]
        cold = times[0]
        warm_runs = times[1:]
        if warm_runs:
            warm_avg = sum(warm_runs) / len(warm_runs)
            print(f"{w} workers: {cold:.2f}s (cold), avg warm over {len(warm_runs)} runs: {warm_avg:.2f}s")
        else:
            print(f"{w} workers: {cold:.2f}s (single run)")
        results.append(
            {
                "workers": w,
                "times": times,
                "cold": cold,
                "warm_avg": warm_avg if warm_runs else None,
            }
        )

    out_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now().isoformat(),
        "src": str(src),
        "skip_dest": str(skip_dest) if skip_dest else None,
        "use_phash": use_phash,
        "repeats": repeats,
        "workers": worker_list,
        "results": results,
    }
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote results to {out_file}")


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark scan_tree with different worker counts.")
    p.add_argument("src", type=Path, help="Source root to scan")
    p.add_argument("--skip-dest", type=Path, default=None, dest="skip_dest", help="Optional path to exclude (e.g., your destination root)")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8], help="Worker counts to test")
    p.add_argument("--repeats", type=int, default=3, help="Runs per worker; first is treated as cold")
    p.add_argument("--use-phash", action="store_true", help="Enable pHash during scan (slower; off by default)")
    p.add_argument("--db-dir", type=Path, default=None, help="Directory to create per-run temp SQLite DB (use same drive as photos to measure write contention)")
    p.add_argument("--output", type=Path, default=Path("bench_scan_results.json"), help="Path to write JSON results")
    return p.parse_args()


def main():
    args = parse_args()
    benchmark(args.src, args.skip_dest, args.use_phash, args.workers, args.repeats, args.db_dir, args.output)


if __name__ == "__main__":
    main()
