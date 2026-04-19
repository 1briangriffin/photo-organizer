import shutil
import logging
from pathlib import Path
from typing import Dict

from tqdm import tqdm
from ..database.ops import DBOperations


class FileMover:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops

    def execute(self, move_mode: bool = False, dry_run: bool = False) -> Dict[str, int]:
        """
        Reads pending moves from DB and applies them.

        Returns a counts dict for caller-side telemetry (pipeline stats):
          planned   — tasks selected after the already-exists filter
          skipped   — tasks discarded because dest already existed (idempotency)
          moved     — successful real-run moves (0 on dry_run)
          copied    — successful real-run copies (0 on dry_run)
          errors    — tasks that raised during copy/move
        """
        tasks = self.db.get_pending_moves()

        to_process = []
        skipped = 0
        for file_id, src, dest, _, _, _ in tasks:
            if Path(dest).exists():
                skipped += 1
                continue
            to_process.append((file_id, src, dest))

        counts: Dict[str, int] = {
            "planned": len(to_process),
            "skipped": skipped,
            "moved": 0,
            "copied": 0,
            "errors": 0,
        }

        if not to_process:
            logging.info("No files need moving.")
            return counts

        logging.info(f"Processing {len(to_process)} files (Move={move_mode}, DryRun={dry_run})...")

        for file_id, src_str, dest_str in tqdm(to_process, desc="Organizing"):
            src = Path(src_str)
            dest = Path(dest_str)

            if dry_run:
                logging.info(f"[DRY RUN] {'Move' if move_mode else 'Copy'} {src} -> {dest}")
                continue

            try:
                dest.parent.mkdir(parents=True, exist_ok=True)

                if move_mode:
                    shutil.move(str(src), str(dest))
                    counts["moved"] += 1
                else:
                    shutil.copy2(str(src), str(dest))
                    counts["copied"] += 1

                try:
                    dest_stat = dest.stat()
                    cur = self.db.conn.cursor()
                    cur.execute(
                        "SELECT hash, sparse_hash, is_seed, size_bytes FROM files WHERE id = ?",
                        (file_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        full_hash, sparse_hash, is_seed, size_bytes = row
                        hash_value = full_hash or sparse_hash
                        if hash_value:
                            self.db.record_occurrence(
                                file_id=file_id,
                                path=dest,
                                is_seed=bool(is_seed),
                                mtime=dest_stat.st_mtime,
                                size_bytes=size_bytes or dest_stat.st_size,
                                hash_value=hash_value,
                                is_sparse=full_hash is None,
                            )
                except Exception as record_err:
                    logging.debug(f"Failed to record occurrence for {dest}: {record_err}")
            except Exception as e:
                logging.error(f"Failed to process {src} -> {dest}: {e}")
                counts["errors"] += 1

        return counts
