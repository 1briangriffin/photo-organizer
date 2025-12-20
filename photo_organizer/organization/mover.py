import shutil
import logging
from pathlib import Path
from tqdm import tqdm
from ..database.ops import DBOperations

class FileMover:
    def __init__(self, db_ops: DBOperations):
        self.db = db_ops

    def execute(self, move_mode: bool = False, dry_run: bool = False):
        """
        Reads pending moves from DB and applies them.
        """
        tasks = self.db.get_pending_moves()
        
        # Filter out files that already exist at destination (idempotency)
        # logic: if dest exists, we assume it's done or requires manual intervention
        to_process = []
        for file_id, src, dest, _, _, _ in tasks:
            if not Path(dest).exists():
                to_process.append((file_id, src, dest))

        if not to_process:
            logging.info("No files need moving.")
            return

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
                else:
                    shutil.copy2(str(src), str(dest))

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
