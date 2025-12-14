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
        for src, dest, _ in tasks:
            if not Path(dest).exists():
                to_process.append((src, dest))

        if not to_process:
            logging.info("No files need moving.")
            return

        logging.info(f"Processing {len(to_process)} files (Move={move_mode}, DryRun={dry_run})...")
        
        for src_str, dest_str in tqdm(to_process, desc="Organizing"):
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
            except Exception as e:
                logging.error(f"Failed to process {src} -> {dest}: {e}")