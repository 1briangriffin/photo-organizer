import logging
from pathlib import Path
from typing import Set, Optional

from .database.db import DBManager
from .database.ops import DBOperations
from .scanning.filesystem import DiskScanner
from .metadata.linking import FileLinker
from .organization.rules import DestinationPlanner
from .organization.mover import FileMover
from . import config

class PhotoOrganizerApp:
    def __init__(self, db_path: Path):
        self.db_manager = DBManager(db_path)

    def organize(self,
                 src_root: Path,
                 dest_root: Path,
                 is_seed: bool = False,
                 move: bool = False,
                 dry_run: bool = False,
                 skip_dirs: Optional[Set[Path]] = None,
                 max_workers: int = 3):
        """
        Executes the 'Phase 1' Organization Pipeline.
        1. Scan & Hash (Deduplicate)
        2. Link (Sidecars/PSDs)
        3. Plan (Calculate Destinations)
        4. Execute (Copy/Move)

        Args:
            max_workers: Number of parallel workers for hashing and file operations
        """
        with self.db_manager as conn:
            db_ops = DBOperations(conn)

            # --- Step 1: Scanning ---
            logging.info(f"Scanning {src_root} (Seed={is_seed})...")
            scanner = DiskScanner()

            # Load known sparse hashes to optimize 2-stage hashing
            known_sparse_hashes = db_ops.fetch_known_sparse_hashes()

            processed_count = 0
            for record in scanner.scan(src_root, is_seed, known_sparse_hashes, skip_dirs, max_workers=max_workers):
                file_id = db_ops.upsert_file_record(record)
                if record.type in ('raw', 'jpeg', 'video', 'psd', 'tiff'):
                    db_ops.upsert_media_metadata(file_id, record)

                hash_value = record.hash or record.sparse_hash
                if hash_value:
                    mtime = record.mtime if record.mtime is not None else record.orig_path.stat().st_mtime
                    db_ops.record_occurrence(
                        file_id=file_id,
                        path=record.orig_path,
                        is_seed=record.is_seed,
                        mtime=mtime,
                        size_bytes=record.size_bytes,
                        hash_value=hash_value,
                        is_sparse=record.hash_is_sparse,
                    )
                
                processed_count += 1
                if processed_count % 1000 == 0:
                    conn.commit()
            
            conn.commit()
            logging.info(f"Scan complete. Processed {processed_count} files.")

            # --- Step 2: Linking ---
            # We must link Sidecars/PSDs before planning destinations
            linker = FileLinker(db_ops)
            linker.link_raw_sidecars()
            linker.link_psds()

            # --- Step 3: Planning ---
            logging.info("Planning destinations...")
            planner = DestinationPlanner(db_ops)
            planner.plan_all(dest_root)
            
            # Important: Assign destinations for Linked files (Sidecars/PSDs)
            # (Note: You may need to add specific methods in Planner for this, 
            #  or ensure plan_all covers them via dependency logic. 
            #  For now, we assume Planner handles the main files, 
            #  and we might need a 'Planner.assign_linked' pass here.)
            self._assign_linked_destinations(db_ops)
            
            conn.commit()

            # --- Step 4: Execution ---
            mover = FileMover(db_ops)
            mover.execute(move_mode=move, dry_run=dry_run)
            
            logging.info("Organization phase complete.")

    def _assign_linked_destinations(self, db_ops: DBOperations):
        """
        Helper to assign destinations for Sidecars/PSDs based on their parents.
        (Ported logic from original assign_sidecar_destinations)
        """
        cur = db_ops.conn.cursor()
        
        # 1. Sidecars follow RAWs
        cur.execute("""
            SELECT rs.sidecar_file_id, f.orig_name, r.dest_path
            FROM raw_sidecars rs
            JOIN files f ON rs.sidecar_file_id = f.id
            JOIN files r ON rs.raw_file_id = r.id
            WHERE f.dest_path IS NULL AND r.dest_path IS NOT NULL
        """)
        rows = cur.fetchall()
        for sidecar_id, name, raw_dest_str in rows:
            raw_dest = Path(raw_dest_str)
            sidecar_dest = raw_dest.with_suffix(Path(name).suffix)
            # (Collision handling omitted for brevity, usually matches RAW stem)
            db_ops.update_dest_path(sidecar_id, str(sidecar_dest))
            
        # 2. PSDs follow Sources
        cur.execute("""
            SELECT psl.psd_file_id, f.orig_name, s.dest_path
            FROM psd_source_links psl
            JOIN files f ON psl.psd_file_id = f.id
            JOIN files s ON psl.source_file_id = s.id
            WHERE f.dest_path IS NULL AND s.dest_path IS NOT NULL
        """)
        rows = cur.fetchall()
        for psd_id, name, src_dest_str in rows:
            src_dest = Path(src_dest_str)
            dest_parent = src_dest.parent

            # PSDs are considered outputs; if the source lives in the raw tree, mirror the
            # folder structure under output instead.
            parts = list(dest_parent.parts)
            for idx, part in enumerate(parts):
                if part.lower() == "raw":
                    parts[idx] = "output"
                    dest_parent = Path(*parts)
                    break

            psd_dest = dest_parent / name  # PSD keeps its own name, just moves folder
            db_ops.update_dest_path(psd_id, str(psd_dest))
