"""
Path synchronization module for detecting and updating renamed files in destinations.

This module scans destination directories and compares the actual file locations
against the database records, detecting files that have been renamed and updating
the database to reflect the current state.
"""

import csv
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ..database.ops import DBOperations
from ..metadata.extract import MetadataExtractor
from ..scanning.hasher import FileHasher
from ..pipeline.observations import ObservationRecorder
from .. import config


class SyncStatus(Enum):
    """Classification of file sync status."""
    UNCHANGED = "unchanged"   # File at expected location
    RENAMED = "renamed"       # Same parent dir, different filename
    MOVED = "moved"          # Different parent directory (warn only)
    MISSING = "missing"       # Not found anywhere in destination
    NEW = "new"              # Found on disk but not in database


@dataclass
class SyncResult:
    """Result of comparing a single DB record to disk state."""
    file_id: int
    status: SyncStatus
    old_path: str
    new_path: Optional[str] = None
    hash_value: Optional[str] = None
    is_sparse: bool = False
    mtime: float = 0.0
    size_bytes: int = 0


@dataclass
class RenameRecord:
    """
    Structured rename detected during sync. Carries everything
    update_dest_path_atomic needs, so apply_renames can run later (e.g. inside
    the pipeline's Phase B savepoint) without re-scanning disk.

    matched_hash / matched_hash_is_sparse describe which key hit the catalog
    lookup. observed_full_hash, when populated, is the full SHA-256 computed
    during FileHasher's sparse-collision escalation — apply_renames will
    attempt to persist it on files.hash (Change 5) and will always prefer it
    when recording the post-rename file_occurrence so the occurrence stores
    the strongest observed identity.
    """
    file_id: int
    old_path: str
    new_path: str
    mtime: float
    size_bytes: int
    matched_hash: str
    matched_hash_is_sparse: bool
    observed_full_hash: Optional[str]
    match_method: str = "hash_same_directory"
    confidence: int = 100


@dataclass(frozen=True)
class RawEditCandidate:
    file_id: int
    old_path: str
    orig_name: str
    size_bytes: Optional[int]
    capture_datetime: Optional[datetime]
    camera_model: Optional[str]


@dataclass(frozen=True)
class RawEditMatch:
    candidate: RawEditCandidate
    confidence: int
    signals: Dict[str, object]


@dataclass
class SyncReport:
    """Statistics and details from a sync operation."""
    scanned_count: int = 0
    unchanged_count: int = 0
    renamed_count: int = 0
    moved_count: int = 0
    missing_count: int = 0
    new_count: int = 0
    imported_count: int = 0
    skipped_hash_count: int = 0
    error_count: int = 0

    confirmed_files: List[str] = field(default_factory=list)
    renamed_files: List[Tuple[str, str]] = field(default_factory=list)  # (old, new) — CSV/log back-compat
    renames: List[RenameRecord] = field(default_factory=list)  # structured, for deferred apply
    moved_files: List[Tuple[str, str]] = field(default_factory=list)
    missing_files: List[str] = field(default_factory=list)
    new_files: List[str] = field(default_factory=list)
    imported_files: List[str] = field(default_factory=list)
    imported_file_ids: Set[int] = field(default_factory=set)
    review_count: int = 0
    review_files: List[str] = field(default_factory=list)
    errors: List[Tuple[str, str]] = field(default_factory=list)  # (path, error)


class DestinationSyncer:
    """
    Synchronizes database paths with actual file locations in destination directories.

    Detects renamed files by comparing content hashes and updates the database
    to reflect the current on-disk state.
    """

    def __init__(self, db_ops: DBOperations, max_workers: int = 3,
                 run_id: Optional[int] = None):
        self.db = db_ops
        self.hasher = FileHasher()
        self.metadata = MetadataExtractor()
        self.max_workers = max_workers
        self.observations = ObservationRecorder(db_ops, run_id)

    def sync_destinations(
        self,
        dest_roots: List[Path],
        apply_renames: bool = True,
        csv_output: Optional[str] = None,
    ) -> SyncReport:
        """
        Main entry point. Scans destination directories and detects renames.

        Algorithm:
        1. Load all dest_path records from database
        2. For each dest_root, scan actual files on disk
        3. Match by hash (full preferred, sparse fallback — Change 4/5)
        4. Detect renames (same parent dir, different name)
        5. Record renames as RenameRecords on the report.
        6. If apply_renames=True (default), persist them immediately via
           self.apply_renames(report). Detect-only callers pass False and
           apply later via self.apply_renames(report) under their own
           transaction boundary (Phase B savepoint).

        Args:
            dest_roots: List of destination root directories to sync.
            apply_renames: When True (default), apply detected renames to the
                DB immediately — preserves historical single-call semantics
                for --sync-dest and auto-sync. When False, rename application
                is deferred to the caller (used by the ingest-dest pipeline
                so renames roll back with the Phase B savepoint on dry-run).
            csv_output: Optional path to write sync report CSV.

        Returns:
            SyncReport with statistics, renamed_files (back-compat tuples),
            and renames (structured RenameRecords).
        """
        report = SyncReport()

        # Load expected files scoped to the requested dest_roots
        all_dest_roots = [r for r in dest_roots if r.exists()]
        db_files = self._load_expected_files(all_dest_roots)
        logging.info(f"Loaded {len(db_files)} files with dest_path from database")

        # Build lookup structures
        # path_to_file: {dest_path: (file_id, hash, sparse_hash, db_mtime, db_size)}
        path_to_file: Dict[str, Tuple[int, Optional[str], Optional[str], Optional[float], Optional[int]]] = {}
        # Separate indices for full and sparse hashes so Change 4's dual-key lookup
        # can prefer full over sparse when the hasher produced both.
        full_hash_to_file: Dict[str, List[Tuple[int, str]]] = {}
        sparse_hash_to_file: Dict[str, List[Tuple[int, str]]] = {}

        for file_id, dest_path, hash_val, sparse_hash, mtime, size in db_files:
            path_to_file[dest_path] = (file_id, hash_val, sparse_hash, mtime, size)
            if hash_val:
                full_hash_to_file.setdefault(hash_val, []).append((file_id, dest_path))
            if sparse_hash:
                sparse_hash_to_file.setdefault(sparse_hash, []).append((file_id, dest_path))

        raw_edit_candidates = self._load_raw_edit_candidates(all_dest_roots)

        # Track which DB files we've matched
        matched_file_ids: Set[int] = set()

        # Scan each destination root
        for dest_root in dest_roots:
            if not dest_root.exists():
                logging.warning(f"Destination root does not exist: {dest_root}")
                continue

            logging.info(f"Scanning destination: {dest_root}")
            self._scan_and_match(
                dest_root=dest_root,
                path_to_file=path_to_file,
                full_hash_to_file=full_hash_to_file,
                sparse_hash_to_file=sparse_hash_to_file,
                raw_edit_candidates=raw_edit_candidates,
                matched_file_ids=matched_file_ids,
                report=report,
            )

        # Find missing files (in DB but not found on disk)
        for dest_path, (file_id, _, _, _, _) in path_to_file.items():
            if file_id not in matched_file_ids:
                report.missing_count += 1
                report.missing_files.append(dest_path)
                self.observations.record_missing_expected(
                    file_id=file_id,
                    path=Path(dest_path),
                    root_kind="dest",
                )
                logging.warning(f"Missing: {dest_path}")

        if apply_renames and report.renames:
            self.apply_renames(report)

        # Write CSV report if requested
        if csv_output:
            self._write_csv_report(csv_output, report)

        return report

    def apply_renames(self, report: SyncReport) -> Set[int]:
        """
        Persist the rename records accumulated during detection.

        Iterates `report.renames` and calls DBOperations.update_dest_path_atomic
        with the structured fields, including the observed full hash so the
        backing helper can upgrade files.hash (Change 5) and record the
        post-rename file_occurrence with the strongest identity available.

        Returns the set of file_ids whose dest_path was updated. Callers in
        the pipeline feed this into PipelineCandidates so Phase B scoping
        sees the renamed files.
        """
        applied: Set[int] = set()
        for rec in report.renames:
            self.db.update_dest_path_atomic(
                file_id=rec.file_id,
                old_path=rec.old_path,
                new_path=rec.new_path,
                new_mtime=rec.mtime,
                size_bytes=rec.size_bytes,
                hash_value=rec.matched_hash,
                is_sparse=rec.matched_hash_is_sparse,
                observed_full_hash=rec.observed_full_hash,
            )
            applied.add(rec.file_id)
        return applied

    def _load_expected_files(self, dest_roots: List[Path]) -> List[Tuple[int, str, Optional[str], Optional[str], Optional[float], Optional[int]]]:
        """Load files with dest_path from database, scoped to the given dest_roots."""
        return self.db.get_all_dest_files(dest_roots if dest_roots else None)

    def _scan_and_match(
        self,
        dest_root: Path,
        path_to_file: Dict[str, Tuple[int, Optional[str], Optional[str], Optional[float], Optional[int]]],
        full_hash_to_file: Dict[str, List[Tuple[int, str]]],
        sparse_hash_to_file: Dict[str, List[Tuple[int, str]]],
        raw_edit_candidates: Dict[str, List[RawEditCandidate]],
        matched_file_ids: Set[int],
        report: SyncReport,
    ):
        """Scan a destination directory and match files against database.

        Rename detection only — no DB writes here. Renames are recorded as
        RenameRecord instances on `report.renames`; callers decide when to
        apply them via apply_renames().
        """
        # Get all supported extensions
        supported_exts = set(config.EXT_TO_TYPE.keys())

        # Walk the destination tree
        for file_path in self._iter_files(dest_root, supported_exts):
            report.scanned_count += 1
            path_str = str(file_path)

            try:
                stat = file_path.stat()
                disk_mtime = stat.st_mtime
                disk_size = stat.st_size
            except OSError as e:
                report.error_count += 1
                report.errors.append((path_str, str(e)))
                continue

            # Check if file is at expected location
            if path_str in path_to_file:
                file_id, db_hash, db_sparse, db_mtime, db_size = path_to_file[path_str]
                matched_file_ids.add(file_id)
                report.unchanged_count += 1
                report.confirmed_files.append(path_str)
                self.observations.record_file_present(
                    file_id=file_id,
                    path=file_path,
                    root_kind="dest",
                    hash_value=db_hash,
                    sparse_hash=db_sparse,
                    hash_is_sparse=db_hash is None and db_sparse is not None,
                    size_bytes=disk_size,
                    mtime=disk_mtime,
                    match_method="path",
                    confidence=100,
                )
                continue

            # File not at expected path - need to identify by hash
            hash_result = self._compute_hash_for_file(file_path, disk_size)
            if hash_result is None:
                report.error_count += 1
                report.errors.append((path_str, "Failed to compute hash"))
                continue

            # Change 4 + 5: dual-key lookup, prefer full-hash match.
            # full_hash identifies 1:1 when it exists on both sides; sparse is a
            # fallback that can match catalog rows which were originally stored
            # sparse-only and never upgraded to full.
            matches: List[Tuple[int, str]] = []
            seen: Set[int] = set()
            matched_on_full = False
            for key, index, is_sparse_key in (
                (hash_result.full_hash, full_hash_to_file, False),
                (hash_result.sparse_hash, sparse_hash_to_file, True),
            ):
                if not key:
                    continue
                for file_id, dest_path in index.get(key, []):
                    if file_id in seen:
                        continue
                    seen.add(file_id)
                    matches.append((file_id, dest_path))
                    if not is_sparse_key:
                        matched_on_full = True
                # Once the full-hash key has resolved any matches, prefer those
                # over the sparse fallback — a full-hash hit is the stronger
                # identity.
                if matched_on_full and matches:
                    break

            if not matches:
                raw_edit_match = self._match_modified_raw(
                    file_path=file_path,
                    disk_size=disk_size,
                    disk_mtime=disk_mtime,
                    hash_result=hash_result,
                    raw_edit_candidates=raw_edit_candidates,
                    matched_file_ids=matched_file_ids,
                )
                if raw_edit_match is not None:
                    self._record_modified_raw_rename(
                        match=raw_edit_match,
                        file_path=file_path,
                        disk_size=disk_size,
                        disk_mtime=disk_mtime,
                        hash_result=hash_result,
                        matched_file_ids=matched_file_ids,
                        report=report,
                    )
                    continue

                # File not in database - it's new
                report.new_count += 1
                report.new_files.append(path_str)
                self.observations.record_new_candidate(
                    path=file_path,
                    root_kind="dest",
                    hash_value=hash_result.full_hash,
                    sparse_hash=hash_result.sparse_hash,
                    hash_is_sparse=hash_result.full_hash is None,
                    size_bytes=disk_size,
                    mtime=disk_mtime,
                )
                continue

            # Found matching hash - check if it's a rename or move
            for file_id, old_dest_path in matches:
                if file_id in matched_file_ids:
                    continue  # Already matched this file

                old_path = Path(old_dest_path)
                new_path = file_path

                # Check if same parent directory (rename) or different (move)
                if old_path.parent == new_path.parent:
                    # RENAME: Same directory, different filename
                    # Record structured; do NOT write to DB here. Callers choose
                    # when to apply (immediately via apply_renames=True on
                    # sync_destinations, or deferred via explicit apply_renames()).
                    matched_key = hash_result.full_hash if matched_on_full else hash_result.sparse_hash
                    matched_is_sparse = not matched_on_full
                    report.renamed_count += 1
                    report.renamed_files.append((old_dest_path, path_str))
                    report.renames.append(RenameRecord(
                        file_id=file_id,
                        old_path=old_dest_path,
                        new_path=path_str,
                        mtime=disk_mtime,
                        size_bytes=disk_size,
                        matched_hash=matched_key,
                        matched_hash_is_sparse=matched_is_sparse,
                        observed_full_hash=hash_result.full_hash,
                    ))
                    self.observations.record_rename_candidate(
                        file_id=file_id,
                        old_path=old_path,
                        new_path=new_path,
                        root_kind="dest",
                        hash_value=matched_key,
                        sparse_hash=hash_result.sparse_hash,
                        hash_is_sparse=matched_is_sparse,
                        size_bytes=disk_size,
                        mtime=disk_mtime,
                        match_method="hash_same_directory",
                        confidence=100 if matched_on_full else 80,
                    )
                    self.observations.record_missing_expected(
                        file_id=file_id,
                        path=old_path,
                        root_kind="dest",
                        payload={"matched_path": str(new_path)},
                    )
                    matched_file_ids.add(file_id)
                    logging.info(f"Renamed: {old_path.name} → {new_path.name}")
                else:
                    # MOVE: Different directory (warn but don't auto-update)
                    report.moved_count += 1
                    report.moved_files.append((old_dest_path, path_str))
                    self.observations.record_move_candidate(
                        file_id=file_id,
                        old_path=old_path,
                        new_path=new_path,
                        root_kind="dest",
                        hash_value=hash_result.full_hash or hash_result.sparse_hash,
                        sparse_hash=hash_result.sparse_hash,
                        hash_is_sparse=not matched_on_full,
                        size_bytes=disk_size,
                        mtime=disk_mtime,
                        match_method="hash_different_directory",
                        confidence=100 if matched_on_full else 80,
                    )
                    self.observations.record_missing_expected(
                        file_id=file_id,
                        path=old_path,
                        root_kind="dest",
                        payload={"matched_path": str(new_path)},
                    )
                    matched_file_ids.add(file_id)
                    logging.warning(
                        f"Moved (not auto-synced): {old_dest_path} → {path_str}"
                    )

                break  # Only match once per file

    def _load_raw_edit_candidates(
        self,
        dest_roots: List[Path],
    ) -> Dict[str, List[RawEditCandidate]]:
        by_parent: Dict[str, List[RawEditCandidate]] = {}
        rows = self.db.get_raw_edit_reconciliation_candidates(
            dest_roots if dest_roots else None
        )
        for file_id, dest_path, orig_name, size_bytes, capture_str, camera_model in rows:
            candidate = RawEditCandidate(
                file_id=int(file_id),
                old_path=dest_path,
                orig_name=orig_name,
                size_bytes=size_bytes,
                capture_datetime=self._parse_datetime(capture_str),
                camera_model=camera_model,
            )
            by_parent.setdefault(str(Path(dest_path).parent), []).append(candidate)
        return by_parent

    def _match_modified_raw(
        self,
        *,
        file_path: Path,
        disk_size: int,
        disk_mtime: float,
        hash_result,
        raw_edit_candidates: Dict[str, List[RawEditCandidate]],
        matched_file_ids: Set[int],
    ) -> Optional[RawEditMatch]:
        if config.EXT_TO_TYPE.get(file_path.suffix.lower()) != "raw":
            return None

        candidates = [
            candidate
            for candidate in raw_edit_candidates.get(str(file_path.parent), [])
            if candidate.file_id not in matched_file_ids
            and not Path(candidate.old_path).exists()
        ]
        if not candidates:
            return None

        observed_dt, observed_camera = self._extract_raw_edit_metadata(
            file_path, disk_mtime
        )
        scored: List[RawEditMatch] = []
        for candidate in candidates:
            score, signals = self._score_raw_edit_candidate(
                candidate=candidate,
                new_path=file_path,
                observed_dt=observed_dt,
                observed_camera=observed_camera,
                disk_size=disk_size,
            )
            if score >= 70:
                scored.append(RawEditMatch(candidate, score, signals))

        if not scored:
            return None

        scored.sort(key=lambda item: item.confidence, reverse=True)
        top = scored[0]
        tied = [item for item in scored if item.confidence == top.confidence]
        if len(tied) > 1:
            for item in tied:
                self.observations.record_modified_rename_candidate(
                    file_id=item.candidate.file_id,
                    old_path=Path(item.candidate.old_path),
                    new_path=file_path,
                    root_kind="dest",
                    hash_value=hash_result.full_hash,
                    sparse_hash=hash_result.sparse_hash,
                    hash_is_sparse=hash_result.full_hash is None,
                    size_bytes=disk_size,
                    mtime=disk_mtime,
                    match_method="raw_edit_metadata_ambiguous",
                    confidence=item.confidence,
                    payload={
                        "signals": item.signals,
                        "status": "review_required",
                    },
                )
            return RawEditMatch(
                candidate=top.candidate,
                confidence=0,
                signals={
                    "ambiguous_candidate_ids": [
                        item.candidate.file_id for item in tied
                    ],
                },
            )

        return top

    def _record_modified_raw_rename(
        self,
        *,
        match: RawEditMatch,
        file_path: Path,
        disk_size: int,
        disk_mtime: float,
        hash_result,
        matched_file_ids: Set[int],
        report: SyncReport,
    ) -> None:
        if match.confidence <= 0:
            report.review_count += 1
            report.review_files.append(str(file_path))
            logging.warning(
                "Modified RAW candidate requires review: %s (%s)",
                file_path,
                match.signals,
            )
            return

        candidate = match.candidate
        matched_key = hash_result.full_hash or hash_result.sparse_hash
        if not matched_key:
            report.error_count += 1
            report.errors.append((str(file_path), "Failed to compute RAW edit hash"))
            return
        matched_is_sparse = hash_result.full_hash is None
        report.renamed_count += 1
        report.renamed_files.append((candidate.old_path, str(file_path)))
        report.renames.append(RenameRecord(
            file_id=candidate.file_id,
            old_path=candidate.old_path,
            new_path=str(file_path),
            mtime=disk_mtime,
            size_bytes=disk_size,
            matched_hash=matched_key,
            matched_hash_is_sparse=matched_is_sparse,
            observed_full_hash=hash_result.full_hash,
            match_method="raw_edit_metadata_same_directory",
            confidence=match.confidence,
        ))
        self.observations.record_modified_rename_candidate(
            file_id=candidate.file_id,
            old_path=Path(candidate.old_path),
            new_path=file_path,
            root_kind="dest",
            hash_value=hash_result.full_hash,
            sparse_hash=hash_result.sparse_hash,
            hash_is_sparse=matched_is_sparse,
            size_bytes=disk_size,
            mtime=disk_mtime,
            match_method="raw_edit_metadata_same_directory",
            confidence=match.confidence,
            payload={"signals": match.signals},
        )
        self.observations.record_missing_expected(
            file_id=candidate.file_id,
            path=Path(candidate.old_path),
            root_kind="dest",
            payload={
                "matched_path": str(file_path),
                "match_method": "raw_edit_metadata_same_directory",
            },
        )
        matched_file_ids.add(candidate.file_id)
        logging.info(
            "Modified RAW rename candidate accepted: %s -> %s (confidence=%s)",
            Path(candidate.old_path).name,
            file_path.name,
            match.confidence,
        )

    def _score_raw_edit_candidate(
        self,
        *,
        candidate: RawEditCandidate,
        new_path: Path,
        observed_dt: Optional[datetime],
        observed_camera: Optional[str],
        disk_size: int,
    ) -> Tuple[int, Dict[str, object]]:
        score = 30
        signals: Dict[str, object] = {"same_directory": True}

        if candidate.capture_datetime and observed_dt:
            delta = abs((candidate.capture_datetime - observed_dt).total_seconds())
            signals["capture_delta_seconds"] = delta
            if delta <= 2:
                score += 35
            elif delta <= 60:
                score += 25
            elif candidate.capture_datetime.date() == observed_dt.date():
                score += 15

        old_sequence = self._camera_sequence_token(Path(candidate.old_path).stem)
        new_sequence = self._camera_sequence_token(new_path.stem)
        if old_sequence and new_sequence and old_sequence == new_sequence:
            score += 25
            signals["sequence_token"] = old_sequence

        old_base = self._stem_without_timestamp(Path(candidate.old_path).stem)
        new_base = self._stem_without_timestamp(new_path.stem)
        if old_base and new_base and (
            old_base == new_base or old_base in new_base or new_base in old_base
        ):
            score += 10
            signals["stem_similarity"] = True

        if candidate.camera_model and observed_camera:
            camera_matches = candidate.camera_model == observed_camera
            signals["camera_model_match"] = camera_matches
            if camera_matches:
                score += 10

        if candidate.size_bytes and candidate.size_bytes > 0:
            ratio = abs(candidate.size_bytes - disk_size) / candidate.size_bytes
            signals["size_delta_ratio"] = ratio
            if ratio <= 0.05:
                score += 10
            elif ratio <= 0.20:
                score += 5

        return min(score, 95), signals

    def _extract_raw_edit_metadata(
        self,
        file_path: Path,
        disk_mtime: float,
    ) -> Tuple[Optional[datetime], Optional[str]]:
        capture_dt, camera_model, _lens = self.metadata.get_image_metadata(file_path)
        if capture_dt is None:
            capture_dt = datetime.fromtimestamp(disk_mtime)
        return capture_dt, camera_model

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _camera_sequence_token(stem: str) -> Optional[str]:
        tokens = re.findall(r"\d+", stem)
        for token in tokens:
            if len(token) < 3 or len(token) > 5:
                continue
            value = int(token)
            if 1900 <= value <= 2099:
                continue
            return token.lstrip("0") or "0"
        return None

    @staticmethod
    def _stem_without_timestamp(stem: str) -> str:
        cleaned = re.sub(
            r"[_-]?\d{4}[-_]\d{2}[-_]\d{2}([_-]\d{2}[-_]\d{2}[-_]\d{2})?$",
            "",
            stem,
        )
        return re.sub(r"[^a-z0-9]+", "_", cleaned.lower()).strip("_")

    def _iter_files(self, root: Path, extensions: Set[str]) -> List[Path]:
        """Iterate over files with supported extensions in directory tree."""
        files = []
        try:
            for item in root.rglob("*"):
                if item.is_file() and item.suffix.lower() in extensions:
                    # Skip hidden files and macOS metadata
                    if not item.name.startswith("."):
                        files.append(item)
        except PermissionError as e:
            logging.warning(f"Permission denied: {e}")
        return files

    def _compute_hash_for_file(self, file_path: Path, file_size: int):
        """Compute hash for a file, using sparse hash for large files."""
        try:
            # Get known sparse hashes to check for collisions
            known_sparse = self.db.fetch_known_sparse_hashes()
            return self.hasher.compute_hash(file_path, known_sparse)
        except Exception as e:
            logging.error(f"Error hashing {file_path}: {e}")
            return None

    def import_new_files(
        self,
        new_file_paths: List[str],
        report: SyncReport,
        dry_run: bool = False,
        ingest_mode: bool = False,
    ) -> Set[int]:
        """
        Import new files found in destinations into the database.

        Args:
            new_file_paths: List of file paths to import (from report.new_files)
            report: SyncReport to update with import results. report.imported_file_ids
                is populated with the working set of file_ids accepted by this
                call — including ids of rows that already existed (upsert returns
                the existing id; those rows are still part of this run's
                candidate set for planner/linker/mover).
            dry_run: If True, don't modify database
            ingest_mode: If True, do NOT set dest_path after import, leaving the file
                         visible to the planner for re-routing (used by --ingest-dest).
                         If False (default), dest_path is set to the file's current
                         location, treating it as already correctly placed.

        Returns:
            Set of file_ids accepted into the ingest working set (same value as
            report.imported_file_ids after the call). On dry_run, returns an
            empty set because no upserts were performed — the caller has no
            ids to feed into downstream scoping.
        """
        from ..scanning.filesystem import DiskScanner

        if not new_file_paths:
            return set()

        scanner = DiskScanner()
        known_sparse = self.db.fetch_known_sparse_hashes()
        imported_ids: Set[int] = set()

        for path_str in new_file_paths:
            file_path = Path(path_str)

            if not file_path.exists():
                logging.warning(f"File no longer exists: {path_str}")
                continue

            try:
                # Use scanner to process the file (extracts metadata, computes hash)
                record = scanner._process_single_file(
                    path=file_path,
                    is_seed=False,  # Files in destination are not seeds
                    known_sparse_hashes=known_sparse,
                )

                if record is None:
                    logging.warning(f"Failed to process file: {path_str}")
                    report.error_count += 1
                    report.errors.append((path_str, "Failed to process file"))
                    continue

                if dry_run:
                    logging.info(f"Would import: {file_path.name} ({record.type})")
                    report.imported_count += 1
                    report.imported_files.append(path_str)
                    continue

                # Insert into database — returns existing id if already catalogued.
                file_id = self.db.upsert_file_record(record)

                # Add media metadata if applicable
                if record.type in ('raw', 'jpeg', 'video', 'psd', 'tiff'):
                    self.db.upsert_media_metadata(file_id, record)

                # Set dest_path unless ingest_mode — ingest leaves dest_path NULL
                # so the planner can assign the correct destination.
                if not ingest_mode:
                    self.db.update_dest_path(file_id, path_str)

                # Record occurrence
                hash_value = record.hash or record.sparse_hash
                if hash_value:
                    stat = file_path.stat()
                    self.db.record_occurrence(
                        file_id=file_id,
                        path=file_path,
                        is_seed=False,
                        mtime=stat.st_mtime,
                        size_bytes=stat.st_size,
                        hash_value=hash_value,
                        is_sparse=record.hash_is_sparse,
                    )
                    self.observations.record_file_observation(
                        observation_type="new_candidate" if ingest_mode else "present",
                        file_id=file_id,
                        path=file_path,
                        root_kind="dest",
                        hash_value=record.hash,
                        sparse_hash=record.sparse_hash,
                        hash_is_sparse=record.hash_is_sparse,
                        size_bytes=record.size_bytes,
                        mtime=stat.st_mtime,
                        match_method="import_new",
                    )

                # Track sparse hash for collision detection
                if record.sparse_hash:
                    known_sparse.add(record.sparse_hash)

                logging.info(f"Imported: {file_path.name} ({record.type})")
                report.imported_count += 1
                report.imported_files.append(path_str)
                report.imported_file_ids.add(file_id)
                imported_ids.add(file_id)

            except Exception as e:
                logging.error(f"Error importing {path_str}: {e}")
                report.error_count += 1
                report.errors.append((path_str, str(e)))

        return imported_ids

    def _write_csv_report(self, csv_path: str, report: SyncReport):
        """Write sync results to CSV file."""
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Summary section
            writer.writerow(['=== SYNC SUMMARY ==='])
            writer.writerow(['Metric', 'Count'])
            writer.writerow(['Scanned', report.scanned_count])
            writer.writerow(['Unchanged', report.unchanged_count])
            writer.writerow(['Renamed', report.renamed_count])
            writer.writerow(['Moved (not synced)', report.moved_count])
            writer.writerow(['Missing', report.missing_count])
            writer.writerow(['New (not in DB)', report.new_count])
            writer.writerow(['Imported', report.imported_count])
            writer.writerow(['Review Required', report.review_count])
            writer.writerow(['Errors', report.error_count])
            writer.writerow([])

            # Renamed files
            if report.renamed_files:
                writer.writerow(['=== RENAMED FILES ==='])
                writer.writerow(['Old Path', 'New Path'])
                for old, new in report.renamed_files:
                    writer.writerow([old, new])
                writer.writerow([])

            # Moved files
            if report.moved_files:
                writer.writerow(['=== MOVED FILES (not auto-synced) ==='])
                writer.writerow(['Old Path', 'New Path'])
                for old, new in report.moved_files:
                    writer.writerow([old, new])
                writer.writerow([])

            # Missing files
            if report.missing_files:
                writer.writerow(['=== MISSING FILES ==='])
                writer.writerow(['Path'])
                for path in report.missing_files:
                    writer.writerow([path])
                writer.writerow([])

            # New files
            if report.new_files:
                writer.writerow(['=== NEW FILES (not in database) ==='])
                writer.writerow(['Path'])
                for path in report.new_files:
                    writer.writerow([path])
                writer.writerow([])

            # Imported files
            if report.imported_files:
                writer.writerow(['=== IMPORTED FILES ==='])
                writer.writerow(['Path'])
                for path in report.imported_files:
                    writer.writerow([path])
                writer.writerow([])

            # Review-required files
            if report.review_files:
                writer.writerow(['=== REVIEW REQUIRED ==='])
                writer.writerow(['Path'])
                for path in report.review_files:
                    writer.writerow([path])
                writer.writerow([])

            # Errors
            if report.errors:
                writer.writerow(['=== ERRORS ==='])
                writer.writerow(['Path', 'Error'])
                for path, error in report.errors:
                    writer.writerow([path, error])

        logging.info(f"Wrote sync report to: {csv_path}")
