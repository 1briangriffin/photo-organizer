"""
Run-history recorder for photo-organizer (and sibling tools like photo-faces).

A RunRecorder is a short-lived object that opens its own SQLite connection,
inserts a placeholder row at start, and updates that row at finish/failure.
Using a separate connection avoids interfering with the main pipeline's
transaction/savepoint state.

SQLite single-writer nuance: SQLite serializes writers. If the main pipeline
holds a long-running write transaction when the recorder tries to INSERT or
UPDATE, the recorder will block until the pipeline releases its lock (or raise
sqlite3.OperationalError if the default busy timeout expires). Current policy:
accept that recorder operations happen between pipeline commits (start before
pipeline begins; finish after pipeline has committed or rolled back), which
avoids contention. A formal busy_timeout policy is deferred until we have a
use case that needs concurrent writers.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


def iso_now() -> str:
    """Return the current UTC time as an ISO-8601 string with an explicit 'Z'.

    Format: YYYY-MM-DDTHH:MM:SS.ssssssZ. Lexicographically sortable and matches
    the contract documented on command_runs.started_at.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _json_default(obj: Any) -> str:
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (set, frozenset)):
        return sorted(str(x) for x in obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON-serializable")


def _dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, ensure_ascii=False)


class RunRecorder:
    """Records the lifecycle of a single CLI invocation into command_runs.

    Usage:
        recorder = RunRecorder(db_path, tool="photo-organizer", command="organize",
                               argv=sys.argv, params={...}, dry_run=args.dry_run,
                               db_path_for_row=db_path, src_root=src, dest_root=dest,
                               app_version="photo-organizer 0.1.0")
        recorder.start()
        try:
            ...do work...
            recorder.finish_success(stats={...}, db_mutates=..., files_mutate=...)
        except KeyboardInterrupt:
            recorder.finish_interrupted()
            raise
        except Exception as e:
            recorder.finish_error(e)
            raise

    If the recorder's own DB writes fail (locked, disk full, etc.) the caller's
    work is not disrupted — failures are logged and swallowed. This is telemetry,
    not load-bearing.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        tool: str,
        command: str,
        argv: Sequence[str],
        params: Optional[Mapping[str, Any]] = None,
        dry_run: bool = False,
        db_path_for_row: Optional[Path] = None,
        src_root: Optional[Path] = None,
        dest_root: Optional[Path] = None,
        app_version: Optional[str] = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._tool = tool
        self._command = command
        self._argv = list(argv)
        self._params = dict(params) if params is not None else None
        self._dry_run = bool(dry_run)
        self._db_path_for_row = db_path_for_row
        self._src_root = src_root
        self._dest_root = dest_root
        self._app_version = app_version
        self._row_id: Optional[int] = None

    def start(self) -> None:
        try:
            with sqlite3.connect(str(self._db_path)) as conn:
                cur = conn.execute(
                    """
                    INSERT INTO command_runs (
                        tool, command, started_at, exit_status, dry_run,
                        db_path, src_root, dest_root, argv_json, params_json, app_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._tool,
                        self._command,
                        iso_now(),
                        "running",  # finalized by finish_success/error/interrupted;
                                    # left as 'running' if the process dies mid-run
                        1 if self._dry_run else 0,
                        str(self._db_path_for_row) if self._db_path_for_row else None,
                        str(self._src_root) if self._src_root else None,
                        str(self._dest_root) if self._dest_root else None,
                        _dumps(self._argv),
                        _dumps(self._params) if self._params is not None else None,
                        self._app_version,
                    ),
                )
                self._row_id = cur.lastrowid
        except sqlite3.Error:
            logging.exception("RunRecorder.start failed; run will not be logged")
            self._row_id = None

    def finish_success(
        self,
        *,
        stats: Optional[Mapping[str, Any]] = None,
        db_mutates: bool = False,
        files_mutate: bool = False,
    ) -> None:
        self._finalize(
            exit_status="success",
            stats=stats,
            db_mutates=db_mutates,
            files_mutate=files_mutate,
        )

    def finish_error(
        self,
        exc: BaseException,
        *,
        stats: Optional[Mapping[str, Any]] = None,
        db_mutates: bool = False,
        files_mutate: bool = False,
    ) -> None:
        self._finalize(
            exit_status="error",
            stats=stats,
            db_mutates=db_mutates,
            files_mutate=files_mutate,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

    def finish_interrupted(
        self,
        *,
        stats: Optional[Mapping[str, Any]] = None,
        db_mutates: bool = False,
        files_mutate: bool = False,
        note: Optional[str] = None,
    ) -> None:
        self._finalize(
            exit_status="interrupted",
            stats=stats,
            db_mutates=db_mutates,
            files_mutate=files_mutate,
            error_type="KeyboardInterrupt",
            error_message=note,
        )

    def _finalize(
        self,
        *,
        exit_status: str,
        stats: Optional[Mapping[str, Any]] = None,
        db_mutates: bool = False,
        files_mutate: bool = False,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        if self._row_id is None:
            return
        try:
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    """
                    UPDATE command_runs
                       SET finished_at = ?,
                           exit_status = ?,
                           db_mutates = ?,
                           files_mutate = ?,
                           stats_json = ?,
                           error_type = ?,
                           error_message = ?
                     WHERE id = ?
                    """,
                    (
                        iso_now(),
                        exit_status,
                        1 if db_mutates else 0,
                        1 if files_mutate else 0,
                        _dumps(dict(stats)) if stats is not None else None,
                        error_type,
                        error_message,
                        self._row_id,
                    ),
                )
        except sqlite3.Error:
            logging.exception(
                "RunRecorder._finalize failed; row %s left in prior state", self._row_id
            )
