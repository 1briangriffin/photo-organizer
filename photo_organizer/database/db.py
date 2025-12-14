"""
Database connection management.
"""
import sqlite3
import logging
from pathlib import Path
from typing import Optional

from .schema import init_schema

class DBManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> sqlite3.Connection:
        """
        Connects to the SQLite database and configures performance pragmas.
        """
        if self._conn:
            return self._conn

        logging.info(f"Connecting to database: {self.db_path}")
        self._conn = sqlite3.connect(self.db_path)
        
        # Performance Tuning (Safe for single-writer, multi-reader)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA temp_store=MEMORY;")
        self._conn.execute("PRAGMA cache_size=-200000;") # ~200MB cache
        self._conn.execute("PRAGMA foreign_keys=ON;")

        # Ensure schema exists
        init_schema(self._conn)
        
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()