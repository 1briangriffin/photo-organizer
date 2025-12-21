"""
Database schema definitions.
"""
import sqlite3
import logging

CURRENT_SCHEMA_VERSION = 1

def init_schema(conn: sqlite3.Connection):
    """
    Applies the core schema to the database.
    Idempotent: safe to run on every startup.
    """
    with conn:
        # 1. Version Tracking (For future migrations)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            );
        """)
        
        # Initialize version if missing
        cur = conn.cursor()
        cur.execute("SELECT version FROM schema_version")
        if not cur.fetchone():
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (CURRENT_SCHEMA_VERSION,))

        # 2. Core File Table
        # Stores the "Identity" of the file (Size, Hash, Path)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            hash            TEXT UNIQUE,          -- Full SHA-256 if known
            sparse_hash     TEXT,                 -- Sparse fingerprint hint for large files
            type            TEXT NOT NULL,
            ext             TEXT NOT NULL,
            orig_name       TEXT NOT NULL,
            orig_path       TEXT NOT NULL,
            dest_path       TEXT,
            size_bytes      INTEGER,
            is_seed         INTEGER NOT NULL DEFAULT 0,
            name_score      INTEGER NOT NULL DEFAULT 0,
            first_seen_at   TEXT NOT NULL,
            last_seen_at    TEXT NOT NULL
        );
        """)

        # 3. Media Metadata (Enrichment Phase)
        # Stores "Content" info (Time, Dimensions, Camera)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS media_metadata (
            file_id         INTEGER PRIMARY KEY,
            capture_datetime TEXT,
            camera_model    TEXT,
            lens_model      TEXT,
            width           INTEGER,
            height          INTEGER,
            duration_sec    REAL,
            aspect_ratio    REAL,
            phash           TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );
        """)

        # 4. Linking Tables (Relationships)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_sidecars (
            raw_file_id      INTEGER NOT NULL,
            sidecar_file_id  INTEGER NOT NULL,
            PRIMARY KEY (raw_file_id, sidecar_file_id),
            FOREIGN KEY(raw_file_id) REFERENCES files(id) ON DELETE CASCADE,
            FOREIGN KEY(sidecar_file_id) REFERENCES files(id) ON DELETE CASCADE
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_outputs (
            raw_file_id      INTEGER NOT NULL,
            output_file_id   INTEGER NOT NULL,
            link_method      TEXT,
            confidence       INTEGER,
            PRIMARY KEY (raw_file_id, output_file_id),
            FOREIGN KEY(raw_file_id) REFERENCES files(id) ON DELETE CASCADE,
            FOREIGN KEY(output_file_id) REFERENCES files(id) ON DELETE CASCADE
        );
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS psd_source_links (
            psd_file_id      INTEGER PRIMARY KEY,
            source_file_id   INTEGER NOT NULL,
            confidence       INTEGER NOT NULL,
            link_method      TEXT NOT NULL,
            linked_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(psd_file_id) REFERENCES files(id) ON DELETE CASCADE,
            FOREIGN KEY(source_file_id) REFERENCES files(id) ON DELETE CASCADE
        );
        """)

        # 5. Indices for Performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_type ON files(type);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_sparse_hash ON files(sparse_hash);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_capture_dt ON media_metadata(capture_datetime);")

        # 6. Logging (Scan Session Data)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS file_occurrences (
            path TEXT PRIMARY KEY,
            file_id INTEGER NOT NULL,
            is_seed INTEGER NOT NULL DEFAULT 0,
            seen_at REAL NOT NULL, 
            mtime REAL NOT NULL,
            size_bytes INTEGER NOT NULL,
            hash TEXT NOT NULL,
            hash_is_sparse INTEGER NOT NULL DEFAULT 0,         
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        )
    """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_occurrences_hash ON file_occurrences(hash);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_occurrences_file_id ON file_occurrences(file_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_occurrences_mtime ON file_occurrences(mtime);")

    logging.debug("Database schema initialized.")
