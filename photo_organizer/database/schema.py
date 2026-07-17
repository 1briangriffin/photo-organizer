"""
Database schema definitions and migrations.
"""
import logging
import sqlite3

CURRENT_SCHEMA_VERSION = 9
# Version history coordination: v4 = face tables, v7 = proposal lifecycle
# columns, v8 = file retirement status, v9 = face clustering support
# (birth dates + cluster era/representative columns). New migrations must
# start at v10 or later.


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def _create_core_schema(conn: sqlite3.Connection) -> None:
    """Create the v2 schema surface idempotently."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        hash            TEXT UNIQUE,
        sparse_hash     TEXT,
        type            TEXT NOT NULL,
        ext             TEXT NOT NULL,
        orig_name       TEXT NOT NULL,
        orig_path       TEXT NOT NULL,
        dest_path       TEXT,
        size_bytes      INTEGER,
        is_seed         INTEGER NOT NULL DEFAULT 0,
        name_score      INTEGER NOT NULL DEFAULT 0,
        first_seen_at   TEXT NOT NULL,
        last_seen_at    TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'active',
        status_changed_at TEXT,
        status_changed_by_run_id INTEGER REFERENCES command_runs(id),
        status_note     TEXT
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS media_metadata (
        file_id          INTEGER PRIMARY KEY,
        capture_datetime TEXT,
        camera_model     TEXT,
        camera_serial_number TEXT,
        camera_file_number TEXT,
        lens_model       TEXT,
        width            INTEGER,
        height           INTEGER,
        duration_sec     REAL,
        aspect_ratio     REAL,
        phash            TEXT,
        FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
    );
    """)

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

    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_type ON files(type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_sparse_hash ON files(sparse_hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_media_capture_dt ON media_metadata(capture_datetime);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_type_dest ON files(type, dest_path);")

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
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_occurrences_hash ON file_occurrences(hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_occurrences_file_id ON file_occurrences(file_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_occurrences_mtime ON file_occurrences(mtime);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS command_runs (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        tool            TEXT NOT NULL,
        command         TEXT NOT NULL,
        started_at      TEXT NOT NULL,
        finished_at     TEXT,
        exit_status     TEXT NOT NULL,
        dry_run         INTEGER NOT NULL DEFAULT 0,
        db_mutates      INTEGER NOT NULL DEFAULT 0,
        files_mutate    INTEGER NOT NULL DEFAULT 0,
        db_path         TEXT,
        src_root        TEXT,
        dest_root       TEXT,
        argv_json       TEXT NOT NULL,
        params_json     TEXT,
        stats_json      TEXT,
        error_type      TEXT,
        error_message   TEXT,
        app_version     TEXT
    );
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_command_runs_started_at ON command_runs(started_at);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_command_runs_tool_command ON command_runs(tool, command);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_command_runs_exit_status ON command_runs(exit_status);")


def _path_key_sql_expr(column: str) -> str:
    """SQLite expression matching the initial Windows-oriented path-key policy."""
    return f"replace(lower(rtrim({column}, '/' || char(92))), '/', char(92))"


def _migration_3_catalog_state(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS file_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL REFERENCES command_runs(id),
        file_id INTEGER REFERENCES files(id),
        observed_at TEXT NOT NULL,
        observation_type TEXT NOT NULL,
        path TEXT NOT NULL,
        path_key TEXT NOT NULL,
        root_kind TEXT NOT NULL,
        root_path_key TEXT,
        hash TEXT,
        sparse_hash TEXT,
        hash_is_sparse INTEGER NOT NULL DEFAULT 0,
        size_bytes INTEGER,
        mtime REAL,
        match_method TEXT,
        confidence INTEGER,
        payload_json TEXT
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_observations_run_id ON file_observations(run_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_observations_file_time ON file_observations(file_id, observed_at);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_observations_path_key ON file_observations(path_key);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_observations_type_run ON file_observations(observation_type, run_id);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS file_location_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER NOT NULL REFERENCES files(id),
        path TEXT NOT NULL,
        path_key TEXT NOT NULL UNIQUE,
        root_kind TEXT NOT NULL,
        root_path_key TEXT,
        status TEXT NOT NULL,
        first_observed_run_id INTEGER REFERENCES command_runs(id),
        last_observed_run_id INTEGER REFERENCES command_runs(id),
        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        hash TEXT,
        sparse_hash TEXT,
        hash_is_sparse INTEGER NOT NULL DEFAULT 0,
        size_bytes INTEGER,
        mtime REAL
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_location_state_file_status ON file_location_state(file_id, status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_location_state_root_path ON file_location_state(root_kind, path_key);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_location_state_last_run ON file_location_state(last_observed_run_id);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS run_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposed_by_run_id INTEGER NOT NULL REFERENCES command_runs(id),
        applied_by_run_id INTEGER REFERENCES command_runs(id),
        action_type TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id INTEGER,
        source_path TEXT,
        source_path_key TEXT,
        target_path TEXT,
        target_path_key TEXT,
        status TEXT NOT NULL,
        confidence INTEGER,
        method TEXT,
        idempotency_key TEXT NOT NULL,
        phase INTEGER NOT NULL,
        sequence INTEGER NOT NULL DEFAULT 0,
        depends_on_action_id INTEGER REFERENCES run_actions(id),
        payload_json TEXT,
        created_at TEXT NOT NULL,
        applied_at TEXT,
        error_message TEXT,
        resolved_by_run_id INTEGER REFERENCES command_runs(id),
        resolved_at TEXT,
        resolution_note TEXT
    );
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_run_actions_idempotency_key ON run_actions(idempotency_key);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_actions_proposed_run ON run_actions(proposed_by_run_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_actions_applied_run ON run_actions(applied_by_run_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_actions_status_phase ON run_actions(status, phase, sequence, id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_actions_entity ON run_actions(entity_type, entity_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_actions_type_status ON run_actions(action_type, status);")

    for table in ("raw_outputs", "raw_sidecars", "psd_source_links"):
        if not _column_exists(conn, table, "created_by_run_id"):
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN created_by_run_id INTEGER REFERENCES command_runs(id)"
            )

    path_key_expr = _path_key_sql_expr("f.dest_path")
    conn.execute(f"""
        INSERT OR IGNORE INTO file_location_state (
            file_id, path, path_key, root_kind, root_path_key, status,
            first_observed_run_id, last_observed_run_id, first_seen_at, last_seen_at,
            hash, sparse_hash, hash_is_sparse, size_bytes, mtime
        )
        SELECT
            f.id,
            f.dest_path,
            {path_key_expr},
            'dest',
            NULL,
            'present',
            NULL,
            NULL,
            f.last_seen_at,
            f.last_seen_at,
            f.hash,
            f.sparse_hash,
            CASE WHEN f.hash IS NULL AND f.sparse_hash IS NOT NULL THEN 1 ELSE 0 END,
            f.size_bytes,
            fo.mtime
        FROM files f
        LEFT JOIN file_occurrences fo ON fo.path = f.dest_path
        WHERE f.dest_path IS NOT NULL
    """)

    occ_path_key_expr = _path_key_sql_expr("fo.path")
    conn.execute(f"""
        INSERT OR IGNORE INTO file_location_state (
            file_id, path, path_key, root_kind, root_path_key, status,
            first_observed_run_id, last_observed_run_id, first_seen_at, last_seen_at,
            hash, sparse_hash, hash_is_sparse, size_bytes, mtime
        )
        SELECT
            fo.file_id,
            fo.path,
            {occ_path_key_expr},
            'unknown',
            NULL,
            'present',
            NULL,
            NULL,
            datetime(fo.seen_at, 'unixepoch'),
            datetime(fo.seen_at, 'unixepoch'),
            CASE WHEN fo.hash_is_sparse = 0 THEN fo.hash ELSE NULL END,
            CASE WHEN fo.hash_is_sparse = 1 THEN fo.hash ELSE NULL END,
            fo.hash_is_sparse,
            fo.size_bytes,
            fo.mtime
        FROM file_occurrences fo
    """)


def _migration_4_face_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS face_persons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        display_name TEXT,
        birth_date TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_by_run_id INTEGER REFERENCES command_runs(id),
        updated_by_run_id INTEGER REFERENCES command_runs(id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        payload_json TEXT
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_persons_status ON face_persons(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_persons_created_run ON face_persons(created_by_run_id);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS face_detections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
        detection_index INTEGER NOT NULL,
        bbox_x REAL NOT NULL,
        bbox_y REAL NOT NULL,
        bbox_w REAL NOT NULL,
        bbox_h REAL NOT NULL,
        confidence REAL,
        model_name TEXT NOT NULL,
        model_version TEXT NOT NULL,
        image_hash TEXT,
        status TEXT NOT NULL DEFAULT 'observed',
        observed_by_run_id INTEGER NOT NULL REFERENCES command_runs(id),
        created_at TEXT NOT NULL,
        payload_json TEXT
    );
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_face_detections_identity
        ON face_detections(file_id, model_name, model_version, COALESCE(image_hash, ''), detection_index);
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_detections_file ON face_detections(file_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_detections_run ON face_detections(observed_by_run_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_detections_model ON face_detections(model_name, model_version);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS face_embeddings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detection_id INTEGER NOT NULL REFERENCES face_detections(id) ON DELETE CASCADE,
        model_name TEXT NOT NULL,
        model_version TEXT NOT NULL,
        vector_dim INTEGER NOT NULL,
        embedding BLOB NOT NULL,
        observed_by_run_id INTEGER NOT NULL REFERENCES command_runs(id),
        created_at TEXT NOT NULL
    );
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_face_embeddings_detection_model
        ON face_embeddings(detection_id, model_name, model_version);
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_embeddings_run ON face_embeddings(observed_by_run_id);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS face_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cluster_key TEXT NOT NULL,
        model_name TEXT NOT NULL,
        model_version TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'proposed',
        era_start TEXT,
        era_end TEXT,
        representative_embedding BLOB,
        representative_dim INTEGER,
        created_by_run_id INTEGER REFERENCES command_runs(id),
        updated_by_run_id INTEGER REFERENCES command_runs(id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        payload_json TEXT
    );
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_face_clusters_identity
        ON face_clusters(cluster_key, model_name, model_version);
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_clusters_status ON face_clusters(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_clusters_run ON face_clusters(created_by_run_id);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS face_cluster_members (
        cluster_id INTEGER NOT NULL REFERENCES face_clusters(id) ON DELETE CASCADE,
        detection_id INTEGER NOT NULL REFERENCES face_detections(id) ON DELETE CASCADE,
        status TEXT NOT NULL DEFAULT 'proposed',
        confidence REAL,
        created_by_run_id INTEGER REFERENCES command_runs(id),
        updated_by_run_id INTEGER REFERENCES command_runs(id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (cluster_id, detection_id)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_cluster_members_detection ON face_cluster_members(detection_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_cluster_members_status ON face_cluster_members(status);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS face_person_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        person_id INTEGER NOT NULL REFERENCES face_persons(id) ON DELETE CASCADE,
        detection_id INTEGER REFERENCES face_detections(id) ON DELETE CASCADE,
        cluster_id INTEGER REFERENCES face_clusters(id) ON DELETE CASCADE,
        confidence REAL,
        link_method TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'accepted',
        created_by_run_id INTEGER REFERENCES command_runs(id),
        updated_by_run_id INTEGER REFERENCES command_runs(id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        payload_json TEXT,
        CHECK (detection_id IS NOT NULL OR cluster_id IS NOT NULL)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_person_links_person ON face_person_links(person_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_person_links_detection ON face_person_links(detection_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_person_links_cluster ON face_person_links(cluster_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_person_links_status ON face_person_links(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_face_person_links_run ON face_person_links(created_by_run_id);")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_face_person_links_person_detection_unique
        ON face_person_links(person_id, detection_id)
        WHERE detection_id IS NOT NULL;
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_face_person_links_person_cluster_unique
        ON face_person_links(person_id, cluster_id)
        WHERE cluster_id IS NOT NULL;
    """)


def _migration_5_run_action_attempt_history(conn: sqlite3.Connection) -> None:
    conn.execute("DROP INDEX IF EXISTS idx_run_actions_idempotency_key;")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_run_actions_run_idempotency_key
        ON run_actions(proposed_by_run_id, idempotency_key);
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_run_actions_idempotency_key
        ON run_actions(idempotency_key);
    """)


def _migration_6_camera_identity_metadata(conn: sqlite3.Connection) -> None:
    if not _column_exists(conn, "media_metadata", "camera_serial_number"):
        conn.execute("ALTER TABLE media_metadata ADD COLUMN camera_serial_number TEXT")
    if not _column_exists(conn, "media_metadata", "camera_file_number"):
        conn.execute("ALTER TABLE media_metadata ADD COLUMN camera_file_number TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_camera_file_number "
        "ON media_metadata(camera_model, camera_serial_number, camera_file_number)"
    )


def _migration_7_proposal_lifecycle(conn: sqlite3.Connection) -> None:
    if not _column_exists(conn, "run_actions", "resolved_by_run_id"):
        conn.execute(
            "ALTER TABLE run_actions ADD COLUMN resolved_by_run_id INTEGER REFERENCES command_runs(id)"
        )
    if not _column_exists(conn, "run_actions", "resolved_at"):
        conn.execute("ALTER TABLE run_actions ADD COLUMN resolved_at TEXT")
    if not _column_exists(conn, "run_actions", "resolution_note"):
        conn.execute("ALTER TABLE run_actions ADD COLUMN resolution_note TEXT")


def _migration_8_file_retirement(conn: sqlite3.Connection) -> None:
    """files.status marks intentionally deleted/retired catalog entries.

    'active' (default) — expected on disk; 'retired' — the user deliberately
    removed the file, so maintenance commands stop reporting it as missing
    while its history (hashes, links, occurrences) stays queryable.
    """
    if not _column_exists(conn, "files", "status"):
        conn.execute("ALTER TABLE files ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
    if not _column_exists(conn, "files", "status_changed_at"):
        conn.execute("ALTER TABLE files ADD COLUMN status_changed_at TEXT")
    if not _column_exists(conn, "files", "status_changed_by_run_id"):
        conn.execute(
            "ALTER TABLE files ADD COLUMN status_changed_by_run_id INTEGER REFERENCES command_runs(id)"
        )
    if not _column_exists(conn, "files", "status_note"):
        conn.execute("ALTER TABLE files ADD COLUMN status_note TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);")


def _migration_9_face_clustering(conn: sqlite3.Connection) -> None:
    """Era-based clustering support: person birth dates drive developmental
    era windows; clusters carry their era range and a representative
    (L2-normalized mean) embedding for cross-age linking."""
    if not _column_exists(conn, "face_persons", "birth_date"):
        conn.execute("ALTER TABLE face_persons ADD COLUMN birth_date TEXT")
    if not _column_exists(conn, "face_clusters", "era_start"):
        conn.execute("ALTER TABLE face_clusters ADD COLUMN era_start TEXT")
    if not _column_exists(conn, "face_clusters", "era_end"):
        conn.execute("ALTER TABLE face_clusters ADD COLUMN era_end TEXT")
    if not _column_exists(conn, "face_clusters", "representative_embedding"):
        conn.execute("ALTER TABLE face_clusters ADD COLUMN representative_embedding BLOB")
    if not _column_exists(conn, "face_clusters", "representative_dim"):
        conn.execute("ALTER TABLE face_clusters ADD COLUMN representative_dim INTEGER")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_face_clusters_era "
        "ON face_clusters(era_start, era_end);"
    )


MIGRATIONS = {
    3: _migration_3_catalog_state,
    4: _migration_4_face_tables,
    5: _migration_5_run_action_attempt_history,
    6: _migration_6_camera_identity_metadata,
    7: _migration_7_proposal_lifecycle,
    8: _migration_8_file_retirement,
    9: _migration_9_face_clustering,
}


def _run_migrations(conn: sqlite3.Connection, current_version: int) -> None:
    for version in range(current_version + 1, CURRENT_SCHEMA_VERSION + 1):
        migration = MIGRATIONS.get(version)
        if migration is not None:
            migration(conn)
        conn.execute("DELETE FROM schema_version")
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))


def init_schema(conn: sqlite3.Connection):
    """
    Applies the core schema and all pending migrations.
    Idempotent: safe to run on every startup.

    Fast path: a catalog already at CURRENT_SCHEMA_VERSION returns after a
    single read. Startup must not take write locks on an up-to-date catalog —
    a long-running command (scan, rethumb) may hold the writer lock, and
    every other tool (UI included) opens a connection through here.
    """
    try:
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        if row is not None and int(row[0]) == CURRENT_SCHEMA_VERSION:
            return
    except sqlite3.OperationalError:
        pass  # schema_version doesn't exist yet — fresh catalog.

    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            );
        """)

        row = conn.execute("SELECT version FROM schema_version").fetchone()
        current_version = int(row[0]) if row else 0

        _create_core_schema(conn)

        if current_version == 0:
            conn.execute("INSERT INTO schema_version (version) VALUES (2)")
            current_version = 2

        if current_version < CURRENT_SCHEMA_VERSION:
            _run_migrations(conn, current_version)

    logging.debug("Database schema initialized.")
