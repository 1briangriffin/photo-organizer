import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces.db_ops import FaceDBOperations
from photo_organizer.models import FileRecord


def _make_conn(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(tmp_path / "catalog.db"))
    init_schema(conn)
    return conn


def _start_run(conn: sqlite3.Connection, command: str = "faces_detect") -> int:
    cur = conn.execute(
        """
        INSERT INTO command_runs (
            tool, command, started_at, exit_status, dry_run, argv_json
        )
        VALUES ('photo-faces', ?, '2026-01-01T00:00:00Z', 'running', 0, '[]')
        """,
        (command,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_image(conn: sqlite3.Connection, image_path: Path) -> int:
    image_path.write_bytes(b"jpeg bytes" * 100)
    ops = DBOperations(conn)
    content = image_path.read_bytes()
    rec = FileRecord(
        hash=hashlib.sha256(content).hexdigest(),
        sparse_hash=None,
        hash_is_sparse=False,
        type="jpeg",
        ext=".jpg",
        orig_name=image_path.name,
        orig_path=image_path,
        size_bytes=len(content),
        mtime=image_path.stat().st_mtime,
        is_seed=False,
        name_score=1,
        capture_datetime=datetime(2023, 1, 1),
    )
    file_id = ops.upsert_file_record(rec)
    ops.upsert_media_metadata(file_id, rec)
    return file_id


def test_record_detection_and_embedding_are_run_provenanced(tmp_path):
    conn = _make_conn(tmp_path)
    try:
        image_id = _seed_image(conn, tmp_path / "face.jpg")
        run_id = _start_run(conn)
        faces = FaceDBOperations(DBOperations(conn))

        detection_id = faces.record_detection(
            run_id=run_id,
            file_id=image_id,
            detection_index=0,
            bbox=(10.0, 20.0, 30.0, 40.0),
            confidence=0.98,
            model_name="detector",
            model_version="1.0",
            image_hash="imagehash",
        )
        embedding_id = faces.record_embedding(
            run_id=run_id,
            detection_id=detection_id,
            embedding=[0.1, 0.2, 0.3],
            model_name="embedder",
            model_version="1.0",
        )
        conn.commit()

        det_row = conn.execute(
            """
            SELECT file_id, observed_by_run_id, model_name, model_version, status
            FROM face_detections WHERE id = ?
            """,
            (detection_id,),
        ).fetchone()
        emb_row = conn.execute(
            """
            SELECT detection_id, vector_dim, observed_by_run_id
            FROM face_embeddings WHERE id = ?
            """,
            (embedding_id,),
        ).fetchone()
        actions = conn.execute(
            """
            SELECT action_type, status, applied_by_run_id
            FROM run_actions
            WHERE proposed_by_run_id = ?
            ORDER BY phase
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    assert det_row == (image_id, run_id, "detector", "1.0", "observed")
    assert emb_row == (detection_id, 3, run_id)
    assert ("face_detect", "applied", run_id) in actions
    assert ("face_embed", "applied", run_id) in actions


def test_cluster_assignment_is_proposed_action_not_accepted_state(tmp_path):
    conn = _make_conn(tmp_path)
    try:
        image_id = _seed_image(conn, tmp_path / "face.jpg")
        run_id = _start_run(conn, command="faces_cluster")
        faces = FaceDBOperations(DBOperations(conn))
        detection_id = faces.record_detection(
            run_id=run_id,
            file_id=image_id,
            detection_index=0,
            bbox=(1.0, 2.0, 3.0, 4.0),
            confidence=0.90,
            model_name="detector",
            model_version="1.0",
            image_hash="hash",
        )

        cluster_id = faces.propose_cluster_assignment(
            run_id=run_id,
            detection_id=detection_id,
            cluster_key="cluster-a",
            model_name="clusterer",
            model_version="1.0",
            confidence=0.75,
        )
        conn.commit()

        cluster = conn.execute(
            "SELECT status, created_by_run_id FROM face_clusters WHERE id = ?",
            (cluster_id,),
        ).fetchone()
        action = conn.execute(
            """
            SELECT action_type, status, applied_by_run_id
            FROM run_actions
            WHERE action_type = 'face_cluster_assign'
            """,
        ).fetchone()
        members = conn.execute(
            "SELECT cluster_id, detection_id, status, confidence FROM face_cluster_members"
        ).fetchall()
    finally:
        conn.close()

    assert cluster == ("proposed", run_id)
    assert action == ("face_cluster_assign", "proposed", None)
    assert members == [(cluster_id, detection_id, "proposed", 0.75)]


def test_person_link_is_accepted_state_with_run_action(tmp_path):
    conn = _make_conn(tmp_path)
    try:
        image_id = _seed_image(conn, tmp_path / "face.jpg")
        run_id = _start_run(conn, command="faces_link")
        faces = FaceDBOperations(DBOperations(conn))
        detection_id = faces.record_detection(
            run_id=run_id,
            file_id=image_id,
            detection_index=0,
            bbox=(1.0, 2.0, 3.0, 4.0),
            confidence=0.90,
            model_name="detector",
            model_version="1.0",
            image_hash="hash",
        )
        person_id = faces.create_person(run_id=run_id, display_name="Ada")
        link_id = faces.link_detection_to_person(
            run_id=run_id,
            detection_id=detection_id,
            person_id=person_id,
            confidence=1.0,
            link_method="manual_seed",
        )
        conn.commit()

        link = conn.execute(
            """
            SELECT person_id, detection_id, status, created_by_run_id
            FROM face_person_links WHERE id = ?
            """,
            (link_id,),
        ).fetchone()
        action = conn.execute(
            """
            SELECT action_type, status, applied_by_run_id
            FROM run_actions
            WHERE action_type = 'face_person_link'
            """,
        ).fetchone()
    finally:
        conn.close()

    assert link == (person_id, detection_id, "accepted", run_id)
    assert action == ("face_person_link", "applied", run_id)


def test_person_link_is_idempotent(tmp_path):
    conn = _make_conn(tmp_path)
    try:
        image_id = _seed_image(conn, tmp_path / "face.jpg")
        run_id = _start_run(conn, command="faces_link")
        faces = FaceDBOperations(DBOperations(conn))
        detection_id = faces.record_detection(
            run_id=run_id,
            file_id=image_id,
            detection_index=0,
            bbox=(1.0, 2.0, 3.0, 4.0),
            confidence=0.90,
            model_name="detector",
            model_version="1.0",
            image_hash="hash",
        )
        person_id = faces.create_person(run_id=run_id, display_name="Ada")

        first_id = faces.link_detection_to_person(
            run_id=run_id,
            detection_id=detection_id,
            person_id=person_id,
            confidence=1.0,
            link_method="manual_seed",
        )
        second_id = faces.link_detection_to_person(
            run_id=run_id,
            detection_id=detection_id,
            person_id=person_id,
            confidence=1.0,
            link_method="manual_seed",
        )
        conn.commit()

        link_count = conn.execute("SELECT COUNT(*) FROM face_person_links").fetchone()[0]
        action_count = conn.execute(
            "SELECT COUNT(*) FROM run_actions WHERE action_type = 'face_person_link'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert first_id == second_id
    assert link_count == 1
    assert action_count == 1
