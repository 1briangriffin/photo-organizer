"""
Face scan pipeline tests: unscanned-file selection, no-faces sentinel,
detection/embedding persistence with run provenance, idempotent re-runs,
and the photo-faces CLI wrapper.

Uses a fake detector — insightface is never loaded.
"""
import json
import sqlite3
from pathlib import Path

import pytest
from PIL import Image

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces import config
from photo_organizer.faces.db_ops import NO_FACES_DETECTION_INDEX, FaceDBOperations
from photo_organizer.faces.detection import FaceScanPipeline


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    try:
        yield db_path, conn, DBOperations(conn)
    finally:
        conn.close()


def _seed_catalog_file(conn, *, file_id, file_type="jpeg", dest_path=None,
                       orig_path="C:/src/x.jpg", file_hash=None,
                       status="active"):
    conn.execute(
        """
        INSERT INTO files (id, hash, type, ext, orig_name, orig_path, dest_path,
                           first_seen_at, last_seen_at, status)
        VALUES (?, ?, ?, '.jpg', 'x.jpg', ?, ?,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', ?)
        """,
        (file_id, file_hash or f"hash-{file_id}", file_type,
         str(orig_path), str(dest_path) if dest_path else None, status),
    )


def _start_run(conn):
    return conn.execute(
        """
        INSERT INTO command_runs (tool, command, started_at, exit_status,
                                  dry_run, argv_json)
        VALUES ('photo-faces', 'scan', '2026-01-01T00:00:00Z', 'running', 0, '[]')
        """
    ).lastrowid


def _write_jpeg(path: Path, size=(120, 100)):
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=(180, 120, 90)).save(path)


class FakeDetector:
    """Stands in for the insightface wrapper: fixed detections per image."""

    def __init__(self, faces_by_stem):
        self.faces_by_stem = faces_by_stem
        self.calls = 0

    def detect_faces(self, img_bgr):
        self.calls += 1
        # The pipeline passes decoded arrays, so route on image dimensions
        # stashed by the test via faces_by_stem key '*' (same for all) or
        # per-call sequencing.
        return self.faces_by_stem


def _fake_face(index=0, score=0.9):
    return {
        "bbox": (10 + index, 12, 30, 32),
        "embedding": [0.1 * (index + 1)] * 8,
        "det_score": score,
        "age": 30 + index,
        "gender": "F",
        "landmarks": None,
    }


# ---------------------------------------------------------------------------
# get_unscanned_files
# ---------------------------------------------------------------------------

def test_unscanned_files_scope(db):
    db_path, conn, ops = db
    face_ops = FaceDBOperations(ops)
    _seed_catalog_file(conn, file_id=1, file_type="jpeg")
    _seed_catalog_file(conn, file_id=2, file_type="tiff")
    _seed_catalog_file(conn, file_id=3, file_type="raw")            # unlinked RAW
    _seed_catalog_file(conn, file_id=4, file_type="raw")            # linked RAW
    _seed_catalog_file(conn, file_id=5, file_type="jpeg", status="retired")
    _seed_catalog_file(conn, file_id=6, file_type="video")
    conn.execute(
        "INSERT INTO raw_outputs (raw_file_id, output_file_id) VALUES (4, 1)"
    )
    conn.commit()

    default_ids = [row[0] for row in face_ops.get_unscanned_files(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )]
    assert default_ids == [1, 2], "default scope is jpeg/tiff, active only"

    with_raw = [row[0] for row in face_ops.get_unscanned_files(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        eligible_types=config.RAW_FALLBACK_TYPES,
    )]
    assert with_raw == [1, 2, 3], "linked RAW (id 4) is always excluded"

    limited = face_ops.get_unscanned_files(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        limit=1,
    )
    assert len(limited) == 1


def test_no_faces_sentinel_marks_file_scanned(db):
    db_path, conn, ops = db
    face_ops = FaceDBOperations(ops)
    _seed_catalog_file(conn, file_id=1)
    run_id = _start_run(conn)
    conn.commit()

    face_ops.record_no_faces_scan(
        run_id=run_id, file_id=1,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        image_hash="hash-1",
    )
    # Idempotent under the detection identity index.
    face_ops.record_no_faces_scan(
        run_id=run_id, file_id=1,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        image_hash="hash-1",
    )
    conn.commit()

    rows = conn.execute(
        "SELECT detection_index, status, observed_by_run_id FROM face_detections"
    ).fetchall()
    assert rows == [(NO_FACES_DETECTION_INDEX, "no_faces", run_id)]

    remaining = face_ops.get_unscanned_files(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    assert remaining == []

    # A different model version re-scans the file.
    other_model = face_ops.get_unscanned_files(
        model_name=config.MODEL_NAME, model_version="buffalo_l_v2",
    )
    assert [row[0] for row in other_model] == [1]


# ---------------------------------------------------------------------------
# FaceScanPipeline
# ---------------------------------------------------------------------------

def _build_scan_fixture(tmp_path, faces):
    dest = tmp_path / "dest"
    img_with_face = dest / "output" / "party.jpg"
    _write_jpeg(img_with_face)

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    _seed_catalog_file(conn, file_id=1, dest_path=img_with_face,
                       orig_path=img_with_face)
    run_id = _start_run(conn)
    conn.commit()
    conn.close()

    pipeline = FaceScanPipeline(
        db_path,
        thumbnail_dir=tmp_path / "thumbs",
        detector=FakeDetector(faces),
    )
    return db_path, pipeline, run_id


def test_scan_records_detections_embeddings_and_thumbnails(tmp_path):
    faces = [_fake_face(0), _fake_face(1, score=0.7)]
    db_path, pipeline, run_id = _build_scan_fixture(tmp_path, faces)

    stats = pipeline.run(run_id=run_id)
    assert stats["images_scanned"] == 1
    assert stats["faces_detected"] == 2
    assert stats["images_failed"] == 0

    conn = sqlite3.connect(str(db_path))
    try:
        detections = conn.execute(
            """
            SELECT id, detection_index, confidence, status,
                   observed_by_run_id, image_hash, payload_json
            FROM face_detections ORDER BY detection_index
            """
        ).fetchall()
        embeddings = conn.execute(
            "SELECT detection_id, vector_dim, observed_by_run_id FROM face_embeddings"
        ).fetchall()
        action_types = {row[0] for row in conn.execute(
            "SELECT DISTINCT action_type FROM run_actions WHERE proposed_by_run_id = ?",
            (run_id,),
        )}
    finally:
        conn.close()

    assert len(detections) == 2
    for det, expected_index in zip(detections, (0, 1)):
        det_id, index, confidence, status, by_run, image_hash, payload_json = det
        assert index == expected_index
        assert status == "observed"
        assert by_run == run_id
        assert image_hash == "hash-1"
        payload = json.loads(payload_json)
        assert payload["estimated_age"] == 30 + expected_index
        assert payload["estimated_gender"] == "F"
        assert (tmp_path / "thumbs" / payload["thumbnail_path"]).exists()

    assert [(row[1], row[2]) for row in embeddings] == [(8, run_id), (8, run_id)]
    assert {"face_detect", "face_embed"} <= action_types


def test_scan_is_idempotent_and_records_no_faces_sentinel(tmp_path):
    db_path, pipeline, run_id = _build_scan_fixture(tmp_path, faces=[])

    stats = pipeline.run(run_id=run_id)
    assert stats["images_scanned"] == 1
    assert stats["images_no_faces"] == 1
    assert stats["faces_detected"] == 0

    # Second run has nothing to do — the sentinel marks the file scanned.
    stats_again = pipeline.run(run_id=run_id)
    assert stats_again["images_scanned"] == 0
    assert pipeline.detector.calls == 1


def test_scan_aborts_when_detection_stack_is_missing(tmp_path):
    class MissingStackDetector:
        def detect_faces(self, img_bgr):
            raise ImportError("insightface is required for face detection.")

    db_path, pipeline, run_id = _build_scan_fixture(tmp_path, faces=[])
    pipeline.detector = MissingStackDetector()

    with pytest.raises(ImportError):
        pipeline.run(run_id=run_id)


def test_scan_counts_missing_files_as_failed(tmp_path):
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    _seed_catalog_file(conn, file_id=1, dest_path=tmp_path / "gone.jpg",
                       orig_path=tmp_path / "also-gone.jpg")
    run_id = _start_run(conn)
    conn.commit()
    conn.close()

    pipeline = FaceScanPipeline(
        db_path, thumbnail_dir=tmp_path / "thumbs",
        detector=FakeDetector([]),
    )
    stats = pipeline.run(run_id=run_id)
    assert stats["images_failed"] == 1
    assert stats["images_scanned"] == 0


# ---------------------------------------------------------------------------
# photo-faces CLI
# ---------------------------------------------------------------------------

def test_cli_missing_db_returns_error(tmp_path):
    from photo_organizer.faces.cli import main

    code = main(["--db", str(tmp_path / "nope.db"), "scan", "--cpu"])
    assert code == 1


def test_cli_scan_records_command_run(tmp_path):
    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.commit()
    conn.close()

    # Empty catalog: scan exits before the detector would load insightface.
    code = main(["--db", str(db_path), "scan", "--cpu"])
    assert code == 0

    conn = sqlite3.connect(str(db_path))
    try:
        run = conn.execute(
            """
            SELECT tool, command, exit_status, db_mutates, params_json, stats_json
            FROM command_runs
            """
        ).fetchone()
    finally:
        conn.close()

    tool, command, exit_status, db_mutates, params_json, stats_json = run
    assert (tool, command, exit_status, db_mutates) == ("photo-faces", "scan", "success", 0)
    params = json.loads(params_json)
    assert params["model_version"] == config.MODEL_VERSION_TAG
    assert params["use_gpu"] is False
    stats = json.loads(stats_json)
    assert stats["images_scanned"] == 0
