"""
Photo-first labeling tests: sampling photos by labeling value, per-photo
detection resolution, the thumbnail bbox-scale regression, and the Label
Photos UI flow (label a face, apply to its cluster, not-a-face).
"""
import sqlite3
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces import config
from photo_organizer.faces.db_ops import FaceDBOperations


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    try:
        yield db_path, conn, FaceDBOperations(DBOperations(conn))
    finally:
        conn.close()


def _start_run(conn, command="scan"):
    return conn.execute(
        """
        INSERT INTO command_runs (tool, command, started_at, exit_status,
                                  dry_run, argv_json)
        VALUES ('photo-faces', ?, '2026-01-01T00:00:00Z', 'running', 0, '[]')
        """,
        (command,),
    ).lastrowid


def _unit(vec):
    arr = np.asarray(vec, dtype=np.float32)
    return (arr / np.linalg.norm(arr)).tolist()


def _seed_photo(conn, *, file_id, path="C:/x.jpg", capture=None):
    conn.execute(
        """
        INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name, orig_path,
                                     dest_path, first_seen_at, last_seen_at)
        VALUES (?, ?, 'jpeg', '.jpg', 'x.jpg', ?, ?,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """,
        (file_id, f"hash-{file_id}", str(path), str(path)),
    )
    if capture:
        conn.execute(
            "INSERT OR IGNORE INTO media_metadata (file_id, capture_datetime) "
            "VALUES (?, ?)",
            (file_id, capture),
        )


def _seed_face(conn, face_ops, *, run_id, file_id, index=0, cluster_key=None,
               confidence=0.9, bbox=(10.0, 10.0, 40.0, 40.0)):
    det = face_ops.record_detection(
        run_id=run_id, file_id=file_id, detection_index=index,
        bbox=bbox, confidence=confidence,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        image_hash=f"hash-{file_id}",
    )
    face_ops.record_embedding(
        run_id=run_id, detection_id=det, embedding=_unit([1, 0, 0, 0]),
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    if cluster_key:
        face_ops.upsert_cluster(
            run_id=run_id, cluster_key=cluster_key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det, cluster_key=cluster_key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
    return det


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def test_photos_sampled_by_cluster_mass_and_year_spread(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)

    # Photo 1 (2010): two faces from a big cluster (5 members elsewhere).
    _seed_photo(conn, file_id=1, capture="2010-06-01T10:00:00")
    for i in (0, 1):
        _seed_face(conn, face_ops, run_id=run_id, file_id=1, index=i,
                   cluster_key="big")
    # Pad the big cluster with faces in other photos.
    for fid in range(10, 13):
        _seed_photo(conn, file_id=fid, capture="2010-07-01T10:00:00")
        _seed_face(conn, face_ops, run_id=run_id, file_id=fid, cluster_key="big")
    # Photo 2 (2012): one face in a small cluster.
    _seed_photo(conn, file_id=2, capture="2012-06-01T10:00:00")
    _seed_face(conn, face_ops, run_id=run_id, file_id=2, cluster_key="small")
    # Photo 3: face in a person-linked cluster — nothing left to label.
    _seed_photo(conn, file_id=3, capture="2014-06-01T10:00:00")
    det = _seed_face(conn, face_ops, run_id=run_id, file_id=3,
                     cluster_key="done")
    person = face_ops.create_person(run_id=run_id, display_name="Sam")
    done_cluster = conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = 'done'"
    ).fetchone()[0]
    face_ops.link_cluster_to_person(run_id=run_id, cluster_id=done_cluster,
                                    person_id=person, link_method="manual")
    conn.commit()

    photos = face_ops.get_photos_for_labeling(limit=10)
    file_ids = [p["file_id"] for p in photos]
    assert 3 not in file_ids, "fully-linked photos drop out"
    assert set(file_ids) == {1, 2, 10, 11, 12}
    # Year spread: one photo per year first (2010 then 2012), rest after.
    assert file_ids[1] == 2
    assert photos[0]["file_id"] == 1, "highest-mass 2010 photo leads its year"
    assert photos[0]["faces"] == 2


def test_photo_detections_resolution_and_floor(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    _seed_photo(conn, file_id=1, capture="2010-06-01T10:00:00")
    keep = _seed_face(conn, face_ops, run_id=run_id, file_id=1, index=0,
                      cluster_key="c1", confidence=0.9)
    junk = _seed_face(conn, face_ops, run_id=run_id, file_id=1, index=1,
                      confidence=0.55)
    person = face_ops.create_person(run_id=run_id, display_name="Sam")
    cluster = conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = 'c1'"
    ).fetchone()[0]
    conn.commit()

    rows = face_ops.get_photo_detections(1, min_det_score=0.7)
    assert [r["detection_id"] for r in rows] == [keep]
    assert rows[0]["person_id"] is None
    assert rows[0]["largest_cluster_id"] == cluster

    # Accepted membership + cluster link resolves the person.
    face_ops.accept_cluster(run_id=run_id, cluster_id=cluster)
    face_ops.link_cluster_to_person(run_id=run_id, cluster_id=cluster,
                                    person_id=person, link_method="manual")
    conn.commit()
    rows = face_ops.get_photo_detections(1, min_det_score=0.7)
    assert rows[0]["person_id"] == person


# ---------------------------------------------------------------------------
# Thumbnail bbox scaling (regression)
# ---------------------------------------------------------------------------

def test_scan_scales_thumbnail_bbox_for_large_images(tmp_path):
    from photo_organizer.faces.detection import FaceScanPipeline

    # 4096-wide image: detection runs at MAX_DETECTION_DIMENSION (2048),
    # so detect-space bboxes are half-scale.
    img_path = tmp_path / "dest" / "big.jpg"
    img_path.parent.mkdir(parents=True)
    Image.new("RGB", (4096, 2048), color=(120, 90, 60)).save(img_path)

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.execute(
        """
        INSERT INTO files (id, hash, type, ext, orig_name, orig_path, dest_path,
                           first_seen_at, last_seen_at)
        VALUES (1, 'h1', 'jpeg', '.jpg', 'big.jpg', ?, ?,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """,
        (str(img_path), str(img_path)),
    )
    run_id = _start_run(conn)
    conn.commit()
    conn.close()

    class OneFaceDetector:
        def detect_faces(self, img_bgr):
            return [{
                "bbox": (100, 100, 50, 50),  # detect-space (2048-wide)
                "embedding": [1.0, 0.0], "det_score": 0.9,
                "age": 30, "gender": "F", "landmarks": None,
            }]

    class RecordingThumbnailer:
        def __init__(self):
            self.bboxes = []

        def save_thumbnail(self, img_rgb, bbox, detection_id, landmarks=None):
            self.bboxes.append((img_rgb.shape, bbox))
            return "0000/face_000001.jpg"

    pipeline = FaceScanPipeline(db_path, thumbnail_dir=tmp_path / "thumbs",
                                detector=OneFaceDetector())
    recorder = RecordingThumbnailer()
    pipeline.thumbnailer = recorder
    pipeline.run(run_id=run_id)

    (shape, bbox), = recorder.bboxes
    assert shape[1] == 4096, "thumbnail crops from the full-res image"
    assert bbox == (200, 200, 100, 100), (
        "detect-space bbox must be scaled to full-res coordinates"
    )


# ---------------------------------------------------------------------------
# Label Photos UI flow
# ---------------------------------------------------------------------------

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP_PATH = str(
    Path(__file__).parent.parent / "photo_organizer" / "faces" / "streamlit_app.py"
)


@pytest.fixture
def label_catalog(tmp_path):
    """A real image on disk with two clustered faces awaiting labels."""
    img_path = tmp_path / "dest" / "party.jpg"
    img_path.parent.mkdir(parents=True)
    Image.new("RGB", (400, 300), color=(150, 120, 90)).save(img_path)

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    face_ops = FaceDBOperations(DBOperations(conn))
    run_id = _start_run(conn)
    _seed_photo(conn, file_id=1, path=img_path, capture="2010-06-01T10:00:00")
    dets = [
        _seed_face(conn, face_ops, run_id=run_id, file_id=1, index=i,
                   cluster_key=f"c{i}", bbox=(20.0 + 100 * i, 20.0, 60.0, 60.0))
        for i in (0, 1)
    ]
    clusters = [
        conn.execute("SELECT id FROM face_clusters WHERE cluster_key = ?",
                     (f"c{i}",)).fetchone()[0]
        for i in (0, 1)
    ]
    conn.commit()
    conn.close()
    return db_path, dets, clusters


def _app(db_path, monkeypatch) -> AppTest:
    monkeypatch.setenv("PHOTO_FACES_DB", str(db_path))
    return AppTest.from_file(APP_PATH, default_timeout=30)


def test_label_photos_page_labels_face_and_cluster(label_catalog, monkeypatch):
    db_path, dets, clusters = label_catalog
    at = _app(db_path, monkeypatch)
    at.run()
    at.sidebar.radio[0].set_value("Label Photos").run()
    assert not at.exception

    # Fill the form (no reruns while filling), then one Save all.
    at.text_input(key=f"lblname_{dets[0]}").set_value("Emma")
    at.text_input(key=f"lblbd_{dets[0]}").set_value("2010-08-22")
    at.button[0].click().run()  # the form's Save all
    assert not at.exception

    conn = sqlite3.connect(str(db_path))
    try:
        person = conn.execute(
            "SELECT id, display_name, birth_date FROM face_persons"
        ).fetchone()
        det_link = conn.execute(
            "SELECT person_id, link_method FROM face_person_links "
            "WHERE detection_id = ? AND status = 'accepted'", (dets[0],),
        ).fetchone()
        cluster_link = conn.execute(
            "SELECT person_id, link_method FROM face_person_links "
            "WHERE cluster_id = ? AND status = 'accepted'", (clusters[0],),
        ).fetchone()
        cluster_status = conn.execute(
            "SELECT status FROM face_clusters WHERE id = ?", (clusters[0],),
        ).fetchone()[0]
        ui_run = conn.execute(
            "SELECT command, exit_status, db_mutates FROM command_runs "
            "WHERE tool = 'photo-faces-ui' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert person[1:] == ("Emma", "2010-08-22")
    assert det_link == (person[0], "photo_label")
    assert cluster_link == (person[0], "photo_label"), (
        "apply-to-cluster defaults on and labels the whole cluster"
    )
    assert cluster_status == "accepted"
    assert ui_run == ("ui-label-faces", "success", 1)

    # Re-render: the face now shows as already labeled.
    at2 = _app(db_path, monkeypatch)
    at2.run()
    at2.sidebar.radio[0].set_value("Label Photos").run()
    assert not at2.exception
    assert any("Already labeled: Emma" in str(el.value) for el in at2.caption)


def test_label_photos_not_a_face(label_catalog, monkeypatch):
    db_path, dets, clusters = label_catalog
    at = _app(db_path, monkeypatch)
    at.run()
    at.sidebar.radio[0].set_value("Label Photos").run()
    at.selectbox(key=f"lbl_{dets[1]}").set_value("(not a face)")
    at.button[0].click().run()
    assert not at.exception

    conn = sqlite3.connect(str(db_path))
    status = conn.execute(
        "SELECT status FROM face_detections WHERE id = ?", (dets[1],),
    ).fetchone()[0]
    conn.close()
    assert status == "not_a_face"
