"""
Review UI tests: dashboard stats helpers and headless Streamlit AppTest
flows — cluster labeling, suggestion accept/reject with audited UI runs.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces import config
from photo_organizer.faces.db_ops import FaceDBOperations
from photo_organizer.faces.linking import CrossAgeLinker

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP_PATH = str(
    Path(__file__).parent.parent / "photo_organizer" / "faces" / "streamlit_app.py"
)


def _start_run(conn, command="cluster"):
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


def _seed_cluster(conn, face_ops, *, run_id, key, era_start, era_end,
                  representative, member_file_ids=()):
    face_ops.upsert_cluster(
        run_id=run_id, cluster_key=key,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        era_start=era_start, era_end=era_end,
        representative_embedding=representative,
    )
    cluster_id = int(conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = ?", (key,)
    ).fetchone()[0])
    for file_id in member_file_ids:
        conn.execute(
            """
            INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name,
                                         orig_path, first_seen_at, last_seen_at)
            VALUES (?, ?, 'jpeg', '.jpg', 'x.jpg', 'C:/x.jpg',
                    '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (file_id, f"hash-{file_id}"),
        )
        conn.execute(
            "INSERT OR IGNORE INTO media_metadata (file_id, capture_datetime) "
            "VALUES (?, '2011-06-01T10:00:00')",
            (file_id,),
        )
        det = face_ops.record_detection(
            run_id=run_id, file_id=file_id, detection_index=0,
            bbox=(1.0, 1.0, 5.0, 5.0), confidence=0.9,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
            image_hash=f"hash-{file_id}",
        )
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
    return cluster_id


@pytest.fixture
def catalog(tmp_path):
    """Catalog with two similar clusters in adjacent eras and one pending
    merge suggestion."""
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    face_ops = FaceDBOperations(DBOperations(conn))
    seed_run = _start_run(conn)
    a = _seed_cluster(conn, face_ops, run_id=seed_run, key="a",
                      era_start="2010-01-01T00:00:00",
                      era_end="2012-01-01T00:00:00",
                      representative=_unit([1.0, 0.8, 0, 0]),
                      member_file_ids=(1, 2))
    b = _seed_cluster(conn, face_ops, run_id=seed_run, key="b",
                      era_start="2012-06-01T00:00:00",
                      era_end="2014-01-01T00:00:00",
                      representative=_unit([0.8, 1.0, 0, 0]),
                      member_file_ids=(3,))
    link_run = _start_run(conn, command="link")
    conn.commit()
    conn.close()
    CrossAgeLinker(db_path, min_confidence=0.1).run(run_id=link_run)

    conn = sqlite3.connect(str(db_path))
    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE action_type = 'face_cluster_merge' "
        "AND status = 'proposed'"
    ).fetchone()[0]
    conn.close()
    return db_path, (a, b), action_id


def _app(db_path, monkeypatch) -> AppTest:
    monkeypatch.setenv("PHOTO_FACES_DB", str(db_path))
    return AppTest.from_file(APP_PATH, default_timeout=30)


# ---------------------------------------------------------------------------
# Stats helper + page
# ---------------------------------------------------------------------------

def test_get_stats_counts(catalog):
    db_path, _clusters, _action = catalog
    conn = sqlite3.connect(str(db_path))
    try:
        stats = FaceDBOperations(DBOperations(conn)).get_stats()
    finally:
        conn.close()
    assert stats["total_detections"] == 3
    assert stats["photos_with_faces"] == 3
    assert stats["clusters_live"] == 2
    assert stats["pending_merges"] == 1
    assert stats["pending_assignments"] == 0
    assert stats["detections_assigned"] == 0
    assert stats["detections_named"] == 0

    # Direct detection-level labels count toward both metrics.
    conn = sqlite3.connect(str(db_path))
    face_ops = FaceDBOperations(DBOperations(conn))
    run_id = _start_run(conn, command="label")
    person = face_ops.create_person(run_id=run_id, display_name="Sam")
    det = conn.execute(
        "SELECT id FROM face_detections WHERE status = 'observed' LIMIT 1"
    ).fetchone()[0]
    face_ops.link_detection_to_person(run_id=run_id, detection_id=det,
                                      person_id=person, confidence=1.0,
                                      link_method="photo_label")
    conn.commit()
    stats = face_ops.get_stats()
    conn.close()
    assert stats["detections_assigned"] == 1
    assert stats["detections_named"] == 1


def test_stats_page_renders(catalog, monkeypatch):
    db_path, _clusters, _action = catalog
    at = _app(db_path, monkeypatch).run()
    assert not at.exception
    labels = [m.label for m in at.metric]
    assert "Faces Detected" in labels
    assert "Pending Merge Suggestions" in labels


# ---------------------------------------------------------------------------
# Cluster review flow
# ---------------------------------------------------------------------------

def test_cluster_review_create_and_assign(catalog, monkeypatch):
    db_path, (cluster_a, _b), _action = catalog
    at = _app(db_path, monkeypatch)
    at.run()
    at.sidebar.radio[0].set_value("Cluster Review").run()
    assert not at.exception

    at.text_input(key=f"name_{cluster_a}").set_value("Sam")
    at.text_input(key=f"bd_{cluster_a}").set_value("2005-03-15")
    at.run()
    at.button(key=f"btn_new_{cluster_a}").click().run()
    assert not at.exception

    conn = sqlite3.connect(str(db_path))
    try:
        person = conn.execute(
            "SELECT id, display_name, birth_date FROM face_persons"
        ).fetchone()
        link = conn.execute(
            "SELECT person_id, cluster_id, status, link_method "
            "FROM face_person_links"
        ).fetchone()
        cluster_status = conn.execute(
            "SELECT status FROM face_clusters WHERE id = ?", (cluster_a,)
        ).fetchone()[0]
        ui_run = conn.execute(
            "SELECT tool, command, exit_status FROM command_runs "
            "WHERE tool = 'photo-faces-ui'"
        ).fetchone()
    finally:
        conn.close()

    assert person[1:] == ("Sam", "2005-03-15")
    assert link == (person[0], cluster_a, "accepted", "manual_review")
    assert cluster_status == "accepted"
    assert ui_run == ("photo-faces-ui", "ui-create-person", "success")


# ---------------------------------------------------------------------------
# Suggestion review flow
# ---------------------------------------------------------------------------

def test_suggestion_review_accept_applies_merge(catalog, monkeypatch):
    db_path, (cluster_a, cluster_b), action_id = catalog
    at = _app(db_path, monkeypatch)
    at.run()
    at.sidebar.radio[0].set_value("Suggestion Review").run()
    assert not at.exception

    at.button(key=f"accept_{action_id}").click().run()
    assert not at.exception

    conn = sqlite3.connect(str(db_path))
    try:
        action = conn.execute(
            "SELECT status FROM run_actions WHERE id = ?", (action_id,)
        ).fetchone()[0]
        linked = {row[0] for row in conn.execute(
            "SELECT cluster_id FROM face_person_links WHERE status = 'accepted'"
        )}
        persons = conn.execute("SELECT COUNT(*) FROM face_persons").fetchone()[0]
        ui_run = conn.execute(
            "SELECT command, exit_status FROM command_runs "
            "WHERE tool = 'photo-faces-ui'"
        ).fetchone()
    finally:
        conn.close()

    assert action == "applied"
    assert linked == {cluster_a, cluster_b}
    assert persons == 1
    assert ui_run == ("ui-accept", "success")


def test_suggestion_review_reject(catalog, monkeypatch):
    db_path, _clusters, action_id = catalog
    at = _app(db_path, monkeypatch)
    at.run()
    at.sidebar.radio[0].set_value("Suggestion Review").run()
    at.button(key=f"reject_{action_id}").click().run()
    assert not at.exception

    conn = sqlite3.connect(str(db_path))
    try:
        status, note = conn.execute(
            "SELECT status, resolution_note FROM run_actions WHERE id = ?",
            (action_id,),
        ).fetchone()
    finally:
        conn.close()
    assert status == "rejected"
    assert note == "rejected in review UI"


def test_name_people_page_names_anonymous_person(catalog, monkeypatch):
    db_path, (cluster_a, _b), action_id = catalog
    # Accepting the merge creates one anonymous person.
    conn = sqlite3.connect(str(db_path))
    from photo_organizer.faces.linking import apply_accepted_proposals
    accept_run = _start_run(conn, command="accept")
    conn.commit()
    apply_accepted_proposals(DBOperations(conn), [action_id], run_id=accept_run)
    conn.commit()
    anon = conn.execute(
        "SELECT id FROM face_persons WHERE display_name IS NULL "
        "AND status = 'active'"
    ).fetchone()[0]
    conn.close()

    at = _app(db_path, monkeypatch)
    at.run()
    at.sidebar.radio[0].set_value("Name People").run()
    assert not at.exception

    at.text_input(key=f"pname_{anon}").set_value("Sam")
    at.text_input(key=f"pbd_{anon}").set_value("2005-03-15")
    at.run()
    at.button(key=f"btn_pname_{anon}").click().run()
    assert not at.exception

    conn = sqlite3.connect(str(db_path))
    try:
        person = conn.execute(
            "SELECT display_name, birth_date, status FROM face_persons "
            "WHERE id = ?", (anon,),
        ).fetchone()
        ui_run = conn.execute(
            "SELECT command, exit_status FROM command_runs "
            "WHERE tool = 'photo-faces-ui'"
        ).fetchone()
    finally:
        conn.close()
    assert person == ("Sam", "2005-03-15", "active")
    assert ui_run == ("ui-name-person", "success")


# ---------------------------------------------------------------------------
# Timeline + query pages render with an accepted identity
# ---------------------------------------------------------------------------

def test_timeline_and_query_pages(catalog, monkeypatch):
    db_path, (cluster_a, _b), _action = catalog
    conn = sqlite3.connect(str(db_path))
    face_ops = FaceDBOperations(DBOperations(conn))
    run_id = _start_run(conn, command="accept")
    person_id = face_ops.create_person(run_id=run_id, display_name="Sam")
    face_ops.accept_cluster(run_id=run_id, cluster_id=cluster_a)
    face_ops.link_cluster_to_person(run_id=run_id, cluster_id=cluster_a,
                                    person_id=person_id,
                                    link_method="manual_review")
    conn.commit()
    conn.close()

    at = _app(db_path, monkeypatch)
    at.run()
    at.sidebar.radio[0].set_value("Timeline").run()
    assert not at.exception
    assert any("2 face(s)" in str(el.value) for el in at.markdown)

    at.sidebar.radio[0].set_value("Query").run()
    assert not at.exception
    at.button[0].click().run()
    assert not at.exception
    assert any("Found 2 photo(s)" in str(el.value) for el in at.markdown)
