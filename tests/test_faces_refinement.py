"""
Refinement and query tests: anchor computation across both link shapes,
threshold/margin auto-assign proposals, acceptance of assignments,
persons summary, and photos-for-person with RAW resolution.
"""
import json
import sqlite3
from pathlib import Path

import numpy as np
import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces import config
from photo_organizer.faces.db_ops import FaceDBOperations
from photo_organizer.faces.linking import apply_accepted_proposals
from photo_organizer.faces.refinement import RefinementEngine


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    try:
        yield db_path, conn, FaceDBOperations(DBOperations(conn))
    finally:
        conn.close()


def _start_run(conn, command="refine"):
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


def _seed_file(conn, file_id, *, file_type="jpeg", capture=None):
    conn.execute(
        """
        INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name, orig_path,
                                     dest_path, first_seen_at, last_seen_at)
        VALUES (?, ?, ?, '.jpg', 'x.jpg', 'C:/orig/x.jpg', ?,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """,
        (file_id, f"hash-{file_id}", file_type, f"C:/dest/{file_id}.jpg"),
    )
    if capture:
        conn.execute(
            "INSERT OR IGNORE INTO media_metadata (file_id, capture_datetime) VALUES (?, ?)",
            (file_id, capture),
        )


def _seed_detection(conn, face_ops, *, run_id, file_id, embedding, capture=None):
    _seed_file(conn, file_id, capture=capture)
    det = face_ops.record_detection(
        run_id=run_id, file_id=file_id, detection_index=0,
        bbox=(1.0, 1.0, 5.0, 5.0), confidence=0.9,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        image_hash=f"hash-{file_id}",
    )
    face_ops.record_embedding(
        run_id=run_id, detection_id=det, embedding=embedding,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    return det


def _seed_cluster(conn, face_ops, *, run_id, key, representative,
                  member_specs=()):
    """member_specs: (file_id, embedding, capture) tuples."""
    face_ops.upsert_cluster(
        run_id=run_id, cluster_key=key,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
        representative_embedding=representative,
    )
    cluster_id = int(conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = ?", (key,)
    ).fetchone()[0])
    for file_id, embedding, capture in member_specs:
        det = _seed_detection(conn, face_ops, run_id=run_id, file_id=file_id,
                              embedding=embedding, capture=capture)
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
    return cluster_id


def _labeled_person(conn, face_ops, *, run_id, name, anchor_embedding, file_id):
    """Person with one accepted detection-level link (an anchor)."""
    person_id = face_ops.create_person(run_id=run_id, display_name=name)
    det = _seed_detection(conn, face_ops, run_id=run_id, file_id=file_id,
                          embedding=anchor_embedding)
    face_ops.link_detection_to_person(
        run_id=run_id, detection_id=det, person_id=person_id,
        confidence=1.0, link_method="seed",
    )
    return person_id


# ---------------------------------------------------------------------------
# Anchors
# ---------------------------------------------------------------------------

def test_anchors_include_cluster_level_links(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)

    person_id = face_ops.create_person(run_id=run_id, display_name="Sam")
    cluster_id = _seed_cluster(
        conn, face_ops, run_id=run_id, key="sam-era",
        representative=_unit([1, 0, 0, 0]),
        member_specs=[(1, _unit([1, 0, 0, 0]), None)],
    )
    face_ops.accept_cluster(run_id=run_id, cluster_id=cluster_id)
    face_ops.link_cluster_to_person(
        run_id=run_id, cluster_id=cluster_id, person_id=person_id,
        link_method="merge_accept",
    )
    conn.commit()

    anchors = face_ops.get_labeled_person_embeddings()
    assert person_id in anchors
    assert len(anchors[person_id]) == 1


# ---------------------------------------------------------------------------
# Refinement proposals
# ---------------------------------------------------------------------------

def test_refine_proposes_only_confident_unambiguous_assignments(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="seed")

    sam = _labeled_person(conn, face_ops, run_id=run_id, name="Sam",
                          anchor_embedding=_unit([1, 0, 0, 0]), file_id=100)
    emma = _labeled_person(conn, face_ops, run_id=run_id, name="Emma",
                           anchor_embedding=_unit([0, 1, 0, 0]), file_id=101)

    near_sam = _seed_cluster(conn, face_ops, run_id=run_id, key="near-sam",
                             representative=_unit([1, 0.1, 0, 0]))
    ambiguous = _seed_cluster(conn, face_ops, run_id=run_id, key="ambiguous",
                              representative=_unit([1, 1, 0, 0]))
    unrelated = _seed_cluster(conn, face_ops, run_id=run_id, key="unrelated",
                              representative=_unit([0, 0, 1, 0]))
    refine_run = _start_run(conn)
    conn.commit()

    stats = RefinementEngine(db_path, threshold=0.85, margin=0.1).run(
        run_id=refine_run,
    )

    assert stats["anchors"] == 2
    assert stats["clusters_evaluated"] == 3
    assert stats["assignments_proposed"] == 1

    action = conn.execute(
        "SELECT status, confidence, payload_json FROM run_actions "
        "WHERE action_type = 'face_person_assign'"
    ).fetchone()
    payload = json.loads(action[2])
    assert action[0] == "proposed"
    assert payload["cluster_id"] == near_sam
    assert payload["person_id"] == sam
    assert payload["similarity"] >= 0.85
    assert payload["margin"] >= 0.1


def test_refine_without_anchors_proposes_nothing(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="cluster")
    _seed_cluster(conn, face_ops, run_id=run_id, key="c",
                  representative=_unit([1, 0, 0, 0]))
    refine_run = _start_run(conn)
    conn.commit()

    stats = RefinementEngine(db_path).run(run_id=refine_run)
    assert stats == {
        "anchors": 0,
        "clusters_evaluated": 1,
        "assignments_proposed": 0,
        "assignments_suppressed_by_rejection": 0,
        "assignments_superseded": 0,
    }


def test_rerefine_supersedes_stale_assignments(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="seed")
    _labeled_person(conn, face_ops, run_id=run_id, name="Sam",
                    anchor_embedding=_unit([1, 0, 0, 0]), file_id=100)
    _seed_cluster(conn, face_ops, run_id=run_id, key="near-sam",
                  representative=_unit([1, 0.1, 0, 0]))
    first_run = _start_run(conn)
    second_run = _start_run(conn)
    conn.commit()

    first = RefinementEngine(db_path, threshold=0.85).run(run_id=first_run)
    assert first["assignments_proposed"] == 1
    # Impossible threshold: nothing regenerated -> prior proposal superseded.
    second = RefinementEngine(db_path, threshold=1.01).run(run_id=second_run)
    assert second["assignments_proposed"] == 0
    assert second["assignments_superseded"] == 1


# ---------------------------------------------------------------------------
# Accepting assignments
# ---------------------------------------------------------------------------

def test_accept_applies_assignment_and_conflict_guard(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="seed")
    sam = _labeled_person(conn, face_ops, run_id=run_id, name="Sam",
                          anchor_embedding=_unit([1, 0, 0, 0]), file_id=100)
    emma = _labeled_person(conn, face_ops, run_id=run_id, name="Emma",
                           anchor_embedding=_unit([0, 1, 0, 0]), file_id=101)
    near_sam = _seed_cluster(conn, face_ops, run_id=run_id, key="near-sam",
                             representative=_unit([1, 0.1, 0, 0]),
                             member_specs=[(1, _unit([1, 0.1, 0, 0]), None)])
    refine_run = _start_run(conn)
    conn.commit()
    RefinementEngine(db_path, threshold=0.85).run(run_id=refine_run)

    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE action_type = 'face_person_assign'"
    ).fetchone()[0]

    # Conflict: the cluster is manually linked to Emma before acceptance.
    face_ops.link_cluster_to_person(run_id=refine_run, cluster_id=near_sam,
                                    person_id=emma, link_method="manual")
    accept_run = _start_run(conn, command="accept")
    conn.commit()
    stats = apply_accepted_proposals(DBOperations(conn), [action_id],
                                     run_id=accept_run)
    assert stats["conflict_components"] == 1
    assert stats["assignments_applied"] == 0

    # Remove the conflicting link; acceptance now applies.
    conn.execute("DELETE FROM face_person_links WHERE cluster_id = ?", (near_sam,))
    conn.commit()
    stats = apply_accepted_proposals(DBOperations(conn), [action_id],
                                     run_id=accept_run)
    conn.commit()

    assert stats["assignments_applied"] == 1
    assert stats["clusters_accepted"] == 1
    link = conn.execute(
        "SELECT person_id, status, link_method FROM face_person_links "
        "WHERE cluster_id = ?", (near_sam,),
    ).fetchone()
    assert link == (sam, "accepted", "auto_assign_accept")
    action_status = conn.execute(
        "SELECT status, applied_by_run_id FROM run_actions WHERE id = ?",
        (action_id,),
    ).fetchone()
    assert action_status == ("applied", accept_run)


def test_accept_assignment_absorbs_unnamed_existing_link(db):
    """An assignment targeting a cluster already linked to an ANONYMOUS
    person absorbs that person instead of conflicting."""
    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="seed")
    sam = _labeled_person(conn, face_ops, run_id=run_id, name="Sam",
                          anchor_embedding=_unit([1, 0, 0, 0]), file_id=100)
    near_sam = _seed_cluster(conn, face_ops, run_id=run_id, key="near-sam",
                             representative=_unit([1, 0.1, 0, 0]),
                             member_specs=[(1, _unit([1, 0.1, 0, 0]), None)])
    refine_run = _start_run(conn)
    conn.commit()
    RefinementEngine(db_path, threshold=0.85).run(run_id=refine_run)
    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE action_type = 'face_person_assign'"
    ).fetchone()[0]

    # Cluster is already linked to an anonymous person group.
    anon = face_ops.create_person(run_id=refine_run, display_name=None)
    face_ops.link_cluster_to_person(run_id=refine_run, cluster_id=near_sam,
                                    person_id=anon, link_method="merge_accept")
    accept_run = _start_run(conn, command="accept")
    conn.commit()

    stats = apply_accepted_proposals(DBOperations(conn), [action_id],
                                     run_id=accept_run)
    conn.commit()

    assert stats["conflict_components"] == 0
    assert stats["assignments_applied"] == 1
    assert stats["persons_absorbed"] == 1
    link = conn.execute(
        "SELECT person_id FROM face_person_links "
        "WHERE cluster_id = ? AND status = 'accepted'", (near_sam,),
    ).fetchone()
    assert link == (sam,)
    assert conn.execute(
        "SELECT status FROM face_persons WHERE id = ?", (anon,)
    ).fetchone()[0] == "merged"


# ---------------------------------------------------------------------------
# Persons summary + photos-for-person
# ---------------------------------------------------------------------------

def _accepted_identity(db, *, raw_link=False):
    """Person 'Sam' with an accepted cluster of two dated photos."""
    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="accept")
    sam = face_ops.create_person(run_id=run_id, display_name="Sam",
                                 birth_date="2005-03-15")
    cluster_id = _seed_cluster(
        conn, face_ops, run_id=run_id, key="sam-era",
        representative=_unit([1, 0, 0, 0]),
        member_specs=[
            (1, _unit([1, 0, 0, 0]), "2010-06-01T10:00:00"),
            (2, _unit([1, 0.05, 0, 0]), "2011-07-01T10:00:00"),
        ],
    )
    face_ops.accept_cluster(run_id=run_id, cluster_id=cluster_id)
    face_ops.link_cluster_to_person(run_id=run_id, cluster_id=cluster_id,
                                    person_id=sam, link_method="merge_accept")
    if raw_link:
        _seed_file(conn, 900, file_type="raw")
        conn.execute("UPDATE files SET dest_path = 'C:/dest/raw/IMG_1.CR2' "
                     "WHERE id = 900")
        conn.execute(
            "INSERT INTO raw_outputs (raw_file_id, output_file_id) VALUES (900, 1)"
        )
    conn.commit()
    return sam


def test_persons_summary_counts_accepted_state(db):
    db_path, conn, face_ops = db
    sam = _accepted_identity(db)

    summary = face_ops.get_persons_summary()
    assert summary == [{
        "id": sam,
        "display_name": "Sam",
        "birth_date": "2005-03-15",
        "clusters": 1,
        "detections": 2,
    }]


def test_photos_for_person_resolves_raw_and_filters_dates(db):
    db_path, conn, face_ops = db
    sam = _accepted_identity(db, raw_link=True)

    photos = face_ops.get_photos_for_person(sam)
    assert [p["file_id"] for p in photos] == [1, 2]
    assert photos[0]["raw_path"] == "C:/dest/raw/IMG_1.CR2"
    assert photos[1]["raw_path"] is None

    filtered = face_ops.get_photos_for_person(
        sam, date_from="2011-01-01", date_to="2011-12-31",
    )
    assert [p["file_id"] for p in filtered] == [2]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def test_cli_refine_persons_query_flow(tmp_path, capsys):
    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    face_ops = FaceDBOperations(DBOperations(conn))
    sam = _accepted_identity((db_path, conn, face_ops))
    conn.close()

    assert main(["--db", str(db_path), "refine"]) == 0
    assert main(["--db", str(db_path), "persons"]) == 0
    out = capsys.readouterr().out
    assert "Sam, born 2005-03-15" in out
    assert "clusters: 1  faces: 2" in out

    csv_path = tmp_path / "sam.csv"
    assert main(["--db", str(db_path), "query", "Sam",
                 "--csv-output", str(csv_path)]) == 0
    out = capsys.readouterr().out
    assert "Sam: 2 photo(s)" in out
    assert csv_path.read_text(encoding="utf-8").count("C:/dest/") == 2

    assert main(["--db", str(db_path), "query", "Sam", "--timeline"]) == 0
    out = capsys.readouterr().out
    assert "2010" in out and "2011" in out

    assert main(["--db", str(db_path), "query", "Nobody"]) == 0
    assert "No person named 'Nobody'" in capsys.readouterr().out
