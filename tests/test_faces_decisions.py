"""
Durable face decisions: rejections become cannot-link constraints that
survive re-clustering (linker, refinement, and accept all consult them),
and construction-true merge tiers are bulk-acceptable as "mechanical".
"""
import json
import sqlite3
from datetime import datetime

import numpy as np
import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import CURRENT_SCHEMA_VERSION, init_schema
from photo_organizer.faces import config
from photo_organizer.faces.db_ops import FaceDBOperations
from photo_organizer.faces.linking import CrossAgeLinker, apply_accepted_proposals
from photo_organizer.faces.refinement import RefinementEngine


def _unit(vec):
    arr = np.asarray(vec, dtype=np.float32)
    return (arr / np.linalg.norm(arr)).tolist()


ALICE = _unit([1.0, 0.1, 0.0, 0.0])
ALICE2 = _unit([1.0, 0.2, 0.0, 0.0])
BOB = _unit([0.0, 0.1, 1.0, 0.0])


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    try:
        yield db_path, conn, FaceDBOperations(DBOperations(conn))
    finally:
        conn.close()


def _start_run(conn, command="link"):
    return conn.execute(
        """
        INSERT INTO command_runs (tool, command, started_at, exit_status,
                                  dry_run, argv_json)
        VALUES ('photo-faces', ?, '2026-01-01T00:00:00Z', 'running', 0, '[]')
        """,
        (command,),
    ).lastrowid


def _add_photo(conn, file_id, captured=None):
    conn.execute(
        """
        INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name, orig_path,
                                     first_seen_at, last_seen_at)
        VALUES (?, ?, 'jpeg', '.jpg', 'x.jpg', 'C:/x.jpg',
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """,
        (file_id, f"hash-{file_id}"),
    )
    conn.execute(
        "INSERT OR IGNORE INTO media_metadata (file_id, capture_datetime) "
        "VALUES (?, ?)",
        (file_id, captured.isoformat() if captured else None),
    )


def _add_detection(face_ops, *, run_id, file_id, det_index=0, embedding):
    detection_id = face_ops.record_detection(
        run_id=run_id, file_id=file_id, detection_index=det_index,
        bbox=(1.0, 1.0, 10.0, 10.0), confidence=0.9,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        image_hash=f"hash-{file_id}",
    )
    face_ops.record_embedding(
        run_id=run_id, detection_id=detection_id,
        embedding=[float(v) for v in embedding],
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    return detection_id


def _make_cluster(conn, face_ops, *, run_id, key, era_start, era_end,
                  representative, detection_ids):
    face_ops.upsert_cluster(
        run_id=run_id, cluster_key=key,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        era_start=era_start, era_end=era_end,
        representative_embedding=[float(v) for v in representative],
    )
    for det in detection_ids:
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
    return int(conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = ?", (key,)
    ).fetchone()[0])


def _proposed_merges(conn, method=None):
    where = "AND method = ?" if method else ""
    params = (method,) if method else ()
    return conn.execute(
        f"""
        SELECT id, method, payload_json FROM run_actions
        WHERE action_type = 'face_cluster_merge' AND status = 'proposed'
        {where} ORDER BY id
        """,
        params,
    ).fetchall()


def test_schema_v10_has_cannot_links_table(db):
    _db_path, conn, _face_ops = db
    assert CURRENT_SCHEMA_VERSION == 10
    version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
    assert version == 10
    columns = {row[1] for row in
               conn.execute("PRAGMA table_info(face_cannot_links)")}
    assert {"detections_a", "detections_b", "person_id", "status"} <= columns


# ---------------------------------------------------------------------------
# Rejection snapshots
# ---------------------------------------------------------------------------

def test_reject_snapshots_detection_sides(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    _add_photo(conn, 1)
    _add_photo(conn, 2)
    d1 = _add_detection(face_ops, run_id=seed_run, file_id=1, embedding=ALICE)
    d2 = _add_detection(face_ops, run_id=seed_run, file_id=2, embedding=ALICE2)
    _make_cluster(conn, face_ops, run_id=seed_run, key="a",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=ALICE, detection_ids=[d1])
    _make_cluster(conn, face_ops, run_id=seed_run, key="b",
                  era_start="2011-06-01T00:00:00", era_end="2013-01-01T00:00:00",
                  representative=ALICE2, detection_ids=[d2])
    link_run = _start_run(conn)
    conn.commit()
    CrossAgeLinker(db_path, min_confidence=0.1, use_tracklets=False).run(
        run_id=link_run)

    (action_id, _method, _payload), = _proposed_merges(conn)
    reject_run = _start_run(conn, command="reject")
    result = face_ops.reject_face_proposals([action_id], run_id=reject_run,
                                            note="different people")
    conn.commit()

    assert result["rejected"] == [action_id]
    assert result["cannot_links_created"] == 1
    constraints = face_ops.get_active_cannot_links()
    assert len(constraints) == 1
    assert constraints[0]["detections_a"] == {d1}
    assert constraints[0]["detections_b"] == {d2}
    assert constraints[0]["person_id"] is None

    status, note = conn.execute(
        "SELECT status, resolution_note FROM run_actions WHERE id = ?",
        (action_id,)).fetchone()
    assert (status, note) == ("rejected", "different people")


def test_rejected_merge_not_reproposed_after_recluster(db):
    """The evaporation bug: re-clustering mints new cluster ids, so a bare
    run_actions rejection can't stop the same faces being re-proposed. The
    cannot-link constraint must."""
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    _add_photo(conn, 1)
    _add_photo(conn, 2)
    d1 = _add_detection(face_ops, run_id=seed_run, file_id=1, embedding=ALICE)
    d2 = _add_detection(face_ops, run_id=seed_run, file_id=2, embedding=ALICE2)
    _make_cluster(conn, face_ops, run_id=seed_run, key="gen1:a",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=ALICE, detection_ids=[d1])
    _make_cluster(conn, face_ops, run_id=seed_run, key="gen1:b",
                  era_start="2011-06-01T00:00:00", era_end="2013-01-01T00:00:00",
                  representative=ALICE2, detection_ids=[d2])
    link_run = _start_run(conn)
    conn.commit()
    CrossAgeLinker(db_path, min_confidence=0.1, use_tracklets=False).run(
        run_id=link_run)

    (action_id, _m, _p), = _proposed_merges(conn)
    reject_run = _start_run(conn, command="reject")
    face_ops.reject_face_proposals([action_id], run_id=reject_run)
    conn.commit()

    # Re-cluster: prior clusters superseded, same detections under new keys
    # (and therefore new cluster ids).
    recluster_run = _start_run(conn, command="cluster")
    face_ops.supersede_proposed_clusters(
        run_id=recluster_run, model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG)
    _make_cluster(conn, face_ops, run_id=recluster_run, key="gen2:a",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=ALICE, detection_ids=[d1])
    _make_cluster(conn, face_ops, run_id=recluster_run, key="gen2:b",
                  era_start="2011-06-01T00:00:00", era_end="2013-01-01T00:00:00",
                  representative=ALICE2, detection_ids=[d2])
    relink_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path, min_confidence=0.1,
                           use_tracklets=False).run(run_id=relink_run)

    assert stats["suggestions_suppressed_by_rejection"] == 1
    assert _proposed_merges(conn) == []


def test_rejected_tracklet_merge_suppressed(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    for file_id, minute in ((1, 0), (2, 2)):
        _add_photo(conn, file_id, datetime(2020, 6, 1, 10, minute))
    d1 = _add_detection(face_ops, run_id=seed_run, file_id=1, embedding=ALICE)
    d2 = _add_detection(face_ops, run_id=seed_run, file_id=2, embedding=ALICE2)
    era = ("2020-01-01T00:00:00", "2022-07-01T00:00:00")
    _make_cluster(conn, face_ops, run_id=seed_run, key="a",
                  era_start=era[0], era_end=era[1],
                  representative=ALICE, detection_ids=[d1])
    _make_cluster(conn, face_ops, run_id=seed_run, key="b",
                  era_start=era[0], era_end=era[1],
                  representative=ALICE2, detection_ids=[d2])
    link_run = _start_run(conn)
    conn.commit()
    stats = CrossAgeLinker(db_path).run(run_id=link_run)
    assert stats["tracklet_merges_proposed"] == 1

    (action_id, method, _p), = _proposed_merges(conn)
    assert method == "same_event_tracklet"
    reject_run = _start_run(conn, command="reject")
    face_ops.reject_face_proposals([action_id], run_id=reject_run)
    relink_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path).run(run_id=relink_run)
    assert stats["tracklet_merges_proposed"] == 0
    assert stats["suggestions_suppressed_by_rejection"] == 1
    assert _proposed_merges(conn) == []


def test_rejected_assignment_suppressed_in_refine(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    _add_photo(conn, 1)
    _add_photo(conn, 2)
    anchor_det = _add_detection(face_ops, run_id=seed_run, file_id=1,
                                embedding=ALICE)
    person_id = face_ops.create_person(run_id=seed_run, display_name="Ava")
    face_ops.link_detection_to_person(
        run_id=seed_run, detection_id=anchor_det, person_id=person_id,
        confidence=None, link_method="seed")

    d2 = _add_detection(face_ops, run_id=seed_run, file_id=2, embedding=ALICE2)
    _make_cluster(conn, face_ops, run_id=seed_run, key="candidate",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=ALICE2, detection_ids=[d2])
    refine_run = _start_run(conn, command="refine")
    conn.commit()

    stats = RefinementEngine(db_path).run(run_id=refine_run)
    assert stats["assignments_proposed"] == 1

    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE action_type = 'face_person_assign' "
        "AND status = 'proposed'").fetchone()[0]
    reject_run = _start_run(conn, command="reject")
    result = face_ops.reject_face_proposals([action_id], run_id=reject_run)
    conn.commit()
    assert result["cannot_links_created"] == 1
    constraint, = face_ops.get_active_cannot_links()
    assert constraint["person_id"] == person_id
    assert constraint["detections_a"] == {d2}

    rerun = _start_run(conn, command="refine")
    conn.commit()
    stats = RefinementEngine(db_path).run(run_id=rerun)
    assert stats["assignments_proposed"] == 0
    assert stats["assignments_suppressed_by_rejection"] == 1


# ---------------------------------------------------------------------------
# Accept-time guard
# ---------------------------------------------------------------------------

def test_accept_refuses_component_violating_cannot_link(db):
    """Rejecting A-C, then accepting A-B and B-C, would join A and C
    transitively — the component must be refused."""
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    dets = {}
    for i, (key, era) in enumerate((
            ("a", ("2010-01-01T00:00:00", "2011-07-01T00:00:00")),
            ("b", ("2011-01-01T00:00:00", "2012-07-01T00:00:00")),
            ("c", ("2012-01-01T00:00:00", "2013-07-01T00:00:00")),
    ), start=1):
        _add_photo(conn, i)
        det = _add_detection(face_ops, run_id=seed_run, file_id=i,
                             embedding=ALICE if i != 2 else ALICE2)
        dets[key] = det
        _make_cluster(conn, face_ops, run_id=seed_run, key=key,
                      era_start=era[0], era_end=era[1],
                      representative=ALICE if i != 2 else ALICE2,
                      detection_ids=[det])
    link_run = _start_run(conn)
    conn.commit()
    CrossAgeLinker(db_path, min_confidence=0.1, use_tracklets=False).run(
        run_id=link_run)

    merges = {}
    for action_id, _method, payload_json in _proposed_merges(conn):
        payload = json.loads(payload_json)
        merges[(payload["cluster_a_id"], payload["cluster_b_id"])] = action_id
    assert len(merges) == 3

    cluster_ids = {key: int(conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = ?", (key,)
    ).fetchone()[0]) for key in ("a", "b", "c")}
    pair_ac = tuple(sorted((cluster_ids["a"], cluster_ids["c"])))
    reject_run = _start_run(conn, command="reject")
    face_ops.reject_face_proposals([merges[pair_ac]], run_id=reject_run)
    conn.commit()

    accept_run = _start_run(conn, command="accept")
    remaining = [aid for pair, aid in merges.items() if pair != pair_ac]
    stats = apply_accepted_proposals(DBOperations(conn), remaining,
                                     run_id=accept_run)

    assert stats["cannot_link_conflicts"] == 1
    assert stats["clusters_accepted"] == 0
    assert stats["merges_applied"] == 0
    assert stats["persons_created"] == 0, "refused component must not leak a person"


# ---------------------------------------------------------------------------
# Mechanical selection
# ---------------------------------------------------------------------------

def _insert_merge_action(conn, run_id, *, method, key, confidence,
                         signals=None):
    conn.execute(
        """
        INSERT INTO run_actions (proposed_by_run_id, action_type, entity_type,
                                 entity_id, status, confidence, method,
                                 idempotency_key, phase, sequence,
                                 payload_json, created_at)
        VALUES (?, 'face_cluster_merge', 'face_cluster', 1, 'proposed', ?, ?,
                ?, 62, 0, ?, '2026-01-01T00:00:00Z')
        """,
        (run_id, confidence, method, key,
         json.dumps({"cluster_a_id": 1, "cluster_b_id": 2,
                     "signals": signals or {}})),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def test_mechanical_selection_excludes_judgment_proposals(db):
    _db_path, conn, face_ops = db
    run_id = _start_run(conn)
    dup = _insert_merge_action(conn, run_id, method="window_duplicate",
                               key="k1", confidence=100)
    clean = _insert_merge_action(conn, run_id, method="same_event_tracklet",
                                 key="k2", confidence=75,
                                 signals={"same_photo_overlap": 0})
    flagged = _insert_merge_action(conn, run_id, method="same_event_tracklet",
                                   key="k3", confidence=45,
                                   signals={"same_photo_overlap": 2})
    scored = _insert_merge_action(conn, run_id, method="cross_age_multisignal",
                                  key="k4", confidence=88)
    conn.commit()

    assert face_ops.get_pending_mechanical_merge_ids() == [dup, clean]
    judgment = {p["id"] for p in face_ops.get_pending_judgment_proposals()}
    assert judgment == {flagged, scored}


def test_cli_reject_and_mechanical_accept_modes(db, tmp_path):
    from photo_organizer.faces.cli import main

    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    _insert_merge_action(conn, run_id, method="cross_age_multisignal",
                         key="cli-k", confidence=70)
    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE idempotency_key = 'cli-k'"
    ).fetchone()[0]
    conn.commit()

    # accept demands exactly one selection mode.
    assert main(["--db", str(db_path), "accept"]) == 1
    assert main(["--db", str(db_path), "accept", "--mechanical", "12"]) == 1

    # reject flips the proposal and (payload has no real clusters, so no
    # snapshot sides) records the rejection durably.
    assert main(["--db", str(db_path), "reject", str(action_id),
                 "--note", "wrong person"]) == 0
    status = conn.execute(
        "SELECT status FROM run_actions WHERE id = ?", (action_id,)
    ).fetchone()[0]
    assert status == "rejected"
