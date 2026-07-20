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
from photo_organizer.faces.clustering import FaceClusterPipeline
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
                         signals=None, cluster_a=1, cluster_b=2):
    conn.execute(
        """
        INSERT INTO run_actions (proposed_by_run_id, action_type, entity_type,
                                 entity_id, status, confidence, method,
                                 idempotency_key, phase, sequence,
                                 payload_json, created_at)
        VALUES (?, 'face_cluster_merge', 'face_cluster', ?, 'proposed', ?, ?,
                ?, 62, 0, ?, '2026-01-01T00:00:00Z')
        """,
        (run_id, cluster_a, confidence, method, key,
         json.dumps({"cluster_a_id": cluster_a, "cluster_b_id": cluster_b,
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


# ---------------------------------------------------------------------------
# Member eviction
# ---------------------------------------------------------------------------

def _five_face_cluster(conn, face_ops, key="mixed"):
    """A person-A cluster (3 faces) contaminated with 2 person-B faces.
    Returns (cluster_id, a_dets, b_dets)."""
    run_id = _start_run(conn, command="cluster")
    a_dets, b_dets = [], []
    for i in range(1, 6):
        _add_photo(conn, i, datetime(2020, 6, i, 10, 0))
        det = _add_detection(face_ops, run_id=run_id, file_id=i,
                             embedding=ALICE if i <= 3 else BOB)
        (a_dets if i <= 3 else b_dets).append(det)
    cluster_id = _make_cluster(
        conn, face_ops, run_id=run_id, key=key,
        era_start="2020-01-01T00:00:00", era_end="2022-07-01T00:00:00",
        representative=ALICE, detection_ids=a_dets + b_dets)
    conn.commit()
    return cluster_id, a_dets, b_dets


def test_batch_eviction_never_splits_coevicted_faces(db):
    """Two person-B faces evicted together from a person-A cluster must be
    constrained against the A-faces only — never against each other."""
    _db_path, conn, face_ops = db
    cluster_id, a_dets, b_dets = _five_face_cluster(conn, face_ops)

    evict_run = _start_run(conn, command="evict")
    result = face_ops.evict_cluster_members(
        run_id=evict_run, cluster_id=cluster_id, detection_ids=b_dets)
    conn.commit()

    assert result["evicted"] == 2
    assert result["constraints_created"] == 1
    constraint, = face_ops.get_active_cannot_links()
    assert constraint["detections_a"] == set(b_dets)
    assert constraint["detections_b"] == set(a_dets), (
        "the remaining side must not include the co-evicted face")

    live = face_ops.get_live_members_for_clusters([cluster_id])
    assert live[cluster_id] == set(a_dets)


def test_cross_save_eviction_prunes_stale_snapshots(db):
    """Evicting b1 today and b2 next week: the first constraint snapshot
    mis-included b2 on the remaining side; the second eviction corrects it
    so b1 and b2 stay free to cluster together."""
    _db_path, conn, face_ops = db
    cluster_id, a_dets, (b1, b2) = _five_face_cluster(conn, face_ops)

    first = _start_run(conn, command="evict")
    face_ops.evict_cluster_members(run_id=first, cluster_id=cluster_id,
                                   detection_ids=[b1])
    constraint, = face_ops.get_active_cannot_links()
    assert b2 in constraint["detections_b"], "snapshot naturally includes b2"

    second = _start_run(conn, command="evict")
    result = face_ops.evict_cluster_members(run_id=second,
                                            cluster_id=cluster_id,
                                            detection_ids=[b2])
    conn.commit()

    assert result["constraint_sides_pruned"] == 1
    constraints = face_ops.get_active_cannot_links()
    assert len(constraints) == 2
    for constraint in constraints:
        crossing = ((b1 in constraint["detections_a"] and
                     b2 in constraint["detections_b"]) or
                    (b2 in constraint["detections_a"] and
                     b1 in constraint["detections_b"]))
        assert not crossing, "b1 and b2 must never be cannot-linked"


def _run_pipeline(db_path, labels):
    """Run the cluster pipeline with an injected backend producing fixed
    labels (detection-id order)."""
    def fixed_labels(embeddings):
        assert len(embeddings) == len(labels)
        return np.asarray(labels), None

    pipeline = FaceClusterPipeline(
        db_path, min_cluster_size=2, pca_dims=0, min_member_sim=0,
        era_size_years=50.0, cluster_fn=fixed_labels)
    return pipeline


def test_clustering_enforces_cannot_links(db):
    """HDBSCAN re-joining evicted faces gets overruled: the smaller
    constraint side is trimmed from the proposed cluster."""
    db_path, conn, face_ops = db
    cluster_id, a_dets, b_dets = _five_face_cluster(conn, face_ops)
    evict_run = _start_run(conn, command="evict")
    face_ops.evict_cluster_members(run_id=evict_run, cluster_id=cluster_id,
                                   detection_ids=b_dets)
    face_ops.supersede_proposed_clusters(
        run_id=evict_run, model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG)
    cluster_run = _start_run(conn, command="cluster")
    conn.commit()

    # The backend lumps all five faces into one cluster again.
    stats = _run_pipeline(db_path, [0, 0, 0, 0, 0]).run(run_id=cluster_run)

    assert stats["members_trimmed_cannot_link"] == 2
    assert stats["clusters_proposed"] == 1
    rows = conn.execute(
        """
        SELECT m.detection_id FROM face_cluster_members m
        JOIN face_clusters c ON c.id = m.cluster_id
        WHERE c.status = 'proposed' AND m.status = 'proposed'
        """).fetchall()
    assert {int(r[0]) for r in rows} == set(a_dets)


def test_evicted_faces_can_form_their_own_cluster(db):
    """The same constraint must NOT stop the evicted faces from clustering
    with each other as their true person."""
    db_path, conn, face_ops = db
    cluster_id, a_dets, b_dets = _five_face_cluster(conn, face_ops)
    evict_run = _start_run(conn, command="evict")
    face_ops.evict_cluster_members(run_id=evict_run, cluster_id=cluster_id,
                                   detection_ids=b_dets)
    face_ops.supersede_proposed_clusters(
        run_id=evict_run, model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG)
    cluster_run = _start_run(conn, command="cluster")
    conn.commit()

    # This time the backend separates them correctly: A-faces and B-faces.
    stats = _run_pipeline(db_path, [0, 0, 0, 1, 1]).run(run_id=cluster_run)

    assert stats["members_trimmed_cannot_link"] == 0
    assert stats["clusters_proposed"] == 2
    rows = conn.execute(
        """
        SELECT c.cluster_key, m.detection_id
        FROM face_cluster_members m
        JOIN face_clusters c ON c.id = m.cluster_id
        WHERE c.status = 'proposed' AND m.status = 'proposed'
        """).fetchall()
    by_key: dict = {}
    for key, det in rows:
        by_key.setdefault(key, set()).add(int(det))
    assert set(a_dets) in by_key.values()
    assert set(b_dets) in by_key.values()


def test_not_a_person_verdict_excludes_from_working_sets(db):
    """Dolls/statues: face-shaped but carrying no person identity — the
    verdict removes them from clustering and tracklet inputs like the
    other detection verdicts."""
    _db_path, conn, face_ops = db
    run_id = _start_run(conn, command="scan")
    _add_photo(conn, 1, datetime(2020, 6, 1, 10, 0))
    live = _add_detection(face_ops, run_id=run_id, file_id=1, det_index=0,
                          embedding=ALICE)
    doll = _add_detection(face_ops, run_id=run_id, file_id=1, det_index=1,
                          embedding=ALICE2)

    face_ops.mark_detection_not_a_person(run_id=run_id, detection_id=doll)

    status = conn.execute("SELECT status FROM face_detections WHERE id = ?",
                          (doll,)).fetchone()[0]
    assert status == 'not_a_person'
    clustering_ids = [row[0] for row in face_ops.get_embeddings_with_capture_dates(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG)]
    assert clustering_ids == [live]
    tracklet_ids = [row[0] for row in face_ops.get_detections_for_tracklets(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG)]
    assert tracklet_ids == [live]
    action = conn.execute(
        "SELECT status, method FROM run_actions "
        "WHERE action_type = 'face_detection_not_a_person'").fetchone()
    assert action == ('applied', 'user_not_a_person')


def test_get_photo_info_resolves_current_path(db):
    _db_path, conn, face_ops = db
    _add_photo(conn, 7, datetime(2020, 6, 1, 10, 0))
    info = face_ops.get_photo_info(7)
    assert info == {"path": "C:/x.jpg", "file_type": "jpeg",
                    "capture_datetime": "2020-06-01T10:00:00"}
    assert face_ops.get_photo_info(999) is None


# ---------------------------------------------------------------------------
# Same-photo flag lifecycle
# ---------------------------------------------------------------------------

def test_same_photo_flags_persist_and_dismissal_sticks(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    _add_photo(conn, 1, datetime(2020, 6, 1, 10, 0))
    _add_photo(conn, 2, datetime(2020, 6, 1, 10, 2))
    live = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=0,
                          embedding=ALICE)
    framed = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=1,
                            embedding=ALICE2)
    other = _add_detection(face_ops, run_id=seed_run, file_id=2,
                           embedding=BOB)
    era = ("2020-01-01T00:00:00", "2022-07-01T00:00:00")
    _make_cluster(conn, face_ops, run_id=seed_run, key="x",
                  era_start=era[0], era_end=era[1],
                  representative=ALICE, detection_ids=[live, framed])
    _make_cluster(conn, face_ops, run_id=seed_run, key="y",
                  era_start=era[0], era_end=era[1],
                  representative=BOB, detection_ids=[other])
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path).run(run_id=link_run)
    assert stats["same_photo_flags"] == 1
    flag, = face_ops.get_pending_same_photo_flags()
    assert {flag["detection_a"], flag["detection_b"]} == {live, framed}
    assert flag["similarity"] >= 0.7

    resolve_run = _start_run(conn, command="ui-flag-dismiss")
    face_ops.resolve_same_photo_flag(run_id=resolve_run,
                                     action_id=flag["action_id"],
                                     note="dismissed: twins")
    relink_run = _start_run(conn)
    conn.commit()

    CrossAgeLinker(db_path).run(run_id=relink_run)
    assert face_ops.get_pending_same_photo_flags() == [], (
        "a dismissed flag must not resurrect on re-link")


# ---------------------------------------------------------------------------
# Accepted-state durability (review findings #1 and #2)
# ---------------------------------------------------------------------------

def test_recluster_never_touches_accepted_clusters(db):
    """Cluster keys are deterministic per era window, so a re-cluster run
    regenerates the keys of accepted clusters. The accepted row must stay
    byte-identical; the new generation takes a suffixed key."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn, command="scan")
    for i in range(1, 4):
        _add_photo(conn, i, datetime(2020, 6, i, 10, 0))
        _add_detection(face_ops, run_id=scan_run, file_id=i, embedding=ALICE)
    first_cluster_run = _start_run(conn, command="cluster")
    conn.commit()

    _run_pipeline(db_path, [0, 0, 0]).run(run_id=first_cluster_run)
    cluster_id, cluster_key, rep_before = conn.execute(
        "SELECT id, cluster_key, representative_embedding FROM face_clusters "
        "WHERE status = 'proposed'").fetchone()

    accept_run = _start_run(conn, command="accept")
    face_ops.accept_cluster(run_id=accept_run, cluster_id=cluster_id)
    person = face_ops.create_person(run_id=accept_run, display_name="Ava")
    face_ops.link_cluster_to_person(run_id=accept_run, cluster_id=cluster_id,
                                    person_id=person,
                                    link_method="manual_review")
    second_cluster_run = _start_run(conn, command="cluster")
    conn.commit()

    _run_pipeline(db_path, [0, 0, 0]).run(run_id=second_cluster_run)

    status, rep_after = conn.execute(
        "SELECT status, representative_embedding FROM face_clusters "
        "WHERE id = ?", (cluster_id,)).fetchone()
    assert status == "accepted", "re-clustering must not revert accepted state"
    assert rep_after == rep_before, "accepted representative is immutable"

    suffixed = conn.execute(
        "SELECT cluster_key, status FROM face_clusters WHERE cluster_key LIKE ?",
        (f"{cluster_key}@%",)).fetchall()
    assert suffixed == [(f"{cluster_key}@{second_cluster_run}", "proposed")], (
        "the colliding generation takes its own suffixed key")

    polluted = conn.execute(
        "SELECT COUNT(*) FROM face_cluster_members "
        "WHERE cluster_id = ? AND status = 'proposed'",
        (cluster_id,)).fetchone()[0]
    assert polluted == 0, "no new proposed memberships on the accepted cluster"


def test_upsert_cluster_returns_accepted_row_untouched(db):
    _db_path, conn, face_ops = db
    run_id = _start_run(conn, command="cluster")
    cluster_id = face_ops.upsert_cluster(
        run_id=run_id, cluster_key="k", model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG, status="accepted",
        era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
        representative_embedding=[1.0, 0.0])

    again = face_ops.upsert_cluster(
        run_id=_start_run(conn, command="cluster"), cluster_key="k",
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        era_start="2020-01-01T00:00:00", era_end="2022-01-01T00:00:00",
        representative_embedding=[0.0, 1.0])

    assert again == cluster_id
    status, era_start = conn.execute(
        "SELECT status, era_start FROM face_clusters WHERE id = ?",
        (cluster_id,)).fetchone()
    assert (status, era_start) == ("accepted", "2010-01-01T00:00:00")


def test_detection_relabel_keeps_single_accepted_owner(db):
    """Relabeling a face retracts the previous person's accepted link, and
    labeling back reactivates the retracted row instead of silently
    ignoring it."""
    _db_path, conn, face_ops = db
    run_id = _start_run(conn, command="scan")
    _add_photo(conn, 1)
    det = _add_detection(face_ops, run_id=run_id, file_id=1, embedding=ALICE)
    ann = face_ops.create_person(run_id=run_id, display_name="Ann")
    ben = face_ops.create_person(run_id=run_id, display_name="Ben")

    def accepted_owners():
        return {row[0] for row in conn.execute(
            "SELECT person_id FROM face_person_links "
            "WHERE detection_id = ? AND status = 'accepted'", (det,))}

    face_ops.link_detection_to_person(run_id=run_id, detection_id=det,
                                      person_id=ann, confidence=None,
                                      link_method="test")
    assert accepted_owners() == {ann}

    face_ops.link_detection_to_person(run_id=run_id, detection_id=det,
                                      person_id=ben, confidence=None,
                                      link_method="test")
    assert accepted_owners() == {ben}, "one accepted owner at a time"

    face_ops.link_detection_to_person(run_id=run_id, detection_id=det,
                                      person_id=ann, confidence=None,
                                      link_method="test")
    assert accepted_owners() == {ann}, "retracted link must reactivate"
    total_rows = conn.execute(
        "SELECT COUNT(*) FROM face_person_links WHERE detection_id = ?",
        (det,)).fetchone()[0]
    assert total_rows == 2, "reactivation reuses rows, never duplicates"


def test_cluster_link_refuses_second_owner_and_reactivates(db):
    _db_path, conn, face_ops = db
    run_id = _start_run(conn, command="cluster")
    _add_photo(conn, 1)
    det = _add_detection(face_ops, run_id=run_id, file_id=1, embedding=ALICE)
    cluster_id = _make_cluster(conn, face_ops, run_id=run_id, key="c",
                               era_start="2010-01-01T00:00:00",
                               era_end="2012-01-01T00:00:00",
                               representative=ALICE, detection_ids=[det])
    ann = face_ops.create_person(run_id=run_id, display_name="Ann")
    ben = face_ops.create_person(run_id=run_id, display_name="Ben")

    link_id = face_ops.link_cluster_to_person(
        run_id=run_id, cluster_id=cluster_id, person_id=ann,
        link_method="test")
    with pytest.raises(ValueError, match="already linked"):
        face_ops.link_cluster_to_person(
            run_id=run_id, cluster_id=cluster_id, person_id=ben,
            link_method="test")

    conn.execute("UPDATE face_person_links SET status = 'retracted' "
                 "WHERE id = ?", (link_id,))
    again = face_ops.link_cluster_to_person(
        run_id=run_id, cluster_id=cluster_id, person_id=ann,
        link_method="test")
    assert again == link_id
    status = conn.execute("SELECT status FROM face_person_links WHERE id = ?",
                          (link_id,)).fetchone()[0]
    assert status == "accepted", "relink after unwind must reactivate"


def test_persons_summary_counts_direct_detection_labels(db):
    _db_path, conn, face_ops = db
    run_id = _start_run(conn, command="scan")
    _add_photo(conn, 1)
    det = _add_detection(face_ops, run_id=run_id, file_id=1, embedding=ALICE)
    person = face_ops.create_person(run_id=run_id, display_name="Ava")
    face_ops.link_detection_to_person(run_id=run_id, detection_id=det,
                                      person_id=person, confidence=None,
                                      link_method="photo_label")
    conn.commit()

    summary, = face_ops.get_persons_summary()
    assert summary["display_name"] == "Ava"
    assert summary["detections"] == 1, "direct labels must count"
    assert summary["clusters"] == 0


def test_cli_rejects_nonsense_numeric_arguments(tmp_path):
    from photo_organizer.faces.cli import main

    assert main(["--db", str(tmp_path / "x.db"), "cluster",
                 "--era-size", "0"]) == 1
    assert main(["--db", str(tmp_path / "x.db"), "link",
                 "--min-confidence", "7"]) == 1
    assert main(["--db", str(tmp_path / "x.db"), "refine",
                 "--threshold", "-1"]) == 1


def test_standard_eras_degenerate_inputs():
    from photo_organizer.faces.clustering import compute_standard_eras

    single = datetime(2020, 6, 1, 10, 0)
    eras = compute_standard_eras(single, single)
    assert len(eras) == 1
    assert eras[0][0] <= single < eras[0][1]

    with pytest.raises(ValueError, match="positive"):
        compute_standard_eras(single, single, era_size_years=0)


# ---------------------------------------------------------------------------
# Named-conflict diagnosis
# ---------------------------------------------------------------------------

def test_find_named_merge_conflicts_reports_bridge(db):
    """Component A—B—C where A belongs to one named person and C to
    another: the finder must report the conflict with the two-edge bridge,
    and rejecting a bridge edge must clear it."""
    from photo_organizer.faces.linking import find_named_merge_conflicts

    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="cluster")
    clusters = {}
    era = ("2020-01-01T00:00:00", "2022-07-01T00:00:00")
    for i, key in enumerate(("a", "b", "c"), start=1):
        _add_photo(conn, i)
        det = _add_detection(face_ops, run_id=run_id, file_id=i,
                             embedding=ALICE)
        clusters[key] = _make_cluster(
            conn, face_ops, run_id=run_id, key=key,
            era_start=era[0], era_end=era[1],
            representative=ALICE, detection_ids=[det])

    james = face_ops.create_person(run_id=run_id, display_name="James")
    matt = face_ops.create_person(run_id=run_id, display_name="Matt")
    face_ops.link_cluster_to_person(run_id=run_id,
                                    cluster_id=clusters["a"],
                                    person_id=james,
                                    link_method="manual_review")
    face_ops.link_cluster_to_person(run_id=run_id,
                                    cluster_id=clusters["c"],
                                    person_id=matt,
                                    link_method="manual_review")

    edge_ab = _insert_merge_action(conn, run_id, method="window_duplicate",
                                   key="e-ab", confidence=100,
                                   cluster_a=clusters["a"],
                                   cluster_b=clusters["b"])
    edge_bc = _insert_merge_action(conn, run_id,
                                   method="same_event_tracklet",
                                   key="e-bc", confidence=75,
                                   cluster_a=clusters["b"],
                                   cluster_b=clusters["c"])
    conn.commit()

    db_ops = DBOperations(conn)
    conflict, = find_named_merge_conflicts(db_ops)
    assert [name for _pid, name in conflict["persons"]] == ["James", "Matt"]
    assert conflict["component_clusters"] == 3
    assert conflict["pending_proposals"] == 2
    bridge, = conflict["bridges"]
    assert {bridge["person_a"], bridge["person_b"]} == {"James", "Matt"}
    assert [e["action_id"] for e in bridge["edges"]] == [edge_ab, edge_bc]

    # Rejecting the wrong link splits the component: conflict resolved.
    reject_run = _start_run(conn, command="reject")
    face_ops.reject_face_proposals([edge_bc], run_id=reject_run)
    conn.commit()
    assert find_named_merge_conflicts(db_ops) == []


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
