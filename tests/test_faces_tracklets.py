"""
Same-event tracklet tests: event grouping, mutual-nearest-neighbor
matching, the same-photo prior (never auto-link, flag high-similarity
pairs), the depiction verdict, and the tracklet evidence tier in the
linker.
"""
import json
import sqlite3
from datetime import datetime

import numpy as np
import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces import config
from photo_organizer.faces.db_ops import FaceDBOperations
from photo_organizer.faces.linking import CrossAgeLinker
from photo_organizer.faces.tracklets import build_tracklets, group_into_events


def _unit(vec):
    arr = np.asarray(vec, dtype=np.float32)
    return arr / np.linalg.norm(arr)


def _dt(hour, minute):
    return datetime(2020, 6, 1, hour, minute)


# ---------------------------------------------------------------------------
# Event grouping
# ---------------------------------------------------------------------------

def test_group_into_events_splits_on_gap():
    photos = [
        (1, _dt(10, 0)), (2, _dt(10, 5)), (3, _dt(10, 12)),   # one event
        (4, _dt(14, 0)),                                       # singleton
        (5, _dt(18, 0)), (6, _dt(18, 14)),                     # second event
    ]
    events = group_into_events(photos, gap_minutes=15.0)
    assert events == [[1, 2, 3], [4], [5, 6]]


def test_group_into_events_sorts_input():
    photos = [(2, _dt(10, 5)), (1, _dt(10, 0))]
    assert group_into_events(photos, gap_minutes=15.0) == [[1, 2]]


# ---------------------------------------------------------------------------
# Tracklet building (pure)
# ---------------------------------------------------------------------------

ALICE = _unit([1.0, 0.1, 0.0, 0.0])
ALICE2 = _unit([1.0, 0.2, 0.0, 0.0])   # Alice, slightly different frame
BOB = _unit([0.0, 0.1, 1.0, 0.0])
CAROL = _unit([0.0, 1.0, 0.0, 0.3])


def _row(det_id, file_id, when, emb):
    return (det_id, file_id, when.isoformat() if when else None, emb)


def test_mutual_nn_chains_one_person_across_event():
    rows = [
        _row(1, 101, _dt(10, 0), ALICE),
        _row(2, 101, _dt(10, 0), BOB),
        _row(3, 102, _dt(10, 3), ALICE2),
        _row(4, 102, _dt(10, 3), BOB),
        _row(5, 103, _dt(10, 6), ALICE),
    ]
    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5)

    assert result.events == 1
    assert result.photos_grouped == 3
    # Alice chains 1-3-5, Bob chains 2-4.
    assert result.tracklets == [[1, 3, 5], [2, 4]]
    # No same-photo links ever, even though 1 and 2 share a photo.
    for det_a, det_b in result.edges:
        assert det_a != det_b


def test_similarity_floor_blocks_weak_matches():
    rows = [
        _row(1, 101, _dt(10, 0), ALICE),
        _row(2, 102, _dt(10, 3), BOB),   # only face in the next photo
    ]
    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5)
    assert result.tracklets == []
    assert result.edges == {}


def test_non_mutual_best_match_does_not_link():
    # Photo 101 has two Alice-like faces; photo 102 has one. The single
    # face's best match wins; the runner-up must not also link.
    rows = [
        _row(1, 101, _dt(10, 0), _unit([1.0, 0.10, 0.0, 0.0])),
        _row(2, 101, _dt(10, 0), _unit([1.0, 0.22, 0.0, 0.0])),
        _row(3, 102, _dt(10, 3), _unit([1.0, 0.20, 0.0, 0.0])),
    ]
    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5)
    assert result.tracklets == [[2, 3]]


def test_adjacency_bridges_a_missed_photo():
    # Alice is missing from the middle photo; depth-2 adjacency still
    # connects her detections around it.
    rows = [
        _row(1, 101, _dt(10, 0), ALICE),
        _row(2, 102, _dt(10, 3), BOB),
        _row(3, 103, _dt(10, 6), ALICE2),
    ]
    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5,
                             file_adjacency=2)
    assert [1, 3] in result.tracklets

    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5,
                             file_adjacency=1)
    assert [1, 3] not in result.tracklets


def test_events_do_not_match_across_gap():
    rows = [
        _row(1, 101, _dt(10, 0), ALICE),
        _row(2, 102, _dt(16, 0), ALICE),   # hours later — different event
    ]
    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5)
    assert result.tracklets == []


def test_undated_rows_are_skipped():
    rows = [
        _row(1, 101, _dt(10, 0), ALICE),
        _row(2, 102, None, ALICE),
    ]
    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5)
    assert result.tracklets == []


def test_same_photo_lookalikes_flagged_never_linked():
    # A live face and a framed photo of the same person in one shot: very
    # similar embeddings, same file. Must be flagged for review, never
    # joined into a tracklet.
    rows = [
        _row(1, 101, _dt(10, 0), ALICE),
        _row(2, 101, _dt(10, 0), ALICE2),
        _row(3, 102, _dt(10, 3), CAROL),
    ]
    result = build_tracklets(rows, gap_minutes=15.0, min_similarity=0.5,
                             same_photo_review_sim=0.7)
    assert len(result.same_photo_flags) == 1
    file_id, det_a, det_b, sim = result.same_photo_flags[0]
    assert (file_id, det_a, det_b) == (101, 1, 2)
    assert sim >= 0.7
    assert result.tracklets == []


# ---------------------------------------------------------------------------
# DB fixtures
# ---------------------------------------------------------------------------

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


def _add_photo(conn, file_id, captured):
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


def _add_detection(face_ops, *, run_id, file_id, det_index, embedding,
                   confidence=0.9):
    detection_id = face_ops.record_detection(
        run_id=run_id, file_id=file_id, detection_index=det_index,
        bbox=(1.0, 1.0, 10.0, 10.0), confidence=confidence,
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


def _tracklet_proposals(conn):
    rows = conn.execute(
        """
        SELECT confidence, payload_json FROM run_actions
        WHERE action_type = 'face_cluster_merge' AND status = 'proposed'
          AND method = 'same_event_tracklet'
        ORDER BY id
        """
    ).fetchall()
    return [(confidence, json.loads(payload)) for confidence, payload in rows]


ERA = ("2020-01-01T00:00:00", "2022-07-01T00:00:00")


# ---------------------------------------------------------------------------
# Depiction verdict
# ---------------------------------------------------------------------------

def test_depiction_verdict_excludes_from_working_sets(db):
    _db_path, conn, face_ops = db
    run_id = _start_run(conn, command="scan")
    _add_photo(conn, 1, _dt(10, 0))
    live = _add_detection(face_ops, run_id=run_id, file_id=1, det_index=0,
                          embedding=ALICE)
    framed = _add_detection(face_ops, run_id=run_id, file_id=1, det_index=1,
                            embedding=ALICE2)

    face_ops.mark_detection_depiction(run_id=run_id, detection_id=framed)

    status = conn.execute("SELECT status FROM face_detections WHERE id = ?",
                          (framed,)).fetchone()[0]
    assert status == 'depiction'

    clustering_ids = [row[0] for row in face_ops.get_embeddings_with_capture_dates(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )]
    assert clustering_ids == [live]

    tracklet_ids = [row[0] for row in face_ops.get_detections_for_tracklets(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )]
    assert tracklet_ids == [live]

    action = conn.execute(
        "SELECT status, method FROM run_actions "
        "WHERE action_type = 'face_detection_depiction'"
    ).fetchone()
    assert action == ('applied', 'user_depiction')


def test_depiction_embedding_never_shapes_anchors(db):
    _db_path, conn, face_ops = db
    run_id = _start_run(conn, command="scan")
    _add_photo(conn, 1, _dt(10, 0))
    live = _add_detection(face_ops, run_id=run_id, file_id=1, det_index=0,
                          embedding=ALICE)
    framed = _add_detection(face_ops, run_id=run_id, file_id=1, det_index=1,
                            embedding=CAROL)

    person = face_ops.create_person(run_id=run_id, display_name="Ava")
    for det in (live, framed):
        face_ops.link_detection_to_person(
            run_id=run_id, detection_id=det, person_id=person,
            confidence=None, link_method="test",
        )
    face_ops.mark_detection_depiction(run_id=run_id, detection_id=framed)
    conn.commit()

    anchors = face_ops.get_labeled_person_embeddings()
    assert len(anchors[person]) == 1
    assert np.allclose(anchors[person][0], ALICE, atol=1e-6)


# ---------------------------------------------------------------------------
# Tracklet tier in the linker
# ---------------------------------------------------------------------------

def test_tracklet_tier_merges_same_era_split(db):
    """The Ava case: one person's burst split into two same-window density
    clusters. The scored tier never compares same-window pairs; tracklet
    evidence must bridge them."""
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")

    for file_id, minute in ((1, 0), (2, 2), (3, 4)):
        _add_photo(conn, file_id, _dt(10, minute))
    d1 = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=0,
                        embedding=ALICE)
    d2 = _add_detection(face_ops, run_id=seed_run, file_id=2, det_index=0,
                        embedding=ALICE2)
    d3 = _add_detection(face_ops, run_id=seed_run, file_id=3, det_index=0,
                        embedding=ALICE)

    id_a = _make_cluster(conn, face_ops, run_id=seed_run, key="era:2020#000",
                         era_start=ERA[0], era_end=ERA[1],
                         representative=ALICE, detection_ids=[d1])
    id_b = _make_cluster(conn, face_ops, run_id=seed_run, key="era:2020#001",
                         era_start=ERA[0], era_end=ERA[1],
                         representative=ALICE2, detection_ids=[d2, d3])
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path).run(run_id=link_run)

    assert stats["events_grouped"] == 1
    assert stats["tracklets_built"] == 1
    assert stats["tracklet_merges_proposed"] == 1
    # Same era window: the scored tier stays silent; only tracklet
    # evidence connects the pair.
    assert stats["suggestions_proposed"] == 0

    proposals = _tracklet_proposals(conn)
    assert len(proposals) == 1
    confidence, payload = proposals[0]
    assert {payload["cluster_a_id"], payload["cluster_b_id"]} == {id_a, id_b}
    signals = payload["signals"]
    # d1-d2 and d1-d3 cross the cluster boundary; d2-d3 is internal to B.
    assert signals["tracklet_pairs"] == 2
    assert signals["mean_pair_similarity"] > 0.9
    assert signals["same_photo_overlap"] == 0
    expected = min(config.TRACKLET_CONFIDENCE_BASE
                   + config.TRACKLET_CONFIDENCE_STEP,
                   config.TRACKLET_CONFIDENCE_CAP)
    assert confidence == int(round(expected * 100))


def test_tracklet_merge_penalized_when_clusters_share_a_photo(db):
    """If merging would put one person in a photo twice (live face plus a
    framed photo, or twins), the proposal survives but carries the
    contradiction: lower confidence, same_photo_overlap signal."""
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")

    _add_photo(conn, 1, _dt(10, 0))
    _add_photo(conn, 2, _dt(10, 2))
    dx1 = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=0,
                         embedding=ALICE)
    dy1 = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=1,
                         embedding=BOB)
    dx2 = _add_detection(face_ops, run_id=seed_run, file_id=2, det_index=0,
                         embedding=ALICE2)

    _make_cluster(conn, face_ops, run_id=seed_run, key="era:2020#000",
                  era_start=ERA[0], era_end=ERA[1],
                  representative=ALICE, detection_ids=[dx1])
    # Cluster B holds Alice's second frame AND the other face from photo 1,
    # so merging A and B puts one person in photo 1 twice.
    _make_cluster(conn, face_ops, run_id=seed_run, key="era:2020#001",
                  era_start=ERA[0], era_end=ERA[1],
                  representative=ALICE2, detection_ids=[dx2, dy1])
    link_run = _start_run(conn)
    conn.commit()

    CrossAgeLinker(db_path).run(run_id=link_run)

    proposals = _tracklet_proposals(conn)
    assert len(proposals) == 1
    confidence, payload = proposals[0]
    assert payload["signals"]["same_photo_overlap"] == 1
    expected = max(config.TRACKLET_CONFIDENCE_BASE
                   - config.TRACKLET_SAME_PHOTO_PENALTY, 0.05)
    assert confidence == int(round(expected * 100))


def test_tracklet_tier_skips_window_duplicates_and_can_be_disabled(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")

    for file_id, minute in ((1, 0), (2, 2)):
        _add_photo(conn, file_id, _dt(10, minute))
    d1 = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=0,
                        embedding=ALICE)
    d2 = _add_detection(face_ops, run_id=seed_run, file_id=2, det_index=0,
                        embedding=ALICE2)

    # Overlapping windows clustered the same detections twice: the
    # window-duplicate tier owns this pair; the tracklet tier must not
    # double-propose it.
    _make_cluster(conn, face_ops, run_id=seed_run, key="era:A",
                  era_start=ERA[0], era_end=ERA[1],
                  representative=ALICE, detection_ids=[d1, d2])
    _make_cluster(conn, face_ops, run_id=seed_run, key="era:B",
                  era_start="2021-01-01T00:00:00", era_end="2023-07-01T00:00:00",
                  representative=ALICE, detection_ids=[d1, d2])
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path).run(run_id=link_run)
    assert stats["duplicates_proposed"] == 1
    assert stats["tracklet_merges_proposed"] == 0

    # And the whole tier can be switched off.
    off_run = _start_run(conn)
    conn.commit()
    stats = CrossAgeLinker(db_path, use_tracklets=False).run(run_id=off_run)
    assert stats["tracklet_merges_proposed"] == 0
    assert stats["tracklets_built"] == 0


def test_same_photo_flags_surface_in_link_stats(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")

    # One photo: a live face and a near-identical framed one, plus a second
    # photo so the linker has two clusters to consider.
    _add_photo(conn, 1, _dt(10, 0))
    _add_photo(conn, 2, _dt(10, 2))
    live = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=0,
                          embedding=ALICE)
    framed = _add_detection(face_ops, run_id=seed_run, file_id=1, det_index=1,
                            embedding=ALICE2)
    other = _add_detection(face_ops, run_id=seed_run, file_id=2, det_index=0,
                           embedding=BOB)

    _make_cluster(conn, face_ops, run_id=seed_run, key="era:2020#000",
                  era_start=ERA[0], era_end=ERA[1],
                  representative=ALICE, detection_ids=[live, framed])
    _make_cluster(conn, face_ops, run_id=seed_run, key="era:2020#001",
                  era_start=ERA[0], era_end=ERA[1],
                  representative=BOB, detection_ids=[other])
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path).run(run_id=link_run)
    assert stats["same_photo_flags"] == 1
