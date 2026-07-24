"""
Face clustering tests: era window computation, cluster/membership primitives,
re-clustering supersede semantics, the pipeline with an injected clustering
backend, and an end-to-end run with the real sklearn HDBSCAN when available.
"""
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces import config
from photo_organizer.faces.clustering import (
    FaceClusterPipeline,
    apply_birth_splits,
    compute_standard_eras,
)
from photo_organizer.faces.db_ops import FaceDBOperations


# ---------------------------------------------------------------------------
# Era computation (pure functions)
# ---------------------------------------------------------------------------

def test_standard_eras_overlap_and_cover_range():
    min_date = datetime(2010, 1, 1)
    max_date = datetime(2015, 1, 1)
    eras = compute_standard_eras(min_date, max_date, era_size_years=2.0)

    assert eras[0][0] == min_date
    assert eras[-1][1] >= max_date
    # 50% overlap: each era starts halfway through the previous one.
    for (start_a, end_a), (start_b, _) in zip(eras, eras[1:]):
        assert start_b < end_a

    # Every date in range is covered by at least one era.
    probe = datetime(2013, 6, 15)
    assert any(start <= probe < end for start, end in eras)


def test_apply_birth_splits_splits_at_interior_birth():
    era = (datetime(2010, 1, 1), datetime(2012, 7, 1))
    birth = datetime(2011, 3, 15)

    result = apply_birth_splits(era, [birth], tolerance_days=180)

    assert result == [
        (datetime(2010, 1, 1), birth),
        (birth, datetime(2012, 7, 1)),
    ]


def test_apply_birth_splits_no_interior_birth_returns_original():
    era = (datetime(2010, 1, 1), datetime(2012, 7, 1))
    # Outside the window, and inside but within tolerance of an edge.
    outside = datetime(2015, 1, 1)
    near_edge = datetime(2010, 2, 1)

    assert apply_birth_splits(era, [outside], tolerance_days=180) == [era]
    assert apply_birth_splits(era, [near_edge], tolerance_days=180) == [era]
    assert apply_birth_splits(era, [], tolerance_days=180) == [era]


def test_apply_birth_splits_merges_close_births_one_split_point():
    """Two births close together (e.g. two months apart) share ONE split
    point instead of carving an unusably thin sliver between them — the
    period stays one (harder to review) window rather than fragmenting."""
    era = (datetime(2000, 1, 1), datetime(2005, 1, 1))
    first = datetime(2002, 8, 25)
    second = datetime(2002, 10, 11)  # 47 days after `first`

    result = apply_birth_splits(era, [first, second], tolerance_days=180)

    assert result == [(era[0], first), (first, era[1])]


def test_apply_birth_splits_multiple_far_apart_births():
    era = (datetime(2000, 1, 1), datetime(2012, 1, 1))
    first = datetime(2003, 6, 1)
    second = datetime(2008, 6, 1)  # ~5 years later, well past tolerance

    result = apply_birth_splits(era, [first, second], tolerance_days=180)

    assert result == [
        (era[0], first), (first, second), (second, era[1]),
    ]


# ---------------------------------------------------------------------------
# Fixtures
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


def _start_run(conn):
    return conn.execute(
        """
        INSERT INTO command_runs (tool, command, started_at, exit_status,
                                  dry_run, argv_json)
        VALUES ('photo-faces', 'cluster', '2026-01-01T00:00:00Z', 'running', 0, '[]')
        """
    ).lastrowid


def _seed_detection(conn, face_ops, *, run_id, file_id, capture_dt,
                    embedding, detection_index=0):
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
        """
        INSERT OR IGNORE INTO media_metadata (file_id, capture_datetime)
        VALUES (?, ?)
        """,
        (file_id, capture_dt.isoformat() if capture_dt else None),
    )
    detection_id = face_ops.record_detection(
        run_id=run_id,
        file_id=file_id,
        detection_index=detection_index,
        bbox=(1.0, 1.0, 10.0, 10.0),
        confidence=0.9,
        model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG,
        image_hash=f"hash-{file_id}",
    )
    face_ops.record_embedding(
        run_id=run_id,
        detection_id=detection_id,
        embedding=embedding,
        model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG,
    )
    return detection_id


def _unit(vec):
    arr = np.asarray(vec, dtype=np.float32)
    return (arr / np.linalg.norm(arr)).tolist()


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def test_upsert_cluster_roundtrips_era_and_representative(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)

    cluster_id = face_ops.upsert_cluster(
        run_id=run_id,
        cluster_key="era:20100101-20120101#000",
        model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG,
        era_start="2010-01-01T00:00:00",
        era_end="2012-01-01T00:00:00",
        representative_embedding=[0.6, 0.8],
        payload={"face_count": 3},
    )
    # Upsert with refreshed metadata keeps the same row.
    again = face_ops.upsert_cluster(
        run_id=run_id,
        cluster_key="era:20100101-20120101#000",
        model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG,
        era_start="2010-01-01T00:00:00",
        era_end="2012-01-01T00:00:00",
        representative_embedding=[0.8, 0.6],
        payload={"face_count": 4},
    )
    conn.commit()

    assert again == cluster_id
    era_start, era_end, dim = conn.execute(
        "SELECT era_start, era_end, representative_dim FROM face_clusters WHERE id = ?",
        (cluster_id,),
    ).fetchone()
    assert (era_start, era_end, dim) == ("2010-01-01T00:00:00", "2012-01-01T00:00:00", 2)


def test_supersede_proposed_clusters_spares_accepted(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    det = _seed_detection(conn, face_ops, run_id=run_id, file_id=1,
                          capture_dt=datetime(2011, 1, 1),
                          embedding=_unit([1, 0, 0, 0]))

    face_ops.propose_cluster_assignment(
        run_id=run_id, detection_id=det, cluster_key="proposed-one",
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    accepted_id = face_ops.upsert_cluster(
        run_id=run_id, cluster_key="accepted-one",
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        status="accepted",
    )
    conn.commit()

    rerun_id = _start_run(conn)
    count = face_ops.supersede_proposed_clusters(
        run_id=rerun_id,
        model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG,
    )
    conn.commit()

    assert count == 1
    statuses = dict(conn.execute(
        "SELECT cluster_key, status FROM face_clusters"
    ).fetchall())
    assert statuses == {"proposed-one": "superseded", "accepted-one": "accepted"}
    member_status = conn.execute(
        "SELECT status FROM face_cluster_members"
    ).fetchone()[0]
    assert member_status == "superseded"


def test_embeddings_reader_excludes_no_faces_sentinels(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    det = _seed_detection(conn, face_ops, run_id=run_id, file_id=1,
                          capture_dt=datetime(2011, 5, 1),
                          embedding=[0.5, 0.5])
    face_ops.record_no_faces_scan(
        run_id=run_id, file_id=2, model_name=config.MODEL_NAME,
        model_version=config.MODEL_VERSION_TAG, image_hash=None,
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name, orig_path,
                                     first_seen_at, last_seen_at)
        VALUES (2, 'hash-2', 'jpeg', '.jpg', 'y.jpg', 'C:/y.jpg',
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """
    )
    conn.commit()

    rows = face_ops.get_embeddings_with_capture_dates(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    assert len(rows) == 1
    detection_id, embedding, capture = rows[0]
    assert detection_id == det
    assert embedding == pytest.approx((0.5, 0.5))
    assert capture == "2011-05-01T00:00:00"


# ---------------------------------------------------------------------------
# Pipeline with injected clustering backend
# ---------------------------------------------------------------------------

def _fake_two_cluster_backend(embeddings):
    """Assign by dominant axis: x-axis → 0, y-axis → 1, ambiguous → noise."""
    labels = []
    for row in embeddings:
        if row[0] > 0.9:
            labels.append(0)
        elif row[1] > 0.9:
            labels.append(1)
        else:
            labels.append(-1)
    return np.array(labels), np.full(len(labels), 0.8)


def _seed_two_identities(conn, face_ops, run_id, *, n_per_identity=3):
    detections = {"x": [], "y": []}
    file_id = 100
    for i in range(n_per_identity):
        capture = datetime(2011, 3, 1 + i)
        detections["x"].append(_seed_detection(
            conn, face_ops, run_id=run_id, file_id=file_id,
            capture_dt=capture, embedding=_unit([1, 0.01 * i, 0, 0]),
        ))
        file_id += 1
        detections["y"].append(_seed_detection(
            conn, face_ops, run_id=run_id, file_id=file_id,
            capture_dt=capture, embedding=_unit([0.01 * i, 1, 0, 0]),
        ))
        file_id += 1
    return detections


def test_cluster_pipeline_proposes_clusters_and_memberships(db):
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    detections = _seed_two_identities(conn, face_ops, scan_run)
    cluster_run = _start_run(conn)
    conn.commit()

    pipeline = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=_fake_two_cluster_backend,
    )
    stats = pipeline.run(run_id=cluster_run)

    assert stats["detections_total"] == 6
    assert stats["clusters_proposed"] >= 2
    assert stats["memberships_proposed"] >= 6

    conn2 = sqlite3.connect(str(db_path))
    try:
        clusters = conn2.execute(
            """
            SELECT id, cluster_key, status, era_start, era_end, representative_dim
            FROM face_clusters WHERE status = 'proposed'
            """
        ).fetchall()
        # Members of one proposed cluster must be exactly one identity's
        # detections (per era; overlapping eras may repeat the grouping).
        memberships = conn2.execute(
            """
            SELECT m.cluster_id, m.detection_id
            FROM face_cluster_members m
            JOIN face_clusters c ON c.id = m.cluster_id
            WHERE m.status = 'proposed' AND c.status = 'proposed'
            """
        ).fetchall()
        actions = conn2.execute(
            "SELECT COUNT(*) FROM run_actions WHERE action_type = 'face_cluster_assign' "
            "AND status = 'proposed'"
        ).fetchone()[0]
    finally:
        conn2.close()

    assert clusters, "proposed clusters must be persisted"
    for _, key, status, era_start, era_end, rep_dim in clusters:
        assert key.startswith("era:")
        assert era_start and era_end
        assert rep_dim == 4

    by_cluster = {}
    for cluster_id, detection_id in memberships:
        by_cluster.setdefault(cluster_id, set()).add(detection_id)
    x_set, y_set = set(detections["x"]), set(detections["y"])
    for members in by_cluster.values():
        assert members <= x_set or members <= y_set, (
            "a proposed cluster must not mix the two identities"
        )
    assert actions >= 6

    # Accepted state stays untouched: no person links, no accepted members.
    conn3 = sqlite3.connect(str(db_path))
    accepted = conn3.execute(
        "SELECT COUNT(*) FROM face_cluster_members WHERE status = 'accepted'"
    ).fetchone()[0]
    conn3.close()
    assert accepted == 0


def test_recluster_supersedes_then_reuses_stable_cluster_keys(db):
    """Re-clustering identical data must not duplicate cluster rows: prior
    proposals are superseded, then deterministic cluster keys re-propose the
    same rows. Clusters that are NOT regenerated stay superseded."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    _seed_two_identities(conn, face_ops, scan_run)
    first_run = _start_run(conn)
    second_run = _start_run(conn)
    # A proposed cluster from an obsolete run that re-clustering won't
    # regenerate (different era key) — must stay superseded.
    face_ops.upsert_cluster(
        run_id=first_run, cluster_key="era:19990101-20000101#000",
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    conn.commit()

    pipeline = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=_fake_two_cluster_backend,
    )
    first_stats = pipeline.run(run_id=first_run)
    second_stats = pipeline.run(run_id=second_run)

    # Run 1 superseded the stale pre-seeded row; run 2 transiently superseded
    # everything run 1 proposed before re-proposing it under the same keys.
    assert first_stats["clusters_superseded"] == 1
    assert second_stats["clusters_superseded"] == first_stats["clusters_proposed"]
    assert second_stats["clusters_proposed"] == first_stats["clusters_proposed"]

    conn2 = sqlite3.connect(str(db_path))
    try:
        counts = dict(conn2.execute(
            "SELECT status, COUNT(*) FROM face_clusters GROUP BY status"
        ).fetchall())
        total = conn2.execute("SELECT COUNT(*) FROM face_clusters").fetchone()[0]
        member_statuses = {row[0] for row in conn2.execute(
            """
            SELECT DISTINCT m.status FROM face_cluster_members m
            JOIN face_clusters c ON c.id = m.cluster_id
            WHERE c.status = 'proposed'
            """
        )}
    finally:
        conn2.close()

    # Regenerated clusters were re-proposed in place (stable keys, no
    # duplicate rows); only the stale era row remains superseded.
    assert counts.get("proposed") == second_stats["clusters_proposed"]
    assert counts.get("superseded") == 1
    assert total == second_stats["clusters_proposed"] + 1
    assert member_statuses == {"proposed"}, (
        "memberships of re-proposed clusters must be flipped back to proposed"
    )


def test_birth_date_splits_standard_era_window_in_pipeline(db):
    """A registered birth date splits the standard window it falls inside
    at the FULL PIPELINE level (not just the pure apply_birth_splits
    function) -- faces dated before the split land in a different era
    than faces dated after it, even though both bursts sit comfortably
    inside what would otherwise be ONE 2.5-year standard window."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    birth = datetime(2010, 6, 1)

    file_id = 100
    for month in (1, 2, 3):  # burst A: well before the birth (axis 0)
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                        capture_dt=datetime(2009, month, 15),
                        embedding=_unit([1, 0.001 * file_id, 0, 0]))
        file_id += 1
    for month in (1, 2, 3):  # burst B: well after the birth (axis 1)
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                        capture_dt=datetime(2011, month, 15),
                        embedding=_unit([0.001 * file_id, 1, 0, 0]))
        file_id += 1
    face_ops.create_person(
        run_id=scan_run, display_name="Kiddo", birth_date="2010-06-01",
    )
    cluster_run = _start_run(conn)
    conn.commit()

    pipeline = FaceClusterPipeline(
        db_path, min_cluster_size=3, era_size_years=2.5,
        cluster_fn=_fake_two_cluster_backend,
    )
    stats = pipeline.run(run_id=cluster_run)
    assert stats["clusters_proposed"] >= 2

    # The fake backend would separate the two bursts by axis regardless of
    # era boundaries (and the standard grid's own 50% overlap can add a
    # second standard window besides), so neither cluster count nor a
    # single exact era pair is the reliable signal. The real proof a split
    # happened: some cluster's era boundary lands EXACTLY at the birth
    # date -- that value only ever appears via apply_birth_splits.
    era_pairs = conn.execute(
        "SELECT era_start, era_end FROM face_clusters WHERE status='proposed'"
    ).fetchall()
    boundary_dates = {row[0] for row in era_pairs} | {row[1] for row in era_pairs}
    assert birth.isoformat() in boundary_dates, (
        "some cluster's era boundary must land exactly at the birth date, "
        "proving the standard window was split there"
    )


def test_undated_detections_are_counted_not_clustered(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    _seed_detection(conn, face_ops, run_id=run_id, file_id=1,
                    capture_dt=None, embedding=[1.0, 0.0])
    cluster_run = _start_run(conn)
    conn.commit()

    pipeline = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=_fake_two_cluster_backend,
    )
    stats = pipeline.run(run_id=cluster_run)
    assert stats["detections_undated"] == 1
    assert stats["clusters_proposed"] == 0


def test_pca_reduces_clustering_input_but_not_representatives(db):
    pytest.importorskip("sklearn")
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    # 8-dim embeddings, two well-separated identities.
    file_id = 1
    for center in ([1, 0, 0, 0, 0, 0, 0, 0], [0, 1, 0, 0, 0, 0, 0, 0]):
        for i in range(4):
            noisy = [v + 0.01 * i for v in center]
            _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                            capture_dt=datetime(2011, 4, file_id),
                            embedding=_unit(noisy))
            file_id += 1
    cluster_run = _start_run(conn)
    conn.commit()

    seen_dims = []

    # PCA to 2 dims: the backend must receive reduced vectors, while stored
    # representatives keep the original dimensionality. The axis-aligned fake
    # backend won't survive PCA's rotation — split on the first principal
    # component instead.
    def pc_split_backend(embeddings):
        seen_dims.append(embeddings.shape[1])
        labels = (embeddings[:, 0] > embeddings[:, 0].mean()).astype(int)
        return labels, None

    pipeline = FaceClusterPipeline(
        db_path, min_cluster_size=3, pca_dims=2, cluster_fn=pc_split_backend,
    )
    stats = pipeline.run(run_id=cluster_run)

    assert seen_dims and set(seen_dims) == {2}
    assert stats["clusters_proposed"] >= 2
    rep_dims = {row[0] for row in conn.execute(
        "SELECT representative_dim FROM face_clusters WHERE status = 'proposed'"
    )}
    assert rep_dims == {8}, "representatives must stay in the original space"


def test_pca_skipped_when_no_room_to_reduce(db):
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    _seed_two_identities(conn, face_ops, scan_run)
    cluster_run = _start_run(conn)
    conn.commit()

    seen_dims = []

    def spy_backend(embeddings):
        seen_dims.append(embeddings.shape[1])
        return _fake_two_cluster_backend(embeddings)

    # Default pca_dims (64) >= the 4 test dims: no reduction, axis-aligned
    # fake backend keeps working.
    pipeline = FaceClusterPipeline(db_path, min_cluster_size=3,
                                   cluster_fn=spy_backend)
    stats = pipeline.run(run_id=cluster_run)
    assert set(seen_dims) == {4}
    assert stats["clusters_proposed"] >= 2


def test_embeddings_query_applies_working_det_score_floor(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    conn.execute(
        """
        INSERT INTO files (id, hash, type, ext, orig_name, orig_path,
                           first_seen_at, last_seen_at)
        VALUES (1, 'h1', 'jpeg', '.jpg', 'x.jpg', 'C:/x.jpg',
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """
    )
    for index, confidence in ((0, 0.55), (1, 0.9)):
        det = face_ops.record_detection(
            run_id=run_id, file_id=1, detection_index=index,
            bbox=(1.0, 1.0, 10.0, 10.0), confidence=confidence,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
            image_hash="h1",
        )
        face_ops.record_embedding(
            run_id=run_id, detection_id=det, embedding=[1.0, 0.0],
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
    conn.commit()

    all_rows = face_ops.get_embeddings_with_capture_dates(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    filtered = face_ops.get_embeddings_with_capture_dates(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        min_det_score=0.7,
    )
    assert len(all_rows) == 2
    assert len(filtered) == 1


def test_mark_cluster_not_faces_removes_detections_everywhere(db):
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    # Same detections proposed into two overlapping-window clusters.
    dets = [
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=i,
                        capture_dt=datetime(2011, 1, i),
                        embedding=_unit([1, 0, 0, 0]))
        for i in (1, 2, 3)
    ]
    for key in ("junk-a", "junk-b"):
        face_ops.upsert_cluster(
            run_id=scan_run, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
        for det in dets:
            face_ops.propose_cluster_assignment(
                run_id=scan_run, detection_id=det, cluster_key=key,
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )
    junk_a = conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = 'junk-a'"
    ).fetchone()[0]
    review_run = _start_run(conn)
    conn.commit()

    marked = face_ops.mark_cluster_not_faces(run_id=review_run, cluster_id=junk_a)
    conn.commit()

    assert marked == 3
    assert conn.execute(
        "SELECT status FROM face_clusters WHERE id = ?", (junk_a,)
    ).fetchone()[0] == "rejected"
    det_statuses = {row[0] for row in conn.execute(
        "SELECT status FROM face_detections")}
    assert det_statuses == {"not_a_face"}
    # Gone from clustering input entirely (both clusters shared them).
    assert face_ops.get_embeddings_with_capture_dates(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    ) == []
    # The file remains scanned — no re-detection on the next scan.
    assert face_ops.get_unscanned_files(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    ) == []
    action = conn.execute(
        "SELECT action_type, status FROM run_actions "
        "WHERE action_type = 'face_cluster_reject'"
    ).fetchone()
    assert action == ("face_cluster_reject", "applied")


def test_coherence_gate_dissolves_mixed_blobs_and_keeps_real_clusters(db):
    """Sparse-window HDBSCAN can chain unrelated faces into one blob (four
    people and a goat at mutual cos ~0.2). The coherence gate must dissolve
    such blobs while leaving a genuine cluster untouched."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)

    # A real identity: five tight members along one axis.
    file_id = 1
    for i in range(5):
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                        capture_dt=datetime(2011, 3, 1 + i),
                        embedding=_unit([1, 0, 0, 0, 0, 0, 0, 0.02 * i]))
        file_id += 1
    # A degenerate blob: six mutually-ORTHOGONAL "faces" (distinct axes) —
    # four different people and a goat have no common direction, so each
    # member's similarity to the blob centroid is only 1/sqrt(6) ~ 0.41.
    for axis in range(1, 7):
        base = [0.0] * 8
        base[axis] = 1.0
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                        capture_dt=datetime(2011, 3, 10 + axis),
                        embedding=_unit(base))
        file_id += 1
    cluster_run = _start_run(conn)
    conn.commit()

    def blob_backend(embeddings):
        # Simulate degenerate HDBSCAN: the tight identity is cluster 0,
        # everything else gets chained into cluster 1.
        labels = [0 if row[0] > 0.9 else 1 for row in embeddings]
        return np.array(labels), None

    pipeline = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=blob_backend,
        min_member_sim=0.45,
    )
    stats = pipeline.run(run_id=cluster_run)

    assert stats["clusters_proposed"] == 1, "only the coherent cluster survives"
    assert stats["clusters_dropped_incoherent"] >= 1
    assert stats["members_trimmed_incoherent"] >= 3
    members = conn.execute(
        """
        SELECT COUNT(*) FROM face_cluster_members m
        JOIN face_clusters c ON c.id = m.cluster_id
        WHERE c.status = 'proposed' AND m.status = 'proposed'
        """
    ).fetchone()[0]
    assert members == 5, "the blob's members must not be persisted"

    # With both gates disabled, the blob would have been proposed. (The
    # cohesion gate must be off too — orthogonal members have no graph
    # edges, so it would dissolve the blob on its own.)
    rerun = _start_run(conn)
    conn.commit()
    stats_open = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=blob_backend,
        min_member_sim=0, cohesion_edge_sim=0,
    ).run(run_id=rerun)
    assert stats_open["clusters_proposed"] == 2


def test_coherence_gate_trims_outliers_but_keeps_cluster(db):
    """A mostly-clean cluster with one intruder keeps its core and sheds the
    intruder, rather than being dropped."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    file_id = 1
    for i in range(4):
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                        capture_dt=datetime(2011, 3, 1 + i),
                        embedding=_unit([1, 0.02 * i, 0, 0]))
        file_id += 1
    intruder_file = file_id
    _seed_detection(conn, face_ops, run_id=scan_run, file_id=intruder_file,
                    capture_dt=datetime(2011, 3, 20),
                    embedding=_unit([0, 0, 1, 0]))
    cluster_run = _start_run(conn)
    conn.commit()

    def one_blob(embeddings):
        return np.zeros(len(embeddings), dtype=int), None

    stats = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=one_blob, min_member_sim=0.45,
    ).run(run_id=cluster_run)

    assert stats["clusters_proposed"] == 1
    assert stats["members_trimmed_incoherent"] == 1
    assert stats["clusters_dropped_incoherent"] == 0
    intruder_membership = conn.execute(
        """
        SELECT COUNT(*) FROM face_cluster_members m
        JOIN face_detections d ON d.id = m.detection_id
        WHERE d.file_id = ?
        """,
        (intruder_file,),
    ).fetchone()[0]
    assert intruder_membership == 0


def test_cluster_review_sample_shows_medoid_and_boundary(db):
    """The review sample leads with the most central member and surfaces the
    planted outliers as mutually-dissimilar edge picks."""
    db_path, conn, face_ops = db
    run_id = _start_run(conn)

    # Six tight members along one axis + two distinct outliers.
    embeddings = {
        **{i: _unit([1.0, 0.02 * i, 0, 0]) for i in range(6)},
        6: _unit([0.3, 1.0, 0, 0]),   # outlier A
        7: _unit([0.3, 0, 1.0, 0]),   # outlier B (dissimilar to A too)
    }
    det_by_file = {}
    for file_id, emb in embeddings.items():
        det_by_file[file_id] = _seed_detection(
            conn, face_ops, run_id=run_id, file_id=file_id + 1,
            capture_dt=datetime(2011, 1, file_id + 1), embedding=emb,
        )
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det_by_file[file_id],
            cluster_key="mixed", model_name=config.MODEL_NAME,
            model_version=config.MODEL_VERSION_TAG,
        )
    conn.commit()

    cluster_id = conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = 'mixed'"
    ).fetchone()[0]
    sample = face_ops.get_cluster_review_sample(cluster_id, limit=4)

    assert len(sample) == 4
    assert sample[0]["role"] == "core"
    assert all(entry["role"] in ("suspect", "edge") for entry in sample[1:])
    # The core member is one of the tight-axis six, with high centroid sim.
    core_det = sample[0]["detection_id"]
    assert core_det in {det_by_file[i] for i in range(6)}
    assert sample[0]["similarity"] > 0.8
    # Both planted outliers must appear among the non-core picks -- the
    # lowest-similarity one is guaranteed a 'suspect' slot rather than
    # risking suppression by farthest-point sampling once the other
    # outlier is picked.
    non_core_dets = {entry["detection_id"] for entry in sample[1:]}
    assert det_by_file[6] in non_core_dets
    assert det_by_file[7] in non_core_dets


def test_cluster_review_sample_surfaces_multiple_near_duplicate_intruders(db):
    """Real-catalog case (cluster 581896, 263 members): a second person
    (Hannah) grafted into Benny's cluster via 5 near-duplicate faces, of
    which the compact preview showed only 1 -- the other 4 only surfaced
    on "Review all faces". Root cause: farthest-point sampling picks one
    intruder, then treats every other intruder as "already covered" by it
    (they're mutually similar), so it keeps filling remaining slots with
    diverse-but-legitimate majority members instead of circling back for
    the rest of the intruder group. This only bites with a large, varied
    majority pool (plenty of "still uncovered" legitimate alternatives to
    pick before the algorithm is forced back to the intruders) -- a small
    or axis-aligned majority doesn't reproduce it, which is why this uses
    a wide random scatter around one dominant axis, matching a real
    person's spread across many photos.

    Reserved 'suspect' slots (lowest raw similarity to centroid, immune to
    the already-picked bias) must guarantee more than one intruder shows.
    """
    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    rng = np.random.default_rng(0)

    majority = {
        i: _unit([1.0, *(0.3 * rng.standard_normal(3))])
        for i in range(50)
    }
    intruders = {50 + i: _unit([0, 1.0, 0.02 * i, 0]) for i in range(5)}
    embeddings = {**majority, **intruders}

    det_by_file = {}
    for file_id, emb in embeddings.items():
        det_by_file[file_id] = _seed_detection(
            conn, face_ops, run_id=run_id, file_id=file_id + 1,
            capture_dt=datetime(2011, 1, 1), embedding=emb,
        )
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det_by_file[file_id],
            cluster_key="grafted", model_name=config.MODEL_NAME,
            model_version=config.MODEL_VERSION_TAG,
        )
    conn.commit()

    cluster_id = conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = 'grafted'"
    ).fetchone()[0]
    sample = face_ops.get_cluster_review_sample(cluster_id, limit=10)

    intruder_dets = {det_by_file[i] for i in intruders}
    shown_intruders = {
        entry["detection_id"] for entry in sample
        if entry["detection_id"] in intruder_dets
    }
    assert len(shown_intruders) >= 3, (
        "at least 3 of the 5 near-duplicate intruders must be sampled -- "
        f"only {shown_intruders} appeared, meaning the sample would let "
        "an intruder group hide behind a single token appearance"
    )
    suspect_dets = {entry["detection_id"] for entry in sample
                    if entry["role"] == "suspect"}
    assert suspect_dets & intruder_dets, (
        "the guaranteed 'suspect' slots must be the ones catching "
        "the intruders, not farthest-point diversity alone"
    )
    # Edges report a visibly lower similarity than the core.
    assert min(e["similarity"] for e in sample[1:]) < 0.6


def test_cli_cluster_records_command_run(tmp_path):
    pytest.importorskip("sklearn")
    import json

    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.commit()
    conn.close()

    code = main(["--db", str(db_path), "cluster", "--era-size", "2.0"])
    assert code == 0

    conn = sqlite3.connect(str(db_path))
    try:
        tool, command, exit_status, params_json, stats_json = conn.execute(
            "SELECT tool, command, exit_status, params_json, stats_json FROM command_runs"
        ).fetchone()
    finally:
        conn.close()
    assert (tool, command, exit_status) == ("photo-faces", "cluster", "success")
    assert json.loads(params_json)["era_size"] == 2.0
    assert json.loads(stats_json)["detections_total"] == 0


def test_cohesion_gate_splits_chained_label(db):
    """Two tight identities forced into one HDBSCAN label must come apart:
    no mutual-kNN edge above the floor crosses the groups."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    file_id = 1
    for axis in (0, 2):
        for i in range(4):
            base = [0.0, 0.0, 0.0, 0.0]
            base[axis] = 1.0
            base[axis + 1] = 0.02 * i
            _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                            capture_dt=datetime(2011, 3, file_id),
                            embedding=_unit(base))
            file_id += 1
    cluster_run = _start_run(conn)
    conn.commit()

    def one_blob(embeddings):
        return np.zeros(len(embeddings), dtype=int), None

    stats = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=one_blob, min_member_sim=0,
    ).run(run_id=cluster_run)

    assert stats["clusters_proposed"] == 2
    assert stats["clusters_split_incohesive"] == 1
    assert stats["memberships_proposed"] == 8
    keys = {row[0] for row in conn.execute(
        "SELECT cluster_key FROM face_clusters WHERE status = 'proposed'")}
    assert any(key.endswith("-1") for key in keys), "split takes a suffix key"


def test_articulation_bridge_flagged_and_queued(db):
    """A face bridging two subgroups keeps its cluster connected but gets
    penalized confidence and lands in the uncertain-members queue."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    file_id = 1
    for i in range(3):  # group A on axis 0
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                        capture_dt=datetime(2011, 3, file_id),
                        embedding=_unit([1, 0.02 * i, 0, 0]))
        file_id += 1
    for i in range(3):  # group B on axis 2
        _seed_detection(conn, face_ops, run_id=scan_run, file_id=file_id,
                        capture_dt=datetime(2011, 3, file_id),
                        embedding=_unit([0, 0, 1, 0.02 * i]))
        file_id += 1
    bridge_file = file_id
    bridge_det = _seed_detection(
        conn, face_ops, run_id=scan_run, file_id=bridge_file,
        capture_dt=datetime(2011, 3, file_id),
        embedding=_unit([0.74, 0, 0.67, 0]))
    cluster_run = _start_run(conn)
    conn.commit()

    def one_blob(embeddings):
        return np.zeros(len(embeddings), dtype=int), None

    stats = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=one_blob, min_member_sim=0,
    ).run(run_id=cluster_run)

    # The bridge holds the blob together as ONE cluster — but it is flagged.
    assert stats["clusters_proposed"] == 1
    assert stats["memberships_proposed"] == 7
    assert stats["members_flagged_articulation"] >= 1

    confidence = conn.execute(
        "SELECT confidence FROM face_cluster_members WHERE detection_id = ?",
        (bridge_det,)).fetchone()[0]
    assert confidence is not None and confidence < 0.65, (
        "bridge confidence must carry the articulation penalty")

    queue = face_ops.get_uncertain_members(max_confidence=0.65)
    assert [m["detection_id"] for m in queue] == [bridge_det]


def test_uncertain_queue_lists_each_detection_once(db):
    """Overlapping era windows put one detection in several clusters; the
    uncertain queue must list the face once (its most-suspect membership) —
    duplicate rows collide in the review form's widget keys."""
    db_path, conn, face_ops = db
    run_id = _start_run(conn)
    det = _seed_detection(conn, face_ops, run_id=run_id, file_id=1,
                          capture_dt=datetime(2011, 3, 1),
                          embedding=_unit([1, 0, 0, 0]))
    for key, confidence in (("win-a", 0.30), ("win-b", 0.40)):
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det, cluster_key=key,
            model_name=config.MODEL_NAME,
            model_version=config.MODEL_VERSION_TAG,
            confidence=confidence,
        )
    conn.commit()

    queue = face_ops.get_uncertain_members(max_confidence=0.55)
    assert len(queue) == 1
    assert queue[0]["detection_id"] == det
    assert queue[0]["confidence"] == 0.30, "most-suspect membership wins"


def test_label_conflict_trims_minority_person(db):
    """Faces the user labeled to two different named people can never share
    a cluster: the minority person's faces return to noise."""
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)
    dets = []
    for file_id in range(1, 7):
        dets.append(_seed_detection(
            conn, face_ops, run_id=scan_run, file_id=file_id,
            capture_dt=datetime(2011, 3, file_id),
            embedding=_unit([1, 0.01 * file_id, 0, 0])))
    ann = face_ops.create_person(run_id=scan_run, display_name="Ann")
    ben = face_ops.create_person(run_id=scan_run, display_name="Ben")
    for det in dets[:3]:
        face_ops.link_detection_to_person(run_id=scan_run, detection_id=det,
                                          person_id=ann, confidence=None,
                                          link_method="seed")
    for det in dets[3:5]:
        face_ops.link_detection_to_person(run_id=scan_run, detection_id=det,
                                          person_id=ben, confidence=None,
                                          link_method="seed")
    cluster_run = _start_run(conn)
    conn.commit()

    def one_blob(embeddings):
        return np.zeros(len(embeddings), dtype=int), None

    stats = FaceClusterPipeline(
        db_path, min_cluster_size=3, cluster_fn=one_blob, min_member_sim=0,
    ).run(run_id=cluster_run)

    assert stats["members_trimmed_label_conflict"] == 2
    assert stats["clusters_proposed"] == 1
    members = {int(row[0]) for row in conn.execute(
        """
        SELECT m.detection_id FROM face_cluster_members m
        JOIN face_clusters c ON c.id = m.cluster_id
        WHERE c.status = 'proposed' AND m.status = 'proposed'
        """)}
    assert members == set(dets[:3] + dets[5:]), (
        "Ann's faces and the unlabeled face stay; Ben's faces are trimmed")


# ---------------------------------------------------------------------------
# Real sklearn backend (skipped when the faces extra is not installed)
# ---------------------------------------------------------------------------

def test_cluster_pipeline_with_real_hdbscan(db):
    pytest.importorskip("sklearn")
    db_path, conn, face_ops = db
    scan_run = _start_run(conn)

    rng = np.random.default_rng(42)
    file_id = 1
    for center in ([1, 0, 0, 0], [0, 1, 0, 0]):
        for _ in range(6):
            noisy = np.asarray(center, dtype=np.float32) + rng.normal(0, 0.02, 4)
            _seed_detection(
                conn, face_ops, run_id=scan_run, file_id=file_id,
                capture_dt=datetime(2011, 4, file_id % 27 + 1),
                embedding=_unit(noisy),
            )
            file_id += 1
    cluster_run = _start_run(conn)
    conn.commit()

    pipeline = FaceClusterPipeline(db_path, min_cluster_size=3)
    stats = pipeline.run(run_id=cluster_run)

    assert stats["clusters_proposed"] >= 2
    assert stats["memberships_proposed"] >= 12
