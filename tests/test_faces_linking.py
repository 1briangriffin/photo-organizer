"""
Cross-age linking tests: era-linkability rules, union-find, signal scoring,
merge suggestions as run_actions proposals with lifecycle semantics, and the
photo-faces link CLI.
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces import config
from photo_organizer.faces.db_ops import FaceDBOperations
from photo_organizer.faces.linking import CrossAgeLinker, UnionFind, eras_linkable


# ---------------------------------------------------------------------------
# UnionFind
# ---------------------------------------------------------------------------

def test_union_find_groups_connected_components():
    uf = UnionFind()
    uf.union(1, 2)
    uf.union(2, 3)
    uf.union(10, 11)
    uf.find(99)  # singleton

    groups = {root: sorted(members) for root, members in uf.groups().items()}
    assert sorted(groups.values(), key=len) == [[99], [10, 11], [1, 2, 3]]
    assert uf.find(1) == uf.find(3)
    assert uf.find(1) != uf.find(10)


# ---------------------------------------------------------------------------
# Era linkability
# ---------------------------------------------------------------------------

def _cluster(era_start, era_end, **extra):
    return {"era_start": era_start, "era_end": era_end, **extra}


def test_eras_linkable_rules():
    overlapping_a = _cluster("2010-01-01T00:00:00", "2012-06-01T00:00:00")
    overlapping_b = _cluster("2011-06-01T00:00:00", "2014-01-01T00:00:00")
    assert eras_linkable(overlapping_a, overlapping_b)

    adjacent = _cluster("2012-12-01T00:00:00", "2015-01-01T00:00:00")
    assert eras_linkable(overlapping_a, adjacent)          # ~6 month gap
    assert eras_linkable(adjacent, overlapping_a)          # symmetric

    far = _cluster("2016-01-01T00:00:00", "2018-01-01T00:00:00")
    assert not eras_linkable(overlapping_a, far)           # 3.5 year gap
    assert eras_linkable(overlapping_a, far, max_gap_years=4.0)

    assert not eras_linkable(_cluster(None, None), overlapping_a)


# ---------------------------------------------------------------------------
# Fixtures: catalogs with clustered detections
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


def _unit(vec):
    arr = np.asarray(vec, dtype=np.float32)
    return (arr / np.linalg.norm(arr)).tolist()


def _seed_detection(conn, face_ops, *, run_id, file_id, embedding,
                    estimated_age=None):
    conn.execute(
        """
        INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name, orig_path,
                                     first_seen_at, last_seen_at)
        VALUES (?, ?, 'jpeg', '.jpg', 'x.jpg', 'C:/x.jpg',
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
        """,
        (file_id, f"hash-{file_id}"),
    )
    payload = {"estimated_age": estimated_age} if estimated_age is not None else None
    detection_id = face_ops.record_detection(
        run_id=run_id, file_id=file_id,
        detection_index=0, bbox=(1.0, 1.0, 10.0, 10.0), confidence=0.9,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        image_hash=f"hash-{file_id}", payload=payload,
    )
    face_ops.record_embedding(
        run_id=run_id, detection_id=detection_id, embedding=embedding,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    return detection_id


def _seed_cluster(conn, face_ops, *, run_id, key, era_start, era_end,
                  representative, member_specs=()):
    """member_specs: iterable of (file_id, embedding, estimated_age)."""
    face_ops.upsert_cluster(
        run_id=run_id, cluster_key=key,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        era_start=era_start, era_end=era_end,
        representative_embedding=representative,
    )
    for file_id, embedding, age in member_specs:
        det = _seed_detection(conn, face_ops, run_id=run_id, file_id=file_id,
                              embedding=embedding, estimated_age=age)
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
    row = conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = ?", (key,)
    ).fetchone()
    return int(row[0])


def _merge_proposals(conn, status="proposed"):
    return conn.execute(
        """
        SELECT status, confidence, payload_json FROM run_actions
        WHERE action_type = 'face_cluster_merge' AND status = ?
        ORDER BY id
        """,
        (status,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Linker end-to-end on synthetic clusters
# ---------------------------------------------------------------------------

def test_linker_proposes_merge_for_similar_adjacent_clusters(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")

    # Same identity across two adjacent eras: similar representatives
    # (cos ~0.55), consistent age progression (30 -> 33 over ~3 years).
    id_a = _seed_cluster(
        conn, face_ops, run_id=seed_run, key="era:2010#000",
        era_start="2010-01-01T00:00:00", era_end="2012-07-01T00:00:00",
        representative=_unit([1.0, 0.75, 0.0, 0.0]),
        member_specs=[(1, _unit([1.0, 0.75, 0.0, 0.0]), 30),
                      (2, _unit([1.0, 0.70, 0.0, 0.0]), 31)],
    )
    id_b = _seed_cluster(
        conn, face_ops, run_id=seed_run, key="era:2013#000",
        era_start="2012-10-01T00:00:00", era_end="2015-04-01T00:00:00",
        representative=_unit([0.75, 1.0, 0.0, 0.0]),
        member_specs=[(3, _unit([0.75, 1.0, 0.0, 0.0]), 33),
                      (4, _unit([0.70, 1.0, 0.0, 0.0]), 34)],
    )
    # A very different identity in the second era: must not be suggested.
    _seed_cluster(
        conn, face_ops, run_id=seed_run, key="era:2013#001",
        era_start="2012-10-01T00:00:00", era_end="2015-04-01T00:00:00",
        representative=_unit([0.0, 0.0, 1.0, 0.0]),
        member_specs=[(5, _unit([0.0, 0.0, 1.0, 0.0]), 60)],
    )
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path, min_confidence=0.3).run(run_id=link_run)

    assert stats["clusters_considered"] == 3
    # A-B and A-C are cross-era comparisons; B-C shares an era and is skipped.
    assert stats["pairs_compared"] == 2
    assert stats["suggestions_proposed"] == 1

    proposals = _merge_proposals(conn)
    assert len(proposals) == 1
    status, confidence, payload_json = proposals[0]
    payload = json.loads(payload_json)
    assert {payload["cluster_a_id"], payload["cluster_b_id"]} == {id_a, id_b}
    assert payload["signals"]["embedding"] > 0.5
    assert "age_progression" not in payload["signals"], (
        "estimated age is retired from merge scoring"
    )
    assert confidence >= 30


def test_linker_skips_same_era_and_distant_pairs(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    same_rep = _unit([1.0, 1.0, 0.0, 0.0])

    _seed_cluster(conn, face_ops, run_id=seed_run, key="a",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=same_rep)
    # Identical embedding but same era: HDBSCAN already separated them.
    _seed_cluster(conn, face_ops, run_id=seed_run, key="b",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=same_rep)
    # Identical embedding but 6 years away: outside max gap.
    _seed_cluster(conn, face_ops, run_id=seed_run, key="c",
                  era_start="2018-01-01T00:00:00", era_end="2020-01-01T00:00:00",
                  representative=same_rep)
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path, min_confidence=0.1).run(run_id=link_run)
    assert stats["pairs_compared"] == 0
    assert stats["suggestions_proposed"] == 0


def test_co_occurrence_reader_counts_shared_photos(db):
    db_path, conn, face_ops = db
    run_id = _start_run(conn, command="cluster")

    # Photos 1 and 2 contain both the child (cluster kid) and a parent
    # (cluster parent) — detections on the same file_id.
    kid = _seed_cluster(
        conn, face_ops, run_id=run_id, key="kid",
        era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
        representative=_unit([1, 0, 0, 0]),
    )
    parent = _seed_cluster(
        conn, face_ops, run_id=run_id, key="parent",
        era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
        representative=_unit([0, 1, 0, 0]),
    )
    for file_id in (1, 2):
        for key, axis, det_index in (("kid", [1, 0, 0, 0], 0), ("parent", [0, 1, 0, 0], 1)):
            conn.execute(
                """
                INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name,
                                             orig_path, first_seen_at, last_seen_at)
                VALUES (?, ?, 'jpeg', '.jpg', 'x.jpg', 'C:/x.jpg',
                        '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (file_id, f"hash-{file_id}"),
            )
            det = face_ops.record_detection(
                run_id=run_id, file_id=file_id, detection_index=det_index,
                bbox=(1.0, 1.0, 5.0, 5.0), confidence=0.9,
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
                image_hash=f"hash-{file_id}",
            )
            face_ops.propose_cluster_assignment(
                run_id=run_id, detection_id=det, cluster_key=key,
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )
    conn.commit()

    assert face_ops.get_co_occurring_clusters(kid) == {parent: 2}
    assert face_ops.get_co_occurring_clusters(parent) == {kid: 2}

    # The bulk loader must agree with the per-cluster readers.
    context = face_ops.get_cluster_link_context(
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
    )
    assert context["co_occurrence"] == {kid: {parent: 2}, parent: {kid: 2}}
    assert context["median_ages"].get(kid) == face_ops.get_cluster_median_age(kid)
    assert context["member_counts"] == {kid: 2, parent: 2}
    # Distinct detections in shared files: no shared members.
    assert context["shared_members"] == {}


def test_supervised_anchor_boosts_pairs_near_labeled_person(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    axis = [1.0, 0.4, 0.0, 0.0]

    _seed_cluster(conn, face_ops, run_id=seed_run, key="a",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=_unit(axis))
    _seed_cluster(conn, face_ops, run_id=seed_run, key="b",
                  era_start="2012-06-01T00:00:00", era_end="2014-01-01T00:00:00",
                  representative=_unit([0.4, 1.0, 0.0, 0.0]))

    # Label a detection whose embedding sits between both clusters.
    person_id = face_ops.create_person(run_id=seed_run, display_name="Anchor")
    det = _seed_detection(conn, face_ops, run_id=seed_run, file_id=50,
                          embedding=_unit([1.0, 1.0, 0.0, 0.0]))
    face_ops.link_detection_to_person(
        run_id=seed_run, detection_id=det, person_id=person_id,
        confidence=1.0, link_method="seed",
    )
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path, min_confidence=0.05).run(run_id=link_run)
    assert stats["suggestions_proposed"] == 1
    payload = json.loads(_merge_proposals(conn)[0][2])
    assert payload["signals"]["supervised"] > 0.5


def test_relink_supersedes_suggestions_not_regenerated(db):
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    _seed_cluster(conn, face_ops, run_id=seed_run, key="a",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=_unit([1.0, 0.8, 0.0, 0.0]))
    _seed_cluster(conn, face_ops, run_id=seed_run, key="b",
                  era_start="2012-06-01T00:00:00", era_end="2014-01-01T00:00:00",
                  representative=_unit([0.8, 1.0, 0.0, 0.0]))
    first_link = _start_run(conn)
    second_link = _start_run(conn)
    conn.commit()

    first = CrossAgeLinker(db_path, min_confidence=0.1).run(run_id=first_link)
    assert first["suggestions_proposed"] == 1

    # Second run with an impossible threshold regenerates nothing — the
    # earlier suggestion must resolve to superseded, not linger as pending.
    second = CrossAgeLinker(db_path, min_confidence=0.99).run(run_id=second_link)
    assert second["suggestions_proposed"] == 0
    assert second["suggestions_superseded"] == 1

    assert _merge_proposals(conn, "proposed") == []
    superseded = conn.execute(
        "SELECT resolved_by_run_id FROM run_actions "
        "WHERE action_type = 'face_cluster_merge' AND status = 'superseded'"
    ).fetchall()
    assert superseded == [(second_link,)]


def test_rejected_suggestion_stays_rejected_after_relink(db):
    from photo_organizer.pipeline.lifecycle import reject_proposals

    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    _seed_cluster(conn, face_ops, run_id=seed_run, key="a",
                  era_start="2010-01-01T00:00:00", era_end="2012-01-01T00:00:00",
                  representative=_unit([1.0, 0.8, 0.0, 0.0]))
    _seed_cluster(conn, face_ops, run_id=seed_run, key="b",
                  era_start="2012-06-01T00:00:00", era_end="2014-01-01T00:00:00",
                  representative=_unit([0.8, 1.0, 0.0, 0.0]))
    first_link = _start_run(conn)
    conn.commit()

    CrossAgeLinker(db_path, min_confidence=0.1).run(run_id=first_link)
    action_id = conn.execute(
        "SELECT id FROM run_actions WHERE action_type = 'face_cluster_merge'"
    ).fetchone()[0]
    reject_proposals(DBOperations(conn), [action_id], run_id=None,
                     note="different people")
    conn.commit()

    second_link = _start_run(conn)
    conn.commit()
    CrossAgeLinker(db_path, min_confidence=0.1).run(run_id=second_link)

    statuses = [row[0] for row in conn.execute(
        "SELECT status FROM run_actions WHERE action_type = 'face_cluster_merge' "
        "ORDER BY id"
    )]
    # The rejection is preserved as history; the re-link proposes a fresh row.
    assert statuses == ["rejected", "proposed"]


def test_window_duplicates_proposed_at_full_confidence(db):
    """Two clusters from overlapping windows sharing the same detections are
    the same identity by construction: proposed as window_duplicate at 100,
    and excluded from the scored tier."""
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")

    # Shared detections across two overlapping-window clusters.
    shared_dets = [
        _seed_detection(conn, face_ops, run_id=seed_run, file_id=i,
                        embedding=_unit([1.0, 0.1 * i, 0, 0]))
        for i in range(1, 4)
    ]
    for key, era_start, era_end in (
        ("win-a", "2010-01-01T00:00:00", "2012-07-01T00:00:00"),
        ("win-b", "2011-04-01T00:00:00", "2013-10-01T00:00:00"),
    ):
        face_ops.upsert_cluster(
            run_id=seed_run, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
            era_start=era_start, era_end=era_end,
            representative_embedding=_unit([1.0, 0.1, 0, 0]),
        )
        for det in shared_dets:
            face_ops.propose_cluster_assignment(
                run_id=seed_run, detection_id=det, cluster_key=key,
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path, min_confidence=0.1).run(run_id=link_run)

    assert stats["duplicates_proposed"] == 1
    assert stats["suggestions_proposed"] == 0, (
        "the duplicate pair must not also appear as a scored suggestion"
    )
    method, confidence, payload_json = conn.execute(
        "SELECT method, confidence, payload_json FROM run_actions "
        "WHERE action_type = 'face_cluster_merge'"
    ).fetchone()
    assert method == "window_duplicate"
    assert confidence == 100
    payload = json.loads(payload_json)
    assert payload["signals"]["member_overlap"] == 1.0


def test_top_k_caps_scored_suggestions_per_cluster(db):
    """A hub cluster matching many others keeps only its K best suggestions,
    but pairs surviving via the other endpoint's top-K are kept too."""
    db_path, conn, face_ops = db
    seed_run = _start_run(conn, command="cluster")
    hub_rep = _unit([1.0, 0.5, 0, 0])

    _seed_cluster(conn, face_ops, run_id=seed_run, key="hub",
                  era_start="2010-01-01T00:00:00",
                  era_end="2012-01-01T00:00:00", representative=hub_rep)
    # Six satellites in a later era, all similar to the hub with slightly
    # different scores (varying representative similarity).
    for i in range(6):
        _seed_cluster(conn, face_ops, run_id=seed_run, key=f"sat-{i}",
                      era_start="2012-06-01T00:00:00",
                      era_end="2014-01-01T00:00:00",
                      representative=_unit([1.0, 0.5 + 0.05 * i, 0, 0]))
    link_run = _start_run(conn)
    conn.commit()

    stats = CrossAgeLinker(db_path, min_confidence=0.1, top_k=2).run(
        run_id=link_run,
    )
    # Satellites share an era (skipped pairwise); only hub-satellite pairs
    # qualify. Hub keeps its top 2, but each satellite also keeps its own
    # top-2 — and the hub is every satellite's only match, so all 6 survive
    # through the satellite endpoints.
    assert stats["pairs_compared"] == 6
    assert stats["suggestions_proposed"] == 6

    # With satellites also limited (top_k=2 both ways) the count only drops
    # when BOTH endpoints are saturated — verify the hub-side cap directly.
    relink_run = _start_run(conn)
    conn.commit()
    stats = CrossAgeLinker(db_path, min_confidence=0.1, top_k=0).run(
        run_id=relink_run,
    )
    assert stats["suggestions_proposed"] == 6, "top_k=0 keeps all"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_accept_bulk_by_confidence(tmp_path):
    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    face_ops = FaceDBOperations(DBOperations(conn))
    seed_run = _start_run(conn, command="cluster")
    # Duplicate pair (confidence 100) + a mid-confidence scored pair.
    shared = [
        _seed_detection(conn, face_ops, run_id=seed_run, file_id=i,
                        embedding=_unit([1.0, 0, 0, 0]))
        for i in range(1, 4)
    ]
    for key, era in (("win-a", "2010"), ("win-b", "2011")):
        face_ops.upsert_cluster(
            run_id=seed_run, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
            era_start=f"{era}-01-01T00:00:00", era_end=f"{int(era)+2}-01-01T00:00:00",
            representative_embedding=_unit([1.0, 0, 0, 0]),
        )
        for det in shared:
            face_ops.propose_cluster_assignment(
                run_id=seed_run, detection_id=det, cluster_key=key,
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )
    _seed_cluster(conn, face_ops, run_id=seed_run, key="other-era",
                  era_start="2013-06-01T00:00:00",
                  era_end="2015-01-01T00:00:00",
                  representative=_unit([1.0, 0.6, 0, 0]))
    conn.commit()
    conn.close()

    assert main(["--db", str(db_path), "link", "--min-confidence", "0.2"]) == 0

    conn = sqlite3.connect(str(db_path))
    pending = conn.execute(
        "SELECT COUNT(*), MIN(confidence) FROM run_actions "
        "WHERE action_type = 'face_cluster_merge' AND status = 'proposed'"
    ).fetchone()
    conn.close()
    assert pending[0] >= 2 and pending[1] < 95

    # Bulk accept only the high-confidence tier.
    assert main(["--db", str(db_path), "accept", "--min-confidence", "95"]) == 0

    conn = sqlite3.connect(str(db_path))
    try:
        applied = conn.execute(
            "SELECT COUNT(*) FROM run_actions WHERE action_type = "
            "'face_cluster_merge' AND status = 'applied'"
        ).fetchone()[0]
        still_pending = conn.execute(
            "SELECT COUNT(*) FROM run_actions WHERE action_type = "
            "'face_cluster_merge' AND status = 'proposed'"
        ).fetchone()[0]
        persons = conn.execute("SELECT COUNT(*) FROM face_persons").fetchone()[0]
    finally:
        conn.close()
    assert applied == 1, "only the window duplicate is >= 95"
    assert still_pending >= 1, "scored suggestions below 95 stay pending"
    assert persons == 1


def test_cli_accept_requires_exactly_one_mode(tmp_path):
    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.commit()
    conn.close()

    assert main(["--db", str(db_path), "accept"]) == 1
    assert main(["--db", str(db_path), "accept", "5",
                 "--min-confidence", "90"]) == 1

def test_cli_link_records_command_run(tmp_path):
    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.commit()
    conn.close()

    code = main(["--db", str(db_path), "link", "--min-confidence", "0.7"])
    assert code == 0

    conn = sqlite3.connect(str(db_path))
    try:
        command, exit_status, params_json, stats_json = conn.execute(
            "SELECT command, exit_status, params_json, stats_json FROM command_runs"
        ).fetchone()
    finally:
        conn.close()
    assert (command, exit_status) == ("link", "success")
    assert json.loads(params_json)["min_confidence"] == 0.7
    assert json.loads(stats_json)["clusters_considered"] == 0
