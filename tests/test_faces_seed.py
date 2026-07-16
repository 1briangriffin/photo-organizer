"""
Identity workflow tests: YAML seed config parsing, seeding face_persons with
provenance, applying accepted merge suggestions (union-find into persons,
conflict guard), labeling, and the CLI wrappers.
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
from photo_organizer.faces.linking import CrossAgeLinker, apply_accepted_proposals
from photo_organizer.faces.seed import apply_seed, load_seed_config

pytest.importorskip("yaml")


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    try:
        yield db_path, conn, FaceDBOperations(DBOperations(conn))
    finally:
        conn.close()


def _start_run(conn, command="seed"):
    return conn.execute(
        """
        INSERT INTO command_runs (tool, command, started_at, exit_status,
                                  dry_run, argv_json)
        VALUES ('photo-faces', ?, '2026-01-01T00:00:00Z', 'running', 0, '[]')
        """,
        (command,),
    ).lastrowid


def _write_config(tmp_path, content):
    path = tmp_path / "faces_config.yaml"
    path.write_text(content, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Seed config parsing
# ---------------------------------------------------------------------------

def test_load_seed_config_parses_people(tmp_path):
    path = _write_config(tmp_path, """
known_people:
  - name: Sam
    birth_date: 2005-03-15
    notes: "oldest child"
  - name: Emma
""")
    people = load_seed_config(path)
    assert people == [
        {"name": "Sam", "birth_date": "2005-03-15", "notes": "oldest child"},
        {"name": "Emma", "birth_date": None, "notes": None},
    ]


@pytest.mark.parametrize("content,match", [
    ("just_a_key: 1", "known_people"),
    ("known_people: not-a-list", "must be a list"),
    ("known_people:\n  - notes: no name\n", "name"),
    ("known_people:\n  - name: Sam\n    birth_date: 15-03-2005\n", "Invalid birth_date"),
])
def test_load_seed_config_rejects_bad_structures(tmp_path, content, match):
    path = _write_config(tmp_path, content)
    with pytest.raises((ValueError, KeyError), match=match):
        load_seed_config(path)


def test_load_seed_config_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_seed_config(tmp_path / "nope.yaml")


# ---------------------------------------------------------------------------
# apply_seed
# ---------------------------------------------------------------------------

def test_apply_seed_creates_and_updates_with_provenance(db):
    db_path, conn, face_ops = db
    run_1 = _start_run(conn)
    ops = DBOperations(conn)

    stats = apply_seed(ops, [
        {"name": "Sam", "birth_date": "2005-03-15", "notes": "oldest"},
        {"name": "Emma", "birth_date": None, "notes": None},
    ], run_id=run_1)
    conn.commit()
    assert stats == {"created": 2, "updated": 0, "unchanged": 0}

    persons = conn.execute(
        "SELECT display_name, birth_date, created_by_run_id FROM face_persons "
        "ORDER BY id"
    ).fetchall()
    assert persons == [("Sam", "2005-03-15", run_1), ("Emma", None, run_1)]
    actions = conn.execute(
        "SELECT action_type, status, method FROM run_actions "
        "WHERE action_type = 'face_person_seed' ORDER BY id"
    ).fetchall()
    assert actions == [("face_person_seed", "applied", "seed_create")] * 2

    # Re-seed: case-insensitive match, birth date fill for Emma, no dupes.
    run_2 = _start_run(conn)
    stats_2 = apply_seed(ops, [
        {"name": "sam", "birth_date": "2005-03-15", "notes": None},
        {"name": "EMMA", "birth_date": "2010-08-22", "notes": None},
    ], run_id=run_2)
    conn.commit()
    assert stats_2 == {"created": 0, "updated": 1, "unchanged": 1}

    persons = conn.execute(
        "SELECT display_name, birth_date FROM face_persons ORDER BY id"
    ).fetchall()
    assert persons == [("Sam", "2005-03-15"), ("Emma", "2010-08-22")]


# ---------------------------------------------------------------------------
# Merge acceptance
# ---------------------------------------------------------------------------

def _unit(vec):
    arr = np.asarray(vec, dtype=np.float32)
    return (arr / np.linalg.norm(arr)).tolist()


def _seed_cluster(conn, face_ops, *, run_id, key, era_start, era_end,
                  representative, member_file_id=None):
    face_ops.upsert_cluster(
        run_id=run_id, cluster_key=key,
        model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        era_start=era_start, era_end=era_end,
        representative_embedding=representative,
    )
    cluster_id = int(conn.execute(
        "SELECT id FROM face_clusters WHERE cluster_key = ?", (key,)
    ).fetchone()[0])
    if member_file_id is not None:
        conn.execute(
            """
            INSERT OR IGNORE INTO files (id, hash, type, ext, orig_name,
                                         orig_path, first_seen_at, last_seen_at)
            VALUES (?, ?, 'jpeg', '.jpg', 'x.jpg', 'C:/x.jpg',
                    '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (member_file_id, f"hash-{member_file_id}"),
        )
        det = face_ops.record_detection(
            run_id=run_id, file_id=member_file_id, detection_index=0,
            bbox=(1.0, 1.0, 5.0, 5.0), confidence=0.9,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
            image_hash=f"hash-{member_file_id}",
        )
        face_ops.propose_cluster_assignment(
            run_id=run_id, detection_id=det, cluster_key=key,
            model_name=config.MODEL_NAME, model_version=config.MODEL_VERSION_TAG,
        )
    return cluster_id


def _link_three_eras(db_path, conn, face_ops):
    """Three same-identity clusters in consecutive eras -> two merge
    suggestions (a-b, b-c). Returns (cluster_ids, action_ids)."""
    seed_run = _start_run(conn, command="cluster")
    rep = [1.0, 0.8, 0.0, 0.0]
    ids = [
        _seed_cluster(conn, face_ops, run_id=seed_run, key=f"era{i}",
                      era_start=f"{2010 + 2 * i}-01-01T00:00:00",
                      era_end=f"{2012 + 2 * i}-01-01T00:00:00",
                      representative=_unit(rep), member_file_id=i + 1)
        for i in range(3)
    ]
    link_run = _start_run(conn, command="link")
    conn.commit()
    CrossAgeLinker(db_path, min_confidence=0.1, max_gap_years=1.0).run(run_id=link_run)
    action_ids = [row[0] for row in conn.execute(
        "SELECT id FROM run_actions WHERE action_type = 'face_cluster_merge' "
        "AND status = 'proposed' ORDER BY id"
    )]
    return ids, action_ids


def test_accepting_merges_creates_person_and_accepts_clusters(db):
    db_path, conn, face_ops = db
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)
    assert len(action_ids) == 2

    accept_run = _start_run(conn, command="accept")
    conn.commit()
    stats = apply_accepted_proposals(
        DBOperations(conn), [*action_ids, 99999], run_id=accept_run,
    )
    conn.commit()

    assert stats["merges_applied"] == 2
    assert stats["proposals_skipped"] == 1
    assert stats["persons_created"] == 1
    assert stats["clusters_accepted"] == 3
    assert stats["conflict_components"] == 0

    cluster_status = {row[0] for row in conn.execute(
        "SELECT status FROM face_clusters WHERE id IN (?,?,?)", cluster_ids
    )}
    assert cluster_status == {"accepted"}
    member_status = {row[0] for row in conn.execute(
        "SELECT status FROM face_cluster_members"
    )}
    assert member_status == {"accepted"}

    links = conn.execute(
        "SELECT person_id, cluster_id, status, link_method FROM face_person_links "
        "ORDER BY cluster_id"
    ).fetchall()
    person_ids = {row[0] for row in links}
    assert len(person_ids) == 1
    assert [row[1] for row in links] == sorted(cluster_ids)
    assert all(row[2] == "accepted" and row[3] == "merge_accept" for row in links)

    resolved = conn.execute(
        "SELECT status, applied_by_run_id FROM run_actions WHERE id IN (?, ?)",
        action_ids,
    ).fetchall()
    assert resolved == [("applied", accept_run)] * 2


def test_accepting_merge_reuses_existing_person(db):
    db_path, conn, face_ops = db
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)

    # The first cluster already belongs to a named person.
    label_run = _start_run(conn, command="seed")
    person_id = face_ops.create_person(
        run_id=label_run, display_name="Sam", birth_date="2005-03-15",
    )
    face_ops.link_cluster_to_person(
        run_id=label_run, cluster_id=cluster_ids[0], person_id=person_id,
        link_method="manual",
    )
    accept_run = _start_run(conn, command="accept")
    conn.commit()

    stats = apply_accepted_proposals(DBOperations(conn), action_ids, run_id=accept_run)
    conn.commit()

    assert stats["persons_created"] == 0
    assert stats["persons_reused"] == 1
    linked_persons = {row[0] for row in conn.execute(
        "SELECT person_id FROM face_person_links WHERE status = 'accepted'"
    )}
    assert linked_persons == {person_id}


def test_conflicting_component_is_skipped(db):
    db_path, conn, face_ops = db
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)

    # Ends of the chain belong to two DIFFERENT named persons; accepting both
    # suggestions would silently merge them — must be refused.
    label_run = _start_run(conn, command="seed")
    sam = face_ops.create_person(run_id=label_run, display_name="Sam")
    emma = face_ops.create_person(run_id=label_run, display_name="Emma")
    face_ops.link_cluster_to_person(run_id=label_run, cluster_id=cluster_ids[0],
                                    person_id=sam, link_method="manual")
    face_ops.link_cluster_to_person(run_id=label_run, cluster_id=cluster_ids[2],
                                    person_id=emma, link_method="manual")
    accept_run = _start_run(conn, command="accept")
    conn.commit()

    stats = apply_accepted_proposals(DBOperations(conn), action_ids, run_id=accept_run)
    conn.commit()

    assert stats["conflict_components"] == 1
    assert stats["merges_applied"] == 0
    # Proposals stay pending for the user to reject/re-review.
    pending = conn.execute(
        "SELECT COUNT(*) FROM run_actions WHERE action_type = 'face_cluster_merge' "
        "AND status = 'proposed'"
    ).fetchone()[0]
    assert pending == 2
    # No new person links were created.
    links = conn.execute(
        "SELECT COUNT(*) FROM face_person_links"
    ).fetchone()[0]
    assert links == 2


def test_component_with_one_named_person_absorbs_anonymous_groups(db):
    """A merge chain touching one named person and one anonymous group is NOT
    a conflict: the anonymous person absorbs into the named one."""
    db_path, conn, face_ops = db
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)

    label_run = _start_run(conn, command="seed")
    sam = face_ops.create_person(run_id=label_run, display_name="Sam")
    face_ops.link_cluster_to_person(run_id=label_run, cluster_id=cluster_ids[0],
                                    person_id=sam, link_method="manual")
    anon = face_ops.create_person(run_id=label_run, display_name=None)
    face_ops.link_cluster_to_person(run_id=label_run, cluster_id=cluster_ids[2],
                                    person_id=anon, link_method="merge_accept")
    accept_run = _start_run(conn, command="accept")
    conn.commit()

    stats = apply_accepted_proposals(DBOperations(conn), action_ids,
                                     run_id=accept_run)
    conn.commit()

    assert stats["conflict_components"] == 0
    assert stats["persons_absorbed"] == 1
    assert stats["merges_applied"] == 2

    linked_persons = {row[0] for row in conn.execute(
        "SELECT person_id FROM face_person_links "
        "WHERE status = 'accepted' AND cluster_id IS NOT NULL"
    )}
    assert linked_persons == {sam}
    anon_status, payload = conn.execute(
        "SELECT status, payload_json FROM face_persons WHERE id = ?", (anon,)
    ).fetchone()
    assert anon_status == "merged"
    assert json.loads(payload)["merged_into"] == sam


def test_component_of_only_anonymous_groups_merges_freely(db):
    """Two anonymous person groups connected by an accepted merge collapse
    into one (lowest id wins), instead of conflicting."""
    db_path, conn, face_ops = db
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)

    setup_run = _start_run(conn, command="accept")
    anon_a = face_ops.create_person(run_id=setup_run, display_name=None)
    anon_b = face_ops.create_person(run_id=setup_run, display_name=None)
    face_ops.link_cluster_to_person(run_id=setup_run, cluster_id=cluster_ids[0],
                                    person_id=anon_a, link_method="merge_accept")
    face_ops.link_cluster_to_person(run_id=setup_run, cluster_id=cluster_ids[2],
                                    person_id=anon_b, link_method="merge_accept")
    accept_run = _start_run(conn, command="accept")
    conn.commit()

    stats = apply_accepted_proposals(DBOperations(conn), action_ids,
                                     run_id=accept_run)
    conn.commit()

    assert stats["conflict_components"] == 0
    assert stats["persons_absorbed"] == 1
    linked_persons = {row[0] for row in conn.execute(
        "SELECT person_id FROM face_person_links "
        "WHERE status = 'accepted' AND cluster_id IS NOT NULL"
    )}
    assert linked_persons == {anon_a}, "lowest person id wins"
    assert conn.execute(
        "SELECT status FROM face_persons WHERE id = ?", (anon_b,)
    ).fetchone()[0] == "merged"


# ---------------------------------------------------------------------------
# Unwinding an accept run
# ---------------------------------------------------------------------------

def test_unwind_reverts_everything_an_accept_run_created(db):
    from photo_organizer.faces.linking import unwind_accept_run

    db_path, conn, face_ops = db
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)
    accept_run = _start_run(conn, command="accept")
    conn.commit()
    apply_accepted_proposals(DBOperations(conn), action_ids, run_id=accept_run)
    conn.commit()

    unwind_run = _start_run(conn, command="unwind")
    stats = unwind_accept_run(DBOperations(conn), accept_run, run_id=unwind_run)
    conn.commit()

    assert stats["links_retracted"] == 3
    assert stats["links_kept_named"] == 0
    assert stats["clusters_reverted"] == 3
    assert stats["persons_retired"] == 1
    assert stats["actions_superseded"] >= 2  # the merges + person links

    cluster_status = {row[0] for row in conn.execute(
        "SELECT status FROM face_clusters")}
    assert cluster_status == {"proposed"}
    member_status = {row[0] for row in conn.execute(
        "SELECT status FROM face_cluster_members")}
    assert member_status == {"proposed"}
    person_status = conn.execute(
        "SELECT status FROM face_persons").fetchone()[0]
    assert person_status == "retired"
    link_status = {row[0] for row in conn.execute(
        "SELECT status FROM face_person_links")}
    assert link_status == {"retracted"}
    merge_rows = conn.execute(
        "SELECT status, resolution_note FROM run_actions "
        "WHERE action_type = 'face_cluster_merge'"
    ).fetchall()
    assert all(status == "superseded" for status, _ in merge_rows)
    assert all(f"#{accept_run}" in note for _, note in merge_rows)


def test_unwind_keeps_persons_named_after_acceptance(db):
    from photo_organizer.faces.linking import unwind_accept_run

    db_path, conn, face_ops = db
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)
    accept_run = _start_run(conn, command="accept")
    conn.commit()
    apply_accepted_proposals(DBOperations(conn), action_ids, run_id=accept_run)
    conn.commit()

    # The user names the person created by acceptance — that work must
    # survive an unwind.
    person_id = conn.execute("SELECT id FROM face_persons").fetchone()[0]
    label_run = _start_run(conn, command="label")
    face_ops.update_person(run_id=label_run, person_id=person_id,
                           display_name="Sam")
    conn.commit()

    unwind_run = _start_run(conn, command="unwind")
    stats = unwind_accept_run(DBOperations(conn), accept_run, run_id=unwind_run)
    conn.commit()

    assert stats["links_retracted"] == 0
    assert stats["links_kept_named"] == 3
    assert stats["clusters_reverted"] == 0
    assert stats["persons_retired"] == 0

    assert conn.execute(
        "SELECT status FROM face_persons WHERE id = ?", (person_id,)
    ).fetchone()[0] == "active"
    cluster_status = {row[0] for row in conn.execute(
        "SELECT status FROM face_clusters")}
    assert cluster_status == {"accepted"}


def test_unwind_unknown_run_fails(db):
    from photo_organizer.faces.linking import unwind_accept_run

    db_path, conn, face_ops = db
    with pytest.raises(SystemExit):
        unwind_accept_run(DBOperations(conn), 99999, run_id=None)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def test_cli_seed_accept_label_flow(tmp_path, capsys):
    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    face_ops = FaceDBOperations(DBOperations(conn))
    cluster_ids, action_ids = _link_three_eras(db_path, conn, face_ops)
    conn.commit()
    conn.close()

    config_path = _write_config(tmp_path, """
known_people:
  - name: Sam
    birth_date: 2005-03-15
""")
    assert main(["--db", str(db_path), "seed", "--config", str(config_path)]) == 0
    assert main(["--db", str(db_path), "accept",
                 *[str(i) for i in action_ids]]) == 0

    conn = sqlite3.connect(str(db_path))
    unnamed = conn.execute(
        "SELECT id FROM face_persons WHERE display_name IS NULL"
    ).fetchone()[0]
    conn.close()

    assert main(["--db", str(db_path), "label", str(unnamed), "Emma",
                 "--birth-date", "2010-08-22"]) == 0

    conn = sqlite3.connect(str(db_path))
    try:
        persons = conn.execute(
            "SELECT display_name, birth_date FROM face_persons ORDER BY id"
        ).fetchall()
        runs = conn.execute(
            "SELECT command, exit_status, db_mutates FROM command_runs "
            "WHERE tool = 'photo-faces' ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert ("Sam", "2005-03-15") in persons
    assert ("Emma", "2010-08-22") in persons
    for command in ("seed", "accept", "label"):
        assert (command, "success", 1) in runs


def test_cli_label_unknown_person_reports_no_mutation(tmp_path):
    from photo_organizer.faces.cli import main

    db_path = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(db_path))
    init_schema(conn)
    conn.commit()
    conn.close()

    assert main(["--db", str(db_path), "label", "42", "Nobody"]) == 0

    conn = sqlite3.connect(str(db_path))
    run = conn.execute(
        "SELECT exit_status, db_mutates, stats_json FROM command_runs"
    ).fetchone()
    conn.close()
    assert run[0] == "success"
    assert run[1] == 0
    assert json.loads(run[2])["persons_labeled"] == 0
