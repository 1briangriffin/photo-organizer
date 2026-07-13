"""
Streamlit-based review and labeling UI for face recognition.

Launch via: photo-faces --db catalog.db ui
Or directly: streamlit run photo_organizer/faces/streamlit_app.py -- --db catalog.db

Every mutation goes through the same primitives as the CLI and records its
own command_runs row (tool='photo-faces-ui'), so decisions made here are as
auditable as CLI ones. Proposals (merges, assignments) come from the standard
run_actions lifecycle.
"""
import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

import streamlit as st

from photo_organizer.database.ops import DBOperations
from photo_organizer.database.schema import init_schema
from photo_organizer.faces.db_ops import FaceDBOperations
from photo_organizer.pipeline.lifecycle import list_proposals, reject_proposals
from photo_organizer.run_log import RunRecorder

REVIEW_ACTION_TYPES = ("face_cluster_merge", "face_person_assign")


def get_db_path() -> Path:
    """Resolve the catalog path from PHOTO_FACES_DB or --db."""
    env = os.environ.get("PHOTO_FACES_DB")
    if env:
        return Path(env).resolve()
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    # Streamlit passes its own args, so we use parse_known_args
    args, _ = parser.parse_known_args()
    return args.db.resolve()


@st.cache_resource
def get_connection(db_path: str) -> sqlite3.Connection:
    """Cached connection. check_same_thread=False: Streamlit reruns scripts
    on worker threads but serializes them, so a single shared connection is
    safe here."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA foreign_keys=ON")
    init_schema(conn)
    return conn


def get_thumbnail_dir(db_path: Path) -> Path:
    return db_path.parent / ".face_thumbnails"


def _ui_run(db_path: Path, conn: sqlite3.Connection, command: str, apply_fn) -> dict:
    """Run a UI mutation inside its own audited command run.

    apply_fn(run_id) performs the mutation on `conn` and returns a stats
    dict; the run row is finalized with those stats.
    """
    recorder = RunRecorder(
        db_path,
        tool="photo-faces-ui",
        command=command,
        argv=["streamlit", command],
        db_path_for_row=db_path,
    )
    recorder.start()
    try:
        stats = apply_fn(recorder.row_id) or {}
        conn.commit()
    except Exception as exc:
        conn.rollback()
        recorder.finish_error(exc)
        raise
    recorder.finish_success(stats=stats, db_mutates=True)
    return stats


def _thumb_grid(container, thumb_dir: Path, thumbnails: list[dict],
                per_row: int = 5, width: int = 110):
    """Render a row-wrapped grid of face thumbnails."""
    shown = [t for t in thumbnails if t.get("thumbnail_path")]
    if not shown:
        container.caption("(no thumbnails)")
        return
    cols = container.columns(min(per_row, len(shown)))
    for i, thumb in enumerate(shown):
        full_path = thumb_dir / thumb["thumbnail_path"]
        caption = ""
        if thumb.get("capture_datetime"):
            caption = str(thumb["capture_datetime"])[:7]
        if thumb.get("estimated_age") is not None:
            caption = f"{caption} ~{int(thumb['estimated_age'])}y".strip()
        col = cols[i % len(cols)]
        if full_path.exists():
            col.image(str(full_path), caption=caption, width=width)
        else:
            col.caption(caption or f"face {thumb['detection_id']}")


# --- Pages ---

def page_stats(face_ops: FaceDBOperations):
    st.header("Face Recognition Stats")
    stats = face_ops.get_stats()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Faces Detected", stats["total_detections"])
    col2.metric("Photos with Faces", stats["photos_with_faces"])
    col3.metric("Live Clusters", stats["clusters_live"])
    col4.metric("Named Persons", stats["persons_named"])

    if stats["total_detections"]:
        pct = stats["detections_assigned"] / stats["total_detections"]
        st.progress(pct, text=f"Labeling progress: {pct:.0%} "
                    f"({stats['detections_assigned']}/{stats['total_detections']} "
                    f"faces assigned)")

    col5, col6, col7 = st.columns(3)
    col5.metric("Pending Merge Suggestions", stats["pending_merges"])
    col6.metric("Pending Auto-Assignments", stats["pending_assignments"])
    col7.metric("Unnamed Persons", stats["persons_unnamed"])


def page_cluster_review(db_path: Path, conn: sqlite3.Connection,
                        face_ops: FaceDBOperations, thumb_dir: Path):
    st.header("Cluster Review")
    clusters = face_ops.get_clusters_for_review(limit=50)
    if not clusters:
        st.success("All clusters are linked to a person!")
        return

    persons = face_ops.get_persons_summary()
    person_options = {p["display_name"]: p["id"]
                      for p in persons if p["display_name"]}

    st.info(f"{len(clusters)} unlinked cluster(s). Largest shown first. "
            "Assigning here is an accepted decision (audited).")

    for cluster in clusters:
        era = (cluster["era_start"] or "?")[:10]
        with st.expander(
            f"Cluster {cluster['id']} | era {era} | "
            f"{cluster['members']} face(s) | {cluster['status']}",
            expanded=False,
        ):
            _thumb_grid(st, thumb_dir,
                        face_ops.get_cluster_thumbnails(cluster["id"]))

            col_assign, col_new = st.columns(2)
            with col_assign:
                if person_options:
                    selected = st.selectbox(
                        "Assign to existing person",
                        options=["", *person_options],
                        key=f"assign_{cluster['id']}",
                    )
                    if selected and st.button("Assign",
                                              key=f"btn_assign_{cluster['id']}"):
                        def apply(run_id, _cid=cluster["id"],
                                  _pid=person_options[selected]):
                            face_ops.accept_cluster(run_id=run_id, cluster_id=_cid)
                            face_ops.link_cluster_to_person(
                                run_id=run_id, cluster_id=_cid, person_id=_pid,
                                link_method="manual_review",
                            )
                            return {"clusters_assigned": 1}
                        _ui_run(db_path, conn, "ui-assign-cluster", apply)
                        st.success(f"Assigned to {selected}")
                        st.rerun()
            with col_new:
                new_name = st.text_input("New person name",
                                         key=f"name_{cluster['id']}")
                new_bd = st.text_input("Birth date (YYYY-MM-DD, optional)",
                                       key=f"bd_{cluster['id']}")
                if new_name and st.button("Create & Assign",
                                          key=f"btn_new_{cluster['id']}"):
                    def apply(run_id, _cid=cluster["id"], _name=new_name,
                              _bd=new_bd):
                        person_id = face_ops.create_person(
                            run_id=run_id, display_name=_name,
                            birth_date=_bd or None,
                        )
                        face_ops.accept_cluster(run_id=run_id, cluster_id=_cid)
                        face_ops.link_cluster_to_person(
                            run_id=run_id, cluster_id=_cid, person_id=person_id,
                            link_method="manual_review",
                        )
                        return {"persons_created": 1, "clusters_assigned": 1}
                    _ui_run(db_path, conn, "ui-create-person", apply)
                    st.success(f"Created '{new_name}' and assigned cluster")
                    st.rerun()


def page_merge_review(db_path: Path, conn: sqlite3.Connection,
                      face_ops: FaceDBOperations, thumb_dir: Path):
    st.header("Suggestion Review")
    db_ops = DBOperations(conn)

    proposals = []
    for action_type in REVIEW_ACTION_TYPES:
        proposals.extend(list_proposals(db_ops, action_type=action_type, limit=20))
    proposals.sort(key=lambda p: -(p["confidence"] or 0))

    if not proposals:
        st.success("No pending suggestions — run `photo-faces link` or "
                   "`photo-faces refine` to generate more.")
        return

    persons = {p["id"]: p["display_name"] or f"person #{p['id']}"
               for p in face_ops.get_persons_summary()}
    st.info(f"{len(proposals)} pending suggestion(s), highest confidence first.")

    for proposal in proposals:
        payload = json.loads(proposal["payload_json"] or "{}")
        confidence = (proposal["confidence"] or 0) / 100
        is_merge = proposal["action_type"] == "face_cluster_merge"

        if is_merge:
            title = (f"#{proposal['id']} Merge clusters "
                     f"{payload.get('cluster_a_id')} + {payload.get('cluster_b_id')}")
            cluster_ids = [payload.get("cluster_a_id"), payload.get("cluster_b_id")]
        else:
            person_name = persons.get(payload.get("person_id"),
                                      f"person #{payload.get('person_id')}")
            title = (f"#{proposal['id']} Assign cluster "
                     f"{payload.get('cluster_id')} → {person_name}")
            cluster_ids = [payload.get("cluster_id")]

        st.subheader(f"{title} — confidence {confidence:.2f}")
        cols = st.columns(len(cluster_ids))
        for col, cluster_id in zip(cols, cluster_ids):
            with col:
                st.write(f"**Cluster {cluster_id}**")
                _thumb_grid(st, thumb_dir,
                            face_ops.get_cluster_thumbnails(int(cluster_id), limit=5),
                            width=90)

        if payload.get("signals"):
            with st.expander("Signal breakdown"):
                st.json(payload["signals"])

        bcol1, bcol2, _ = st.columns(3)
        if bcol1.button("Accept", key=f"accept_{proposal['id']}"):
            from photo_organizer.faces.linking import apply_accepted_proposals

            def apply(run_id, _aid=proposal["id"]):
                return apply_accepted_proposals(db_ops, [_aid], run_id=run_id)
            stats = _ui_run(db_path, conn, "ui-accept", apply)
            if stats.get("conflict_components"):
                st.warning("Conflict: would merge two named persons — skipped.")
            else:
                st.success("Accepted and applied.")
            st.rerun()
        if bcol2.button("Reject", key=f"reject_{proposal['id']}"):
            def apply(run_id, _aid=proposal["id"]):
                rejected, _ = reject_proposals(
                    db_ops, [_aid], run_id=run_id,
                    note="rejected in review UI",
                )
                return {"rejected": len(rejected)}
            _ui_run(db_path, conn, "ui-reject", apply)
            st.info("Rejected.")
            st.rerun()
        st.divider()


def page_timeline(face_ops: FaceDBOperations, thumb_dir: Path):
    st.header("Person Timeline")
    named = [p for p in face_ops.get_persons_summary() if p["display_name"]]
    if not named:
        st.warning("No named persons yet — label clusters or accept merges first.")
        return

    selected = st.selectbox("Select person",
                            options=[p["display_name"] for p in named])
    person = next(p for p in named if p["display_name"] == selected)
    if person["birth_date"]:
        st.write(f"Born: {person['birth_date']}")

    timeline = face_ops.get_person_detection_timeline(person["id"])
    if not timeline:
        st.info("No accepted face detections for this person yet.")
        return

    st.write(f"{len(timeline)} face(s) across the collection:")
    per_row = 8
    for start in range(0, len(timeline), per_row):
        _thumb_grid(st, thumb_dir, timeline[start:start + per_row],
                    per_row=per_row, width=90)


def page_query(face_ops: FaceDBOperations):
    st.header("Query Photos")
    named = [p for p in face_ops.get_persons_summary() if p["display_name"]]
    if not named:
        st.warning("No named persons yet.")
        return

    selected = st.selectbox("Person", options=[p["display_name"] for p in named])
    col1, col2 = st.columns(2)
    date_from = col1.text_input("From date (YYYY-MM-DD)", value="")
    date_to = col2.text_input("To date (YYYY-MM-DD)", value="")

    if st.button("Search"):
        person = next(p for p in named if p["display_name"] == selected)
        photos = face_ops.get_photos_for_person(
            person["id"],
            date_from=date_from or None,
            date_to=date_to or None,
        )
        st.write(f"Found {len(photos)} photo(s)")
        for photo in photos:
            date = (photo["capture_datetime"] or "unknown date")[:10]
            line = f"- {date} | `{photo['path']}`"
            if photo["raw_path"]:
                line += f" (RAW: `{photo['raw_path']}`)"
            st.write(line)


# --- Main App ---

def run_app():
    st.set_page_config(page_title="Photo Faces", layout="wide")
    st.title("Photo Faces — Review & Labeling")

    db_path = get_db_path()
    if not db_path.exists():
        st.error(f"Catalog not found: {db_path}")
        st.stop()
    conn = get_connection(str(db_path))
    face_ops = FaceDBOperations(DBOperations(conn))
    thumb_dir = get_thumbnail_dir(db_path)

    page = st.sidebar.radio("Navigation", [
        "Stats",
        "Cluster Review",
        "Suggestion Review",
        "Timeline",
        "Query",
    ])

    if page == "Stats":
        page_stats(face_ops)
    elif page == "Cluster Review":
        page_cluster_review(db_path, conn, face_ops, thumb_dir)
    elif page == "Suggestion Review":
        page_merge_review(db_path, conn, face_ops, thumb_dir)
    elif page == "Timeline":
        page_timeline(face_ops, thumb_dir)
    elif page == "Query":
        page_query(face_ops)


if __name__ == "__main__":
    run_app()
