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
from photo_organizer.run_log import RunRecorder


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
    recorder.finish_success(
        stats=stats,
        db_mutates=any(bool(v) for v in stats.values()) if stats else True,
    )
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
        # Caption with the capture date — the context a human actually uses.
        # Estimated age is deliberately not shown (unreliable). Review
        # samples add their role and centroid similarity (core 0.94 / 0.41).
        caption = str(thumb["capture_datetime"])[:7] if thumb.get(
            "capture_datetime") else ""
        if thumb.get("similarity") is not None:
            tag = "core" if thumb.get("role") == "core" else "edge"
            caption = f"{caption} · {tag} {thumb['similarity']:.2f}".strip(" ·")
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
        named_pct = stats["detections_named"] / stats["total_detections"]
        st.progress(named_pct, text=f"Named: {named_pct:.0%} "
                    f"({stats['detections_named']}/{stats['total_detections']} "
                    f"faces belong to a named person)")
        grouped_pct = stats["detections_assigned"] / stats["total_detections"]
        st.caption(f"Grouped (incl. anonymous person groups): {grouped_pct:.0%} "
                   f"({stats['detections_assigned']})")

    col5, col6, col7 = st.columns(3)
    col5.metric(
        "Merge Suggestions Pending", stats["pending_merges"],
        help="Cluster-pair merges proposed by `photo-faces link`, awaiting "
             "accept/reject in Suggestion Review.",
    )
    col6.metric(
        "Refine Suggestions Pending", stats["pending_assignments"],
        help="Cluster→person assignments proposed by `photo-faces refine` "
             "from your labels. 0 until refine runs after a labeling session.",
    )
    col7.metric(
        "Anonymous Person Groups", stats["persons_unnamed"],
        help="Person records without a name (created when accepted merges "
             "had no named person) — the Name People page queue. People "
             "still latent in unaccepted clusters are not counted anywhere "
             "as persons yet.",
    )


VERDICT_KEEP = ""
VERDICT_NOT_PERSON = "Not this person"
VERDICT_DEPICTION = "Depiction (photo of a photo / screen / reflection)"
VERDICT_DOLL = "Doll / statue (not a person)"
VERDICT_NOT_FACE = "Not a face"
MEMBER_PAGE_SIZE = 48


def _cluster_member_review(db_path: Path, conn: sqlite3.Connection,
                           face_ops: FaceDBOperations, thumb_dir: Path,
                           cluster_id: int):
    """Full-membership cleanup for one cluster: every face, most suspect
    first, each with a verdict selector and an optional relabel. One Save
    applies the whole page in a single audited run; "Not this person"
    verdicts are evicted as ONE batch so co-evicted faces are never
    cannot-linked against each other."""
    members = face_ops.get_cluster_members_detail(cluster_id)
    if not members:
        st.caption("No live members.")
        return

    pages = (len(members) + MEMBER_PAGE_SIZE - 1) // MEMBER_PAGE_SIZE
    page = 1
    if pages > 1:
        page = int(st.number_input(
            f"Page (of {pages}, ~{MEMBER_PAGE_SIZE} faces each, most "
            f"suspect first)",
            min_value=1, max_value=pages, value=1,
            key=f"member_page_{cluster_id}",
        ))
    page_members = members[(page - 1) * MEMBER_PAGE_SIZE:
                           page * MEMBER_PAGE_SIZE]

    with st.form(key=f"member_form_{cluster_id}_{page}"):
        st.caption(
            "Sorted by similarity to the cluster core — intruders and "
            "framed photos surface first. 'Not this person' removes the "
            "face from this group durably (it can still cluster with its "
            "true person); add a name to label it in the same save."
        )
        columns = st.columns(4)
        for i, member in enumerate(page_members):
            det_id = member["detection_id"]
            with columns[i % 4]:
                thumb = member.get("thumbnail_path")
                full_path = thumb_dir / thumb if thumb else None
                caption = (f"{str(member['capture_datetime'])[:7]} · "
                           f"{member['similarity']:.2f}")
                if full_path is not None and full_path.exists():
                    st.image(str(full_path), caption=caption, width=110)
                else:
                    st.caption(f"face {det_id} · {caption}")
                st.selectbox(
                    "Verdict", options=[VERDICT_KEEP, VERDICT_NOT_PERSON,
                                        VERDICT_DEPICTION, VERDICT_DOLL,
                                        VERDICT_NOT_FACE],
                    key=f"verdict_{cluster_id}_{det_id}",
                    label_visibility="collapsed",
                )
                st.text_input(
                    "Actually is (name, optional)",
                    key=f"who_{cluster_id}_{det_id}",
                    label_visibility="collapsed",
                    placeholder="actually is…",
                )
        submitted = st.form_submit_button("Save verdicts", type="primary")

    if not submitted:
        return

    verdicts: dict[int, str] = {}
    who: dict[int, str] = {}
    for member in page_members:
        det_id = member["detection_id"]
        verdict = st.session_state.get(f"verdict_{cluster_id}_{det_id}", "")
        name = (st.session_state.get(f"who_{cluster_id}_{det_id}", "") or "").strip()
        if verdict:
            verdicts[det_id] = verdict
        if name:
            who[det_id] = name
    if not verdicts and not who:
        st.info("No verdicts filled in — nothing saved.")
        return

    def apply(run_id):
        result = {"evicted": 0, "depictions": 0, "not_persons": 0,
                  "not_faces": 0, "faces_labeled": 0, "persons_created": 0}
        for det_id, verdict in verdicts.items():
            if verdict == VERDICT_DEPICTION:
                face_ops.mark_detection_depiction(run_id=run_id,
                                                  detection_id=det_id)
                result["depictions"] += 1
            elif verdict == VERDICT_DOLL:
                face_ops.mark_detection_not_a_person(run_id=run_id,
                                                     detection_id=det_id)
                result["not_persons"] += 1
            elif verdict == VERDICT_NOT_FACE:
                face_ops.mark_detection_not_a_face(run_id=run_id,
                                                   detection_id=det_id)
                result["not_faces"] += 1
        evict_ids = [d for d, v in verdicts.items()
                     if v == VERDICT_NOT_PERSON]
        if evict_ids:
            evicted = face_ops.evict_cluster_members(
                run_id=run_id, cluster_id=cluster_id,
                detection_ids=evict_ids, note="evicted in cluster review",
            )
            result["evicted"] = evicted["evicted"]
        for det_id, name in who.items():
            if verdicts.get(det_id) in (VERDICT_DEPICTION, VERDICT_DOLL,
                                        VERDICT_NOT_FACE):
                continue  # a non-live face gets no person label
            existing = face_ops.find_person_by_name(name)
            if existing is not None:
                person_id = existing[0]
            else:
                person_id = face_ops.create_person(run_id=run_id,
                                                   display_name=name)
                result["persons_created"] += 1
            face_ops.link_detection_to_person(
                run_id=run_id, detection_id=det_id, person_id=person_id,
                confidence=None, link_method="manual_review",
            )
            result["faces_labeled"] += 1
        return result

    stats = _ui_run(db_path, conn, "ui-member-verdicts", apply)
    st.success(
        f"Saved: {stats['evicted']} evicted, {stats['depictions']} "
        f"depiction(s), {stats['not_persons']} doll/statue, "
        f"{stats['not_faces']} not-a-face, "
        f"{stats['faces_labeled']} labeled "
        f"({stats['persons_created']} new person(s))."
    )
    st.rerun()


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
                        face_ops.get_cluster_review_sample(cluster["id"],
                                                           limit=10))

            if st.toggle(f"Review all {cluster['members']} face(s) — "
                         f"per-face verdicts (evict / depiction / relabel)",
                         key=f"member_toggle_{cluster['id']}"):
                _cluster_member_review(db_path, conn, face_ops, thumb_dir,
                                       cluster["id"])

            if st.button("Not a face", key=f"btn_junk_{cluster['id']}",
                         help="Reject this cluster and mark its detections "
                              "as non-faces everywhere"):
                def apply(run_id, _cid=cluster["id"]):
                    marked = face_ops.mark_cluster_not_faces(
                        run_id=run_id, cluster_id=_cid,
                    )
                    return {"clusters_rejected": 1, "detections_marked": marked}
                stats = _ui_run(db_path, conn, "ui-not-a-face", apply)
                st.info(f"Rejected — {stats['detections_marked']} detection(s) "
                        f"marked as non-faces.")
                st.rerun()

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


def _same_photo_flag_triage(db_path: Path, conn: sqlite3.Connection,
                            face_ops: FaceDBOperations, file_id: int,
                            file_flags: list[dict]):
    """Triage one photo's same-photo flags with the FULL image in view:
    reflections need the whole scene to call, a wall of framed pictures
    means several depictions at once, and dolls get their own verdict.
    Verdicts apply per face in one form; saving resolves every pending
    flag on the photo."""
    from photo_organizer.faces import config as fconfig

    info = face_ops.get_photo_info(file_id)
    detections = face_ops.get_photo_detections(
        file_id, min_det_score=fconfig.MIN_WORKING_DET_SCORE,
    )
    if info is None or not detections:
        st.caption(f"File {file_id}: no longer available for review.")
        return

    flagged_ids = {flag["detection_a"] for flag in file_flags}
    flagged_ids |= {flag["detection_b"] for flag in file_flags}
    face_numbers = {d["detection_id"]: i + 1 for i, d in enumerate(detections)}
    flagged_label = ", ".join(
        str(face_numbers[det]) for det in sorted(flagged_ids)
        if det in face_numbers)

    with st.expander(
        f"{info['path']} — {len(file_flags)} flagged pair(s), "
        f"faces {flagged_label}",
        expanded=False,
    ):
        det_key = tuple((d["detection_id"], d["bbox"]) for d in detections)
        annotated, crops = _render_photo(info["path"], info["file_type"],
                                         det_key)
        if annotated is not None:
            st.image(annotated,
                     caption=f"{(info['capture_datetime'] or 'undated')[:10]}"
                             f" — flagged: faces {flagged_label}")
        else:
            st.warning(f"Could not load {info['path']} — judging from "
                       f"face crops only.")

        with st.form(key=f"flag_form_{file_id}"):
            columns = st.columns(min(4, len(detections)))
            for i, det in enumerate(detections):
                det_id = det["detection_id"]
                with columns[i % len(columns)]:
                    if annotated is not None and i < len(crops):
                        st.image(crops[i], caption=f"Face {i + 1}", width=110)
                    else:
                        st.caption(f"Face {i + 1}")
                    if det_id in flagged_ids:
                        st.caption("⚑ flagged")
                    st.selectbox(
                        "Verdict",
                        options=[VERDICT_KEEP, VERDICT_DEPICTION,
                                 VERDICT_DOLL, VERDICT_NOT_FACE],
                        key=f"flagv_{file_id}_{det_id}",
                        label_visibility="collapsed",
                    )
            save = st.form_submit_button("Save verdicts & resolve flags",
                                         type="primary")
            dismiss = st.form_submit_button("All live (twins) — dismiss flags")

    if not save and not dismiss:
        return

    verdicts: dict[int, str] = {}
    if save:
        for det in detections:
            det_id = det["detection_id"]
            verdict = st.session_state.get(f"flagv_{file_id}_{det_id}", "")
            if verdict:
                verdicts[det_id] = verdict
        if not verdicts:
            st.info("No verdicts filled in — use the dismiss button if "
                    "every face is live.")
            return

    def apply(run_id):
        result = {"depictions": 0, "not_persons": 0, "not_faces": 0,
                  "flags_resolved": 0}
        for det_id, verdict in verdicts.items():
            if verdict == VERDICT_DEPICTION:
                face_ops.mark_detection_depiction(run_id=run_id,
                                                  detection_id=det_id)
                result["depictions"] += 1
            elif verdict == VERDICT_DOLL:
                face_ops.mark_detection_not_a_person(run_id=run_id,
                                                     detection_id=det_id)
                result["not_persons"] += 1
            elif verdict == VERDICT_NOT_FACE:
                face_ops.mark_detection_not_a_face(run_id=run_id,
                                                   detection_id=det_id)
                result["not_faces"] += 1
        note = ("dismissed: all live (twins/lookalikes)" if not verdicts
                else f"resolved with verdicts: {len(verdicts)} face(s)")
        for flag in file_flags:
            face_ops.resolve_same_photo_flag(
                run_id=run_id, action_id=flag["action_id"], note=note)
            result["flags_resolved"] += 1
        return result

    stats = _ui_run(db_path, conn, "ui-flag-triage", apply)
    st.success(
        f"Resolved {stats['flags_resolved']} flag(s): "
        f"{stats['depictions']} depiction(s), {stats['not_persons']} "
        f"doll/statue, {stats['not_faces']} not-a-face."
    )
    st.rerun()


def page_merge_review(db_path: Path, conn: sqlite3.Connection,
                      face_ops: FaceDBOperations, thumb_dir: Path):
    st.header("Suggestion Review")
    db_ops = DBOperations(conn)

    # Mechanical merges (window duplicates, clean tracklet evidence) are
    # true by construction — they get one bulk button, not one review card
    # each, so the queue below only holds real decisions.
    mechanical_ids = face_ops.get_pending_mechanical_merge_ids()
    if mechanical_ids:
        st.info(
            f"{len(mechanical_ids)} mechanical merge(s) pending — window "
            f"duplicates and clean same-event tracklet evidence. These need "
            f"no judgment; the named-person conflict guard still applies."
        )
        if st.button(f"Accept all {len(mechanical_ids)} mechanical merges"):
            from photo_organizer.faces.linking import apply_accepted_proposals

            def apply(run_id, _ids=tuple(mechanical_ids)):
                return apply_accepted_proposals(db_ops, list(_ids),
                                                run_id=run_id)
            stats = _ui_run(db_path, conn, "ui-accept-mechanical", apply)
            st.success(
                f"Applied {stats.get('merges_applied', 0)} merge(s); "
                f"{stats.get('conflict_components', 0)} conflict(s) and "
                f"{stats.get('cannot_link_conflicts', 0)} cannot-link "
                f"conflict(s) skipped."
            )
            st.rerun()

    # Same-photo flags: near-identical faces in ONE photo — framed photos,
    # mirrors/reflections, dolls, or twins. Triage happens per PHOTO with
    # the full image rendered: reflections need the whole scene to call,
    # a wall of framed pictures means several depictions at once, and a
    # shelf of dolls needs its own verdict entirely.
    flags = face_ops.get_pending_same_photo_flags()
    if not flags:
        st.caption(
            "No same-photo flags pending. Flags (near-identical faces "
            "in one photo — framed pictures, reflections, dolls, twins) "
            "are generated by `photo-faces link`; re-run it to refresh them."
        )
    else:
        by_file: dict = {}
        for flag in flags:
            by_file.setdefault(flag["file_id"], []).append(flag)
        st.subheader(f"Same-photo flags — {len(flags)} pair(s) in "
                     f"{len(by_file)} photo(s)")
        for file_id, file_flags in by_file.items():
            _same_photo_flag_triage(db_path, conn, face_ops, file_id,
                                    file_flags)
        st.divider()

    proposals = face_ops.get_pending_judgment_proposals(limit=40)
    if not proposals:
        st.success("No pending judgment suggestions — run `photo-faces link` "
                   "or `photo-faces refine` to generate more.")
        return

    persons = {p["id"]: p["display_name"] or f"person #{p['id']}"
               for p in face_ops.get_persons_summary()}
    st.info(f"{len(proposals)} suggestion(s) needing judgment, highest "
            f"confidence first.")

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
                            face_ops.get_cluster_review_sample(int(cluster_id),
                                                               limit=5),
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
            # The face-aware reject also snapshots the faces involved as a
            # cannot-link constraint, so re-clustering can never resurface
            # this suggestion under new cluster ids.
            def apply(run_id, _aid=proposal["id"]):
                result = face_ops.reject_face_proposals(
                    [_aid], run_id=run_id, note="rejected in review UI",
                )
                return {"rejected": len(result["rejected"]),
                        "cannot_links_created": result["cannot_links_created"]}
            _ui_run(db_path, conn, "ui-reject", apply)
            st.info("Rejected — this pairing will not be suggested again.")
            st.rerun()
        st.divider()


@st.cache_data(show_spinner="Rendering photo…", max_entries=8)
def _render_photo(path: str, file_type: str, det_key: tuple,
                  display_width: int = 1400) -> tuple:
    """Load a photo once and return (annotated JPEG bytes, [crop JPEG bytes]).

    Cached by path + detection set: widget interactions rerun the script, and
    without caching every rerun would re-decode and re-encode a full-res
    photo. Detection bboxes are in detect-space (the ≤MAX_DETECTION_DIMENSION
    downscale detection ran on); crops come from the full-res image, the
    overlay is drawn at display size. Stored thumbnails are never trusted
    (they may predate the thumbnail-scale fix).
    """
    import io

    from PIL import Image, ImageDraw

    from photo_organizer.faces import config as fconfig
    from photo_organizer.faces.image_loader import load_image_as_rgb

    img = load_image_as_rgb(Path(path), file_type)
    if img is None:
        return None, []
    full_h, full_w = img.shape[:2]
    detect_scale = min(
        1.0, fconfig.MAX_DETECTION_DIMENSION / max(full_h, full_w),
    )

    pil = Image.fromarray(img)
    crops = []
    for _detection_id, bbox in det_key:
        x, y, w, h = (v / detect_scale for v in bbox)
        pad_x, pad_y = w * 0.3, h * 0.3
        crop = pil.crop((
            max(0, int(x - pad_x)), max(0, int(y - pad_y)),
            min(full_w, int(x + w + pad_x)), min(full_h, int(y + h + pad_y)),
        ))
        crop.thumbnail((220, 220))
        buf = io.BytesIO()
        crop.save(buf, "JPEG", quality=88)
        crops.append(buf.getvalue())

    display_scale = min(1.0, display_width / full_w)
    if display_scale < 1.0:
        pil = pil.resize((int(full_w * display_scale),
                          int(full_h * display_scale)))
    draw = ImageDraw.Draw(pil)
    line = max(2, pil.width // 400)
    for i, (_detection_id, bbox) in enumerate(det_key):
        x, y, w, h = (v * display_scale / detect_scale for v in bbox)
        draw.rectangle((x, y, x + w, y + h), outline=(255, 80, 80), width=line)
        draw.text((x + line, y + line), str(i + 1), fill=(255, 80, 80))
    buf = io.BytesIO()
    pil.save(buf, "JPEG", quality=85)
    return buf.getvalue(), crops


NEW_PERSON = "(new person — type the name below)"
NOT_A_FACE = "(not a face)"
# A real face that is not live at capture time: framed photo on the wall,
# screen, poster, mirror. Its capture date says nothing about the person, so
# it is excluded from era clustering, tracklets, co-occurrence, and anchors.
DEPICTION = "(a photo of a photo — framed picture, screen, poster)"
# A face-like object that is not a person at all — dolls, statues,
# mannequins. Excluded from all identity work, like not-a-face.
DOLL = "(doll / statue — not a person)"


@st.cache_data(show_spinner="Sampling photos by labeling value…")
def _sample_photos(db_path_str: str, catalog_version: int) -> list[dict]:
    """Cached photo sampling — the aggregation walks every live cluster
    membership and takes ~a minute on a large catalog, far too slow to run
    per page render. catalog_version keys the cache: it advances on CLI runs
    (scan/cluster/link/...) that reshape the cluster landscape; individual
    label saves deliberately do NOT invalidate (already-labeled faces simply
    show as labeled). The Refresh button clears it manually."""
    from photo_organizer.faces import config as fconfig

    conn = get_connection(db_path_str)
    return FaceDBOperations(DBOperations(conn)).get_photos_for_labeling(
        limit=20, min_det_score=fconfig.MIN_WORKING_DET_SCORE,
    )


def _catalog_version(conn: sqlite3.Connection) -> int:
    """Last non-UI command run id — cheap proxy for 'the cluster landscape
    changed'."""
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) FROM command_runs "
        "WHERE tool != 'photo-faces-ui'"
    ).fetchone()
    return int(row[0])


def page_label_photos(db_path: Path, conn: sqlite3.Connection,
                      face_ops: FaceDBOperations, thumb_dir: Path):
    from photo_organizer.faces import config as fconfig

    st.header("Label Photos")
    st.caption("Photos sampled by labeling value — naming the faces here "
               "resolves the most people per click. Fill in the faces you "
               "recognize, leave the rest on skip, then Save all once.")

    photos = _sample_photos(str(db_path), _catalog_version(conn))
    if st.button("↻ Refresh photo list",
                 help="Re-sample after labeling sessions; the list also "
                      "refreshes automatically after cluster/link/scan runs."):
        _sample_photos.clear()
        st.rerun()
    if not photos:
        st.success("Nothing left to label — every face is linked or junk.")
        return

    options = {
        f"{(p['capture_datetime'] or 'undated')[:10]} — {p['faces']} face(s) — "
        f"resolves ~{p['score']} — {Path(p['path']).name}": p
        for p in photos
    }
    choice = st.selectbox("Photo", list(options))
    photo = options[choice]
    detections = face_ops.get_photo_detections(
        photo["file_id"], min_det_score=fconfig.MIN_WORKING_DET_SCORE,
    )

    det_key = tuple((d["detection_id"], d["bbox"]) for d in detections)
    annotated, crops = _render_photo(photo["path"], photo["file_type"], det_key)
    if annotated is None:
        st.error(f"Could not load {photo['path']}")
        return
    st.image(annotated, caption=f"{(photo['capture_datetime'] or 'undated')[:10]}"
                                f" — {photo['path']}")

    persons = face_ops.get_persons_summary()
    names_by_id = {p["id"]: p["display_name"] for p in persons}
    named_options = {p["display_name"]: p["id"]
                     for p in persons if p["display_name"]}

    # One form for the whole photo: nothing reruns while you fill it in,
    # and one Save applies every label in a single audited run.
    entries = []
    with st.form(key=f"label_form_{photo['file_id']}"):
        for i, det in enumerate(detections):
            col_face, col_ctl = st.columns([1, 3])
            with col_face:
                if i < len(crops):
                    st.image(crops[i], caption=f"Face {i + 1}", width=110)
            with col_ctl:
                if det["person_id"] is not None:
                    who = (names_by_id.get(det["person_id"])
                           or f"person #{det['person_id']}")
                    st.caption(f"Already labeled: {who}")
                    continue

                selected = st.selectbox(
                    f"Face {i + 1} is…",
                    options=["", *named_options, NOT_A_FACE, DEPICTION, DOLL],
                    key=f"lbl_{det['detection_id']}",
                    help="Leave blank to skip this face for now. Use the "
                         "depiction option for faces inside framed photos, "
                         "screens, or mirrors — real faces, wrong date. "
                         "Dolls and statues get their own option.",
                )
                new_name = st.text_input(
                    "…or a new/other name",
                    key=f"lblname_{det['detection_id']}",
                    help="Typing a name here wins over the dropdown; an "
                         "existing person with this name is reused.",
                )
                new_bd = st.text_input(
                    "Birth date for a NEW person (YYYY-MM-DD, optional)",
                    key=f"lblbd_{det['detection_id']}",
                )
                cluster_size = det.get("cluster_size") or 0
                if det["largest_cluster_id"] is not None:
                    apply_cluster = st.checkbox(
                        f"…and label its whole cluster ({cluster_size} "
                        f"similar face(s) found by clustering)",
                        value=True,
                        key=f"lblclu_{det['detection_id']}",
                    )
                    with st.expander("Peek at this face's cluster "
                                     "(core face + most dissimilar members)"):
                        _thumb_grid(st, thumb_dir,
                                    face_ops.get_cluster_review_sample(
                                        det["largest_cluster_id"], limit=5),
                                    width=80)
                else:
                    apply_cluster = False
                    st.caption("Not part of a cluster — labels just this face.")
                entries.append((det, i))
            st.divider()

        submitted = st.form_submit_button(
            "Save all", type="primary",
            help="Applies every filled-in face in one audited run.",
        )

    if not submitted:
        return

    filled = any(
        st.session_state.get(f"lbl_{det['detection_id']}", "")
        or (st.session_state.get(f"lblname_{det['detection_id']}", "") or "").strip()
        for det, _i in entries
    )
    if not filled:
        st.info("Nothing filled in — nothing saved.")
        return

    def apply(run_id):
        result = {"faces_labeled": 0, "clusters_labeled": 0,
                  "persons_created": 0, "not_faces": 0, "depictions": 0,
                  "not_persons": 0}
        for det, _i in entries:
            det_id = det["detection_id"]
            selected = st.session_state.get(f"lbl_{det_id}", "")
            typed = (st.session_state.get(f"lblname_{det_id}", "") or "").strip()
            birth = (st.session_state.get(f"lblbd_{det_id}", "") or "").strip()
            apply_cluster = bool(st.session_state.get(f"lblclu_{det_id}", False))

            if not selected and not typed:
                continue
            if selected == NOT_A_FACE and not typed:
                face_ops.mark_detection_not_a_face(run_id=run_id,
                                                   detection_id=det_id)
                result["not_faces"] += 1
                continue
            if selected == DEPICTION and not typed:
                face_ops.mark_detection_depiction(run_id=run_id,
                                                  detection_id=det_id)
                result["depictions"] += 1
                continue
            if selected == DOLL and not typed:
                face_ops.mark_detection_not_a_person(run_id=run_id,
                                                     detection_id=det_id)
                result["not_persons"] += 1
                continue

            if typed:
                existing = face_ops.find_person_by_name(typed)
                if existing is not None:
                    person_id = existing[0]
                else:
                    person_id = face_ops.create_person(
                        run_id=run_id, display_name=typed,
                        birth_date=birth or None,
                    )
                    result["persons_created"] += 1
            else:
                person_id = named_options[selected]

            face_ops.link_detection_to_person(
                run_id=run_id, detection_id=det_id, person_id=person_id,
                confidence=1.0, link_method="photo_label",
            )
            result["faces_labeled"] += 1

            cluster_id = det["largest_cluster_id"]
            if apply_cluster and cluster_id is not None:
                row = conn.execute(
                    "SELECT person_id FROM face_person_links "
                    "WHERE cluster_id = ? AND status = 'accepted'",
                    (cluster_id,),
                ).fetchone()
                if row is None:
                    face_ops.accept_cluster(run_id=run_id, cluster_id=cluster_id)
                    face_ops.link_cluster_to_person(
                        run_id=run_id, cluster_id=cluster_id,
                        person_id=person_id, link_method="photo_label",
                    )
                    result["clusters_labeled"] += 1
                elif row[0] != person_id and names_by_id.get(row[0]) is None:
                    face_ops.absorb_person(run_id=run_id, absorbed_id=row[0],
                                           winner_id=person_id)
        return result

    stats = _ui_run(db_path, conn, "ui-label-faces", apply)
    if any(stats.values()):
        st.success(
            f"Saved: {stats['faces_labeled']} face(s) labeled "
            f"({stats['clusters_labeled']} whole cluster(s)), "
            f"{stats['persons_created']} new person(s), "
            f"{stats['not_faces']} marked not-a-face, "
            f"{stats['depictions']} marked as depictions, "
            f"{stats['not_persons']} marked doll/statue."
        )
        st.rerun()
    else:
        st.info("Nothing filled in — nothing saved.")


def page_name_people(db_path: Path, conn: sqlite3.Connection,
                     face_ops: FaceDBOperations, thumb_dir: Path):
    st.header("Name People")
    persons = face_ops.get_persons_summary()
    unnamed = sorted((p for p in persons if not p["display_name"]),
                     key=lambda p: -p["detections"])
    named_options = {p["display_name"]: p["id"]
                     for p in persons if p["display_name"]}

    if not unnamed:
        st.success("No anonymous person groups right now.")
        st.caption(
            "Groups appear here when accepted merges connect clusters that "
            "don't belong to a named person yet — e.g. after a bulk accept "
            "(`photo-faces accept --min-confidence 95`) sweeps the "
            "window-duplicate merges. Unmerged clusters are not persons yet; "
            "review them via Label Photos or Suggestion Review."
        )
        return

    st.info(f"{len(unnamed)} anonymous person group(s), largest first. "
            "Give them a name, or fold them into someone you've already named.")

    for person in unnamed[:50]:
        with st.expander(
            f"Person #{person['id']} | {person['clusters']} cluster(s) | "
            f"{person['detections']} face(s)",
            expanded=False,
        ):
            _thumb_grid(st, thumb_dir,
                        face_ops.get_person_detection_timeline(person["id"])[:10])

            col_name, col_merge = st.columns(2)
            with col_name:
                new_name = st.text_input("Name", key=f"pname_{person['id']}")
                new_bd = st.text_input("Birth date (YYYY-MM-DD, optional)",
                                       key=f"pbd_{person['id']}")
                if new_name and st.button("Name person",
                                          key=f"btn_pname_{person['id']}"):
                    def apply(run_id, _pid=person["id"], _name=new_name,
                              _bd=new_bd):
                        face_ops.update_person(
                            run_id=run_id, person_id=_pid,
                            display_name=_name, birth_date=_bd or None,
                        )
                        return {"persons_labeled": 1}
                    _ui_run(db_path, conn, "ui-name-person", apply)
                    st.success(f"Named '{new_name}'")
                    st.rerun()
            with col_merge:
                if named_options:
                    target = st.selectbox(
                        "This is actually…",
                        options=["", *named_options],
                        key=f"pmerge_{person['id']}",
                    )
                    if target and st.button("Fold into person",
                                            key=f"btn_pmerge_{person['id']}"):
                        def apply(run_id, _pid=person["id"],
                                  _winner=named_options[target]):
                            return face_ops.absorb_person(
                                run_id=run_id, absorbed_id=_pid,
                                winner_id=_winner,
                            )
                        _ui_run(db_path, conn, "ui-absorb-person", apply)
                        st.success(f"Folded into {target}")
                        st.rerun()


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
        "Label Photos",
        "Cluster Review",
        "Suggestion Review",
        "Name People",
        "Timeline",
        "Query",
    ])

    import time as _time
    _render_started = _time.perf_counter()

    if page == "Stats":
        page_stats(face_ops)
    elif page == "Label Photos":
        page_label_photos(db_path, conn, face_ops, thumb_dir)
    elif page == "Cluster Review":
        page_cluster_review(db_path, conn, face_ops, thumb_dir)
    elif page == "Suggestion Review":
        page_merge_review(db_path, conn, face_ops, thumb_dir)
    elif page == "Name People":
        page_name_people(db_path, conn, face_ops, thumb_dir)
    elif page == "Timeline":
        page_timeline(face_ops, thumb_dir)
    elif page == "Query":
        page_query(face_ops)

    st.sidebar.caption(
        f"page rendered in {_time.perf_counter() - _render_started:.2f}s"
    )


if __name__ == "__main__":
    run_app()
