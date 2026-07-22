"""
CLI entry point for face recognition tools.

Provides the `photo-faces` command. Phase 1 ships the `scan` subcommand
(detection + embeddings); clustering, linking, and review commands arrive
with later phases of the facial-recognition port.

Every invocation records a command_runs row (tool='photo-faces') so face
runs appear in `photo-catalog-query --show-runs` alongside organizer runs.
"""
import argparse
import logging
import sqlite3
import sys
import warnings
from pathlib import Path

from ..database.schema import init_schema
from ..run_log import RunRecorder
from . import config

APP_VERSION = "photo-faces 0.1.0"


def setup_logging(verbose: bool):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # insightface 1.0.x calls a scikit-image API deprecated in skimage 0.26
    # (SimilarityTransform.estimate); the warning fires once per aligned face
    # and is not actionable here. Scoped to insightface so our own
    # deprecations stay visible.
    warnings.filterwarnings(
        "ignore", category=FutureWarning, module=r"insightface(\..*)?",
    )


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Face recognition tools for photo-organizer"
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to photo_catalog.db")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Enable debug logging")

    sub = p.add_subparsers(dest="command", required=True)

    scan_p = sub.add_parser(
        "scan",
        help="Detect faces and record embeddings for unscanned catalog images",
    )
    scan_p.add_argument("--gpu", dest="use_gpu",
                        action="store_true", default=True,
                        help="Use GPU acceleration (default)")
    scan_p.add_argument("--cpu", dest="use_gpu", action="store_false",
                        help="Force CPU inference")
    scan_p.add_argument("--limit", type=int, default=None,
                        help="Max images to process this run")
    scan_p.add_argument("--include-raw", action="store_true",
                        help="Also scan RAW files that have no linked JPEG/TIFF "
                             "output (requires rawpy; much slower per file)")
    scan_p.add_argument("--thumbnail-dir", type=Path, default=None,
                        help="Dir for face thumbnails "
                             f"(default: <db_dir>/{config.THUMBNAIL_DIR_NAME})")

    cluster_p = sub.add_parser(
        "cluster",
        help="Propose identity clusters from scanned embeddings (era-based "
             "HDBSCAN); re-running supersedes prior proposed clusters",
    )
    cluster_p.add_argument("--era-size", type=float,
                           default=config.DEFAULT_ERA_SIZE_YEARS,
                           help="Era window size in years "
                                f"(default {config.DEFAULT_ERA_SIZE_YEARS})")
    cluster_p.add_argument("--min-cluster-size", type=int,
                           default=config.HDBSCAN_MIN_CLUSTER_SIZE,
                           help="Minimum faces to form a cluster "
                                f"(default {config.HDBSCAN_MIN_CLUSTER_SIZE})")
    cluster_p.add_argument("--min-samples", type=int,
                           default=config.HDBSCAN_MIN_SAMPLES,
                           help="HDBSCAN min_samples "
                                f"(default {config.HDBSCAN_MIN_SAMPLES})")
    cluster_p.add_argument("--pca-dims", type=int,
                           default=config.CLUSTER_PCA_DIMS,
                           help="PCA-reduce embeddings to N dims before "
                                "clustering; 0 clusters on full dimensions "
                                f"(default {config.CLUSTER_PCA_DIMS})")
    cluster_p.add_argument("--min-det-score", type=float,
                           default=config.MIN_WORKING_DET_SCORE,
                           help="Exclude detections below this det_score from "
                                "clustering (junk filter; 0 includes all; "
                                f"default {config.MIN_WORKING_DET_SCORE})")
    cluster_p.add_argument("--min-member-sim", type=float,
                           default=config.MIN_MEMBER_SIMILARITY,
                           help="Coherence gate: members below this cosine "
                                "similarity to their cluster centroid are "
                                "trimmed to noise (0 disables; default "
                                f"{config.MIN_MEMBER_SIMILARITY})")
    cluster_p.add_argument("--cohesion-edge-sim", type=float,
                           default=config.COHESION_MIN_EDGE_SIM,
                           help="Cosine floor for mutual-kNN graph edges in "
                                "the cohesion gate; chained clusters split "
                                "into components when weak edges are cut "
                                "(0 disables; default "
                                f"{config.COHESION_MIN_EDGE_SIM})")
    cluster_p.add_argument("--cohesion-knn", type=int,
                           default=config.COHESION_KNN,
                           help="Mutual neighbors considered per member in "
                                "the cohesion gate (default "
                                f"{config.COHESION_KNN})")

    link_p = sub.add_parser(
        "link",
        help="Score cluster pairs across eras and propose cross-age merge "
             "suggestions (review via photo-catalog-query --show-proposals "
             "--action-type face_cluster_merge)",
    )
    link_p.add_argument("--min-confidence", type=float,
                        default=config.MIN_MERGE_CONFIDENCE,
                        help="Minimum weighted score to propose a merge "
                             f"(default {config.MIN_MERGE_CONFIDENCE})")
    link_p.add_argument("--max-gap-years", type=float,
                        default=config.MAX_ERA_GAP_YEARS,
                        help="Maximum gap between eras to compare "
                             f"(default {config.MAX_ERA_GAP_YEARS})")
    link_p.add_argument("--top-k", type=int, default=config.LINK_TOP_K,
                        help="Keep only each cluster's K best scored "
                             "suggestions; 0 keeps all "
                             f"(default {config.LINK_TOP_K})")
    link_p.add_argument("--event-gap-minutes", type=float,
                        default=config.EVENT_GAP_MINUTES,
                        help="Photos closer together than this are one event "
                             "for same-event tracklet evidence "
                             f"(default {config.EVENT_GAP_MINUTES})")
    link_p.add_argument("--no-tracklets", action="store_true",
                        help="Skip the same-event tracklet evidence tier")

    seed_p = sub.add_parser(
        "seed",
        help="Load known people (names + birth dates) from a YAML config; "
             "birth dates unlock developmental era windows in clustering",
    )
    seed_p.add_argument("--config", type=Path, required=True,
                        help="Path to faces_config.yaml")

    accept_p = sub.add_parser(
        "accept",
        help="Accept face suggestions: clusters join into a person (created "
             "or reused) and the proposals become applied",
    )
    accept_p.add_argument("action_ids", nargs="*", type=int, metavar="ACTION_ID",
                          help="Proposal ids "
                               "(from photo-catalog-query --show-proposals)")
    accept_p.add_argument("--min-confidence", type=int, default=None,
                          metavar="PERCENT",
                          help="Bulk mode: accept ALL pending face suggestions "
                               "at or above this confidence (0-100). Window "
                               "duplicates are proposed at 100. The union-find "
                               "and named-person conflict guard apply as usual.")
    accept_p.add_argument("--mechanical", action="store_true",
                          help="Bulk mode: accept only the construction-true "
                               "tiers — window duplicates (same faces "
                               "clustered twice by overlapping windows) and "
                               "tracklet merges with no same-photo "
                               "contradiction. Leaves every judgment-required "
                               "suggestion for review.")

    reject_p = sub.add_parser(
        "reject",
        help="Reject face suggestions durably: the proposals flip to "
             "rejected AND the faces involved are recorded as cannot-link "
             "constraints, so re-clustering can never resurface the same "
             "suggestion under new cluster ids",
    )
    reject_p.add_argument("action_ids", nargs="+", type=int, metavar="ACTION_ID",
                          help="Proposal ids "
                               "(from photo-catalog-query --show-proposals)")
    reject_p.add_argument("--note", type=str, default=None,
                          help="Reason recorded on the rejection")

    rescan_p = sub.add_parser(
        "rescan-misoriented",
        help="Find scanned JPEG/TIFF files whose EXIF orientation tag means "
             "detection saw sideways pixels (pre-orientation-fix scans), "
             "and invalidate their detections so the next scan redoes them "
             "upright. Dry-run by default.",
    )
    rescan_p.add_argument("--apply", action="store_true",
                          help="Actually delete the affected files' "
                               "detections (files with human decisions are "
                               "always skipped and reported). Follow with "
                               "photo-faces scan.")

    repair_p = sub.add_parser(
        "repair-contaminated-clusters",
        help="Find clusters carrying BOTH accepted members and proposed "
             "members (an accepted cluster that had unrelated new faces "
             "grafted onto it by a later re-cluster run before the "
             "write-path guard existed) and evict the proposed portion, "
             "restoring the cluster to its accepted core. Dry-run by "
             "default.",
    )
    repair_p.add_argument("--apply", action="store_true",
                          help="Actually evict the grafted members "
                               "(durable cannot-links prevent recurrence). "
                               "Freed faces are picked up by the next "
                               "photo-faces cluster run.")

    unlink_p = sub.add_parser(
        "unlink-cluster",
        help="Retract a cluster's accepted person-link and revert the "
             "cluster/its accepted memberships to proposed — for an "
             "accepted merge that turns out to be wrong even after "
             "naming (photo-faces unwind deliberately spares links to "
             "named persons; this is the manual override for exactly "
             "that case). The person record itself is untouched.",
    )
    unlink_p.add_argument("cluster_id", type=int)
    unlink_p.add_argument("--note", type=str, default=None)

    sub.add_parser(
        "conflicts",
        help="Show pending merge components that would fuse two NAMED "
             "persons (accept skips them wholesale), with the shortest "
             "bridge of proposals to review — reject the wrong one with "
             "photo-faces reject, then re-run accept",
    )

    rethumb_p = sub.add_parser(
        "rethumb",
        help="Regenerate face thumbnails from source images with correct "
             "bbox scaling (fixes thumbnails written before the scale fix)",
    )
    rethumb_p.add_argument("--min-det-score", type=float,
                           default=config.MIN_WORKING_DET_SCORE,
                           help="Only regenerate for detections at/above this "
                                "score (0 = all; default "
                                f"{config.MIN_WORKING_DET_SCORE})")
    rethumb_p.add_argument("--workers", type=int, default=4,
                           help="Parallel image decoders (default 4)")
    rethumb_p.add_argument("--limit", type=int, default=None,
                           help="Process at most N files (trial runs)")
    rethumb_p.add_argument("--thumbnail-dir", type=Path, default=None,
                           help="Thumbnail dir (default: "
                                f"<db_dir>/{config.THUMBNAIL_DIR_NAME})")

    unwind_p = sub.add_parser(
        "unwind",
        help="Revert the accepted face state a previous accept run created "
             "(links retracted, clusters back to proposed, anonymous persons "
             "retired). Links to since-named persons are kept.",
    )
    unwind_p.add_argument("accept_run_id", type=int,
                          help="command_runs id of the accept run to unwind "
                               "(find it via photo-catalog-query --show-runs "
                               "--tool photo-faces --command accept)")

    label_p = sub.add_parser(
        "label",
        help="Name a person (e.g. one created by accepting merges)",
    )
    label_p.add_argument("person_id", type=int)
    label_p.add_argument("name", type=str)
    label_p.add_argument("--birth-date", type=str, default=None,
                         help="Birth date in YYYY-MM-DD format")

    refine_p = sub.add_parser(
        "refine",
        help="Propose assigning unlinked clusters to labeled persons when "
             "their embedding is close to exactly one person's anchor",
    )
    refine_p.add_argument("--threshold", type=float,
                          default=config.AUTO_ASSIGN_THRESHOLD,
                          help="Minimum cosine similarity to an anchor "
                               f"(default {config.AUTO_ASSIGN_THRESHOLD})")
    refine_p.add_argument("--margin", type=float,
                          default=config.AUTO_ASSIGN_MARGIN,
                          help="Required gap over the second-best anchor "
                               f"(default {config.AUTO_ASSIGN_MARGIN})")

    sub.add_parser(
        "persons",
        help="List persons with accepted cluster and face counts",
    )

    query_p = sub.add_parser(
        "query",
        help="Find photos of a person by name (accepted appearances only)",
    )
    query_p.add_argument("person_name", type=str)
    query_p.add_argument("--from", dest="date_from", type=str, default=None,
                         help="ISO lower bound on capture date (e.g. 2012-01-01)")
    query_p.add_argument("--to", dest="date_to", type=str, default=None,
                         help="ISO upper bound on capture date")
    query_p.add_argument("--timeline", action="store_true",
                         help="Print per-year photo counts instead of paths")
    query_p.add_argument("--csv-output", type=str, default=None,
                         help="Write matching photos to a CSV file")

    sub.add_parser(
        "ui",
        help="Launch the Streamlit review & labeling app (requires the "
             "faces extra)",
    )

    return p.parse_args(argv)


def _validate_args(args) -> None:
    """Reject nonsense numeric arguments before any run starts — a zero
    era size is an infinite loop, not a preference, and argparse only
    checks types."""
    def require(condition: bool, message: str) -> None:
        if not condition:
            raise SystemExit(message)

    if args.command == "cluster":
        require(args.era_size > 0, "--era-size must be positive")
        require(args.min_cluster_size >= 2,
                "--min-cluster-size must be at least 2")
        require(args.min_samples >= 1, "--min-samples must be at least 1")
        require(args.pca_dims >= 0, "--pca-dims cannot be negative")
        require(0 <= args.min_det_score <= 1,
                "--min-det-score must be within [0, 1]")
        require(0 <= args.min_member_sim <= 1,
                "--min-member-sim must be within [0, 1]")
        require(0 <= args.cohesion_edge_sim <= 1,
                "--cohesion-edge-sim must be within [0, 1]")
        require(args.cohesion_knn >= 1, "--cohesion-knn must be at least 1")
    elif args.command == "link":
        require(0 <= args.min_confidence <= 1,
                "--min-confidence must be within [0, 1]")
        require(args.max_gap_years >= 0, "--max-gap-years cannot be negative")
        require(args.top_k >= 0, "--top-k cannot be negative")
        require(args.event_gap_minutes > 0,
                "--event-gap-minutes must be positive")
    elif args.command == "refine":
        require(0 <= args.threshold <= 1, "--threshold must be within [0, 1]")
        require(0 <= args.margin <= 1, "--margin must be within [0, 1]")
    elif args.command == "accept" and args.min_confidence is not None:
        require(0 <= args.min_confidence <= 100,
                "--min-confidence is a percentage: 0-100")


def _run_cluster(args, db_path: Path, run_id) -> dict:
    from .clustering import FaceClusterPipeline

    pipeline = FaceClusterPipeline(
        db_path,
        era_size_years=args.era_size,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
        pca_dims=args.pca_dims,
        min_det_score=args.min_det_score,
        min_member_sim=args.min_member_sim,
        cohesion_edge_sim=args.cohesion_edge_sim,
        cohesion_knn=args.cohesion_knn,
    )
    return pipeline.run(run_id=run_id)


def _run_link(args, db_path: Path, run_id) -> dict:
    from .linking import CrossAgeLinker

    linker = CrossAgeLinker(
        db_path,
        min_confidence=args.min_confidence,
        max_gap_years=args.max_gap_years,
        top_k=args.top_k,
        use_tracklets=not args.no_tracklets,
        event_gap_minutes=args.event_gap_minutes,
    )
    return linker.run(run_id=run_id)


def _run_seed(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .seed import apply_seed, load_seed_config

    people = load_seed_config(args.config)
    with DBManager(db_path) as conn:
        stats = apply_seed(DBOperations(conn), people, run_id=run_id)
        conn.commit()
    return stats


def _run_accept(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .linking import apply_accepted_proposals

    modes = [bool(args.action_ids), args.min_confidence is not None,
             bool(args.mechanical)]
    if sum(modes) != 1:
        raise SystemExit(
            "accept takes exactly one of: explicit ACTION_IDs, "
            "--min-confidence, or --mechanical."
        )

    with DBManager(db_path) as conn:
        db_ops = DBOperations(conn)
        action_ids = args.action_ids
        if args.min_confidence is not None:
            action_ids = [row[0] for row in db_ops.conn.execute(
                """
                SELECT id FROM run_actions
                WHERE action_type IN ('face_cluster_merge', 'face_person_assign')
                  AND status = 'proposed'
                  AND confidence >= ?
                ORDER BY id
                """,
                (args.min_confidence,),
            )]
            logging.info(
                f"Bulk accept: {len(action_ids)} pending suggestion(s) at "
                f"confidence >= {args.min_confidence}."
            )
        elif args.mechanical:
            from .db_ops import FaceDBOperations

            action_ids = FaceDBOperations(db_ops).get_pending_mechanical_merge_ids()
            logging.info(
                f"Mechanical accept: {len(action_ids)} window-duplicate and "
                f"clean tracklet merge(s)."
            )
        stats = apply_accepted_proposals(db_ops, action_ids, run_id=run_id)
        conn.commit()
    return stats


def _run_reject(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .db_ops import FaceDBOperations

    with DBManager(db_path) as conn:
        face_ops = FaceDBOperations(DBOperations(conn))
        result = face_ops.reject_face_proposals(
            args.action_ids, run_id=run_id, note=args.note,
        )
        conn.commit()
    stats = {
        "proposals_rejected": len(result["rejected"]),
        "proposals_skipped": len(result["skipped"]),
        "cannot_links_created": result["cannot_links_created"],
    }
    if result["skipped"]:
        logging.warning(
            f"Skipped {len(result['skipped'])} id(s) that are not pending "
            f"proposals: {', '.join(str(i) for i in result['skipped'])}"
        )
    logging.info(
        f"Rejected {stats['proposals_rejected']} proposal(s); "
        f"{stats['cannot_links_created']} cannot-link constraint(s) recorded."
    )
    return stats


def _run_rescan_misoriented(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .db_ops import FaceDBOperations
    from .image_loader import read_exif_orientation

    try:
        from tqdm import tqdm
    except ImportError:  # pragma: no cover - tqdm is a core dependency
        tqdm = lambda x, **kw: x  # noqa: E731

    stats = {
        "files_checked": 0,
        "files_misoriented": 0,
        "files_skipped_human_touched": 0,
        "files_invalidated": 0,
        "detections_deleted": 0,
    }
    with DBManager(db_path) as conn:
        face_ops = FaceDBOperations(DBOperations(conn))
        files = face_ops.get_scanned_pil_files(
            model_name=config.MODEL_NAME,
            model_version=config.MODEL_VERSION_TAG,
        )
        stats["files_checked"] = len(files)

        misoriented = []
        for file_id, path, _type in tqdm(files, desc="Reading EXIF headers",
                                         unit="file"):
            orientation = read_exif_orientation(Path(path))
            if orientation is not None and orientation != 1:
                misoriented.append(file_id)
        stats["files_misoriented"] = len(misoriented)

        touched = face_ops.get_files_with_human_touched_detections(misoriented)
        stats["files_skipped_human_touched"] = len(touched)
        eligible = [f for f in misoriented if f not in touched]

        if not args.apply:
            logging.info(
                f"DRY RUN: {stats['files_misoriented']} of "
                f"{stats['files_checked']} scanned JPEG/TIFF file(s) carry a "
                f"non-upright EXIF orientation — their detections were "
                f"computed on sideways pixels. {len(eligible)} would be "
                f"invalidated ({len(touched)} skipped: human decisions on "
                f"their faces). Re-run with --apply, then photo-faces scan."
            )
            return stats

        counts = face_ops.invalidate_detections_for_files(
            run_id=run_id, file_ids=eligible,
        )
        conn.commit()
        stats["files_invalidated"] = counts["files"]
        stats["detections_deleted"] = counts["detections"]

    if touched:
        logging.warning(
            f"{len(touched)} misoriented file(s) kept their detections "
            f"because faces there carry labels/verdicts — review them "
            f"manually if their crops look rotated."
        )
    logging.info(
        f"Invalidated {stats['files_invalidated']} file(s) "
        f"({stats['detections_deleted']} detection(s) deleted). "
        f"Run `photo-faces scan` to re-detect them upright."
    )
    return stats


def _run_repair_contaminated_clusters(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .db_ops import FaceDBOperations

    with DBManager(db_path) as conn:
        face_ops = FaceDBOperations(DBOperations(conn))
        contaminated = face_ops.get_contaminated_clusters()
        stats = {
            "clusters_contaminated": len(contaminated),
            "clusters_repaired": 0,
            "members_evicted": 0,
        }
        if not args.apply:
            logging.info(
                f"DRY RUN: {stats['clusters_contaminated']} cluster(s) carry "
                f"both accepted and proposed members — the proposed portion "
                f"was grafted on by a re-cluster run after acceptance and "
                f"does not belong. Re-run with --apply to evict it "
                f"(durable cannot-links prevent recurrence; freed faces "
                f"are picked up by the next photo-faces cluster run)."
            )
            return stats
        for entry in contaminated:
            result = face_ops.evict_cluster_members(
                run_id=run_id, cluster_id=entry["cluster_id"],
                detection_ids=entry["proposed_detection_ids"],
                note="repair: grafted onto accepted cluster by a later "
                     "re-cluster run",
            )
            stats["members_evicted"] += result["evicted"]
            stats["clusters_repaired"] += 1
        conn.commit()
    logging.info(
        f"Repaired {stats['clusters_repaired']} cluster(s): "
        f"{stats['members_evicted']} grafted member(s) evicted."
    )
    return stats


def _run_unlink_cluster(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .db_ops import FaceDBOperations

    with DBManager(db_path) as conn:
        face_ops = FaceDBOperations(DBOperations(conn))
        stats = face_ops.unlink_cluster_from_person(
            run_id=run_id, cluster_id=args.cluster_id, note=args.note,
        )
        conn.commit()
    logging.info(
        f"Cluster {args.cluster_id}: {stats['links_retracted']} link(s) "
        f"retracted, {stats['members_reverted']} accepted membership(s) "
        f"reverted to proposed."
    )
    return stats


def _run_conflicts(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .linking import find_named_merge_conflicts

    with DBManager(db_path) as conn:
        conflicts = find_named_merge_conflicts(DBOperations(conn))

    if not conflicts:
        logging.info("No pending merge components touch two named persons.")
        return {"conflicts": 0}

    for conflict in conflicts:
        names = " / ".join(name for _pid, name in conflict["persons"])
        logging.info(
            f"CONFLICT: {names} — {conflict['component_clusters']} "
            f"cluster(s), {conflict['pending_proposals']} pending merge(s) "
            f"held back."
        )
        for bridge in conflict["bridges"]:
            logging.info(
                f"  bridge {bridge['person_a']} <-> {bridge['person_b']} "
                f"({len(bridge['edges'])} merge(s)):"
            )
            for edge in bridge["edges"]:
                logging.info(
                    f"    action #{edge['action_id']}: cluster "
                    f"{edge['cluster_a_id']} + {edge['cluster_b_id']} "
                    f"[{edge['method']}, conf {edge['confidence']}] — "
                    f"reject with: photo-faces reject {edge['action_id']}"
                )
    return {"conflicts": len(conflicts)}


def _run_rethumb(args, db_path: Path, run_id) -> dict:
    from .detection import regenerate_thumbnails

    thumbnail_dir = args.thumbnail_dir or (db_path.parent / config.THUMBNAIL_DIR_NAME)
    return regenerate_thumbnails(
        db_path,
        thumbnail_dir=thumbnail_dir,
        min_det_score=args.min_det_score,
        max_workers=args.workers,
        limit=args.limit,
    )


def _run_unwind(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .linking import unwind_accept_run

    with DBManager(db_path) as conn:
        stats = unwind_accept_run(
            DBOperations(conn), args.accept_run_id, run_id=run_id,
        )
        conn.commit()
    return stats


def _run_label(args, db_path: Path, run_id) -> dict:
    from datetime import datetime

    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .db_ops import FaceDBOperations

    if args.birth_date:
        try:
            datetime.strptime(args.birth_date, "%Y-%m-%d")
        except ValueError:
            raise SystemExit(
                f"Invalid --birth-date '{args.birth_date}': expected YYYY-MM-DD"
            )

    with DBManager(db_path) as conn:
        face_ops = FaceDBOperations(DBOperations(conn))
        updated = face_ops.update_person(
            run_id=run_id, person_id=args.person_id,
            display_name=args.name, birth_date=args.birth_date,
        )
        conn.commit()
    if updated:
        logging.info(f"Person #{args.person_id} labeled '{args.name}'"
                     + (f" (born {args.birth_date})" if args.birth_date else ""))
    else:
        logging.error(f"No person with id {args.person_id}.")
    return {"persons_labeled": 1 if updated else 0}


def _run_refine(args, db_path: Path, run_id) -> dict:
    from .refinement import RefinementEngine

    engine = RefinementEngine(
        db_path, threshold=args.threshold, margin=args.margin,
    )
    return engine.run(run_id=run_id)


def _run_persons(args, db_path: Path, run_id) -> dict:
    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .db_ops import FaceDBOperations

    with DBManager(db_path) as conn:
        persons = FaceDBOperations(DBOperations(conn)).get_persons_summary()

    if not persons:
        print("No persons yet — seed identities or accept merges first.")
        return {"persons": 0}

    print(f"{len(persons)} person(s):\n")
    for person in persons:
        name = person["display_name"] or "(unnamed)"
        born = f", born {person['birth_date']}" if person["birth_date"] else ""
        print(f"#{person['id']} {name}{born}")
        print(f"  clusters: {person['clusters']}  faces: {person['detections']}")
        print()
    return {"persons": len(persons)}


def _run_query(args, db_path: Path, run_id) -> dict:
    import csv

    from ..database.db import DBManager
    from ..database.ops import DBOperations
    from .db_ops import FaceDBOperations

    with DBManager(db_path) as conn:
        face_ops = FaceDBOperations(DBOperations(conn))
        person = face_ops.find_person_by_name(args.person_name)
        if person is None:
            print(f"No person named '{args.person_name}'. "
                  f"See `photo-faces persons` for known names.")
            return {"photos": 0}
        person_id, display_name, birth_date = person
        photos = face_ops.get_photos_for_person(
            person_id, date_from=args.date_from, date_to=args.date_to,
        )

    if args.csv_output:
        with open(args.csv_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["file_id", "path", "capture_datetime", "raw_path"])
            for photo in photos:
                writer.writerow([photo["file_id"], photo["path"],
                                 photo["capture_datetime"], photo["raw_path"]])
        print(f"Wrote {len(photos)} rows to: {args.csv_output}")

    if not photos:
        print(f"No accepted appearances of {display_name} in that range.")
        return {"photos": 0}

    if args.timeline:
        by_year: dict = {}
        for photo in photos:
            year = (photo["capture_datetime"] or "unknown")[:4]
            by_year[year] = by_year.get(year, 0) + 1
        print(f"{display_name}: {len(photos)} photo(s)\n")
        for year in sorted(by_year):
            print(f"  {year}  {'#' * min(by_year[year], 60)} {by_year[year]}")
    else:
        print(f"{display_name}: {len(photos)} photo(s)\n")
        for photo in photos:
            capture = (photo["capture_datetime"] or "?")[:19]
            print(f"{capture}  {photo['path']}")
            if photo["raw_path"]:
                print(f"{'':19}  RAW: {photo['raw_path']}")
    return {"photos": len(photos)}


def _run_scan(args, db_path: Path, run_id) -> dict:
    from .detection import FaceScanPipeline

    thumbnail_dir = args.thumbnail_dir or (db_path.parent / config.THUMBNAIL_DIR_NAME)
    pipeline = FaceScanPipeline(
        db_path,
        thumbnail_dir=thumbnail_dir,
        use_gpu=args.use_gpu,
        include_raw=args.include_raw,
    )
    return pipeline.run(run_id=run_id, limit=args.limit)


def _launch_ui(db_path: Path) -> int:
    """Exec streamlit with the review app. Mutations made in the UI record
    their own command runs (tool='photo-faces-ui'), so the launcher itself
    is not recorded."""
    import subprocess

    app_path = Path(__file__).parent / "streamlit_app.py"
    try:
        return subprocess.call([
            sys.executable, "-m", "streamlit", "run", str(app_path),
            "--", "--db", str(db_path),
        ])
    except FileNotFoundError:
        logging.error("streamlit is not installed. Install with: "
                      "uv sync --extra faces")
        return 1


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)

    try:
        _validate_args(args)
    except SystemExit as exc:
        logging.error(str(exc))
        return 1

    db_path = Path(args.db).resolve()
    # photo-faces never creates the catalog — it annotates an existing one.
    if not db_path.exists():
        logging.error(f"Database not found at {db_path}.")
        return 1

    if args.command == "ui":
        return _launch_ui(db_path)

    # Bring the schema up to date so RunRecorder and the face tables exist.
    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
    finally:
        conn.close()

    params = {
        "command": args.command,
        "use_gpu": bool(getattr(args, "use_gpu", False)),
        "limit": getattr(args, "limit", None),
        "include_raw": bool(getattr(args, "include_raw", False)),
        "era_size": getattr(args, "era_size", None),
        "min_cluster_size": getattr(args, "min_cluster_size", None),
        "pca_dims": getattr(args, "pca_dims", None),
        "min_det_score": getattr(args, "min_det_score", None),
        "min_member_sim": getattr(args, "min_member_sim", None),
        "min_confidence": getattr(args, "min_confidence", None),
        "model_name": config.MODEL_NAME,
        "model_version": config.MODEL_VERSION_TAG,
    }
    recorder = RunRecorder(
        db_path,
        tool="photo-faces",
        command=args.command,
        argv=sys.argv if argv is None else ["photo-faces", *argv],
        params=params,
        db_path_for_row=db_path,
        app_version=APP_VERSION,
    )
    recorder.start()

    try:
        if args.command == "scan":
            stats = _run_scan(args, db_path, recorder.row_id)
        elif args.command == "cluster":
            stats = _run_cluster(args, db_path, recorder.row_id)
        elif args.command == "link":
            stats = _run_link(args, db_path, recorder.row_id)
        elif args.command == "seed":
            stats = _run_seed(args, db_path, recorder.row_id)
        elif args.command == "accept":
            stats = _run_accept(args, db_path, recorder.row_id)
        elif args.command == "reject":
            stats = _run_reject(args, db_path, recorder.row_id)
        elif args.command == "conflicts":
            stats = _run_conflicts(args, db_path, recorder.row_id)
        elif args.command == "rescan-misoriented":
            stats = _run_rescan_misoriented(args, db_path, recorder.row_id)
        elif args.command == "repair-contaminated-clusters":
            stats = _run_repair_contaminated_clusters(args, db_path,
                                                       recorder.row_id)
        elif args.command == "unlink-cluster":
            stats = _run_unlink_cluster(args, db_path, recorder.row_id)
        elif args.command == "unwind":
            stats = _run_unwind(args, db_path, recorder.row_id)
        elif args.command == "rethumb":
            stats = _run_rethumb(args, db_path, recorder.row_id)
        elif args.command == "label":
            stats = _run_label(args, db_path, recorder.row_id)
        elif args.command == "refine":
            stats = _run_refine(args, db_path, recorder.row_id)
        elif args.command == "persons":
            stats = _run_persons(args, db_path, recorder.row_id)
        elif args.command == "query":
            stats = _run_query(args, db_path, recorder.row_id)
        else:  # pragma: no cover - argparse enforces the choices
            raise SystemExit(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        recorder.finish_interrupted(note="cancelled by user")
        logging.warning("Operation cancelled by user.")
        return 1
    except SystemExit as e:
        # Precondition failures (bad arguments) raised by command handlers.
        recorder.finish_error(e)
        logging.error(str(e))
        return 1
    except Exception as e:
        recorder.finish_error(e)
        logging.exception(f"Fatal error during {args.command}.")
        return 1

    recorder.finish_success(
        stats=stats,
        db_mutates=stats.get("faces_detected", 0) > 0
                   or stats.get("images_no_faces", 0) > 0
                   or stats.get("clusters_proposed", 0) > 0
                   or stats.get("clusters_superseded", 0) > 0
                   or stats.get("suggestions_proposed", 0) > 0
                   or stats.get("suggestions_superseded", 0) > 0
                   or stats.get("created", 0) > 0
                   or stats.get("updated", 0) > 0
                   or stats.get("merges_applied", 0) > 0
                   or stats.get("assignments_applied", 0) > 0
                   or stats.get("assignments_proposed", 0) > 0
                   or stats.get("assignments_superseded", 0) > 0
                   or stats.get("persons_labeled", 0) > 0
                   or stats.get("proposals_rejected", 0) > 0
                   or stats.get("files_invalidated", 0) > 0
                   or stats.get("clusters_repaired", 0) > 0
                   or stats.get("links_retracted", 0) > 0
                   or stats.get("clusters_reverted", 0) > 0
                   or stats.get("persons_retired", 0) > 0,
        files_mutate=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
