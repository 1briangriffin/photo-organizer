"""
Cross-age face linking.

Scores pairs of era clusters to find the same person at different ages,
using weighted multi-signal scoring:

1. Embedding similarity (0.35): cosine sim between representative embeddings
2. Co-occurrence with anchors (0.25): shared photos with the same other people
3. Age progression (0.20): estimated-age difference matches the era time gap
4. Temporal continuity (0.10): similarity near era boundaries (proxy)
5. Supervised anchor match (0.10): both clusters resemble a labeled person

Merge suggestions are run_actions proposals (action_type
'face_cluster_merge'), so the standard proposal lifecycle applies: review
with `photo-catalog-query --show-proposals --action-type face_cluster_merge`,
reject with `--reject-proposal`, and re-running the linker supersedes stale
suggestions. Accepting merges (union-find into persons) happens in the
review/refine phase.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from ..database.db import DBManager
from ..database.ops import DBOperations
from ..pipeline.actions import (
    ActionSpec,
    PHASE_FACE_MERGE_PROPOSE,
    RunActionRecorder,
)
from ..pipeline.lifecycle import supersede_stale_proposals
from . import config
from .db_ops import FaceDBOperations


class UnionFind:
    """Simple union-find for merging clusters into persons (used when
    accepted merge suggestions are applied in the review phase)."""

    def __init__(self):
        self.parent: dict[int, int] = {}
        self.rank: dict[int, int] = {}

    def find(self, x: int) -> int:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        return self.parent[x]

    def union(self, x: int, y: int):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        # Union by rank
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1

    def groups(self) -> dict[int, list[int]]:
        """Return connected components as {root: [members]}."""
        result: dict[int, list[int]] = {}
        for x in self.parent:
            root = self.find(x)
            result.setdefault(root, []).append(x)
        return result


def _parse_era(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def eras_linkable(cluster_a: dict, cluster_b: dict,
                  max_gap_years: float = config.MAX_ERA_GAP_YEARS) -> bool:
    """
    True when two clusters' eras overlap or are separated by at most
    max_gap_years. Same-era pairs are excluded by the caller (HDBSCAN
    already separated identities within one era).
    """
    a_start = _parse_era(cluster_a["era_start"])
    a_end = _parse_era(cluster_a["era_end"])
    b_start = _parse_era(cluster_b["era_start"])
    b_end = _parse_era(cluster_b["era_end"])
    if None in (a_start, a_end, b_start, b_end):
        return False

    if a_start < b_end and b_start < a_end:
        return True

    gap = timedelta(days=int(max_gap_years * 365.25))
    if a_end <= b_start:
        return b_start - a_end <= gap
    return a_start - b_end <= gap


class CrossAgeLinker:
    """Scores live cluster pairs and proposes merge suggestions."""

    def __init__(self, db_path: Path,
                 min_confidence: float = config.MIN_MERGE_CONFIDENCE,
                 max_gap_years: float = config.MAX_ERA_GAP_YEARS,
                 top_k: int = config.LINK_TOP_K):
        self.db_manager = DBManager(db_path)
        self.min_confidence = min_confidence
        self.max_gap_years = max_gap_years
        self.top_k = top_k

    def run(self, *, run_id: int) -> Dict[str, Any]:
        import heapq

        try:
            from tqdm import tqdm
        except ImportError:  # pragma: no cover - tqdm is a core dependency
            tqdm = lambda x, **kw: x  # noqa: E731

        stats = {
            "clusters_considered": 0,
            "pairs_compared": 0,
            "duplicates_proposed": 0,
            "suggestions_proposed": 0,
            "suggestions_superseded": 0,
        }

        with self.db_manager as conn:
            db_ops = DBOperations(conn)
            face_ops = FaceDBOperations(db_ops)
            recorder = RunActionRecorder(db_ops, run_id)

            clusters = face_ops.get_clusters_for_linking(
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )
            stats["clusters_considered"] = len(clusters)
            if len(clusters) < 2:
                stats["suggestions_superseded"] = supersede_stale_proposals(
                    db_ops, run_id,
                )
                conn.commit()
                logging.info("Not enough clusters to link.")
                return stats

            anchors = self._compute_anchor_embeddings(face_ops)
            logging.info("Loading cluster context (co-occurrence, ages)...")
            context = face_ops.get_cluster_link_context(
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )
            co_occurrence = context["co_occurrence"]
            median_ages = context["median_ages"]

            # --- Tier 1: window duplicates. Clusters sharing a majority of
            # their member detections are the same identity by construction
            # (overlapping era windows clustered the same faces twice) — no
            # model judgment involved, so they are proposed at full
            # confidence and skipped by the scored tier below.
            member_counts = context["member_counts"]
            duplicate_pairs: set = set()
            for (lo, hi), shared in context["shared_members"].items():
                denom = min(member_counts.get(lo, 0), member_counts.get(hi, 0))
                if denom == 0 or shared / denom < config.DUPLICATE_MEMBER_OVERLAP:
                    continue
                duplicate_pairs.add((lo, hi))
                recorder.record(ActionSpec(
                    action_type="face_cluster_merge",
                    entity_type="face_cluster",
                    entity_id=lo,
                    source_path=None,
                    target_path=None,
                    status="proposed",
                    phase=PHASE_FACE_MERGE_PROPOSE,
                    sequence=stats["duplicates_proposed"] + 1,
                    idempotency_key=(
                        f"face_cluster_merge:{config.MODEL_NAME}:"
                        f"{config.MODEL_VERSION_TAG}:{lo}:{hi}"
                    ),
                    confidence=100,
                    method="window_duplicate",
                    payload={
                        "cluster_a_id": lo,
                        "cluster_b_id": hi,
                        "signals": {
                            "member_overlap": round(shared / denom, 3),
                            "shared_detections": shared,
                        },
                    },
                ))
                stats["duplicates_proposed"] += 1
            if stats["duplicates_proposed"]:
                logging.info(
                    f"Proposed {stats['duplicates_proposed']} window-duplicate "
                    f"merge(s) from shared detections."
                )

            # Parse era bounds once — the pair loop must not re-parse ISO
            # strings millions of times.
            for cluster in clusters:
                cluster["_era"] = (_parse_era(cluster["era_start"]),
                                   _parse_era(cluster["era_end"]))
            gap = timedelta(days=int(self.max_gap_years * 365.25))

            # --- Tier 2: scored cross-age pairs, capped at top_k proposals
            # per cluster. Union-find at accept time only needs a spanning
            # set of each identity's pair graph, so proposing every
            # qualifying pair (quadratic per identity) adds review burden
            # without adding connectivity. A pair survives when it is in the
            # top-k of EITHER endpoint.
            keep_k = self.top_k if self.top_k > 0 else None
            top_pairs: dict = {}
            candidates: dict = {}

            progress = tqdm(enumerate(clusters), total=len(clusters),
                            desc="Scoring cluster pairs", unit="cluster")
            for i, cluster_a in progress:
                if hasattr(progress, "set_postfix"):
                    progress.set_postfix(
                        compared=stats["pairs_compared"],
                        candidates=len(candidates),
                    )
                a_start, a_end = cluster_a["_era"]
                if a_start is None or a_end is None:
                    continue
                for cluster_b in clusters[i + 1:]:
                    b_start, b_end = cluster_b["_era"]
                    if b_start is None or b_end is None:
                        continue
                    # Same era window: HDBSCAN already decided these are
                    # different identities.
                    if a_start == b_start and a_end == b_end:
                        continue
                    # Inline eras_linkable, on pre-parsed datetimes.
                    overlapping = a_start < b_end and b_start < a_end
                    if not overlapping:
                        if a_end <= b_start:
                            if b_start - a_end > gap:
                                continue
                        elif a_start - b_end > gap:
                            continue

                    pair_key = ((cluster_a["id"], cluster_b["id"])
                                if cluster_a["id"] < cluster_b["id"]
                                else (cluster_b["id"], cluster_a["id"]))
                    if pair_key in duplicate_pairs:
                        continue  # already proposed by the duplicate tier

                    score, signals = self._score_pair(
                        cluster_a, cluster_b, anchors, co_occurrence, median_ages,
                    )
                    stats["pairs_compared"] += 1
                    if score < self.min_confidence:
                        continue

                    signals["cluster_a_key"] = cluster_a["cluster_key"]
                    signals["cluster_b_key"] = cluster_b["cluster_key"]
                    candidates[pair_key] = (score, signals)
                    if keep_k is not None:
                        for endpoint in pair_key:
                            heap = top_pairs.setdefault(endpoint, [])
                            if len(heap) < keep_k:
                                heapq.heappush(heap, (score, pair_key))
                            else:
                                heapq.heappushpop(heap, (score, pair_key))

            if keep_k is not None:
                surviving = {pair for heap in top_pairs.values()
                             for _, pair in heap}
            else:
                surviving = set(candidates)

            for pair_key in tqdm(sorted(surviving), desc="Recording proposals",
                                 unit="proposal"):
                score, signals = candidates[pair_key]
                lo, hi = pair_key
                cluster_a_key = signals.pop("cluster_a_key")
                cluster_b_key = signals.pop("cluster_b_key")
                recorder.record(ActionSpec(
                    action_type="face_cluster_merge",
                    entity_type="face_cluster",
                    entity_id=lo,
                    source_path=None,
                    target_path=None,
                    status="proposed",
                    phase=PHASE_FACE_MERGE_PROPOSE,
                    sequence=stats["suggestions_proposed"] + 1,
                    idempotency_key=(
                        f"face_cluster_merge:{config.MODEL_NAME}:"
                        f"{config.MODEL_VERSION_TAG}:{lo}:{hi}"
                    ),
                    confidence=int(round(score * 100)),
                    method="cross_age_multisignal",
                    payload={
                        "cluster_a_id": lo,
                        "cluster_b_id": hi,
                        "cluster_a_key": cluster_a_key,
                        "cluster_b_key": cluster_b_key,
                        "signals": signals,
                    },
                ))
                stats["suggestions_proposed"] += 1

            # Suggestions from earlier link runs that this run did not
            # regenerate (clusters superseded, thresholds changed) resolve
            # through the standard scope-level supersede.
            stats["suggestions_superseded"] = supersede_stale_proposals(
                db_ops, run_id,
            )
            conn.commit()

            logging.info(
                f"Compared {stats['pairs_compared']} cluster pair(s): "
                f"{stats['duplicates_proposed']} window-duplicate merge(s), "
                f"{stats['suggestions_proposed']} scored suggestion(s) "
                f"(top-{self.top_k if self.top_k > 0 else 'all'} per cluster "
                f"from {len(candidates)} candidate(s)), "
                f"{stats['suggestions_superseded']} stale one(s) superseded."
            )
        return stats

    def _score_pair(self, cluster_a: dict, cluster_b: dict,
                    anchors: dict[int, np.ndarray],
                    co_occurrence: dict[int, dict[int, int]],
                    median_ages: dict[int, Optional[float]],
                    ) -> tuple[float, dict]:
        """Compute the weighted confidence score for a cluster pair."""
        signals: dict[str, float] = {}
        # Representatives arrive as float32 arrays from the loader — no
        # per-pair conversion (this runs for millions of pairs).
        emb_a = cluster_a["representative"]
        emb_b = cluster_b["representative"]

        # Signal 1: embedding similarity. Map cosine [0.2, 0.6] -> [0, 1]:
        # same-person-across-ages typically lands in that band for ArcFace.
        cos_sim = float(np.dot(emb_a, emb_b))
        signals["embedding"] = min(max((cos_sim - 0.2) / 0.4, 0.0), 1.0)

        signals["co_occurrence"] = self._score_co_occurrence(
            cluster_a["id"], cluster_b["id"], co_occurrence,
        )
        signals["age_progression"] = self._score_age_progression(
            cluster_a, cluster_b, median_ages,
        )
        # Temporal continuity: boundary-face similarity, approximated by the
        # representative similarity on a tighter band.
        signals["temporal"] = min(max((cos_sim - 0.3) / 0.3, 0.0), 1.0)
        signals["supervised"] = self._score_supervised_anchor(emb_a, emb_b, anchors)

        total = (
            config.EMBEDDING_SIMILARITY_WEIGHT * signals["embedding"]
            + config.CO_OCCURRENCE_WEIGHT * signals["co_occurrence"]
            + config.AGE_PROGRESSION_WEIGHT * signals["age_progression"]
            + config.TEMPORAL_CONTINUITY_WEIGHT * signals["temporal"]
            + config.SUPERVISED_ANCHOR_WEIGHT * signals["supervised"]
        )

        signals = {k: round(v, 3) for k, v in signals.items()}
        signals["total"] = round(total, 3)
        return total, signals

    @staticmethod
    def _score_co_occurrence(id_a: int, id_b: int,
                             co_occurrence: dict[int, dict[int, int]]) -> float:
        """Shared co-occurring clusters (e.g. the same parents appearing in
        photos with both the toddler cluster and the school-age cluster)."""
        co_a = co_occurrence.get(id_a, {})
        co_b = co_occurrence.get(id_b, {})
        if not co_a or not co_b:
            return 0.0
        shared = set(co_a) & set(co_b)
        if not shared:
            return 0.0
        total_shared = sum(min(co_a[c], co_b[c]) for c in shared)
        max_possible = max(sum(co_a.values()), sum(co_b.values()), 1)
        return min(total_shared / max_possible * 2, 1.0)

    def _score_age_progression(self, cluster_a: dict, cluster_b: dict,
                               median_ages: dict[int, Optional[float]]) -> float:
        """Does the estimated-age difference match the era time gap?"""
        a_mid = self._era_midpoint(cluster_a)
        b_mid = self._era_midpoint(cluster_b)
        if a_mid is None or b_mid is None:
            return 0.0
        time_gap_years = abs((b_mid - a_mid).days) / 365.25
        if time_gap_years < 0.5:
            return 0.5  # Very close in time: neutral evidence.

        age_a = median_ages.get(cluster_a["id"])
        age_b = median_ages.get(cluster_b["id"])
        if age_a is None or age_b is None:
            return 0.0
        error = abs(abs(age_b - age_a) - time_gap_years)
        return float(max(0.0, 1.0 - error / 10.0))

    @staticmethod
    def _score_supervised_anchor(emb_a: np.ndarray, emb_b: np.ndarray,
                                 anchors: dict[int, np.ndarray]) -> float:
        """Both clusters resembling the same labeled person boosts the pair."""
        best = 0.0
        for anchor in anchors.values():
            sim_a = float(np.dot(emb_a, anchor))
            sim_b = float(np.dot(emb_b, anchor))
            if sim_a > 0.3 and sim_b > 0.3:
                best = max(best, (sim_a * sim_b) ** 0.5)
        return min(max((best - 0.3) / 0.4, 0.0), 1.0)

    @staticmethod
    def _compute_anchor_embeddings(face_ops: FaceDBOperations) -> dict[int, np.ndarray]:
        """Mean embedding per labeled person (empty until identities exist)."""
        anchors: dict[int, np.ndarray] = {}
        for person_id, embeddings in face_ops.get_labeled_person_embeddings().items():
            mean_emb = np.mean(np.asarray(embeddings, dtype=np.float32), axis=0)
            norm = np.linalg.norm(mean_emb)
            if norm > 0:
                mean_emb = mean_emb / norm
            anchors[person_id] = mean_emb
        return anchors

    @staticmethod
    def _era_midpoint(cluster: dict) -> Optional[datetime]:
        start = _parse_era(cluster["era_start"])
        end = _parse_era(cluster["era_end"])
        if start is None or end is None:
            return None
        return start + (end - start) / 2


def unwind_accept_run(db_ops: DBOperations, accept_run_id: int,
                      *, run_id: int) -> Dict[str, Any]:
    """
    Revert the accepted face state a previous accept run created, using its
    run provenance to target exactly that run's effects:

    - cluster/person links it created are retracted — EXCEPT links to persons
      that have since been named (naming is later human work the unwind must
      not destroy; those are kept and reported)
    - clusters it accepted revert to proposed (unless still held accepted by
      a surviving link), and so do the memberships it accepted
    - unnamed persons it created are retired once they have no accepted links
    - the merge/assign proposals it applied resolve to superseded with a note

    Everything happens under the unwind run's own provenance, so the unwind
    is itself auditable and the original run's history stays intact.
    """
    from datetime import datetime, UTC

    now = datetime.now(UTC).isoformat()
    conn = db_ops.conn
    stats: Dict[str, Any] = {}

    run_row = conn.execute(
        "SELECT tool, command FROM command_runs WHERE id = ?",
        (accept_run_id,),
    ).fetchone()
    if run_row is None:
        raise SystemExit(f"No command run with id {accept_run_id}.")

    stats["links_kept_named"] = conn.execute(
        """
        SELECT COUNT(*) FROM face_person_links
        WHERE created_by_run_id = ? AND status = 'accepted'
          AND person_id IN (
              SELECT id FROM face_persons WHERE display_name IS NOT NULL
          )
        """,
        (accept_run_id,),
    ).fetchone()[0]

    cur = conn.execute(
        """
        UPDATE face_person_links
           SET status = 'retracted', updated_by_run_id = ?, updated_at = ?
         WHERE created_by_run_id = ? AND status = 'accepted'
           AND person_id NOT IN (
               SELECT id FROM face_persons WHERE display_name IS NOT NULL
           )
        """,
        (run_id, now, accept_run_id),
    )
    stats["links_retracted"] = cur.rowcount or 0

    still_linked = """
        SELECT cluster_id FROM face_person_links
        WHERE status = 'accepted' AND cluster_id IS NOT NULL
    """
    cur = conn.execute(
        f"""
        UPDATE face_cluster_members
           SET status = 'proposed', updated_by_run_id = ?, updated_at = ?
         WHERE status = 'accepted' AND updated_by_run_id = ?
           AND cluster_id NOT IN ({still_linked})
        """,
        (run_id, now, accept_run_id),
    )
    stats["memberships_reverted"] = cur.rowcount or 0

    cur = conn.execute(
        f"""
        UPDATE face_clusters
           SET status = 'proposed', updated_by_run_id = ?, updated_at = ?
         WHERE status = 'accepted' AND updated_by_run_id = ?
           AND id NOT IN ({still_linked})
        """,
        (run_id, now, accept_run_id),
    )
    stats["clusters_reverted"] = cur.rowcount or 0

    cur = conn.execute(
        """
        UPDATE face_persons
           SET status = 'retired', updated_by_run_id = ?, updated_at = ?
         WHERE created_by_run_id = ? AND status = 'active'
           AND display_name IS NULL
           AND id NOT IN (
               SELECT person_id FROM face_person_links WHERE status = 'accepted'
           )
        """,
        (run_id, now, accept_run_id),
    )
    stats["persons_retired"] = cur.rowcount or 0

    cur = conn.execute(
        """
        UPDATE run_actions
           SET status = 'superseded', resolved_by_run_id = ?, resolved_at = ?,
               resolution_note = ?
         WHERE applied_by_run_id = ? AND status = 'applied'
           AND action_type IN ('face_cluster_merge', 'face_person_assign',
                               'face_person_link')
        """,
        (run_id, now, f"unwound accept run #{accept_run_id}", accept_run_id),
    )
    stats["actions_superseded"] = cur.rowcount or 0

    logging.info(
        f"Unwound accept run #{accept_run_id}: "
        f"{stats['links_retracted']} link(s) retracted "
        f"({stats['links_kept_named']} kept — named persons), "
        f"{stats['clusters_reverted']} cluster(s) and "
        f"{stats['memberships_reverted']} membership(s) back to proposed, "
        f"{stats['persons_retired']} anonymous person(s) retired, "
        f"{stats['actions_superseded']} applied action(s) superseded."
    )
    return stats


def apply_accepted_proposals(db_ops: DBOperations, action_ids,
                             *, run_id: int) -> Dict[str, Any]:
    """
    Apply user-accepted face proposals: cross-age merges
    (`face_cluster_merge`) and refinement assignments (`face_person_assign`).

    Assignments apply first — each links a cluster to its proposed person —
    so merges can reuse the persons those assignments establish. Merges then
    union-find the accepted pairs together with every existing accepted
    cluster→person link; each connected component resolves to one person
    (reused when the component already touches exactly one, created when it
    touches none). Component clusters and their memberships become accepted,
    cluster-level person links are recorded, and the proposals flip to
    applied.

    A component (or assignment target) touching a DIFFERENT existing person
    is a conflict the model cannot resolve automatically — those are skipped
    and reported for review rather than silently merging named identities.

    Returns stats including skipped ids and conflicts.
    """
    face_ops = FaceDBOperations(db_ops)
    stats: Dict[str, Any] = {
        "merges_applied": 0,
        "assignments_applied": 0,
        "proposals_skipped": 0,
        "persons_created": 0,
        "persons_reused": 0,
        "clusters_accepted": 0,
        "conflict_components": 0,
    }

    all_proposals, skipped = face_ops.get_face_proposals(action_ids)
    stats["proposals_skipped"] = len(skipped)
    if skipped:
        logging.warning(
            f"Skipped {len(skipped)} id(s) that are not pending face "
            f"proposals: {', '.join(str(i) for i in skipped)}"
        )
    assignments = [p for p in all_proposals
                   if p["action_type"] == "face_person_assign"]
    proposals = [
        {
            "action_id": p["action_id"],
            "cluster_a_id": int(p["payload"]["cluster_a_id"]),
            "cluster_b_id": int(p["payload"]["cluster_b_id"]),
        }
        for p in all_proposals if p["action_type"] == "face_cluster_merge"
    ]

    # --- Assignments first: they establish cluster→person links merges can
    # reuse within the same accept invocation.
    if assignments:
        linked = dict(face_ops.get_accepted_cluster_person_links())
        for assignment in assignments:
            cluster_id = int(assignment["payload"]["cluster_id"])
            person_id = int(assignment["payload"]["person_id"])
            existing = linked.get(cluster_id)
            if existing is not None and existing != person_id:
                stats["conflict_components"] += 1
                logging.warning(
                    f"Assignment of cluster {cluster_id} to person {person_id} "
                    f"conflicts with existing link to person {existing} — skipped."
                )
                continue
            face_ops.accept_cluster(run_id=run_id, cluster_id=cluster_id)
            if existing is None:
                face_ops.link_cluster_to_person(
                    run_id=run_id, cluster_id=cluster_id, person_id=person_id,
                    link_method="auto_assign_accept",
                    confidence=(assignment["confidence"] / 100
                                if assignment["confidence"] is not None else None),
                )
                linked[cluster_id] = person_id
            face_ops.mark_merge_proposal_applied(
                action_id=assignment["action_id"], run_id=run_id,
            )
            stats["assignments_applied"] += 1
            stats["clusters_accepted"] += 1
            stats["persons_reused"] += 1

    if not proposals:
        if assignments:
            logging.info(
                f"Applied {stats['assignments_applied']} assignment(s); "
                f"{stats['conflict_components']} conflict(s) skipped."
            )
        return stats

    uf = UnionFind()
    for proposal in proposals:
        uf.union(proposal["cluster_a_id"], proposal["cluster_b_id"])

    cluster_to_person: dict[int, int] = {}
    person_clusters: dict[int, list[int]] = {}
    for cluster_id, person_id in face_ops.get_accepted_cluster_person_links():
        cluster_to_person[cluster_id] = person_id
        person_clusters.setdefault(person_id, []).append(cluster_id)
        uf.find(cluster_id)
    for cluster_ids in person_clusters.values():
        for other in cluster_ids[1:]:
            uf.union(cluster_ids[0], other)

    conflicted_clusters: set[int] = set()
    for root, cluster_ids in uf.groups().items():
        persons = {cluster_to_person[c] for c in cluster_ids if c in cluster_to_person}
        if len(persons) > 1:
            stats["conflict_components"] += 1
            conflicted_clusters.update(cluster_ids)
            logging.warning(
                f"Merge component {sorted(cluster_ids)} touches multiple "
                f"persons {sorted(persons)} — skipped; resolve by relabeling "
                f"or rejecting one of the suggestions."
            )
            continue

        if persons:
            person_id = persons.pop()
            stats["persons_reused"] += 1
        else:
            person_id = face_ops.create_person(run_id=run_id, display_name=None)
            stats["persons_created"] += 1

        for cluster_id in cluster_ids:
            face_ops.accept_cluster(run_id=run_id, cluster_id=cluster_id)
            if cluster_to_person.get(cluster_id) != person_id:
                face_ops.link_cluster_to_person(
                    run_id=run_id, cluster_id=cluster_id, person_id=person_id,
                    link_method="merge_accept",
                )
            stats["clusters_accepted"] += 1

    for proposal in proposals:
        if (proposal["cluster_a_id"] in conflicted_clusters
                or proposal["cluster_b_id"] in conflicted_clusters):
            continue
        face_ops.mark_merge_proposal_applied(
            action_id=proposal["action_id"], run_id=run_id,
        )
        stats["merges_applied"] += 1

    logging.info(
        f"Applied {stats['merges_applied']} merge(s): "
        f"{stats['clusters_accepted']} cluster(s) accepted, "
        f"{stats['persons_created']} person(s) created, "
        f"{stats['persons_reused']} reused, "
        f"{stats['conflict_components']} conflict component(s) skipped."
    )
    return stats
