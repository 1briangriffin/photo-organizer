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
                 max_gap_years: float = config.MAX_ERA_GAP_YEARS):
        self.db_manager = DBManager(db_path)
        self.min_confidence = min_confidence
        self.max_gap_years = max_gap_years

    def run(self, *, run_id: int) -> Dict[str, Any]:
        stats = {
            "clusters_considered": 0,
            "pairs_compared": 0,
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
            co_occurrence = {
                cluster["id"]: face_ops.get_co_occurring_clusters(cluster["id"])
                for cluster in clusters
            }
            median_ages = {
                cluster["id"]: face_ops.get_cluster_median_age(cluster["id"])
                for cluster in clusters
            }

            for i, cluster_a in enumerate(clusters):
                for cluster_b in clusters[i + 1:]:
                    # Same era window: HDBSCAN already decided these are
                    # different identities.
                    if (cluster_a["era_start"], cluster_a["era_end"]) == (
                            cluster_b["era_start"], cluster_b["era_end"]):
                        continue
                    if not eras_linkable(cluster_a, cluster_b, self.max_gap_years):
                        continue

                    score, signals = self._score_pair(
                        cluster_a, cluster_b, anchors, co_occurrence, median_ages,
                    )
                    stats["pairs_compared"] += 1
                    if score < self.min_confidence:
                        continue

                    lo, hi = sorted((cluster_a["id"], cluster_b["id"]))
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
                            "cluster_a_key": cluster_a["cluster_key"],
                            "cluster_b_key": cluster_b["cluster_key"],
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
                f"Compared {stats['pairs_compared']} cluster pair(s), "
                f"proposed {stats['suggestions_proposed']} merge suggestion(s), "
                f"superseded {stats['suggestions_superseded']} stale one(s)."
            )
        return stats

    def _score_pair(self, cluster_a: dict, cluster_b: dict,
                    anchors: dict[int, np.ndarray],
                    co_occurrence: dict[int, dict[int, int]],
                    median_ages: dict[int, Optional[float]],
                    ) -> tuple[float, dict]:
        """Compute the weighted confidence score for a cluster pair."""
        signals: dict[str, float] = {}
        emb_a = np.asarray(cluster_a["representative"], dtype=np.float32)
        emb_b = np.asarray(cluster_b["representative"], dtype=np.float32)

        # Signal 1: embedding similarity. Map cosine [0.2, 0.6] -> [0, 1]:
        # same-person-across-ages typically lands in that band for ArcFace.
        cos_sim = float(np.dot(emb_a, emb_b))
        signals["embedding"] = float(np.clip((cos_sim - 0.2) / 0.4, 0.0, 1.0))

        signals["co_occurrence"] = self._score_co_occurrence(
            cluster_a["id"], cluster_b["id"], co_occurrence,
        )
        signals["age_progression"] = self._score_age_progression(
            cluster_a, cluster_b, median_ages,
        )
        # Temporal continuity: boundary-face similarity, approximated by the
        # representative similarity on a tighter band.
        signals["temporal"] = float(np.clip((cos_sim - 0.3) / 0.3, 0.0, 1.0))
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
        return float(np.clip((best - 0.3) / 0.4, 0.0, 1.0))

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
