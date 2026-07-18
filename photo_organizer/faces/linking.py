"""
Cross-age face linking.

Proposes cluster merges in three evidence tiers:

1. Window duplicates: clusters sharing a majority of member detections are
   the same identity by construction (overlapping era windows clustered the
   same faces twice). Full confidence.
2. Same-event tracklets: physical sequence evidence — faces matched
   photo-to-photo within a capture-time event (see tracklets.py). Connects
   clusters HDBSCAN split within one era window, which the scored tier
   deliberately never compares. Confidence scales with matched pairs and is
   penalized when the merged pair would put one person in a photo twice
   (possible depiction/twins — surfaced, not decided).
3. Scored cross-age pairs: weighted multi-signal scoring over embedding
   similarity, co-occurrence with shared companions, temporal continuity,
   and supervised anchors (weights in config; estimated age is deliberately
   not a signal).

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


def cannot_link_cluster_pairs(cannot_links: list[dict],
                              det_clusters: dict[int, list[int]]) -> set:
    """
    Resolve detection-level cannot-link constraints (rejected merges) to
    the CURRENT cluster generation: every pair of live clusters where one
    holds a side-a detection and the other a side-b detection is a pair the
    user has already said no to. Person-bound constraints (detections_b is
    None) are consumed by refinement/accept, not here.
    """
    pairs: set = set()
    for constraint in cannot_links:
        if constraint["detections_b"] is None:
            continue
        clusters_a = {c for det in constraint["detections_a"]
                      for c in det_clusters.get(det, ())}
        clusters_b = {c for det in constraint["detections_b"]
                      for c in det_clusters.get(det, ())}
        for cluster_a in clusters_a:
            for cluster_b in clusters_b:
                if cluster_a != cluster_b:
                    pairs.add((cluster_a, cluster_b)
                              if cluster_a < cluster_b
                              else (cluster_b, cluster_a))
    return pairs


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
                 top_k: int = config.LINK_TOP_K,
                 use_tracklets: bool = True,
                 event_gap_minutes: float = config.EVENT_GAP_MINUTES):
        self.db_manager = DBManager(db_path)
        self.min_confidence = min_confidence
        self.max_gap_years = max_gap_years
        self.top_k = top_k
        self.use_tracklets = use_tracklets
        self.event_gap_minutes = event_gap_minutes

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
            "events_grouped": 0,
            "tracklets_built": 0,
            "tracklet_merges_proposed": 0,
            "same_photo_flags": 0,
            "suggestions_proposed": 0,
            "suggestions_suppressed_by_rejection": 0,
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

            # Cannot-link constraints from past rejections, resolved onto
            # the current cluster generation. Every tier consults them: a
            # pair the user already said no to is never proposed again,
            # whatever the evidence tier.
            rejected_pairs = cannot_link_cluster_pairs(
                face_ops.get_active_cannot_links(), context["det_clusters"],
            )

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
                if (lo, hi) in rejected_pairs:
                    stats["suggestions_suppressed_by_rejection"] += 1
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

            # --- Tier 2: same-event tracklet evidence. Unlike the scored
            # tier below, this may propose SAME-window pairs: sequence
            # evidence is exactly what justifies second-guessing HDBSCAN's
            # split of one era window into two density modes.
            tracklet_pairs: set = set()
            if self.use_tracklets:
                tracklet_pairs = self._propose_tracklet_merges(
                    face_ops, recorder, context, duplicate_pairs, stats,
                    rejected_pairs=rejected_pairs,
                )

            # --- Tier 3: scored cross-age pairs. Similarity comes from
            # blocked matrix products per linkable window pair (BLAS does
            # the 512-dim dot products; a Python per-pair loop took an hour
            # at 26k clusters / 226M pairs), then only pairs whose cosine
            # clears an exact algebraic floor are scored individually.
            candidates = self._score_pairs_blocked(
                clusters, anchors, co_occurrence,
                excluded_pairs=duplicate_pairs | tracklet_pairs,
                rejected_pairs=rejected_pairs,
                stats=stats, tqdm=tqdm,
            )

            # Cap at top_k proposals per cluster. Union-find at accept time
            # only needs a spanning set of each identity's pair graph, so
            # proposing every qualifying pair (quadratic per identity) adds
            # review burden without adding connectivity. A pair survives
            # when it is in the top-k of EITHER endpoint.
            keep_k = self.top_k if self.top_k > 0 else None
            if keep_k is not None:
                top_pairs: dict = {}
                for pair_key, (score, _signals) in candidates.items():
                    for endpoint in pair_key:
                        heap = top_pairs.setdefault(endpoint, [])
                        if len(heap) < keep_k:
                            heapq.heappush(heap, (score, pair_key))
                        else:
                            heapq.heappushpop(heap, (score, pair_key))
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
                f"{stats['tracklet_merges_proposed']} tracklet merge(s) "
                f"({stats['same_photo_flags']} same-photo pair(s) flagged), "
                f"{stats['suggestions_proposed']} scored suggestion(s) "
                f"(top-{self.top_k if self.top_k > 0 else 'all'} per cluster "
                f"from {len(candidates)} candidate(s)), "
                f"{stats['suggestions_suppressed_by_rejection']} suppressed "
                f"by past rejections, "
                f"{stats['suggestions_superseded']} stale one(s) superseded."
            )
        return stats

    def _propose_tracklet_merges(self, face_ops: FaceDBOperations,
                                 recorder: RunActionRecorder,
                                 context: dict,
                                 duplicate_pairs: set,
                                 stats: Dict[str, Any],
                                 *, rejected_pairs: set = frozenset()) -> set:
        """Tier 2: build same-event tracklets and propose merges for cluster
        pairs bridged by matched detections. Returns the proposed pair keys
        so the scored tier skips them.

        A pair whose clusters also share a photo would put one person in
        that photo twice — legitimate for framed photos/mirrors/twins, an
        error otherwise. Per the depicted-faces constraint that evidence
        lowers confidence and is surfaced in the signals; it never silently
        blocks or forces the merge.
        """
        from .tracklets import build_tracklets

        rows = face_ops.get_detections_for_tracklets(
            model_name=config.MODEL_NAME,
            model_version=config.MODEL_VERSION_TAG,
            min_det_score=config.MIN_WORKING_DET_SCORE,
        )
        result = build_tracklets(rows, gap_minutes=self.event_gap_minutes)
        stats["events_grouped"] = result.events
        stats["tracklets_built"] = len(result.tracklets)
        stats["same_photo_flags"] = len(result.same_photo_flags)

        det_clusters = context["det_clusters"]
        co_occurrence = context["co_occurrence"]

        # Roll matched detection pairs up to cluster pairs. A detection can
        # sit in several overlapping-window clusters, so one edge may
        # support several pairs; window duplicates among them are already
        # proposed by tier 1 and skipped here.
        pair_sims: dict[tuple[int, int], list[float]] = {}
        for (det_a, det_b), sim in result.edges.items():
            for cluster_a in det_clusters.get(det_a, ()):
                for cluster_b in det_clusters.get(det_b, ()):
                    if cluster_a == cluster_b:
                        continue
                    key = ((cluster_a, cluster_b) if cluster_a < cluster_b
                           else (cluster_b, cluster_a))
                    if key in duplicate_pairs:
                        continue
                    pair_sims.setdefault(key, []).append(sim)

        proposed: set = set()
        for pair_key in sorted(pair_sims):
            if pair_key in rejected_pairs:
                stats["suggestions_suppressed_by_rejection"] += 1
                continue
            sims = pair_sims[pair_key]
            lo, hi = pair_key
            same_photo_overlap = co_occurrence.get(lo, {}).get(hi, 0)
            confidence = min(
                config.TRACKLET_CONFIDENCE_BASE
                + config.TRACKLET_CONFIDENCE_STEP * (len(sims) - 1),
                config.TRACKLET_CONFIDENCE_CAP,
            )
            if same_photo_overlap:
                confidence = max(
                    confidence - config.TRACKLET_SAME_PHOTO_PENALTY, 0.05,
                )
            recorder.record(ActionSpec(
                action_type="face_cluster_merge",
                entity_type="face_cluster",
                entity_id=lo,
                source_path=None,
                target_path=None,
                status="proposed",
                phase=PHASE_FACE_MERGE_PROPOSE,
                sequence=stats["tracklet_merges_proposed"] + 1,
                idempotency_key=(
                    f"face_cluster_merge:{config.MODEL_NAME}:"
                    f"{config.MODEL_VERSION_TAG}:{lo}:{hi}"
                ),
                confidence=int(round(confidence * 100)),
                method="same_event_tracklet",
                payload={
                    "cluster_a_id": lo,
                    "cluster_b_id": hi,
                    "signals": {
                        "tracklet_pairs": len(sims),
                        "mean_pair_similarity": round(sum(sims) / len(sims), 3),
                        "same_photo_overlap": same_photo_overlap,
                    },
                },
            ))
            stats["tracklet_merges_proposed"] += 1
            proposed.add(pair_key)

        if stats["tracklet_merges_proposed"]:
            logging.info(
                f"Proposed {stats['tracklet_merges_proposed']} tracklet "
                f"merge(s) from {stats['tracklets_built']} tracklet(s) "
                f"across {stats['events_grouped']} event(s)."
            )
        return proposed

    def _score_pairs_blocked(self, clusters: list[dict],
                             anchors: dict[int, np.ndarray],
                             co_occurrence: dict[int, dict[int, int]],
                             *,
                             excluded_pairs: set,
                             stats: Dict[str, Any],
                             tqdm,
                             rejected_pairs: set = frozenset()) -> dict:
        """Score all linkable cross-window cluster pairs; return
        {pair_key: (score, signals)} for pairs at or above min_confidence.

        Estimated age is deliberately NOT a signal: buffalo_l's age head is
        too noisy to be evidence.

        Clusters are grouped by era window (same-window pairs are never
        compared — HDBSCAN already separated them) and cosine similarity is
        computed as one matrix product per linkable window pair. Only pairs
        clearing the algebraic cosine floor (see _prescreen_cosine_floor)
        are scored individually — same scores and proposals as the old
        per-pair loop, minutes instead of an hour at tens of thousands of
        clusters.
        """
        windows: dict[tuple[datetime, datetime], list[int]] = {}
        cluster_windows: dict[int, tuple[datetime, datetime]] = {}
        for idx, cluster in enumerate(clusters):
            era = (_parse_era(cluster["era_start"]),
                   _parse_era(cluster["era_end"]))
            if era[0] is None or era[1] is None:
                continue
            windows.setdefault(era, []).append(idx)
            cluster_windows[cluster["id"]] = era

        # Representatives arrive as float32 views from the loader; one
        # stacked matrix serves every block product.
        reps = np.stack([np.asarray(c["representative"], dtype=np.float32)
                         for c in clusters])
        anchor_rows = None
        if anchors:
            anchor_matrix = np.stack([np.asarray(a, dtype=np.float32)
                                      for a in anchors.values()])
            # Cluster x anchor similarities, once — not once per pair.
            anchor_rows = reps @ anchor_matrix.T

        floor = self._prescreen_cosine_floor(bool(anchors))
        gap = timedelta(days=int(self.max_gap_years * 365.25))

        # Window keys sort by start date, so each window's scan of later
        # windows can stop at the first one beyond the era gap.
        window_keys = sorted(windows)
        block_pairs: list[tuple[tuple, tuple]] = []
        for i, window_a in enumerate(window_keys):
            for window_b in window_keys[i + 1:]:
                if window_b[0] - window_a[1] > gap:
                    break
                block_pairs.append((window_a, window_b))

        # pairs_compared preserves the old per-pair loop's meaning: every
        # linkable cross-window pair not already owned by an evidence tier,
        # regardless of score.
        total_pairs = 0
        excluded_in_scope = 0
        for lo, hi in excluded_pairs:
            window_lo = cluster_windows.get(lo)
            window_hi = cluster_windows.get(hi)
            if window_lo is None or window_hi is None or window_lo == window_hi:
                continue
            first, second = sorted((window_lo, window_hi))
            if second[0] - first[1] <= gap:
                excluded_in_scope += 1

        w_emb = config.EMBEDDING_SIMILARITY_WEIGHT
        w_co = config.CO_OCCURRENCE_WEIGHT
        w_temp = config.TEMPORAL_CONTINUITY_WEIGHT
        w_sup = config.SUPERVISED_ANCHOR_WEIGHT

        candidates: dict = {}
        progress = tqdm(block_pairs, desc="Scoring window blocks", unit="block")
        for window_a, window_b in progress:
            if hasattr(progress, "set_postfix"):
                progress.set_postfix(compared=total_pairs,
                                     candidates=len(candidates))
            idx_a = windows[window_a]
            idx_b = windows[window_b]
            total_pairs += len(idx_a) * len(idx_b)
            reps_b = reps[idx_b]
            # Chunk block rows so one huge window pair stays within memory.
            for row0 in range(0, len(idx_a), 2048):
                rows = idx_a[row0:row0 + 2048]
                sims = reps[rows] @ reps_b.T
                for ai, bi in np.argwhere(sims >= floor):
                    ia = rows[int(ai)]
                    ib = idx_b[int(bi)]
                    cluster_a = clusters[ia]
                    cluster_b = clusters[ib]
                    pair_key = ((cluster_a["id"], cluster_b["id"])
                                if cluster_a["id"] < cluster_b["id"]
                                else (cluster_b["id"], cluster_a["id"]))
                    if pair_key in excluded_pairs:
                        continue  # already proposed by an evidence tier

                    cos_sim = float(sims[ai, bi])
                    signals: dict[str, float] = {
                        # Map cosine [0.2, 0.6] -> [0, 1]: same-person-
                        # across-ages typically lands in that band.
                        "embedding": min(max((cos_sim - 0.2) / 0.4, 0.0), 1.0),
                        "co_occurrence": self._score_co_occurrence(
                            cluster_a["id"], cluster_b["id"], co_occurrence),
                        # Temporal continuity: boundary-face similarity,
                        # approximated on a tighter band.
                        "temporal": min(max((cos_sim - 0.3) / 0.3, 0.0), 1.0),
                        "supervised": (
                            self._score_anchor_rows(anchor_rows[ia],
                                                    anchor_rows[ib])
                            if anchor_rows is not None else 0.0),
                    }
                    total = (w_emb * signals["embedding"]
                             + w_co * signals["co_occurrence"]
                             + w_temp * signals["temporal"]
                             + w_sup * signals["supervised"])
                    if total < self.min_confidence:
                        continue
                    # Suppression is counted only for pairs that would have
                    # been proposed — that is the number the user's past
                    # rejections actually saved.
                    if pair_key in rejected_pairs:
                        stats["suggestions_suppressed_by_rejection"] += 1
                        continue
                    signals = {k: round(v, 3) for k, v in signals.items()}
                    signals["total"] = round(total, 3)
                    # Keys must follow id order so cluster_a_key matches
                    # cluster_a_id in the recorded payload (window order and
                    # id order can disagree).
                    lo_first = cluster_a["id"] < cluster_b["id"]
                    signals["cluster_a_key"] = (cluster_a if lo_first
                                                else cluster_b)["cluster_key"]
                    signals["cluster_b_key"] = (cluster_b if lo_first
                                                else cluster_a)["cluster_key"]
                    candidates[pair_key] = (total, signals)

        stats["pairs_compared"] = total_pairs - excluded_in_scope
        return candidates

    def _prescreen_cosine_floor(self, have_anchors: bool) -> float:
        """Largest cosine below which no pair can reach min_confidence even
        with perfect co-occurrence and anchor evidence (both signals are
        clamped to [0, 1], so their weights bound their contribution). The
        floor is exact: prescreening changes runtime, never results.
        """
        ceiling_other = config.CO_OCCURRENCE_WEIGHT + (
            config.SUPERVISED_ANCHOR_WEIGHT if have_anchors else 0.0)
        needed = self.min_confidence - ceiling_other
        if needed <= 0:
            return -1.0  # every pair could qualify; no prescreen

        def cosine_part(cos: float) -> float:
            emb = min(max((cos - 0.2) / 0.4, 0.0), 1.0)
            temp = min(max((cos - 0.3) / 0.3, 0.0), 1.0)
            return (config.EMBEDDING_SIMILARITY_WEIGHT * emb
                    + config.TEMPORAL_CONTINUITY_WEIGHT * temp)

        if cosine_part(1.0) < needed:
            return 2.0  # min_confidence is unreachable at these weights

        lo, hi = -1.0, 1.0  # bisect the monotone piecewise-linear part
        for _ in range(50):
            mid = (lo + hi) / 2
            if cosine_part(mid) >= needed:
                hi = mid
            else:
                lo = mid
        # lo sits strictly below the crossover; the margin absorbs float32
        # matrix-product rounding relative to this float64 arithmetic.
        return max(lo - 1e-4, -1.0)

    @staticmethod
    def _score_anchor_rows(rows_a: np.ndarray, rows_b: np.ndarray) -> float:
        """Both clusters resembling the same labeled person boosts the pair
        (rows are the precomputed cluster x anchor similarities)."""
        mask = (rows_a > 0.3) & (rows_b > 0.3)
        if not mask.any():
            return 0.0
        best = float(np.sqrt(np.max(rows_a[mask] * rows_b[mask])))
        return min(max((best - 0.3) / 0.4, 0.0), 1.0)

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
        "persons_absorbed": 0,
        "clusters_accepted": 0,
        "conflict_components": 0,
        "cannot_link_conflicts": 0,
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

    def person_names() -> dict:
        return dict(db_ops.conn.execute(
            "SELECT id, display_name FROM face_persons WHERE status = 'active'"
        ).fetchall())

    names = person_names()

    def is_named(pid) -> bool:
        return names.get(pid) is not None

    # Cannot-link constraints from past rejections. Detection-set overlap is
    # the test: constraints outlive the cluster generation they were
    # recorded against.
    cannot_links = face_ops.get_active_cannot_links()
    merge_constraints = [c for c in cannot_links
                         if c["detections_b"] is not None]
    person_constraints = [c for c in cannot_links
                          if c["person_id"] is not None]
    member_cache: dict[int, set[int]] = {}

    def members_of(cluster_ids) -> set[int]:
        missing = [c for c in cluster_ids if c not in member_cache]
        if missing:
            loaded = face_ops.get_live_members_for_clusters(missing)
            for cluster_id in missing:
                member_cache[cluster_id] = loaded.get(cluster_id, set())
        detections: set[int] = set()
        for cluster_id in cluster_ids:
            detections |= member_cache[cluster_id]
        return detections

    def person_blocked(person_id: int, detections: set[int]) -> bool:
        return any(c["person_id"] == person_id and
                   (c["detections_a"] & detections)
                   for c in person_constraints)

    # --- Assignments first: they establish cluster→person links merges can
    # reuse within the same accept invocation. An existing link to an UNNAMED
    # person is not a conflict — anonymous groups are placeholders, so the
    # existing person absorbs into the assignment's target. Only a link to a
    # DIFFERENT named person blocks the assignment.
    if assignments:
        linked = dict(face_ops.get_accepted_cluster_person_links())
        for assignment in assignments:
            cluster_id = int(assignment["payload"]["cluster_id"])
            person_id = int(assignment["payload"]["person_id"])
            if person_blocked(person_id, members_of([cluster_id])):
                stats["cannot_link_conflicts"] += 1
                logging.warning(
                    f"Assignment of cluster {cluster_id} to person "
                    f"{person_id} violates a cannot-link from a past "
                    f"rejection — skipped."
                )
                continue
            existing = linked.get(cluster_id)
            if existing is not None and existing != person_id:
                if is_named(existing):
                    stats["conflict_components"] += 1
                    logging.warning(
                        f"Assignment of cluster {cluster_id} to person "
                        f"{person_id} conflicts with existing link to named "
                        f"person {existing} ({names[existing]!r}) — skipped."
                    )
                    continue
                absorb = face_ops.absorb_person(
                    run_id=run_id, absorbed_id=existing, winner_id=person_id,
                )
                stats["persons_absorbed"] += 1
                for moved_cluster, moved_person in list(linked.items()):
                    if moved_person == existing:
                        linked[moved_cluster] = person_id
                existing = person_id
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
        named_persons = sorted(p for p in persons if is_named(p))

        # Two or more NAMED identities in one component is the only true
        # conflict — accepting would silently fuse people the user has
        # explicitly distinguished. Anonymous persons are placeholders and
        # merge freely.
        if len(named_persons) > 1:
            stats["conflict_components"] += 1
            conflicted_clusters.update(cluster_ids)
            logging.warning(
                f"Merge component {sorted(cluster_ids)} touches multiple "
                f"NAMED persons "
                f"{[(p, names[p]) for p in named_persons]} — skipped; "
                f"reject one of the suggestions or relabel first."
            )
            continue

        component_detections = members_of(cluster_ids)
        if any((c["detections_a"] & component_detections)
               and (c["detections_b"] & component_detections)
               for c in merge_constraints):
            stats["cannot_link_conflicts"] += 1
            conflicted_clusters.update(cluster_ids)
            logging.warning(
                f"Merge component {sorted(cluster_ids)} joins faces the "
                f"user separated in a past rejection — skipped."
            )
            continue

        if named_persons:
            person_id = named_persons[0]
        elif persons:
            person_id = min(persons)
        else:
            person_id = None

        # A component landing on a person the user has rejected for any of
        # its faces is a conflict, not an accept. Checked before creating a
        # person so a refused component leaves no orphan behind.
        if person_id is not None and person_blocked(person_id,
                                                    component_detections):
            stats["cannot_link_conflicts"] += 1
            conflicted_clusters.update(cluster_ids)
            logging.warning(
                f"Merge component {sorted(cluster_ids)} resolves to person "
                f"{person_id}, which a past rejection excludes for its "
                f"faces — skipped."
            )
            continue

        if person_id is not None:
            stats["persons_reused"] += 1
        else:
            person_id = face_ops.create_person(run_id=run_id, display_name=None)
            stats["persons_created"] += 1

        for absorbed in sorted(persons - {person_id}):
            face_ops.absorb_person(
                run_id=run_id, absorbed_id=absorbed, winner_id=person_id,
            )
            stats["persons_absorbed"] += 1
            for cluster_id, owner in list(cluster_to_person.items()):
                if owner == absorbed:
                    cluster_to_person[cluster_id] = person_id

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
        f"{stats['persons_absorbed']} anonymous absorbed, "
        f"{stats['conflict_components']} conflict component(s) skipped, "
        f"{stats['cannot_link_conflicts']} cannot-link conflict(s) skipped."
    )
    return stats
